use std::{
    collections::BTreeMap,
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, mpsc},
    time,
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    bilibili::{BilibiliResolver, PlayRequest},
    dlna,
    frame::{self, Decoder, Frame},
    state::{AppState, NvaEvent, SessionOrigin, SupersededPlay},
    upnp,
};

const MAX_HEADERS: usize = 64 * 1024;
const MAX_BODY: usize = 512 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(8);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const WORKER_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const NVA_SERVER: &str = "Linux/3.0.0, UPnP/1.0, Platinum/1.0.5.13";
const NVA_HTTP_SERVER: &str = "UDashboardOS/1.0 UPnP/1.0 UDashboard/0.1";

#[derive(Clone)]
struct Receiver {
    state: AppState,
    resolver: BilibiliResolver,
    active_request: Arc<Mutex<Option<ActiveRequest>>>,
    port: u16,
}

#[derive(Clone)]
struct ActiveRequest {
    session_id: String,
    request: PlayRequest,
    available_qualities: Vec<u64>,
    danmaku_enabled: bool,
}

#[derive(Debug)]
struct InboundCommand {
    method: String,
    params: Option<Value>,
    play_epoch: Option<u64>,
}

#[derive(Debug)]
struct InitialRequest {
    method: String,
    path: String,
    protocol: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    leftover: Vec<u8>,
}

impl InitialRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn path(&self) -> &str {
        self.path.split('?').next().unwrap_or(&self.path)
    }

    fn response_protocol(&self) -> &'static str {
        if self.protocol.eq_ignore_ascii_case("HTTP/1.0") {
            "HTTP/1.0"
        } else {
            "HTTP/1.1"
        }
    }
}

pub async fn run(state: AppState, listen: SocketAddrV4) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot bind NVA listener to {listen}"))?;
    let receiver = Receiver {
        state,
        resolver: BilibiliResolver::new()?,
        active_request: Arc::new(Mutex::new(None)),
        port: listen.port(),
    };
    info!(%listen, "NVA control listener ready");
    loop {
        let (stream, peer) = listener.accept().await.context("NVA accept failed")?;
        let receiver = receiver.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(receiver, stream, peer).await {
                debug!(%peer, %error, "NVA connection closed with an error");
            }
        });
    }
}

async fn handle_connection(
    receiver: Receiver,
    mut stream: TcpStream,
    peer: SocketAddr,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let request = read_request(&mut stream).await?;
    debug!(%peer, method = request.method, path = request.path, protocol = request.protocol, "NVA HTTP request");
    match request.method.as_str() {
        "SETUP" | "RESTORE" | "STARTRESTORE" => {
            handle_upgrade(receiver, stream, peer, request).await
        }
        "GET" | "HEAD" => {
            let body = if request.path().eq_ignore_ascii_case("/description.xml")
                || request
                    .path()
                    .eq_ignore_ascii_case("/bilibili/description.xml")
            {
                Some(upnp::description_xml(&receiver.state, receiver.port))
            } else {
                upnp::service_document(request.path()).map(str::to_owned)
            };
            match body {
                Some(body) => {
                    let bytes = if request.method == "HEAD" {
                        &[][..]
                    } else {
                        body.as_bytes()
                    };
                    write_response(
                        &mut stream,
                        request.response_protocol(),
                        200,
                        "OK",
                        "text/xml; charset=utf-8",
                        bytes,
                        Some(body.len()),
                        &["EXT:"],
                    )
                    .await
                }
                None => {
                    write_response(
                        &mut stream,
                        request.response_protocol(),
                        404,
                        "Not Found",
                        "text/plain; charset=utf-8",
                        b"not found",
                        None,
                        &[],
                    )
                    .await
                }
            }
        }
        "POST"
            if request
                .path()
                .eq_ignore_ascii_case("/NirvanaControl/action") =>
        {
            let action = upnp::soap_action(
                request.header("soapaction"),
                &String::from_utf8_lossy(&request.body),
            );
            match action.and_then(|action| upnp::soap_response(&action).ok()) {
                Some(body) => {
                    write_response(
                        &mut stream,
                        request.response_protocol(),
                        200,
                        "OK",
                        "text/xml; charset=utf-8",
                        body.as_bytes(),
                        None,
                        &["EXT:"],
                    )
                    .await
                }
                None => {
                    let body = upnp::soap_fault(401, "Invalid Action");
                    write_response(
                        &mut stream,
                        request.response_protocol(),
                        500,
                        "Internal Server Error",
                        "text/xml; charset=utf-8",
                        body.as_bytes(),
                        None,
                        &["EXT:"],
                    )
                    .await
                }
            }
        }
        "SUBSCRIBE" => {
            let sid = format!("uuid:{}", Uuid::new_v4());
            let sid_header = format!("SID: {sid}");
            write_response(
                &mut stream,
                request.response_protocol(),
                200,
                "OK",
                "text/plain",
                b"",
                None,
                &[&sid_header, "TIMEOUT: Second-120"],
            )
            .await
        }
        "UNSUBSCRIBE" => {
            write_response(
                &mut stream,
                request.response_protocol(),
                200,
                "OK",
                "text/plain",
                b"",
                None,
                &[],
            )
            .await
        }
        _ => {
            write_response(
                &mut stream,
                request.response_protocol(),
                405,
                "Method Not Allowed",
                "text/plain; charset=utf-8",
                b"method not allowed",
                None,
                &["Allow: GET, HEAD, POST, SUBSCRIBE, UNSUBSCRIBE, SETUP, RESTORE, STARTRESTORE"],
            )
            .await
        }
    }
}

async fn handle_upgrade(
    receiver: Receiver,
    mut stream: TcpStream,
    peer: SocketAddr,
    request: InitialRequest,
) -> Result<()> {
    if request.path() != "/projection" {
        bail!("NVA setup path must be /projection");
    }
    let session_id = safe_identifier(
        request.header("session").unwrap_or_default(),
        Uuid::new_v4().to_string(),
    );
    let client_id = safe_identifier(
        request.header("uuid").unwrap_or_default(),
        peer.ip().to_string(),
    );
    let restore = !request.method.eq_ignore_ascii_case("SETUP");
    let response_protocol = if request.protocol.eq_ignore_ascii_case("NVA/1.0") {
        "NVA/1.0"
    } else {
        "HTTP/1.0"
    };
    let header = format!(
        "{response_protocol} 200 OK\r\nNvaVersion: 1\r\nSession: {session_id}\r\n\
Connection: Keep-Alive\r\nUUID: {}\r\nDate: {}\r\nContent-Length: 0\r\n\
Server: {NVA_SERVER}\r\n\r\n",
        upnp::nva_tv_id(receiver.state.nva_device_uuid()),
        Utc::now().format("%a, %d %b %Y %H:%M:%S GMT")
    );
    time::timeout(WRITE_TIMEOUT, stream.write_all(header.as_bytes())).await??;
    info!(%peer, session = short(&session_id), client = short(&client_id), method = request.method, "NVA control session connected");
    session_loop(receiver, stream, session_id, request.leftover, restore).await
}

async fn session_loop(
    receiver: Receiver,
    stream: TcpStream,
    session_id: String,
    initial: Vec<u8>,
    restore: bool,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let mut decoder = Decoder::default();
    let mut events = receiver.state.subscribe_nva();
    let mut ping = time::interval(Duration::from_secs(1));
    ping.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    ping.tick().await;
    let mut sequence = 0_u32;
    let mut buffer = [0_u8; 8 * 1024];
    let (command_sender, mut command_receiver) = mpsc::channel::<InboundCommand>(16);
    let worker_receiver = receiver.clone();
    let worker_session = session_id.clone();
    let mut worker = tokio::spawn(async move {
        while let Some(command) = command_receiver.recv().await {
            let play_epoch = command.play_epoch;
            let mut result = worker_receiver
                .handle_command(&worker_session, &command.method, command.params, play_epoch)
                .await;
            if let Some(play_epoch) = play_epoch {
                if let Err(superseded) = worker_receiver.state.ensure_play_epoch(play_epoch) {
                    // A newer Play or an already-queued Stop always wins over
                    // an error returned by the stale resolver/SOAP request.
                    result = Err(superseded);
                }
                worker_receiver.state.complete_play_epoch(play_epoch).await;
            }
            if let Err(error) = result {
                let superseded = error.downcast_ref::<SupersededPlay>().is_some();
                warn!(
                    session = short(&worker_session),
                    method = command.method,
                    %error,
                    "NVA command failed"
                );
                if command_failure_requires_cleanup(&command.method, superseded) {
                    let owns_active_cast = worker_receiver
                        .state
                        .session()
                        .await
                        .is_some_and(|session| session.id == worker_session);
                    if owns_active_cast
                        && let Err(cleanup_error) =
                            dlna::stop(worker_receiver.state.clone(), Some(&worker_session)).await
                    {
                        warn!(
                            session = short(&worker_session),
                            %cleanup_error,
                            "failed to acknowledge DLNA Stop while cleaning up a failed NVA Play"
                        );
                    }
                    if !worker_receiver
                        .state
                        .session_was_terminated(&worker_session)
                        .await
                    {
                        worker_receiver
                            .state
                            .mark_session_terminated(&worker_session)
                            .await;
                        worker_receiver.state.emit_nva(NvaEvent {
                            session_id: worker_session.clone(),
                            method: "OnPlayState".into(),
                            params: Some(json!({"playState": 7})),
                            close_after: true,
                        });
                    }
                }
            }
        }
    });

    let outcome: Result<bool> = async {
        if restore {
            for event in receiver.restore_events(&session_id).await {
                sequence = next_sequence(sequence);
                let close_after = event.close_after;
                write_event(&mut writer, sequence, &event).await?;
                if close_after {
                    receiver.clear_active_request_if(&session_id).await;
                    return Ok(true);
                }
            }
        }
        if !initial.is_empty() {
            process_frames(
                &receiver.state,
                &session_id,
                &mut writer,
                &command_sender,
                decoder.push(&initial),
            )
            .await?;
        }
        loop {
            tokio::select! {
                read = reader.read(&mut buffer) => match read {
                    Ok(0) => return Ok(false),
                    Ok(length) => {
                        process_frames(
                            &receiver.state,
                            &session_id,
                            &mut writer,
                            &command_sender,
                            decoder.push(&buffer[..length]),
                        ).await?;
                    }
                    Err(error) => return Err(error.into()),
                },
                _ = ping.tick() => {
                    sequence = next_sequence(sequence);
                    writer.write_all(&frame::encode_ping(sequence)).await?;
                }
                event = events.recv() => match event {
                    Ok(event) if event.session_id == session_id => {
                        sequence = next_sequence(sequence);
                        write_event(&mut writer, sequence, &event).await?;
                        if event.close_after {
                            receiver.clear_active_request_if(&session_id).await;
                            return Ok(true);
                        }
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(false),
                }
            }
        }
    }
    .await;

    let terminal_close = outcome.as_ref().is_ok_and(|close| *close);
    drop(command_sender);
    if terminal_close {
        worker.abort();
        let _ = worker.await;
    } else {
        let cancelled_play = receiver
            .state
            .cancel_pending_play_for(Some(&session_id))
            .await;
        let worker_timed_out = time::timeout(WORKER_SHUTDOWN_GRACE, &mut worker)
            .await
            .is_err();
        if worker_timed_out {
            warn!(
                session = short(&session_id),
                "NVA command worker did not stop after its client disconnected"
            );
            worker.abort();
            let _ = worker.await;
        }
        let abandoned_connect = receiver
            .state
            .session()
            .await
            .is_some_and(|session| session.id == session_id && session.phase == "connecting");
        if cancelled_play && (worker_timed_out || abandoned_connect) {
            if let Err(error) = dlna::stop(receiver.state.clone(), Some(&session_id)).await {
                warn!(
                    session = short(&session_id),
                    %error,
                    "failed to acknowledge DLNA Stop while cleaning up an abandoned Play"
                );
            }
            receiver.clear_active_request_if(&session_id).await;
        }
    }
    info!(
        session = short(&session_id),
        "NVA control session disconnected"
    );
    outcome.map(|_| ())
}

async fn process_frames(
    state: &AppState,
    session_id: &str,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    sender: &mpsc::Sender<InboundCommand>,
    frames: Vec<Result<Frame, frame::DecodeError>>,
) -> Result<()> {
    for decoded in frames {
        match decoded {
            Ok(Frame::Command(command)) => {
                let params = command
                    .json
                    .as_deref()
                    .map(serde_json::from_str::<Value>)
                    .transpose()
                    .context("NVA command contains invalid JSON")?;
                if command.method == "GetVolume" {
                    let body = serde_json::to_string(&json!({"volume": 50}))?;
                    writer
                        .write_all(&frame::encode_reply(command.sequence, Some(&body))?)
                        .await?;
                } else {
                    writer
                        .write_all(&frame::encode_reply(command.sequence, None)?)
                        .await?;
                    let play_epoch = if matches!(command.method.as_str(), "Play" | "PlayUrl") {
                        Some(state.begin_play_epoch(session_id).await)
                    } else {
                        if command.method == "Stop" {
                            state.cancel_pending_play_for(Some(session_id)).await;
                        }
                        None
                    };
                    sender
                        .send(InboundCommand {
                            method: command.method,
                            params,
                            play_epoch,
                        })
                        .await
                        .map_err(|_| anyhow!("NVA command worker stopped"))?;
                }
            }
            Ok(Frame::Reply(_)) | Ok(Frame::Ping(_)) => {}
            Err(error) => debug!(%error, "discarded malformed NVA frame"),
        }
    }
    Ok(())
}

async fn write_event(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    sequence: u32,
    event: &NvaEvent,
) -> Result<()> {
    let json = event
        .params
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    writer
        .write_all(&frame::encode_command(
            sequence,
            &event.method,
            json.as_deref(),
        )?)
        .await?;
    Ok(())
}

impl Receiver {
    async fn handle_command(
        &self,
        session_id: &str,
        method: &str,
        params: Option<Value>,
        play_epoch: Option<u64>,
    ) -> Result<()> {
        let params = params.unwrap_or_else(|| json!({}));
        match method {
            "Play" => {
                let play_epoch = play_epoch.context("Play has no generation")?;
                if self.state.session_was_terminated(session_id).await {
                    bail!("NVA session has already ended");
                }
                self.state.ensure_play_epoch(play_epoch)?;
                self.broadcast_play_state(session_id, 3, false);
                let danmaku_enabled = requested_danmaku_enabled(&params).unwrap_or(true);
                let initial_speed = requested_speed(&params).unwrap_or(1.0);
                let request = PlayRequest::from_value(&params)?;
                let resolved = self.resolver.resolve_play(&request).await;
                self.state.ensure_play_epoch(play_epoch)?;
                let media = resolved?;
                let live = media.live;
                let quality = media.quality.clone();
                let qualities = media.available_qualities.clone();
                dlna::play(
                    self.state.clone(),
                    session_id,
                    SessionOrigin::Nva,
                    media,
                    request.seek_position_ms,
                    play_epoch,
                )
                .await?;
                self.state.ensure_play_epoch(play_epoch)?;
                self.complete_successful_play(
                    session_id,
                    request,
                    quality,
                    qualities,
                    danmaku_enabled,
                    initial_speed,
                    live,
                )
                .await;
                Ok(())
            }
            "PlayUrl" => {
                let play_epoch = play_epoch.context("PlayUrl has no generation")?;
                if self.state.session_was_terminated(session_id).await {
                    bail!("NVA session has already ended");
                }
                self.state.ensure_play_epoch(play_epoch)?;
                self.broadcast_play_state(session_id, 3, false);
                let danmaku_enabled = requested_danmaku_enabled(&params).unwrap_or(true);
                let initial_speed = requested_speed(&params).unwrap_or(1.0);
                let resolved = self.resolver.resolve_play_url(&params).await;
                self.state.ensure_play_epoch(play_epoch)?;
                let (request, media) = resolved?;
                let live = media.live;
                let quality = media.quality.clone();
                let qualities = media.available_qualities.clone();
                dlna::play(
                    self.state.clone(),
                    session_id,
                    SessionOrigin::Nva,
                    media,
                    request.seek_position_ms,
                    play_epoch,
                )
                .await?;
                self.state.ensure_play_epoch(play_epoch)?;
                self.complete_successful_play(
                    session_id,
                    request,
                    quality,
                    qualities,
                    danmaku_enabled,
                    initial_speed,
                    live,
                )
                .await;
                Ok(())
            }
            "Pause" => dlna::pause(self.state.clone(), session_id).await,
            "Resume" => dlna::resume(self.state.clone(), session_id).await,
            "Stop" => {
                dlna::stop(self.state.clone(), Some(session_id)).await?;
                self.clear_active_request_if(session_id).await;
                Ok(())
            }
            "Seek" => {
                let object = params
                    .as_object()
                    .ok_or_else(|| anyhow!("Seek parameters must be an object"))?;
                let value = object
                    .get("seekTs")
                    .or_else(|| object.get("seek_ts"))
                    .or_else(|| object.get("position"));
                let position = crate::bilibili::nva_seek_position_ms(value)?;
                dlna::seek(self.state.clone(), session_id, position).await
            }
            "SetVolume" => {
                let volume = params
                    .get("volume")
                    .and_then(|value| {
                        value
                            .as_u64()
                            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
                    })
                    .unwrap_or(50)
                    .min(100) as u8;
                dlna::set_volume(self.state.clone(), session_id, volume).await
            }
            "SwitchQn" => self.switch_quality(session_id, &params).await,
            method if is_speed_command(method) => self.switch_speed(session_id, &params).await,
            "SwitchDanmaku" => self.switch_danmaku(session_id, &params).await,
            "SendDanmaku" | "RequestDanmaku" | "AppendDanmaku" => {
                debug!(
                    method,
                    "acknowledged NVA danmaku payload without rendering it"
                );
                Ok(())
            }
            "Heartbeat" | "SwitchEpisode" => Ok(()),
            _ => {
                debug!(method, "acknowledged unknown NVA command");
                Ok(())
            }
        }
    }

    async fn switch_quality(&self, session_id: &str, params: &Value) -> Result<()> {
        if self.state.session_was_terminated(session_id).await {
            bail!("NVA session has already ended");
        }
        let quality = params
            .get("qn")
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
            })
            .ok_or_else(|| anyhow!("SwitchQn has no quality"))?;
        let (mut request, danmaku_enabled) = {
            let current = self.active_request.lock().await;
            let active = current
                .as_ref()
                .filter(|active| active.session_id == session_id)
                .ok_or_else(|| anyhow!("there is no resolvable active NVA media"))?;
            (active.request.clone(), active.danmaku_enabled)
        };
        request.desired_quality = quality;
        let (previous_rate, previous_live) = session_rate(&self.state, session_id).await;
        let previous_paused = self
            .state
            .session()
            .await
            .is_some_and(|session| session.id == session_id && session.phase == "paused");
        let previous_position_ms = if previous_live {
            0
        } else {
            dlna::position(self.state.clone(), session_id)
                .await
                .map(|(position, _)| position)
                .unwrap_or(0)
        };

        let play_epoch = self.state.begin_play_epoch(session_id).await;
        let result = async {
            let resolved = self.resolver.resolve_play(&request).await;
            self.state.ensure_play_epoch(play_epoch)?;
            let media = resolved?;
            let selected = media.quality.clone();
            let qualities = media.available_qualities.clone();
            self.broadcast_play_state(session_id, 3, false);
            if let Err(error) = dlna::play(
                self.state.clone(),
                session_id,
                SessionOrigin::Nva,
                media,
                previous_position_ms,
                play_epoch,
            )
            .await
            {
                if error.downcast_ref::<SupersededPlay>().is_none()
                    && let Err(cleanup_error) =
                        dlna::stop(self.state.clone(), Some(session_id)).await
                {
                    warn!(
                        session = short(session_id),
                        %cleanup_error,
                        "failed to acknowledge DLNA Stop after a quality-switch failure"
                    );
                }
                return Err(error);
            }
            self.state.ensure_play_epoch(play_epoch)?;
            *self.active_request.lock().await = Some(ActiveRequest {
                session_id: session_id.to_owned(),
                request: request.clone(),
                available_qualities: qualities.clone(),
                danmaku_enabled,
            });
            // A fresh play restarts the renderer at 1x, so the rate the phone was
            // watching has to be put back on top of the new stream.
            if previous_paused && !previous_live {
                if let Err(error) = dlna::pause(self.state.clone(), session_id).await {
                    debug!(%error, "renderer would not restore the paused state");
                }
            } else if previous_rate != 1.0 && !previous_live {
                let speed = speed_argument(previous_rate);
                if let Err(error) = dlna::set_speed(self.state.clone(), session_id, &speed).await {
                    debug!(%error, %speed, "renderer would not take back the current rate");
                }
            }
            if !previous_live {
                let title = self
                    .state
                    .session()
                    .await
                    .filter(|session| session.id == session_id)
                    .map_or_else(String::new, |session| session.title);
                self.broadcast_control_state(
                    session_id,
                    danmaku_enabled,
                    &request,
                    &title,
                    &selected,
                    &qualities,
                )
                .await;
            }
            Ok(())
        }
        .await;
        self.state.complete_play_epoch(play_epoch).await;
        result
    }

    /// A rate change is AVTransport `Play` with a different `Speed`, so a live stream
    /// has nothing to change and is acknowledged without touching playback.
    async fn switch_speed(&self, session_id: &str, params: &Value) -> Result<()> {
        let Some(rate) = requested_speed(params) else {
            debug!(method = "SwitchSpeed", "SwitchSpeed carried no usable rate");
            return Ok(());
        };
        let (_, live) = session_rate(&self.state, session_id).await;
        if live {
            debug!(rate, "ignored SwitchSpeed for a live stream");
            return Ok(());
        }
        let speed = speed_argument(rate);
        let result = if dlna::target_accepts_speed(&self.state, &speed).await {
            dlna::set_speed(self.state.clone(), session_id, &speed).await
        } else {
            debug!(speed, "target does not advertise this rate");
            Ok(())
        };
        // Announce after the attempt either way: the phone builds its speed menu from
        // this message, and silence is what leaves it showing a rate that is not playing.
        self.broadcast_speed(session_id).await;
        result
    }

    async fn switch_danmaku(&self, session_id: &str, params: &Value) -> Result<()> {
        let Some(enabled) = requested_danmaku_enabled(params) else {
            debug!(
                method = "SwitchDanmaku",
                "SwitchDanmaku carried no usable state"
            );
            return Ok(());
        };
        {
            let mut current = self.active_request.lock().await;
            let active = current
                .as_mut()
                .filter(|active| active.session_id == session_id)
                .ok_or_else(|| anyhow!("there is no active NVA media"))?;
            active.danmaku_enabled = enabled;
        }
        self.state.emit_nva(danmaku_event(session_id, enabled));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_successful_play(
        &self,
        session_id: &str,
        request: PlayRequest,
        selected_quality: String,
        available_qualities: Vec<u64>,
        danmaku_enabled: bool,
        initial_speed: f64,
        live: bool,
    ) {
        *self.active_request.lock().await = Some(ActiveRequest {
            session_id: session_id.to_owned(),
            request: request.clone(),
            available_qualities: available_qualities.clone(),
            danmaku_enabled,
        });

        // A DLNA target can apply the sender's initial rate only through another
        // AVTransport Play. Treat that as best effort: the media is already playing,
        // and a target that did not advertise the rate must not make Play fail.
        if !live && initial_speed != 1.0 {
            let speed = speed_argument(initial_speed);
            if dlna::target_accepts_speed(&self.state, &speed).await {
                if let Err(error) = dlna::set_speed(self.state.clone(), session_id, &speed).await {
                    debug!(%error, %speed, "renderer rejected the initial NVA rate");
                }
            } else {
                debug!(%speed, "target does not advertise the initial NVA rate");
            }
        }

        if !live {
            let title = self
                .state
                .session()
                .await
                .filter(|session| session.id == session_id)
                .map_or_else(String::new, |session| session.title);
            self.broadcast_control_state(
                session_id,
                danmaku_enabled,
                &request,
                &title,
                &selected_quality,
                &available_qualities,
            )
            .await;
        }
    }

    /// These four messages are a capability handshake for Android senders.  Keep
    /// them adjacent and in UDashboard's order: interleaving an awaited state read
    /// after the first event can make a phone decide the later controls are absent.
    async fn broadcast_control_state(
        &self,
        session_id: &str,
        danmaku_enabled: bool,
        request: &PlayRequest,
        title: &str,
        selected_quality: &str,
        available_qualities: &[u64],
    ) {
        let events = control_state_events(
            &self.state,
            session_id,
            danmaku_enabled,
            request,
            title,
            selected_quality,
            available_qualities,
        )
        .await;
        for event in events {
            self.state.emit_nva(event);
        }
    }

    async fn restore_events(&self, session_id: &str) -> Vec<NvaEvent> {
        let Some(session) = self
            .state
            .session()
            .await
            .filter(|session| session.id == session_id)
        else {
            return vec![play_state_event(session_id, 7, true)];
        };
        let active = self
            .active_request
            .lock()
            .await
            .as_ref()
            .filter(|active| active.session_id == session_id)
            .cloned();
        let danmaku_enabled = active.as_ref().is_none_or(|active| active.danmaku_enabled);
        let available_qualities = active
            .as_ref()
            .map_or(&[][..], |active| active.available_qualities.as_slice());
        let mut events = vec![play_state_event(
            session_id,
            play_state_for_phase(&session.phase),
            false,
        )];
        if session.live {
            return events;
        }
        let default_request = PlayRequest::default();
        let request = active
            .as_ref()
            .map_or(&default_request, |active| &active.request);
        events.extend(
            control_state_events(
                &self.state,
                session_id,
                danmaku_enabled,
                request,
                &session.title,
                &session.quality,
                available_qualities,
            )
            .await,
        );
        events
    }

    async fn broadcast_speed(&self, session_id: &str) {
        let event = speed_event(&self.state, session_id).await;
        self.state.emit_nva(event);
    }

    fn broadcast_play_state(&self, session_id: &str, play_state: u8, close_after: bool) {
        self.state.emit_nva(NvaEvent {
            session_id: session_id.to_owned(),
            method: "OnPlayState".into(),
            params: Some(json!({"playState": play_state})),
            close_after,
        });
    }

    async fn clear_active_request_if(&self, session_id: &str) {
        let mut active = self.active_request.lock().await;
        if active
            .as_ref()
            .is_some_and(|active| active.session_id == session_id)
        {
            *active = None;
        }
    }
}

fn play_state_event(session_id: &str, play_state: u8, close_after: bool) -> NvaEvent {
    NvaEvent {
        session_id: session_id.to_owned(),
        method: "OnPlayState".into(),
        params: Some(json!({"playState": play_state})),
        close_after,
    }
}

fn play_state_for_phase(phase: &str) -> u8 {
    match phase {
        "playing" => 4,
        "paused" => 5,
        "error" | "stopped" => 7,
        _ => 3,
    }
}

fn danmaku_event(session_id: &str, enabled: bool) -> NvaEvent {
    NvaEvent {
        session_id: session_id.to_owned(),
        method: "OnDanmakuSwitch".into(),
        params: Some(json!({"open": enabled})),
        close_after: false,
    }
}

fn quality_payload(selected: &str, qualities: &[u64]) -> Value {
    let current = selected.parse::<u64>().unwrap_or(0);
    let mut qualities = qualities
        .iter()
        .copied()
        .filter(|quality| *quality != 0)
        .collect::<Vec<_>>();
    if current != 0 && !qualities.contains(&current) {
        qualities.push(current);
    }
    // Direct PlayUrl media has no Bilibili quality catalog. UDashboard still
    // publishes one qn=0 option so Android clients keep a valid source-quality
    // selection instead of treating an empty menu as an unsupported control.
    if qualities.is_empty() {
        qualities.push(current);
    }
    qualities.dedup();
    let options = qualities
        .into_iter()
        .map(|quality| {
            let description = quality_description(quality);
            json!({
                "description": description,
                "displayDesc": description,
                "needLogin": false,
                "needVip": false,
                "quality": quality,
                "superscript": "NVA2DLNA"
            })
        })
        .collect::<Vec<_>>();
    json!({
        "curQn": current,
        "supportQnList": options,
        "userDesireQn": current
    })
}

fn episode_event(
    session_id: &str,
    request: &PlayRequest,
    title: &str,
    selected: &str,
    qualities: &[u64],
) -> NvaEvent {
    NvaEvent {
        session_id: session_id.to_owned(),
        method: "OnEpisodeSwitch".into(),
        params: Some(json!({
            "playItem": {
                "aid": request.aid,
                "cid": request.cid,
                "contentType": request.content_type,
                "epId": request.episode_id,
                "seasonId": request.season_id,
            },
            "qnDesc": quality_payload(selected, qualities),
            "title": title,
        })),
        close_after: false,
    }
}

fn quality_event(session_id: &str, selected: &str, qualities: &[u64]) -> NvaEvent {
    NvaEvent {
        session_id: session_id.to_owned(),
        method: "OnQnSwitch".into(),
        params: Some(quality_payload(selected, qualities)),
        close_after: false,
    }
}

async fn control_state_events(
    state: &AppState,
    session_id: &str,
    danmaku_enabled: bool,
    request: &PlayRequest,
    title: &str,
    selected_quality: &str,
    available_qualities: &[u64],
) -> Vec<NvaEvent> {
    // Resolve the applied speed before emitting anything so the four capability
    // announcements remain adjacent on the broadcast channel.
    let speed = speed_event(state, session_id).await;
    vec![
        danmaku_event(session_id, danmaku_enabled),
        episode_event(
            session_id,
            request,
            title,
            selected_quality,
            available_qualities,
        ),
        quality_event(session_id, selected_quality, available_qualities),
        speed,
    ]
}

fn command_failure_is_terminal(method: &str) -> bool {
    matches!(method, "Play" | "PlayUrl")
}

fn command_failure_requires_cleanup(method: &str, superseded: bool) -> bool {
    command_failure_is_terminal(method) && !superseded
}

/// The rates a phone puts in its speed menu once a device announces them. The list
/// is what the menu is built from, which is why [`SPEED_EVENT`] has to be sent even
/// when the current rate is the obvious 1x.
const SPEED_MENU: [f64; 6] = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0];
const SPEED_EVENT: &str = "SpeedChanged";
/// Widest and narrowest multiplier a sender may ask for. Outside this the phone has
/// no UI for it and most renderers reject the value.
const MIN_SPEED: f64 = 0.25;
const MAX_SPEED: f64 = 4.0;

fn flexible_bool(value: &Value) -> Option<bool> {
    value.as_bool().or_else(|| {
        value.as_str().and_then(|value| {
            if value.eq_ignore_ascii_case("true") || value == "1" {
                Some(true)
            } else if value.eq_ignore_ascii_case("false") || value == "0" {
                Some(false)
            } else {
                None
            }
        })
    })
}

/// Initial Play and later SwitchDanmaku commands use different names for the
/// same persisted preference across Bilibili Android releases.
fn requested_danmaku_enabled(params: &Value) -> Option<bool> {
    [
        "open",
        "enabled",
        "danmakuOpen",
        "danmaku_open",
        "danmakuSwitch",
        "danmaku_switch",
        "danmakuSwitchSave",
        "danmaku_switch_save",
    ]
    .iter()
    .find_map(|key| params.get(*key))
    .and_then(flexible_bool)
}

/// Bilibili clients disagree on the key they put the rate under, and the value is a
/// plain multiplier rather than an index into the announced menu.
fn requested_speed(params: &Value) -> Option<f64> {
    [
        "speed",
        "currSpeed",
        "curr_speed",
        "userDesireSpeed",
        "user_desire_speed",
        "desireSpeed",
        "desire_speed",
        "rate",
        "playSpeed",
        "value",
    ]
    .iter()
    .find_map(|key| params.get(*key))
    .and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
    })
    .filter(|rate| rate.is_finite() && *rate > 0.0)
    .map(|rate| rate.clamp(MIN_SPEED, MAX_SPEED))
}

fn is_speed_command(method: &str) -> bool {
    matches!(method, "SwitchSpeed" | "PlaySpeed")
}

/// The AVTransport `Speed` argument for a multiplier. Whole numbers go out bare so
/// they match the trick-play values renderers advertise, and everything is rounded
/// to two decimals because that is the shape of the advertised `allowedValueList`.
fn speed_argument(rate: f64) -> String {
    let rate = (rate * 100.0).round() / 100.0;
    if rate == rate.floor() {
        format!("{}", rate as u64)
    } else {
        format!("{rate}")
    }
}

/// The rate a session is holding and whether its media can hold one at all: a live
/// stream has no timeline to speed up.
async fn session_rate(state: &AppState, session_id: &str) -> (f64, bool) {
    let session = state
        .session()
        .await
        .filter(|session| session.id == session_id);
    let live = session.as_ref().is_some_and(|session| session.live);
    let rate = session
        .and_then(|session| session.speed.parse::<f64>().ok())
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .unwrap_or(1.0);
    (rate, live)
}

/// Reports the rate the bridge actually applied. Reading it back instead of echoing
/// the request is what keeps a renderer that rejected the value from leaving the
/// phone showing a speed that is not happening.
async fn speed_event(state: &AppState, session_id: &str) -> NvaEvent {
    let (rate, _) = session_rate(state, session_id).await;
    NvaEvent {
        session_id: session_id.to_owned(),
        method: SPEED_EVENT.into(),
        params: Some(json!({
            "currSpeed": rate,
            "supportSpeedList": &SPEED_MENU[..],
        })),
        close_after: false,
    }
}

fn quality_description(quality: u64) -> String {
    match quality {
        0 => "源画质".into(),
        6 => "极速 240P".into(),
        16 => "流畅 360P".into(),
        32 => "清晰 480P".into(),
        64 => "高清 720P".into(),
        74 => "高清 720P60".into(),
        77 | 80 => "高清 1080P".into(),
        102 | 112 => "高清 1080P+".into(),
        116 => "高清 1080P60".into(),
        120 | 121 => "超清 4K".into(),
        125 => "HDR 真彩色".into(),
        126 => "杜比视界".into(),
        127 => "超高清 8K".into(),
        value => format!("清晰度 {value}"),
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<InitialRequest> {
    let mut bytes = Vec::with_capacity(4096);
    let deadline = time::Instant::now() + READ_TIMEOUT;
    let header_end = loop {
        if bytes.len() > MAX_HEADERS {
            bail!("request headers are too large");
        }
        if let Some(index) = find_bytes(&bytes, b"\r\n\r\n") {
            break index + 4;
        }
        let mut buffer = [0_u8; 4096];
        let length = time::timeout_at(deadline, stream.read(&mut buffer)).await??;
        if length == 0 {
            bail!("client closed before headers completed");
        }
        bytes.extend_from_slice(&buffer[..length]);
    };
    let header_text = std::str::from_utf8(&bytes[..header_end])?;
    let mut lines = header_text.split("\r\n");
    let mut request_line = lines
        .next()
        .context("request line is missing")?
        .split_whitespace();
    let method = request_line
        .next()
        .context("method is missing")?
        .to_uppercase();
    let path = request_line.next().context("path is missing")?.to_owned();
    let protocol = request_line
        .next()
        .context("protocol is missing")?
        .to_owned();
    if !matches!(protocol.as_str(), "HTTP/1.0" | "HTTP/1.1" | "NVA/1.0") {
        bail!("unsupported request protocol");
    }
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').context("malformed request header")?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    let content_length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    if content_length > MAX_BODY {
        bail!("request body is too large");
    }
    while bytes.len() < header_end + content_length {
        let mut buffer = [0_u8; 8192];
        let length = time::timeout_at(deadline, stream.read(&mut buffer)).await??;
        if length == 0 {
            bail!("client closed before body completed");
        }
        bytes.extend_from_slice(&buffer[..length]);
    }
    let body_end = header_end + content_length;
    Ok(InitialRequest {
        method,
        path,
        protocol,
        headers,
        body: bytes[header_end..body_end].to_vec(),
        leftover: bytes[body_end..].to_vec(),
    })
}

// These are the independent fields of an HTTP/NVA wire response; keeping
// them explicit makes the unusual HEAD compatibility length auditable.
#[allow(clippy::too_many_arguments)]
async fn write_response(
    stream: &mut TcpStream,
    protocol: &str,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
    declared_length: Option<usize>,
    extra: &[&str],
) -> Result<()> {
    let mut header = format!(
        "{protocol} {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
Date: {}\r\nServer: {NVA_HTTP_SERVER}\r\nConnection: close\r\n",
        declared_length.unwrap_or(body.len()),
        Utc::now().format("%a, %d %b %Y %H:%M:%S GMT")
    );
    for line in extra {
        header.push_str(line);
        header.push_str("\r\n");
    }
    header.push_str("\r\n");
    time::timeout(WRITE_TIMEOUT, async {
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.flush().await
    })
    .await??;
    Ok(())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn safe_identifier(value: &str, fallback: String) -> String {
    let value = value.trim();
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        value.to_owned()
    } else {
        fallback
    }
}

fn next_sequence(value: u32) -> u32 {
    value.wrapping_add(1).max(1)
}

fn short(value: &str) -> String {
    value.chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_reject_header_injection() {
        assert_eq!(safe_identifier("ok-123", "fallback".into()), "ok-123");
        assert_eq!(
            safe_identifier("bad\r\nX: y", "fallback".into()),
            "fallback"
        );
    }

    #[test]
    fn restore_maps_bridge_phases_to_nva_play_states() {
        assert_eq!(play_state_for_phase("connecting"), 3);
        assert_eq!(play_state_for_phase("playing"), 4);
        assert_eq!(play_state_for_phase("paused"), 5);
        assert_eq!(play_state_for_phase("error"), 7);
    }

    #[test]
    fn setup_uses_the_legacy_tv_server_fingerprint() {
        assert_eq!(NVA_SERVER, "Linux/3.0.0, UPnP/1.0, Platinum/1.0.5.13");
    }

    #[test]
    fn only_initial_play_failures_terminate_the_nva_session() {
        assert!(command_failure_is_terminal("Play"));
        assert!(command_failure_is_terminal("PlayUrl"));
        for method in ["Pause", "Seek", "SetVolume", "SwitchQn"] {
            assert!(!command_failure_is_terminal(method));
        }
    }

    #[test]
    fn a_superseded_play_never_closes_before_the_queued_command_runs() {
        assert!(!command_failure_requires_cleanup("Play", true));
        assert!(!command_failure_requires_cleanup("PlayUrl", true));
        assert!(command_failure_requires_cleanup("Play", false));
    }

    #[test]
    fn every_client_spelling_of_a_rate_is_understood() {
        for key in [
            "speed",
            "currSpeed",
            "userDesireSpeed",
            "desire_speed",
            "value",
        ] {
            let params = json!({(key): 1.5});
            assert_eq!(requested_speed(&params), Some(1.5), "{key}");
        }
        assert_eq!(requested_speed(&json!({"rate": " 1.50 "})), Some(1.5));
        assert_eq!(requested_speed(&json!({"speed": 99})), Some(4.0));
        assert_eq!(requested_speed(&json!({"speed": 0.1})), Some(0.25));
        for params in [
            json!({}),
            json!({"speed": 0}),
            json!({"speed": -1}),
            json!({"speed": "max"}),
            json!({"quality": 80}),
        ] {
            assert_eq!(requested_speed(&params), None, "{params}");
        }
    }

    #[test]
    fn play_speed_is_a_switch_speed_command_alias() {
        assert!(is_speed_command("SwitchSpeed"));
        assert!(is_speed_command("PlaySpeed"));
        assert!(!is_speed_command("SpeedChanged"));
    }

    #[test]
    fn play_and_switch_danmaku_aliases_are_understood() {
        for params in [
            json!({"open": false}),
            json!({"enabled": "false"}),
            json!({"danmakuOpen": false}),
            json!({"danmaku_switch": "0"}),
            json!({"danmakuSwitchSave": "false"}),
            json!({"danmaku_switch_save": false}),
        ] {
            assert_eq!(requested_danmaku_enabled(&params), Some(false), "{params}");
        }
        assert_eq!(
            requested_danmaku_enabled(&json!({"open": "true"})),
            Some(true)
        );
        assert_eq!(requested_danmaku_enabled(&json!({"open": "invalid"})), None);
        assert!(requested_danmaku_enabled(&json!({})).unwrap_or(true));
    }

    #[test]
    fn rates_are_normalised_to_the_shortest_form_avtransport_accepts() {
        assert_eq!(speed_argument(2.0), "2");
        assert_eq!(speed_argument(1.0), "1");
        assert_eq!(speed_argument(1.50), "1.5");
        assert_eq!(speed_argument(1.25), "1.25");
        assert_eq!(speed_argument(1.3333), "1.33");
        assert_eq!(speed_argument(0.25), "0.25");
    }

    #[tokio::test]
    async fn the_speed_announcement_repeats_the_menu_and_the_applied_rate() {
        let state = speed_test_state().await;
        let params = speed_event(&state, "s-1")
            .await
            .params
            .expect("SpeedChanged carries parameters");
        assert_eq!(params["currSpeed"], json!(1.25));
        assert_eq!(
            params["supportSpeedList"],
            json!([0.5, 0.75, 1.0, 1.25, 1.5, 2.0])
        );
        // An unknown session is reported at neutral speed instead of not at all: the
        // menu the phone builds comes from this message and nothing else.
        assert_eq!(session_rate(&state, "another").await, (1.0, false));
    }

    #[tokio::test]
    async fn successful_play_controls_follow_udashboard_order() {
        let state = speed_test_state().await;
        let request = PlayRequest {
            aid: "42".into(),
            cid: "99".into(),
            episode_id: "7".into(),
            season_id: "6".into(),
            content_type: 2,
            ..PlayRequest::default()
        };
        let events =
            control_state_events(&state, "s-1", false, &request, "正片", "120", &[120, 80]).await;
        assert_eq!(
            events
                .iter()
                .map(|event| event.method.as_str())
                .collect::<Vec<_>>(),
            [
                "OnDanmakuSwitch",
                "OnEpisodeSwitch",
                "OnQnSwitch",
                "SpeedChanged"
            ]
        );
        assert_eq!(events[0].params.as_ref().unwrap()["open"], false);
        assert_eq!(events[1].params.as_ref().unwrap()["playItem"]["aid"], "42");
        assert_eq!(
            events[1].params.as_ref().unwrap()["playItem"]["seasonId"],
            "6"
        );
        assert_eq!(
            events[1].params.as_ref().unwrap()["playItem"]["contentType"],
            2
        );
        assert_eq!(events[2].params.as_ref().unwrap()["curQn"], 120);
        assert_eq!(events[3].params.as_ref().unwrap()["currSpeed"], 1.25);
    }

    #[tokio::test]
    async fn direct_play_url_advertises_source_quality_instead_of_an_empty_menu() {
        let state = speed_test_state().await;
        let events = control_state_events(
            &state,
            "s-1",
            true,
            &PlayRequest::default(),
            "直链",
            "source",
            &[],
        )
        .await;
        let quality = events[2].params.as_ref().unwrap();
        assert_eq!(quality["curQn"], 0);
        assert_eq!(quality["supportQnList"][0]["quality"], 0);
        assert_eq!(quality["supportQnList"][0]["displayDesc"], "源画质");
    }

    #[tokio::test]
    async fn restore_repeats_play_and_all_phone_control_state() {
        let state = speed_test_state().await;
        let receiver = test_receiver(state, false);
        let events = receiver.restore_events("s-1").await;
        assert_eq!(
            events
                .iter()
                .map(|event| event.method.as_str())
                .collect::<Vec<_>>(),
            [
                "OnPlayState",
                "OnDanmakuSwitch",
                "OnEpisodeSwitch",
                "OnQnSwitch",
                "SpeedChanged"
            ]
        );
        assert_eq!(events[0].params.as_ref().unwrap()["playState"], 4);
        assert_eq!(events[1].params.as_ref().unwrap()["open"], false);
        assert_eq!(events[2].params.as_ref().unwrap()["title"], "正片");
        assert_eq!(events[3].params.as_ref().unwrap()["curQn"], 80);
        assert_eq!(events[4].params.as_ref().unwrap()["currSpeed"], 1.25);
    }

    #[tokio::test]
    async fn live_restore_does_not_advertise_vod_only_controls() {
        let state = speed_test_state().await;
        let mut session = state.session().await.unwrap();
        session.live = true;
        state.set_session(Some(session)).await;
        let receiver = test_receiver(state, true);
        let events = receiver.restore_events("s-1").await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].method, "OnPlayState");
    }

    #[tokio::test]
    async fn switch_danmaku_persists_and_echoes_the_new_state() {
        let state = speed_test_state().await;
        let mut events = state.subscribe_nva();
        let receiver = test_receiver(state, true);
        receiver
            .switch_danmaku("s-1", &json!({"danmakuSwitch": "false"}))
            .await
            .unwrap();
        let event = events.recv().await.unwrap();
        assert_eq!(event.method, "OnDanmakuSwitch");
        assert_eq!(event.params.unwrap()["open"], false);
        assert!(
            !receiver
                .active_request
                .lock()
                .await
                .as_ref()
                .unwrap()
                .danmaku_enabled
        );
    }

    fn test_receiver(state: AppState, danmaku_enabled: bool) -> Receiver {
        Receiver {
            state,
            resolver: BilibiliResolver::new().expect("test resolver"),
            active_request: Arc::new(Mutex::new(Some(ActiveRequest {
                session_id: "s-1".into(),
                request: PlayRequest {
                    aid: "42".into(),
                    cid: "99".into(),
                    episode_id: "7".into(),
                    season_id: "6".into(),
                    content_type: 2,
                    ..PlayRequest::default()
                },
                available_qualities: vec![120, 80],
                danmaku_enabled,
            }))),
            port: 9959,
        }
    }

    async fn speed_test_state() -> AppState {
        use crate::state::SessionView;
        use std::{net::Ipv4Addr, path::PathBuf};
        use uuid::Uuid;
        let state = AppState::new(&crate::config::RuntimeConfig {
            web_listen: std::net::SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080),
            nva_listen: std::net::SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9959),
            lelink_listen: std::net::SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 52288),
            advertise_ip: Ipv4Addr::new(192, 0, 2, 10),
            config_path: PathBuf::from("unused.json"),
            web_dir: PathBuf::from("web/dist"),
            ffmpeg: PathBuf::from("ffmpeg"),
            nva_name: "UniNVA".into(),
            dlna_name: "UniDLNA".into(),
            lelink_name: "UniLE".into(),
            device_uuid: Uuid::nil(),
            nva_device_uuid: Uuid::nil(),
            retired_nva_device_uuid: None,
            selected_udn: None,
            scan_interface_ids: Vec::new(),
        })
        .expect("test state");
        state
            .set_session(Some(SessionView {
                id: "s-1".into(),
                origin: SessionOrigin::Nva,
                title: "正片".into(),
                phase: "playing".into(),
                quality: "80".into(),
                speed: speed_argument(1.25),
                input: "dash".into(),
                output: "mp2t".into(),
                backend: crate::state::SessionBackend::Dlna,
                target_name: "Fake TV".into(),
                target_udn: "uuid:fake".into(),
                started_unix_ms: 0,
                error: None,
                live: false,
            }))
            .await;
        state
    }
}
