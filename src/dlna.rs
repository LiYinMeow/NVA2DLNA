use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{StreamExt, stream};
use reqwest::{Client, Url, redirect::Policy};
use roxmltree::{Document, Node};
use serde_json::json;
use tokio::{
    net::UdpSocket,
    sync::{RwLock, Semaphore},
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Host;
use uuid::Uuid;

use crate::{
    bilibili::{MediaSource, ResolvedMedia},
    state::{
        AppState, HlsResourceStore, MAX_FFMPEG_CONSUMERS, MediaEntry, MediaInput, NvaEvent,
        RemuxFormat, Renderer, SessionView, SupersededPlay, now_ms,
    },
    upnp,
};

const SSDP_DESTINATION: &str = "239.255.255.250:1900";
const DISCOVERY_WINDOW: Duration = Duration::from_millis(2300);
const DESCRIPTION_FETCH_WINDOW: Duration = Duration::from_secs(12);
const MAX_DISCOVERY_LOCATIONS: usize = 64;
const MAX_LOCATIONS_PER_PEER: usize = 4;
const DESCRIPTION_FETCH_CONCURRENCY: usize = 8;
const MAX_DESCRIPTION_BYTES: usize = 512 * 1024;
const MAX_SOAP_RESPONSE_BYTES: usize = 256 * 1024;

pub async fn scan(state: AppState) -> Result<Vec<Renderer>> {
    let _scan_guard = state.scan_operation().await;
    state.set_scanning(true).await;
    let result = scan_inner(&state).await;
    state.set_scanning(false).await;
    let renderers = result?;
    state.replace_renderers(renderers.clone()).await;
    Ok(renderers)
}

async fn scan_inner(state: &AppState) -> Result<Vec<Renderer>> {
    let socket = UdpSocket::bind((state.advertise_ip(), 0))
        .await
        .context("cannot bind DLNA discovery socket")?;
    socket.set_broadcast(true)?;
    let request = concat!(
        "M-SEARCH * HTTP/1.1\r\n",
        "HOST: 239.255.255.250:1900\r\n",
        "MAN: \"ssdp:discover\"\r\n",
        "MX: 2\r\n",
        "ST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n",
        "USER-AGENT: NVA2DLNA/0.1 UPnP/1.1\r\n\r\n"
    );
    for _ in 0..2 {
        socket
            .send_to(request.as_bytes(), SSDP_DESTINATION)
            .await
            .context("cannot send DLNA M-SEARCH")?;
    }
    let deadline = time::Instant::now() + DISCOVERY_WINDOW;
    let mut locations = HashMap::new();
    let mut locations_per_peer = HashMap::<IpAddr, usize>::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let received = time::timeout_at(deadline, socket.recv_from(&mut buffer)).await;
        let Ok(Ok((length, peer))) = received else {
            break;
        };
        let text = String::from_utf8_lossy(&buffer[..length]);
        let headers = parse_headers(&text);
        if valid_renderer_search_response(&text, &headers)
            && let Some(location) = headers.get("location")
            && location.len() <= 4096
            && !locations.contains_key(location)
            && locations.len() < MAX_DISCOVERY_LOCATIONS
            && locations_per_peer.get(&peer.ip()).copied().unwrap_or(0) < MAX_LOCATIONS_PER_PEER
            && let Ok(location_url) = Url::parse(location)
            && validate_dlna_url(&location_url, peer.ip()).is_ok()
        {
            // LOCATION may contain a hostname. Keep the SSDP sender's address
            // so media tokens remain restricted to the discovered renderer.
            locations.insert(location.clone(), peer.ip());
            *locations_per_peer.entry(peer.ip()).or_default() += 1;
        }
    }

    let mut renderers = Vec::new();
    let fetches = stream::iter(locations)
        .map(|(location, discovered_ip)| async move {
            let result = fetch_renderer(state, &location, discovered_ip).await;
            (location, result)
        })
        .buffer_unordered(DESCRIPTION_FETCH_CONCURRENCY);
    tokio::pin!(fetches);
    let fetch_deadline = time::Instant::now() + DESCRIPTION_FETCH_WINDOW;
    loop {
        let fetched = time::timeout_at(fetch_deadline, fetches.next()).await;
        match fetched {
            Ok(Some((_, Ok(Some(renderer))))) => renderers.push(renderer),
            Ok(Some((_location, Ok(None)))) => {}
            Ok(Some((location, Err(error)))) => {
                debug!(%error, %location, "ignored unusable DLNA description")
            }
            Ok(None) => break,
            Err(_) => {
                debug!("DLNA description fetch window expired");
                break;
            }
        }
    }
    info!(count = renderers.len(), "DLNA discovery completed");
    Ok(renderers)
}

fn valid_renderer_search_response(text: &str, headers: &HashMap<String, String>) -> bool {
    let status_ok = text.lines().next().map(str::trim).is_some_and(|line| {
        (line.starts_with("HTTP/1.1 ") || line.starts_with("HTTP/1.0 "))
            && line
                .split_ascii_whitespace()
                .nth(1)
                .is_some_and(|status| status == "200")
    });
    let renderer_st = headers
        .get("st")
        .is_some_and(|value| is_media_renderer(value));
    let valid_usn = headers
        .get("usn")
        .is_some_and(|value| value.len() <= 1024 && value.to_ascii_lowercase().contains("uuid:"));
    status_ok && renderer_st && valid_usn
}

fn parse_headers(text: &str) -> HashMap<String, String> {
    text.lines()
        .skip(1)
        .filter_map(|line| line.trim_end_matches('\r').split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect()
}

async fn fetch_renderer(
    state: &AppState,
    location: &str,
    discovered_ip: IpAddr,
) -> Result<Option<Renderer>> {
    let location_url = Url::parse(location).context("DLNA LOCATION is invalid")?;
    let client = pinned_dlna_client(&location_url, discovered_ip)?;
    let response = client
        .get(location_url.clone())
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .context("cannot fetch DLNA device description")?;
    if !response.status().is_success() {
        bail!("DLNA description returned {}", response.status());
    }
    let bytes = read_limited_response(response, MAX_DESCRIPTION_BYTES).await?;
    let xml = std::str::from_utf8(&bytes).context("DLNA description is not UTF-8")?;
    let document = Document::parse(xml).context("DLNA description XML is invalid")?;
    let device = document
        .descendants()
        .filter(|node| node.is_element() && node.tag_name().name() == "device")
        .find(|device| {
            child_text(*device, "deviceType")
                .as_deref()
                .is_some_and(is_media_renderer)
        });
    let Some(device) = device else {
        return Ok(None);
    };
    let udn = child_text(device, "UDN").context("MediaRenderer has no UDN")?;
    if udn.len() > 1024 {
        bail!("MediaRenderer UDN is too long");
    }
    let own = format!("uuid:{}", upnp::nva_tv_id(state.device_uuid()));
    if udn.eq_ignore_ascii_case(&own) {
        return Ok(None);
    }
    let base = document
        .descendants()
        .find(|node| node.is_element() && node.tag_name().name() == "URLBase")
        .and_then(|node| node.text())
        .map(|value| {
            location_url
                .join(value.trim())
                .context("MediaRenderer URLBase is invalid")
        })
        .transpose()?
        .unwrap_or_else(|| location_url.clone());
    validate_dlna_url(&base, discovered_ip)?;

    let mut av_transport = None;
    let mut rendering_control = None;
    for service in device
        .descendants()
        .filter(|node| node.is_element() && node.tag_name().name() == "service")
    {
        let Some(service_type) = child_text(service, "serviceType") else {
            continue;
        };
        let Some(control) = child_text(service, "controlURL") else {
            continue;
        };
        let control = base
            .join(&control)
            .or_else(|_| location_url.join(&control))?;
        validate_dlna_url(&control, discovered_ip)?;
        if service_type.contains(":service:AVTransport:") {
            av_transport = Some((service_type, control.to_string()));
        } else if service_type.contains(":service:RenderingControl:") {
            rendering_control = Some((service_type, control.to_string()));
        }
    }
    let (av_transport_service_type, av_transport_url) =
        av_transport.context("MediaRenderer has no AVTransport service")?;
    let address = discovered_ip.to_string();
    Ok(Some(Renderer {
        udn,
        friendly_name: child_text(device, "friendlyName")
            .unwrap_or_else(|| "未命名 DLNA 设备".into()),
        manufacturer: child_text(device, "manufacturer").unwrap_or_default(),
        model_name: child_text(device, "modelName").unwrap_or_default(),
        location: location.to_owned(),
        av_transport_url,
        av_transport_service_type,
        rendering_control_url: rendering_control.as_ref().map(|(_, url)| url.clone()),
        rendering_control_service_type: rendering_control.map(|(kind, _)| kind),
        sink_protocols: Vec::new(),
        address,
        last_seen_unix_ms: now_ms(),
    }))
}

fn validate_dlna_url(url: &Url, discovered_ip: IpAddr) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https") {
        bail!("DLNA endpoint must use HTTP or HTTPS");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("DLNA endpoint must not contain credentials");
    }
    if url.host_str().is_none_or(|host| host.len() > 253) || url.port_or_known_default().is_none() {
        bail!("DLNA endpoint has no valid host or port");
    }
    match url.host() {
        Some(Host::Ipv4(ip)) if IpAddr::V4(ip) == discovered_ip => Ok(()),
        Some(Host::Ipv6(ip)) if IpAddr::V6(ip) == discovered_ip => Ok(()),
        Some(Host::Domain(_)) => Ok(()),
        Some(_) => bail!("DLNA endpoint IP does not match its SSDP sender"),
        None => bail!("DLNA endpoint has no host"),
    }
}

fn pinned_dlna_client(url: &Url, discovered_ip: IpAddr) -> Result<Client> {
    validate_dlna_url(url, discovered_ip)?;
    let host = url.host_str().context("DLNA endpoint has no host")?;
    let port = url
        .port_or_known_default()
        .context("DLNA endpoint has no known port")?;
    let mut builder = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .gzip(false)
        .connect_timeout(Duration::from_secs(3))
        .read_timeout(Duration::from_secs(7))
        .user_agent("NVA2DLNA/0.1 UPnP/1.1");
    if matches!(url.host(), Some(Host::Domain(_))) {
        builder = builder.resolve_to_addrs(host, &[SocketAddr::new(discovered_ip, port)]);
    }
    builder.build().context("cannot create pinned DLNA client")
}

async fn read_limited_response(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        bail!("DLNA response is too large");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > limit {
            bail!("DLNA response is too large");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn child_text(node: Node<'_, '_>, name: &str) -> Option<String> {
    node.children()
        .find(|child| child.is_element() && child.tag_name().name() == name)
        .and_then(|child| child.text())
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
}

fn is_media_renderer(device_type: &str) -> bool {
    device_type
        .strip_prefix("urn:schemas-upnp-org:device:MediaRenderer:")
        .and_then(|version| version.parse::<u16>().ok())
        .is_some_and(|version| version >= 1)
}

pub async fn play(
    state: AppState,
    session_id: &str,
    media: ResolvedMedia,
    seek_position_ms: u64,
    play_epoch: u64,
) -> Result<()> {
    let _guard = state.operation().await;
    state.ensure_play_epoch(play_epoch)?;
    if let Some(previous) = state.session().await {
        if let Some(previous_renderer) = state.renderer(&previous.target_udn).await {
            let _ = soap_call(
                &previous_renderer.av_transport_url,
                &previous_renderer.av_transport_service_type,
                "Stop",
                "<InstanceID>0</InstanceID>",
                &previous_renderer.address,
            )
            .await;
        }
        state.ensure_play_epoch(play_epoch)?;
        state.revoke_media().await;
        if previous.id != session_id {
            state.mark_session_terminated(&previous.id).await;
            state.emit_nva(NvaEvent {
                session_id: previous.id,
                method: "OnPlayState".into(),
                params: Some(json!({"playState": 7})),
                close_after: true,
            });
        }
    }
    let renderer = state.selected_renderer().await;
    state.ensure_play_epoch(play_epoch)?;
    let renderer = renderer?;
    let token = Uuid::new_v4().simple().to_string();
    let cancellation = CancellationToken::new();
    let is_live = media.live;
    let (input, mime, input_label, output_label, file_name) = match media.source {
        MediaSource::Progressive { url } => {
            let (mime, file_name, remux_format) = stream_format(&url);
            if let Some(format) = remux_format {
                (
                    MediaInput::Remux { url, format },
                    mime,
                    "progressive",
                    "ffmpeg-remux",
                    file_name,
                )
            } else {
                (
                    MediaInput::Progressive { url },
                    mime,
                    "progressive",
                    "proxy",
                    file_name,
                )
            }
        }
        MediaSource::Dash {
            video_url,
            video_backup_urls,
            audio_url,
            audio_backup_urls,
        } => (
            MediaInput::Dash {
                video_url,
                video_backup_urls,
                audio_url,
                audio_backup_urls,
            },
            "video/mp2t",
            "dash",
            "ffmpeg-remux",
            "stream.ts",
        ),
    };
    let media_url = format!(
        "http://{}:{}/media/{token}/{file_name}",
        state.advertise_ip(),
        state.web_port()
    );
    let media_entry = MediaEntry {
        token: token.clone(),
        owner_session: session_id.to_owned(),
        input,
        mime: mime.to_owned(),
        created_unix_ms: now_ms(),
        allowed_renderer_ip: renderer.address.clone(),
        cancellation: cancellation.clone(),
        ffmpeg_consumers: Arc::new(Semaphore::new(MAX_FFMPEG_CONSUMERS)),
        hls_resources: Arc::new(RwLock::new(HlsResourceStore::default())),
    };
    let session_view = SessionView {
        id: session_id.to_owned(),
        title: media.title.clone(),
        phase: "connecting".into(),
        quality: media.quality.clone(),
        input: input_label.into(),
        output: output_label.into(),
        target_name: renderer.friendly_name.clone(),
        target_udn: renderer.udn.clone(),
        started_unix_ms: now_ms(),
        error: None,
        live: is_live,
    };

    let result: Result<()> = async {
        state.register_media(media_entry).await;
        state.set_session(Some(session_view)).await;
        state.ensure_play_epoch(play_epoch)?;
        let metadata = upnp::didl(&media.title, &media_url, mime);
        let set_uri = soap_call(
            &renderer.av_transport_url,
            &renderer.av_transport_service_type,
            "SetAVTransportURI",
            &format!(
                "<InstanceID>0</InstanceID><CurrentURI>{}</CurrentURI>\
<CurrentURIMetaData>{}</CurrentURIMetaData>",
                upnp::xml_escape(&media_url),
                upnp::xml_escape(&metadata)
            ),
            &renderer.address,
        )
        .await;
        state.ensure_play_epoch(play_epoch)?;
        set_uri?;
        let play = soap_call(
            &renderer.av_transport_url,
            &renderer.av_transport_service_type,
            "Play",
            "<InstanceID>0</InstanceID><Speed>1</Speed>",
            &renderer.address,
        )
        .await;
        state.ensure_play_epoch(play_epoch)?;
        play?;
        if seek_position_ms > 0 && input_label == "progressive" && !is_live {
            // Many DMRs reject Seek while still STOPPED even after accepting
            // SetAVTransportURI. Start first, then apply the resume position.
            time::sleep(Duration::from_millis(150)).await;
            if let Err(error) = seek_inner(&renderer, seek_position_ms).await {
                warn!(%error, "DLNA target rejected the initial resume position");
            }
        }
        state.ensure_play_epoch(play_epoch)?;
        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            spawn_transport_monitor(
                state.clone(),
                renderer,
                session_id.to_owned(),
                token,
                cancellation,
                is_live,
            );
            Ok(())
        }
        Err(error) => {
            state.revoke_media().await;
            if error.downcast_ref::<SupersededPlay>().is_none() {
                state
                    .update_session_phase_if(session_id, "error", Some(error.to_string()))
                    .await;
            }
            Err(error)
        }
    }
}

pub async fn pause(state: AppState, session_id: &str) -> Result<()> {
    control_simple(&state, session_id, "Pause", "<InstanceID>0</InstanceID>").await?;
    let _ = state
        .update_session_phase_if(session_id, "paused", None)
        .await;
    state.emit_nva(NvaEvent {
        session_id: session_id.into(),
        method: "OnPlayState".into(),
        params: Some(json!({"playState": 5})),
        close_after: false,
    });
    Ok(())
}

pub async fn resume(state: AppState, session_id: &str) -> Result<()> {
    control_simple(
        &state,
        session_id,
        "Play",
        "<InstanceID>0</InstanceID><Speed>1</Speed>",
    )
    .await?;
    let _ = state
        .update_session_phase_if(session_id, "playing", None)
        .await;
    state.emit_nva(NvaEvent {
        session_id: session_id.into(),
        method: "OnPlayState".into(),
        params: Some(json!({"playState": 4})),
        close_after: false,
    });
    Ok(())
}

pub async fn stop(state: AppState, nva_session: Option<&str>) -> Result<()> {
    state.cancel_pending_play_for(nva_session).await;
    let _guard = state.operation().await;
    stop_locked(&state, nva_session).await
}

async fn stop_locked(state: &AppState, nva_session: Option<&str>) -> Result<()> {
    let session = state.session().await;
    if let (Some(owner), Some(session)) = (nva_session, session.as_ref())
        && session.id != owner
    {
        debug!(caller = %owner.chars().take(8).collect::<String>(), active = %session.id.chars().take(8).collect::<String>(), "ignored Stop from a stale NVA session");
        return Ok(());
    }
    let renderer = match session.as_ref() {
        Some(session) => state.renderer(&session.target_udn).await,
        None => None,
    };
    let ended_session = nva_session
        .map(str::to_owned)
        .or_else(|| session.as_ref().map(|session| session.id.clone()));
    state.revoke_media().await;
    state.set_session(None).await;
    if let Some(session_id) = ended_session.as_deref() {
        state.mark_session_terminated(session_id).await;
    }
    let result = if let Some(renderer) = renderer {
        soap_call(
            &renderer.av_transport_url,
            &renderer.av_transport_service_type,
            "Stop",
            "<InstanceID>0</InstanceID>",
            &renderer.address,
        )
        .await
    } else {
        Ok(())
    };
    if let Some(session_id) = ended_session {
        state.emit_nva(NvaEvent {
            session_id,
            method: "OnPlayState".into(),
            params: Some(json!({"playState": 7})),
            close_after: true,
        });
    }
    result
}

pub async fn select_target(state: AppState, udn: String) -> Result<()> {
    state.cancel_pending_play_for(None).await;
    let _guard = state.operation().await;
    // Validate before touching a running route.
    state
        .renderer(&udn)
        .await
        .ok_or_else(|| anyhow!("找不到指定的 DLNA 目标"))?;
    if state
        .session()
        .await
        .is_some_and(|session| session.target_udn != udn)
        && let Err(error) = stop_locked(&state, None).await
    {
        // Local state and the media token are already revoked by stop_locked.
        // A powered-off old renderer must not prevent selecting a new one.
        warn!(%error, "old DLNA target did not acknowledge Stop while switching target");
    }
    state.select_renderer(Some(udn)).await
}

pub async fn seek(state: AppState, session_id: &str, position_ms: u64) -> Result<()> {
    let _guard = state.operation().await;
    let renderer = current_renderer_for_session(&state, session_id).await?;
    seek_inner(&renderer, position_ms).await
}

async fn seek_inner(renderer: &Renderer, position_ms: u64) -> Result<()> {
    let seconds = position_ms / 1000;
    let target = format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    );
    soap_call(
        &renderer.av_transport_url,
        &renderer.av_transport_service_type,
        "Seek",
        &format!("<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>{target}</Target>"),
        &renderer.address,
    )
    .await
}

pub async fn set_volume(state: AppState, session_id: &str, volume: u8) -> Result<()> {
    let _guard = state.operation().await;
    let renderer = current_renderer_for_session(&state, session_id).await?;
    let (Some(url), Some(service)) = (
        renderer.rendering_control_url.as_deref(),
        renderer.rendering_control_service_type.as_deref(),
    ) else {
        return Ok(());
    };
    soap_call(
        url,
        service,
        "SetVolume",
        &format!(
            "<InstanceID>0</InstanceID><Channel>Master</Channel><DesiredVolume>{}</DesiredVolume>",
            volume.min(100)
        ),
        &renderer.address,
    )
    .await
}

async fn control_simple(
    state: &AppState,
    session_id: &str,
    action: &str,
    args: &str,
) -> Result<()> {
    let _guard = state.operation().await;
    let renderer = current_renderer_for_session(state, session_id).await?;
    soap_call(
        &renderer.av_transport_url,
        &renderer.av_transport_service_type,
        action,
        args,
        &renderer.address,
    )
    .await
}

async fn current_renderer_for_session(state: &AppState, session_id: &str) -> Result<Renderer> {
    let session = state
        .session()
        .await
        .filter(|session| session.id == session_id)
        .ok_or_else(|| anyhow!("the NVA session does not own the active cast"))?;
    state
        .renderer(&session.target_udn)
        .await
        .ok_or_else(|| anyhow!("当前 DLNA 播放目标已离线"))
}

fn spawn_transport_monitor(
    state: AppState,
    renderer: Renderer,
    session_id: String,
    media_token: String,
    cancellation: CancellationToken,
    is_live: bool,
) {
    tokio::spawn(async move {
        const PLAY_SUCCESS_FALLBACK_FAILURES: u32 = 3;
        const START_FAILURE_SAMPLES: u8 = 20;
        const END_CONFIRMATION_SAMPLES: u8 = 5;
        const STARTUP_DEADLINE: Duration = Duration::from_secs(25);

        let mut interval = time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let monitor_started = time::Instant::now();
        let mut seen_active = false;
        let mut terminal_samples = 0_u8;
        let mut last_play_state = None;
        let mut play_success_sent = false;
        let mut query_failures = 0_u32;
        // Avoid racing the renderer's short SET_URI -> PLAY transition and
        // keep immediate Stop deterministic.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = interval.tick() => {}
            }
            if state.media(&media_token).await.is_none() {
                break;
            }

            let transport = match soap_query(
                &renderer.av_transport_url,
                &renderer.av_transport_service_type,
                "GetTransportInfo",
                "<InstanceID>0</InstanceID>",
                &renderer.address,
            )
            .await
            .and_then(|xml| parse_transport_state(&xml))
            {
                Ok(xml) => {
                    query_failures = 0;
                    xml
                }
                Err(error) => {
                    query_failures = query_failures.saturating_add(1);
                    terminal_samples = 0;
                    if query_failures == 1 || query_failures.is_multiple_of(30) {
                        debug!(
                            session = %session_id.chars().take(8).collect::<String>(),
                            failures = query_failures,
                            %error,
                            "DLNA transport status query failed"
                        );
                    }
                    if query_failures == PLAY_SUCCESS_FALLBACK_FAILURES && !play_success_sent {
                        let _guard = state.operation().await;
                        if cancellation.is_cancelled() || state.media(&media_token).await.is_none()
                        {
                            break;
                        }
                        // Some otherwise functional DMRs omit query actions.
                        // Fall back to the accepted Play SOAP result after a
                        // short grace period instead of hanging the sender.
                        seen_active = true;
                        let _ = state
                            .update_session_phase_if(&session_id, "playing", None)
                            .await;
                        state.emit_nva(NvaEvent {
                            session_id: session_id.clone(),
                            method: "OnPlayState".into(),
                            params: Some(json!({"playState": 4})),
                            close_after: false,
                        });
                        state.emit_nva(NvaEvent {
                            session_id: session_id.clone(),
                            method: "PLAY_SUCCESS".into(),
                            params: None,
                            close_after: false,
                        });
                        last_play_state = Some(4);
                        play_success_sent = true;
                    }
                    continue;
                }
            };
            let operation_guard = state.operation().await;
            if cancellation.is_cancelled() || state.media(&media_token).await.is_none() {
                break;
            }

            if !seen_active && monitor_started.elapsed() >= STARTUP_DEADLINE {
                let replacement_pending =
                    state.pending_play_owner().await.as_deref() == Some(session_id.as_str());
                if replacement_pending {
                    drop(operation_guard);
                    continue;
                }
                if state
                    .finish_session_if(
                        &session_id,
                        &media_token,
                        Some("DLNA target remained in a transitional state".to_owned()),
                    )
                    .await
                {
                    warn!(
                        session = %session_id.chars().take(8).collect::<String>(),
                        "DLNA target exceeded the playback startup deadline"
                    );
                    state.emit_nva(NvaEvent {
                        session_id: session_id.clone(),
                        method: "OnPlayState".into(),
                        params: Some(json!({"playState": 7})),
                        close_after: true,
                    });
                }
                break;
            }

            let play_state = match transport.as_str() {
                "PLAYING" => {
                    seen_active = true;
                    terminal_samples = 0;
                    let _ = state
                        .update_session_phase_if(&session_id, "playing", None)
                        .await;
                    Some(4)
                }
                "PAUSED_PLAYBACK" | "PAUSED_RECORDING" => {
                    seen_active = true;
                    terminal_samples = 0;
                    let _ = state
                        .update_session_phase_if(&session_id, "paused", None)
                        .await;
                    Some(5)
                }
                "STOPPED" | "NO_MEDIA_PRESENT" => {
                    terminal_samples = terminal_samples.saturating_add(1);
                    None
                }
                "TRANSITIONING" => {
                    terminal_samples = 0;
                    None
                }
                _ => unreachable!("transport states are validated before monitoring"),
            };

            if let Some(play_state) = play_state
                && last_play_state != Some(play_state)
            {
                state.emit_nva(NvaEvent {
                    session_id: session_id.clone(),
                    method: "OnPlayState".into(),
                    params: Some(json!({"playState": play_state})),
                    close_after: false,
                });
                last_play_state = Some(play_state);
            }
            if seen_active && !play_success_sent {
                state.emit_nva(NvaEvent {
                    session_id: session_id.clone(),
                    method: "PLAY_SUCCESS".into(),
                    params: None,
                    close_after: false,
                });
                play_success_sent = true;
            }

            let confirmed_end = seen_active && terminal_samples >= END_CONFIRMATION_SAMPLES;
            let confirmed_start_failure = !seen_active && terminal_samples >= START_FAILURE_SAMPLES;
            if confirmed_end || confirmed_start_failure {
                let replacement_pending =
                    state.pending_play_owner().await.as_deref() == Some(session_id.as_str());
                if replacement_pending {
                    // The old token may report STOPPED while the same phone
                    // session is still resolving its replacement Play.  Let
                    // that queued command (or a queued Stop) own termination.
                    terminal_samples = 0;
                    drop(operation_guard);
                    continue;
                }
                let terminal_error = confirmed_start_failure
                    .then(|| "DLNA target did not enter playback".to_owned());
                if state
                    .finish_session_if(&session_id, &media_token, terminal_error)
                    .await
                {
                    info!(
                        session = %session_id.chars().take(8).collect::<String>(),
                        start_failure = confirmed_start_failure,
                        "DLNA target reported terminal playback state"
                    );
                    state.emit_nva(NvaEvent {
                        session_id: session_id.clone(),
                        method: "OnPlayState".into(),
                        params: Some(json!({"playState": 7})),
                        close_after: true,
                    });
                }
                break;
            }
            drop(operation_guard);

            if !seen_active || is_live {
                continue;
            }
            if let Ok(position) = soap_query(
                &renderer.av_transport_url,
                &renderer.av_transport_service_type,
                "GetPositionInfo",
                "<InstanceID>0</InstanceID>",
                &renderer.address,
            )
            .await
            {
                let _guard = state.operation().await;
                if cancellation.is_cancelled() || state.media(&media_token).await.is_none() {
                    break;
                }
                let elapsed = soap_text(&position, "RelTime").and_then(parse_dlna_time_ms);
                let duration = soap_text(&position, "TrackDuration").and_then(parse_dlna_time_ms);
                if let (Some(position_ms), Some(duration_ms)) = (elapsed, duration)
                    && duration_ms > 0
                {
                    state.emit_nva(NvaEvent {
                        session_id: session_id.clone(),
                        method: "OnProgress".into(),
                        params: Some(json!({
                            "position": position_ms / 1000,
                            "duration": duration_ms / 1000,
                        })),
                        close_after: false,
                    });
                }
            }
        }
    });
}

fn parse_transport_state(xml: &str) -> Result<String> {
    let state = soap_text(xml, "CurrentTransportState")
        .ok_or_else(|| anyhow!("DLNA transport response has no CurrentTransportState"))?
        .to_ascii_uppercase();
    match state.as_str() {
        "PLAYING" | "PAUSED_PLAYBACK" | "PAUSED_RECORDING" | "STOPPED" | "NO_MEDIA_PRESENT"
        | "TRANSITIONING" => Ok(state),
        _ => Err(anyhow!("DLNA target returned unknown transport state")),
    }
}

fn soap_text(xml: &str, name: &str) -> Option<String> {
    Document::parse(xml)
        .ok()?
        .descendants()
        .find(|node| node.is_element() && node.tag_name().name() == name)
        .and_then(|node| node.text())
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("NOT_IMPLEMENTED"))
        .map(str::to_owned)
}

fn parse_dlna_time_ms(value: String) -> Option<u64> {
    let mut parts = value.split(':');
    let hours = parts.next()?.parse::<u64>().ok()?;
    let minutes = parts.next()?.parse::<u64>().ok()?;
    let seconds = parts.next()?.parse::<f64>().ok()?;
    if parts.next().is_some() || minutes >= 60 || !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let total_ms = ((hours * 3600 + minutes * 60) as f64 * 1000.0 + seconds * 1000.0).round();
    (total_ms >= 0.0 && total_ms <= u64::MAX as f64).then_some(total_ms as u64)
}

async fn soap_call(
    control_url: &str,
    service: &str,
    action: &str,
    arguments: &str,
    discovered_ip: &str,
) -> Result<()> {
    soap_query(control_url, service, action, arguments, discovered_ip)
        .await
        .map(|_| ())
}

async fn soap_query(
    control_url: &str,
    service: &str,
    action: &str,
    arguments: &str,
    discovered_ip: &str,
) -> Result<String> {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body>\
<u:{action} xmlns:u=\"{service}\">{arguments}</u:{action}></s:Body></s:Envelope>"
    );
    let control_url = Url::parse(control_url).context("DLNA control URL is invalid")?;
    let discovered_ip = discovered_ip
        .parse::<IpAddr>()
        .context("DLNA renderer address is invalid")?;
    let client = pinned_dlna_client(&control_url, discovered_ip)?;
    let response = client
        .post(control_url)
        .header("Content-Type", "text/xml; charset=\"utf-8\"")
        .header("SOAPAction", format!("\"{service}#{action}\""))
        .body(body)
        .timeout(Duration::from_secs(7))
        .send()
        .await
        .with_context(|| format!("DLNA {action} request failed"))?;
    let status = response.status();
    let response_bytes = read_limited_response(response, MAX_SOAP_RESPONSE_BYTES).await;
    if !status.is_success() {
        let summary = match response_bytes {
            Ok(bytes) => String::from_utf8_lossy(&bytes)
                .chars()
                .take(300)
                .collect::<String>(),
            Err(_) => "response body exceeded the safety limit".to_owned(),
        };
        bail!("DLNA {action} returned {status}: {summary}");
    }
    String::from_utf8(response_bytes?).context("DLNA SOAP response is not UTF-8")
}

fn stream_format(url: &str) -> (&'static str, &'static str, Option<RemuxFormat>) {
    let path = Url::parse(url)
        .ok()
        .map(|url| url.path().to_ascii_lowercase())
        .unwrap_or_else(|| url.to_ascii_lowercase());
    if path.ends_with(".flv") {
        ("video/mp2t", "stream.ts", Some(RemuxFormat::Flv))
    } else if path.ends_with(".m3u8") {
        ("video/mp2t", "stream.ts", Some(RemuxFormat::Hls))
    } else if path.ends_with(".webm") {
        ("video/webm", "stream.webm", None)
    } else if path.ends_with(".ts") || path.ends_with(".m2ts") {
        ("video/mp2t", "stream.ts", None)
    } else if path.ends_with(".mp3") {
        ("audio/mpeg", "stream.mp3", None)
    } else if path.ends_with(".m4a") {
        ("audio/mp4", "stream.m4a", None)
    } else if path.ends_with(".aac") {
        ("audio/aac", "stream.aac", None)
    } else {
        // Signed progressive VOD URLs may omit a suffix.
        ("video/mp4", "stream.mp4", None)
    }
}

pub async fn periodic_scan(state: AppState) {
    loop {
        if let Err(error) = scan(state.clone()).await {
            warn!(%error, "periodic DLNA scan failed");
        }
        time::sleep(Duration::from_secs(30)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_case_insensitive_ssdp_headers() {
        let headers = parse_headers(
            "HTTP/1.1 200 OK\r\nLOCATION: http://10.0.0.2/device.xml\r\nST: test\r\n\r\n",
        );
        assert_eq!(
            headers.get("location").map(String::as_str),
            Some("http://10.0.0.2/device.xml")
        );
    }

    #[test]
    fn discovery_accepts_only_renderer_success_responses() {
        let valid = concat!(
            "HTTP/1.1 200 OK\r\n",
            "ST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n",
            "USN: uuid:renderer::urn:schemas-upnp-org:device:MediaRenderer:1\r\n",
            "LOCATION: http://10.0.0.2/device.xml\r\n\r\n"
        );
        assert!(valid_renderer_search_response(valid, &parse_headers(valid)));

        let wrong_status = valid.replacen("200 OK", "404 Not Found", 1);
        assert!(!valid_renderer_search_response(
            &wrong_status,
            &parse_headers(&wrong_status)
        ));
        let wrong_target = valid.replace("device:MediaRenderer:1", "device:MediaServer:1");
        assert!(!valid_renderer_search_response(
            &wrong_target,
            &parse_headers(&wrong_target)
        ));
    }

    #[test]
    fn dlna_endpoints_are_pinned_to_the_ssdp_sender() {
        let peer = "10.0.0.2".parse().unwrap();
        assert!(
            validate_dlna_url(&Url::parse("http://10.0.0.2/device.xml").unwrap(), peer).is_ok()
        );
        assert!(
            validate_dlna_url(&Url::parse("http://tv.local/device.xml").unwrap(), peer).is_ok()
        );
        assert!(validate_dlna_url(&Url::parse("http://127.0.0.1/admin").unwrap(), peer).is_err());
        assert!(validate_dlna_url(&Url::parse("file:///etc/passwd").unwrap(), peer).is_err());
    }

    #[test]
    fn accepts_newer_media_renderer_versions() {
        assert!(is_media_renderer(
            "urn:schemas-upnp-org:device:MediaRenderer:1"
        ));
        assert!(is_media_renderer(
            "urn:schemas-upnp-org:device:MediaRenderer:2"
        ));
        assert!(!is_media_renderer(
            "urn:schemas-upnp-org:device:MediaServer:1"
        ));
    }

    #[test]
    fn keeps_progressive_media_type_and_extension_aligned() {
        assert_eq!(
            stream_format("https://cdn.example/video.flv?token=1"),
            ("video/mp2t", "stream.ts", Some(RemuxFormat::Flv))
        );
        assert_eq!(
            stream_format("https://cdn.example/live.m3u8"),
            ("video/mp2t", "stream.ts", Some(RemuxFormat::Hls))
        );
        assert_eq!(
            stream_format("https://cdn.example/signed-resource"),
            ("video/mp4", "stream.mp4", None)
        );
    }

    #[test]
    fn transport_monitor_rejects_missing_and_unknown_states() {
        assert_eq!(
            parse_transport_state(
                "<Envelope><CurrentTransportState>playing</CurrentTransportState></Envelope>"
            )
            .expect("valid transport state"),
            "PLAYING"
        );
        assert!(parse_transport_state("<Envelope />").is_err());
        assert!(
            parse_transport_state(
                "<Envelope><CurrentTransportState>VENDOR_WAIT</CurrentTransportState></Envelope>"
            )
            .is_err()
        );
    }
}
