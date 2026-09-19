//! 乐播 (LeLink/hpplay) native **V1** receiver front end: the plaintext HTTP dialect a
//! phone sender speaks when its discovery record carries no `vv="2"`. Casts arriving
//! here are relayed to the selected DLNA renderer through [`crate::dlna`].
//!
//! The phone drives both faces: a UDP probe on 25353 that we answer with an `LBTP`
//! packet, then plain HTTP/1.1 on 52288 over a kept-alive socket.
//!
//! The wire is unusually forgiving -- every response check the phone makes is a
//! substring match on `200`/`successful`, and it never reads a header we send. That
//! buys two constraints worth respecting: a `401` anywhere in a reply switches the
//! phone into its casting-code flow (it then re-sends `/play` with a Digest header),
//! and `603`/`453` read as failures. Replies are therefore bare.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    bilibili::{MediaSource, ResolvedMedia},
    dlna,
    state::{AppState, SessionOrigin, TransportState},
};

/// The port a LeLink sender dials by default; advertised so it can be moved.
pub const CONTROL_PORT: u16 = 52288;
/// Where the phone broadcasts its probes and, separately, listens for answers.
pub const BROWSE_PORT: u16 = 25353;

const PROBE_MAGIC: &str = "magic-number:PTBL";
const REPLY_MAGIC: &str = "magic-number:LBTP";
/// The phone only keeps a connection alive by polling `POST /feedback` while the
/// advertised channel version looks like 3.x or 5.x; anything else and it falls back
/// to bare TCP reachability probes.
const CHANNEL_VERSION: &str = "5.0.1";
const VOLUME_STEP: u8 = 5;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// The phone polls every two seconds, so a quieter connection than this has gone away
/// without saying so.
const IDLE_WINDOW: Duration = Duration::from_secs(120);

/// Stable identity for the native face, distinct from the NVA and DMR device ids.
pub fn device_id(state: &AppState) -> String {
    Uuid::new_v5(&state.device_uuid(), b"nva2dlna-lelink")
        .simple()
        .to_string()
}

#[derive(Serialize)]
struct DeviceRecord {
    devicename: String,
    deviceip: String,
    u: String,
    lelinkport: u16,
    /// V1 senders take the push/reverse port from `airplay` and the control port from
    /// `lelinkport`, so all three names are pointed at the one listener.
    raop: u16,
    airplay: u16,
    channel: &'static str,
}

/// The third `CRLF`-separated line is the only thing the phone parses. `vv` is
/// deliberately absent: `vv=="2"` is the sole V2 selector, and a missing one is read
/// as a legacy device.
pub fn browse_record(state: &AppState, control_port: u16) -> String {
    let interface = state.advertise_ip();
    let record = DeviceRecord {
        devicename: state.lelink_name().to_owned(),
        deviceip: interface.to_string(),
        u: device_id(state),
        lelinkport: control_port,
        raop: control_port,
        airplay: control_port,
        channel: CHANNEL_VERSION,
    };
    serde_json::to_string(&record).unwrap_or_else(|_| "{}".to_owned())
}

pub fn browse_reply(state: &AppState, control_port: u16) -> String {
    format!(
        "{REPLY_MAGIC}\r\nxor-key:0000\r\n{}\r\n",
        browse_record(state, control_port)
    )
}

pub async fn browse(state: AppState, control_port: u16) -> Result<()> {
    let interface = state.advertise_ip();
    let socket = browse_socket(interface)?;
    browse_on(state, socket, control_port).await
}

/// Takes the socket as a seam so the loop can be driven from a test without claiming
/// the protocol's fixed port.
pub async fn browse_on(state: AppState, socket: UdpSocket, control_port: u16) -> Result<()> {
    let listening = socket
        .local_addr()
        .context("LeLink browse socket has no address")?;
    info!(%listening, "LeLink browse responder ready");
    let broadcast = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, BROWSE_PORT));
    let reply = browse_reply(&state, control_port);
    let mut buffer = [0_u8; 2048];
    loop {
        let (length, peer) = socket
            .recv_from(&mut buffer)
            .await
            .context("LeLink browse receive failed")?;
        let probe = String::from_utf8_lossy(&buffer[..length]);
        if !probe.contains(PROBE_MAGIC) {
            continue;
        }
        // The probe leaves an ephemeral port while the answer listener is bound to
        // 25353, so unicast alone would be dropped. Both are sent; the second is
        // harmless and covers senders that listen where they spoke from.
        if let Err(error) = socket.send_to(reply.as_bytes(), broadcast).await {
            debug!(%peer, %error, "cannot broadcast LeLink browse reply");
        }
        if let Err(error) = socket.send_to(reply.as_bytes(), peer).await {
            debug!(%peer, %error, "cannot unicast LeLink browse reply");
        }
    }
}

fn browse_socket(interface: Ipv4Addr) -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_broadcast(true)?;
    let bind_ip = if cfg!(windows) {
        interface
    } else {
        Ipv4Addr::UNSPECIFIED
    };
    socket
        .bind(&SocketAddrV4::new(bind_ip, BROWSE_PORT).into())
        .with_context(|| format!("cannot bind LeLink browse socket on {bind_ip}:{BROWSE_PORT}"))?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into()).context("cannot create async LeLink browse socket")
}

pub async fn run(state: AppState, listen: SocketAddrV4) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot bind LeLink receiver to {listen}"))?;
    run_on(state, listener).await
}

/// Bind the native LeLink control socket. Windows allocates outbound TCP source
/// ports from a range which commonly includes 52288, so a browser connection can
/// temporarily make the conventional LeLink port unavailable even though no other
/// receiver is running. LeLink discovery carries the control port explicitly; when
/// that exact collision occurs it is safe to let the OS choose another port and
/// advertise the port returned by [`TcpListener::local_addr`].
///
/// The optional error is the failure from the preferred port. Callers should log it
/// together with the actual listener address so an operator can distinguish a
/// fallback from a receiver which silently moved ports. Errors other than
/// `AddrInUse` are returned instead of being hidden by a fallback.
pub async fn bind_control(preferred: SocketAddrV4) -> Result<(TcpListener, Option<io::Error>)> {
    match TcpListener::bind(preferred).await {
        Ok(listener) => Ok((listener, None)),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse && preferred.port() != 0 => {
            let fallback = SocketAddrV4::new(*preferred.ip(), 0);
            let listener = TcpListener::bind(fallback).await.with_context(|| {
                format!(
                    "cannot bind a dynamic LeLink receiver after preferred address {preferred} was unavailable: {error}"
                )
            })?;
            Ok((listener, Some(error)))
        }
        Err(error) => {
            Err(error).with_context(|| format!("cannot bind LeLink receiver to {preferred}"))
        }
    }
}

/// The accepting half, split out so a test can hand in an ephemeral port.
pub async fn run_on(state: AppState, listener: TcpListener) -> Result<()> {
    let listening = listener
        .local_addr()
        .context("LeLink socket has no address")?;
    info!(%listening, name = state.lelink_name(), "LeLink V1 receiver ready");
    loop {
        let (stream, peer) = listener.accept().await.context("LeLink accept failed")?;
        let state = state.clone();
        tokio::spawn(async move {
            match serve(state, stream, peer).await {
                Ok(()) => debug!(%peer, "LeLink connection closed"),
                Err(error) => debug!(%peer, %error, "LeLink connection failed"),
            }
        });
    }
}

async fn serve(state: AppState, mut stream: TcpStream, peer: SocketAddr) -> Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let Some(request) = next_request(&mut stream, &mut buffer, &mut chunk).await? else {
            return Ok(());
        };
        let close = header_value(&request, "connection")
            .is_some_and(|value| value.eq_ignore_ascii_case("close"));
        let reply = handle(&state, peer, &request).await;
        stream
            .write_all(reply.as_bytes())
            .await
            .context("LeLink reply could not be written")?;
        stream.flush().await.ok();
        if close {
            return Ok(());
        }
    }
}

/// One request off a kept-alive connection, or `None` once the sender has hung up.
async fn next_request(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    chunk: &mut [u8],
) -> Result<Option<String>> {
    loop {
        if let Some(head) = find_header_end(buffer) {
            let length = content_length(&buffer[..head]).unwrap_or(0);
            if buffer.len() >= head + 4 + length {
                let end = head + 4 + length;
                let request = String::from_utf8_lossy(&buffer[..end]).into_owned();
                buffer.drain(..end);
                return Ok(Some(request));
            }
        }
        if buffer.len() > MAX_REQUEST_BYTES {
            return Err(anyhow!("LeLink request exceeded {MAX_REQUEST_BYTES} bytes"));
        }
        match tokio::time::timeout(IDLE_WINDOW, stream.read(chunk)).await {
            Ok(Ok(0)) => return Ok(None),
            Ok(Ok(read)) => buffer.extend_from_slice(&chunk[..read]),
            Ok(Err(error)) => return Err(error).context("LeLink connection read failed"),
            Err(_) => return Ok(None),
        }
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(head: &[u8]) -> Option<usize> {
    let head = String::from_utf8_lossy(head);
    header_value(&head, "content-length")?.trim().parse().ok()
}

fn header_value(request: &str, name: &str) -> Option<String> {
    request.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.trim().to_owned())
    })
}

fn request_target(request: &str) -> &str {
    request.split_whitespace().nth(1).unwrap_or_default()
}

fn path_of(target: &str) -> &str {
    target.split_once('?').map_or(target, |(path, _)| path)
}

fn query_param(target: &str, name: &str) -> Option<String> {
    let (_, query) = target.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| value.to_owned())
    })
}

/// The V1 `/play` body is `text/parameters`: `Name: value` lines, not JSON.
fn text_parameter(body: &str, name: &str) -> Option<String> {
    body.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

fn body_of(request: &str) -> &str {
    match request.split_once("\r\n\r\n") {
        Some((_, body)) => body,
        None => "",
    }
}

fn reply(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\
Connection: keep-alive\r\n\r\n{body}",
        body.len()
    )
}

async fn handle(state: &AppState, peer: SocketAddr, request: &str) -> String {
    let target = request_target(request);
    match path_of(target) {
        "/server-info" => reply(""),
        "/play" => cast(state, peer, request).await,
        "/send_videoInfo" => cast_json(state, peer, request).await,
        "/stop" => stop(state, peer).await,
        "/rate" => match query_param(target, "value").as_deref() {
            Some(value) if value.starts_with('1') => resume(state, peer).await,
            Some(value) if value.starts_with('0') => pause(state, peer).await,
            Some(value) => {
                warn!(%peer, value, "LeLink /rate carried an unusable value");
                reply("")
            }
            None => reply(""),
        },
        "/scrub" => {
            if request.starts_with("GET") {
                progress(state).await
            } else {
                seek(state, peer, target).await
            }
        }
        "/add_volume" => volume(state, peer, true).await,
        "/sub_volume" => volume(state, peer, false).await,
        "/feedback" => reply(""),
        "/reverse" => {
            // The sender opens this second connection and expects us to keep it. What
            // we answer is never parsed, so the event channel stays open for the plist
            // push a later step can add.
            debug!(%peer, "LeLink event channel opened");
            reply("")
        }
        other => {
            debug!(%peer, path = other, "acknowledged unsupported LeLink request");
            reply("")
        }
    }
}

async fn cast(state: &AppState, peer: SocketAddr, request: &str) -> String {
    let body = body_of(request);
    let Some(url) = text_parameter(body, "Content-Location") else {
        warn!(%peer, "LeLink /play had no Content-Location");
        return reply("");
    };
    let url_id = text_parameter(body, "Content-URLID").unwrap_or_default();
    start_cast(
        state,
        peer,
        request,
        url,
        url_id,
        body_start_position_ms(body),
    )
    .await
}

async fn cast_json(state: &AppState, peer: SocketAddr, request: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body_of(request)) else {
        warn!(%peer, "LeLink /send_videoInfo body was not JSON");
        return reply("");
    };
    let Some(url) = value.get("playUrl").and_then(|url| url.as_str()) else {
        warn!(%peer, "LeLink /send_videoInfo had no playUrl");
        return reply("");
    };
    let url_id = value
        .get("urlId")
        .and_then(|id| id.as_str())
        .unwrap_or_default()
        .to_owned();
    let position_ms = value
        .get("mStartPosition")
        .and_then(|position| position.as_u64())
        .unwrap_or(0)
        * 1000;
    start_cast(state, peer, request, url.to_owned(), url_id, position_ms).await
}

/// V1 carries no media title, and `Start-Position` shares the sender's seconds-based
/// seek vocabulary (`POST /scrub?position=` is logged by the caller as seconds).
fn body_start_position_ms(body: &str) -> u64 {
    text_parameter(body, "Start-Position")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        * 1000
}

async fn start_cast(
    state: &AppState,
    peer: SocketAddr,
    request: &str,
    url: String,
    url_id: String,
    position_ms: u64,
) -> String {
    let phone_session = header_value(request, "x-lelink-session-id").unwrap_or_default();
    let repeated = {
        let sink = state.lelink().await;
        sink.session_id.is_some()
            && sink.phone_session == phone_session
            && (url_id.is_empty() || sink.url_id == url_id)
    };
    if repeated {
        info!(%peer, "LeLink sender repeated its cast request; keeping the session");
        return reply("");
    }
    let session_id = Uuid::new_v4().simple().to_string();
    let epoch = state.begin_play_epoch(&session_id).await;
    let media = ResolvedMedia {
        source: MediaSource::Progressive { url: url.clone() },
        title: media_title(&url),
        quality: String::new(),
        available_qualities: Vec::new(),
        duration_ms: None,
        live: false,
    };
    let outcome = dlna::play(
        state.clone(),
        &session_id,
        SessionOrigin::Lelink,
        media,
        position_ms,
        epoch,
    )
    .await;
    state.complete_play_epoch(epoch).await;
    if let Err(error) = outcome {
        warn!(%peer, %error, "LeLink cast could not be relayed to the DLNA target");
        return reply("");
    }
    {
        let mut sink = state.lelink().await;
        sink.session_id = Some(session_id);
        sink.phone_session = phone_session;
        sink.url_id = url_id;
        sink.transport = TransportState::Playing;
    }
    info!(%peer, "LeLink cast relayed to the selected DLNA renderer");
    reply("")
}

fn media_title(url: &str) -> String {
    let file = url
        .split_once("?")
        .map_or(url, |(path, _)| path)
        .rsplit('/')
        .next()
        .unwrap_or_default();
    if file.is_empty() {
        return "乐播投屏".to_owned();
    }
    percent_decode(file)
}

/// The sender URL-escapes nothing in practice, but a stray `%20` would otherwise show
/// up verbatim in our own UI.
fn percent_decode(value: &str) -> String {
    if !value.contains('%') {
        return value.to_owned();
    }
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) = (
                (bytes[index + 1] as char).to_digit(16),
                (bytes[index + 2] as char).to_digit(16),
            )
        {
            out.push((high * 16 + low) as u8);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn active_session(state: &AppState) -> Option<String> {
    state.lelink().await.session_id.clone()
}

async fn pause(state: &AppState, peer: SocketAddr) -> String {
    let Some(session_id) = active_session(state).await else {
        return reply("");
    };
    match dlna::pause(state.clone(), &session_id).await {
        Ok(()) => {
            state.lelink().await.transport = TransportState::PausedPlayback;
            reply("")
        }
        Err(error) => failure(peer, "pause", error),
    }
}

async fn resume(state: &AppState, peer: SocketAddr) -> String {
    let Some(session_id) = active_session(state).await else {
        return reply("");
    };
    match dlna::resume(state.clone(), &session_id).await {
        Ok(()) => {
            state.lelink().await.transport = TransportState::Playing;
            reply("")
        }
        Err(error) => failure(peer, "resume", error),
    }
}

async fn stop(state: &AppState, peer: SocketAddr) -> String {
    let session_id = active_session(state).await;
    if let Err(error) = dlna::stop(state.clone(), session_id.as_deref()).await {
        return failure(peer, "stop", error);
    }
    let mut sink = state.lelink().await;
    sink.session_id = None;
    sink.url_id = String::new();
    sink.transport = TransportState::Stopped;
    reply("")
}

async fn seek(state: &AppState, peer: SocketAddr, target: &str) -> String {
    let Some(session_id) = active_session(state).await else {
        return reply("");
    };
    let Some(seconds) = query_param(target, "position").and_then(|value| value.parse::<u64>().ok())
    else {
        debug!(%peer, "LeLink /scrub had no parsable position");
        return reply("");
    };
    if let Err(error) = dlna::seek(state.clone(), &session_id, seconds * 1000).await {
        return failure(peer, "seek", error);
    }
    reply("")
}

async fn volume(state: &AppState, peer: SocketAddr, louder: bool) -> String {
    let Some(session_id) = active_session(state).await else {
        return reply("");
    };
    let volume = {
        let mut sink = state.lelink().await;
        sink.volume = if louder {
            sink.volume.saturating_add(VOLUME_STEP)
        } else {
            sink.volume.saturating_sub(VOLUME_STEP)
        };
        sink.volume
    };
    if let Err(error) = dlna::set_volume(state.clone(), &session_id, volume).await {
        return failure(peer, "volume", error);
    }
    reply("")
}

async fn progress(state: &AppState) -> String {
    let Some(session_id) = active_session(state).await else {
        return reply("");
    };
    let (position_ms, duration_ms) = dlna::position(state.clone(), &session_id)
        .await
        .unwrap_or((0, 0));
    // The sender reads the number after the last colon as the position and the one
    // before "position" as the duration, so these two lines are the whole contract.
    reply(&format!(
        "duration:{}\r\nposition:{}",
        duration_ms / 1000,
        position_ms / 1000
    ))
}

fn failure(peer: SocketAddr, what: &str, error: anyhow::Error) -> String {
    // A refused command must not look like a refusal to talk at all: the sender only
    // distinguishes "200" from everything else, and anything else ends the session.
    warn!(%peer, %error, "LeLink {what} could not be relayed to the DLNA target");
    reply("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn control_bind_keeps_an_available_preferred_port() {
        let reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let preferred = match reservation.local_addr().unwrap() {
            SocketAddr::V4(address) => address,
            SocketAddr::V6(_) => unreachable!("the test requested IPv4"),
        };
        drop(reservation);

        let (listener, preferred_error) = bind_control(preferred).await.unwrap();
        assert!(preferred_error.is_none());
        assert_eq!(listener.local_addr().unwrap(), SocketAddr::V4(preferred));
    }

    #[tokio::test]
    async fn control_bind_escapes_an_outbound_source_port_collision() {
        let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let preferred = match reservation.local_addr().unwrap() {
            SocketAddr::V4(address) => address,
            SocketAddr::V6(_) => unreachable!("the test requested IPv4"),
        };
        drop(reservation);

        let outbound = tokio::net::TcpSocket::new_v4().unwrap();
        outbound.bind(SocketAddr::V4(preferred)).unwrap();
        let (client, accepted) = tokio::join!(
            outbound.connect(server.local_addr().unwrap()),
            server.accept()
        );
        let _client = client.unwrap();
        let (_server_side, _) = accepted.unwrap();

        let (listener, preferred_error) = bind_control(preferred).await.unwrap();
        let actual = listener.local_addr().unwrap();
        assert_eq!(
            preferred_error.as_ref().map(io::Error::kind),
            Some(io::ErrorKind::AddrInUse)
        );
        assert_ne!(actual, SocketAddr::V4(preferred));
        assert_eq!(actual.ip(), SocketAddr::V4(preferred).ip());
        assert_ne!(actual.port(), 0);
    }

    #[test]
    fn a_play_body_is_read_as_name_value_lines_not_json() {
        let body = "Content-Location: http://192.0.2.99:8000/movie.mp4\r\n\
Start-Position: 12\r\nContent-URLID: url-1";
        assert_eq!(
            text_parameter(body, "Content-Location").as_deref(),
            Some("http://192.0.2.99:8000/movie.mp4"),
            "a URL keeps its own colons"
        );
        assert_eq!(
            text_parameter(body, "content-urlid").as_deref(),
            Some("url-1"),
            "the sender's header names vary in case"
        );
        assert_eq!(text_parameter(body, "Content-LocationX"), None);
        assert_eq!(body_start_position_ms(body), 12_000, "V1 seeks in seconds");
        assert_eq!(body_start_position_ms("Start-Position: abc"), 0);
        assert_eq!(body_start_position_ms(""), 0);
        assert_eq!(
            text_parameter("Content-Location:\r\nStart-Position: 1", "Content-Location"),
            None,
            "an empty value is no URL at all"
        );
    }

    #[test]
    fn a_query_is_read_from_the_path_the_sender_put_on_the_request_line() {
        let target = "/scrub?position=90";
        assert_eq!(path_of(target), "/scrub");
        assert_eq!(query_param(target, "position").as_deref(), Some("90"));
        assert_eq!(query_param("/scrub", "position"), None);
        assert_eq!(
            query_param("/rate?value=0.000000", "value").as_deref(),
            Some("0.000000")
        );
        assert_eq!(
            request_target("POST /rate?value=1.000000 HTTP/1.1"),
            "/rate?value=1.000000"
        );
    }

    #[test]
    fn a_title_falls_back_to_the_file_name_the_sender_offered() {
        assert_eq!(
            media_title("http://192.0.2.99:8000/vod/show.mp4?sign=abc"),
            "show.mp4",
            "a signed query is not part of the name"
        );
        assert_eq!(media_title("http://host/%E9%A3%8E.mp4"), "风.mp4");
        assert_eq!(
            media_title("http://host/a%2hb"),
            "a%2hb",
            "a stray escape survives"
        );
        assert_eq!(media_title("http://host/"), "乐播投屏");
    }

    #[test]
    fn a_reply_is_bare_and_carries_the_exact_body_length() {
        assert_eq!(
            reply("duration:296\r\nposition:83"),
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 25\r\n\
Connection: keep-alive\r\n\r\nduration:296\r\nposition:83"
        );
        assert!(reply("").contains("Content-Length: 0"));
    }

    #[test]
    fn a_frame_is_split_off_a_keep_alive_stream_and_the_remainder_is_kept() {
        let mut buffer = Vec::new();
        assert!(find_header_end(&buffer).is_none());

        buffer.extend_from_slice(
            b"GET /feedback HTTP/1.1\r\nContent-Length: 4\r\n\r\nABCDGET /stop HTT",
        );
        let head = find_header_end(&buffer).expect("a finished header");
        assert_eq!(content_length(&buffer[..head]), Some(4));
        let first = {
            let end = head + 4 + 4;
            let request = String::from_utf8_lossy(&buffer[..end]).into_owned();
            buffer.drain(..end);
            request
        };
        assert!(first.ends_with("ABCD"), "{first}");
        assert_eq!(body_of(&first), "ABCD");
        assert!(
            String::from_utf8_lossy(&buffer).starts_with("GET /stop HTT"),
            "the next request must survive the read that delivered both"
        );
        assert_eq!(body_of("no body here"), "");
        assert_eq!(header_value(&first, "content-length").as_deref(), Some("4"));
        assert_eq!(header_value(&first, "nope"), None);
    }
}
