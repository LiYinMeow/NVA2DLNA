use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{StreamExt, stream};
use reqwest::{Client, Url, redirect::Policy};
use roxmltree::{Document, Node};
use serde_json::json;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, UdpSocket},
    sync::{RwLock, Semaphore},
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Host;
use uuid::Uuid;

use crate::{
    bilibili::{MediaSource, ResolvedMedia},
    dmr, lelink_discovery,
    network::InterfaceAddress,
    state::{
        AppState, HlsResourceStore, LelinkEndpoint, MAX_FFMPEG_CONSUMERS, MediaEntry, MediaInput,
        NvaEvent, PlaybackClock, RemuxFormat, RemuxProgress, Renderer, SessionBackend,
        SessionOrigin, SessionView, SupersededPlay, now_ms,
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
/// A rate list is a nice-to-have, so it must never be what makes discovery feel slow.
const SCPD_FETCH_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PLAY_SPEEDS: usize = 32;
const MAX_SOAP_RESPONSE_BYTES: usize = 256 * 1024;
/// Observed on the wire from the LeLink sender: SetAVTransportURI and Play are
/// repeated five times at 500 ms spacing, because a TV that is still waking up
/// usually refuses the first attempt.
const CAST_ATTEMPTS: u8 = 5;
const CAST_RETRY_DELAY: Duration = Duration::from_millis(500);
const LELINK_IO_TIMEOUT: Duration = Duration::from_secs(3);
const LELINK_RATES: [f64; 7] = [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0];
const MAX_LELINK_RESPONSE_BYTES: usize = 64 * 1024;
const LELINK_GET_PROGRESS: u8 = 0x18;
const LELINK_PROGRESS: u8 = 0x4c;
const LELINK_PROGRESS_FRAME_LEN: usize = 13;

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
    let interfaces = state.discovery_interfaces().await?;
    if interfaces.is_empty() {
        bail!("没有可用于发现的 IPv4 网卡；请检查扫描网卡和 Web 监听地址设置");
    }
    let scans = stream::iter(interfaces)
        .map(|interface| async move {
            let id = interface.id.clone();
            let address = interface.address;
            (id, address, scan_interface(state, interface).await)
        })
        .buffer_unordered(8);
    tokio::pin!(scans);
    let mut merged = HashMap::<String, Renderer>::new();
    let mut successful_interfaces = 0_usize;
    let mut failures = Vec::new();
    while let Some((id, address, result)) = scans.next().await {
        match result {
            Ok(renderers) => {
                successful_interfaces += 1;
                for renderer in renderers {
                    merge_renderer(&mut merged, renderer);
                }
            }
            Err(error) => {
                warn!(interface = %id, %address, %error, "output discovery failed on interface");
                failures.push(format!("{id} ({address}): {error}"));
            }
        }
    }
    if successful_interfaces == 0 {
        bail!("所有扫描网卡均不可用: {}", failures.join("; "));
    }
    let mut renderers = merged.into_values().collect::<Vec<_>>();
    renderers.sort_by(|left, right| {
        left.friendly_name
            .to_lowercase()
            .cmp(&right.friendly_name.to_lowercase())
            .then_with(|| left.udn.to_lowercase().cmp(&right.udn.to_lowercase()))
    });
    info!(
        count = renderers.len(),
        interfaces = successful_interfaces,
        "multi-interface output discovery completed"
    );
    Ok(renderers)
}

async fn scan_interface(state: &AppState, interface: InterfaceAddress) -> Result<Vec<Renderer>> {
    // mDNS and SSDP are independent multicast protocols. Starting both windows
    // together keeps a manual scan at the existing latency instead of doubling it.
    let socket = ssdp_discovery_socket(interface.address)?;
    let lelink_scan = tokio::spawn(lelink_discovery::scan(interface.address));
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
        .map(|(location, discovered_ip)| {
            let interface = interface.clone();
            async move {
                let result = fetch_renderer(state, &location, discovered_ip, &interface).await;
                (location, result)
            }
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
    let lelink_count = match lelink_scan.await {
        Ok(Ok(peers)) => lelink_discovery::attach(&mut renderers, peers),
        Ok(Err(error)) => {
            debug!(%error, "LeLink discovery was unavailable");
            0
        }
        Err(error) => {
            debug!(%error, "LeLink discovery task failed");
            0
        }
    };
    info!(
        interface = %interface.id,
        local_address = %interface.address,
        count = renderers.len(),
        lelink_count, "output discovery completed"
    );
    Ok(renderers)
}

fn ssdp_discovery_socket(interface: Ipv4Addr) -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
        .context("cannot create DLNA discovery socket")?;
    socket
        .bind(&SocketAddr::from((interface, 0)).into())
        .with_context(|| format!("cannot bind DLNA discovery socket on {interface}"))?;
    socket
        .set_multicast_if_v4(&interface)
        .with_context(|| format!("cannot select DLNA multicast interface {interface}"))?;
    socket.set_broadcast(true)?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into()).context("cannot create async DLNA discovery socket")
}

fn merge_renderer(renderers: &mut HashMap<String, Renderer>, candidate: Renderer) {
    let key = candidate.udn.trim().to_ascii_lowercase();
    match renderers.entry(key) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(candidate);
        }
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            if prefer_route(&candidate, entry.get()) {
                entry.insert(candidate);
            }
        }
    }
}

/// Prefer a directly connected route, then use stable adapter/address ordering so
/// concurrent scan completion cannot randomly change the gateway address.
fn prefer_route(candidate: &Renderer, current: &Renderer) -> bool {
    let candidate_direct = renderer_is_on_link(candidate);
    let current_direct = renderer_is_on_link(current);
    candidate_direct && !current_direct
        || (candidate_direct == current_direct
            && (
                candidate.discovery_interface_id.as_str(),
                candidate.gateway_address.as_str(),
            ) < (
                current.discovery_interface_id.as_str(),
                current.gateway_address.as_str(),
            ))
}

fn renderer_is_on_link(renderer: &Renderer) -> bool {
    let Ok(target) = renderer.address.parse::<Ipv4Addr>() else {
        return false;
    };
    let Ok(gateway) = renderer.gateway_address.parse::<Ipv4Addr>() else {
        return false;
    };
    let prefix = renderer.gateway_prefix_length.min(32);
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    u32::from(target) & mask == u32::from(gateway) & mask
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
    interface: &InterfaceAddress,
) -> Result<Option<Renderer>> {
    let location_url = Url::parse(location).context("DLNA LOCATION is invalid")?;
    let client = pinned_dlna_client(&location_url, discovered_ip, IpAddr::V4(interface.address))?;
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
    if is_self_advertised(state, &udn) {
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
        let scpd = child_text(service, "SCPDURL")
            .and_then(|value| {
                base.join(&value)
                    .or_else(|_| location_url.join(&value))
                    .ok()
            })
            .filter(|url| validate_dlna_url(url, discovered_ip).is_ok())
            .map(|url| url.to_string());
        if service_type.contains(":service:AVTransport:") {
            av_transport = Some((service_type, control.to_string(), scpd));
        } else if service_type.contains(":service:RenderingControl:") {
            rendering_control = Some((service_type, control.to_string()));
        }
    }
    let (av_transport_service_type, av_transport_url, av_transport_scpd_url) =
        av_transport.context("MediaRenderer has no AVTransport service")?;
    let play_speeds = match av_transport_scpd_url.as_deref() {
        Some(scpd) => fetch_play_speeds(scpd, discovered_ip, interface.address).await,
        None => Vec::new(),
    };
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
        av_transport_scpd_url,
        rendering_control_url: rendering_control.as_ref().map(|(_, url)| url.clone()),
        rendering_control_service_type: rendering_control.map(|(kind, _)| kind),
        play_speeds,
        lelink: None,
        address,
        gateway_address: interface.address.to_string(),
        discovery_interface_id: interface.id.clone(),
        gateway_prefix_length: interface.prefix_length,
        last_seen_unix_ms: now_ms(),
    }))
}

/// Rates a target says it can play. Whether `1.5` is a request or a fault is a
/// per-device property that only the device's own description can answer, so a
/// discovery miss here stays silent and the caller treats it as "unknown".
async fn fetch_play_speeds(
    location: &str,
    discovered_ip: IpAddr,
    local_address: Ipv4Addr,
) -> Vec<String> {
    let Ok(url) = Url::parse(location) else {
        return Vec::new();
    };
    let Ok(client) = pinned_dlna_client(&url, discovered_ip, IpAddr::V4(local_address)) else {
        return Vec::new();
    };
    let response = match client.get(url).timeout(SCPD_FETCH_TIMEOUT).send().await {
        Ok(response) => response,
        Err(error) => {
            debug!(%error, %location, "cannot fetch AVTransport description");
            return Vec::new();
        }
    };
    if !response.status().is_success() {
        return Vec::new();
    }
    let bytes = match read_limited_response(response, MAX_DESCRIPTION_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return Vec::new(),
    };
    let Ok(xml) = std::str::from_utf8(&bytes) else {
        return Vec::new();
    };
    play_speeds_from_scpd(xml)
}

fn play_speeds_from_scpd(xml: &str) -> Vec<String> {
    let Ok(document) = Document::parse(xml) else {
        return Vec::new();
    };
    let Some(variable) = document
        .descendants()
        .filter(|node| node.is_element() && node.tag_name().name() == "stateVariable")
        .find(|node| child_text(*node, "name").as_deref() == Some("TransportPlaySpeed"))
    else {
        return Vec::new();
    };
    variable
        .descendants()
        .filter(|node| node.is_element() && node.tag_name().name() == "allowedValue")
        .filter_map(|node| node.text().map(|text| text.trim().to_owned()))
        .filter(|text| !text.is_empty())
        .take(MAX_PLAY_SPEEDS)
        .collect()
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

fn pinned_dlna_client(url: &Url, discovered_ip: IpAddr, local_address: IpAddr) -> Result<Client> {
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
        .local_address(local_address)
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

/// Whether a discovered device is one of the two we advertise ourselves. Selecting
/// it as a cast target would bridge the media straight back into our own sink.
fn is_self_advertised(state: &AppState, udn: &str) -> bool {
    [
        format!("uuid:{}", upnp::nva_tv_id(state.nva_device_uuid())),
        format!("uuid:{}", dmr::udn(state)),
    ]
    .iter()
    .any(|own| udn.eq_ignore_ascii_case(own))
}

pub async fn play(
    state: AppState,
    session_id: &str,
    origin: SessionOrigin,
    media: ResolvedMedia,
    seek_position_ms: u64,
    play_epoch: u64,
) -> Result<()> {
    let _guard = state.operation().await;
    state.ensure_play_epoch(play_epoch)?;
    if let Some(previous) = state.session().await {
        if let Some(previous_renderer) = state.renderer(&previous.target_udn).await {
            let _ = stop_renderer(&previous_renderer, previous.backend, &previous.id).await;
        }
        state.ensure_play_epoch(play_epoch)?;
        state.revoke_media_for(&previous.id).await;
        if previous.id != session_id {
            state.mark_session_terminated(&previous.id).await;
            if previous.origin == SessionOrigin::Nva {
                state.emit_nva(NvaEvent {
                    session_id: previous.id.clone(),
                    method: "OnPlayState".into(),
                    params: Some(json!({"playState": 7})),
                    close_after: true,
                });
            }
            state.announce_closed(&previous.id, previous.origin);
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
        renderer.gateway_address,
        state.web_port()
    );
    let ffmpeg_input = matches!(&input, MediaInput::Dash { .. } | MediaInput::Remux { .. });
    let start_offset_ms = if ffmpeg_input && !is_live {
        media
            .duration_ms
            .map_or(seek_position_ms, |duration| seek_position_ms.min(duration))
    } else {
        0
    };
    // FFmpeg rebases a remuxed transport stream to zero. Passing the same seek
    // to the target would apply the requested offset for a second time.
    let renderer_seek_position_ms = if ffmpeg_input { 0 } else { seek_position_ms };
    let media_entry = MediaEntry {
        token: token.clone(),
        owner_session: session_id.to_owned(),
        input,
        mime: mime.to_owned(),
        duration_ms: media.duration_ms,
        start_offset_ms,
        created_unix_ms: now_ms(),
        allowed_renderer_ip: renderer.address.clone(),
        gateway_address: renderer.gateway_address.clone(),
        cancellation: cancellation.clone(),
        ffmpeg_consumers: Arc::new(Semaphore::new(MAX_FFMPEG_CONSUMERS)),
        remux_progress: Arc::new(RemuxProgress::default()),
        playback_clock: Arc::new(PlaybackClock::new(start_offset_ms)),
        hls_resources: Arc::new(RwLock::new(HlsResourceStore::default())),
    };
    let session_view = SessionView {
        id: session_id.to_owned(),
        origin,
        title: media.title.clone(),
        phase: "connecting".into(),
        quality: media.quality.clone(),
        speed: "1".into(),
        input: input_label.into(),
        output: output_label.into(),
        backend: SessionBackend::Dlna,
        target_name: renderer.friendly_name.clone(),
        target_udn: renderer.udn.clone(),
        started_unix_ms: now_ms(),
        error: None,
        live: is_live,
    };

    let result: Result<SessionBackend> = async {
        state.register_media(media_entry).await;
        state.set_session(Some(session_view)).await;
        state.ensure_play_epoch(play_epoch)?;
        if renderer
            .lelink
            .as_ref()
            .and_then(|endpoint| endpoint.main_port)
            .is_some()
        {
            match lelink_v1_play(
                &renderer,
                session_id,
                &media_url,
                &token,
                renderer_seek_position_ms,
            )
            .await
            {
                Ok(()) => {
                    state.ensure_play_epoch(play_epoch)?;
                    let _ = state
                        .update_session_backend_if(session_id, SessionBackend::LelinkV1)
                        .await;
                    let _ = state
                        .update_session_phase_if(session_id, "playing", None)
                        .await;
                    return Ok(SessionBackend::LelinkV1);
                }
                Err(error) => {
                    warn!(%error, "native LeLink /play failed; falling back to DLNA");
                    state.ensure_play_epoch(play_epoch)?;
                }
            }
        }
        let renderer_duration_ms = media
            .duration_ms
            .map(|duration| duration.saturating_sub(start_offset_ms));
        let metadata = upnp::didl(&media.title, &media_url, mime, renderer_duration_ms);
        let set_uri = soap_call_retried(
            &state,
            &renderer,
            "SetAVTransportURI",
            &format!(
                "<InstanceID>0</InstanceID><CurrentURI>{}</CurrentURI>\
<CurrentURIMetaData>{}</CurrentURIMetaData>",
                upnp::xml_escape(&media_url),
                upnp::xml_escape(&metadata)
            ),
            play_epoch,
        )
        .await;
        state.ensure_play_epoch(play_epoch)?;
        set_uri?;
        let play = soap_call_retried(
            &state,
            &renderer,
            "Play",
            "<InstanceID>0</InstanceID><Speed>1</Speed>",
            play_epoch,
        )
        .await;
        state.ensure_play_epoch(play_epoch)?;
        play?;
        if renderer_seek_position_ms > 0 && !is_live {
            // Many DMRs reject Seek while still STOPPED even after accepting
            // SetAVTransportURI. Start first, then apply the resume position.
            time::sleep(Duration::from_millis(150)).await;
            if let Err(error) = seek_inner(&renderer, renderer_seek_position_ms).await {
                warn!(%error, "DLNA target rejected the initial resume position");
            }
        }
        state.ensure_play_epoch(play_epoch)?;
        Ok(SessionBackend::Dlna)
    }
    .await;

    match result {
        Ok(SessionBackend::Dlna) => {
            if let Some(entry) = state.media(&token).await {
                entry.playback_clock.mark_playing(1.0);
            }
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
        Ok(SessionBackend::LelinkV1) => {
            if let Some(entry) = state.media(&token).await {
                entry.playback_clock.mark_playing(1.0);
            }
            state.emit_nva(NvaEvent {
                session_id: session_id.to_owned(),
                method: "OnPlayState".into(),
                params: Some(json!({"playState": 4})),
                close_after: false,
            });
            state.emit_nva(NvaEvent {
                session_id: session_id.to_owned(),
                method: "PLAY_SUCCESS".into(),
                params: None,
                close_after: false,
            });
            spawn_lelink_monitor(
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
            state.revoke_media_for(session_id).await;
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
    let _guard = state.operation().await;
    let (renderer, backend) = current_route_for_session(&state, session_id).await?;
    match backend {
        SessionBackend::Dlna => {
            soap_call(
                &renderer.av_transport_url,
                &renderer.av_transport_service_type,
                "Pause",
                "<InstanceID>0</InstanceID>",
                &renderer.address,
                &renderer.gateway_address,
            )
            .await?;
        }
        SessionBackend::LelinkV1 => {
            lelink_v1_request(&renderer, session_id, "POST", "/rate?value=0.000000", "").await?;
        }
    }
    let _ = state
        .update_session_phase_if(session_id, "paused", None)
        .await;
    if let Some(entry) = state.media_for_owner(session_id).await {
        entry.playback_clock.mark_paused();
    }
    state.emit_nva(NvaEvent {
        session_id: session_id.into(),
        method: "OnPlayState".into(),
        params: Some(json!({"playState": 5})),
        close_after: false,
    });
    Ok(())
}

pub async fn resume(state: AppState, session_id: &str) -> Result<()> {
    let _guard = state.operation().await;
    let speed = session_speed(&state, session_id).await;
    let (renderer, backend) = current_route_for_session(&state, session_id).await?;
    match backend {
        SessionBackend::LelinkV1 => {
            lelink_v1_request(&renderer, session_id, "POST", "/rate?value=1.000000", "").await?;
            if speed != "1"
                && let Some((endpoint, rate_index)) = lelink_rate_target(&renderer, &speed)
            {
                send_lelink_rate(endpoint, &renderer.gateway_address, rate_index).await?;
            }
        }
        SessionBackend::Dlna => {
            soap_call(
                &renderer.av_transport_url,
                &renderer.av_transport_service_type,
                "Play",
                &format!(
                    "<InstanceID>0</InstanceID><Speed>{}</Speed>",
                    upnp::xml_escape(if speed.is_empty() { "1" } else { &speed })
                ),
                &renderer.address,
                &renderer.gateway_address,
            )
            .await?;
        }
    }
    mark_playing(&state, session_id, &speed).await;
    Ok(())
}

/// AVTransport has no speed action of its own: resending `Play` with a different
/// `Speed` argument is how a rate change is signalled, and it doubles as a resume.
/// Whether a deliberate change is worth sending is the caller's call, made with
/// [`target_accepts_speed`]; a resume must never be gated, or a refused rate would
/// strand the cast paused.
pub async fn set_speed(state: AppState, session_id: &str, speed: &str) -> Result<()> {
    let _guard = state.operation().await;
    let (renderer, backend) = current_route_for_session(&state, session_id).await?;
    match backend {
        SessionBackend::LelinkV1 => {
            let (endpoint, rate_index) = lelink_rate_target(&renderer, speed)
                .context("LeLink target did not publish a confirmed Telecontrol rate endpoint")?;
            send_lelink_rate(endpoint, &renderer.gateway_address, rate_index).await?;
        }
        SessionBackend::Dlna => {
            soap_call(
                &renderer.av_transport_url,
                &renderer.av_transport_service_type,
                "Play",
                &format!(
                    "<InstanceID>0</InstanceID><Speed>{}</Speed>",
                    upnp::xml_escape(if speed.is_empty() { "1" } else { speed })
                ),
                &renderer.address,
                &renderer.gateway_address,
            )
            .await?;
        }
    }
    mark_playing(&state, session_id, speed).await;
    Ok(())
}

/// Whether the selected route can accept this rate. Once native `/play` has fallen
/// back to DLNA, Telecontrol must not be advertised or used for that session.
pub async fn target_accepts_speed(state: &AppState, speed: &str) -> bool {
    let Ok(renderer) = state.selected_renderer().await else {
        return false;
    };
    let active_backend = state
        .session()
        .await
        .filter(|session| session.target_udn == renderer.udn)
        .map(|session| session.backend);
    route_accepts_speed(&renderer, speed, active_backend)
}

fn route_accepts_speed(
    renderer: &Renderer,
    speed: &str,
    active_backend: Option<SessionBackend>,
) -> bool {
    match active_backend {
        Some(SessionBackend::Dlna) => renderer.accepts_speed(speed),
        Some(SessionBackend::LelinkV1) => lelink_rate_target(renderer, speed).is_some(),
        None => lelink_rate_target(renderer, speed).is_some() || renderer.accepts_speed(speed),
    }
}

fn lelink_rate_target<'a>(renderer: &'a Renderer, speed: &str) -> Option<(&'a LelinkEndpoint, u8)> {
    let endpoint = renderer.lelink.as_ref()?;
    endpoint.control_port?;
    Some((endpoint, telecontrol_rate_index(speed)?))
}

/// LeLink Telecontrol uses a compact enumeration, not an arbitrary floating point
/// value. These are the exact seven values implemented by the TV APK.
fn telecontrol_rate_index(speed: &str) -> Option<u8> {
    let speed = speed.trim().parse::<f64>().ok()?;
    LELINK_RATES
        .iter()
        .position(|candidate| (candidate - speed).abs() < 0.001)
        .and_then(|index| u8::try_from(index).ok())
}

async fn send_lelink_rate(
    endpoint: &LelinkEndpoint,
    gateway_address: &str,
    rate_index: u8,
) -> Result<()> {
    send_lelink_rate_once(endpoint, gateway_address, rate_index).await
}

async fn send_lelink_rate_once(
    endpoint: &LelinkEndpoint,
    gateway_address: &str,
    rate_index: u8,
) -> Result<()> {
    let requested_rate = LELINK_RATES
        .get(usize::from(rate_index))
        .copied()
        .context("LeLink rate index is outside the supported range")?;
    let port = endpoint
        .control_port
        .context("LeLink target did not publish its Telecontrol port")?;
    let address = endpoint
        .address
        .parse::<IpAddr>()
        .context("LeLink Telecontrol address is invalid")?;
    let destination = SocketAddr::new(address, port);
    let local_address = gateway_address
        .parse::<Ipv4Addr>()
        .context("LeLink gateway address is invalid")?;
    let socket = TcpSocket::new_v4().context("cannot create LeLink Telecontrol socket")?;
    socket
        .bind(SocketAddr::new(IpAddr::V4(local_address), 0))
        .with_context(|| format!("cannot bind LeLink Telecontrol to {local_address}"))?;
    // tv820 firmware applies SET_RATE even though its 0x48 confirmation path is
    // unreachable. Do not block the phone command on queries the TV cannot answer.
    let set_frame = [0_u8, 0, 0, 6, 0x12, rate_index];
    time::timeout(LELINK_IO_TIMEOUT, async {
        let mut stream = socket
            .connect(destination)
            .await
            .with_context(|| format!("cannot connect to LeLink Telecontrol at {destination}"))?;
        stream
            .write_all(&set_frame)
            .await
            .context("cannot write LeLink Telecontrol SET_RATE")?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("LeLink Telecontrol SET_RATE operation timed out")??;
    debug!(
        %destination,
        rate_index,
        requested_rate,
        "LeLink Telecontrol SET_RATE written; confirmation is unavailable on this receiver"
    );
    Ok(())
}

/// Query the actual LeLink player timeline over Telecontrol.
///
/// The V1 HTTP `/scrub` endpoint is an AirPlay compatibility surface and is not
/// authoritative on every receiver.  The TV APK's native contract is a five-byte
/// GET_PROGRESS (0x18) frame followed by a 13-byte PROGRESS (0x4c) frame carrying
/// duration and position as big-endian signed millisecond integers.
async fn query_lelink_progress(
    endpoint: &LelinkEndpoint,
    gateway_address: &str,
) -> Result<(u64, u64)> {
    let port = endpoint
        .control_port
        .context("LeLink target did not publish its Telecontrol port")?;
    let address = endpoint
        .address
        .parse::<IpAddr>()
        .context("LeLink Telecontrol address is invalid")?;
    let destination = SocketAddr::new(address, port);
    let local_address = gateway_address
        .parse::<Ipv4Addr>()
        .context("LeLink gateway address is invalid")?;
    let socket = TcpSocket::new_v4().context("cannot create LeLink Telecontrol socket")?;
    socket
        .bind(SocketAddr::new(IpAddr::V4(local_address), 0))
        .with_context(|| format!("cannot bind LeLink Telecontrol to {local_address}"))?;

    time::timeout(LELINK_IO_TIMEOUT, async {
        let mut stream = socket
            .connect(destination)
            .await
            .with_context(|| format!("cannot connect to LeLink Telecontrol at {destination}"))?;
        stream
            .write_all(&[0, 0, 0, 5, LELINK_GET_PROGRESS])
            .await
            .context("cannot write LeLink Telecontrol GET_PROGRESS")?;

        let mut length = [0_u8; 4];
        stream
            .read_exact(&mut length)
            .await
            .context("cannot read LeLink Telecontrol progress length")?;
        let frame_len = usize::try_from(u32::from_be_bytes(length))
            .context("LeLink Telecontrol progress length is not representable")?;
        if frame_len != LELINK_PROGRESS_FRAME_LEN {
            bail!(
                "LeLink Telecontrol returned progress frame length {frame_len}, expected {LELINK_PROGRESS_FRAME_LEN}"
            );
        }
        let mut payload = [0_u8; LELINK_PROGRESS_FRAME_LEN - 4];
        stream
            .read_exact(&mut payload)
            .await
            .context("cannot read LeLink Telecontrol progress payload")?;
        parse_lelink_progress_payload(&payload)
    })
    .await
    .context("LeLink Telecontrol GET_PROGRESS operation timed out")?
}

fn parse_lelink_progress_payload(payload: &[u8]) -> Result<(u64, u64)> {
    if payload.len() != LELINK_PROGRESS_FRAME_LEN - 4 {
        bail!(
            "LeLink Telecontrol progress payload has length {}, expected {}",
            payload.len(),
            LELINK_PROGRESS_FRAME_LEN - 4
        );
    }
    if payload[0] != LELINK_PROGRESS {
        bail!(
            "LeLink Telecontrol returned command 0x{:02x}, expected PROGRESS 0x{LELINK_PROGRESS:02x}",
            payload[0]
        );
    }
    let duration_ms = i32::from_be_bytes(payload[1..5].try_into().unwrap());
    let position_ms = i32::from_be_bytes(payload[5..9].try_into().unwrap());
    if duration_ms < 0 || position_ms < 0 {
        bail!(
            "LeLink Telecontrol returned negative progress duration={duration_ms} position={position_ms}"
        );
    }
    Ok((position_ms as u64, duration_ms as u64))
}

async fn lelink_v1_play(
    renderer: &Renderer,
    session_id: &str,
    media_url: &str,
    media_token: &str,
    position_ms: u64,
) -> Result<()> {
    let body = format!(
        "Content-Location: {media_url}\r\nStart-Position: {}\r\nContent-URLID: {media_token}\r\n\r\n",
        position_ms / 1000
    );
    lelink_v1_request(renderer, session_id, "POST", "/play", &body)
        .await
        .map(|_| ())
}

async fn stop_renderer(
    renderer: &Renderer,
    backend: SessionBackend,
    session_id: &str,
) -> Result<()> {
    match backend {
        SessionBackend::Dlna => {
            soap_call(
                &renderer.av_transport_url,
                &renderer.av_transport_service_type,
                "Stop",
                "<InstanceID>0</InstanceID>",
                &renderer.address,
                &renderer.gateway_address,
            )
            .await
        }
        SessionBackend::LelinkV1 => lelink_v1_request(renderer, session_id, "POST", "/stop", "")
            .await
            .map(|_| ()),
    }
}

async fn lelink_v1_request(
    renderer: &Renderer,
    session_id: &str,
    method: &str,
    target: &str,
    body: &str,
) -> Result<String> {
    if session_id.is_empty()
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b':')
    {
        bail!("LeLink session id is not a safe HTTP header value");
    }
    if !target.starts_with('/') || target.bytes().any(|byte| byte.is_ascii_whitespace()) {
        bail!("LeLink request target is invalid");
    }
    let endpoint = renderer
        .lelink
        .as_ref()
        .context("renderer has no LeLink endpoint")?;
    let port = endpoint
        .main_port
        .context("LeLink target did not publish its main port")?;
    let address = endpoint
        .address
        .parse::<IpAddr>()
        .context("LeLink main address is invalid")?;
    let destination = SocketAddr::new(address, port);
    let local_address = renderer
        .gateway_address
        .parse::<Ipv4Addr>()
        .context("LeLink gateway address is invalid")?;
    let socket = TcpSocket::new_v4().context("cannot create LeLink V1 socket")?;
    socket
        .bind(SocketAddr::new(IpAddr::V4(local_address), 0))
        .with_context(|| format!("cannot bind LeLink V1 to {local_address}"))?;
    let mut stream = time::timeout(LELINK_IO_TIMEOUT, socket.connect(destination))
        .await
        .context("LeLink V1 connection timed out")?
        .with_context(|| format!("cannot connect to LeLink V1 at {destination}"))?;
    let request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {destination}\r\nUser-Agent: MediaControl/1.0\r\n\
X-LeLink-Session-ID: {session_id}\r\nX-LeLink-Platform: Android\r\n\
Content-Type: text/parameters\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    time::timeout(LELINK_IO_TIMEOUT, stream.write_all(request.as_bytes()))
        .await
        .context("LeLink V1 request write timed out")?
        .context("cannot write LeLink V1 request")?;
    let (status, response_body) =
        time::timeout(LELINK_IO_TIMEOUT, read_lelink_response(&mut stream))
            .await
            .context("LeLink V1 response timed out")??;
    if !(200..300).contains(&status) {
        let summary = response_body.chars().take(300).collect::<String>();
        bail!("LeLink V1 {method} {target} returned HTTP {status}: {summary}");
    }
    Ok(response_body)
}

async fn read_lelink_response(stream: &mut tokio::net::TcpStream) -> Result<(u16, String)> {
    let mut response = Vec::new();
    let mut expected_len: Option<(usize, usize)> = None;
    loop {
        if let Some((head_end, body_len)) = expected_len
            && response.len() >= head_end.saturating_add(body_len)
        {
            break;
        }
        let mut chunk = [0_u8; 4096];
        let read = stream
            .read(&mut chunk)
            .await
            .context("cannot read LeLink V1 response")?;
        if read == 0 {
            break;
        }
        if response.len().saturating_add(read) > MAX_LELINK_RESPONSE_BYTES {
            bail!("LeLink V1 response is too large");
        }
        response.extend_from_slice(&chunk[..read]);
        if expected_len.is_none()
            && let Some(head_end) = response.windows(4).position(|bytes| bytes == b"\r\n\r\n")
        {
            let body_start = head_end + 4;
            let head = std::str::from_utf8(&response[..head_end])
                .context("LeLink V1 response headers are not UTF-8")?;
            let body_len = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if body_start.saturating_add(body_len) > MAX_LELINK_RESPONSE_BYTES {
                bail!("LeLink V1 response body is too large");
            }
            expected_len = Some((body_start, body_len));
        }
    }
    let head_end = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .context("LeLink V1 response has no complete HTTP headers")?;
    let head = std::str::from_utf8(&response[..head_end])
        .context("LeLink V1 response headers are not UTF-8")?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .context("LeLink V1 response has no valid HTTP status")?;
    let body_start = head_end + 4;
    let body_len = expected_len.map(|(_, len)| len).unwrap_or(0);
    let body_end = body_start.saturating_add(body_len);
    if response.len() < body_end {
        bail!("LeLink V1 response body was truncated");
    }
    let body = String::from_utf8(response[body_start..body_end].to_vec())
        .context("LeLink V1 response body is not UTF-8")?;
    Ok((status, body))
}

fn parse_lelink_scrub(body: &str) -> Result<(u64, u64)> {
    let seconds = |name: &str| {
        body.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim().eq_ignore_ascii_case(name).then(|| {
                value
                    .trim()
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite() && *value >= 0.0)
                    .map(|value| (value * 1000.0).round() as u64)
            })?
        })
    };
    Ok((
        seconds("position").context("LeLink scrub response omitted position")?,
        seconds("duration").context("LeLink scrub response omitted duration")?,
    ))
}

async fn lelink_position(renderer: &Renderer, session_id: &str) -> Result<(u64, u64)> {
    if let Some(endpoint) = renderer
        .lelink
        .as_ref()
        .filter(|endpoint| endpoint.control_port.is_some())
    {
        return query_lelink_progress(endpoint, &renderer.gateway_address).await;
    }
    let body = lelink_v1_request(renderer, session_id, "GET", "/scrub", "").await?;
    parse_lelink_scrub(&body)
}

async fn mark_playing(state: &AppState, session_id: &str, speed: &str) {
    let _ = state.update_session_speed_if(session_id, speed).await;
    let _ = state
        .update_session_phase_if(session_id, "playing", None)
        .await;
    if let Some(entry) = state.media_for_owner(session_id).await {
        entry
            .playback_clock
            .mark_playing(speed.trim().parse().unwrap_or(1.0));
    }
    state.emit_nva(NvaEvent {
        session_id: session_id.into(),
        method: "OnPlayState".into(),
        params: Some(json!({"playState": 4})),
        close_after: false,
    });
}

async fn session_speed(state: &AppState, session_id: &str) -> String {
    state
        .session()
        .await
        .filter(|session| session.id == session_id)
        .map(|session| session.speed)
        .filter(|speed| !speed.is_empty())
        .unwrap_or_else(|| "1".into())
}

pub async fn stop(state: AppState, owner_session: Option<&str>) -> Result<()> {
    state.cancel_pending_play_for(owner_session).await;
    let _guard = state.operation().await;
    // A replacement can register its epoch while Stop is waiting for the
    // operation lock. Cancel again under the lock so it cannot revive playback
    // after this Stop completes.
    state.cancel_pending_play_for(owner_session).await;
    stop_locked(&state, owner_session).await
}

async fn stop_locked(state: &AppState, owner_session: Option<&str>) -> Result<()> {
    let session = state.session().await;
    if let (Some(owner), Some(session)) = (owner_session, session.as_ref())
        && session.id != owner
    {
        debug!(caller = %owner.chars().take(8).collect::<String>(), active = %session.id.chars().take(8).collect::<String>(), "ignored Stop from a stale cast session");
        return Ok(());
    }
    let renderer = match session.as_ref() {
        Some(session) => state.renderer(&session.target_udn).await,
        None => None,
    };
    let origin = session.as_ref().map(|session| session.origin);
    let backend = session
        .as_ref()
        .map(|session| session.backend)
        .unwrap_or_default();
    let ended_session = owner_session
        .map(str::to_owned)
        .or_else(|| session.as_ref().map(|session| session.id.clone()));
    if let Some(session_id) = ended_session.as_deref() {
        state.revoke_media_for(session_id).await;
    }
    state.set_session(None).await;
    if let Some(session_id) = ended_session.as_deref() {
        state.mark_session_terminated(session_id).await;
    }
    let result = if let Some(renderer) = renderer {
        stop_renderer(
            &renderer,
            backend,
            ended_session.as_deref().unwrap_or_default(),
        )
        .await
    } else {
        Ok(())
    };
    if let (Some(session_id), Some(origin)) = (ended_session, origin) {
        if origin == SessionOrigin::Nva {
            state.emit_nva(NvaEvent {
                session_id: session_id.clone(),
                method: "OnPlayState".into(),
                params: Some(json!({"playState": 7})),
                close_after: true,
            });
        }
        state.announce_closed(&session_id, origin);
    }
    result
}

pub async fn select_target(state: AppState, udn: String) -> Result<()> {
    state.cancel_pending_play_for(None).await;
    let _guard = state.operation().await;
    state.cancel_pending_play_for(None).await;
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
    let operation_guard = state.operation().await;
    let session = state
        .session()
        .await
        .filter(|session| session.id == session_id)
        .ok_or_else(|| anyhow!("the NVA session does not own the active cast"))?;
    if session.live {
        debug!(session = %session_id.chars().take(8).collect::<String>(), "ignored seek for a live stream");
        return Ok(());
    }
    if let Some(entry) = state.media_for_owner(session_id).await
        && matches!(
            &entry.input,
            MediaInput::Dash { .. } | MediaInput::Remux { .. }
        )
    {
        // Register the replacement while the operation lock still protects the
        // session/entry snapshot. Stop cancels this epoch before waiting for the
        // same lock, so a concurrent Stop can never be followed by a revived Play.
        let play_epoch = state.begin_play_epoch(session_id).await;
        drop(operation_guard);
        return restart_ffmpeg_at(state, session, entry, position_ms, play_epoch).await;
    }

    let (renderer, backend) = current_route_for_session(&state, session_id).await?;
    match backend {
        SessionBackend::Dlna => seek_inner(&renderer, position_ms).await,
        SessionBackend::LelinkV1 => lelink_v1_request(
            &renderer,
            session_id,
            "POST",
            &format!("/scrub?position={}", position_ms / 1000),
            "",
        )
        .await
        .map(|_| ()),
    }
}

/// An FFmpeg-backed transport stream advertises OP=00 and cannot be scrubbed by
/// the renderer. Replace it with a fresh token whose FFmpeg inputs begin at the
/// requested absolute media position instead.
async fn restart_ffmpeg_at(
    state: AppState,
    session: SessionView,
    entry: MediaEntry,
    position_ms: u64,
    play_epoch: u64,
) -> Result<()> {
    let source = match entry.input {
        MediaInput::Dash {
            video_url,
            video_backup_urls,
            audio_url,
            audio_backup_urls,
        } => MediaSource::Dash {
            video_url,
            video_backup_urls,
            audio_url,
            audio_backup_urls,
        },
        MediaInput::Remux { url, .. } => MediaSource::Progressive { url },
        MediaInput::Progressive { .. } => unreachable!("direct media is seeked by the renderer"),
    };
    let position_ms = entry.duration_ms.map_or(position_ms, |duration| {
        position_ms.min(duration.saturating_sub(1))
    });
    let was_paused = session.phase == "paused";
    let previous_speed = session.speed.clone();
    let session_id = session.id.clone();
    let media = ResolvedMedia {
        source,
        title: session.title,
        quality: session.quality,
        available_qualities: Vec::new(),
        duration_ms: entry.duration_ms,
        live: false,
    };

    let result = play(
        state.clone(),
        &session_id,
        session.origin,
        media,
        position_ms,
        play_epoch,
    )
    .await;
    state.complete_play_epoch(play_epoch).await;
    if let Err(error) = result {
        if error.downcast_ref::<SupersededPlay>().is_none() {
            let _ = stop(state.clone(), Some(&session_id)).await;
        }
        return Err(error);
    }

    // A replacement resource starts in PLAYING at 1x. Restore the state the
    // sender had before the seek after the new target route is established.
    if was_paused {
        pause(state, &session_id).await
    } else if previous_speed != "1" {
        set_speed(state, &session_id, &previous_speed).await
    } else {
        Ok(())
    }
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
        &renderer.gateway_address,
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
        &renderer.gateway_address,
    )
    .await
}

pub async fn set_mute(state: AppState, session_id: &str, muted: bool) -> Result<()> {
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
        "SetMute",
        &format!(
            "<InstanceID>0</InstanceID><Channel>Master</Channel><DesiredMute>{}</DesiredMute>",
            u8::from(muted)
        ),
        &renderer.address,
        &renderer.gateway_address,
    )
    .await
}

/// Where the cast has got to. A front end with a polling sender cannot answer from
/// the cached session view, because only the target knows the real position.
pub async fn position(state: AppState, session_id: &str) -> Result<(u64, u64)> {
    let _guard = state.operation().await;
    let (renderer, backend) = current_route_for_session(&state, session_id).await?;
    let entry = state
        .media_for_owner(session_id)
        .await
        .context("the active cast has no media timeline")?;
    let queried = if backend == SessionBackend::LelinkV1 {
        lelink_position(&renderer, session_id)
            .await
            .map(|(position, duration)| (Some(position), Some(duration)))
    } else {
        soap_query(
            &renderer.av_transport_url,
            &renderer.av_transport_service_type,
            "GetPositionInfo",
            "<InstanceID>0</InstanceID>",
            &renderer.address,
            &renderer.gateway_address,
        )
        .await
        .map(|response| {
            (
                soap_text(&response, "RelTime").and_then(parse_dlna_time_ms),
                soap_text(&response, "TrackDuration").and_then(parse_dlna_time_ms),
            )
        })
    };
    match queried {
        Ok((position, duration)) => effective_progress(&entry, position, duration, true)
            .context("the renderer returned no usable playback timeline"),
        Err(error) => effective_progress(&entry, None, None, true).ok_or(error),
    }
}

/// Converts a renderer-local timeline into the source's absolute timeline.
/// Renderer samples are authoritative. The local clock is only exposed when an
/// FFmpeg watermark proves that at least that much media has already been made
/// available to the target.
fn effective_progress(
    entry: &MediaEntry,
    renderer_position_ms: Option<u64>,
    renderer_duration_ms: Option<u64>,
    allow_estimate: bool,
) -> Option<(u64, u64)> {
    let remuxed = matches!(
        &entry.input,
        MediaInput::Dash { .. } | MediaInput::Remux { .. }
    );
    let offset_ms = if remuxed { entry.start_offset_ms } else { 0 };
    let duration_ms = entry
        .duration_ms
        .filter(|duration| *duration > 0)
        .or_else(|| {
            renderer_duration_ms
                .filter(|duration| *duration > 0)
                .map(|duration| offset_ms.saturating_add(duration))
        });
    let renderer_position_ms = renderer_position_ms
        .filter(|_| renderer_progress_usable(renderer_position_ms, renderer_duration_ms));
    let position_ms = if let Some(position_ms) = renderer_position_ms {
        let absolute = offset_ms.saturating_add(position_ms);
        let absolute = duration_ms.map_or(absolute, |duration| absolute.min(duration));
        entry.playback_clock.observe(absolute);
        absolute
    } else if allow_estimate && remuxed {
        let duration_ms = duration_ms?;
        let watermark = entry.remux_progress.current()?.out_time_ms?;
        entry
            .playback_clock
            .estimate()
            .min(offset_ms.saturating_add(watermark))
            .min(duration_ms)
    } else {
        return None;
    };
    Some((position_ms, duration_ms.unwrap_or(0)))
}

fn renderer_progress_usable(position_ms: Option<u64>, duration_ms: Option<u64>) -> bool {
    position_ms
        .is_some_and(|position| position > 0 || duration_ms.is_some_and(|duration| duration > 0))
}

async fn current_renderer_for_session(state: &AppState, session_id: &str) -> Result<Renderer> {
    current_route_for_session(state, session_id)
        .await
        .map(|(renderer, _)| renderer)
}

async fn current_route_for_session(
    state: &AppState,
    session_id: &str,
) -> Result<(Renderer, SessionBackend)> {
    let session = state
        .session()
        .await
        .filter(|session| session.id == session_id)
        .ok_or_else(|| anyhow!("the NVA session does not own the active cast"))?;
    let renderer = state
        .renderer(&session.target_udn)
        .await
        .ok_or_else(|| anyhow!("当前 DLNA 播放目标已离线"))?;
    Ok((renderer, session.backend))
}

fn spawn_lelink_monitor(
    state: AppState,
    renderer: Renderer,
    session_id: String,
    media_token: String,
    cancellation: CancellationToken,
    is_live: bool,
) {
    tokio::spawn(async move {
        const END_CONFIRMATION_SAMPLES: u8 = 3;
        let mut interval = time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut seen_renderer_progress = false;
        let mut terminal_samples = 0_u8;
        let mut progress_failures = 0_u8;
        interval.tick().await;
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = interval.tick() => {}
            }
            let Some(entry) = state.media(&media_token).await else {
                break;
            };
            let renderer_progress = match lelink_position(&renderer, &session_id).await {
                Ok(progress) => {
                    if progress == (0, 0) {
                        progress_failures = progress_failures.saturating_add(1);
                    } else {
                        progress_failures = 0;
                    }
                    Some(progress)
                }
                Err(error) => {
                    debug!(%error, "LeLink progress monitor query failed");
                    terminal_samples = 0;
                    progress_failures = progress_failures.saturating_add(1);
                    None
                }
            };
            let progress = renderer_progress
                .filter(|(position, duration)| {
                    renderer_progress_usable(Some(*position), Some(*duration))
                })
                .and_then(|(position, duration)| {
                    effective_progress(&entry, Some(position), Some(duration), false)
                })
                .or_else(|| effective_progress(&entry, None, None, progress_failures >= 3));
            if renderer_progress.is_some_and(|(_, duration)| duration > 0) {
                seen_renderer_progress = true;
            }
            if let Some((position_ms, duration_ms)) = progress
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
            if is_live || !seen_renderer_progress || renderer_progress.is_none() {
                continue;
            }
            let (position_ms, duration_ms) = renderer_progress.unwrap();
            let at_end = duration_ms > 0 && position_ms.saturating_add(500) >= duration_ms;
            let reset_after_start = position_ms == 0 && duration_ms == 0;
            if at_end || reset_after_start {
                terminal_samples = terminal_samples.saturating_add(1);
            } else {
                terminal_samples = 0;
            }
            if terminal_samples < END_CONFIRMATION_SAMPLES {
                continue;
            }
            let _guard = state.operation().await;
            if state
                .finish_session_if(&session_id, &media_token, None)
                .await
            {
                state.emit_nva(NvaEvent {
                    session_id: session_id.clone(),
                    method: "OnPlayState".into(),
                    params: Some(json!({"playState": 7})),
                    close_after: true,
                });
            }
            break;
        }
    });
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
        let mut progress_failures = 0_u8;
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
                &renderer.gateway_address,
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
                if let Some(entry) = state.media(&media_token).await {
                    if play_state == 5 {
                        entry.playback_clock.mark_paused();
                    } else {
                        let speed = session_speed(&state, &session_id).await;
                        entry
                            .playback_clock
                            .mark_playing(speed.trim().parse().unwrap_or(1.0));
                    }
                }
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
            let queried = soap_query(
                &renderer.av_transport_url,
                &renderer.av_transport_service_type,
                "GetPositionInfo",
                "<InstanceID>0</InstanceID>",
                &renderer.address,
                &renderer.gateway_address,
            )
            .await;
            let _guard = state.operation().await;
            let Some(entry) = state.media(&media_token).await else {
                break;
            };
            if cancellation.is_cancelled() {
                break;
            }
            let renderer_progress = match queried {
                Ok(position) => {
                    let elapsed = soap_text(&position, "RelTime").and_then(parse_dlna_time_ms);
                    let duration =
                        soap_text(&position, "TrackDuration").and_then(parse_dlna_time_ms);
                    let usable = renderer_progress_usable(elapsed, duration);
                    if usable {
                        progress_failures = 0;
                        (elapsed, duration)
                    } else {
                        progress_failures = progress_failures.saturating_add(1);
                        // Duration-only replies cannot anchor a position, but
                        // they can still bound the local FFmpeg/clock fallback.
                        (None, duration)
                    }
                }
                Err(error) => {
                    progress_failures = progress_failures.saturating_add(1);
                    if progress_failures == 1 || progress_failures.is_multiple_of(30) {
                        debug!(%error, failures = progress_failures, "DLNA position query failed");
                    }
                    (None, None)
                }
            };
            if let Some((position_ms, duration_ms)) = effective_progress(
                &entry,
                renderer_progress.0,
                renderer_progress.1,
                progress_failures >= 3,
            ) && duration_ms > 0
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
    local_address: &str,
) -> Result<()> {
    soap_query(
        control_url,
        service,
        action,
        arguments,
        discovered_ip,
        local_address,
    )
    .await
    .map(|_| ())
}

async fn soap_call_retried(
    state: &AppState,
    renderer: &Renderer,
    action: &str,
    arguments: &str,
    play_epoch: u64,
) -> Result<()> {
    let mut result: Result<()> = Ok(());
    for attempt in 1..=CAST_ATTEMPTS {
        if attempt > 1 {
            state.ensure_play_epoch(play_epoch)?;
            time::sleep(CAST_RETRY_DELAY).await;
        }
        result = soap_call(
            &renderer.av_transport_url,
            &renderer.av_transport_service_type,
            action,
            arguments,
            &renderer.address,
            &renderer.gateway_address,
        )
        .await;
        if result.is_ok() || attempt == CAST_ATTEMPTS {
            break;
        }
        debug!(%action, attempt, "DLNA target refused the cast command, retrying");
    }
    result
}

async fn soap_query(
    control_url: &str,
    service: &str,
    action: &str,
    arguments: &str,
    discovered_ip: &str,
    local_address: &str,
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
    let local_address = local_address
        .parse::<IpAddr>()
        .context("DLNA gateway address is invalid")?;
    let client = pinned_dlna_client(&control_url, discovered_ip, local_address)?;
    let response = client
        .post(control_url)
        .header("Content-Type", "text/xml; charset=\"utf-8\"")
        .header("User-Agent", "UPnP/1.0")
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
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        path::PathBuf,
    };

    use tokio::net::TcpListener;

    use super::*;

    fn progress_entry(
        duration_ms: Option<u64>,
        start_offset_ms: u64,
        clock_position_ms: u64,
    ) -> MediaEntry {
        MediaEntry {
            token: "progress-token".into(),
            owner_session: "progress-session".into(),
            input: MediaInput::Remux {
                url: "https://cdn.example/video.flv".into(),
                format: RemuxFormat::Flv,
            },
            mime: "video/mp2t".into(),
            duration_ms,
            start_offset_ms,
            created_unix_ms: 0,
            allowed_renderer_ip: "192.0.2.20".into(),
            gateway_address: "192.0.2.1".into(),
            cancellation: CancellationToken::new(),
            ffmpeg_consumers: Arc::new(Semaphore::new(MAX_FFMPEG_CONSUMERS)),
            remux_progress: Arc::new(RemuxProgress::default()),
            playback_clock: Arc::new(PlaybackClock::new(clock_position_ms)),
            hls_resources: Arc::new(RwLock::new(HlsResourceStore::default())),
        }
    }

    #[test]
    fn effective_progress_prefers_renderer_over_clock_and_ffmpeg_watermark() {
        let entry = progress_entry(Some(120_000), 40_000, 95_000);
        let generation = entry.remux_progress.begin_generation();
        assert!(entry.remux_progress.update(generation, 5_000));

        assert_eq!(
            effective_progress(&entry, Some(30_000), Some(80_000), true),
            Some((70_000, 120_000))
        );

        // The renderer observation also re-anchors the local clock. With a later
        // watermark out of the way, the paused estimate stays on that observation.
        assert!(entry.remux_progress.update(generation, 100_000));
        assert_eq!(
            effective_progress(&entry, None, None, true),
            Some((70_000, 120_000))
        );
    }

    #[test]
    fn effective_progress_shifts_renderer_timeline_by_remux_seek_offset() {
        let entry = progress_entry(None, 40_000, 40_000);

        assert_eq!(
            effective_progress(&entry, Some(30_000), Some(80_000), false),
            Some((70_000, 120_000))
        );
    }

    #[test]
    fn effective_progress_estimate_is_clamped_by_clock_watermark_and_duration() {
        let entry = progress_entry(Some(100_000), 40_000, 80_000);
        let generation = entry.remux_progress.begin_generation();
        assert!(entry.remux_progress.update(generation, 12_000));

        assert_eq!(
            effective_progress(&entry, None, None, true),
            Some((52_000, 100_000)),
            "the FFmpeg watermark caps a clock that is further ahead"
        );

        entry.playback_clock.seek(45_000);
        assert_eq!(
            effective_progress(&entry, None, None, true),
            Some((45_000, 100_000)),
            "the clock caps a watermark that is further ahead"
        );

        entry.playback_clock.seek(200_000);
        assert!(entry.remux_progress.update(generation, 200_000));
        assert_eq!(
            effective_progress(&entry, None, None, true),
            Some((100_000, 100_000)),
            "neither estimate may exceed the source duration"
        );
    }

    #[test]
    fn zero_zero_is_loading_but_unknown_duration_keeps_a_real_renderer_position() {
        let loading = progress_entry(Some(100_000), 0, 8_000);
        let generation = loading.remux_progress.begin_generation();
        assert!(loading.remux_progress.update(generation, 20_000));
        assert!(!renderer_progress_usable(Some(0), Some(0)));
        assert!(!renderer_progress_usable(None, Some(100_000)));
        assert_eq!(
            effective_progress(&loading, Some(0), Some(0), true),
            Some((8_000, 100_000)),
            "0/0 must use the bounded local fallback instead of resetting progress"
        );

        let unknown_duration = progress_entry(None, 0, 0);
        assert_eq!(
            effective_progress(&unknown_duration, Some(12_500), None, false),
            Some((12_500, 0)),
            "a renderer position remains useful even when total duration is unknown"
        );
    }

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
    fn a_target_reports_the_rates_it_can_play() {
        let description = r#"<?xml version="1.0"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
  <actionList><action><name>Play</name><argumentList>
    <argument><relatedStateVariable>TransportPlaySpeed</relatedStateVariable></argument>
  </argumentList></action></actionList>
  <serviceStateTable>
    <stateVariable sendEvents="no">
      <name>TransportPlaySpeed</name><dataType>string</dataType>
      <allowedValueList><allowedValue>1</allowedValue>
      <allowedValue> 1.5 </allowedValue><allowedValue></allowedValue>
      <allowedValue>-1</allowedValue></allowedValueList>
    </stateVariable>
    <stateVariable sendEvents="yes">
      <name>AbsTime</name><dataType>string</dataType>
      <allowedValueList><allowedValue>not-a-rate</allowedValue></allowedValueList>
    </stateVariable>
  </serviceStateTable>
</scpd>"#;
        assert_eq!(
            play_speeds_from_scpd(description),
            ["1", "1.5", "-1"].map(str::to_owned).to_vec()
        );

        let ranged = r#"<?xml version="1.0"?><scpd><serviceStateTable>
  <stateVariable><name>TransportPlaySpeed</name>
    <allowedValueRange><minimum>0.5</minimum><maximum>4</maximum></allowedValueRange>
  </stateVariable></serviceStateTable></scpd>"#;
        assert_eq!(play_speeds_from_scpd(ranged), Vec::<String>::new());
        assert_eq!(play_speeds_from_scpd("<scpd><"), Vec::<String>::new());
    }

    #[test]
    fn a_target_that_published_no_rate_list_is_not_read_as_refusing() {
        let integer_only = renderer_with_speeds(&["1", "2", "-1"]);
        assert!(integer_only.accepts_speed("1"));
        assert!(integer_only.accepts_speed("2"));
        assert!(!integer_only.accepts_speed("1.5"));
        assert!(renderer_with_speeds(&[]).accepts_speed("1.5"));
    }

    #[test]
    fn lelink_telecontrol_rate_mapping_matches_the_tv_apk() {
        for (speed, index) in [
            ("0.5", 0),
            ("0.75", 1),
            ("1", 2),
            ("1.25", 3),
            ("1.5", 4),
            ("1.75", 5),
            ("2", 6),
        ] {
            assert_eq!(telecontrol_rate_index(speed), Some(index), "{speed}");
        }
        assert_eq!(telecontrol_rate_index("1.2"), None);
        assert_eq!(telecontrol_rate_index("4"), None);
    }

    #[test]
    fn active_lelink_route_never_advertises_dlna_only_rates() {
        let mut target = renderer_with_speeds(&["1", "4"]);
        target.lelink = Some(LelinkEndpoint {
            uid: Some("tv".into()),
            name: "TV".into(),
            address: target.address.clone(),
            control_port: Some(53388),
            main_port: Some(52288),
            last_seen_unix_ms: now_ms(),
        });

        assert!(route_accepts_speed(
            &target,
            "1.5",
            Some(SessionBackend::LelinkV1)
        ));
        assert!(!route_accepts_speed(
            &target,
            "4",
            Some(SessionBackend::LelinkV1)
        ));
        assert!(route_accepts_speed(
            &target,
            "4",
            Some(SessionBackend::Dlna)
        ));
        assert!(route_accepts_speed(&target, "4", None));
    }

    #[tokio::test]
    async fn lelink_rate_sends_only_set_and_never_waits_for_a_reply() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let receiver = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut frame = [0_u8; 6];
            stream.read_exact(&mut frame).await.unwrap();
            let mut extra = [0_u8; 1];
            let extra_bytes = time::timeout(Duration::from_secs(1), stream.read(&mut extra))
                .await
                .expect("the bridge kept the Telecontrol socket open")
                .unwrap();
            (frame, extra_bytes)
        });
        let endpoint = LelinkEndpoint {
            uid: Some("silent-tv".into()),
            name: "Silent TV".into(),
            address: Ipv4Addr::LOCALHOST.to_string(),
            control_port: Some(port),
            main_port: Some(52288),
            last_seen_unix_ms: now_ms(),
        };

        time::timeout(
            Duration::from_millis(500),
            send_lelink_rate(&endpoint, "127.0.0.1", 4),
        )
        .await
        .expect("SET_RATE waited for an unavailable confirmation")
        .unwrap();
        let (frame, extra_bytes) = receiver.await.unwrap();
        assert_eq!(frame, [0, 0, 0, 6, 0x12, 4]);
        assert_eq!(
            extra_bytes, 0,
            "unexpected Telecontrol query followed SET_RATE"
        );
    }

    #[tokio::test]
    async fn lelink_rate_still_reports_a_connection_failure() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let endpoint = LelinkEndpoint {
            uid: Some("offline-tv".into()),
            name: "Offline TV".into(),
            address: Ipv4Addr::LOCALHOST.to_string(),
            control_port: Some(port),
            main_port: Some(52288),
            last_seen_unix_ms: now_ms(),
        };

        let error = send_lelink_rate(&endpoint, "127.0.0.1", 4)
            .await
            .expect_err("an offline Telecontrol endpoint must fail");
        let summary = format!("{error:#}");
        assert!(summary.contains("LeLink Telecontrol"), "{summary}");
    }

    #[tokio::test]
    async fn lelink_progress_uses_the_native_millisecond_timeline() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let receiver = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 5];
            stream.read_exact(&mut request).await.unwrap();
            let duration_ms = 296_789_i32;
            let position_ms = 83_456_i32;
            let mut response = Vec::with_capacity(LELINK_PROGRESS_FRAME_LEN);
            response.extend_from_slice(&(LELINK_PROGRESS_FRAME_LEN as u32).to_be_bytes());
            response.push(LELINK_PROGRESS);
            response.extend_from_slice(&duration_ms.to_be_bytes());
            response.extend_from_slice(&position_ms.to_be_bytes());
            stream.write_all(&response).await.unwrap();
            request
        });
        let endpoint = LelinkEndpoint {
            uid: Some("progress-tv".into()),
            name: "Progress TV".into(),
            address: Ipv4Addr::LOCALHOST.to_string(),
            control_port: Some(port),
            main_port: Some(52288),
            last_seen_unix_ms: now_ms(),
        };

        assert_eq!(
            query_lelink_progress(&endpoint, "127.0.0.1").await.unwrap(),
            (83_456, 296_789),
            "the public order is position then duration and both remain milliseconds"
        );
        assert_eq!(receiver.await.unwrap(), [0, 0, 0, 5, LELINK_GET_PROGRESS]);
    }

    #[test]
    fn lelink_progress_rejects_a_wrong_command_or_negative_time() {
        let mut payload = [0_u8; LELINK_PROGRESS_FRAME_LEN - 4];
        payload[0] = 0x48;
        assert!(parse_lelink_progress_payload(&payload).is_err());

        payload[0] = LELINK_PROGRESS;
        payload[1..5].copy_from_slice(&(-1_i32).to_be_bytes());
        payload[5..9].copy_from_slice(&(-1_i32).to_be_bytes());
        assert!(parse_lelink_progress_payload(&payload).is_err());
    }

    #[tokio::test]
    async fn lelink_v1_play_uses_the_proxy_url_and_a_stable_session_header() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let receiver = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let read = stream.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..read]);
                let text = String::from_utf8_lossy(&request);
                let Some(head_end) = text.find("\r\n\r\n") else {
                    continue;
                };
                let content_length = text[..head_end]
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if request.len() >= head_end + 4 + content_length {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        let mut renderer = renderer_with_speeds(&["1"]);
        renderer.address = Ipv4Addr::LOCALHOST.to_string();
        renderer.gateway_address = Ipv4Addr::LOCALHOST.to_string();
        renderer.lelink = Some(LelinkEndpoint {
            uid: Some("tv".into()),
            name: "TV".into(),
            address: Ipv4Addr::LOCALHOST.to_string(),
            control_port: None,
            main_port: Some(port),
            last_seen_unix_ms: now_ms(),
        });

        lelink_v1_play(
            &renderer,
            "bridge-session-1",
            "http://127.0.0.1:8080/media/token/stream.ts",
            "token",
            12_345,
        )
        .await
        .unwrap();
        let request = receiver.await.unwrap();
        assert!(request.starts_with("POST /play HTTP/1.1\r\n"));
        assert!(request.contains("X-LeLink-Session-ID: bridge-session-1\r\n"));
        assert!(request.contains("User-Agent: MediaControl/1.0\r\n"));
        assert!(request.contains("X-LeLink-Platform: Android\r\n"));
        assert!(
            request.contains("Content-Location: http://127.0.0.1:8080/media/token/stream.ts\r\n")
        );
        assert!(request.contains("Start-Position: 12\r\n"));
        assert!(request.ends_with("Content-URLID: token\r\n\r\n"));
    }

    #[test]
    fn lelink_scrub_parameters_are_seconds_on_the_wire() {
        assert_eq!(
            parse_lelink_scrub("duration: 90.25\nposition: 12.5\n").unwrap(),
            (12_500, 90_250)
        );
    }

    fn renderer_with_speeds(speeds: &[&str]) -> Renderer {
        Renderer {
            udn: "uuid:target".into(),
            friendly_name: "Target".into(),
            manufacturer: String::new(),
            model_name: String::new(),
            location: "http://10.0.0.2/description.xml".into(),
            av_transport_url: "http://10.0.0.2/control".into(),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
            play_speeds: speeds.iter().map(|speed| (*speed).to_owned()).collect(),
            lelink: None,
            address: "10.0.0.2".into(),
            gateway_address: "10.0.0.1".into(),
            discovery_interface_id: "test".into(),
            gateway_prefix_length: 24,
            last_seen_unix_ms: 0,
        }
    }

    #[test]
    fn our_own_devices_never_become_cast_targets() {
        let state = test_state();
        // The two identities must stay distinct, or one check would hide the other.
        assert_ne!(upnp::nva_tv_id(state.nva_device_uuid()), dmr::udn(&state));
        assert!(is_self_advertised(
            &state,
            &format!("uuid:{}", upnp::nva_tv_id(state.nva_device_uuid()))
        ));
        assert!(is_self_advertised(
            &state,
            &format!("uuid:{}", dmr::udn(&state)).to_uppercase()
        ));
        assert!(!is_self_advertised(
            &state,
            "uuid:12345678-1234-1234-1234-123456789012"
        ));
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

    #[tokio::test]
    async fn discovery_reads_the_rate_list_off_the_targets_own_description() {
        let port = serve_fake_target(true).await;
        let renderer = fetch_renderer(
            &test_state(),
            &format!("http://127.0.0.1:{port}/description.xml"),
            Ipv4Addr::LOCALHOST.into(),
            &loopback_interface(),
        )
        .await
        .unwrap()
        .expect("a renderer");

        assert_eq!(
            renderer.av_transport_scpd_url.as_deref(),
            Some(format!("http://127.0.0.1:{port}/AVTransport.xml").as_str())
        );
        assert_eq!(renderer.play_speeds, ["1", "2", "-1"].map(String::from));
        assert!(!renderer.accepts_speed("1.5"));
        assert!(renderer.accepts_speed("1"));
    }

    #[tokio::test]
    async fn a_target_that_will_not_serve_its_description_stays_a_usable_target() {
        let port = serve_fake_target(false).await;
        let renderer = fetch_renderer(
            &test_state(),
            &format!("http://127.0.0.1:{port}/description.xml"),
            Ipv4Addr::LOCALHOST.into(),
            &loopback_interface(),
        )
        .await
        .unwrap()
        .expect("a renderer");

        assert_eq!(renderer.play_speeds, Vec::<String>::new());
        assert!(renderer.accepts_speed("1.5"));
    }

    fn loopback_interface() -> InterfaceAddress {
        InterfaceAddress {
            id: "test".into(),
            name: "test".into(),
            address: Ipv4Addr::LOCALHOST,
            prefix_length: 8,
        }
    }

    /// Serves one MediaRenderer description and, when `with_scpd` is set, its
    /// AVTransport description; otherwise that second request answers 404. Returns the
    /// ephemeral port both are listening on.
    async fn serve_fake_target(with_scpd: bool) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut head = [0_u8; 512];
                let read = stream.read(&mut head).await.unwrap_or(0);
                let (status, body) =
                    if String::from_utf8_lossy(&head[..read]).starts_with("GET /AVTransport.xml") {
                        if with_scpd {
                            ("200 OK", FAKE_AV_TRANSPORT_SCPD)
                        } else {
                            ("404 Not Found", "")
                        }
                    } else {
                        ("200 OK", FAKE_TARGET_DESCRIPTION)
                    };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: text/xml\r\nContent-Length: \
{}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        port
    }

    const FAKE_TARGET_DESCRIPTION: &str = r#"<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaRenderer:1</deviceType>
    <friendlyName>Integer-only TV</friendlyName>
    <UDN>uuid:integer-only-target</UDN>
    <serviceList><service>
      <serviceType>urn:schemas-upnp-org:service:AVTransport:1</serviceType>
      <serviceId>urn:upnp-org:serviceId:AVTransport</serviceId>
      <SCPDURL>AVTransport.xml</SCPDURL>
      <controlURL>AVTransport/control</controlURL>
      <eventSubURL>AVTransport/event</eventSubURL>
    </service></serviceList>
  </device>
</root>"#;

    const FAKE_AV_TRANSPORT_SCPD: &str = r#"<?xml version="1.0"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
  <actionList><action><name>Play</name><argumentList>
    <argument><name>Speed</name>
      <relatedStateVariable>TransportPlaySpeed</relatedStateVariable></argument>
  </argumentList></action></actionList>
  <serviceStateTable><stateVariable sendEvents="no">
    <name>TransportPlaySpeed</name><dataType>string</dataType>
    <allowedValueList><allowedValue>1</allowedValue><allowedValue>2</allowedValue>
      <allowedValue>-1</allowedValue></allowedValueList>
  </stateVariable></serviceStateTable>
</scpd>"#;

    fn test_state() -> AppState {
        AppState::new(&crate::config::RuntimeConfig {
            web_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080),
            nva_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9959),
            lelink_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 52288),
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
        .expect("test state")
    }
}
