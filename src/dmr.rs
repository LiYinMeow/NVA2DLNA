//! A standards-compliant UPnP AV MediaRenderer service that this process hosts for
//! *inbound* casting. Any control point that speaks AVTransport:1 -- including the
//! DLNA mode of casting apps -- can `SetAVTransportURI` here, and we relay the play
//! to the selected real renderer through [`crate::dlna`].
//!
//! This is deliberately a second, independently identified device: the NVA face on
//! the raw TCP port keeps its own byte-compatible `description.xml`, so adding this
//! sink cannot regress the Bilibili path.

use std::{net::IpAddr, time::Duration};

use anyhow::{Result, anyhow};
use axum::{
    Router,
    body::to_bytes,
    extract::{Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use roxmltree::Document;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    bilibili::{MediaSource, ResolvedMedia, is_public_ip},
    dlna,
    state::{
        AppState, DmrSink, GenaService, GenaSubscription, SessionOrigin, SessionUpdate,
        TransportState,
    },
    upnp::{self, AV_TRANSPORT, CONNECTION_MANAGER, RENDERING_CONTROL},
};

const SERVICE_ID_AV: &str = "urn:upnp-org:serviceId:AVTransport";
const SERVICE_ID_RC: &str = "urn:upnp-org:serviceId:RenderingControl";
const SERVICE_ID_CM: &str = "urn:upnp-org:serviceId:ConnectionManager";
const SINK_PROTOCOLS: &str = "http-get:*:video/mp4:*,http-get:*:video/mpeg:*,\
http-get:*:video/mp2t:*,http-get:*:video/quicktime:*,http-get:*:video/x-matroska:*,\
http-get:*:audio/mp4:*,http-get:*:audio/mpeg:*";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1800);
const PLAY_MODES: [&str; 5] = ["NORMAL", "SHUFFLE", "REPEAT_ALL", "REPEAT_ONE", "RANDOM"];
/// The rates we advertise and forward. The fractional entries are what a Bilibili
/// phone's speed menu asks for once it sees them; the trick-play values were already
/// advertised, and a control point that rewinds should not be cut off. Anything a
/// real player rejects surfaces as that player's SOAP fault rather than being
/// silently swallowed here.
const PLAY_SPEEDS: [&str; 14] = [
    "1", "0.25", "0.5", "0.75", "1.25", "1.5", "1.75", "2", "3", "4", "-1", "-2", "8", "16",
];
const MAX_TIMEOUT: Duration = Duration::from_secs(86_400);
const XML_TYPE: &str = "text/xml; charset=\"utf-8\"";
const SERVER: &str = "NVA2DLNA/0.1 UPnP/1.0 DLNAGW/1.5";
const MAX_SOAP_BYTES: usize = 512 * 1024;

/// Stable identity for the sink device, distinct from the NVA device identity.
pub fn udn(state: &AppState) -> String {
    Uuid::new_v5(&state.device_uuid(), b"nva2dlna-dmr")
        .simple()
        .to_string()
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/dmr/description.xml", get(description))
        .route("/dmr/scpd/{document}", get(scpd))
        .route("/dmr/control/{service}", any(control))
        .route("/dmr/event/{service}", any(event))
}

async fn description(State(state): State<AppState>) -> Response {
    xml_response(StatusCode::OK, description_xml(&state))
}

fn description_xml(state: &AppState) -> String {
    let base = format!("http://{}:{}/dmr/", state.advertise_ip(), state.web_port());
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<root xmlns=\"urn:schemas-upnp-org:device-1-0\" xmlns:dlna=\"urn:schemas-dlna-org:device-1-0\">\
<specVersion><major>1</major><minor>0</minor></specVersion><URLBase>{base}</URLBase><device>\
<deviceType>{}</deviceType><friendlyName>{}</friendlyName>\
<manufacturer>NVA2DLNA</manufacturer>\
<modelDescription>DLNA media renderer bridge</modelDescription>\
<modelName>NVA2DLNA Bridge</modelName><modelNumber>1</modelNumber><UDN>uuid:{}</UDN>\
<dlna:X_DLNADOC>DMR-1.50</dlna:X_DLNADOC>\
<presentationURL>../</presentationURL>\
<serviceList>{}{}{}</serviceList></device></root>",
        upnp::MEDIA_RENDERER,
        // Both devices answer a MediaRenderer search, so this face carries its own name
        // rather than a suffix on the NVA one: several control points de-duplicate by
        // friendly name, and whichever entry survives then lacks the other's channel.
        upnp::xml_escape(state.dlna_name()),
        udn(state),
        service(
            AV_TRANSPORT,
            SERVICE_ID_AV,
            "control/AVTransport",
            "event/AVTransport",
            "AVTransport.xml",
        ),
        service(
            RENDERING_CONTROL,
            SERVICE_ID_RC,
            "control/RenderingControl",
            "event/RenderingControl",
            "RenderingControl.xml",
        ),
        service(
            CONNECTION_MANAGER,
            SERVICE_ID_CM,
            "control/ConnectionManager",
            "event/ConnectionManager",
            "ConnectionManager.xml",
        ),
    )
}

fn service(kind: &str, id: &str, control: &str, event: &str, scpd: &str) -> String {
    format!(
        "<service><serviceType>{kind}</serviceType><serviceId>{id}</serviceId>\
<controlURL>{control}</controlURL><eventSubURL>{event}</eventSubURL>\
<SCPDURL>scpd/{scpd}</SCPDURL></service>"
    )
}

async fn scpd(State(_state): State<AppState>, Path(document): Path<String>) -> Response {
    let body = match document.to_ascii_lowercase().as_str() {
        "avtransport.xml" => AV_TRANSPORT_SCPD,
        "renderingcontrol.xml" => RENDERING_CONTROL_SCPD,
        "connectionmanager.xml" => CONNECTION_MANAGER_SCPD,
        _ => return not_found(),
    };
    xml_response(StatusCode::OK, body.to_owned())
}

#[derive(Debug)]
struct SoapCall {
    action: String,
    args: Vec<(String, String)>,
}

impl SoapCall {
    fn arg(&self, name: &str) -> Option<&str> {
        self.args
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Every service we expose is single-instance, so InstanceID is mandated to be 0.
    fn instance_ok(&self) -> bool {
        self.arg("InstanceID")
            .is_none_or(|value| value.trim() == "0")
    }
}

fn parse_soap(body: &str) -> Result<SoapCall> {
    let document = Document::parse(body).map_err(|_| anyhow!("malformed SOAP envelope"))?;
    let action = document
        .descendants()
        .find(|node| {
            node.is_element()
                && node
                    .parent()
                    .is_some_and(|parent| parent.tag_name().name().ends_with("Body"))
        })
        .ok_or_else(|| anyhow!("SOAP Body has no action element"))?;
    let name = action.tag_name().name().to_owned();
    let args = action
        .children()
        .filter(|node| node.is_element())
        .map(|node| {
            (
                node.tag_name().name().to_owned(),
                node.text().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    Ok(SoapCall { action: name, args })
}

fn envelope(service: &str, action: &str, payload: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body>\
<u:{action}Response xmlns:u=\"{service}\">{payload}</u:{action}Response>\
</s:Body></s:Envelope>"
    )
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, XML_TYPE)], body).into_response()
}

fn soap_error(status: StatusCode, code: u16, description: &str) -> Response {
    xml_response(status, upnp::soap_fault(code, description))
}

async fn control(
    State(state): State<AppState>,
    Path(service): Path<String>,
    request: axum::extract::Request,
) -> Response {
    if *request.method() != axum::http::Method::POST {
        return soap_error(StatusCode::METHOD_NOT_ALLOWED, 405, "method not allowed");
    }
    let service_kind = match service.as_str() {
        "AVTransport" => Service::AvTransport,
        "RenderingControl" => Service::RenderingControl,
        "ConnectionManager" => Service::ConnectionManager,
        _ => return soap_error(StatusCode::NOT_FOUND, 401, "unknown service"),
    };
    let body = match to_bytes(request.into_body(), MAX_SOAP_BYTES).await {
        Ok(body) => String::from_utf8_lossy(&body).into_owned(),
        Err(_) => return soap_error(StatusCode::BAD_REQUEST, 402, "unreadable SOAP body"),
    };
    let call = match parse_soap(&body) {
        Ok(call) => call,
        Err(_) => return soap_error(StatusCode::BAD_REQUEST, 402, "unreadable SOAP body"),
    };
    debug!(?service_kind, action = call.action, "DMR control request");
    let (service_type, response) = match service_kind {
        Service::AvTransport => (AV_TRANSPORT, av_transport(&state, &call).await),
        Service::RenderingControl => (RENDERING_CONTROL, rendering_control(&state, &call).await),
        Service::ConnectionManager => (CONNECTION_MANAGER, connection_manager(&call)),
    };
    match response {
        Ok(payload) => xml_response(
            StatusCode::OK,
            envelope(service_type, &call.action, &payload),
        ),
        Err((status, code, message)) => soap_error(status, code, &message),
    }
}

#[derive(Debug, Clone, Copy)]
enum Service {
    AvTransport,
    RenderingControl,
    ConnectionManager,
}

type SoapFailure = (StatusCode, u16, String);

fn transport_failure(error: anyhow::Error) -> SoapFailure {
    (StatusCode::INTERNAL_SERVER_ERROR, 500, error.to_string())
}

/// Our published rate list is wider than any particular renderer's, so a Play that the
/// far end would silently drop is faulted here the way real hardware faults it.
async fn renderer_takes_speed(state: &AppState, speed: &str) -> Result<(), SoapFailure> {
    if dlna::target_accepts_speed(state, speed).await {
        return Ok(());
    }
    Err((
        StatusCode::BAD_REQUEST,
        402,
        format!("{speed} is not a speed the selected renderer advertises"),
    ))
}

async fn av_transport(state: &AppState, call: &SoapCall) -> Result<String, SoapFailure> {
    if !call.instance_ok() {
        return Err((StatusCode::BAD_REQUEST, 402, "InstanceID must be 0".into()));
    }
    match call.action.as_str() {
        "SetAVTransportURI" => {
            let uri = call.arg("CurrentURI").unwrap_or_default().trim().to_owned();
            validate_cast_uri(state, &uri).map_err(bad_request)?;
            let metadata = call
                .arg("CurrentURIMetaData")
                .unwrap_or_default()
                .trim()
                .to_owned();
            let title = didl_title(&metadata).unwrap_or_else(|| "DLNA casting".to_owned());
            let previous = {
                let sink = state.dmr().await;
                sink.session_id.clone()
            };
            if let Some(session_id) = previous {
                let _ = dlna::stop(state.clone(), Some(&session_id)).await;
            }
            {
                let mut sink = state.dmr().await;
                sink.uri = uri;
                sink.metadata = metadata;
                sink.title = title;
                sink.transport = TransportState::Stopped;
                sink.session_id = None;
                sink.position_ms = 0;
                sink.duration_ms = 0;
            }
            publish(state, GenaService::AvTransport).await;
            Ok(String::new())
        }
        "Play" => {
            let speed = call.arg("Speed").unwrap_or("1").trim().to_owned();
            if !PLAY_SPEEDS.contains(&speed.as_str()) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    402,
                    format!("{speed} is not an advertised Play speed"),
                ));
            }
            let snapshot = {
                let sink = state.dmr().await;
                (
                    sink.uri.clone(),
                    sink.title.clone(),
                    sink.transport,
                    sink.session_id.clone(),
                    sink.reported_position_ms(),
                    sink.speed.clone(),
                )
            };
            let (uri, title, transport, session_id, position_ms, current_speed) = snapshot;
            if uri.is_empty() {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    701,
                    "no CurrentURI has been set".into(),
                ));
            }
            if matches!(transport, TransportState::Playing) && session_id.is_some() {
                // Resending Play with a different Speed is how AVTransport changes the
                // rate. Falling through to a fresh cast would restart the video.
                if let Some(session_id) = session_id.as_deref() {
                    if current_speed != speed {
                        renderer_takes_speed(state, &speed).await?;
                        dlna::set_speed(state.clone(), session_id, &speed)
                            .await
                            .map_err(transport_failure)?;
                        {
                            let mut sink = state.dmr().await;
                            sink.speed = speed;
                        }
                        publish(state, GenaService::AvTransport).await;
                    }
                    return Ok(String::new());
                }
            }
            if let (TransportState::PausedPlayback, Some(session_id)) = (transport, session_id) {
                // The same rate is a plain resume, which must never be blocked by what
                // the target advertises; a different one is a deliberate rate change.
                if speed != current_speed {
                    renderer_takes_speed(state, &speed).await?;
                }
                dlna::set_speed(state.clone(), &session_id, &speed)
                    .await
                    .map_err(transport_failure)?;
                {
                    let mut sink = state.dmr().await;
                    sink.transport = TransportState::Playing;
                    sink.started_at = std::time::Instant::now();
                    sink.speed = speed;
                }
                publish(state, GenaService::AvTransport).await;
                return Ok(String::new());
            }
            publish_transition(state, TransportState::Transitioning).await;
            let session_id = Uuid::new_v4().simple().to_string();
            let epoch = state.begin_play_epoch(&session_id).await;
            let media = ResolvedMedia {
                source: MediaSource::Progressive { url: uri },
                title,
                quality: String::new(),
                available_qualities: Vec::new(),
                duration_ms: None,
                live: false,
            };
            let outcome = dlna::play(
                state.clone(),
                &session_id,
                SessionOrigin::Dmr,
                media,
                position_ms,
                epoch,
            )
            .await;
            state.complete_play_epoch(epoch).await;
            if let Err(error) = outcome {
                {
                    let mut sink = state.dmr().await;
                    sink.transport = TransportState::Stopped;
                    sink.session_id = None;
                }
                publish(state, GenaService::AvTransport).await;
                return Err(transport_failure(error));
            }
            {
                let mut sink = state.dmr().await;
                sink.session_id = Some(session_id);
                sink.transport = TransportState::Playing;
                sink.started_at = std::time::Instant::now();
                sink.position_ms = position_ms;
                sink.speed = speed;
            }
            publish(state, GenaService::AvTransport).await;
            Ok(String::new())
        }
        "Pause" => {
            let session_id = active_session(state).await?;
            dlna::pause(state.clone(), &session_id)
                .await
                .map_err(transport_failure)?;
            publish_transition(state, TransportState::PausedPlayback).await;
            Ok(String::new())
        }
        "Stop" => {
            let session_id = { state.dmr().await.session_id.clone() };
            dlna::stop(state.clone(), session_id.as_deref())
                .await
                .map_err(transport_failure)?;
            {
                let mut sink = state.dmr().await;
                sink.transport = TransportState::Stopped;
                sink.session_id = None;
                sink.position_ms = 0;
            }
            publish(state, GenaService::AvTransport).await;
            Ok(String::new())
        }
        "Seek" => {
            let unit = call.arg("Unit").unwrap_or("ABS_TIME").trim().to_owned();
            let target = call.arg("Target").unwrap_or_default().trim().to_owned();
            if !matches!(unit.as_str(), "ABS_TIME" | "REL_TIME") {
                return Err((
                    StatusCode::BAD_REQUEST,
                    402,
                    format!("unsupported Unit {unit}"),
                ));
            }
            let position_ms = parse_clock(&target)
                .ok_or_else(|| (StatusCode::BAD_REQUEST, 402, "unparsable Target".into()))?;
            let session_id = active_session(state).await?;
            dlna::seek(state.clone(), &session_id, position_ms)
                .await
                .map_err(transport_failure)?;
            {
                let mut sink = state.dmr().await;
                sink.position_ms = position_ms;
                sink.started_at = std::time::Instant::now();
            }
            publish(state, GenaService::AvTransport).await;
            Ok(String::new())
        }
        "GetTransportInfo" => {
            let sink = state.dmr().await;
            Ok(format!(
                "<CurrentTransportState>{}</CurrentTransportState>\
<CurrentTransportStatus>OK</CurrentTransportStatus>\
<CurrentSpeed>{}</CurrentSpeed>",
                sink.transport.as_str(),
                upnp::xml_escape(if sink.speed.is_empty() {
                    "1"
                } else {
                    &sink.speed
                })
            ))
        }
        "GetPositionInfo" => {
            let sink = state.dmr().await;
            Ok(format!(
                "<TrackDuration>{}</TrackDuration>\
<TrackMetaData>{}</TrackMetaData><TrackURI>{}</TrackURI>\
<RelTime>{}</RelTime><RelCount>2147483647</RelCount>\
<AbsTime>{}</AbsTime><AbsCount>2147483647</AbsCount>",
                format_clock(sink.duration_ms),
                upnp::xml_escape(&sink.metadata),
                upnp::xml_escape(&sink.uri),
                format_clock(sink.reported_position_ms()),
                format_clock(sink.reported_position_ms()),
            ))
        }
        "GetMediaInfo" => {
            let sink = state.dmr().await;
            Ok(format!(
                "<NrTracks>1</NrTracks><MediaDuration>{}</MediaDuration>\
<CurrentURI>{}</CurrentURI><CurrentURIMetaData>{}</CurrentURIMetaData>\
<NextURI></NextURI><NextURIMetaData></NextURIMetaData><PlayMode>NORMAL</PlayMode>\
<RecMediaFormat></RecMediaFormat><WriteStatus>NOT_WRITABLE</WriteStatus>",
                format_clock(sink.duration_ms),
                upnp::xml_escape(&sink.uri),
                upnp::xml_escape(&sink.metadata),
            ))
        }
        "GetInstanceID" => Ok("<InstanceID>0</InstanceID>".to_owned()),
        "GetDeviceCapabilities" => Ok("<PlayMedia>NONE</PlayMedia><RecMedia>NONE</RecMedia>\
<RecQualityModes>NORMAL</RecQualityModes>"
            .to_owned()),
        "SetPlayMode" => {
            let mode = call
                .arg("NewPlayMode")
                .map(|value| value.trim().to_ascii_uppercase())
                .unwrap_or_default();
            if !PLAY_MODES.contains(&mode.as_str()) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    501,
                    format!("{mode} is not an allowed PlayMode"),
                ));
            }
            state.dmr().await.play_mode = mode;
            Ok(String::new())
        }
        "GetPlayMode" => Ok(format!(
            "<CurrentPlayMode>{}</CurrentPlayMode>",
            state.dmr().await.play_mode
        )),
        "GetTransportSettings" => Ok(format!(
            "<PlayMode>{}</PlayMode><RecMediaFormat>NORMAL</RecMediaFormat>",
            state.dmr().await.play_mode
        )),
        other => Err((
            StatusCode::NOT_FOUND,
            401,
            format!("unsupported AVTransport action {other}"),
        )),
    }
}

async fn rendering_control(state: &AppState, call: &SoapCall) -> Result<String, SoapFailure> {
    if !call.instance_ok() {
        return Err((StatusCode::BAD_REQUEST, 402, "InstanceID must be 0".into()));
    }
    match call.action.as_str() {
        "GetVolume" => Ok(format!(
            "<CurrentVolume>{}</CurrentVolume>",
            state.dmr().await.volume
        )),
        "SetVolume" => {
            let desired: u8 = call
                .arg("DesiredVolume")
                .and_then(|value| value.trim().parse().ok())
                .ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        402,
                        "DesiredVolume required".into(),
                    )
                })?;
            let session_id = { state.dmr().await.session_id.clone() };
            if let Some(session_id) = session_id.as_deref() {
                dlna::set_volume(state.clone(), session_id, desired)
                    .await
                    .map_err(transport_failure)?;
            }
            state.dmr().await.volume = desired;
            publish(state, GenaService::RenderingControl).await;
            Ok(String::new())
        }
        "GetMute" => Ok(format!(
            "<CurrentMute>{}</CurrentMute>",
            u8::from(state.dmr().await.muted)
        )),
        "SetMute" => {
            let desired = match call
                .arg("DesiredMute")
                .and_then(|value| value.trim().parse::<u8>().ok())
            {
                Some(0) => false,
                Some(1) => true,
                _ => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        402,
                        "DesiredMute must be 0 or 1".into(),
                    ));
                }
            };
            let session_id = { state.dmr().await.session_id.clone() };
            if let Some(session_id) = session_id.as_deref() {
                dlna::set_mute(state.clone(), session_id, desired)
                    .await
                    .map_err(transport_failure)?;
            }
            state.dmr().await.muted = desired;
            publish(state, GenaService::RenderingControl).await;
            Ok(String::new())
        }
        "GetPresetNameList" => Ok("<PresetNameList>DEFAULT</PresetNameList>".to_owned()),
        "ListPresets" => Ok("<PresetNameList>DEFAULT</PresetNameList>\
<OutPresetName>DEFAULT</OutPresetName><RecPresetIndex>0</RecPresetIndex>"
            .to_owned()),
        other => Err((
            StatusCode::NOT_FOUND,
            401,
            format!("unsupported RenderingControl action {other}"),
        )),
    }
}

fn connection_manager(call: &SoapCall) -> Result<String, SoapFailure> {
    if !call.instance_ok() {
        return Err((StatusCode::BAD_REQUEST, 402, "InstanceID must be 0".into()));
    }
    match call.action.as_str() {
        "GetProtocolInfo" => Ok(format!(
            "<Source></Source><Sink>{}</Sink>",
            upnp::xml_escape(SINK_PROTOCOLS)
        )),
        "GetCurrentConnectionIDs" => Ok("<ConnectionIDs>0</ConnectionIDs>".to_owned()),
        "GetCurrentConnectionInfo" => Ok(
            "<ConnectionID>0</ConnectionID><RcsID>-1</RcsID><AVTransportID>-1</AVTransportID>\
<ProtocolInfo>http-get:*:video/mp4:*</ProtocolInfo><PeerConnectionManager></PeerConnectionManager>\
<PeerConnectionID>-1</PeerConnectionID><ConnectionStatus>OK</ConnectionStatus>"
                .to_owned(),
        ),
        other => Err((
            StatusCode::NOT_FOUND,
            401,
            format!("unsupported ConnectionManager action {other}"),
        )),
    }
}

fn bad_request(error: anyhow::Error) -> SoapFailure {
    (StatusCode::BAD_REQUEST, 402, error.to_string())
}

async fn active_session(state: &AppState) -> Result<String, SoapFailure> {
    state.dmr().await.session_id.clone().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        500,
        "nothing is playing".into(),
    ))
}

/// A control point may cast any reachable http(s) URL, but never our own media
/// proxy: that would make the bridge fetch the stream it is itself serving.
fn validate_cast_uri(state: &AppState, uri: &str) -> Result<()> {
    let parsed = url::Url::parse(uri).map_err(|_| anyhow!("CurrentURI is not a valid URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(anyhow!("only http and https CurrentURI are accepted"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(anyhow!("CurrentURI must not embed credentials"));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("CurrentURI has no host"))?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip.is_multicast() {
            return Err(anyhow!("CurrentURI is a multicast group"));
        }
        let back_at_us =
            ip == IpAddr::V4(state.advertise_ip()) && parsed.port() == Some(state.web_port());
        if back_at_us {
            return Err(anyhow!("CurrentURI points back at this bridge"));
        }
    }
    Ok(())
}

/// DIDL-Lite is nested as escaped XML inside the SOAP body, so `roxmltree` already
/// handed us a decoded document here.
fn didl_title(metadata: &str) -> Option<String> {
    let fragment = metadata.trim();
    if fragment.is_empty() {
        return None;
    }
    title_in(fragment).or_else(|| title_in(&with_didl_namespaces(fragment)))
}

fn title_in(metadata: &str) -> Option<String> {
    let document = Document::parse(metadata).ok()?;
    document
        .descendants()
        .find(|node| node.is_element() && node.tag_name().name() == "title")
        .and_then(|node| node.text())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Senders that copy DIDL fragments often drop the namespace declarations they were
/// only inherited in the full document, which makes them unprefixed-parseable.
fn with_didl_namespaces(fragment: &str) -> String {
    let Some(end) = fragment.find([' ', '>', '/']) else {
        return fragment.to_owned();
    };
    let mut patched = String::with_capacity(fragment.len() + 128);
    patched.push_str(&fragment[..end]);
    patched.push_str(
        " xmlns:dc=\"http://purl.org/dc/elements/1.1/\" \
xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\"",
    );
    patched.push_str(&fragment[end..]);
    patched
}

fn format_clock(ms: u64) -> String {
    let total = ms / 1000;
    format!(
        "{:02}:{:02}:{:02}",
        total / 3600,
        (total / 60) % 60,
        total % 60
    )
}

fn parse_clock(value: &str) -> Option<u64> {
    let mut parts = value.split(':').map(|part| part.parse::<f64>().ok());
    let hours = parts.next()?? as u64;
    let minutes = parts.next()?? as u64;
    let seconds = parts.next()??;
    Some((hours * 3600 + minutes * 60) * 1000 + (seconds * 1000.0) as u64)
}

async fn publish_transition(state: &AppState, transport: TransportState) {
    state.dmr().await.transport = transport;
    publish(state, GenaService::AvTransport).await;
}

/// Send a LastChange property set to every live subscriber of `service`.
async fn publish(state: &AppState, service: GenaService) {
    let (targets, sequence, body) = {
        let mut sink = state.dmr().await;
        let now = std::time::Instant::now();
        sink.subscriptions.retain(|sub| sub.expires_at > now);
        let targets = sink
            .subscriptions
            .iter()
            .filter(|sub| sub.service == service)
            .cloned()
            .collect::<Vec<_>>();
        sink.sequence = sink.sequence.wrapping_add(1);
        (targets, sink.sequence, event_body(service, &sink))
    };
    let Some(body) = body else {
        return;
    };
    for sub in targets {
        spawn_notify(state.clone(), sub, sequence, &body);
    }
}

fn event_body(service: GenaService, sink: &DmrSink) -> Option<String> {
    let inner = match service {
        GenaService::AvTransport => avtransport_event(sink),
        GenaService::RenderingControl => rendering_control_event(sink),
        GenaService::ConnectionManager => return None,
    };
    Some(format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
<e:propertyset xmlns:e=\"urn:schemas-upnp-org:event-1-0\"><e:property>\
<LastChange>{}</LastChange></e:property></e:propertyset>",
        upnp::xml_escape(&inner)
    ))
}

fn avtransport_event(sink: &DmrSink) -> String {
    format!(
        "<Event xmlns=\"urn:schemas-upnp-org:metadata-1-0/AVT/\"><InstanceID val=\"0\">\
<TransportState val=\"{}\"></TransportState>\
<TransportStatus val=\"OK\"></TransportStatus>\
<CurrentTransportActions val=\"{}\"></CurrentTransportActions></InstanceID></Event>",
        sink.transport.as_str(),
        current_transport_actions(sink)
    )
}

fn current_transport_actions(sink: &DmrSink) -> &'static str {
    match sink.transport {
        TransportState::Playing => "Play,Pause,Stop,Seek",
        TransportState::PausedPlayback => "Play,Stop,Seek",
        TransportState::Stopped | TransportState::Transitioning if !sink.uri.is_empty() => {
            "Play,Stop,Seek"
        }
        _ => "Stop",
    }
}

fn rendering_control_event(sink: &DmrSink) -> String {
    format!(
        "<Event xmlns=\"urn:schemas-upnp-org:metadata-1-0/RCS/\"><InstanceID val=\"0\">\
<Volume channel=\"Master\" val=\"{}\"></Volume><Mute channel=\"Master\" val=\"{}\"></Mute>\
</InstanceID></Event>",
        sink.volume,
        u8::from(sink.muted)
    )
}

fn spawn_notify(state: AppState, sub: GenaSubscription, sequence: u32, body: &str) {
    let body = body.to_owned();
    tokio::spawn(async move {
        if let Err(error) = send_notify(&state, &sub, sequence, &body).await {
            debug!(%error, sid = %sub.sid, "GENA notify failed");
        }
    });
}

async fn send_notify(
    state: &AppState,
    sub: &GenaSubscription,
    sequence: u32,
    body: &str,
) -> Result<()> {
    let method = reqwest::Method::from_bytes(b"NOTIFY")?;
    let url = reqwest::Url::parse(&sub.callback)?;
    let host = match url.port() {
        Some(port) => format!("{}:{}", url.host_str().unwrap_or_default(), port),
        None => url.host_str().unwrap_or_default().to_owned(),
    };
    let response = state
        .http()
        .request(method, url)
        .header("HOST", host)
        .header(header::CONTENT_TYPE, XML_TYPE)
        .header("NT", "upnp:event")
        .header("NTS", "upnp:propchange")
        .header("SID", &sub.sid)
        .header("SEQ", sequence.to_string())
        .body(body.to_owned())
        .send()
        .await?;
    debug!(status = response.status().as_u16(), sid = %sub.sid, "GENA notify delivered");
    Ok(())
}

async fn event(
    State(state): State<AppState>,
    Path(service): Path<String>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> Response {
    let Some(service_kind) = (match service.as_str() {
        "AVTransport" => Some(GenaService::AvTransport),
        "RenderingControl" => Some(GenaService::RenderingControl),
        "ConnectionManager" => Some(GenaService::ConnectionManager),
        _ => None,
    }) else {
        return not_found();
    };
    let method = request.method().as_str().to_ascii_uppercase();
    match method.as_str() {
        "SUBSCRIBE" => subscribe(state, service_kind, headers).await,
        "UNSUBSCRIBE" => unsubscribe(state, &headers).await,
        _ => empty_response(StatusCode::METHOD_NOT_ALLOWED),
    }
}

fn not_found() -> Response {
    empty_response(StatusCode::NOT_FOUND)
}

fn precondition_failed() -> Response {
    empty_response(StatusCode::PRECONDITION_FAILED)
}

fn empty_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .body(axum::body::Body::empty())
        .expect("static response")
}

async fn subscribe(state: AppState, service: GenaService, headers: HeaderMap) -> Response {
    let timeout = headers
        .get("TIMEOUT")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_timeout)
        .unwrap_or(DEFAULT_TIMEOUT);
    let existing = headers
        .get("SID")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if let Some(sid) = existing.as_deref() {
        let mut sink = state.dmr().await;
        if let Some(sub) = sink
            .subscriptions
            .iter_mut()
            .find(|sub| sub.sid == sid && sub.service == service)
        {
            sub.expires_at = std::time::Instant::now() + timeout;
            return upnp_headers(sid, timeout, StatusCode::OK);
        }
    }
    let callback = match headers
        .get("CALLBACK")
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => first_local_callback(value).await,
        None => return precondition_failed(),
    };
    let Ok(callback) = callback else {
        return precondition_failed();
    };
    let sid = format!("uuid:{}", Uuid::new_v4().simple());
    let subscription = GenaSubscription {
        sid: sid.clone(),
        service,
        callback,
        expires_at: std::time::Instant::now() + timeout,
    };
    let (initial, sequence) = {
        let mut sink = state.dmr().await;
        sink.subscriptions.push(subscription.clone());
        sink.sequence = sink.sequence.wrapping_add(1);
        (event_body(service, &sink), sink.sequence)
    };
    if let Some(body) = initial {
        spawn_notify(state.clone(), subscription, sequence, &body);
    }
    upnp_headers(&sid, timeout, StatusCode::OK)
}

async fn unsubscribe(state: AppState, headers: &HeaderMap) -> Response {
    let Some(sid) = headers.get("SID").and_then(|value| value.to_str().ok()) else {
        return precondition_failed();
    };
    state.dmr().await.subscriptions.retain(|sub| sub.sid != sid);
    empty_response(StatusCode::OK)
}

fn upnp_headers(sid: &str, timeout: Duration, status: StatusCode) -> Response {
    let headers = [
        (
            HeaderName::from_bytes(b"SID").expect("valid header name"),
            HeaderValue::from_str(sid).expect("generated sid is a valid header value"),
        ),
        (
            HeaderName::from_bytes(b"TIMEOUT").expect("valid header name"),
            HeaderValue::from_str(&format!("Second-{}", timeout.as_secs()))
                .expect("valid header value"),
        ),
        (header::SERVER, HeaderValue::from_static(SERVER)),
    ];
    (status, headers).into_response()
}

fn parse_timeout(value: &str) -> Option<Duration> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("infinite") || value.starts_with("infinite:") {
        return Some(MAX_TIMEOUT);
    }
    let seconds = value
        .get(7..)
        .filter(|_| value.len() >= 7 && value[..7].eq_ignore_ascii_case("Second-"))?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(seconds).min(MAX_TIMEOUT))
}

/// CALLBACK is a comma-separated list of angle-bracketed URIs; we deliver to the
/// first one that resolves to a site-local address.
async fn first_local_callback(value: &str) -> Result<String> {
    for candidate in callback_uris(value) {
        let parsed = url::Url::parse(&candidate).map_err(|_| anyhow!("CALLBACK uri is invalid"))?;
        if parsed.scheme() != "http" {
            return Err(anyhow!("CALLBACK must be an http uri"));
        }
        let Some(host) = parsed.host_str() else {
            return Err(anyhow!("CALLBACK has no host"));
        };
        // Eventing is LAN-internal; refusing to POST to a routable address keeps a
        // control point from turning this host into an outbound request fan-out.
        let resolves_local = match host.parse::<IpAddr>() {
            Ok(ip) => !is_public_ip(ip),
            Err(_) => {
                let port = parsed.port_or_known_default().unwrap_or(80);
                tokio::net::lookup_host((host, port))
                    .await
                    .is_ok_and(|sockets| {
                        sockets.into_iter().any(|socket| !is_public_ip(socket.ip()))
                    })
            }
        };
        if resolves_local {
            return Ok(candidate);
        }
    }
    Err(anyhow!("no site-local CALLBACK uri"))
}

fn callback_uris(value: &str) -> Vec<String> {
    value
        .split('<')
        .skip(1)
        .filter_map(|part| part.split('>').next())
        .map(str::trim)
        .filter(|uri| !uri.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Keeps the sink's reported transport honest when the cast ends behind our back:
/// the media monitor finishes sessions, and the other front end may displace us.
pub async fn run(state: AppState) {
    info!(
        description = %format!("http://{}:{}/dmr/description.xml", state.advertise_ip(), state.web_port()),
        "DLNA media renderer sink ready"
    );
    let mut updates = state.subscribe_session_updates();
    let mut sweep = tokio::time::interval(Duration::from_secs(30));
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            update = updates.recv() => match update {
                Ok(SessionUpdate::Closed { session_id, origin: SessionOrigin::Dmr }) => {
                    let mut sink = state.dmr().await;
                    if sink.session_id.as_deref() != Some(session_id.as_str()) {
                        continue;
                    }
                    sink.session_id = None;
                    sink.transport = TransportState::Stopped;
                    sink.position_ms = 0;
                    drop(sink);
                    publish(&state, GenaService::AvTransport).await;
                }
                Ok(SessionUpdate::Phase { session_id, phase, error }) => {
                    let mut sink = state.dmr().await;
                    if sink.session_id.as_deref() != Some(session_id.as_str()) || phase != "error" {
                        continue;
                    }
                    sink.transport = TransportState::Stopped;
                    drop(sink);
                    warn!(%phase, error = error.as_deref().unwrap_or("-"), "DLNA sink cast failed");
                    publish(&state, GenaService::AvTransport).await;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    debug!(missed, "DLNA sink event lag");
                }
                Err(_) => return,
            },
            _ = sweep.tick() => {
                let now = std::time::Instant::now();
                state.dmr().await.subscriptions.retain(|sub| sub.expires_at > now);
            }
        }
    }
}

pub(crate) const AV_TRANSPORT_SCPD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
<specVersion><major>1</major><minor>0</minor></specVersion>
<actionList>
<action><name>SetAVTransportURI</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>CurrentURI</name><direction>in</direction><relatedStateVariable>AVTransportURI</relatedStateVariable></argument>
<argument><name>CurrentURIMetaData</name><direction>in</direction><relatedStateVariable>AVTransportURIMetaData</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetMediaInfo</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>NrTracks</name><direction>out</direction><relatedStateVariable>NumberOfTracks</relatedStateVariable></argument>
<argument><name>MediaDuration</name><direction>out</direction><relatedStateVariable>CurrentMediaDuration</relatedStateVariable></argument>
<argument><name>CurrentURI</name><direction>out</direction><relatedStateVariable>AVTransportURI</relatedStateVariable></argument>
<argument><name>CurrentURIMetaData</name><direction>out</direction><relatedStateVariable>AVTransportURIMetaData</relatedStateVariable></argument>
<argument><name>NextURI</name><direction>out</direction><relatedStateVariable>NextAVTransportURI</relatedStateVariable></argument>
<argument><name>NextURIMetaData</name><direction>out</direction><relatedStateVariable>NextAVTransportURIMetaData</relatedStateVariable></argument>
<argument><name>PlayMode</name><direction>out</direction><relatedStateVariable>PlayMode</relatedStateVariable></argument>
<argument><name>RecMediaFormat</name><direction>out</direction><relatedStateVariable>RecMediaFormat</relatedStateVariable></argument>
<argument><name>WriteStatus</name><direction>out</direction><relatedStateVariable>RecordMediumWriteStatus</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetTransportInfo</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>CurrentTransportState</name><direction>out</direction><relatedStateVariable>TransportState</relatedStateVariable></argument>
<argument><name>CurrentTransportStatus</name><direction>out</direction><relatedStateVariable>TransportStatus</relatedStateVariable></argument>
<argument><name>CurrentSpeed</name><direction>out</direction><relatedStateVariable>TransportPlaySpeed</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetPositionInfo</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>TrackDuration</name><direction>out</direction><relatedStateVariable>CurrentTrackDuration</relatedStateVariable></argument>
<argument><name>TrackMetaData</name><direction>out</direction><relatedStateVariable>CurrentTrackMetaData</relatedStateVariable></argument>
<argument><name>TrackURI</name><direction>out</direction><relatedStateVariable>CurrentTrackURI</relatedStateVariable></argument>
<argument><name>RelTime</name><direction>out</direction><relatedStateVariable>RelativeTimePosition</relatedStateVariable></argument>
<argument><name>AbsTime</name><direction>out</direction><relatedStateVariable>AbsoluteTimePosition</relatedStateVariable></argument>
<argument><name>RelCount</name><direction>out</direction><relatedStateVariable>RelativeCounterPosition</relatedStateVariable></argument>
<argument><name>AbsCount</name><direction>out</direction><relatedStateVariable>AbsoluteCounterPosition</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetDeviceCapabilities</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>PlayMedia</name><direction>out</direction><relatedStateVariable>PossiblePlayMedia</relatedStateVariable></argument>
<argument><name>RecMedia</name><direction>out</direction><relatedStateVariable>PossibleRecMediaFormat</relatedStateVariable></argument>
<argument><name>RecQualityModes</name><direction>out</direction><relatedStateVariable>PossibleRecordQualityModes</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetTransportSettings</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>PlayMode</name><direction>out</direction><relatedStateVariable>PlayMode</relatedStateVariable></argument>
<argument><name>RecMediaFormat</name><direction>out</direction><relatedStateVariable>RecMediaFormat</relatedStateVariable></argument>
</argumentList></action>
<action><name>Stop</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
</argumentList></action>
<action><name>Play</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>Speed</name><direction>in</direction><relatedStateVariable>TransportPlaySpeed</relatedStateVariable></argument>
</argumentList></action>
<action><name>Pause</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
</argumentList></action>
<action><name>Seek</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>Unit</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_SeekMode</relatedStateVariable></argument>
<argument><name>Target</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_SeekTarget</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetInstanceID</name><argumentList>
<argument><name>InstanceID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
</argumentList></action>
<action><name>SetPlayMode</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>NewPlayMode</name><direction>in</direction><relatedStateVariable>PlayMode</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetPlayMode</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>CurrentPlayMode</name><direction>out</direction><relatedStateVariable>PlayMode</relatedStateVariable></argument>
</argumentList></action>
</actionList>
<serviceStateTable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_InstanceID</name><dataType>ui4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_SeekMode</name><dataType>string</dataType>
<allowedValueList><allowedValue>ABS_TIME</allowedValue><allowedValue>REL_TIME</allowedValue><allowedValue>TRACK_NR</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_SeekTarget</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="yes"><name>LastChange</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>TransportState</name><dataType>string</dataType>
<allowedValueList><allowedValue>STOPPED</allowedValue><allowedValue>PLAYING</allowedValue><allowedValue>TRANSITIONING</allowedValue><allowedValue>PAUSED_PLAYBACK</allowedValue><allowedValue>PAUSED_RECORDING</allowedValue><allowedValue>RECORDING</allowedValue><allowedValue>NO_MEDIA_PRESENT</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>TransportStatus</name><dataType>string</dataType>
<allowedValueList><allowedValue>OK</allowedValue><allowedValue>RECOMMENDED_RESET</allowedValue><allowedValue>UNCALIBRATED</allowedValue><allowedValue>SERVICE_UNAVAILABLE</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>TransportPlaySpeed</name><dataType>string</dataType><allowedValueList><allowedValue>1</allowedValue><allowedValue>0.25</allowedValue><allowedValue>0.5</allowedValue><allowedValue>0.75</allowedValue><allowedValue>1.25</allowedValue><allowedValue>1.5</allowedValue><allowedValue>1.75</allowedValue><allowedValue>2</allowedValue><allowedValue>3</allowedValue><allowedValue>4</allowedValue><allowedValue>-1</allowedValue><allowedValue>-2</allowedValue><allowedValue>8</allowedValue><allowedValue>16</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>PlayMode</name><dataType>string</dataType><allowedValueList><allowedValue>NORMAL</allowedValue><allowedValue>SHUFFLE</allowedValue><allowedValue>REPEAT_ALL</allowedValue><allowedValue>REPEAT_ONE</allowedValue><allowedValue>RANDOM</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>NumberOfTracks</name><dataType>ui4</dataType><allowedValueRange><minimum>0</minimum><maximum>1</maximum></allowedValueRange></stateVariable>
<stateVariable sendEvents="no"><name>CurrentMediaDuration</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>AVTransportURI</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>AVTransportURIMetaData</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>NextAVTransportURI</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>NextAVTransportURIMetaData</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>CurrentTrackMetaData</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>CurrentTrackURI</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>CurrentTrackDuration</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>RelativeTimePosition</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>AbsoluteTimePosition</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>RelativeCounterPosition</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>AbsoluteCounterPosition</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>CurrentTrack</name><dataType>ui4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>RecMediaFormat</name><dataType>string</dataType><allowedValueList><allowedValue>NOT_WRITABLE</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>RecordMediumWriteStatus</name><dataType>string</dataType><allowedValueList><allowedValue>NOT_WRITABLE</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>PossiblePlayMedia</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>PossibleRecMediaFormat</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>PossibleRecordQualityModes</name><dataType>string</dataType></stateVariable>
</serviceStateTable>
</scpd>"#;

pub(crate) const RENDERING_CONTROL_SCPD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
<specVersion><major>1</major><minor>0</minor></specVersion>
<actionList>
<action><name>GetVolume</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>Channel</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Channel</relatedStateVariable></argument>
<argument><name>CurrentVolume</name><direction>out</direction><relatedStateVariable>Volume</relatedStateVariable></argument>
</argumentList></action>
<action><name>SetVolume</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>Channel</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Channel</relatedStateVariable></argument>
<argument><name>DesiredVolume</name><direction>in</direction><relatedStateVariable>Volume</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetMute</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>Channel</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Channel</relatedStateVariable></argument>
<argument><name>CurrentMute</name><direction>out</direction><relatedStateVariable>Mute</relatedStateVariable></argument>
</argumentList></action>
<action><name>SetMute</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>Channel</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Channel</relatedStateVariable></argument>
<argument><name>DesiredMute</name><direction>in</direction><relatedStateVariable>Mute</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetPresetNameList</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>PresetNameList</name><direction>out</direction><relatedStateVariable>PresetNameList</relatedStateVariable></argument>
</argumentList></action>
<action><name>ListPresets</name><argumentList>
<argument><name>InstanceID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable></argument>
<argument><name>PresetName</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_PresetName</relatedStateVariable></argument>
<argument><name>RecPresetIndex</name><direction>out</direction><relatedStateVariable>PresetAudio</relatedStateVariable></argument>
<argument><name>OutPresetName</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_PresetName</relatedStateVariable></argument>
<argument><name>PresetNameList</name><direction>out</direction><relatedStateVariable>PresetNameList</relatedStateVariable></argument>
</argumentList></action>
</actionList>
<serviceStateTable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_InstanceID</name><dataType>ui4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_Channel</name><dataType>string</dataType><allowedValueList><allowedValue>Master</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_PresetName</name><dataType>string</dataType><allowedValueList><allowedValue>DEFAULT</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>PresetAudio</name><dataType>ui4</dataType></stateVariable>
<stateVariable sendEvents="yes"><name>LastChange</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>Volume</name><dataType>ui2</dataType><allowedValueRange><minimum>0</minimum><maximum>100</maximum><step>1</step></allowedValueRange></stateVariable>
<stateVariable sendEvents="no"><name>Mute</name><dataType>boolean</dataType></stateVariable>
<stateVariable sendEvents="no"><name>Loudness</name><dataType>boolean</dataType></stateVariable>
<stateVariable sendEvents="no"><name>PresetNameList</name><dataType>string</dataType></stateVariable>
</serviceStateTable>
</scpd>"#;

pub(crate) const CONNECTION_MANAGER_SCPD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
<specVersion><major>1</major><minor>0</minor></specVersion>
<actionList>
<action><name>GetProtocolInfo</name><argumentList>
<argument><name>Source</name><direction>out</direction><relatedStateVariable>SourceProtocolInfo</relatedStateVariable></argument>
<argument><name>Sink</name><direction>out</direction><relatedStateVariable>SinkProtocolInfo</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetCurrentConnectionIDs</name><argumentList>
<argument><name>ConnectionIDs</name><direction>out</direction><relatedStateVariable>CurrentConnectionIDs</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetCurrentConnectionInfo</name><argumentList>
<argument><name>ConnectionID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ConnectionID</relatedStateVariable></argument>
<argument><name>RcsID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_RcsID</relatedStateVariable></argument>
<argument><name>AVTransportID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_AVTransportID</relatedStateVariable></argument>
<argument><name>ProtocolInfo</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ProtocolInfo</relatedStateVariable></argument>
<argument><name>PeerConnectionManager</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_PeerConnectionManager</relatedStateVariable></argument>
<argument><name>PeerConnectionID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_PeerConnectionID</relatedStateVariable></argument>
<argument><name>ConnectionStatus</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ConnectionStatus</relatedStateVariable></argument>
</argumentList></action>
</actionList>
<serviceStateTable>
<stateVariable sendEvents="no"><name>SourceProtocolInfo</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>SinkProtocolInfo</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>CurrentConnectionIDs</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionStatus</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionManager</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_Direction</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ProtocolInfo</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionID</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_AVTransportID</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_RcsID</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_PeerConnectionID</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_PeerConnectionManager</name><dataType>string</dataType></stateVariable>
</serviceStateTable>
</scpd>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soap_arguments_are_extracted_with_entities_decoded() {
        let body = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body>\
<u:SetAVTransportURI xmlns:u=\"urn:schemas-upnp-org:service:AVTransport:1\">\
<InstanceID>0</InstanceID><CurrentURI>http://10.0.0.5:8000/a.mp4</CurrentURI>\
<CurrentURIMetaData>&lt;DIDL-Lite&gt;&lt;dc:title&gt;A &amp;amp; B&lt;/dc:title&gt;&lt;/DIDL-Lite&gt;\
</CurrentURIMetaData></u:SetAVTransportURI></s:Body></s:Envelope>";
        let call = parse_soap(body).expect("soap parses");
        assert_eq!(call.action, "SetAVTransportURI");
        assert_eq!(call.arg("CurrentURI"), Some("http://10.0.0.5:8000/a.mp4"));
        assert!(
            call.arg("CurrentURIMetaData")
                .is_some_and(|md| md.contains("A &amp; B"))
        );
        assert!(call.instance_ok());
    }

    #[test]
    fn nonzero_instance_id_is_rejected() {
        let body = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body>\
<u:Stop xmlns:u=\"urn:schemas-upnp-org:service:AVTransport:1\"><InstanceID>1</InstanceID>\
</u:Stop></s:Body></s:Envelope>";
        let call = parse_soap(body).expect("soap parses");
        assert!(!call.instance_ok());
    }

    #[test]
    fn clock_formatting_round_trips() {
        assert_eq!(format_clock(0), "00:00:00");
        assert_eq!(format_clock(3_723_400), "01:02:03");
        assert_eq!(parse_clock("01:02:03"), Some(3_723_000));
        assert_eq!(parse_clock("00:00:10.5"), Some(10_500));
        assert_eq!(parse_clock("00:01:30.250"), Some(90_250));
        assert_eq!(parse_clock("nonsense"), None);
    }

    #[test]
    fn callback_uris_are_taken_from_the_angle_bracketed_list() {
        let callbacks = callback_uris("<http://192.168.1.20:49152/>, <http://192.168.1.20:49153/>");
        assert_eq!(
            callbacks,
            vec![
                "http://192.168.1.20:49152/".to_owned(),
                "http://192.168.1.20:49153/".to_owned()
            ]
        );
        assert_eq!(callback_uris("not a uri"), Vec::<String>::new());
    }

    #[tokio::test]
    async fn callback_selection_prefers_a_site_local_uri() {
        let chosen = first_local_callback("<http://8.8.8.8:1234/>, <http://192.168.1.20:49152/>")
            .await
            .expect("local callback");
        assert_eq!(chosen, "http://192.168.1.20:49152/");
        assert!(
            first_local_callback("<http://8.8.8.8:1234/>")
                .await
                .is_err()
        );
        assert!(first_local_callback("<ftp://192.168.1.20/>").await.is_err());
        assert!(first_local_callback("no brackets").await.is_err());
    }

    #[test]
    fn timeout_parsing_clamps_and_defaults() {
        assert_eq!(parse_timeout("Second-180"), Some(Duration::from_secs(180)));
        assert_eq!(parse_timeout("Second-999999999"), Some(MAX_TIMEOUT));
        assert_eq!(parse_timeout("infinite"), Some(MAX_TIMEOUT));
        assert_eq!(parse_timeout("Second-abc"), None);
    }

    #[test]
    fn cast_uri_rejects_self_proxy_and_non_http() {
        let state = test_state();
        assert!(validate_cast_uri(&state, "file:///etc/passwd").is_err());
        assert!(
            validate_cast_uri(&state, "http://192.0.2.10:8080/media/t/a.mp4").is_err(),
            "our own advertised address must not be castable"
        );
        assert!(validate_cast_uri(&state, "http://239.255.255.250:1900/").is_err());
        assert!(validate_cast_uri(&state, "http://user:pw@10.0.0.5/a.mp4").is_err());
        assert!(validate_cast_uri(&state, "http://192.0.2.7:8000/a.mp4").is_ok());
        assert!(validate_cast_uri(&state, "http://192.0.2.10:9959/description.xml").is_ok());
    }

    #[test]
    fn didl_titles_survive_missing_namespace_declarations() {
        assert_eq!(
            didl_title(r#"<DIDL-Lite><dc:title>正片</dc:title></DIDL-Lite>"#).as_deref(),
            Some("正片")
        );
        assert_eq!(
            didl_title(
                "<DIDL-Lite xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\
<dc:title>带命名空间</dc:title></DIDL-Lite>"
            )
            .as_deref(),
            Some("带命名空间")
        );
        assert_eq!(didl_title(""), None);
        assert_eq!(didl_title("<DIDL-Lite><res>0</res></DIDL-Lite>"), None);
    }

    #[test]
    fn description_uses_relative_urls_under_its_own_base() {
        let state = test_state();
        let description = description_xml(&state);
        assert!(description.contains("<URLBase>http://192.0.2.10:8080/dmr/</URLBase>"));
        assert!(description.contains("<controlURL>control/AVTransport</controlURL>"));
        assert!(description.contains("<SCPDURL>scpd/AVTransport.xml</SCPDURL>"));
        assert!(description.contains("<dlna:X_DLNADOC>DMR-1.50</dlna:X_DLNADOC>"));
        assert!(description.contains("<friendlyName>UniDLNA</friendlyName>"));
        assert_ne!(udn(&state), upnp::nva_tv_id(state.nva_device_uuid()));
    }

    #[test]
    fn service_descriptions_match_the_advertised_services() {
        let state = test_state();
        let text = description_xml(&state);
        let description = Document::parse(&text).expect("description parses");
        let advertised = description
            .descendants()
            .filter(|node| node.has_tag_name("serviceType"))
            .filter_map(|node| node.text())
            .collect::<Vec<_>>();
        assert_eq!(
            advertised,
            vec![AV_TRANSPORT, RENDERING_CONTROL, CONNECTION_MANAGER]
        );
        for document in [
            AV_TRANSPORT_SCPD,
            RENDERING_CONTROL_SCPD,
            CONNECTION_MANAGER_SCPD,
        ] {
            let scpd = Document::parse(document).expect("scpd parses");
            assert!(scpd.root().first_element_child().is_some());
        }
        let scpd = Document::parse(AV_TRANSPORT_SCPD).expect("avtransport scpd parses");
        let actions = scpd
            .descendants()
            .filter(|node| {
                node.has_tag_name("name") && node.parent().is_some_and(|p| p.has_tag_name("action"))
            })
            .filter_map(|node| node.text())
            .collect::<Vec<_>>();
        assert!(actions.contains(&"SetAVTransportURI"));
        assert!(actions.contains(&"GetPositionInfo"));
        assert!(!actions.contains(&"SetMute"));
    }

    #[test]
    fn transport_events_escape_the_inner_document_exactly_once() {
        let sink = DmrSink {
            uri: "http://10.0.0.5/a.mp4".into(),
            metadata: "<DIDL-Lite><dc:title>A & B</dc:title></DIDL-Lite>".into(),
            transport: TransportState::Playing,
            ..DmrSink::default()
        };
        let body = event_body(GenaService::AvTransport, &sink).expect("avt event");
        assert!(body.contains("<e:propertyset xmlns:e=\"urn:schemas-upnp-org:event-1-0\">"));
        assert!(
            body.contains("&lt;Event xmlns=&quot;urn:schemas-upnp-org:metadata-1-0/AVT/&quot;")
        );
        assert!(body.contains("TransportState val=&quot;PLAYING&quot;"));
        // DIDL metadata is not an evented AVTransport property, so it stays out of
        // LastChange entirely and only surfaces through GetPositionInfo.
        assert!(!body.contains("A &amp;"));
        let decoded = body
            .split("<LastChange>")
            .nth(1)
            .and_then(|tail| tail.split("</LastChange>").next())
            .expect("LastChange text");
        assert!(!decoded.contains("<Event"));
    }

    #[test]
    fn rendering_control_events_report_the_master_volume() {
        let sink = DmrSink {
            volume: 42,
            muted: true,
            ..DmrSink::default()
        };
        let body = event_body(GenaService::RenderingControl, &sink).expect("rcs event");
        assert!(body.contains("Volume channel=&quot;Master&quot; val=&quot;42&quot;"));
        assert!(body.contains("Mute channel=&quot;Master&quot; val=&quot;1&quot;"));
        assert!(event_body(GenaService::ConnectionManager, &sink).is_none());
    }

    #[test]
    fn the_scpd_declares_every_action_the_handlers_accept() {
        let declared = |scpd: &str| {
            let document = Document::parse(scpd).expect("scpd parses");
            document
                .descendants()
                .filter(|node| {
                    node.has_tag_name("name")
                        && node.parent().is_some_and(|p| p.has_tag_name("action"))
                })
                .filter_map(|node| node.text())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let avt = declared(AV_TRANSPORT_SCPD);
        for action in ["SetPlayMode", "GetPlayMode", "Seek", "Stop"] {
            assert!(avt.contains(&action.to_owned()), "{action} missing");
        }
        let rcs = declared(RENDERING_CONTROL_SCPD);
        for action in [
            "SetVolume",
            "GetVolume",
            "SetMute",
            "GetMute",
            "ListPresets",
        ] {
            assert!(rcs.contains(&action.to_owned()), "{action} missing");
        }
        let scpd = Document::parse(AV_TRANSPORT_SCPD).expect("avtransport scpd parses");
        let allowed = |variable: &str| {
            scpd.descendants()
                .find(|node| {
                    node.has_tag_name("stateVariable")
                        && node.children().any(|child| {
                            child.has_tag_name("name") && child.text() == Some(variable)
                        })
                })
                .unwrap_or_else(|| panic!("no {variable} state variable"))
                .descendants()
                .filter(|node| node.has_tag_name("allowedValue"))
                .filter_map(|node| node.text())
                .collect::<Vec<_>>()
        };
        assert_eq!(allowed("PlayMode"), PLAY_MODES.to_vec());
        assert_eq!(allowed("TransportPlaySpeed"), PLAY_SPEEDS.to_vec());
    }

    fn test_state() -> AppState {
        use std::{
            net::{Ipv4Addr, SocketAddrV4},
            path::PathBuf,
        };
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
