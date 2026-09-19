//! Drives the DLNA sink front end the way a real control point would: raw HTTP over
//! a loopback socket, against a fake renderer that records the SOAP we relay onward.

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    time::Duration,
};

use axum::Router;
use nva2dlna::{
    config::RuntimeConfig,
    dmr,
    state::{AppState, Renderer, now_ms},
    upnp,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

#[tokio::test]
async fn a_dlna_cast_is_relayed_to_the_selected_renderer() {
    let (renderer_port, mut renderer) = echo_server().await;
    let (state, config_path) = test_state(renderer_port).await;
    let (base, shutdown) = serve(state.clone()).await;

    let description = get(&base, "/dmr/description.xml").await;
    assert!(description.contains(&format!(
        "<deviceType>{}</deviceType>",
        upnp::MEDIA_RENDERER
    )));
    assert!(description.contains("<dlna:X_DLNADOC>DMR-1.50</dlna:X_DLNADOC>"));
    assert!(description.contains(&format!("<UDN>uuid:{}</UDN>", dmr::udn(&state))));

    let scpd = get(&base, "/dmr/scpd/AVTransport.xml").await;
    assert!(scpd.contains("<name>SetAVTransportURI</name>"));
    assert!(scpd.contains("<name>LastChange</name>"));

    let (status, _) = post(
        &base,
        "AVTransport",
        "SetAVTransportURI",
        "<InstanceID>0</InstanceID>\
<CurrentURI>http://192.0.2.99:8000/movie.mp4</CurrentURI>\
<CurrentURIMetaData>&lt;DIDL-Lite&gt;&lt;dc:title&gt;远端影片&lt;/dc:title&gt;&lt;/DIDL-Lite&gt;\
</CurrentURIMetaData>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        renderer.try_recv().is_err(),
        "SetAVTransportURI must not reach the renderer before Play"
    );

    let (status, body) = post(
        &base,
        "AVTransport",
        "GetTransportInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("<CurrentTransportState>STOPPED</CurrentTransportState>"));

    let (status, _) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>1</Speed>",
    )
    .await;
    assert_eq!(status, 200);

    let set_uri = recv(&mut renderer).await;
    assert!(set_uri.contains("#SetAVTransportURI"));
    // What we hand the real renderer is a URL it can fetch from us, carrying the
    // title and media type the control point announced.
    // The media URL is served from the target-side gateway interface, not from
    // the receiver/advertisement interface on the phone-side LAN.
    assert!(set_uri.contains("http://127.0.0.1:8080/media/"));
    assert!(set_uri.contains("stream.mp4"));
    assert!(set_uri.contains("远端影片"));
    assert!(set_uri.contains("http-get:*:video/mp4:*"));
    recv_until(&mut renderer, "#Play").await;
    assert!(
        state.session().await.is_some(),
        "the bridge session should be live while the DLNA cast plays"
    );

    let (status, body) = post(
        &base,
        "AVTransport",
        "GetPositionInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert_eq!(status, 200);
    // The control point is told about the URI it cast, not our internal proxy URL.
    assert!(body.contains("<TrackURI>http://192.0.2.99:8000/movie.mp4</TrackURI>"));

    let (status, body) = post(
        &base,
        "AVTransport",
        "GetTransportInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("<CurrentTransportState>PLAYING</CurrentTransportState>"));

    let (status, body) = post(&base, "ConnectionManager", "GetProtocolInfo", "").await;
    assert_eq!(status, 200);
    assert!(body.contains("<Sink>http-get"));

    let (status, _) = post(
        &base,
        "RenderingControl",
        "SetMute",
        "<InstanceID>0</InstanceID><Channel>Master</Channel><DesiredMute>1</DesiredMute>",
    )
    .await;
    assert_eq!(status, 200);
    recv_until(&mut renderer, "#SetMute").await;
    let (status, body) = post(
        &base,
        "RenderingControl",
        "GetMute",
        "<InstanceID>0</InstanceID><Channel>Master</Channel>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("<CurrentMute>1</CurrentMute>"));

    let (status, _) = post(
        &base,
        "AVTransport",
        "SetPlayMode",
        "<InstanceID>0</InstanceID><NewPlayMode>REPEAT_ONE</NewPlayMode>",
    )
    .await;
    assert_eq!(status, 200);
    let (_, body) = post(
        &base,
        "AVTransport",
        "GetPlayMode",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert!(body.contains("<CurrentPlayMode>REPEAT_ONE</CurrentPlayMode>"));

    let (status, body) = post(
        &base,
        "AVTransport",
        "SetPlayMode",
        "<InstanceID>0</InstanceID><NewPlayMode>REPEAT_SUNSET</NewPlayMode>",
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("<errorCode>501</errorCode>"));

    let (status, body) = post(
        &base,
        "AVTransport",
        "SetNextAVTransportURI",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert_eq!(status, 404);
    assert!(body.contains("<errorCode>401</errorCode>"));

    let (status, _) = post(&base, "AVTransport", "Stop", "<InstanceID>0</InstanceID>").await;
    assert_eq!(status, 200);
    recv_until(&mut renderer, "#Stop").await;
    assert!(state.session().await.is_none());

    shutdown.send(()).unwrap();
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn a_second_play_with_a_new_speed_changes_the_rate_without_recasting() {
    let (renderer_port, mut renderer) = echo_server().await;
    let (state, config_path) = test_state(renderer_port).await;
    let (base, shutdown) = serve(state.clone()).await;

    let (status, _) = post(
        &base,
        "AVTransport",
        "SetAVTransportURI",
        "<InstanceID>0</InstanceID><CurrentURI>http://192.0.2.99:8000/movie.mp4</CurrentURI>\
<CurrentURIMetaData></CurrentURIMetaData>",
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>1</Speed>",
    )
    .await;
    assert_eq!(status, 200);
    recv_until(&mut renderer, "#SetAVTransportURI").await;
    recv_until(&mut renderer, "#Play").await;

    let (status, _) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>2</Speed>",
    )
    .await;
    assert_eq!(status, 200);
    let replayed = recv_until(&mut renderer, "#Play").await;
    assert!(replayed.contains("<Speed>2</Speed>"), "{replayed}");
    // The rate change must not restart the video from the beginning.
    let mut stray = Vec::new();
    while let Ok(Some(request)) =
        tokio::time::timeout(Duration::from_millis(300), renderer.recv()).await
    {
        stray.push(request);
    }
    assert!(
        stray
            .iter()
            .all(|request| !request.contains("#SetAVTransportURI")),
        "a speed change recast the media: {stray:?}"
    );

    let (status, body) = post(
        &base,
        "AVTransport",
        "GetTransportInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("<CurrentSpeed>2</CurrentSpeed>"));

    // Fractional rates are in the advertised list, so the phone's 1.5x entry has to
    // be forwarded rather than bounced.
    let (status, _) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>1.5</Speed>",
    )
    .await;
    assert_eq!(status, 200);
    let replayed = recv_until(&mut renderer, "#Play").await;
    assert!(replayed.contains("<Speed>1.5</Speed>"), "{replayed}");
    let (_, body) = post(
        &base,
        "AVTransport",
        "GetTransportInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert!(body.contains("<CurrentSpeed>1.5</CurrentSpeed>"));

    let (status, body) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>1.4</Speed>",
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("<errorCode>402</errorCode>"));
    let (_, body) = post(
        &base,
        "AVTransport",
        "GetTransportInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert!(
        body.contains("<CurrentSpeed>1.5</CurrentSpeed>"),
        "a rejected speed must leave the reported rate alone"
    );

    // Pause then resume: the resume keeps the rate the cast was running at.
    post(&base, "AVTransport", "Pause", "<InstanceID>0</InstanceID>").await;
    recv_until(&mut renderer, "#Pause").await;
    let (status, _) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>2</Speed>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        recv_until(&mut renderer, "#Play")
            .await
            .contains("<Speed>2</Speed>")
    );
    assert!(state.session().await.is_some());

    // A target that advertises nothing but 1x must not be reported as playing faster
    // than it really is.
    let mut integer_only = state.selected_renderer().await.unwrap();
    integer_only.play_speeds = ["1"].map(String::from).to_vec();
    state.replace_renderers(vec![integer_only]).await;
    let (status, body) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>1.5</Speed>",
    )
    .await;
    assert_eq!(status, 400);
    assert!(body.contains("<errorCode>402</errorCode>"), "{body}");
    assert!(body.contains("selected renderer"), "{body}");
    let (_, body) = post(
        &base,
        "AVTransport",
        "GetTransportInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await;
    assert!(
        body.contains("<CurrentSpeed>2</CurrentSpeed>"),
        "a rate the target does not advertise must not be reported as playing: {body}"
    );

    // Asking for the rate already in effect is a resume, so the list can never strand
    // the cast in its paused state.
    post(&base, "AVTransport", "Pause", "<InstanceID>0</InstanceID>").await;
    recv_until(&mut renderer, "#Pause").await;
    let (status, _) = post(
        &base,
        "AVTransport",
        "Play",
        "<InstanceID>0</InstanceID><Speed>2</Speed>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        recv_until(&mut renderer, "#Play")
            .await
            .contains("<Speed>2</Speed>")
    );

    shutdown.send(()).unwrap();
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn a_subscriber_receives_a_propchange_when_the_cast_state_changes() {
    let (renderer_port, mut renderer) = echo_server().await;
    let (state, config_path) = test_state(renderer_port).await;
    let (base, shutdown) = serve(state.clone()).await;
    let (events, mut notify) = echo_server().await;

    let response = raw(
        &base,
        format!(
            "SUBSCRIBE /dmr/event/AVTransport HTTP/1.1\r\nHOST: 127.0.0.1\r\n\
CALLBACK: <http://127.0.0.1:{events}/event>\r\nNT: upnp:event\r\nTIMEOUT: Second-60\r\n\
Connection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    let sid = header(&response, "sid").expect("SUBSCRIBE answers with a SID");
    assert!(sid.starts_with("uuid:"));
    assert_eq!(header(&response, "timeout").as_deref(), Some("Second-60"));

    let initial = recv(&mut notify).await;
    assert!(initial.starts_with("NOTIFY /event HTTP/1.1"), "{initial}");
    assert_eq!(header(&initial, "sid").as_deref(), Some(sid.as_str()));
    assert_eq!(header(&initial, "nts").as_deref(), Some("upnp:propchange"));
    assert_eq!(header(&initial, "nt").as_deref(), Some("upnp:event"));
    assert!(initial.contains("NO_MEDIA_PRESENT"));

    post(
        &base,
        "AVTransport",
        "SetAVTransportURI",
        "<InstanceID>0</InstanceID><CurrentURI>http://192.0.2.99:8000/movie.mp4</CurrentURI>\
<CurrentURIMetaData></CurrentURIMetaData>",
    )
    .await;
    let changed = recv(&mut notify).await;
    assert!(changed.contains("STOPPED"));
    assert!(changed.contains("val=&quot;"));

    let (status, _) = post(&base, "AVTransport", "Play", "<InstanceID>0</InstanceID>").await;
    assert_eq!(status, 200);
    assert!(recv(&mut renderer).await.contains("#SetAVTransportURI"));
    assert!(recv(&mut renderer).await.contains("#Play"));
    assert!(recv(&mut notify).await.contains("TRANSITIONING"));
    assert!(recv(&mut notify).await.contains("PLAYING"));

    shutdown.send(()).unwrap();
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn a_callback_outside_the_lan_never_subscribes() {
    let (renderer_port, _renderer) = echo_server().await;
    let (state, config_path) = test_state(renderer_port).await;
    let (base, shutdown) = serve(state.clone()).await;

    let response = raw(
        &base,
        "SUBSCRIBE /dmr/event/AVTransport HTTP/1.1\r\nHOST: 127.0.0.1\r\n\
CALLBACK: <http://8.8.8.8:1234/event>\r\nNT: upnp:event\r\nConnection: close\r\n\r\n"
            .to_owned(),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 412"), "{response}");

    let response = raw(
        &base,
        "SUBSCRIBE /dmr/event/AVTransport HTTP/1.1\r\nHOST: 127.0.0.1\r\n\
NT: upnp:event\r\nConnection: close\r\n\r\n"
            .to_owned(),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 412"), "{response}");

    shutdown.send(()).unwrap();
    let _ = tokio::fs::remove_file(config_path).await;
}

struct Endpoint {
    port: u16,
}

/// A loopback HTTP peer that records every request and always answers an empty 200.
/// It stands in for both the downstream DLNA renderer and an upstream control point.
async fn echo_server() -> (u16, mpsc::Receiver<String>) {
    let socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let (requests, receiver) = mpsc::channel(16);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let request = read_request(&mut stream).await;
            if requests.send(request).await.is_err() {
                break;
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: 0\r\n\
Connection: close\r\n\r\n",
                )
                .await
                .unwrap();
        }
    });
    (port, receiver)
}

async fn recv(receiver: &mut mpsc::Receiver<String>) -> String {
    tokio::time::timeout(Duration::from_secs(3), receiver.recv())
        .await
        .expect("nothing arrived")
        .expect("channel closed")
}

/// The transport monitor also polls the renderer while a session is live, so an
/// assertion about one onward call has to look past those requests.
async fn recv_until(receiver: &mut mpsc::Receiver<String>, needle: &str) -> String {
    for _ in 0..8 {
        let request = recv(receiver).await;
        if request.contains(needle) {
            return request;
        }
    }
    panic!("never saw {needle}");
}

async fn serve(state: AppState) -> (Endpoint, oneshot::Sender<()>) {
    let app = Router::new().merge(dmr::routes()).with_state(state);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    (Endpoint { port }, shutdown)
}

async fn get(base: &Endpoint, path: &str) -> String {
    let request = format!("GET {path} HTTP/1.1\r\nHOST: 127.0.0.1\r\nConnection: close\r\n\r\n");
    raw(base, request).await
}

async fn post(base: &Endpoint, service: &str, action: &str, args: &str) -> (u16, String) {
    let type_name = format!("{service}:1");
    let body = format!(
        "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">\
<s:Body><u:{action} xmlns:u=\"urn:schemas-upnp-org:service:{type_name}\">{args}</u:{action}>\
</s:Body></s:Envelope>"
    );
    let request = format!(
        "POST /dmr/control/{service} HTTP/1.1\r\nHOST: 127.0.0.1\r\n\
CONTENT-TYPE: text/xml; charset=\"utf-8\"\r\n\
SOAPACTION: \"urn:schemas-upnp-org:service:{type_name}#{action}\"\r\n\
Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let response = raw(base, request).await;
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    (status, response)
}

async fn raw(base: &Endpoint, request: String) -> String {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, base.port))
        .await
        .unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

/// HTTP header names are case-insensitive and hyper always lowers them, so look
/// them up that way rather than matching the wire text.
fn header(message: &str, name: &str) -> Option<String> {
    message.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.trim().to_owned())
    })
}

async fn read_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let (header_end, content_length) = loop {
        let length = stream.read(&mut buffer).await.unwrap();
        assert!(length > 0);
        bytes.extend_from_slice(&buffer[..length]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let end = index + 4;
            let header = String::from_utf8_lossy(&bytes[..end]);
            let content_length = header
                .lines()
                .find_map(|line| {
                    line.split_once(':').and_then(|(name, value)| {
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap_or(0))
                    })
                })
                .unwrap_or(0);
            break (end, content_length);
        }
    };
    while bytes.len() < header_end + content_length {
        let length = stream.read(&mut buffer).await.unwrap();
        assert!(length > 0);
        bytes.extend_from_slice(&buffer[..length]);
    }
    String::from_utf8(bytes).unwrap()
}

async fn test_state(renderer_port: u16) -> (AppState, PathBuf) {
    let config_path = std::env::temp_dir().join(format!(
        "nva2dlna-dmr-test-{}.json",
        Uuid::new_v4().simple()
    ));
    let state = AppState::new(&RuntimeConfig {
        web_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080),
        nva_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9959),
        lelink_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 52288),
        advertise_ip: Ipv4Addr::new(192, 0, 2, 10),
        config_path: config_path.clone(),
        web_dir: "web/dist".into(),
        ffmpeg: "ffmpeg".into(),
        nva_name: "UniNVA".into(),
        dlna_name: "UniDLNA".into(),
        lelink_name: "UniLE".into(),
        device_uuid: Uuid::new_v4(),
        nva_device_uuid: Uuid::new_v4(),
        retired_nva_device_uuid: None,
        selected_udn: None,
        scan_interface_ids: Vec::new(),
    })
    .unwrap();
    let udn = "uuid:fake-renderer".to_owned();
    state
        .replace_renderers(vec![Renderer {
            udn: udn.clone(),
            friendly_name: "Fake TV".into(),
            manufacturer: "Tests".into(),
            model_name: "SOAP Sink".into(),
            location: format!("http://127.0.0.1:{renderer_port}/description.xml"),
            av_transport_url: format!("http://127.0.0.1:{renderer_port}/control"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            av_transport_scpd_url: None,
            rendering_control_url: Some(format!("http://127.0.0.1:{renderer_port}/rcs")),
            rendering_control_service_type: Some(upnp::RENDERING_CONTROL.into()),
            play_speeds: vec![],
            lelink: None,
            address: "127.0.0.1".into(),
            gateway_address: "127.0.0.1".into(),
            discovery_interface_id: "test".into(),
            gateway_prefix_length: 8,
            last_seen_unix_ms: now_ms(),
        }])
        .await;
    state.select_renderer(Some(udn)).await.unwrap();
    (state, config_path)
}
