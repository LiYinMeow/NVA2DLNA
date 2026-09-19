//! Drives the 乐播 V1 receiver the way a phone sender does: literal bytes on a
//! kept-alive loopback socket, plus the UDP browse probe, asserting the DLNA renderer
//! downstream receives the relayed SOAP.

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    time::Duration,
};

use nva2dlna::{
    config::RuntimeConfig,
    lelink,
    state::{AppState, Renderer, SessionOrigin, TransportState, now_ms},
    upnp,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::mpsc,
};
use uuid::Uuid;

#[tokio::test]
async fn a_lelink_cast_is_relayed_to_the_selected_dlna_renderer() {
    let (renderer_port, mut renderer) = fake_renderer().await;
    let (state, config_path) = test_state(renderer_port).await;
    let mut phone = connect(&state).await;

    let hello = exchange(&mut phone, "GET /server-info", None, "").await;
    assert!(hello.starts_with("HTTP/1.1 200"), "{hello}");
    // A `401` anywhere in this reply switches the sender into its casting-code flow.
    assert_bare(&hello);

    let play = exchange(
        &mut phone,
        "POST /play",
        Some("text/parameters"),
        "Content-Location: http://192.0.2.99:8000/movie.mp4\r\nStart-Position: 12\r\nContent-URLID: url-1",
    )
    .await;
    assert_bare(&play);

    let set_uri = recv_until(&mut renderer, "#SetAVTransportURI").await;
    assert!(
        set_uri.contains("http://127.0.0.1:8080/media/"),
        "the renderer must be pointed at our own proxy: {set_uri}"
    );
    assert!(set_uri.contains("stream.mp4"));
    recv_until(&mut renderer, "#Play").await;
    let seek = recv_until(&mut renderer, "#Seek").await;
    assert!(
        seek.contains("<Target>00:00:12</Target>"),
        "V1 Start-Position is seconds: {seek}"
    );

    let session = state.session().await.expect("the cast is live");
    assert_eq!(session.origin, SessionOrigin::Lelink);
    assert_eq!(session.speed, "1");

    // V1 has no /pause: /rate?value=0|1.000000 is the pause and resume pair.
    exchange(&mut phone, "POST /rate?value=0.000000", None, "").await;
    recv_until(&mut renderer, "#Pause").await;
    assert_eq!(
        state.lelink().await.transport,
        TransportState::PausedPlayback
    );
    exchange(&mut phone, "POST /rate?value=1.000000", None, "").await;
    recv_until(&mut renderer, "#Play").await;
    assert_eq!(state.lelink().await.transport, TransportState::Playing);

    exchange(&mut phone, "POST /scrub?position=90", None, "").await;
    let seek = recv_until(&mut renderer, "#Seek").await;
    assert!(seek.contains("<Target>00:01:30</Target>"), "{seek}");

    // The only verb whose body the sender reads. Two lines, both in seconds.
    let progress = exchange(&mut phone, "GET /scrub", None, "").await;
    assert!(
        progress.ends_with("duration:296\r\nposition:83"),
        "{progress}"
    );

    exchange(&mut phone, "POST /add_volume", None, "").await;
    let volume = recv_until(&mut renderer, "#SetVolume").await;
    assert!(
        volume.contains("<DesiredVolume>55</DesiredVolume>"),
        "{volume}"
    );
    exchange(&mut phone, "POST /sub_volume", None, "").await;
    let volume = recv_until(&mut renderer, "#SetVolume").await;
    assert!(
        volume.contains("<DesiredVolume>50</DesiredVolume>"),
        "{volume}"
    );

    exchange(&mut phone, "POST /stop", None, "").await;
    recv_until(&mut renderer, "#Stop").await;
    assert!(state.session().await.is_none());
    let sink = state.lelink().await;
    assert!(sink.session_id.is_none());
    assert_eq!(sink.transport, TransportState::Stopped);

    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn a_repeated_play_from_the_same_phone_session_does_not_recast() {
    let (renderer_port, mut renderer) = fake_renderer().await;
    let (state, config_path) = test_state(renderer_port).await;
    let mut phone = connect(&state).await;

    for _ in 0..2 {
        exchange(
            &mut phone,
            "POST /play",
            Some("text/parameters"),
            "Content-Location: http://192.0.2.99:8000/movie.mp4\r\nContent-URLID: url-1",
        )
        .await;
    }
    recv_until(&mut renderer, "#SetAVTransportURI").await;
    recv_until(&mut renderer, "#Play").await;

    // retryPush resends the same request after a slow answer; it must not restart.
    exchange(
        &mut phone,
        "POST /play",
        Some("text/parameters"),
        "Content-Location: http://192.0.2.99:8000/movie.mp4\r\nContent-URLID: url-1",
    )
    .await;
    assert_no_recast(&mut renderer).await;

    // A different video from the same session is a real recast.
    exchange(
        &mut phone,
        "POST /play",
        Some("text/parameters"),
        "Content-Location: http://192.0.2.99:8000/other.mp4\r\nContent-URLID: url-2",
    )
    .await;
    recv_until(&mut renderer, "#SetAVTransportURI").await;

    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn a_json_video_info_cast_and_a_keep_alive_are_both_accepted() {
    let (renderer_port, mut renderer) = fake_renderer().await;
    let (state, config_path) = test_state(renderer_port).await;
    let mut phone = connect(&state).await;

    let body = "{\"mStartPosition\":0,\"playUrl\":\"http://192.0.2.99:8000/show.mp4\",\
\"urlId\":\"j-1\",\"header\":{}}";
    exchange(
        &mut phone,
        "POST /send_videoInfo",
        Some("application/json"),
        body,
    )
    .await;
    recv_until(&mut renderer, "#SetAVTransportURI").await;
    recv_until(&mut renderer, "#Play").await;
    assert_eq!(
        state.session().await.expect("live").title,
        "show.mp4",
        "V1 carries no title, so the file name stands in"
    );

    for _ in 0..3 {
        let feedback = exchange(&mut phone, "POST /feedback", None, "").await;
        assert_bare(&feedback);
    }
    assert!(
        state.session().await.is_some(),
        "keep-alive polls must not disturb the cast"
    );

    // A command for a session that is not there yet answers, and closes nothing.
    exchange(&mut phone, "POST /scrub?position=5", None, "").await;
    assert!(state.session().await.is_some());

    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn browse_answers_a_probe_with_a_v1_record_on_its_third_line() {
    const ADVERTISED_CONTROL_PORT: u16 = 43123;
    let (renderer_port, _renderer) = fake_renderer().await;
    let (state, config_path) = test_state(renderer_port).await;
    let responder = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let phone = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let responder_port = responder.local_addr().unwrap().port();
    tokio::spawn(lelink::browse_on(state, responder, ADVERTISED_CONTROL_PORT));

    phone
        .send_to(b"not a probe at all", ("127.0.0.1", responder_port))
        .await
        .unwrap();
    phone
        .send_to(
            b"magic-number:PTBL\r\nxor-key:0000\r\n{\"type\":\"search\",\"ver\":\"31899\"}",
            ("127.0.0.1", responder_port),
        )
        .await
        .unwrap();

    let mut buffer = [0_u8; 2048];
    let (length, _) = tokio::time::timeout(Duration::from_secs(3), phone.recv_from(&mut buffer))
        .await
        .expect("no browse reply")
        .expect("browse receive failed");
    let reply = String::from_utf8_lossy(&buffer[..length]).into_owned();
    assert!(reply.starts_with("magic-number:LBTP\r\n"), "{reply}");

    // DeviceAdjuster only ever parses split("\r\n")[2].
    let line = reply.split("\r\n").nth(2).expect("a third line");
    let record: serde_json::Value = serde_json::from_str(line).expect("the third line is JSON");
    assert_eq!(
        record["lelinkport"],
        serde_json::json!(ADVERTISED_CONTROL_PORT)
    );
    assert_eq!(
        record["airplay"],
        serde_json::json!(ADVERTISED_CONTROL_PORT),
        "V1 senders take the push port from airplay"
    );
    assert_eq!(record["raop"], serde_json::json!(ADVERTISED_CONTROL_PORT));
    assert_eq!(record["deviceip"], serde_json::json!("192.0.2.10"));
    assert!(record.get("vv").is_none(), "vv=2 would select V2");
    assert!(
        record.get("dlna_mode_desc").is_none(),
        "UniLe is a native input face and must not advertise a removed DLNA sink"
    );
    assert!(record["u"].as_str().unwrap().len() >= 32);

    let _ = tokio::fs::remove_file(config_path).await;
}

/// The phone never inspects the headers it gets back, so any digits in the reply that
/// it does look for are a hazard: 401 starts the casting-code flow, 603/453 fail.
fn assert_bare(reply: &str) {
    for forbidden in ["401", "603", "453"] {
        assert!(
            !reply.contains(forbidden),
            "{forbidden} in a V1 reply is read as: {reply}"
        );
    }
}

async fn assert_no_recast(renderer: &mut mpsc::Receiver<String>) {
    let mut stray = Vec::new();
    while let Ok(Some(request)) =
        tokio::time::timeout(Duration::from_millis(400), renderer.recv()).await
    {
        stray.push(request);
    }
    assert!(
        stray
            .iter()
            .all(|request| !request.contains("#SetAVTransportURI")),
        "a repeated /play recast the media: {stray:?}"
    );
}

async fn recv_until(receiver: &mut mpsc::Receiver<String>, needle: &str) -> String {
    for _ in 0..10 {
        let request = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .expect("nothing arrived from the renderer")
            .expect("channel closed");
        if request.contains(needle) {
            return request;
        }
    }
    panic!("never saw {needle}");
}

async fn connect(state: &AppState) -> TcpStream {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(lelink::run_on(state.clone(), listener));
    TcpStream::connect(("127.0.0.1", port)).await.unwrap()
}

/// One request the way `protocol/i.java` writes it, and the reply that comes back.
async fn exchange(
    stream: &mut TcpStream,
    target: &str,
    content_type: Option<&str>,
    body: &str,
) -> String {
    let (method, path) = target.split_once(' ').expect("a request line");
    let head = match content_type {
        Some(content_type) => format!(
            "{method} {path} HTTP/1.1\r\nHost: 192.0.2.10:52288\r\n\
x-lelink-session-id: PHONESESSION\r\nContent-Type: {content_type}\r\n\
Content-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
            body.len()
        ),
        None => format!(
            "{method} {path} HTTP/1.1\r\nHost: 192.0.2.10:52288\r\n\
x-lelink-session-id: PHONESESSION\r\nConnection: keep-alive\r\n\r\n"
        ),
    };
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    read_reply(stream).await
}

async fn read_reply(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut chunk))
            .await
            .expect("the receiver never answered")
            .unwrap();
        assert!(read > 0, "the receiver closed the connection");
        bytes.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let Some(index) = text.find("\r\n\r\n") else {
            continue;
        };
        let length = text[..index]
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap_or(0))
                })
            })
            .unwrap_or(0);
        if bytes.len() >= index + 4 + length {
            return text;
        }
    }
}

/// A loopback DLNA renderer that records what it is told and answers position queries,
/// so the seconds-based progress contract can be asserted end to end.
async fn fake_renderer() -> (u16, mpsc::Receiver<String>) {
    let socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let (requests, receiver) = mpsc::channel(32);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let request = read_request(&mut stream).await;
            let position = request.contains("GetPositionInfo");
            if requests.send(request).await.is_err() {
                break;
            }
            let body = if position {
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">\
<s:Body><u:GetPositionInfoResponse \
xmlns:u=\"urn:schemas-upnp-org:service:AVTransport:1\">\n<InstanceID>0</InstanceID>\
<AbsTime>0:01:23</AbsTime><RelTime>0:01:23</RelTime>\
<TrackDuration>0:04:56</TrackDuration><TrackMetaData></TrackMetaData>\
<TrackURI>http://192.0.2.10:8080/media/token/stream.mp4</TrackURI>\
</u:GetPositionInfoResponse></s:Body></s:Envelope>"
            } else {
                ""
            };
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\n\
Connection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).await.unwrap();
        }
    });
    (port, receiver)
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
            let head = String::from_utf8_lossy(&bytes[..end]);
            let content_length = head
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
        "nva2dlna-lelink-test-{}.json",
        Uuid::new_v4().simple()
    ));
    let state = AppState::new(&RuntimeConfig {
        web_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080),
        nva_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9959),
        lelink_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, lelink::CONTROL_PORT),
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
