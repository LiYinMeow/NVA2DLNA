use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};

use nva2dlna::{
    bilibili::{MediaSource, ResolvedMedia},
    config::RuntimeConfig,
    dlna,
    state::{AppState, Renderer, SessionView, SupersededPlay, now_ms},
    upnp,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

#[tokio::test]
async fn set_uri_play_and_stop_are_sent_in_order() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sender, mut receiver) = mpsc::channel(3);
    tokio::spawn(async move {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            sender.send(request).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        }
    });

    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    let udn = "uuid:fake-renderer".to_owned();
    state
        .replace_renderers(vec![Renderer {
            udn: udn.clone(),
            friendly_name: "Fake TV".into(),
            manufacturer: "Tests".into(),
            model_name: "SOAP Sink".into(),
            location: format!("http://127.0.0.1:{port}/description.xml"),
            av_transport_url: format!("http://127.0.0.1:{port}/control"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            rendering_control_url: None,
            rendering_control_service_type: None,
            sink_protocols: vec![],
            address: "127.0.0.1".into(),
            last_seen_unix_ms: now_ms(),
        }])
        .await;
    state.select_renderer(Some(udn)).await.unwrap();

    let play_epoch = state.begin_play_epoch("session-1").await;
    dlna::play(
        state.clone(),
        "session-1",
        ResolvedMedia {
            source: MediaSource::Progressive {
                url: "https://cdn.example.net/video.mp4".into(),
            },
            title: "A & B".into(),
            quality: "80".into(),
            available_qualities: vec![80],
            live: false,
        },
        0,
        play_epoch,
    )
    .await
    .unwrap();
    dlna::stop(state.clone(), Some("session-1")).await.unwrap();

    let set_uri = receiver.recv().await.unwrap();
    let play = receiver.recv().await.unwrap();
    let stop = receiver.recv().await.unwrap();
    assert!(set_uri.contains("#SetAVTransportURI"));
    assert!(set_uri.contains("/media/"));
    assert!(set_uri.contains("A &amp;amp; B"));
    assert!(play.contains("#Play"));
    assert!(stop.contains("#Stop"));
    assert!(state.session().await.is_none());
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn stale_nva_session_cannot_stop_the_active_cast() {
    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    state
        .set_session(Some(SessionView {
            id: "new-session".into(),
            title: "Still playing".into(),
            phase: "playing".into(),
            quality: "80".into(),
            input: "progressive".into(),
            output: "proxy".into(),
            target_name: "Fake TV".into(),
            target_udn: "uuid:fake-renderer".into(),
            started_unix_ms: now_ms(),
            error: None,
            live: false,
        }))
        .await;

    dlna::stop(state.clone(), Some("old-session"))
        .await
        .unwrap();

    assert_eq!(
        state
            .session()
            .await
            .as_ref()
            .map(|session| session.id.as_str()),
        Some("new-session")
    );
    assert!(!state.session_was_terminated("new-session").await);
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn newer_play_and_owner_scoped_stop_supersede_deterministically() {
    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();

    let first = state.begin_play_epoch("session-a").await;
    let second = state.begin_play_epoch("session-b").await;
    assert!(state.ensure_play_epoch(first).is_err());
    assert!(state.ensure_play_epoch(second).is_ok());

    assert!(!state.cancel_pending_play_for(Some("session-a")).await);
    assert!(state.ensure_play_epoch(second).is_ok());
    assert!(state.cancel_pending_play_for(Some("session-b")).await);
    assert!(state.ensure_play_epoch(second).is_err());
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn stop_waiting_behind_set_uri_still_reaches_the_renderer() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (request_sender, mut request_receiver) = mpsc::channel(2);
    let (release_sender, release_receiver) = oneshot::channel();
    tokio::spawn(async move {
        let (mut set_uri_stream, _) = listener.accept().await.unwrap();
        let set_uri = read_http_request(&mut set_uri_stream).await;
        request_sender.send(set_uri).await.unwrap();
        release_receiver.await.unwrap();
        write_empty_soap_response(&mut set_uri_stream).await;

        let (mut stop_stream, _) = listener.accept().await.unwrap();
        let stop = read_http_request(&mut stop_stream).await;
        request_sender.send(stop).await.unwrap();
        write_empty_soap_response(&mut stop_stream).await;
    });

    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    let udn = "uuid:blocked-renderer".to_owned();
    state
        .replace_renderers(vec![Renderer {
            udn: udn.clone(),
            friendly_name: "Blocked TV".into(),
            manufacturer: "Tests".into(),
            model_name: "SOAP Sink".into(),
            location: format!("http://127.0.0.1:{port}/description.xml"),
            av_transport_url: format!("http://127.0.0.1:{port}/control"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            rendering_control_url: None,
            rendering_control_service_type: None,
            sink_protocols: vec![],
            address: "127.0.0.1".into(),
            last_seen_unix_ms: now_ms(),
        }])
        .await;
    state.select_renderer(Some(udn)).await.unwrap();

    let play_epoch = state.begin_play_epoch("session-stop-race").await;
    let play_state = state.clone();
    let play_task = tokio::spawn(async move {
        dlna::play(
            play_state,
            "session-stop-race",
            ResolvedMedia {
                source: MediaSource::Progressive {
                    url: "https://cdn.example.net/video.mp4".into(),
                },
                title: "Stop race".into(),
                quality: "80".into(),
                available_qualities: vec![80],
                live: false,
            },
            0,
            play_epoch,
        )
        .await
    });

    let set_uri = tokio::time::timeout(std::time::Duration::from_secs(2), request_receiver.recv())
        .await
        .expect("SetAVTransportURI timed out")
        .expect("SetAVTransportURI request missing");
    assert!(set_uri.contains("#SetAVTransportURI"));
    assert!(
        state
            .cancel_pending_play_for(Some("session-stop-race"))
            .await
    );
    let stop_state = state.clone();
    let stop_task =
        tokio::spawn(async move { dlna::stop(stop_state, Some("session-stop-race")).await });
    release_sender.send(()).unwrap();

    let play_error = play_task
        .await
        .expect("Play task panicked")
        .expect_err("Play should have been superseded");
    assert!(play_error.downcast_ref::<SupersededPlay>().is_some());
    stop_task
        .await
        .expect("Stop task panicked")
        .expect("Stop failed");
    let stop = tokio::time::timeout(std::time::Duration::from_secs(2), request_receiver.recv())
        .await
        .expect("Stop request timed out")
        .expect("Stop request missing");
    assert!(stop.contains("#Stop"));
    assert!(state.session().await.is_none());
    let _ = tokio::fs::remove_file(config_path).await;
}

fn test_config(config_path: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        web_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080),
        nva_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9959),
        advertise_ip: Ipv4Addr::LOCALHOST,
        config_path,
        web_dir: "web/dist".into(),
        ffmpeg: "ffmpeg".into(),
        friendly_name: "Test NVA".into(),
        device_uuid: Uuid::new_v4(),
        selected_udn: None,
    }
}

async fn read_http_request(stream: &mut TcpStream) -> String {
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
                            .then(|| value.trim().parse::<usize>().unwrap())
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

async fn write_empty_soap_response(stream: &mut TcpStream) {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
}
