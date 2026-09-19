use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};

use nva2dlna::{
    bilibili::{MediaSource, ResolvedMedia},
    config::RuntimeConfig,
    dlna,
    state::{
        AppState, LelinkEndpoint, MediaInput, Renderer, SessionBackend, SessionOrigin, SessionView,
        SupersededPlay, now_ms,
    },
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
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
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

    let play_epoch = state.begin_play_epoch("session-1").await;
    dlna::play(
        state.clone(),
        "session-1",
        SessionOrigin::Nva,
        ResolvedMedia {
            source: MediaSource::Progressive {
                url: "https://cdn.example.net/video.mp4".into(),
            },
            title: "A & B".into(),
            quality: "80".into(),
            available_qualities: vec![80],
            duration_ms: Some(296_789),
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
    assert!(set_uri.contains("duration=&quot;00:04:56.789&quot;"));
    assert!(play.contains("#Play"));
    assert!(stop.contains("#Stop"));
    assert!(state.session().await.is_none());
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn seeking_a_dash_remux_replaces_the_stream_at_the_requested_offset() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sender, mut receiver) = mpsc::channel(5);
    tokio::spawn(async move {
        for _ in 0..5 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            sender.send(request).await.unwrap();
            write_empty_soap_response(&mut stream).await;
        }
    });

    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    let udn = "uuid:dash-seek-renderer".to_owned();
    state
        .replace_renderers(vec![Renderer {
            udn: udn.clone(),
            friendly_name: "Seek TV".into(),
            manufacturer: "Tests".into(),
            model_name: "SOAP Sink".into(),
            location: format!("http://127.0.0.1:{port}/description.xml"),
            av_transport_url: format!("http://127.0.0.1:{port}/control"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
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

    let play_epoch = state.begin_play_epoch("dash-seek").await;
    dlna::play(
        state.clone(),
        "dash-seek",
        SessionOrigin::Nva,
        ResolvedMedia {
            source: MediaSource::Dash {
                video_url: "https://cdn.example.net/video.m4s".into(),
                video_backup_urls: Vec::new(),
                audio_url: "https://cdn.example.net/audio.m4s".into(),
                audio_backup_urls: Vec::new(),
            },
            title: "Seekable DASH".into(),
            quality: "120".into(),
            available_qualities: vec![120, 80],
            duration_ms: Some(120_000),
            live: false,
        },
        0,
        play_epoch,
    )
    .await
    .unwrap();
    let old_entry = state.media_for_owner("dash-seek").await.unwrap();

    dlna::seek(state.clone(), "dash-seek", 42_999)
        .await
        .unwrap();
    let new_entry = state.media_for_owner("dash-seek").await.unwrap();
    assert_ne!(old_entry.token, new_entry.token);
    assert!(old_entry.cancellation.is_cancelled());
    assert_eq!(new_entry.start_offset_ms, 42_999);
    assert_eq!(new_entry.duration_ms, Some(120_000));
    assert!(matches!(new_entry.input, MediaInput::Dash { .. }));

    let requests = [
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
    ];
    assert!(requests[0].contains("#SetAVTransportURI"));
    assert!(requests[1].contains("#Play"));
    assert!(requests[2].contains("#Stop"));
    assert!(requests[3].contains("#SetAVTransportURI"));
    assert!(requests[3].contains("00:01:17.001"));
    assert!(requests[4].contains("#Play"));
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn failed_native_lelink_play_falls_back_to_dlna_for_the_whole_session() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sender, mut receiver) = mpsc::channel(4);
    tokio::spawn(async move {
        for attempt in 0..4 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            sender.send(request).await.unwrap();
            let status = if attempt == 0 {
                "500 Native Play Rejected"
            } else {
                "200 OK"
            };
            stream
                .write_all(
                    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
        }
    });

    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    let udn = "uuid:lelink-fallback".to_owned();
    state
        .replace_renderers(vec![Renderer {
            udn: udn.clone(),
            friendly_name: "Hybrid TV".into(),
            manufacturer: "Tests".into(),
            model_name: "LeLink + DLNA".into(),
            location: format!("http://127.0.0.1:{port}/description.xml"),
            av_transport_url: format!("http://127.0.0.1:{port}/control"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
            play_speeds: vec!["1".into(), "1.5".into()],
            lelink: Some(LelinkEndpoint {
                uid: Some("hybrid-tv".into()),
                name: "Hybrid TV".into(),
                address: Ipv4Addr::LOCALHOST.to_string(),
                control_port: Some(port),
                main_port: Some(port),
                last_seen_unix_ms: now_ms(),
            }),
            address: Ipv4Addr::LOCALHOST.to_string(),
            gateway_address: Ipv4Addr::LOCALHOST.to_string(),
            discovery_interface_id: "test".into(),
            gateway_prefix_length: 8,
            last_seen_unix_ms: now_ms(),
        }])
        .await;
    state.select_renderer(Some(udn)).await.unwrap();

    let play_epoch = state.begin_play_epoch("fallback-session").await;
    dlna::play(
        state.clone(),
        "fallback-session",
        SessionOrigin::Nva,
        ResolvedMedia {
            source: MediaSource::Progressive {
                url: "https://cdn.example.net/video.mp4".into(),
            },
            title: "Fallback".into(),
            quality: "80".into(),
            available_qualities: vec![80],
            duration_ms: None,
            live: false,
        },
        0,
        play_epoch,
    )
    .await
    .unwrap();

    assert!(receiver.recv().await.unwrap().starts_with("POST /play "));
    assert!(
        receiver
            .recv()
            .await
            .unwrap()
            .contains("#SetAVTransportURI")
    );
    assert!(receiver.recv().await.unwrap().contains("#Play"));
    assert_eq!(
        state.session().await.map(|session| session.backend),
        Some(SessionBackend::Dlna)
    );
    assert!(dlna::target_accepts_speed(&state, "1.5").await);
    dlna::set_speed(state.clone(), "fallback-session", "1.5")
        .await
        .unwrap();
    let speed = receiver.recv().await.unwrap();
    assert!(speed.contains("#Play"));
    assert!(speed.contains("<Speed>1.5</Speed>"));
    state.revoke_media_for("fallback-session").await;
    state.set_session(None).await;
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn native_lelink_session_keeps_all_transport_controls_on_v1() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sender, mut receiver) = mpsc::channel(6);
    tokio::spawn(async move {
        for _ in 0..6 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            let is_scrub_query = request.starts_with("GET /scrub ");
            sender.send(request).await.unwrap();
            let body = if is_scrub_query {
                "duration: 120.0\nposition: 5.5\n"
            } else {
                ""
            };
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/parameters\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });

    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    let udn = "uuid:native-lelink".to_owned();
    state
        .replace_renderers(vec![Renderer {
            udn: udn.clone(),
            friendly_name: "Native TV".into(),
            manufacturer: "Tests".into(),
            model_name: "LeLink V1".into(),
            location: format!("http://127.0.0.1:{port}/description.xml"),
            av_transport_url: format!("http://127.0.0.1:{port}/must-not-be-used"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
            play_speeds: vec!["1".into()],
            lelink: Some(LelinkEndpoint {
                uid: Some("native-tv".into()),
                name: "Native TV".into(),
                address: Ipv4Addr::LOCALHOST.to_string(),
                control_port: None,
                main_port: Some(port),
                last_seen_unix_ms: now_ms(),
            }),
            address: Ipv4Addr::LOCALHOST.to_string(),
            gateway_address: Ipv4Addr::LOCALHOST.to_string(),
            discovery_interface_id: "test".into(),
            gateway_prefix_length: 8,
            last_seen_unix_ms: now_ms(),
        }])
        .await;
    state.select_renderer(Some(udn)).await.unwrap();

    let play_epoch = state.begin_play_epoch("native-session").await;
    dlna::play(
        state.clone(),
        "native-session",
        SessionOrigin::Nva,
        ResolvedMedia {
            source: MediaSource::Progressive {
                url: "https://cdn.example.net/video.mp4".into(),
            },
            title: "Native".into(),
            quality: "80".into(),
            available_qualities: vec![80],
            duration_ms: None,
            live: false,
        },
        0,
        play_epoch,
    )
    .await
    .unwrap();
    assert_eq!(
        state.session().await.map(|session| session.backend),
        Some(SessionBackend::LelinkV1)
    );
    dlna::pause(state.clone(), "native-session").await.unwrap();
    dlna::resume(state.clone(), "native-session").await.unwrap();
    dlna::seek(state.clone(), "native-session", 42_999)
        .await
        .unwrap();
    assert_eq!(
        dlna::position(state.clone(), "native-session")
            .await
            .unwrap(),
        (5_500, 120_000)
    );
    dlna::stop(state.clone(), Some("native-session"))
        .await
        .unwrap();

    let requests = [
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
        receiver.recv().await.unwrap(),
    ];
    assert!(requests[0].starts_with("POST /play "));
    assert!(requests[1].starts_with("POST /rate?value=0.000000 "));
    assert!(requests[2].starts_with("POST /rate?value=1.000000 "));
    assert!(requests[3].starts_with("POST /scrub?position=42 "));
    assert!(requests[4].starts_with("GET /scrub "));
    assert!(requests[5].starts_with("POST /stop "));
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("SOAPAction"))
    );
    let _ = tokio::fs::remove_file(config_path).await;
}

#[tokio::test]
async fn a_renderer_that_refuses_the_first_cast_command_is_retried() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sender, mut receiver) = mpsc::channel(4);
    let fault_body = "<?xml version=\"1.0\"?><s:Envelope \
xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><s:Fault>\
<faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail>\
<UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>701</errorCode>\
<errorDescription>renderer is waking up</errorDescription></UPnPError></detail>\
</s:Fault></s:Body></s:Envelope>";
    tokio::spawn(async move {
        for attempt in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            sender.send(request).await.unwrap();
            let response = if attempt == 0 {
                format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/xml\r\n\
Content-Length: {}\r\nConnection: close\r\n\r\n{fault_body}",
                    fault_body.len()
                )
            } else {
                "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: 0\r\n\
Connection: close\r\n\r\n"
                    .to_owned()
            };
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    let udn = "uuid:flaky-renderer".to_owned();
    state
        .replace_renderers(vec![Renderer {
            udn: udn.clone(),
            friendly_name: "Flaky TV".into(),
            manufacturer: "Tests".into(),
            model_name: "SOAP Sink".into(),
            location: format!("http://127.0.0.1:{port}/description.xml"),
            av_transport_url: format!("http://127.0.0.1:{port}/control"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
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

    let play_epoch = state.begin_play_epoch("session-flaky").await;
    dlna::play(
        state.clone(),
        "session-flaky",
        SessionOrigin::Nva,
        ResolvedMedia {
            source: MediaSource::Progressive {
                url: "https://cdn.example.net/video.mp4".into(),
            },
            title: "Retry me".into(),
            quality: "80".into(),
            available_qualities: vec![80],
            duration_ms: None,
            live: false,
        },
        0,
        play_epoch,
    )
    .await
    .expect("the cast should succeed once the renderer answers");

    let first = receiver.recv().await.unwrap();
    let second = receiver.recv().await.unwrap();
    let third = receiver.recv().await.unwrap();
    // hyper emits lower-cased header names and HTTP names are case-insensitive.
    assert!(
        first.to_ascii_lowercase().contains("user-agent: upnp/1.0"),
        "{first}"
    );
    assert!(first.contains("#SetAVTransportURI"));
    assert!(
        second.contains("#SetAVTransportURI"),
        "the refusal must be retried"
    );
    assert!(third.contains("#Play"));
    assert!(state.session().await.is_some());
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
            origin: SessionOrigin::Nva,
            title: "Still playing".into(),
            phase: "playing".into(),
            quality: "80".into(),
            speed: "1".into(),
            input: "progressive".into(),
            output: "proxy".into(),
            backend: SessionBackend::Dlna,
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
async fn stop_cancels_a_replacement_registered_while_waiting_for_the_operation_lock() {
    let config_path =
        std::env::temp_dir().join(format!("nva2dlna-test-{}.json", Uuid::new_v4().simple()));
    let state = AppState::new(&test_config(config_path.clone())).unwrap();
    let operation_guard = state.operation().await;
    let stop_state = state.clone();
    let stop_task =
        tokio::spawn(async move { dlna::stop(stop_state, Some("seek-stop-race")).await });

    // Stop performs its first pending-play cancellation before it waits for the
    // operation lock. Register the replacement only after it has reached that wait.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let replacement_epoch = state.begin_play_epoch("seek-stop-race").await;
    assert!(state.ensure_play_epoch(replacement_epoch).is_ok());
    drop(operation_guard);

    stop_task.await.unwrap().unwrap();
    assert!(state.ensure_play_epoch(replacement_epoch).is_err());
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
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
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

    let play_epoch = state.begin_play_epoch("session-stop-race").await;
    let play_state = state.clone();
    let play_task = tokio::spawn(async move {
        dlna::play(
            play_state,
            "session-stop-race",
            SessionOrigin::Nva,
            ResolvedMedia {
                source: MediaSource::Progressive {
                    url: "https://cdn.example.net/video.mp4".into(),
                },
                title: "Stop race".into(),
                quality: "80".into(),
                available_qualities: vec![80],
                duration_ms: None,
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
        lelink_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 52288),
        advertise_ip: Ipv4Addr::LOCALHOST,
        config_path,
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
