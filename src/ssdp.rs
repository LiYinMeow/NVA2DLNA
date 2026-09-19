use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::OnceLock,
    time::Duration,
};

use anyhow::{Context, Result};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{net::UdpSocket, sync::watch, time};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{state::AppState, upnp};

const GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 255, 250);
const PORT: u16 = 1900;
const MAX_AGE: u64 = 120;
/// The exact receiver fingerprint used by the working UDashboard NVA implementation.
const SERVER: &str = "UDashboard/0.1 UPnP/1.0 Bilibili-NVA/1.0 DLNA/1.5";
const NVA_CONFIG_ID: u32 = 3;

static PROCESS_BOOT_ID: OnceLock<u32> = OnceLock::new();

#[derive(Clone)]
struct Target {
    st: String,
    usn: String,
    location: String,
}

pub async fn run(
    state: AppState,
    nva_port: u16,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let interface = state.advertise_ip();
    let socket = create_socket(interface)?;
    let targets = targets(&state, nva_port);
    info!(%interface, targets = targets.len(), "NVA SSDP advertiser ready");
    if let Some(retired_uuid) = state.retired_nva_device_uuid() {
        let retired_targets = targets_for_uuid(&state, nva_port, retired_uuid);
        send_byebye_burst(&socket, &retired_targets).await;
        info!(%retired_uuid, "retired the NVA identity previously cached as DLNA");
    }
    send_notifications(&socket, &targets, true).await;

    let mut interval = time::interval(Duration::from_secs(40));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    interval.tick().await;
    let mut buffer = [0_u8; 4096];
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                let (length, peer) = received.context("SSDP receive failed")?;
                if let Some(st) = search_target(&buffer[..length]) {
                    for target in matching_targets(&targets, &st) {
                        let response = search_response(target);
                        if let Err(error) = socket.send_to(response.as_bytes(), peer).await {
                            debug!(%peer, %error, "cannot send SSDP search response");
                        }
                    }
                }
            }
            _ = interval.tick() => {
                send_notifications(&socket, &targets, true).await;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    send_byebye_burst(&socket, &targets).await;
                    info!("NVA SSDP identity retired on shutdown");
                    return Ok(());
                }
            }
        }
    }
}

fn create_socket(interface: Ipv4Addr) -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    let bind_ip = if cfg!(windows) {
        interface
    } else {
        Ipv4Addr::UNSPECIFIED
    };
    socket
        .bind(&SocketAddrV4::new(bind_ip, PORT).into())
        .with_context(|| format!("cannot bind SSDP socket on {bind_ip}:{PORT}"))?;
    socket.join_multicast_v4(&GROUP, &interface)?;
    if let Err(error) = socket.set_multicast_if_v4(&interface) {
        warn!(%error, %interface, "cannot select SSDP multicast interface; using the OS route");
    }
    socket.set_multicast_ttl_v4(2)?;
    socket.set_multicast_loop_v4(cfg!(windows))?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into()).context("cannot create async SSDP socket")
}

/// UniLink has no DLNA input face. Advertising a second generic MediaRenderer from
/// this host makes Bilibili cache that response first and downgrade UniNVA to DLNA.
/// Keep the exact seven-target NVA identity used by the working pre-LeLink build
/// and UDashboard. `NIRVANA_SERVICE` is an XML/SOAP service type, not an SSDP ST.
fn targets(state: &AppState, nva_port: u16) -> Vec<Target> {
    targets_for_uuid(state, nva_port, state.nva_device_uuid())
}

fn targets_for_uuid(state: &AppState, nva_port: u16, device_uuid: Uuid) -> Vec<Target> {
    let interface = state.advertise_ip();
    let uuid = format!("uuid:{}", upnp::nva_tv_id(device_uuid));
    let location = format!("http://{interface}:{nva_port}/description.xml");
    [
        "upnp:rootdevice",
        uuid.as_str(),
        upnp::MEDIA_RENDERER,
        upnp::AV_TRANSPORT,
        upnp::RENDERING_CONTROL,
        upnp::CONNECTION_MANAGER,
        upnp::NIRVANA_DISCOVERY,
    ]
    .into_iter()
    .map(|st| Target {
        st: st.to_owned(),
        usn: if st.eq_ignore_ascii_case(&uuid) {
            uuid.clone()
        } else {
            format!("{uuid}::{st}")
        },
        location: location.clone(),
    })
    .collect()
}

fn search_target(packet: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(packet).ok()?;
    let mut lines = text.lines();
    let request_line = lines.next()?.trim_end_matches('\r');
    let mut request_parts = request_line.split_ascii_whitespace();
    if !request_parts.next()?.eq_ignore_ascii_case("M-SEARCH") || request_parts.next()? != "*" {
        return None;
    }
    let protocol = request_parts.next()?;
    if request_parts.next().is_some()
        || !matches!(
            protocol.to_ascii_uppercase().as_str(),
            "HTTP/1.0" | "HTTP/1.1"
        )
    {
        return None;
    }
    let mut st = None;
    let mut discover = false;
    let mut man_present = false;
    for line in lines {
        let Some((name, value)) = line.trim_end_matches('\r').split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("st") {
            st = Some(value.trim().to_owned());
        } else if name.trim().eq_ignore_ascii_case("man") {
            man_present = true;
            discover = value.to_ascii_lowercase().contains("ssdp:discover");
        }
    }
    let st = st?.trim().to_owned();
    if st.is_empty() || st.len() > 256 || st.chars().any(char::is_control) {
        return None;
    }
    // Deployed Bilibili TV senders also use a legacy HTTP/1.0 probe with the
    // exact Nirvana target and no MAN/MX headers. Keep this narrowly scoped so
    // ordinary SSDP targets still require the standard discovery marker.
    let legacy_nirvana = st.eq_ignore_ascii_case(upnp::NIRVANA_DISCOVERY)
        && (protocol.eq_ignore_ascii_case("HTTP/1.0") || protocol.eq_ignore_ascii_case("HTTP/1.1"))
        && (!man_present || discover);
    let standard_discovery = protocol.eq_ignore_ascii_case("HTTP/1.1") && discover;
    (standard_discovery || legacy_nirvana).then_some(st)
}

fn matching_targets<'a>(targets: &'a [Target], st: &str) -> Vec<&'a Target> {
    if st.eq_ignore_ascii_case("ssdp:all") {
        return targets.iter().collect();
    }
    targets
        .iter()
        .filter(|target| target.st.eq_ignore_ascii_case(st))
        .collect()
}

/// The compatibility headers Bilibili checks before opening `/projection`.
fn compatibility_headers(boot_id: u32) -> String {
    format!("BOOTID.UPNP.ORG: {boot_id}\r\nCONFIGID.UPNP.ORG: {NVA_CONFIG_ID}\r\n")
}

fn search_response(target: &Target) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age={MAX_AGE}\r\nEXT:\r\n\
LOCATION: {}\r\nSERVER: {SERVER}\r\nST: {}\r\nUSN: {}\r\n{}\r\n",
        target.location,
        target.st,
        target.usn,
        compatibility_headers(process_boot_id())
    )
}

async fn send_notifications(socket: &UdpSocket, targets: &[Target], alive: bool) {
    let destination = SocketAddr::V4(SocketAddrV4::new(GROUP, PORT));
    let boot_id = process_boot_id();
    for target in targets {
        let nts = if alive { "ssdp:alive" } else { "ssdp:byebye" };
        let location_headers = if alive {
            format!(
                "CACHE-CONTROL: max-age={MAX_AGE}\r\nLOCATION: {}\r\n",
                target.location
            )
        } else {
            String::new()
        };
        let packet = format!(
            "NOTIFY * HTTP/1.1\r\nHOST: {GROUP}:{PORT}\r\n{location_headers}\
NT: {}\r\nNTS: {nts}\r\nSERVER: {SERVER}\r\nUSN: {}\r\n{}\r\n",
            target.st,
            target.usn,
            compatibility_headers(boot_id)
        );
        let _ = socket.send_to(packet.as_bytes(), destination).await;
    }
}

async fn send_byebye_burst(socket: &UdpSocket, targets: &[Target]) {
    for attempt in 0..3 {
        send_notifications(socket, targets, false).await;
        if attempt < 2 {
            time::sleep(Duration::from_millis(40)).await;
        }
    }
}

fn process_boot_id() -> u32 {
    *PROCESS_BOOT_ID.get_or_init(|| {
        let bytes = Uuid::new_v4().into_bytes();
        (u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) & 0x7fff_ffff).max(1)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_nva_discovery_search() {
        let packet = format!(
            "M-SEARCH * HTTP/1.1\r\nMAN: \"ssdp:discover\"\r\nST: {}\r\n\r\n",
            upnp::NIRVANA_DISCOVERY
        );
        assert_eq!(
            search_target(packet.as_bytes()).as_deref(),
            Some(upnp::NIRVANA_DISCOVERY)
        );
    }

    #[test]
    fn accepts_private_nva_search_without_man_or_mx_on_http_10_and_11() {
        for protocol in ["HTTP/1.0", "HTTP/1.1"] {
            let packet = format!(
                "M-SEARCH * {protocol}\r\nHOST: 239.255.255.250:1900\r\nST: {}\r\n\r\n",
                upnp::NIRVANA_DISCOVERY
            );
            assert_eq!(
                search_target(packet.as_bytes()).as_deref(),
                Some(upnp::NIRVANA_DISCOVERY)
            );
        }

        let service_type = format!(
            "M-SEARCH * HTTP/1.0\r\nST: {}\r\n\r\n",
            upnp::NIRVANA_SERVICE
        );
        assert!(search_target(service_type.as_bytes()).is_none());
    }

    #[test]
    fn does_not_relax_discovery_for_non_nva_http_10_targets() {
        let packet =
            b"M-SEARCH * HTTP/1.0\r\nST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n\r\n";
        assert!(search_target(packet).is_none());

        let packet_with_man = b"M-SEARCH * HTTP/1.0\r\nMAN: \"ssdp:discover\"\r\nST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n\r\n";
        assert!(search_target(packet_with_man).is_none());
    }

    #[test]
    fn response_uses_nva_compatibility_fingerprint() {
        let target = Target {
            st: upnp::NIRVANA_DISCOVERY.into(),
            usn: "uuid:XYTEST::urn:schemas-upnp-org:service:NirvanaControl:3".into(),
            location: "http://192.0.2.10:9959/description.xml".into(),
        };
        let response = search_response(&target);
        assert!(response.contains(&format!("SERVER: {SERVER}")));
        assert!(response.contains("CONFIGID.UPNP.ORG: 3"));
        let boot = response
            .lines()
            .find_map(|line| line.strip_prefix("BOOTID.UPNP.ORG: "))
            .and_then(|value| value.parse::<u32>().ok())
            .expect("positive boot id");
        assert!((1..=0x7fff_ffff).contains(&boot));
    }

    fn group_state() -> AppState {
        use std::{path::PathBuf, sync::LazyLock};
        static STATE: LazyLock<AppState> = LazyLock::new(|| {
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
                lelink_name: "UniLe".into(),
                device_uuid: Uuid::nil(),
                nva_device_uuid: Uuid::nil(),
                retired_nva_device_uuid: None,
                selected_udn: None,
                scan_interface_ids: Vec::new(),
            })
            .expect("test state")
        });
        STATE.clone()
    }

    #[test]
    fn advertises_the_working_seven_target_nva_identity() {
        let state = group_state();
        let targets = targets(&state, 9959);
        let nva_uuid = format!("uuid:{}", upnp::nva_tv_id(state.nva_device_uuid()));
        assert_eq!(targets.len(), 7);
        assert!(
            targets
                .iter()
                .all(|target| target.usn.starts_with(&nva_uuid))
        );
        assert!(
            targets
                .iter()
                .all(|target| { target.location == "http://192.0.2.10:9959/description.xml" })
        );
        let renderer = matching_targets(&targets, upnp::MEDIA_RENDERER);
        assert_eq!(
            renderer.len(),
            1,
            "a generic DMR response would mask UniNVA"
        );
        assert!(matching_targets(&targets, upnp::NIRVANA_SERVICE).is_empty());
        assert_eq!(matching_targets(&targets, upnp::NIRVANA_DISCOVERY).len(), 1);
        assert_eq!(matching_targets(&targets, "ssdp:all").len(), 7);
        assert_eq!(targets[0].st, "upnp:rootdevice");
        assert_eq!(targets[6].st, upnp::NIRVANA_DISCOVERY);
    }

    #[test]
    fn every_advertised_target_carries_the_nva_compatibility_headers() {
        let state = group_state();
        for target in targets(&state, 9959) {
            let response = search_response(&target);
            assert!(response.contains("BOOTID.UPNP.ORG"));
            assert!(response.contains("CONFIGID.UPNP.ORG: 3"));
            assert!(response.ends_with("\r\n\r\n"));
        }
    }
}
