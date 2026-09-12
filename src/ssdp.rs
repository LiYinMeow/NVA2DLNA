use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::OnceLock,
    time::Duration,
};

use anyhow::{Context, Result};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{net::UdpSocket, time};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{state::AppState, upnp};

const GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 255, 250);
const PORT: u16 = 1900;
const MAX_AGE: u64 = 120;
const SERVER: &str = "UDashboard/0.1 UPnP/1.0 Bilibili-NVA/1.0 DLNA/1.5";
const NVA_CONFIG_ID: u32 = 3;

static PROCESS_BOOT_ID: OnceLock<u32> = OnceLock::new();

#[derive(Clone)]
struct Target {
    st: String,
    usn: String,
}

pub async fn run(state: AppState, nva_port: u16) -> Result<()> {
    let interface = state.advertise_ip();
    let socket = create_socket(interface)?;
    let location = format!("http://{interface}:{nva_port}/description.xml");
    let targets = targets(&state);
    info!(%interface, %location, "NVA SSDP advertiser ready");
    send_notifications(&socket, &targets, &location, true).await;

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
                        let response = search_response(target, &location);
                        if let Err(error) = socket.send_to(response.as_bytes(), peer).await {
                            debug!(%peer, %error, "cannot send SSDP search response");
                        }
                    }
                }
            }
            _ = interval.tick() => {
                send_notifications(&socket, &targets, &location, true).await;
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
    socket.set_multicast_if_v4(&interface)?;
    socket.set_multicast_loop_v4(false)?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into()).context("cannot create async SSDP socket")
}

fn targets(state: &AppState) -> Vec<Target> {
    let id = upnp::nva_tv_id(state.device_uuid());
    let uuid = format!("uuid:{id}");
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
        && protocol.eq_ignore_ascii_case("HTTP/1.0")
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

fn search_response(target: &Target, location: &str) -> String {
    let boot_id = process_boot_id();
    format!(
        "HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age={MAX_AGE}\r\nEXT:\r\n\
LOCATION: {location}\r\nSERVER: {SERVER}\r\nST: {}\r\nUSN: {}\r\n\
BOOTID.UPNP.ORG: {boot_id}\r\nCONFIGID.UPNP.ORG: {NVA_CONFIG_ID}\r\n\r\n",
        target.st, target.usn
    )
}

async fn send_notifications(socket: &UdpSocket, targets: &[Target], location: &str, alive: bool) {
    let destination = SocketAddr::V4(SocketAddrV4::new(GROUP, PORT));
    let boot_id = process_boot_id();
    for target in targets {
        let nts = if alive { "ssdp:alive" } else { "ssdp:byebye" };
        let location_headers = if alive {
            format!("CACHE-CONTROL: max-age={MAX_AGE}\r\nLOCATION: {location}\r\n")
        } else {
            String::new()
        };
        let packet = format!(
            "NOTIFY * HTTP/1.1\r\nHOST: {GROUP}:{PORT}\r\n{location_headers}\
NT: {}\r\nNTS: {nts}\r\nSERVER: {SERVER}\r\nUSN: {}\r\n\
BOOTID.UPNP.ORG: {boot_id}\r\nCONFIGID.UPNP.ORG: {NVA_CONFIG_ID}\r\n\r\n",
            target.st, target.usn
        );
        let _ = socket.send_to(packet.as_bytes(), destination).await;
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
        let packet = b"M-SEARCH * HTTP/1.1\r\nMAN: \"ssdp:discover\"\r\nST: urn:schemas-upnp-org:service:NirvanaControl:3\r\n\r\n";
        assert_eq!(
            search_target(packet).as_deref(),
            Some("urn:schemas-upnp-org:service:NirvanaControl:3")
        );
    }

    #[test]
    fn accepts_exact_legacy_nva_http_10_search_without_man_or_mx() {
        let packet = b"M-SEARCH * HTTP/1.0\r\nHOST: 239.255.255.250:1900\r\nST: urn:schemas-upnp-org:service:NirvanaControl:3\r\n\r\n";
        assert_eq!(
            search_target(packet).as_deref(),
            Some("urn:schemas-upnp-org:service:NirvanaControl:3")
        );
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
        };
        let response = search_response(&target, "http://192.0.2.10:9959/description.xml");
        assert!(response.contains(&format!("SERVER: {SERVER}")));
        assert!(response.contains("CONFIGID.UPNP.ORG: 3"));
        let boot = response
            .lines()
            .find_map(|line| line.strip_prefix("BOOTID.UPNP.ORG: "))
            .and_then(|value| value.parse::<u32>().ok())
            .expect("positive boot id");
        assert!((1..=0x7fff_ffff).contains(&boot));
    }
}
