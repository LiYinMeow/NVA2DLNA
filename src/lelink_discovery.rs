//! Discovery of native LeLink output targets.
//!
//! A LeLink television normally publishes `_leboremote._tcp.local.` and a DLNA
//! MediaRenderer at the same time. We keep the DLNA endpoint as the stable/selectable
//! target, then attach the native record to it. This avoids duplicate rows in the web
//! UI and lets controls that DLNA cannot express use the LeLink side channel.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use anyhow::{Context, Result};
use mdns_sd::{IfKind, ResolvedService, ServiceDaemon, ServiceEvent};
use tokio::time;
use tracing::{debug, info};

use crate::state::{LelinkEndpoint, Renderer, now_ms};

pub const SERVICE_TYPE: &str = "_leboremote._tcp.local.";
const DISCOVERY_WINDOW: Duration = Duration::from_millis(2300);

#[derive(Clone, Debug)]
pub(crate) struct DiscoveredLelink {
    pub endpoint: LelinkEndpoint,
    /// Some firmwares include the UDN of their DLNA face in TXT. It is used only
    /// to disambiguate several virtual renderers sharing one host.
    pub dlna_udn: Option<String>,
}

/// Browse one mDNS window. Missing TXT ports do not discard a service: the service
/// type itself proves this is a LeLink peer, while individual controls decide whether
/// the optional port they require is available.
pub(crate) async fn scan(local_ip: Ipv4Addr) -> Result<Vec<DiscoveredLelink>> {
    let daemon = ServiceDaemon::new().context("cannot start LeLink mDNS browser")?;
    // mdns-sd enables all adapters by default. Explicitly narrow this daemon to
    // the chosen egress address so overlapping/multi-homed networks cannot leak
    // discoveries into one another.
    daemon
        .disable_interface(IfKind::All)
        .context("cannot disable default mDNS interfaces")?;
    daemon
        .enable_interface(IpAddr::V4(local_ip))
        .context("cannot enable selected mDNS interface")?;
    let receiver = daemon
        .browse(SERVICE_TYPE)
        .context("cannot browse LeLink mDNS services")?;
    let deadline = time::Instant::now() + DISCOVERY_WINDOW;
    let mut peers = HashMap::<(String, Ipv4Addr), DiscoveredLelink>::new();

    loop {
        match time::timeout_at(deadline, receiver.recv_async()).await {
            Ok(Ok(ServiceEvent::ServiceResolved(service))) => {
                collect_resolved(&mut peers, &service, local_ip);
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => break,
        }
    }

    let _ = daemon.stop_browse(SERVICE_TYPE);
    match daemon.shutdown() {
        Ok(status) => match time::timeout(Duration::from_secs(1), status.recv_async()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => debug!(%error, "LeLink mDNS shutdown status channel closed"),
            Err(_) => debug!("timed out waiting for LeLink mDNS shutdown"),
        },
        Err(error) => debug!(%error, "cannot stop LeLink mDNS browser"),
    }
    let peers = peers.into_values().collect::<Vec<_>>();
    info!(interface = %local_ip, count = peers.len(), "LeLink discovery completed");
    Ok(peers)
}

fn collect_resolved(
    peers: &mut HashMap<(String, Ipv4Addr), DiscoveredLelink>,
    service: &ResolvedService,
    local_ip: Ipv4Addr,
) {
    let uid = nonempty(service.get_property_val_str("u"));
    let name = ["devicename", "deviceName", "name"]
        .into_iter()
        .find_map(|key| nonempty(service.get_property_val_str(key)))
        .unwrap_or_else(|| instance_name(service.get_fullname()));
    let identity = uid.clone().unwrap_or_else(|| name.clone());
    let control_port = txt_port(service, "port");
    let main_port = txt_port(service, "lelinkport");
    let dlna_udn = ["dlna_udn_uuid", "dln_UUID", "dlna_uuid"]
        .into_iter()
        .find_map(|key| nonempty(service.get_property_val_str(key)));
    let seen = now_ms();

    for address in service.get_addresses_v4() {
        if address == local_ip || address.is_unspecified() || address.is_multicast() {
            continue;
        }
        let peer = DiscoveredLelink {
            endpoint: LelinkEndpoint {
                uid: uid.clone(),
                name: name.clone(),
                address: address.to_string(),
                control_port,
                main_port,
                last_seen_unix_ms: seen,
            },
            dlna_udn: dlna_udn.clone(),
        };
        peers.insert((identity.clone(), address), peer);
    }
}

fn txt_port(service: &ResolvedService, key: &str) -> Option<u16> {
    service
        .get_property_val_str(key)
        .and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|port| *port != 0)
}

fn nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn instance_name(fullname: &str) -> String {
    let fullname = fullname.trim_end_matches('.');
    let service = SERVICE_TYPE.trim_end_matches('.');
    fullname
        .strip_suffix(service)
        .map(|name| name.trim_end_matches('.'))
        .filter(|name| !name.is_empty())
        .unwrap_or(fullname)
        .to_owned()
}

/// Attach every resolved native service to exactly one DLNA renderer. An IP address
/// is authoritative when it has only one renderer. Hosts with several virtual DMRs
/// require a matching UID/advertised DLNA UDN or a unique normalized name.
pub(crate) fn attach(renderers: &mut [Renderer], peers: Vec<DiscoveredLelink>) -> usize {
    let mut attached = 0;
    for peer in peers {
        let candidates = renderers
            .iter()
            .enumerate()
            .filter(|(_, renderer)| {
                renderer.address == peer.endpoint.address && renderer.lelink.is_none()
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let selected = match candidates.as_slice() {
            [only] => Some(*only),
            [] => None,
            many => best_candidate(renderers, many, &peer),
        };
        let Some(index) = selected else {
            debug!(
                address = %peer.endpoint.address,
                name = %peer.endpoint.name,
                candidates = candidates.len(),
                "LeLink peer has no unambiguous DLNA counterpart"
            );
            continue;
        };
        renderers[index].lelink = Some(peer.endpoint);
        attached += 1;
    }
    attached
}

fn best_candidate(
    renderers: &[Renderer],
    candidates: &[usize],
    peer: &DiscoveredLelink,
) -> Option<usize> {
    let mut scored = candidates
        .iter()
        .map(|index| (*index, match_score(&renderers[*index], peer)))
        .filter(|(_, score)| *score > 0)
        .collect::<Vec<_>>();
    scored.sort_unstable_by(|left, right| right.1.cmp(&left.1));
    let (best_index, best_score) = *scored.first()?;
    if scored.get(1).is_some_and(|(_, score)| *score == best_score) {
        None
    } else {
        Some(best_index)
    }
}

fn match_score(renderer: &Renderer, peer: &DiscoveredLelink) -> u8 {
    if peer
        .dlna_udn
        .as_deref()
        .is_some_and(|udn| same_identifier(udn, &renderer.udn))
    {
        return 100;
    }
    if peer
        .endpoint
        .uid
        .as_deref()
        .is_some_and(|uid| same_identifier(uid, &renderer.udn))
    {
        return 90;
    }
    let peer_name = normalized_name(&peer.endpoint.name);
    let renderer_name = normalized_name(&renderer.friendly_name);
    if !peer_name.is_empty() && peer_name == renderer_name {
        return 80;
    }
    if peer_name.chars().count() >= 4
        && renderer_name.chars().count() >= 4
        && (peer_name.contains(&renderer_name) || renderer_name.contains(&peer_name))
    {
        return 40;
    }
    0
}

fn same_identifier(left: &str, right: &str) -> bool {
    let normalize = |value: &str| {
        let lowercase = value.trim().to_ascii_lowercase();
        let without_urn = lowercase.strip_prefix("urn:").unwrap_or(&lowercase);
        let without_uuid = without_urn.strip_prefix("uuid:").unwrap_or(without_urn);
        without_uuid
            .chars()
            .filter(|character| character.is_ascii_alphanumeric())
            .collect::<String>()
    };
    let left = normalize(left);
    !left.is_empty() && left == normalize(right)
}

fn normalized_name(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upnp;

    fn renderer(udn: &str, name: &str, address: &str) -> Renderer {
        Renderer {
            udn: udn.into(),
            friendly_name: name.into(),
            manufacturer: String::new(),
            model_name: String::new(),
            location: format!("http://{address}/description.xml"),
            av_transport_url: format!("http://{address}/control"),
            av_transport_service_type: upnp::AV_TRANSPORT.into(),
            av_transport_scpd_url: None,
            rendering_control_url: None,
            rendering_control_service_type: None,
            play_speeds: Vec::new(),
            lelink: None,
            address: address.into(),
            gateway_address: "10.0.0.1".into(),
            discovery_interface_id: "test".into(),
            gateway_prefix_length: 24,
            last_seen_unix_ms: 1,
        }
    }

    fn peer(uid: Option<&str>, name: &str, address: &str) -> DiscoveredLelink {
        DiscoveredLelink {
            endpoint: LelinkEndpoint {
                uid: uid.map(str::to_owned),
                name: name.into(),
                address: address.into(),
                control_port: Some(53388),
                main_port: Some(52288),
                last_seen_unix_ms: 2,
            },
            dlna_udn: None,
        }
    }

    #[test]
    fn a_single_dlna_face_on_the_same_ip_is_merged() {
        let mut renderers = vec![renderer("uuid:one", "DLNA name", "10.0.0.8")];
        assert_eq!(
            attach(&mut renderers, vec![peer(None, "LeLink name", "10.0.0.8")]),
            1
        );
        assert_eq!(
            renderers[0]
                .lelink
                .as_ref()
                .and_then(|peer| peer.control_port),
            Some(53388)
        );
    }

    #[test]
    fn a_shared_ip_uses_uid_or_name_instead_of_guessing() {
        let mut renderers = vec![
            renderer("uuid:first", "Bedroom TV", "10.0.0.8"),
            renderer("uuid:second", "Living-room TV", "10.0.0.8"),
        ];
        assert_eq!(
            attach(
                &mut renderers,
                vec![peer(Some("SECOND"), "unrelated", "10.0.0.8")]
            ),
            1
        );
        assert!(renderers[0].lelink.is_none());
        assert!(renderers[1].lelink.is_some());

        let mut ambiguous = vec![
            renderer("uuid:first", "A", "10.0.0.9"),
            renderer("uuid:second", "B", "10.0.0.9"),
        ];
        assert_eq!(attach(&mut ambiguous, vec![peer(None, "C", "10.0.0.9")]), 0);
    }

    #[test]
    fn instance_names_drop_only_the_service_suffix() {
        assert_eq!(
            instance_name("客厅电视._leboremote._tcp.local."),
            "客厅电视"
        );
    }

    #[test]
    fn identifier_prefixes_are_case_insensitive() {
        assert!(same_identifier(
            "URN:UUID:ABCDEF01-2345-6789-ABCD-EF0123456789",
            "uuid:abcdef01-2345-6789-abcd-ef0123456789"
        ));
        assert!(same_identifier("UUID:SECOND", "urn:uuid:second"));
    }
}
