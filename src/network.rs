//! IPv4 interface inventory and outbound discovery-interface selection.
//!
//! Receiver advertisements intentionally keep using `advertise_ip`.  Output
//! discovery has its own persisted adapter selection so a host connected to two
//! LANs can receive a cast on one side and serve the selected renderer on the
//! other side.

use std::{collections::BTreeMap, net::Ipv4Addr};

use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterfaceAddress {
    pub id: String,
    pub name: String,
    pub address: Ipv4Addr,
    pub prefix_length: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InterfaceSummary {
    pub id: String,
    pub name: String,
    pub ipv4: String,
    pub prefix_length: u8,
    pub selected: bool,
    pub up: bool,
}

/// Enumerate usable IPv4 addresses. The adapter name is the persistent id: unlike
/// an IPv4 address, it normally survives DHCP renewal. An adapter with several IPv4
/// addresses is scanned through each address while appearing only once in the UI.
pub fn available_ipv4() -> Result<Vec<InterfaceAddress>> {
    let mut addresses = if_addrs::get_if_addrs()
        .context("cannot enumerate network interfaces")?
        .into_iter()
        .filter_map(|interface| match interface.addr {
            if_addrs::IfAddr::V4(address) if usable_ipv4(address.ip) => {
                let id = if interface.name.trim().is_empty() {
                    address.ip.to_string()
                } else {
                    interface.name.clone()
                };
                Some(InterfaceAddress {
                    id,
                    name: interface.name,
                    address: address.ip,
                    prefix_length: prefix_length(address.netmask),
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    addresses.sort_by(|left, right| {
        left.id
            .to_lowercase()
            .cmp(&right.id.to_lowercase())
            .then_with(|| left.address.octets().cmp(&right.address.octets()))
    });
    addresses.dedup_by(|left, right| left.id == right.id && left.address == right.address);
    Ok(addresses)
}

pub fn interface_summaries(
    addresses: &[InterfaceAddress],
    selected_ids: &[String],
    advertise_ip: Ipv4Addr,
) -> Vec<InterfaceSummary> {
    let effective = select_addresses(addresses, selected_ids, advertise_ip);
    let selected = effective
        .iter()
        .map(|interface| interface.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut grouped = BTreeMap::<String, &InterfaceAddress>::new();
    for interface in addresses {
        let replace = grouped.get(&interface.id).is_some_and(|current| {
            current.address != advertise_ip && interface.address == advertise_ip
        });
        if replace || !grouped.contains_key(&interface.id) {
            grouped.insert(interface.id.clone(), interface);
        }
    }
    grouped
        .into_values()
        .map(|interface| InterfaceSummary {
            id: interface.id.clone(),
            name: interface.name.clone(),
            ipv4: interface.address.to_string(),
            prefix_length: interface.prefix_length,
            selected: selected.contains(interface.id.as_str()),
            up: true,
        })
        .collect()
}

/// Resolve persisted adapter ids to every usable address on those adapters.
///
/// An empty selection is automatic mode and scans all LAN adapters. If every
/// explicitly selected adapter disappeared (for example it was renamed), fall back
/// to the receiver address instead of silently disabling output discovery.
pub fn select_addresses(
    addresses: &[InterfaceAddress],
    selected_ids: &[String],
    advertise_ip: Ipv4Addr,
) -> Vec<InterfaceAddress> {
    let mut selected = if selected_ids.is_empty() {
        addresses.to_vec()
    } else {
        addresses
            .iter()
            .filter(|interface| selected_ids.iter().any(|id| id == &interface.id))
            .cloned()
            .collect::<Vec<_>>()
    };
    if selected.is_empty() {
        selected = addresses
            .iter()
            .filter(|interface| interface.address == advertise_ip)
            .cloned()
            .collect();
    }
    if selected.is_empty() && usable_ipv4(advertise_ip) {
        selected.push(InterfaceAddress {
            id: advertise_ip.to_string(),
            name: "接收网卡".into(),
            address: advertise_ip,
            prefix_length: 32,
        });
    }
    selected
}

pub fn receive_interface_id(
    addresses: &[InterfaceAddress],
    advertise_ip: Ipv4Addr,
) -> Option<String> {
    addresses
        .iter()
        .find(|interface| interface.address == advertise_ip)
        .map(|interface| interface.id.clone())
}

pub fn usable_ipv4(ip: Ipv4Addr) -> bool {
    !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_multicast()
        && !ip.is_broadcast()
        && !ip.is_link_local()
}

fn prefix_length(mask: Ipv4Addr) -> u8 {
    u32::from(mask).count_ones() as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(id: &str, ip: [u8; 4], prefix_length: u8) -> InterfaceAddress {
        InterfaceAddress {
            id: id.into(),
            name: id.into(),
            address: Ipv4Addr::from(ip),
            prefix_length,
        }
    }

    #[test]
    fn automatic_mode_scans_every_usable_adapter_address() {
        let available = vec![
            address("lan-a", [10, 0, 0, 2], 24),
            address("lan-b", [192, 168, 8, 2], 24),
        ];
        assert_eq!(
            select_addresses(&available, &[], Ipv4Addr::new(10, 0, 0, 2)),
            available
        );
    }

    #[test]
    fn selecting_an_adapter_uses_all_of_its_ipv4_addresses() {
        let available = vec![
            address("lan-a", [10, 0, 0, 2], 24),
            address("lan-a", [10, 0, 1, 2], 24),
            address("lan-b", [192, 168, 8, 2], 24),
        ];
        let selected =
            select_addresses(&available, &["lan-a".into()], Ipv4Addr::new(192, 168, 8, 2));
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|item| item.id == "lan-a"));
    }

    #[test]
    fn a_stale_selection_falls_back_to_the_receive_adapter() {
        let receive = address("lan-now", [10, 0, 0, 2], 24);
        assert_eq!(
            select_addresses(
                std::slice::from_ref(&receive),
                &["lan-old".into()],
                receive.address,
            ),
            vec![receive]
        );
    }
}
