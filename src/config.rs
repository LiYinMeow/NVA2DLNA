use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

/// Increment when the wire-facing NVA identity has to be reclassified by senders.
pub const CURRENT_NVA_IDENTITY_VERSION: u32 = 1;

#[derive(Clone, Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    /// Web UI and LAN media HTTP listener (legacy combined form).
    #[arg(long, env = "NVA2DLNA_WEB_LISTEN", default_value = "0.0.0.0:8080")]
    pub web_listen: SocketAddrV4,

    /// Web HTTP listener IPv4. `*` or an empty value listens on every interface.
    /// When set, this overrides only the IP part of --web-listen.
    #[arg(
        long,
        env = "NVA2DLNA_WEB_IP",
        value_name = "IP|*",
        value_parser = parse_web_ip
    )]
    pub web_ip: Option<Ipv4Addr>,

    /// Web HTTP listener port. When set, this overrides only the port part of
    /// --web-listen.
    #[arg(
        long,
        env = "NVA2DLNA_WEB_PORT",
        value_name = "PORT",
        value_parser = parse_web_port
    )]
    pub web_port: Option<u16>,

    /// NVA control and device-description listener.
    #[arg(long, env = "NVA2DLNA_NVA_LISTEN", default_value = "0.0.0.0:9958")]
    pub nva_listen: SocketAddrV4,

    /// Preferred LeLink native V1 receiver address. If the port is already used,
    /// an available port is selected and published by the UDP 25353 browse responder.
    #[arg(long, env = "NVA2DLNA_LELINK_LISTEN", default_value = "0.0.0.0:52288")]
    pub lelink_listen: SocketAddrV4,

    /// IPv4 address advertised to phones and DLNA renderers. Auto-detected when omitted.
    #[arg(long, env = "NVA2DLNA_ADVERTISE_IP")]
    pub advertise_ip: Option<Ipv4Addr>,

    /// JSON file containing the stable receiver ID and selected DLNA target.
    #[arg(long, env = "NVA2DLNA_CONFIG", default_value = "data/config.json")]
    pub config: PathBuf,

    /// Directory containing the built Vite site.
    #[arg(long, env = "NVA2DLNA_WEB_DIR", default_value = "web/dist")]
    pub web_dir: PathBuf,

    /// FFmpeg executable used for DASH video/audio remuxing.
    #[arg(long, env = "NVA2DLNA_FFMPEG", default_value = "ffmpeg")]
    pub ffmpeg: PathBuf,

    /// What a Bilibili phone shows for the NVA TV face.
    #[arg(long, visible_alias = "friendly-name", env = "NVA2DLNA_NVA_NAME")]
    pub nva_name: Option<String>,

    /// Reserved for compatibility with configurations that used the removed DLNA
    /// input face. DLNA remains available as an output target.
    #[arg(long, env = "NVA2DLNA_DLNA_NAME", default_value = "UniDLNA")]
    pub dlna_name: String,

    /// What a 乐播 sender shows for the native receiver face.
    #[arg(long, env = "NVA2DLNA_LELINK_NAME", default_value = "UniLe")]
    pub lelink_name: String,
}

impl Cli {
    /// Resolve the legacy combined listener with the independently configurable
    /// IP and port. Keeping this merge in one place ensures the HTTP server,
    /// media URLs, discovery filters, and startup log all use the same address.
    pub fn resolved_web_listen(&self) -> SocketAddrV4 {
        resolve_web_listen(self.web_listen, self.web_ip, self.web_port)
    }
}

fn resolve_web_listen(base: SocketAddrV4, ip: Option<Ipv4Addr>, port: Option<u16>) -> SocketAddrV4 {
    SocketAddrV4::new(ip.unwrap_or(*base.ip()), port.unwrap_or(base.port()))
}

fn parse_web_ip(value: &str) -> std::result::Result<Ipv4Addr, String> {
    let value = value.trim();
    if value.is_empty() || value == "*" {
        return Ok(Ipv4Addr::UNSPECIFIED);
    }
    value.parse::<Ipv4Addr>().map_err(|_| {
        format!("invalid Web listener IPv4 address `{value}`; use `*` for all interfaces")
    })
}

fn parse_web_port(value: &str) -> std::result::Result<u16, String> {
    let port = value
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("invalid Web listener port `{value}`"))?;
    if port == 0 {
        return Err("Web listener port must be between 1 and 65535".into());
    }
    Ok(port)
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub web_listen: SocketAddrV4,
    pub nva_listen: SocketAddrV4,
    pub lelink_listen: SocketAddrV4,
    pub advertise_ip: Ipv4Addr,
    pub config_path: PathBuf,
    pub web_dir: PathBuf,
    pub ffmpeg: PathBuf,
    pub nva_name: String,
    pub dlna_name: String,
    pub lelink_name: String,
    pub device_uuid: Uuid,
    pub nva_device_uuid: Uuid,
    pub retired_nva_device_uuid: Option<Uuid>,
    pub selected_udn: Option<String>,
    /// Adapter names used only for outbound SSDP/mDNS discovery. Empty means
    /// automatic mode (all currently usable LAN adapters).
    pub scan_interface_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedConfig {
    pub device_uuid: Uuid,
    #[serde(default)]
    pub nva_identity_version: u32,
    /// Dedicated identity for the NVA face. Older configs do not have it; they
    /// are migrated once so phones discard a UDN previously cached as DLNA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nva_device_uuid: Option<Uuid>,
    /// Kept deliberately after migration so every later start can repeat the
    /// small SSDP byebye burst. Some senders retain a stale classification
    /// across application restarts and may have missed the first multicast.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_nva_device_uuid: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_udn: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scan_interface_ids: Vec<String>,
}

/// Receiver discovery records always publish `advertise_ip`. A listener bound to
/// another concrete address would therefore produce a device that is discoverable
/// but impossible to connect to. Wildcard listeners are valid because they accept
/// connections arriving on the advertised interface.
pub fn validate_receiver_listeners(
    advertise_ip: Ipv4Addr,
    nva_listen: SocketAddrV4,
    lelink_listen: SocketAddrV4,
) -> Result<()> {
    validate_receiver_listener("NVA", advertise_ip, nva_listen)?;
    validate_receiver_listener("LeLink", advertise_ip, lelink_listen)
}

fn validate_receiver_listener(
    protocol: &str,
    advertise_ip: Ipv4Addr,
    listen: SocketAddrV4,
) -> Result<()> {
    if listen.ip().is_unspecified() || *listen.ip() == advertise_ip {
        return Ok(());
    }
    bail!(
        "{protocol} 监听地址 {} 与公告地址 {advertise_ip} 不一致；{protocol} 监听 IP 必须是 0.0.0.0 或公告地址 {advertise_ip}",
        listen.ip()
    )
}

impl PersistedConfig {
    pub async fn load_or_create(path: &Path) -> Result<Self> {
        match fs::read(path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("cannot parse {}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let backup = path.with_extension("json.bak");
                match fs::read(&backup).await {
                    Ok(bytes) => {
                        let config = serde_json::from_slice(&bytes)
                            .with_context(|| format!("cannot parse {}", backup.display()))?;
                        fs::rename(&backup, path).await.with_context(|| {
                            format!(
                                "cannot recover {} from {}",
                                path.display(),
                                backup.display()
                            )
                        })?;
                        return Ok(config);
                    }
                    Err(backup_error) if backup_error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(backup_error) => {
                        return Err(backup_error)
                            .with_context(|| format!("cannot read {}", backup.display()));
                    }
                }
                let config = Self {
                    device_uuid: Uuid::new_v4(),
                    nva_identity_version: CURRENT_NVA_IDENTITY_VERSION,
                    nva_device_uuid: Some(Uuid::new_v4()),
                    retired_nva_device_uuid: None,
                    selected_udn: None,
                    scan_interface_ids: Vec::new(),
                };
                config.save(path).await?;
                Ok(config)
            }
            Err(error) => Err(error).with_context(|| format!("cannot read {}", path.display())),
        }
    }

    pub fn current_nva_device_uuid(&self) -> Uuid {
        self.nva_device_uuid.unwrap_or(self.device_uuid)
    }

    /// Give the NVA endpoint a fresh, independently persistent UDN when its
    /// wire profile changes. The shared device UUID remains untouched because
    /// LeLink and the optional standards-only DMR derive identities from it.
    pub async fn migrate_nva_identity(&mut self, path: &Path) -> Result<Option<Uuid>> {
        if self.nva_device_uuid.is_some()
            && self.nva_identity_version >= CURRENT_NVA_IDENTITY_VERSION
        {
            return Ok(None);
        }
        let retired = self.current_nva_device_uuid();
        self.nva_device_uuid = Some(Uuid::new_v4());
        self.retired_nva_device_uuid = Some(retired);
        self.nva_identity_version = CURRENT_NVA_IDENTITY_VERSION;
        self.save(path).await?;
        Ok(Some(retired))
    }

    pub async fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        let temporary = path.with_extension("json.tmp");
        let backup = path.with_extension("json.bak");
        fs::write(&temporary, bytes)
            .await
            .with_context(|| format!("cannot write {}", temporary.display()))?;
        if fs::try_exists(path).await.unwrap_or(false) {
            if fs::try_exists(&backup).await.unwrap_or(false) {
                fs::remove_file(&backup)
                    .await
                    .with_context(|| format!("cannot remove stale {}", backup.display()))?;
            }
            fs::rename(path, &backup)
                .await
                .with_context(|| format!("cannot back up {}", path.display()))?;
        }
        if let Err(error) = fs::rename(&temporary, path).await {
            if fs::try_exists(&backup).await.unwrap_or(false) {
                let _ = fs::rename(&backup, path).await;
            }
            return Err(error).with_context(|| format!("cannot commit {}", path.display()));
        }
        if fs::try_exists(&backup).await.unwrap_or(false) {
            let _ = fs::remove_file(backup).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn web_ip_accepts_wildcards_and_ipv4_only() {
        assert_eq!(parse_web_ip("*").unwrap(), Ipv4Addr::UNSPECIFIED);
        assert_eq!(parse_web_ip("").unwrap(), Ipv4Addr::UNSPECIFIED);
        assert_eq!(parse_web_ip("0.0.0.0").unwrap(), Ipv4Addr::UNSPECIFIED);
        assert_eq!(
            parse_web_ip("192.168.50.8").unwrap(),
            Ipv4Addr::new(192, 168, 50, 8)
        );
        assert!(parse_web_ip("localhost").is_err());
        assert!(parse_web_ip("::").is_err());
    }

    #[test]
    fn split_web_settings_override_the_legacy_listener_by_component() {
        let defaults = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080);
        assert_eq!(
            resolve_web_listen(defaults, None, None),
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080)
        );

        assert_eq!(
            resolve_web_listen(defaults, Some(Ipv4Addr::new(10, 42, 10, 78)), None),
            SocketAddrV4::new(Ipv4Addr::new(10, 42, 10, 78), 8080)
        );

        assert_eq!(
            resolve_web_listen(defaults, None, Some(18080)),
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 18080)
        );

        let legacy = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9000);
        assert_eq!(
            resolve_web_listen(legacy, Some(Ipv4Addr::UNSPECIFIED), Some(28080)),
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 28080)
        );
    }

    #[test]
    fn split_web_port_rejects_zero_and_out_of_range_values() {
        assert!(parse_web_port("0").is_err());
        assert!(parse_web_port("65536").is_err());
        assert_eq!(parse_web_port("65535").unwrap(), 65535);
    }

    #[test]
    fn receiver_listeners_accept_wildcard_or_the_advertised_address() {
        let advertised = Ipv4Addr::new(192, 168, 1, 20);
        assert!(
            validate_receiver_listeners(
                advertised,
                SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9958),
                SocketAddrV4::new(advertised, 52288),
            )
            .is_ok()
        );
    }

    #[test]
    fn receiver_listener_mismatch_identifies_nva_and_lelink_separately() {
        let advertised = Ipv4Addr::new(192, 168, 1, 20);
        let other = Ipv4Addr::new(192, 168, 2, 20);
        let nva_error = validate_receiver_listeners(
            advertised,
            SocketAddrV4::new(other, 9958),
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 52288),
        )
        .unwrap_err()
        .to_string();
        assert!(nva_error.contains("NVA 监听地址"));

        let lelink_error = validate_receiver_listeners(
            advertised,
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9958),
            SocketAddrV4::new(other, 52288),
        )
        .unwrap_err()
        .to_string();
        assert!(lelink_error.contains("LeLink 监听地址"));
    }

    #[tokio::test]
    async fn legacy_config_migrates_nva_identity_once_and_preserves_target() {
        let legacy_uuid = Uuid::parse_str("fa70189e-d181-4f3d-9e2b-afe904e8565e").unwrap();
        let path =
            std::env::temp_dir().join(format!("nva2dlna-config-migration-{}.json", Uuid::new_v4()));
        let legacy =
            format!("{{\"deviceUuid\":\"{legacy_uuid}\",\"selectedUdn\":\"uuid:renderer\"}}");
        fs::write(&path, legacy).await.unwrap();

        let mut config = PersistedConfig::load_or_create(&path).await.unwrap();
        assert_eq!(config.nva_device_uuid, None);
        assert_eq!(config.nva_identity_version, 0);
        assert_eq!(config.selected_udn.as_deref(), Some("uuid:renderer"));
        assert_eq!(
            config.migrate_nva_identity(&path).await.unwrap(),
            Some(legacy_uuid)
        );
        let migrated_uuid = config.current_nva_device_uuid();
        assert_ne!(migrated_uuid, legacy_uuid);
        assert_eq!(config.nva_identity_version, CURRENT_NVA_IDENTITY_VERSION);
        assert_eq!(config.retired_nva_device_uuid, Some(legacy_uuid));

        let mut reloaded = PersistedConfig::load_or_create(&path).await.unwrap();
        assert_eq!(reloaded.current_nva_device_uuid(), migrated_uuid);
        assert_eq!(reloaded.retired_nva_device_uuid, Some(legacy_uuid));
        assert_eq!(reloaded.selected_udn.as_deref(), Some("uuid:renderer"));
        assert_eq!(reloaded.migrate_nva_identity(&path).await.unwrap(), None);

        let _ = fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn stale_nva_profile_rotates_only_the_nva_uuid() {
        let shared_uuid = Uuid::new_v4();
        let stale_nva_uuid = Uuid::new_v4();
        let path = std::env::temp_dir().join(format!(
            "nva2dlna-profile-migration-{}.json",
            Uuid::new_v4()
        ));
        fs::write(
            &path,
            format!(
                "{{\"deviceUuid\":\"{shared_uuid}\",\"nvaDeviceUuid\":\"{stale_nva_uuid}\",\"selectedUdn\":\"uuid:renderer\"}}"
            ),
        )
        .await
        .unwrap();

        let mut config = PersistedConfig::load_or_create(&path).await.unwrap();
        assert_eq!(
            config.migrate_nva_identity(&path).await.unwrap(),
            Some(stale_nva_uuid)
        );
        assert_eq!(config.device_uuid, shared_uuid);
        assert_ne!(config.current_nva_device_uuid(), stale_nva_uuid);
        assert_eq!(config.retired_nva_device_uuid, Some(stale_nva_uuid));
        assert_eq!(config.selected_udn.as_deref(), Some("uuid:renderer"));
        assert_eq!(config.migrate_nva_identity(&path).await.unwrap(), None);

        let _ = fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn interrupted_windows_replace_recovers_the_backup() {
        let device_uuid = Uuid::new_v4();
        let path =
            std::env::temp_dir().join(format!("nva2dlna-config-recovery-{}.json", Uuid::new_v4()));
        let backup = path.with_extension("json.bak");
        fs::write(&backup, format!("{{\"deviceUuid\":\"{device_uuid}\"}}"))
            .await
            .unwrap();

        let config = PersistedConfig::load_or_create(&path).await.unwrap();
        assert_eq!(config.device_uuid, device_uuid);
        assert!(fs::try_exists(&path).await.unwrap());
        assert!(!fs::try_exists(&backup).await.unwrap());

        let _ = fs::remove_file(path).await;
    }
}
