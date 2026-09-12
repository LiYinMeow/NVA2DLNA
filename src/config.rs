use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

#[derive(Clone, Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    /// Web UI and LAN media HTTP listener.
    #[arg(long, env = "NVA2DLNA_WEB_LISTEN", default_value = "0.0.0.0:8080")]
    pub web_listen: SocketAddrV4,

    /// NVA control and device-description listener.
    #[arg(long, env = "NVA2DLNA_NVA_LISTEN", default_value = "0.0.0.0:9959")]
    pub nva_listen: SocketAddrV4,

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

    #[arg(long, env = "NVA2DLNA_NAME", default_value = "我的小电视")]
    pub friendly_name: String,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub web_listen: SocketAddrV4,
    pub nva_listen: SocketAddrV4,
    pub advertise_ip: Ipv4Addr,
    pub config_path: PathBuf,
    pub web_dir: PathBuf,
    pub ffmpeg: PathBuf,
    pub friendly_name: String,
    pub device_uuid: Uuid,
    pub selected_udn: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedConfig {
    pub device_uuid: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_udn: Option<String>,
}

impl PersistedConfig {
    pub async fn load_or_create(path: &Path) -> Result<Self> {
        match fs::read(path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("cannot parse {}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let config = Self {
                    device_uuid: Uuid::new_v4(),
                    selected_udn: None,
                };
                config.save(path).await?;
                Ok(config)
            }
            Err(error) => Err(error).with_context(|| format!("cannot read {}", path.display())),
        }
    }

    pub async fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        let temporary = path.with_extension("json.tmp");
        fs::write(&temporary, bytes)
            .await
            .with_context(|| format!("cannot write {}", temporary.display()))?;
        if fs::try_exists(path).await.unwrap_or(false) {
            fs::remove_file(path)
                .await
                .with_context(|| format!("cannot replace {}", path.display()))?;
        }
        fs::rename(&temporary, path)
            .await
            .with_context(|| format!("cannot commit {}", path.display()))
    }
}
