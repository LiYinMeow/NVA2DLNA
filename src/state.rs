use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, Semaphore, broadcast};
use tokio_util::sync::CancellationToken;

use crate::config::PersistedConfig;

const TERMINATED_SESSION_TTL_MS: u64 = 5 * 60 * 1000;
pub const MAX_FFMPEG_CONSUMERS: usize = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Renderer {
    pub udn: String,
    pub friendly_name: String,
    pub manufacturer: String,
    pub model_name: String,
    pub location: String,
    pub av_transport_url: String,
    pub av_transport_service_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendering_control_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendering_control_service_type: Option<String>,
    #[serde(default)]
    pub sink_protocols: Vec<String>,
    pub address: String,
    pub last_seen_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicRenderer {
    #[serde(flatten)]
    pub renderer: Renderer,
    pub selected: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionView {
    pub id: String,
    pub title: String,
    pub phase: String,
    pub quality: String,
    pub input: String,
    pub output: String,
    pub target_name: String,
    pub target_udn: String,
    pub started_unix_ms: u64,
    pub error: Option<String>,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub enum MediaInput {
    Progressive {
        url: String,
    },
    Remux {
        url: String,
        format: RemuxFormat,
    },
    Dash {
        video_url: String,
        video_backup_urls: Vec<String>,
        audio_url: String,
        audio_backup_urls: Vec<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemuxFormat {
    Flv,
    Hls,
}

#[derive(Debug, Default)]
pub struct HlsResourceStore {
    pub urls: HashMap<String, String>,
    pub order: VecDeque<String>,
}

#[derive(Clone, Debug)]
pub struct MediaEntry {
    pub token: String,
    pub owner_session: String,
    pub input: MediaInput,
    pub mime: String,
    pub created_unix_ms: u64,
    pub allowed_renderer_ip: String,
    pub cancellation: CancellationToken,
    pub ffmpeg_consumers: Arc<Semaphore>,
    pub hls_resources: Arc<RwLock<HlsResourceStore>>,
}

#[derive(Clone, Debug)]
pub struct NvaEvent {
    pub session_id: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
    pub close_after: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("play request was superseded")]
pub struct SupersededPlay;

#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    pub device_uuid: uuid::Uuid,
    pub friendly_name: String,
    pub advertise_ip: std::net::Ipv4Addr,
    pub web_port: u16,
    pub ffmpeg: PathBuf,
    pub config_path: PathBuf,
    pub selected_udn: RwLock<Option<String>>,
    pub renderers: RwLock<HashMap<String, Renderer>>,
    pub session: RwLock<Option<SessionView>>,
    pub media: RwLock<HashMap<String, MediaEntry>>,
    pub scanning: RwLock<bool>,
    pub scan_operation: Mutex<()>,
    pub operation: Mutex<()>,
    pub http: reqwest::Client,
    pub nva_events: broadcast::Sender<NvaEvent>,
    pub terminated_sessions: RwLock<HashMap<String, u64>>,
    pub play_epoch: AtomicU64,
    pub pending_play: Mutex<Option<(u64, String)>>,
}

impl AppState {
    pub fn new(config: &crate::config::RuntimeConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(std::time::Duration::from_secs(4))
            .read_timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("NVA2DLNA/0.1")
            .build()?;
        let (nva_events, _) = broadcast::channel(64);
        Ok(Self(Arc::new(Inner {
            device_uuid: config.device_uuid,
            friendly_name: config.friendly_name.clone(),
            advertise_ip: config.advertise_ip,
            web_port: config.web_listen.port(),
            ffmpeg: config.ffmpeg.clone(),
            config_path: config.config_path.clone(),
            selected_udn: RwLock::new(config.selected_udn.clone()),
            renderers: RwLock::new(HashMap::new()),
            session: RwLock::new(None),
            media: RwLock::new(HashMap::new()),
            scanning: RwLock::new(false),
            scan_operation: Mutex::new(()),
            operation: Mutex::new(()),
            http,
            nva_events,
            terminated_sessions: RwLock::new(HashMap::new()),
            play_epoch: AtomicU64::new(0),
            pending_play: Mutex::new(None),
        })))
    }

    pub fn device_uuid(&self) -> uuid::Uuid {
        self.0.device_uuid
    }

    pub fn friendly_name(&self) -> &str {
        &self.0.friendly_name
    }

    pub fn advertise_ip(&self) -> std::net::Ipv4Addr {
        self.0.advertise_ip
    }

    pub fn web_port(&self) -> u16 {
        self.0.web_port
    }

    pub fn ffmpeg(&self) -> &std::path::Path {
        &self.0.ffmpeg
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.0.http
    }

    pub fn subscribe_nva(&self) -> broadcast::Receiver<NvaEvent> {
        self.0.nva_events.subscribe()
    }

    pub fn emit_nva(&self, event: NvaEvent) {
        let _ = self.0.nva_events.send(event);
    }

    pub async fn operation(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.0.operation.lock().await
    }

    pub async fn scan_operation(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.0.scan_operation.lock().await
    }

    pub async fn begin_play_epoch(&self, session_id: &str) -> u64 {
        let mut pending = self.0.pending_play.lock().await;
        let epoch = self
            .0
            .play_epoch
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        *pending = Some((epoch, session_id.to_owned()));
        epoch
    }

    pub async fn cancel_pending_play_for(&self, session_id: Option<&str>) -> bool {
        let mut pending = self.0.pending_play.lock().await;
        let matches = pending
            .as_ref()
            .is_some_and(|(_, owner)| session_id.is_none_or(|session_id| owner == session_id));
        if matches {
            self.0.play_epoch.fetch_add(1, Ordering::AcqRel);
            *pending = None;
        }
        matches
    }

    pub async fn complete_play_epoch(&self, expected: u64) {
        let mut pending = self.0.pending_play.lock().await;
        if pending
            .as_ref()
            .is_some_and(|(epoch, _)| *epoch == expected)
        {
            *pending = None;
        }
    }

    pub async fn pending_play_owner(&self) -> Option<String> {
        self.0
            .pending_play
            .lock()
            .await
            .as_ref()
            .map(|(_, owner)| owner.clone())
    }

    pub fn ensure_play_epoch(&self, expected: u64) -> Result<()> {
        if self.0.play_epoch.load(Ordering::Acquire) == expected {
            Ok(())
        } else {
            Err(SupersededPlay.into())
        }
    }

    pub async fn set_scanning(&self, value: bool) {
        *self.0.scanning.write().await = value;
    }

    pub async fn scanning(&self) -> bool {
        *self.0.scanning.read().await
    }

    pub async fn replace_renderers(&self, renderers: Vec<Renderer>) {
        let mut current = self.0.renderers.write().await;
        for renderer in renderers {
            current.insert(renderer.udn.clone(), renderer);
        }
        let cutoff = now_ms().saturating_sub(120_000);
        current.retain(|_, renderer| renderer.last_seen_unix_ms >= cutoff);
    }

    pub async fn public_renderers(&self) -> Vec<PublicRenderer> {
        let selected = self.0.selected_udn.read().await.clone();
        let mut values = self
            .0
            .renderers
            .read()
            .await
            .values()
            .cloned()
            .map(|renderer| PublicRenderer {
                selected: selected.as_deref() == Some(renderer.udn.as_str()),
                renderer,
            })
            .collect::<Vec<_>>();
        values.sort_by_key(|item| (!item.selected, item.renderer.friendly_name.to_lowercase()));
        values
    }

    pub async fn renderer(&self, udn: &str) -> Option<Renderer> {
        self.0.renderers.read().await.get(udn).cloned()
    }

    pub async fn selected_renderer(&self) -> Result<Renderer> {
        let selected = self
            .0
            .selected_udn
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("请先在管理页面选择一个 DLNA 播放目标"))?;
        self.renderer(&selected)
            .await
            .ok_or_else(|| anyhow!("已选择的 DLNA 目标当前不在线"))
    }

    pub async fn selected_udn(&self) -> Option<String> {
        self.0.selected_udn.read().await.clone()
    }

    pub async fn select_renderer(&self, udn: Option<String>) -> Result<()> {
        if let Some(udn) = udn.as_deref()
            && self.renderer(udn).await.is_none()
        {
            return Err(anyhow!("找不到指定的 DLNA 目标"));
        }
        *self.0.selected_udn.write().await = udn.clone();
        PersistedConfig {
            device_uuid: self.device_uuid(),
            selected_udn: udn,
        }
        .save(&self.0.config_path)
        .await
    }

    pub async fn session(&self) -> Option<SessionView> {
        self.0.session.read().await.clone()
    }

    pub async fn set_session(&self, session: Option<SessionView>) {
        *self.0.session.write().await = session;
    }

    pub async fn update_session_phase(&self, phase: &str, error: Option<String>) {
        if let Some(session) = self.0.session.write().await.as_mut() {
            session.phase = phase.to_owned();
            session.error = error;
        }
    }

    pub async fn update_session_phase_if(
        &self,
        session_id: &str,
        phase: &str,
        error: Option<String>,
    ) -> bool {
        let mut session = self.0.session.write().await;
        let Some(session) = session.as_mut().filter(|session| session.id == session_id) else {
            return false;
        };
        session.phase = phase.to_owned();
        session.error = error;
        true
    }

    pub async fn register_media(&self, entry: MediaEntry) {
        let mut media = self.0.media.write().await;
        for previous in media.values() {
            previous.ffmpeg_consumers.close();
            previous.cancellation.cancel();
        }
        media.clear();
        media.insert(entry.token.clone(), entry);
    }

    pub async fn media(&self, token: &str) -> Option<MediaEntry> {
        self.0.media.read().await.get(token).cloned()
    }

    pub async fn revoke_media(&self) {
        let mut media = self.0.media.write().await;
        for entry in media.values() {
            entry.ffmpeg_consumers.close();
            entry.cancellation.cancel();
        }
        media.clear();
    }

    pub async fn finish_session_if(
        &self,
        session_id: &str,
        media_token: &str,
        terminal_error: Option<String>,
    ) -> bool {
        let mut session = self.0.session.write().await;
        if session
            .as_ref()
            .is_none_or(|session| session.id != session_id)
        {
            return false;
        }
        let mut media = self.0.media.write().await;
        if !media
            .get(media_token)
            .is_some_and(|entry| entry.owner_session == session_id && entry.token == media_token)
        {
            return false;
        }
        if let Some(entry) = media.remove(media_token) {
            entry.ffmpeg_consumers.close();
            entry.cancellation.cancel();
        }
        if let Some(error) = terminal_error {
            if let Some(session) = session.as_mut() {
                session.phase = "error".into();
                session.error = Some(error);
            }
        } else {
            *session = None;
        }
        drop(media);
        drop(session);
        self.mark_session_terminated(session_id).await;
        true
    }

    pub async fn mark_session_terminated(&self, session_id: &str) {
        let now = now_ms();
        let mut terminated = self.0.terminated_sessions.write().await;
        terminated.retain(|_, expires_at| *expires_at > now);
        terminated.insert(
            session_id.to_owned(),
            now.saturating_add(TERMINATED_SESSION_TTL_MS),
        );
    }

    pub async fn session_was_terminated(&self, session_id: &str) -> bool {
        let now = now_ms();
        let mut terminated = self.0.terminated_sessions.write().await;
        terminated.retain(|_, expires_at| *expires_at > now);
        terminated.contains_key(session_id)
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
