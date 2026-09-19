use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, Semaphore, broadcast};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{CURRENT_NVA_IDENTITY_VERSION, PersistedConfig},
    network::{self, InterfaceAddress, InterfaceSummary},
};

const TERMINATED_SESSION_TTL_MS: u64 = 5 * 60 * 1000;
pub const MAX_FFMPEG_CONSUMERS: usize = 2;

/// Native LeLink capabilities discovered next to a DLNA MediaRenderer.
///
/// LeLink TVs normally advertise both protocols from the same address. Media still
/// travels through the renderer's DLNA endpoint, while `control_port` is the
/// independent Telecontrol channel used for features such as playback rate. The
/// ports are optional because an otherwise valid `_leboremote._tcp` record must
/// still identify the device as LeLink when an older firmware omits either TXT key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LelinkEndpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    pub name: String,
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_port: Option<u16>,
    pub last_seen_unix_ms: u64,
}

/// One outbound cast target plus whatever it told us it can do. The capability fields
/// are only as fresh as the scan that filled them, so a command must not assume a
/// target gained an ability between scans.
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
    pub av_transport_scpd_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendering_control_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendering_control_service_type: Option<String>,
    /// Rates the target itself declared in its AVTransport description, empty when it
    /// published no list. See [`Renderer::accepts_speed`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub play_speeds: Vec<String>,
    /// Present when this DLNA endpoint was also seen through LeLink discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lelink: Option<LelinkEndpoint>,
    pub address: String,
    /// Local IPv4 address that reached this renderer. Media URLs and outbound
    /// control connections must use it when the bridge spans two LANs.
    #[serde(default)]
    pub gateway_address: String,
    /// Persistent adapter id (normally its OS name) used for discovery.
    #[serde(default)]
    pub discovery_interface_id: String,
    #[serde(default)]
    pub gateway_prefix_length: u8,
    pub last_seen_unix_ms: u64,
}

impl Renderer {
    /// Whether this target will take `speed` as its AVTransport `Play` argument.
    ///
    /// A renderer that lists only integer trick-play does refuse 1.5x, and the only
    /// way to know before faulting the cast is to read its `allowedValueList`. An
    /// absent list is treated as a yes: publishing nothing has told us nothing, and
    /// `1` is mandatory for every DMR so it never depends on what we scraped.
    pub fn accepts_speed(&self, speed: &str) -> bool {
        speed == "1"
            || self.play_speeds.is_empty()
            || self.play_speeds.iter().any(|declared| declared == speed)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicRenderer {
    #[serde(flatten)]
    pub renderer: Renderer,
    pub selected: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionOrigin {
    Nva,
    Dmr,
    Lelink,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionBackend {
    #[default]
    Dlna,
    LelinkV1,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionView {
    pub id: String,
    pub origin: SessionOrigin,
    pub title: String,
    pub phase: String,
    pub quality: String,
    pub speed: String,
    pub input: String,
    pub output: String,
    pub backend: SessionBackend,
    pub target_name: String,
    pub target_udn: String,
    pub started_unix_ms: u64,
    pub error: Option<String>,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub enum SessionUpdate {
    Phase {
        session_id: String,
        phase: String,
        error: Option<String>,
    },
    Closed {
        session_id: String,
        origin: SessionOrigin,
    },
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

/// The newest timestamp emitted by an FFmpeg remux process for one media token.
///
/// A renderer may briefly keep two HTTP connections open. Every process owns a
/// generation for diagnostics, while all generations for the same media token
/// contribute to one monotonic watermark because they share the same offset.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RemuxProgressSnapshot {
    pub generation: u64,
    pub out_time_ms: Option<u64>,
    pub updated_unix_ms: Option<u64>,
}

#[derive(Debug, Default)]
pub struct RemuxProgress {
    inner: StdMutex<RemuxProgressSnapshot>,
}

impl RemuxProgress {
    /// Registers another FFmpeg consumer for this media token.
    pub fn begin_generation(&self) -> u64 {
        let mut progress = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        progress.generation = progress.generation.wrapping_add(1).max(1);
        progress.generation
    }

    /// Advances the shared watermark for any registered consumer. A later short
    /// probe must not permanently silence the earlier connection that the target
    /// continues to use for playback.
    pub fn update(&self, generation: u64, out_time_ms: u64) -> bool {
        self.update_at(generation, out_time_ms, now_ms())
    }

    fn update_at(&self, generation: u64, out_time_ms: u64, updated_unix_ms: u64) -> bool {
        let mut progress = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if generation == 0 || generation > progress.generation {
            return false;
        }
        progress.out_time_ms = Some(
            progress
                .out_time_ms
                .map_or(out_time_ms, |current| current.max(out_time_ms)),
        );
        progress.updated_unix_ms = Some(updated_unix_ms);
        true
    }

    pub fn snapshot(&self) -> RemuxProgressSnapshot {
        *self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns the current generation only after it has emitted a timestamp.
    pub fn current(&self) -> Option<RemuxProgressSnapshot> {
        let snapshot = self.snapshot();
        snapshot.out_time_ms.map(|_| snapshot)
    }
}

#[derive(Debug)]
struct PlaybackClockState {
    anchor_position_ms: u64,
    anchor_instant: Instant,
    running: bool,
    rate: f64,
}

/// A monotonic local estimate anchored by renderer observations.
///
/// Positions passed to this clock are absolute media positions. FFmpeg progress
/// is deliberately not folded in here; callers may use its watermark to clamp an
/// estimate without turning buffered remux output into an authoritative position.
#[derive(Debug)]
pub struct PlaybackClock {
    inner: StdMutex<PlaybackClockState>,
}

impl Default for PlaybackClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl PlaybackClock {
    pub fn new(position_ms: u64) -> Self {
        Self {
            inner: StdMutex::new(PlaybackClockState {
                anchor_position_ms: position_ms,
                anchor_instant: Instant::now(),
                running: false,
                rate: 1.0,
            }),
        }
    }

    pub fn mark_playing(&self, rate: f64) {
        self.mark_playing_at(rate, Instant::now());
    }

    fn mark_playing_at(&self, rate: f64, now: Instant) {
        let mut clock = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clock.anchor_position_ms = estimate_at(&clock, now);
        clock.anchor_instant = now;
        clock.running = true;
        clock.rate = if rate.is_finite() && rate > 0.0 {
            rate
        } else {
            1.0
        };
    }

    pub fn mark_paused(&self) {
        self.mark_paused_at(Instant::now());
    }

    fn mark_paused_at(&self, now: Instant) {
        let mut clock = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clock.anchor_position_ms = estimate_at(&clock, now);
        clock.anchor_instant = now;
        clock.running = false;
    }

    /// Re-anchors the estimate to an authoritative renderer position without
    /// changing whether the clock is currently running.
    pub fn observe(&self, position_ms: u64) {
        self.set_position_at(position_ms, Instant::now());
    }

    /// Re-anchors the estimate after a requested seek while preserving play/pause.
    pub fn seek(&self, position_ms: u64) {
        self.set_position_at(position_ms, Instant::now());
    }

    fn set_position_at(&self, position_ms: u64, now: Instant) {
        let mut clock = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clock.anchor_position_ms = position_ms;
        clock.anchor_instant = now;
    }

    pub fn estimate(&self) -> u64 {
        let clock = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        estimate_at(&clock, Instant::now())
    }
}

fn estimate_at(clock: &PlaybackClockState, now: Instant) -> u64 {
    if !clock.running {
        return clock.anchor_position_ms;
    }
    let elapsed_ms = now
        .checked_duration_since(clock.anchor_instant)
        .unwrap_or_default()
        .as_secs_f64()
        * 1_000.0
        * clock.rate;
    let elapsed_ms = if elapsed_ms >= u64::MAX as f64 {
        u64::MAX
    } else {
        elapsed_ms as u64
    };
    clock.anchor_position_ms.saturating_add(elapsed_ms)
}

#[derive(Clone, Debug)]
pub struct MediaEntry {
    pub token: String,
    pub owner_session: String,
    pub input: MediaInput,
    pub mime: String,
    pub duration_ms: Option<u64>,
    pub start_offset_ms: u64,
    pub created_unix_ms: u64,
    pub allowed_renderer_ip: String,
    pub gateway_address: String,
    pub cancellation: CancellationToken,
    pub ffmpeg_consumers: Arc<Semaphore>,
    pub remux_progress: Arc<RemuxProgress>,
    pub playback_clock: Arc<PlaybackClock>,
    pub hls_resources: Arc<RwLock<HlsResourceStore>>,
}

#[derive(Clone, Debug)]
pub struct NvaEvent {
    pub session_id: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
    pub close_after: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TransportState {
    #[default]
    NoMediaPresent,
    Stopped,
    Playing,
    PausedPlayback,
    Transitioning,
}

impl TransportState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoMediaPresent => "NO_MEDIA_PRESENT",
            Self::Stopped => "STOPPED",
            Self::Playing => "PLAYING",
            Self::PausedPlayback => "PAUSED_PLAYBACK",
            Self::Transitioning => "TRANSITIONING",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenaService {
    AvTransport,
    RenderingControl,
    ConnectionManager,
}

#[derive(Clone, Debug)]
pub struct GenaSubscription {
    pub sid: String,
    pub service: GenaService,
    pub callback: String,
    pub expires_at: std::time::Instant,
}

#[derive(Clone, Debug)]
pub struct DmrSink {
    pub uri: String,
    pub metadata: String,
    pub title: String,
    pub transport: TransportState,
    pub session_id: Option<String>,
    pub speed: String,
    pub position_ms: u64,
    pub started_at: std::time::Instant,
    pub duration_ms: u64,
    pub volume: u8,
    pub muted: bool,
    pub play_mode: String,
    pub subscriptions: Vec<GenaSubscription>,
    pub sequence: u32,
}

impl Default for DmrSink {
    fn default() -> Self {
        Self {
            uri: String::new(),
            metadata: String::new(),
            title: String::new(),
            transport: TransportState::default(),
            session_id: None,
            speed: String::new(),
            position_ms: 0,
            // Instant has no Default, and a zero-elapsed anchor is what we want anyway.
            started_at: std::time::Instant::now(),
            duration_ms: 0,
            volume: 100,
            muted: false,
            play_mode: "NORMAL".into(),
            subscriptions: Vec::new(),
            sequence: 0,
        }
    }
}

impl DmrSink {
    /// Position as reported to control points: the last committed position plus
    /// wall-clock drift while the transport is playing.
    pub fn reported_position_ms(&self) -> u64 {
        if self.transport != TransportState::Playing {
            return self.position_ms;
        }
        let elapsed = self
            .started_at
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        self.position_ms.saturating_add(elapsed)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("play request was superseded")]
pub struct SupersededPlay;

/// What the 乐播 V1 receiver front end has to remember between the phone's separate
/// HTTP requests. The session id on the wire is generated by the phone, so it is kept
/// only to recognise a reconnect and make a repeated `POST /play` idempotent.
#[derive(Clone, Debug)]
pub struct LelinkSink {
    pub session_id: Option<String>,
    pub phone_session: String,
    pub url_id: String,
    pub transport: TransportState,
    pub volume: u8,
}

impl Default for LelinkSink {
    fn default() -> Self {
        Self {
            session_id: None,
            phone_session: String::new(),
            url_id: String::new(),
            transport: TransportState::default(),
            volume: 50,
        }
    }
}

#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    pub device_uuid: uuid::Uuid,
    pub nva_device_uuid: uuid::Uuid,
    pub retired_nva_device_uuid: Option<uuid::Uuid>,
    pub nva_name: String,
    pub dlna_name: String,
    pub lelink_name: String,
    pub advertise_ip: std::net::Ipv4Addr,
    pub web_listen_ip: std::net::Ipv4Addr,
    pub web_port: u16,
    pub ffmpeg: PathBuf,
    pub config_path: PathBuf,
    pub selected_udn: RwLock<Option<String>>,
    pub scan_interface_ids: RwLock<Vec<String>>,
    pub config_write: Mutex<()>,
    pub renderers: RwLock<HashMap<String, Renderer>>,
    pub session: RwLock<Option<SessionView>>,
    pub dmr: Mutex<DmrSink>,
    pub lelink: Mutex<LelinkSink>,
    pub media: RwLock<HashMap<String, MediaEntry>>,
    pub scanning: RwLock<bool>,
    pub scan_operation: Mutex<()>,
    pub operation: Mutex<()>,
    pub http: reqwest::Client,
    pub nva_events: broadcast::Sender<NvaEvent>,
    pub session_updates: broadcast::Sender<SessionUpdate>,
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
        let (session_updates, _) = broadcast::channel(64);
        Ok(Self(Arc::new(Inner {
            device_uuid: config.device_uuid,
            nva_device_uuid: config.nva_device_uuid,
            retired_nva_device_uuid: config.retired_nva_device_uuid,
            nva_name: config.nva_name.clone(),
            dlna_name: config.dlna_name.clone(),
            lelink_name: config.lelink_name.clone(),
            advertise_ip: config.advertise_ip,
            web_listen_ip: *config.web_listen.ip(),
            web_port: config.web_listen.port(),
            ffmpeg: config.ffmpeg.clone(),
            config_path: config.config_path.clone(),
            selected_udn: RwLock::new(config.selected_udn.clone()),
            scan_interface_ids: RwLock::new(config.scan_interface_ids.clone()),
            config_write: Mutex::new(()),
            renderers: RwLock::new(HashMap::new()),
            session: RwLock::new(None),
            dmr: Mutex::new(DmrSink::default()),
            lelink: Mutex::new(LelinkSink::default()),
            media: RwLock::new(HashMap::new()),
            scanning: RwLock::new(false),
            scan_operation: Mutex::new(()),
            operation: Mutex::new(()),
            http,
            nva_events,
            session_updates,
            terminated_sessions: RwLock::new(HashMap::new()),
            play_epoch: AtomicU64::new(0),
            pending_play: Mutex::new(None),
        })))
    }

    pub fn device_uuid(&self) -> uuid::Uuid {
        self.0.device_uuid
    }

    pub fn nva_device_uuid(&self) -> uuid::Uuid {
        self.0.nva_device_uuid
    }

    pub fn retired_nva_device_uuid(&self) -> Option<uuid::Uuid> {
        self.0.retired_nva_device_uuid
    }

    /// The name each face is advertised under. They are separate values on purpose:
    /// senders that key their device list on the name keep only one of several entries
    /// when the names collide, and the faces do not all do the same things.
    pub fn nva_name(&self) -> &str {
        &self.0.nva_name
    }

    pub fn dlna_name(&self) -> &str {
        &self.0.dlna_name
    }

    pub fn lelink_name(&self) -> &str {
        &self.0.lelink_name
    }

    pub fn advertise_ip(&self) -> std::net::Ipv4Addr {
        self.0.advertise_ip
    }

    pub fn web_port(&self) -> u16 {
        self.0.web_port
    }

    /// Address used by local FFmpeg processes to call back into the HTTP proxy.
    /// A concrete listener may not accept loopback; a wildcard listener can.
    pub fn internal_http_ip(&self) -> std::net::Ipv4Addr {
        if self.0.web_listen_ip.is_unspecified() {
            std::net::Ipv4Addr::LOCALHOST
        } else {
            self.0.web_listen_ip
        }
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

    pub fn subscribe_session_updates(&self) -> broadcast::Receiver<SessionUpdate> {
        self.0.session_updates.subscribe()
    }

    pub fn announce_closed(&self, session_id: &str, origin: SessionOrigin) {
        let _ = self.0.session_updates.send(SessionUpdate::Closed {
            session_id: session_id.to_owned(),
            origin,
        });
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
        let cutoff = now_ms().saturating_sub(120_000);
        for mut renderer in renderers {
            // mDNS is UDP and one missed response must not make a dual-protocol TV
            // flicker back to DLNA in the management page. Keep only a still-fresh
            // LeLink observation; its own timestamp prevents indefinite retention
            // while the DLNA half continues refreshing normally.
            if renderer.lelink.is_none()
                && let Some(previous) = current
                    .get(&renderer.udn)
                    .and_then(|previous| previous.lelink.as_ref())
                    .filter(|lelink| lelink.last_seen_unix_ms >= cutoff)
            {
                renderer.lelink = Some(previous.clone());
            }
            current.insert(renderer.udn.clone(), renderer);
        }
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

    pub async fn scan_interface_ids(&self) -> Vec<String> {
        self.0.scan_interface_ids.read().await.clone()
    }

    /// Resolve the persisted adapter selection at scan time so DHCP/address
    /// changes take effect without restarting the bridge.
    pub async fn discovery_interfaces(&self) -> Result<Vec<InterfaceAddress>> {
        let available = network::available_ipv4()?;
        let selected = self.scan_interface_ids().await;
        let mut selected = network::select_addresses(&available, &selected, self.advertise_ip());
        if !self.0.web_listen_ip.is_unspecified() {
            selected.retain(|interface| interface.address == self.0.web_listen_ip);
        }
        Ok(selected)
    }

    pub async fn network_interfaces(
        &self,
    ) -> Result<(Vec<InterfaceSummary>, Vec<String>, Option<String>)> {
        let available = network::available_ipv4()?;
        let configured = self.scan_interface_ids().await;
        let mut effective = network::select_addresses(&available, &configured, self.advertise_ip());
        if !self.0.web_listen_ip.is_unspecified() {
            effective.retain(|interface| interface.address == self.0.web_listen_ip);
        }
        let mut selected = effective
            .iter()
            .map(|interface| interface.id.clone())
            .collect::<Vec<_>>();
        selected.sort();
        selected.dedup();
        let selected_set = selected
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let mut summaries =
            network::interface_summaries(&available, &configured, self.advertise_ip());
        for interface in &mut summaries {
            interface.selected = selected_set.contains(interface.id.as_str());
        }
        let receive = network::receive_interface_id(&available, self.advertise_ip());
        Ok((summaries, selected, receive))
    }

    /// Persist a validated outbound discovery-adapter selection. The receiver's
    /// advertise address is deliberately unaffected.
    pub async fn set_scan_interface_ids(&self, ids: Vec<String>) -> Result<()> {
        const MAX_SCAN_INTERFACES: usize = 32;
        if ids.len() > MAX_SCAN_INTERFACES {
            return Err(anyhow!("最多可选择 {MAX_SCAN_INTERFACES} 个扫描网卡"));
        }
        let available = network::available_ipv4()?;
        let known = available
            .iter()
            .map(|interface| interface.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut normalized = Vec::with_capacity(ids.len());
        for id in ids {
            let id = id.trim();
            if id.is_empty() || !known.contains(id) {
                return Err(anyhow!("扫描网卡不存在或不可用: {id}"));
            }
            if !normalized.iter().any(|stored| stored == id) {
                normalized.push(id.to_owned());
            }
        }
        if !self.0.web_listen_ip.is_unspecified() {
            let resolved = network::select_addresses(&available, &normalized, self.advertise_ip());
            if resolved
                .iter()
                .any(|interface| interface.address != self.0.web_listen_ip)
            {
                return Err(anyhow!(
                    "所选扫描网卡无法访问当前 Web 监听地址；跨网关模式请使用 --web-ip '*' 或 NVA2DLNA_WEB_IP='*'（当前端口 {}）",
                    self.web_port()
                ));
            }
        }
        let previous = {
            let mut current = self.0.scan_interface_ids.write().await;
            std::mem::replace(&mut *current, normalized)
        };
        if let Err(error) = self.persist_config().await {
            *self.0.scan_interface_ids.write().await = previous;
            return Err(error);
        }
        self.prune_renderers_for_discovery_interfaces().await?;
        Ok(())
    }

    async fn prune_renderers_for_discovery_interfaces(&self) -> Result<()> {
        let selected = self.discovery_interfaces().await?;
        let ids = selected
            .iter()
            .map(|interface| interface.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let active_target = self.session().await.map(|session| session.target_udn);
        self.0.renderers.write().await.retain(|udn, renderer| {
            active_target.as_deref() == Some(udn.as_str())
                || ids.contains(renderer.discovery_interface_id.as_str())
        });
        Ok(())
    }

    pub async fn select_renderer(&self, udn: Option<String>) -> Result<()> {
        if let Some(udn) = udn.as_deref()
            && self.renderer(udn).await.is_none()
        {
            return Err(anyhow!("找不到指定的 DLNA 目标"));
        }
        *self.0.selected_udn.write().await = udn;
        self.persist_config().await
    }

    async fn persist_config(&self) -> Result<()> {
        let _guard = self.0.config_write.lock().await;
        PersistedConfig {
            device_uuid: self.device_uuid(),
            nva_identity_version: CURRENT_NVA_IDENTITY_VERSION,
            nva_device_uuid: Some(self.nva_device_uuid()),
            retired_nva_device_uuid: self.retired_nva_device_uuid(),
            selected_udn: self.0.selected_udn.read().await.clone(),
            scan_interface_ids: self.0.scan_interface_ids.read().await.clone(),
        }
        .save(&self.0.config_path)
        .await
    }

    pub async fn session(&self) -> Option<SessionView> {
        self.0.session.read().await.clone()
    }

    pub async fn dmr(&self) -> tokio::sync::MutexGuard<'_, DmrSink> {
        self.0.dmr.lock().await
    }

    pub async fn lelink(&self) -> tokio::sync::MutexGuard<'_, LelinkSink> {
        self.0.lelink.lock().await
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

    /// Whether the transport is running at this rate is reported through
    /// `TransportPlaySpeed`, which does not send events, so the session view is
    /// the only place our own UI can read it back.
    pub async fn update_session_speed_if(&self, session_id: &str, speed: &str) -> bool {
        let mut session = self.0.session.write().await;
        let Some(session) = session.as_mut().filter(|session| session.id == session_id) else {
            return false;
        };
        session.speed = speed.to_owned();
        true
    }

    pub async fn update_session_backend_if(
        &self,
        session_id: &str,
        backend: SessionBackend,
    ) -> bool {
        let mut session = self.0.session.write().await;
        let Some(session) = session.as_mut().filter(|session| session.id == session_id) else {
            return false;
        };
        session.backend = backend;
        true
    }

    /// Registers a stream token without disturbing another front end's session:
    /// only tokens owned by the same session are replaced.
    pub async fn register_media(&self, entry: MediaEntry) {
        let mut media = self.0.media.write().await;
        let superseded = media
            .iter()
            .filter(|(_, existing)| existing.owner_session == entry.owner_session)
            .map(|(_, existing)| existing.clone())
            .collect::<Vec<_>>();
        for existing in superseded {
            existing.ffmpeg_consumers.close();
            existing.cancellation.cancel();
            media.remove(&existing.token);
        }
        media.insert(entry.token.clone(), entry);
    }

    pub async fn media(&self, token: &str) -> Option<MediaEntry> {
        self.0.media.read().await.get(token).cloned()
    }

    pub async fn media_for_owner(&self, session_id: &str) -> Option<MediaEntry> {
        self.0
            .media
            .read()
            .await
            .values()
            .find(|entry| entry.owner_session == session_id)
            .cloned()
    }

    pub async fn revoke_media_for(&self, session_id: &str) {
        let mut media = self.0.media.write().await;
        let owned = media
            .iter()
            .filter(|(_, entry)| entry.owner_session == session_id)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        for entry in owned {
            entry.ffmpeg_consumers.close();
            entry.cancellation.cancel();
            media.remove(&entry.token);
        }
    }

    pub async fn finish_session_if(
        &self,
        session_id: &str,
        media_token: &str,
        terminal_error: Option<String>,
    ) -> bool {
        let mut session = self.0.session.write().await;
        let origin = session
            .as_ref()
            .filter(|session| session.id == session_id)
            .map(|session| session.origin);
        let Some(origin) = origin else {
            return false;
        };
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
        let phase_error = terminal_error.clone();
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
        match phase_error {
            Some(error) => {
                let _ = self.0.session_updates.send(SessionUpdate::Phase {
                    session_id: session_id.to_owned(),
                    phase: "error".into(),
                    error: Some(error),
                });
            }
            None => self.announce_closed(session_id, origin),
        }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn concurrent_remux_generations_share_a_monotonic_watermark() {
        let progress = RemuxProgress::default();
        let first = progress.begin_generation();
        assert!(progress.update_at(first, 8_000, 10));
        assert_eq!(progress.current().unwrap().out_time_ms, Some(8_000));

        let second = progress.begin_generation();
        assert_ne!(first, second);
        assert_eq!(
            progress.snapshot(),
            RemuxProgressSnapshot {
                generation: second,
                out_time_ms: Some(8_000),
                updated_unix_ms: Some(10),
            }
        );
        assert!(progress.update_at(first, 9_000, 20));
        assert!(progress.update_at(second, 500, 30));
        assert!(progress.update_at(first, 8_500, 40));
        assert!(!progress.update_at(second + 1, 20_000, 50));
        assert_eq!(
            progress.current(),
            Some(RemuxProgressSnapshot {
                generation: second,
                out_time_ms: Some(9_000),
                updated_unix_ms: Some(40),
            })
        );
    }

    #[test]
    fn playback_clock_preserves_continuity_across_rate_and_pause_changes() {
        let base = Instant::now();
        let clock = PlaybackClock::new(5_000);
        clock.set_position_at(5_000, base);
        clock.mark_playing_at(2.0, base);
        assert_eq!(
            clock_estimate_at(&clock, base + Duration::from_millis(750)),
            6_500
        );

        let paused_at = base + Duration::from_secs(1);
        clock.mark_paused_at(paused_at);
        assert_eq!(clock_estimate_at(&clock, paused_at), 7_000);
        assert_eq!(
            clock_estimate_at(&clock, paused_at + Duration::from_secs(30)),
            7_000
        );

        clock.mark_playing_at(0.5, paused_at + Duration::from_secs(30));
        assert_eq!(
            clock_estimate_at(&clock, paused_at + Duration::from_secs(32)),
            8_000
        );
    }

    #[test]
    fn playback_clock_observation_reanchors_an_absolute_running_position() {
        let base = Instant::now();
        let clock = PlaybackClock::new(0);
        clock.mark_playing_at(1.0, base);
        clock.set_position_at(42_000, base + Duration::from_secs(5));
        assert_eq!(
            clock_estimate_at(&clock, base + Duration::from_secs(6)),
            43_000
        );

        clock.mark_playing_at(f64::NAN, base + Duration::from_secs(6));
        assert_eq!(
            clock_estimate_at(&clock, base + Duration::from_secs(7)),
            44_000
        );
    }

    fn clock_estimate_at(clock: &PlaybackClock, now: Instant) -> u64 {
        let state = clock
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        estimate_at(&state, now)
    }
}
