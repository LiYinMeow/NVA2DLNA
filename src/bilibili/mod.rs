mod parse;

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Url, header::LOCATION, redirect::Policy};
use serde_json::Value;
use tracing::{info, warn};

use parse::{extract_media_source, quality_label, quality_options, validate_media_url};

pub(crate) use parse::{is_public_ip, validate_media_url as validate_outbound_media_url};

const TV_PLAY_URL: &str = "https://api.bilibili.com/x/tv/playurl";
const LIVE_PLAY_URL: &str = "https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo";
const MAX_API_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_EMBEDDED_METADATA_BYTES: usize = 256 * 1024;
const MAX_API_REDIRECTS: usize = 3;
/// The best picture a sender can ask for. The TV playurl endpoint only lists it in
/// `accept_quality` when the request itself is made at that ceiling, so a receiver that
/// always asks for the sender's current 1080p choice never learns that 4K exists.
const NVA_4K_QUALITY: u64 = 120;

// Public application identities used by deployed TV/Nirvana receivers. They
// identify the emulated application and are deliberately not user-configurable.
const ORIGINAL_NIRVANA_APP_KEY: &str = "4ebafd7c4951b366";
const ORIGINAL_NIRVANA_APP_SECRET: &str = "8cb98205e9b2ad3669aad0fce12a4c13";
const ANDROID_TV_APP_KEY: &str = "4409e2ce8ffd12b8";
const ANDROID_TV_APP_SECRET: &str = "59b43e04ad6965f34319062b478f83dd";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MediaSource {
    Progressive {
        url: String,
    },
    Dash {
        video_url: String,
        video_backup_urls: Vec<String>,
        audio_url: String,
        audio_backup_urls: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedMedia {
    pub source: MediaSource,
    pub title: String,
    pub quality: String,
    pub available_qualities: Vec<u64>,
    pub duration_ms: Option<u64>,
    pub live: bool,
}

#[derive(Clone)]
pub struct PlayRequest {
    pub aid: String,
    pub oid: String,
    pub cid: String,
    pub episode_id: String,
    pub season_id: String,
    pub room_id: String,
    pub access_key: String,
    pub desired_quality: u64,
    pub content_type: u64,
    pub seek_position_ms: u64,
    pub title: String,
}

impl Default for PlayRequest {
    fn default() -> Self {
        Self {
            aid: String::new(),
            oid: String::new(),
            cid: String::new(),
            episode_id: String::new(),
            season_id: String::new(),
            room_id: String::new(),
            access_key: String::new(),
            desired_quality: 80,
            content_type: 1,
            seek_position_ms: 0,
            title: String::new(),
        }
    }
}

impl std::fmt::Debug for PlayRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlayRequest")
            .field("aid", &self.aid)
            .field("oid", &self.oid)
            .field("cid", &self.cid)
            .field("episode_id", &self.episode_id)
            .field("season_id", &self.season_id)
            .field("room_id", &self.room_id)
            .field("access_key", &"[redacted]")
            .field("desired_quality", &self.desired_quality)
            .field("content_type", &self.content_type)
            .field("seek_position_ms", &self.seek_position_ms)
            .field("title", &self.title)
            .finish()
    }
}

impl PlayRequest {
    pub fn from_value(value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("Play parameters must be a JSON object"))?;
        let aid = value_string(object.get("aid").or_else(|| object.get("avid")));
        let episode_id = value_string(
            object
                .get("epId")
                .or_else(|| object.get("ep_id"))
                .or_else(|| object.get("epid")),
        );
        let mut oid = value_string(object.get("oid").or_else(|| object.get("object_id")));
        if oid.is_empty() {
            oid = if is_real_id(&episode_id) {
                episode_id.clone()
            } else {
                aid.clone()
            };
        }
        let seek_value = object.get("seekTs").or_else(|| object.get("seek_ts"));
        let request = Self {
            aid,
            oid,
            cid: value_string(object.get("cid")),
            episode_id,
            season_id: value_string(object.get("seasonId").or_else(|| object.get("season_id"))),
            room_id: value_string(
                object
                    .get("roomId")
                    .or_else(|| object.get("room_id"))
                    .or_else(|| object.get("roomid")),
            ),
            access_key: value_string(object.get("accessKey").or_else(|| object.get("access_key"))),
            desired_quality: value_u64(
                object
                    .get("userDesireQn")
                    .or_else(|| object.get("user_desire_qn"))
                    .or_else(|| object.get("desireQn"))
                    .or_else(|| object.get("desire_qn"))
                    .or_else(|| object.get("currentQn"))
                    .or_else(|| object.get("current_qn")),
            )
            .unwrap_or(80),
            content_type: value_u64(
                object
                    .get("contentType")
                    .or_else(|| object.get("content_type")),
            )
            .unwrap_or(1),
            seek_position_ms: seek_value
                .map(|value| nva_seek_position_ms(Some(value)))
                .transpose()?
                .unwrap_or(0),
            title: value_string(object.get("title")),
        };
        if !is_real_id(&request.room_id) && (!is_real_id(&request.aid) || !is_real_id(&request.cid))
        {
            bail!("Play requires aid/cid, or roomId for a live stream");
        }
        Ok(request)
    }

    pub fn title_or_default(&self) -> String {
        if !self.title.trim().is_empty() {
            self.title.trim().chars().take(160).collect()
        } else if is_real_id(&self.room_id) {
            format!("哔哩哔哩直播间 {}", self.room_id)
        } else {
            format!("哔哩哔哩 AV{}", self.aid)
        }
    }
}

#[derive(Clone, Copy)]
enum TvProfile {
    AndroidDash,
    Legacy,
}

impl TvProfile {
    fn label(self) -> &'static str {
        match self {
            Self::AndroidDash => "android-tv-dash",
            Self::Legacy => "legacy-nirvana",
        }
    }

    fn app_key(self) -> &'static str {
        match self {
            Self::AndroidDash => ANDROID_TV_APP_KEY,
            Self::Legacy => ORIGINAL_NIRVANA_APP_KEY,
        }
    }

    fn app_secret(self) -> &'static str {
        match self {
            Self::AndroidDash => ANDROID_TV_APP_SECRET,
            Self::Legacy => ORIGINAL_NIRVANA_APP_SECRET,
        }
    }
}

#[derive(Clone)]
pub struct BilibiliResolver {
    client: Client,
}

impl BilibiliResolver {
    pub fn new() -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::REFERER,
            "https://www.bilibili.com/".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::ORIGIN,
            "https://www.bilibili.com".parse().unwrap(),
        );
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(15))
            .redirect(Policy::none())
            .user_agent("Mozilla/5.0 NVA2DLNA/0.1")
            .default_headers(headers)
            .build()
            .context("cannot create Bilibili client")?;
        Ok(Self { client })
    }

    pub async fn resolve_play(&self, request: &PlayRequest) -> Result<ResolvedMedia> {
        if is_real_id(&request.room_id) {
            return self
                .resolve_live(request)
                .await
                .and_then(|value| resolved_media(request, &value));
        }
        match self.resolve_tv(request, TvProfile::AndroidDash).await {
            Ok(value) => match resolved_media(request, &value) {
                Ok(media) => {
                    info!(
                        requested_quality = request.desired_quality,
                        selected_quality = media.quality,
                        "resolved Bilibili Android TV DASH media"
                    );
                    return Ok(media);
                }
                Err(error) => warn!(%error, "Android TV media unusable; trying legacy profile"),
            },
            Err(error) => warn!(%error, "Android TV play URL failed; trying legacy profile"),
        }
        let value = self.resolve_tv(request, TvProfile::Legacy).await?;
        resolved_media(request, &value)
    }

    pub async fn resolve_play_url(&self, params: &Value) -> Result<(PlayRequest, ResolvedMedia)> {
        let object = params
            .as_object()
            .ok_or_else(|| anyhow!("PlayUrl parameters must be a JSON object"))?;
        let raw_url = value_string(object.get("url"));
        let mut metadata = object
            .get("content")
            .cloned()
            .map(decode_metadata)
            .unwrap_or_else(|| Value::Object(Default::default()));
        if !metadata.is_object() {
            metadata = Value::Object(Default::default());
        }
        if let Some(target) = metadata.as_object_mut() {
            for aliases in [
                &["aid", "avid"][..],
                &["oid", "object_id"],
                &["cid"],
                &["epId", "ep_id", "epid"],
                &["seasonId", "season_id"],
                &["roomId", "room_id", "roomid"],
                &["accessKey", "access_key"],
                &[
                    "userDesireQn",
                    "user_desire_qn",
                    "desireQn",
                    "desire_qn",
                    "currentQn",
                    "current_qn",
                ],
                &["contentType", "content_type"],
                &["seekTs", "seek_ts"],
            ] {
                if aliases.iter().any(|key| target.contains_key(*key)) {
                    continue;
                }
                if let Some((key, value)) = aliases
                    .iter()
                    .find_map(|key| object.get(*key).map(|value| (*key, value.clone())))
                {
                    target.insert(key.to_owned(), value);
                }
            }
            target
                .entry("title")
                .or_insert_with(|| Value::String(value_string(object.get("title"))));
        }
        let can_resolve = metadata.as_object().is_some_and(|value| {
            let aid = value_string(value.get("aid").or_else(|| value.get("avid")));
            let cid = value_string(value.get("cid"));
            let room = value_string(
                value
                    .get("roomId")
                    .or_else(|| value.get("room_id"))
                    .or_else(|| value.get("roomid")),
            );
            is_real_id(&room) || (is_real_id(&aid) && is_real_id(&cid))
        });
        if let Some(target) = metadata.as_object_mut() {
            target
                .entry("aid")
                .or_insert_with(|| Value::String("direct".into()));
            target
                .entry("cid")
                .or_insert_with(|| Value::String("direct".into()));
        }
        let request = PlayRequest::from_value_allow_direct(&metadata)?;
        match validate_media_url(&raw_url) {
            Ok(()) => Ok((
                request.clone(),
                ResolvedMedia {
                    source: MediaSource::Progressive { url: raw_url },
                    title: request.title_or_default(),
                    quality: "source".into(),
                    available_qualities: Vec::new(),
                    duration_ms: None,
                    live: is_real_id(&request.room_id),
                },
            )),
            Err(_) if can_resolve => {
                let media = self.resolve_play(&request).await?;
                Ok((request, media))
            }
            Err(error) => Err(error),
        }
    }

    async fn resolve_live(&self, request: &PlayRequest) -> Result<Value> {
        let mut url = Url::parse(LIVE_PLAY_URL).unwrap();
        let quality = request.desired_quality.to_string();
        url.query_pairs_mut().extend_pairs([
            ("room_id", request.room_id.as_str()),
            ("protocol", "0,1"),
            ("format", "0,1,2"),
            ("codec", "0,1"),
            ("qn", quality.as_str()),
            ("platform", "web"),
            ("ptype", "8"),
        ]);
        self.get_json_url(url).await
    }

    async fn resolve_tv(&self, request: &PlayRequest, profile: TvProfile) -> Result<Value> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let api_requested_quality = tv_api_requested_quality(request.desired_quality, profile);
        let value = self
            .get_json_url(signed_tv_play_url(request, timestamp, profile))
            .await?;
        log_playurl_quality(profile, api_requested_quality, request, &value);
        Ok(value)
    }

    async fn get_json_url(&self, mut url: Url) -> Result<Value> {
        let mut redirects = 0;
        let response = loop {
            parse::validate_api_url(&url)?;
            let response =
                self.client.get(url.clone()).send().await.map_err(|error| {
                    anyhow!("Bilibili API request failed: {}", error.without_url())
                })?;
            if response.status().is_redirection() {
                if redirects == MAX_API_REDIRECTS {
                    bail!("Bilibili API exceeded the redirect limit");
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .context("Bilibili redirect omitted Location")?
                    .to_str()?;
                url = url.join(location)?;
                redirects += 1;
                continue;
            }
            if !response.status().is_success() {
                bail!("Bilibili API returned HTTP {}", response.status());
            }
            break response;
        };
        if response
            .content_length()
            .is_some_and(|length| length > MAX_API_RESPONSE_BYTES as u64)
        {
            bail!("Bilibili API response is too large");
        }
        let bytes = response.bytes().await?;
        if bytes.len() > MAX_API_RESPONSE_BYTES {
            bail!("Bilibili API response is too large");
        }
        let value: Value =
            serde_json::from_slice(&bytes).context("Bilibili API returned malformed JSON")?;
        let code = value.get("code").and_then(Value::as_i64).unwrap_or(0);
        if code != 0 {
            let message = value
                .get("message")
                .or_else(|| value.get("msg"))
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            bail!("Bilibili rejected the play request ({code}): {message}");
        }
        Ok(value)
    }
}

impl PlayRequest {
    fn from_value_allow_direct(value: &Value) -> Result<Self> {
        match Self::from_value(value) {
            Ok(request) => Ok(request),
            Err(_) => {
                let object = value.as_object().cloned().unwrap_or_default();
                Ok(Self {
                    aid: value_string(object.get("aid")),
                    cid: value_string(object.get("cid")),
                    title: value_string(object.get("title")),
                    desired_quality: value_u64(object.get("userDesireQn")).unwrap_or(80),
                    seek_position_ms: object
                        .get("seekTs")
                        .map(|value| nva_seek_position_ms(Some(value)))
                        .transpose()?
                        .unwrap_or(0),
                    ..Self::default()
                })
            }
        }
    }
}

fn resolved_media(request: &PlayRequest, value: &Value) -> Result<ResolvedMedia> {
    let quality = quality_label(value, request.desired_quality);
    let live = is_real_id(&request.room_id);
    Ok(ResolvedMedia {
        source: extract_media_source(value, request.desired_quality)?,
        available_qualities: quality_options(value, quality.parse().unwrap_or(0)),
        title: request.title_or_default(),
        quality,
        duration_ms: if live {
            None
        } else {
            playurl_duration_ms(value)
        },
        live,
    })
}

/// Bilibili reports the authoritative VOD length in `timelength` milliseconds.
/// Older/variant playurl payloads can omit it while retaining a DASH duration in
/// seconds or per-segment `durl[].length` values in milliseconds.
fn playurl_duration_ms(value: &Value) -> Option<u64> {
    let root = value
        .get("data")
        .or_else(|| value.get("result"))
        .unwrap_or(value);
    value_u64(
        root.get("timelength")
            .or_else(|| root.get("time_length"))
            .or_else(|| root.get("timeLength")),
    )
    .filter(|duration| *duration > 0)
    .or_else(|| {
        root.get("dash")
            .and_then(|dash| value_seconds_ms(dash.get("duration")))
            .filter(|duration| *duration > 0)
    })
    .or_else(|| {
        root.get("durl")
            .and_then(Value::as_array)
            .filter(|segments| !segments.is_empty())
            .and_then(|segments| {
                segments.iter().try_fold(0_u64, |total, segment| {
                    total.checked_add(value_u64(segment.get("length"))?)
                })
            })
            .filter(|duration| *duration > 0)
    })
}

fn value_seconds_ms(value: Option<&Value>) -> Option<u64> {
    let seconds = value.and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.trim().parse().ok()))
    })?;
    let milliseconds = seconds * 1_000.0;
    (seconds.is_finite()
        && seconds > 0.0
        && milliseconds.is_finite()
        && milliseconds <= u64::MAX as f64)
        .then(|| milliseconds.round() as u64)
}

/// The `qn` a profile is asked for. Only the Android TV DASH profile probes at the 4K
/// ceiling, because the legacy Nirvana profile answers with a `durl` menu sized to the
/// requested value and would report a worse picture than the sender chose. Selection
/// elsewhere still uses `desired_quality`, so this never upgrades the played stream.
fn tv_api_requested_quality(desired_quality: u64, profile: TvProfile) -> u64 {
    match profile {
        TvProfile::AndroidDash => desired_quality.max(NVA_4K_QUALITY),
        TvProfile::Legacy => desired_quality,
    }
}

/// Which side dropped a quality. Without this a missing 4K entry cannot be told apart
/// from an account that simply is not entitled to one.
fn log_playurl_quality(
    profile: TvProfile,
    api_requested_quality: u64,
    request: &PlayRequest,
    response: &Value,
) {
    let root = response
        .get("data")
        .or_else(|| response.get("result"))
        .unwrap_or(response);
    let response_quality = value_u64(root.get("quality"));
    let accept_quality = root
        .get("accept_quality")
        .or_else(|| root.get("acceptQuality"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| value_u64(Some(value)))
        .take(16)
        .collect::<Vec<_>>();
    info!(
        stage = "playurl-quality",
        profile = profile.label(),
        controller_requested_quality = request.desired_quality,
        api_requested_quality,
        response_quality = response_quality.unwrap_or_default(),
        response_quality_present = response_quality.is_some(),
        accept_quality = %accept_quality
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        delivery = media_delivery(root),
        "Bilibili answered a playurl quality request"
    );
}

fn media_delivery(root: &Value) -> &'static str {
    if root.get("dash").is_some_and(|dash| !dash.is_null()) {
        "dash"
    } else if root
        .get("durl")
        .and_then(Value::as_array)
        .is_some_and(|segments| !segments.is_empty())
    {
        "durl"
    } else {
        "none"
    }
}

fn signed_tv_play_url(request: &PlayRequest, timestamp: u64, profile: TvProfile) -> Url {
    let mut params = BTreeMap::from([
        ("access_key", request.access_key.clone()),
        ("actionKey", "appkey".into()),
        ("appkey", profile.app_key().into()),
        ("cid", request.cid.clone()),
        ("fourk", "1".into()),
        ("is_proj", "1".into()),
        ("mobile_access_key", request.access_key.clone()),
        ("object_id", request.oid.clone()),
        ("ogv_aid", request.aid.clone()),
        (
            "playurl_type",
            if is_real_id(&request.episode_id) {
                "2"
            } else {
                "1"
            }
            .into(),
        ),
        (
            "qn",
            tv_api_requested_quality(request.desired_quality, profile).to_string(),
        ),
        ("ts", timestamp.to_string()),
    ]);
    match profile {
        TvProfile::AndroidDash => {
            params.extend([
                ("build", "105700".into()),
                ("channel", "master".into()),
                ("device_name", "android".into()),
                ("fnval", "976".into()),
                ("fnver", "0".into()),
                ("mobi_app", "android_tv_yst".into()),
                ("platform", "android".into()),
            ]);
        }
        TvProfile::Legacy => {
            params.extend([
                ("build", "36700100".into()),
                ("platform", "ios".into()),
                ("protocol", "0".into()),
            ]);
        }
    }
    let unsigned = encoded_query(&params);
    params.insert(
        "sign",
        format!(
            "{:x}",
            md5::compute(format!("{unsigned}{}", profile.app_secret()))
        ),
    );
    let mut url = Url::parse(TV_PLAY_URL).unwrap();
    url.set_query(Some(&encoded_query(&params)));
    url
}

fn encoded_query(params: &BTreeMap<&str, String>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

fn decode_metadata(mut value: Value) -> Value {
    for _ in 0..2 {
        let Value::String(encoded) = &value else {
            break;
        };
        if encoded.len() > MAX_EMBEDDED_METADATA_BYTES {
            break;
        }
        let Ok(decoded) = serde_json::from_str(encoded) else {
            break;
        };
        value = decoded;
    }
    value
}

fn is_real_id(value: &str) -> bool {
    value.parse::<u64>().is_ok_and(|value| value != 0)
}

fn value_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn value_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

pub fn nva_seek_position_ms(value: Option<&Value>) -> Result<u64> {
    let value = value.ok_or_else(|| anyhow!("missing seek position"))?;
    if let Some(seconds) = value.as_u64() {
        return seconds
            .checked_mul(1_000)
            .ok_or_else(|| anyhow!("seek position is too large"));
    }
    let seconds = value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.trim().parse().ok()))
        .ok_or_else(|| anyhow!("seek position is invalid"))?;
    let milliseconds = seconds * 1_000.0;
    if !seconds.is_finite()
        || seconds < 0.0
        || !milliseconds.is_finite()
        || milliseconds > u64::MAX as f64
    {
        bail!("seek position is invalid");
    }
    Ok(milliseconds.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_play_and_redacts_access_key() {
        let request = PlayRequest::from_value(&json!({
            "aid": 1,
            "cid": 2,
            "accessKey": "secret",
            "userDesireQn": 116,
            "seasonId": "44",
            "contentType": "2"
        }))
        .unwrap();
        assert_eq!(request.desired_quality, 116);
        assert_eq!(request.season_id, "44");
        assert_eq!(request.content_type, 2);
        assert!(!format!("{request:?}").contains("secret"));
    }

    #[test]
    fn resolves_the_exact_playurl_duration_in_milliseconds() {
        let request = PlayRequest {
            aid: "1".into(),
            oid: "1".into(),
            cid: "2".into(),
            title: "Timed video".into(),
            ..PlayRequest::default()
        };
        let media = resolved_media(
            &request,
            &json!({"data": {
                "quality": 80,
                "timelength": 296_789,
                "dash": {"duration": 297},
                "durl": [{
                    "url": "https://cdn.bilivideo.com/video.mp4",
                    "length": 296_000
                }]
            }}),
        )
        .unwrap();
        assert_eq!(media.duration_ms, Some(296_789));
    }

    #[test]
    fn playurl_duration_uses_documented_fallback_units() {
        assert_eq!(
            playurl_duration_ms(&json!({"data": {"dash": {"duration": "1.25"}}})),
            Some(1_250)
        );
        assert_eq!(
            playurl_duration_ms(&json!({"result": {"durl": [
                {"length": "1200"},
                {"length": 345}
            ]}})),
            Some(1_545)
        );
        assert_eq!(
            playurl_duration_ms(&json!({"data": {"timelength": 0}})),
            None
        );
    }

    #[test]
    fn live_media_never_claims_a_finite_playurl_duration() {
        let request = PlayRequest {
            room_id: "42".into(),
            ..PlayRequest::default()
        };
        let media = resolved_media(
            &request,
            &json!({"data": {
                "timelength": 296_789,
                "durl": [{"url": "https://cdn.bilivideo.com/live.flv"}]
            }}),
        )
        .unwrap();
        assert_eq!(media.duration_ms, None);
    }

    #[test]
    fn signed_android_request_asks_for_dash_at_the_4k_ceiling() {
        let request = PlayRequest {
            aid: "1".into(),
            oid: "1".into(),
            cid: "2".into(),
            desired_quality: 116,
            ..PlayRequest::default()
        };
        let url = signed_tv_play_url(&request, 123, TvProfile::AndroidDash);
        let query = url.query_pairs().collect::<BTreeMap<_, _>>();
        assert_eq!(query.get("fnval").map(|v| v.as_ref()), Some("976"));
        // The menu is only as good as the ceiling it was probed at, so the request goes
        // out at 120 while track selection still honours the sender's 116.
        assert_eq!(query.get("qn").map(|v| v.as_ref()), Some("120"));
        assert_eq!(
            tv_api_requested_quality(request.desired_quality, TvProfile::Legacy),
            116,
            "the legacy profile must not be asked for better picture than chosen"
        );
        assert_eq!(
            tv_api_requested_quality(NVA_4K_QUALITY + 1, TvProfile::AndroidDash),
            121,
            "a sender already above the ceiling is never lowered"
        );
        let selected = json!({"data": {"quality": 116, "dash": {"video": [
            {"id": 116, "codecs": 12, "bandwidth": 100, "baseUrl": "https://cdn.bilivideo.cn/a.m4s"},
            {"id": 120, "codecs": 12, "bandwidth": 400, "baseUrl": "https://cdn.bilivideo.cn/b.m4s"}
        ], "audio": []}}});
        assert_eq!(
            quality_label(&selected, request.desired_quality),
            "116",
            "probing at 4K must not silently upgrade the played stream"
        );
    }
}
