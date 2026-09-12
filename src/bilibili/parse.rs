use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Url;
use serde_json::Value;
use tracing::info;
use url::Host;

use super::MediaSource;

const BILIBILI_DOMAIN_SUFFIXES: &[&str] = &[
    "bilibili.com",
    "bilibili.tv",
    "biliapi.com",
    "biliapi.net",
    "biliapi.cn",
    "bilivideo.com",
    "bilivideo.cn",
    "hdslb.com",
];

pub fn extract_media_source(response: &Value, desired_quality: u64) -> Result<MediaSource> {
    let root = response
        .get("data")
        .or_else(|| response.get("result"))
        .unwrap_or(response);
    let mut first_rejection = None;

    if let Some(segments) = root.get("durl").and_then(Value::as_array) {
        if segments.len() > 1 {
            bail!("Bilibili returned a multi-segment durl");
        }
        if let Some(segment) = segments.first() {
            match progressive_url(segment) {
                Ok(Some(url)) => return Ok(MediaSource::Progressive { url }),
                Ok(None) => {}
                Err(error) => first_rejection = Some(error),
            }
        }
    }

    if let Some(dash) = root.get("dash") {
        let videos = dash
            .get("video")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Bilibili DASH response has no video track"))?;
        let video = select_dash_video_track(videos, desired_quality)
            .ok_or_else(|| anyhow!("Bilibili DASH response has no usable video track"))?;
        let audio = dash
            .get("audio")
            .and_then(Value::as_array)
            .and_then(|tracks| {
                tracks
                    .iter()
                    .max_by_key(|track| (audio_is_aac(track), bandwidth(track)))
            })
            .ok_or_else(|| anyhow!("Bilibili DASH response has no usable audio track"))?;
        info!(
            requested_quality = desired_quality,
            video_quality = value_u64(video.get("id")).unwrap_or(0),
            video_codec = video_codec_name(video),
            video_codec_profile = track_codec(video),
            video_codec_id = value_u64(video.get("codecid")).unwrap_or(0),
            video_width = value_u64(video.get("width")).unwrap_or(0),
            video_height = value_u64(video.get("height")).unwrap_or(0),
            video_bandwidth = bandwidth(video),
            video_frame_rate = video
                .get("frame_rate")
                .or_else(|| video.get("frameRate"))
                .and_then(|value| value.as_str())
                .unwrap_or("unknown"),
            audio_codec = track_codec(audio),
            audio_bandwidth = bandwidth(audio),
            audio_aac = audio_is_aac(audio),
            "selected DASH tracks"
        );
        let mut video_urls = track_urls(video)?;
        let mut audio_urls = track_urls(audio)?;
        if !video_urls.is_empty() && !audio_urls.is_empty() {
            return Ok(MediaSource::Dash {
                video_url: video_urls.remove(0),
                video_backup_urls: video_urls,
                audio_url: audio_urls.remove(0),
                audio_backup_urls: audio_urls,
            });
        }
    }

    if let Some(streams) = root
        .pointer("/playurl_info/playurl/stream")
        .and_then(Value::as_array)
    {
        let mut live_candidates = Vec::new();
        for codec in streams
            .iter()
            .flat_map(|stream| array(stream.get("format")))
            .flat_map(|format| array(format.get("codec")))
        {
            let base = codec
                .get("base_url")
                .or_else(|| codec.get("baseUrl"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            for info in array(codec.get("url_info")) {
                let host = info.get("host").and_then(Value::as_str).unwrap_or_default();
                let extra = info
                    .get("extra")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let candidate = format!("{host}{base}{extra}");
                match validate_media_url(&candidate) {
                    Ok(()) => live_candidates.push(candidate),
                    Err(error) if first_rejection.is_none() => first_rejection = Some(error),
                    Err(_) => {}
                }
            }
        }
        live_candidates.sort_by_key(|url| live_url_priority(url));
        if let Some(url) = live_candidates.into_iter().next() {
            return Ok(MediaSource::Progressive { url });
        }
    }

    for key in ["live_mobile", "live_stream"] {
        let mut candidates = Vec::new();
        collect_http_strings(root.get(key).unwrap_or(&Value::Null), &mut candidates);
        for candidate in candidates {
            match validate_media_url(candidate) {
                Ok(()) => {
                    return Ok(MediaSource::Progressive {
                        url: candidate.to_owned(),
                    });
                }
                Err(error) if first_rejection.is_none() => first_rejection = Some(error),
                Err(_) => {}
            }
        }
    }
    if let Some(error) = first_rejection {
        return Err(error);
    }
    bail!("Bilibili response has no usable media URL")
}

fn live_url_priority(url: &str) -> u8 {
    let path = Url::parse(url)
        .ok()
        .map(|url| url.path().to_ascii_lowercase())
        .unwrap_or_else(|| url.to_ascii_lowercase());
    if path.ends_with(".flv") {
        0
    } else if path.ends_with(".m3u8") {
        1
    } else if path.ends_with(".ts") || path.ends_with(".m2ts") {
        2
    } else {
        3
    }
}

fn array(value: Option<&Value>) -> impl Iterator<Item = &Value> {
    value.and_then(Value::as_array).into_iter().flatten()
}

fn progressive_url(segment: &Value) -> Result<Option<String>> {
    let mut candidates = Vec::new();
    if let Some(url) = segment.get("url").and_then(Value::as_str) {
        candidates.push(url);
    }
    candidates.extend(
        segment
            .get("backup_url")
            .or_else(|| segment.get("backupUrl"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str),
    );
    for candidate in candidates {
        if validate_media_url(candidate).is_ok() {
            return Ok(Some(candidate.to_owned()));
        }
    }
    Ok(None)
}

fn track_urls(track: &Value) -> Result<Vec<String>> {
    let primary = track
        .get("base_url")
        .or_else(|| track.get("baseUrl"))
        .and_then(Value::as_str);
    let backups = track
        .get("backup_url")
        .or_else(|| track.get("backupUrl"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    let mut output = Vec::new();
    let mut first_error = None;
    for candidate in primary.into_iter().chain(backups) {
        match validate_media_url(candidate) {
            Ok(()) => output.push(candidate.to_owned()),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }
    if output.is_empty()
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(output)
}

fn bandwidth(track: &Value) -> u64 {
    value_u64(track.get("bandwidth")).unwrap_or(0)
}

fn video_is_avc(track: &Value) -> bool {
    if value_u64(track.get("codecid")) == Some(7) {
        return true;
    }
    track
        .get("codecs")
        .or_else(|| track.get("codec"))
        .and_then(Value::as_str)
        .is_some_and(|codec| {
            let codec = codec.to_ascii_lowercase();
            codec.starts_with("avc1") || codec.starts_with("avc3") || codec.starts_with("h264")
        })
}

fn video_is_hevc(track: &Value) -> bool {
    if value_u64(track.get("codecid")) == Some(12) {
        return true;
    }
    track
        .get("codecs")
        .or_else(|| track.get("codec"))
        .and_then(Value::as_str)
        .is_some_and(|codec| {
            let codec = codec.to_ascii_lowercase();
            codec.starts_with("hev1")
                || codec.starts_with("hvc1")
                || codec.starts_with("hevc")
                || codec.starts_with("h265")
        })
}

fn video_is_av1(track: &Value) -> bool {
    if value_u64(track.get("codecid")) == Some(13) {
        return true;
    }
    track
        .get("codecs")
        .or_else(|| track.get("codec"))
        .and_then(Value::as_str)
        .is_some_and(|codec| codec.to_ascii_lowercase().starts_with("av01"))
}

fn video_codec_rank(track: &Value) -> u8 {
    if video_is_avc(track) {
        3
    } else if video_is_hevc(track) {
        2
    } else if video_is_av1(track) {
        1
    } else {
        0
    }
}

fn video_codec_name(track: &Value) -> &'static str {
    if video_is_avc(track) {
        "avc"
    } else if video_is_hevc(track) {
        "hevc"
    } else if video_is_av1(track) {
        "av1"
    } else {
        "unknown"
    }
}

fn audio_is_aac(track: &Value) -> bool {
    track
        .get("codecs")
        .or_else(|| track.get("codec"))
        .and_then(Value::as_str)
        .is_some_and(|codec| codec.to_ascii_lowercase().starts_with("mp4a"))
}

fn track_codec(track: &Value) -> &str {
    track
        .get("codecs")
        .or_else(|| track.get("codec"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}

fn quality_tier(quality: u64) -> Option<u8> {
    match quality {
        6 => Some(1),
        16 => Some(2),
        32 => Some(3),
        64 => Some(4),
        74 => Some(5),
        77 | 80 => Some(6),
        102 | 112 => Some(7),
        116 => Some(8),
        120 | 121 => Some(9),
        125 => Some(10),
        126 => Some(11),
        127 => Some(12),
        _ => None,
    }
}

fn select_dash_video_track(videos: &[Value], desired: u64) -> Option<&Value> {
    let desired_tier = quality_tier(desired);
    let chosen_tier = desired_tier.and_then(|wanted| {
        videos
            .iter()
            .filter_map(|track| value_u64(track.get("id")).and_then(quality_tier))
            .filter(|tier| *tier <= wanted)
            .max()
    });
    let exact_unknown = desired_tier.is_none()
        && videos
            .iter()
            .any(|track| value_u64(track.get("id")) == Some(desired));
    videos
        .iter()
        .filter(|track| match chosen_tier {
            Some(tier) => value_u64(track.get("id")).and_then(quality_tier) == Some(tier),
            None if exact_unknown => value_u64(track.get("id")) == Some(desired),
            None => true,
        })
        .max_by_key(|track| (video_codec_rank(track), bandwidth(track)))
}

pub fn quality_label(response: &Value, fallback: u64) -> String {
    let root = response
        .get("data")
        .or_else(|| response.get("result"))
        .unwrap_or(response);
    root.pointer("/dash/video")
        .and_then(Value::as_array)
        .and_then(|videos| select_dash_video_track(videos, fallback))
        .and_then(|track| value_u64(track.get("id")))
        .or_else(|| value_u64(root.get("quality")))
        .unwrap_or(fallback)
        .to_string()
}

pub fn quality_options(response: &Value, selected: u64) -> Vec<u64> {
    let root = response
        .get("data")
        .or_else(|| response.get("result"))
        .unwrap_or(response);
    let mut values = root
        .get("accept_quality")
        .or_else(|| root.get("acceptQuality"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| value_u64(Some(value)))
        .collect::<Vec<_>>();
    if values.is_empty() {
        values.extend(
            root.pointer("/dash/video")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|track| value_u64(track.get("id"))),
        );
    }
    values.push(selected);
    values.sort_unstable_by(|left, right| {
        quality_tier(*right)
            .unwrap_or(0)
            .cmp(&quality_tier(*left).unwrap_or(0))
            .then_with(|| right.cmp(left))
    });
    values.dedup();
    values.truncate(16);
    values
}

pub fn validate_api_url(url: &Url) -> Result<()> {
    if url.scheme() != "https" {
        bail!("Bilibili API must use HTTPS");
    }
    let host = url.host_str().context("Bilibili API URL has no host")?;
    if !is_allowed_bilibili_host(host) {
        bail!("Bilibili API host is not allowed");
    }
    Ok(())
}

pub fn validate_media_url(value: &str) -> Result<()> {
    if value.len() > 16 * 1024 {
        bail!("media URL exceeds 16 KiB");
    }
    let url = Url::parse(value).context("media URL is invalid")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("media URL must use HTTP or HTTPS");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("media URL must not contain credentials");
    }
    match url.host() {
        Some(Host::Domain(host)) if is_public_hostname(host) => Ok(()),
        Some(Host::Ipv4(ip)) if is_public_ipv4(ip) => Ok(()),
        Some(Host::Ipv6(ip)) if is_public_ipv6(ip) => Ok(()),
        Some(_) => bail!("media URL does not point to a public Internet host"),
        None => bail!("media URL has no host"),
    }
}

fn is_allowed_bilibili_host(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    BILIBILI_DOMAIN_SUFFIXES
        .iter()
        .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
        || host.ends_with(".akamaized.net")
}

fn is_public_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    const RESERVED: &[&str] = &[
        "localhost",
        "local",
        "internal",
        "lan",
        "home",
        "home.arpa",
        "invalid",
        "test",
        "example",
        "onion",
    ];
    host.contains('.')
        && !RESERVED
            .iter()
            .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, d] = ip.octets();
    !(a == 0
        || a == 10
        || a == 127
        || a >= 224
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 168)
        || (a == 192 && b == 0 && c == 2)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || (a == 255 && b == 255 && c == 255 && d == 255))
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    let segments = ip.segments();
    (segments[0] & 0xe000) == 0x2000
        && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
        && segments[0] != 0x3fff
}

pub(crate) fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn collect_http_strings<'a>(value: &'a Value, output: &mut Vec<&'a str>) {
    match value {
        Value::String(value) if value.starts_with("http://") || value.starts_with("https://") => {
            output.push(value)
        }
        Value::Array(values) => {
            for value in values {
                collect_http_strings(value, output);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_http_strings(value, output);
            }
        }
        _ => {}
    }
}

fn value_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn chooses_avc_at_requested_quality() {
        let value = json!({"data":{"dash":{
            "video":[
                {"id":116,"codecid":12,"bandwidth":900,"baseUrl":"https://cdn.example.net/hevc.m4s"},
                {"id":116,"codecid":7,"bandwidth":700,"baseUrl":"https://cdn.example.net/avc.m4s"},
                {"id":120,"codecid":7,"bandwidth":1000,"baseUrl":"https://cdn.example.net/4k.m4s"}
            ],
            "audio":[
                {"id":30280,"codecs":"mp4a.40.2","bandwidth":192,"baseUrl":"https://cdn.example.net/audio.m4s"}
            ]
        }}});
        assert_eq!(quality_label(&value, 116), "116");
        let source = extract_media_source(&value, 116).expect("usable 1080p source");
        assert!(matches!(
            source,
            MediaSource::Dash { video_url, .. } if video_url.ends_with("/avc.m4s")
        ));
    }

    #[test]
    fn prefers_hevc_over_av1_when_4k_has_no_avc_track() {
        let value = json!({"data":{"dash":{
            "video":[
                {"id":120,"codecs":"av01.0.12M.08","bandwidth":1800,"baseUrl":"https://cdn.example.net/av1.m4s"},
                {"id":120,"codecs":"hev1.1.6.L150.90","bandwidth":1500,"baseUrl":"https://cdn.example.net/hevc.m4s"},
                {"id":120,"codecs":"future-codec","bandwidth":9999,"baseUrl":"https://cdn.example.net/unknown.m4s"}
            ],
            "audio":[
                {"id":30280,"codecs":"mp4a.40.2","bandwidth":192,"baseUrl":"https://cdn.example.net/audio.m4s"}
            ]
        }}});
        let source = extract_media_source(&value, 120).expect("usable 4K source");
        assert!(matches!(
            source,
            MediaSource::Dash { video_url, .. } if video_url.ends_with("/hevc.m4s")
        ));
    }

    #[test]
    fn accepts_public_ip_cdn_but_rejects_lan() {
        assert!(validate_media_url("https://114.230.222.21/video.m4s").is_ok());
        assert!(validate_media_url("http://10.0.0.2/video.m4s").is_err());
    }

    #[test]
    fn prefers_flv_for_live_remux_when_hls_is_listed_first() {
        let value = json!({"data":{"playurl_info":{"playurl":{"stream":[
            {"format":[{"codec":[{"base_url":"/live.m3u8","url_info":[{"host":"https://cdn.example.net","extra":"?a=1"}]}]}]},
            {"format":[{"codec":[{"base_url":"/live.flv","url_info":[{"host":"https://cdn.example.net","extra":"?a=1"}]}]}]}
        ]}}}});
        let source = extract_media_source(&value, 10000).expect("live source");
        assert!(matches!(
            source,
            MediaSource::Progressive { url } if url.contains("live.flv")
        ));
    }
}
