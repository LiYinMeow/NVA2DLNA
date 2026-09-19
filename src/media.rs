use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use reqwest::{Client, Url, redirect::Policy};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::lookup_host,
    process::Command,
    sync::OwnedSemaphorePermit,
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    bilibili::{is_public_ip, validate_outbound_media_url},
    state::{
        AppState, HlsResourceStore, MAX_FFMPEG_CONSUMERS, MediaEntry, MediaInput, RemuxFormat,
    },
};

const BILI_REFERER: &str = "https://www.bilibili.com/";
const BILI_ORIGIN: &str = "https://www.bilibili.com";
const BILI_USER_AGENT: &str = "Mozilla/5.0 NVA2DLNA/0.1";
const MAX_MEDIA_REDIRECTS: usize = 4;
const MAX_HLS_PLAYLIST_BYTES: usize = 4 * 1024 * 1024;
const MAX_HLS_RESOURCES: usize = 4096;
const HLS_SNIFF_BYTES: usize = 1024;
const FFMPEG_CONSUMER_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
const MAX_FFMPEG_DIAGNOSTICS_BYTES: usize = 64 * 1024;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/media/{token}/{name}", get(media_get).head(media_head))
        .route(
            "/upstream/{token}/{track}",
            get(upstream_get).head(upstream_head),
        )
        .route(
            "/upstream/{token}/hls/{resource}",
            get(hls_resource_get).head(hls_resource_head),
        )
}

async fn media_get(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((token, _name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    media_response(state, peer, token, headers, false).await
}

async fn media_head(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((token, _name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    media_response(state, peer, token, headers, true).await
}

async fn media_response(
    state: AppState,
    peer: SocketAddr,
    token: String,
    headers: HeaderMap,
    head_only: bool,
) -> Response<Body> {
    let Some(entry) = state.media(&token).await else {
        return simple(StatusCode::NOT_FOUND, "media token is no longer active");
    };
    if !same_ip(peer.ip(), &entry.allowed_renderer_ip) && !peer.ip().is_loopback() {
        return simple(
            StatusCode::FORBIDDEN,
            "media token belongs to another renderer",
        );
    }
    match &entry.input {
        MediaInput::Progressive { url } => {
            match proxy_urls(
                std::slice::from_ref(url),
                &headers,
                head_only,
                entry.cancellation.clone(),
            )
            .await
            {
                Ok((response, _)) => response,
                Err(error) => {
                    warn!(%error, "progressive media proxy failed");
                    simple(StatusCode::BAD_GATEWAY, "upstream media request failed")
                }
            }
        }
        MediaInput::Dash { .. } | MediaInput::Remux { .. } if head_only => {
            dash_headers(&entry, Body::empty())
        }
        MediaInput::Dash { .. } | MediaInput::Remux { .. } => {
            let range = headers
                .get(header::RANGE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("none");
            let user_agent = headers
                .get(header::USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("none");
            let content_features = headers
                .get("getcontentFeatures.dlna.org")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("none");
            let Some(lease) = FfmpegLease::acquire(&entry).await else {
                if entry.cancellation.is_cancelled() || entry.ffmpeg_consumers.is_closed() {
                    return simple(StatusCode::GONE, "media token is no longer active");
                }
                warn!(
                    media_token = %entry.token.chars().take(8).collect::<String>(),
                    renderer = %peer.ip(),
                    range,
                    user_agent,
                    content_features,
                    max_consumers = MAX_FFMPEG_CONSUMERS,
                    "rejected excess concurrent FFmpeg consumer"
                );
                let mut response = simple(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "remuxed stream has too many consumers; retry shortly",
                );
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
                return response;
            };
            info!(
                media_token = %entry.token.chars().take(8).collect::<String>(),
                renderer = %peer.ip(),
                range,
                user_agent,
                content_features,
                active_consumers = lease.active_consumers,
                "accepted FFmpeg consumer"
            );
            match ffmpeg_stream(&state, &entry, lease) {
                Ok(body) => dash_headers(&entry, body),
                Err(error) => {
                    warn!(%error, "cannot start DASH merger");
                    simple(
                        StatusCode::BAD_GATEWAY,
                        "FFmpeg DASH merger could not start",
                    )
                }
            }
        }
    }
}

async fn upstream_get(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((token, track)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    upstream_response(state, peer, token, track, headers, false).await
}

async fn upstream_head(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((token, track)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    upstream_response(state, peer, token, track, headers, true).await
}

async fn upstream_response(
    state: AppState,
    peer: SocketAddr,
    token: String,
    track: String,
    headers: HeaderMap,
    head_only: bool,
) -> Response<Body> {
    let Some(entry) = state.media(&token).await else {
        return simple(StatusCode::NOT_FOUND, "media token is no longer active");
    };
    if !internal_or_cast_peer(&state, &entry, peer.ip()) {
        return simple(StatusCode::FORBIDDEN, "upstream proxy is cast-session-only");
    }
    if let (MediaInput::Remux { url, format }, "source") = (&entry.input, track.as_str()) {
        return match hls_or_media_response(
            &state,
            &entry,
            url,
            &headers,
            head_only,
            *format == RemuxFormat::Hls,
            None,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                warn!(%error, "remux source proxy failed");
                simple(StatusCode::BAD_GATEWAY, "remux source request failed")
            }
        };
    }
    let urls = match (&entry.input, track.as_str()) {
        (
            MediaInput::Dash {
                video_url,
                video_backup_urls,
                ..
            },
            "video",
        ) => std::iter::once(video_url.clone())
            .chain(video_backup_urls.iter().cloned())
            .collect::<Vec<_>>(),
        (
            MediaInput::Dash {
                audio_url,
                audio_backup_urls,
                ..
            },
            "audio",
        ) => std::iter::once(audio_url.clone())
            .chain(audio_backup_urls.iter().cloned())
            .collect::<Vec<_>>(),
        _ => return simple(StatusCode::NOT_FOUND, "unknown DASH track"),
    };
    match proxy_urls(&urls, &headers, head_only, entry.cancellation.clone()).await {
        Ok((response, _)) => response,
        Err(error) => {
            warn!(track, %error, "all DASH upstream URLs failed");
            simple(StatusCode::BAD_GATEWAY, "DASH upstream request failed")
        }
    }
}

async fn hls_resource_get(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((token, resource)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    hls_resource_response(state, peer, token, resource, headers, false).await
}

async fn hls_resource_head(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path((token, resource)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    hls_resource_response(state, peer, token, resource, headers, true).await
}

async fn hls_resource_response(
    state: AppState,
    peer: SocketAddr,
    token: String,
    resource: String,
    headers: HeaderMap,
    head_only: bool,
) -> Response<Body> {
    let Some(entry) = state.media(&token).await else {
        return simple(StatusCode::NOT_FOUND, "media token is no longer active");
    };
    if !internal_or_cast_peer(&state, &entry, peer.ip()) {
        return simple(StatusCode::FORBIDDEN, "HLS proxy is cast-session-only");
    }
    let url = {
        let mut resources = entry.hls_resources.write().await;
        touch_hls_resource(&mut resources, &resource)
    };
    let Some(url) = url else {
        return simple(StatusCode::NOT_FOUND, "unknown HLS resource");
    };
    match hls_or_media_response(
        &state,
        &entry,
        &url,
        &headers,
        head_only,
        false,
        Some(&resource),
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            warn!(%error, "HLS resource proxy failed");
            simple(StatusCode::BAD_GATEWAY, "HLS resource request failed")
        }
    }
}

async fn hls_or_media_response(
    state: &AppState,
    entry: &MediaEntry,
    url: &str,
    headers: &HeaderMap,
    head_only: bool,
    force_hls: bool,
    pinned_resource: Option<&str>,
) -> Result<Response<Body>> {
    let expected_playlist = force_hls || looks_like_hls_url(url);
    let urls = [url.to_owned()];
    let playlist_headers = headers_without_range(headers);
    let initial_headers = if expected_playlist || head_only {
        &playlist_headers
    } else {
        headers
    };
    // Keep the body for HEAD requests: an extensionless resource can only be
    // identified as a playlist from its response body.  Non-playlist bodies
    // are only sniffed, then dropped before the HEAD response is returned.
    let (mut response, mut final_url) =
        proxy_urls(&urls, initial_headers, false, entry.cancellation.clone()).await?;
    let is_playlist =
        expected_playlist || looks_like_hls_url(final_url.as_str()) || response_is_hls(&response);

    // A playlist must be fetched as a complete entity.  In particular, never
    // rewrite a caller's partial Range response as if it were a full manifest.
    if is_playlist && response_is_partial(&response) {
        (response, final_url) =
            proxy_urls(&urls, &playlist_headers, false, entry.cancellation.clone()).await?;
        if response_is_partial(&response) {
            return Err(anyhow!("HLS origin did not return a complete playlist"));
        }
    }

    let was_partial = response_is_partial(&response);
    let (parts, body) = response.into_parts();
    let (mut parts, bytes) = if is_playlist {
        (
            parts,
            to_bytes(body, MAX_HLS_PLAYLIST_BYTES)
                .await
                .context("HLS playlist exceeds the size limit")?,
        )
    } else {
        let mut body_stream = body.into_data_stream();
        let mut buffered = Vec::new();
        let mut prefix = Vec::new();
        let detected = loop {
            match body_stream.next().await {
                Some(Ok(bytes)) => {
                    let remaining = HLS_SNIFF_BYTES.saturating_sub(prefix.len());
                    prefix.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
                    buffered.push(bytes);
                    if let Some(detected) = hls_prefix_status(&prefix, false) {
                        break detected;
                    }
                    if prefix.len() == HLS_SNIFF_BYTES {
                        break false;
                    }
                }
                Some(Err(error)) => return Err(error.into()),
                None => break hls_prefix_status(&prefix, true).unwrap_or(false),
            }
        };
        if !detected {
            if head_only {
                return Ok(Response::from_parts(parts, Body::empty()));
            }
            let buffered = stream::iter(buffered.into_iter().map(Ok::<Bytes, axum::Error>));
            return Ok(Response::from_parts(
                parts,
                Body::from_stream(buffered.chain(body_stream)),
            ));
        }

        let buffered_len = buffered.iter().map(Bytes::len).sum::<usize>();
        if buffered_len > MAX_HLS_PLAYLIST_BYTES {
            return Err(anyhow!("HLS playlist exceeds the size limit"));
        }
        let rest = to_bytes(
            Body::from_stream(body_stream),
            MAX_HLS_PLAYLIST_BYTES - buffered_len,
        )
        .await
        .context("HLS playlist exceeds the size limit")?;
        let mut bytes = Vec::with_capacity(buffered_len + rest.len());
        for chunk in buffered {
            bytes.extend_from_slice(&chunk);
        }
        bytes.extend_from_slice(&rest);
        (parts, Bytes::from(bytes))
    };

    // Content sniffing may discover an extensionless playlist only after a
    // ranged GET.  Repeat it without Range before parsing and rewriting.
    let bytes = if !is_playlist && was_partial {
        let (full_response, full_url) =
            proxy_urls(&urls, &playlist_headers, false, entry.cancellation.clone()).await?;
        if response_is_partial(&full_response) {
            return Err(anyhow!("HLS origin did not return a complete playlist"));
        }
        final_url = full_url;
        let (full_parts, full_body) = full_response.into_parts();
        parts = full_parts;
        to_bytes(full_body, MAX_HLS_PLAYLIST_BYTES)
            .await
            .context("HLS playlist exceeds the size limit")?
    } else {
        bytes
    };
    if hls_prefix_status(&bytes, true) != Some(true) {
        return Err(anyhow!("HLS playlist is missing the #EXTM3U header"));
    }
    let playlist = std::str::from_utf8(&bytes).context("HLS playlist is not UTF-8")?;
    let rewritten =
        rewrite_hls_playlist(state, entry, &final_url, playlist, pinned_resource).await?;
    parts.status = StatusCode::OK;
    parts.headers.remove(header::CONTENT_RANGE);
    parts.headers.remove(header::ACCEPT_RANGES);
    parts.headers.remove(header::ETAG);
    parts.headers.remove(header::LAST_MODIFIED);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apple.mpegurl"),
    );
    parts.headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&rewritten.len().to_string())?,
    );
    parts
        .headers
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    let body = if head_only {
        Body::empty()
    } else {
        Body::from(rewritten)
    };
    Ok(Response::from_parts(parts, body))
}

fn headers_without_range(headers: &HeaderMap) -> HeaderMap {
    let mut headers = headers.clone();
    headers.remove(header::RANGE);
    headers
}

fn response_is_partial(response: &Response<Body>) -> bool {
    response.status() == StatusCode::PARTIAL_CONTENT
        || response.headers().contains_key(header::CONTENT_RANGE)
}

fn hls_prefix_status(bytes: &[u8], eof: bool) -> Option<bool> {
    const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";
    const MARKER: &[u8] = b"#EXTM3U";

    let mut start = 0;
    if bytes.starts_with(UTF8_BOM) {
        start = UTF8_BOM.len();
    } else if !eof && bytes.len() < UTF8_BOM.len() && UTF8_BOM.starts_with(bytes) {
        return None;
    }
    while bytes.get(start).is_some_and(u8::is_ascii_whitespace) {
        start += 1;
    }
    let candidate = &bytes[start..];
    if candidate.starts_with(MARKER) {
        Some(true)
    } else if !eof && MARKER.starts_with(candidate) {
        None
    } else {
        Some(false)
    }
}

fn looks_like_hls_url(url: &str) -> bool {
    Url::parse(url)
        .ok()
        .is_some_and(|url| url.path().to_ascii_lowercase().ends_with(".m3u8"))
}

fn response_is_hls(response: &Response<Body>) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            let value = value.to_ascii_lowercase();
            value.contains("mpegurl") || value.contains("vnd.apple.mpegurl")
        })
}

async fn rewrite_hls_playlist(
    state: &AppState,
    entry: &MediaEntry,
    base: &Url,
    playlist: &str,
    pinned_resource: Option<&str>,
) -> Result<String> {
    let playlist = playlist.strip_prefix('\u{feff}').unwrap_or(playlist);
    let playlist = playlist.trim_start_matches(|character: char| character.is_ascii_whitespace());
    let mut batch = HlsRewriteBatch::default();
    let mut output = String::with_capacity(playlist.len() + 256);
    for line in playlist.lines() {
        let line = line.trim_start_matches(|character: char| character.is_ascii_whitespace());
        let rewritten = if line.starts_with('#') {
            rewrite_hls_uri_attributes(line, |uri| {
                local_hls_resource_url(state, entry, base, uri, &mut batch)
            })?
        } else if line.trim().is_empty() {
            String::new()
        } else {
            local_hls_resource_url(state, entry, base, line.trim(), &mut batch)?
        };
        output.push_str(&rewritten);
        output.push('\n');
    }
    let mut resources = entry.hls_resources.write().await;
    commit_hls_resource_batch(&mut resources, batch, pinned_resource, MAX_HLS_RESOURCES)?;
    Ok(output)
}

#[derive(Debug, Default)]
struct HlsRewriteBatch {
    urls: HashMap<String, String>,
    order: Vec<String>,
}

fn rewrite_hls_uri_attributes<F>(line: &str, mut rewrite: F) -> Result<String>
where
    F: FnMut(&str) -> Result<String>,
{
    let Some(colon) = line.find(':') else {
        return Ok(line.to_owned());
    };
    let attribute_start = colon + 1;
    let attributes = &line[attribute_start..];
    let mut output = String::with_capacity(line.len() + 64);
    output.push_str(&line[..attribute_start]);
    let mut segment_start = 0;
    let mut quoted = false;
    for (index, byte) in attributes.bytes().enumerate() {
        match byte {
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                output.push_str(&rewrite_hls_attribute_segment(
                    &attributes[segment_start..index],
                    &mut rewrite,
                )?);
                output.push(',');
                segment_start = index + 1;
            }
            _ => {}
        }
    }
    if quoted {
        return Err(anyhow!("HLS tag has an unterminated quoted attribute"));
    }
    output.push_str(&rewrite_hls_attribute_segment(
        &attributes[segment_start..],
        &mut rewrite,
    )?);
    Ok(output)
}

fn rewrite_hls_attribute_segment<F>(segment: &str, rewrite: &mut F) -> Result<String>
where
    F: FnMut(&str) -> Result<String>,
{
    let bytes = segment.as_bytes();
    let mut name_start = 0;
    while bytes.get(name_start).is_some_and(u8::is_ascii_whitespace) {
        name_start += 1;
    }
    let mut name_end = name_start;
    while bytes
        .get(name_end)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        name_end += 1;
    }
    let mut equals = name_end;
    while bytes.get(equals).is_some_and(u8::is_ascii_whitespace) {
        equals += 1;
    }
    if bytes.get(equals) != Some(&b'=')
        || !segment[name_start..name_end].eq_ignore_ascii_case("URI")
    {
        return Ok(segment.to_owned());
    }
    let mut value_start = equals + 1;
    while bytes.get(value_start).is_some_and(u8::is_ascii_whitespace) {
        value_start += 1;
    }
    if value_start == bytes.len() {
        return Err(anyhow!("HLS URI attribute has no value"));
    }

    if bytes[value_start] == b'"' {
        let content_start = value_start + 1;
        let close = segment[content_start..]
            .find('"')
            .map(|offset| content_start + offset)
            .ok_or_else(|| anyhow!("HLS tag has an unterminated URI attribute"))?;
        if !segment[close + 1..].trim().is_empty() {
            return Err(anyhow!("HLS URI attribute has trailing data"));
        }
        let mut output = String::with_capacity(segment.len() + 32);
        output.push_str(&segment[..content_start]);
        output.push_str(&rewrite(&segment[content_start..close])?);
        output.push_str(&segment[close..]);
        Ok(output)
    } else {
        let value_end = segment.trim_end().len();
        if value_end <= value_start {
            return Err(anyhow!("HLS URI attribute has no value"));
        }
        let mut output = String::with_capacity(segment.len() + 32);
        output.push_str(&segment[..value_start]);
        output.push_str(&rewrite(&segment[value_start..value_end])?);
        output.push_str(&segment[value_end..]);
        Ok(output)
    }
}

fn local_hls_resource_url(
    state: &AppState,
    entry: &MediaEntry,
    base: &Url,
    uri: &str,
    batch: &mut HlsRewriteBatch,
) -> Result<String> {
    let upstream = base.join(uri).context("HLS resource URI is invalid")?;
    validate_outbound_media_url(upstream.as_str())?;
    let resource = format!("{:x}", md5::compute(upstream.as_str().as_bytes()));
    let upstream = upstream.to_string();
    if let Some(existing) = batch.urls.get(&resource) {
        if existing != &upstream {
            return Err(anyhow!("HLS resource identifier collision"));
        }
    } else {
        if batch.urls.len() >= MAX_HLS_RESOURCES {
            return Err(anyhow!("HLS playlist has too many resources"));
        }
        batch.order.push(resource.clone());
        batch.urls.insert(resource.clone(), upstream);
    }
    Ok(format!(
        "http://{}:{}/upstream/{}/hls/{resource}",
        entry.gateway_address,
        state.web_port(),
        entry.token
    ))
}

fn internal_or_cast_peer(state: &AppState, entry: &MediaEntry, peer: IpAddr) -> bool {
    peer.is_loopback()
        || peer == IpAddr::V4(state.internal_http_ip())
        || entry
            .gateway_address
            .parse::<IpAddr>()
            .is_ok_and(|address| address == peer)
        || entry
            .allowed_renderer_ip
            .parse::<IpAddr>()
            .is_ok_and(|address| address == peer)
}

fn touch_hls_resource(resources: &mut HlsResourceStore, resource: &str) -> Option<String> {
    let url = resources.urls.get(resource)?.clone();
    resources.order.retain(|stored| stored != resource);
    resources.order.push_back(resource.to_owned());
    Some(url)
}

fn commit_hls_resource_batch(
    resources: &mut HlsResourceStore,
    mut batch: HlsRewriteBatch,
    pinned_resource: Option<&str>,
    limit: usize,
) -> Result<()> {
    let pinned = pinned_resource
        .filter(|resource| !batch.urls.contains_key(*resource))
        .and_then(|resource| {
            resources
                .urls
                .get(resource)
                .cloned()
                .map(|url| (resource.to_owned(), url))
        });
    let protected_count = batch.urls.len() + usize::from(pinned.is_some());
    if protected_count > limit {
        return Err(anyhow!("HLS playlist has too many resources"));
    }
    for (resource, url) in &batch.urls {
        if resources
            .urls
            .get(resource)
            .is_some_and(|existing| existing != url)
        {
            return Err(anyhow!("HLS resource identifier collision"));
        }
    }

    // Detach every protected entry before eviction.  They are inserted again
    // at the LRU tail only after enough unreferenced entries have been freed.
    for resource in &batch.order {
        resources.urls.remove(resource);
        resources.order.retain(|stored| stored != resource);
    }
    if let Some((resource, _)) = &pinned {
        resources.urls.remove(resource);
        resources.order.retain(|stored| stored != resource);
    }
    while resources.urls.len() + protected_count > limit {
        if let Some(expired) = resources.order.pop_front() {
            resources.urls.remove(&expired);
        } else if let Some(expired) = resources.urls.keys().next().cloned() {
            resources.urls.remove(&expired);
        } else {
            break;
        }
    }
    for resource in batch.order {
        let url = batch
            .urls
            .remove(&resource)
            .context("HLS rewrite batch is inconsistent")?;
        resources.urls.insert(resource.clone(), url);
        resources.order.push_back(resource);
    }
    if let Some((resource, url)) = pinned {
        resources.urls.insert(resource.clone(), url);
        resources.order.push_back(resource);
    }
    debug_assert!(resources.urls.len() <= limit);
    debug_assert_eq!(resources.urls.len(), resources.order.len());
    Ok(())
}

async fn proxy_urls(
    urls: &[String],
    incoming_headers: &HeaderMap,
    head_only: bool,
    cancellation: CancellationToken,
) -> Result<(Response<Body>, Url)> {
    let mut last_error = None;
    for url in urls {
        let mut current = match Url::parse(url) {
            Ok(url) => url,
            Err(error) => {
                last_error = Some(anyhow!(error).context("media URL is invalid"));
                continue;
            }
        };
        for redirect_count in 0..=MAX_MEDIA_REDIRECTS {
            let client = match pinned_media_client(&current).await {
                Ok(client) => client,
                Err(error) => {
                    last_error = Some(error);
                    break;
                }
            };
            // A one-byte Range GET is more interoperable than HEAD with
            // signed Bilibili CDNs. Its body is discarded for a local HEAD.
            let mut request = client
                .get(current.clone())
                .header(header::REFERER, BILI_REFERER)
                .header(header::ORIGIN, BILI_ORIGIN)
                .header(header::USER_AGENT, BILI_USER_AGENT)
                .header(header::ACCEPT_ENCODING, "identity");
            if let Some(value) = incoming_headers.get(header::ACCEPT) {
                request = request.header(header::ACCEPT, value);
            }
            if head_only {
                request = request.header(header::RANGE, "bytes=0-0");
            } else if let Some(value) = incoming_headers.get(header::RANGE) {
                request = request.header(header::RANGE, value);
            }

            let result = tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(anyhow!("media request was cancelled"));
                }
                result = request.send() => result,
            };
            match result {
                Ok(response) if response.status().is_redirection() => {
                    if redirect_count == MAX_MEDIA_REDIRECTS {
                        last_error = Some(anyhow!("media redirect limit exceeded"));
                        break;
                    }
                    let Some(location) = response
                        .headers()
                        .get(header::LOCATION)
                        .and_then(|value| value.to_str().ok())
                    else {
                        last_error = Some(anyhow!("media redirect has no valid Location"));
                        break;
                    };
                    match current.join(location) {
                        Ok(next) => current = next,
                        Err(error) => {
                            last_error = Some(anyhow!(error).context("media redirect is invalid"));
                            break;
                        }
                    }
                }
                Ok(response) if response.status().is_success() => {
                    let status = response.status();
                    let upstream_headers = response.headers().clone();
                    let body = if head_only {
                        Body::empty()
                    } else {
                        let mut stream = response.bytes_stream();
                        Body::from_stream(async_stream::stream! {
                            loop {
                                tokio::select! {
                                    _ = cancellation.cancelled() => break,
                                    item = stream.next() => match item {
                                        Some(Ok(bytes)) => yield Ok::<Bytes, io::Error>(bytes),
                                        Some(Err(error)) => {
                                            yield Err(io::Error::other(error.to_string()));
                                            break;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        })
                    };
                    let mut output =
                        Response::builder().status(if head_only { StatusCode::OK } else { status });
                    let copied_headers = if head_only {
                        &[
                            header::CONTENT_TYPE,
                            header::ACCEPT_RANGES,
                            header::CACHE_CONTROL,
                            header::ETAG,
                            header::LAST_MODIFIED,
                        ][..]
                    } else {
                        &[
                            header::CONTENT_TYPE,
                            header::CONTENT_LENGTH,
                            header::CONTENT_RANGE,
                            header::ACCEPT_RANGES,
                            header::CACHE_CONTROL,
                            header::ETAG,
                            header::LAST_MODIFIED,
                        ][..]
                    };
                    for name in copied_headers {
                        if let Some(value) = upstream_headers.get(name) {
                            output = output.header(name, value);
                        }
                    }
                    if head_only
                        && let Some(total) = content_range_total(&upstream_headers)
                            .or_else(|| upstream_headers.get(header::CONTENT_LENGTH).cloned())
                    {
                        output = output.header(header::CONTENT_LENGTH, total);
                    }
                    let output = output
                        .header("transferMode.dlna.org", "Streaming")
                        .body(body)
                        .map_err(anyhow::Error::from)?;
                    return Ok((output, current));
                }
                Ok(response) => {
                    last_error = Some(anyhow!("upstream returned {}", response.status()));
                    break;
                }
                Err(error) => {
                    last_error = Some(anyhow!(error.without_url()));
                    break;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("no upstream URL is available")))
}

async fn pinned_media_client(url: &Url) -> Result<Client> {
    validate_outbound_media_url(url.as_str())?;
    let host = url.host_str().context("media URL has no host")?;
    let port = url
        .port_or_known_default()
        .context("media URL has no known port")?;
    let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        time::timeout(std::time::Duration::from_secs(5), lookup_host((host, port)))
            .await
            .context("media DNS lookup timed out")??
            .collect::<Vec<_>>()
    };
    if addresses.is_empty()
        || addresses.len() > 32
        || addresses.iter().any(|address| !is_public_ip(address.ip()))
    {
        return Err(anyhow!(
            "media host did not resolve exclusively to public addresses"
        ));
    }
    let mut builder = Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(5))
        .read_timeout(std::time::Duration::from_secs(30))
        .redirect(Policy::none())
        .user_agent(BILI_USER_AGENT);
    if host.parse::<IpAddr>().is_err() {
        builder = builder.resolve_to_addrs(host, &addresses);
    }
    builder.build().context("cannot create pinned media client")
}

fn content_range_total(headers: &HeaderMap) -> Option<HeaderValue> {
    let range = headers.get(header::CONTENT_RANGE)?.to_str().ok()?;
    let total = range.rsplit_once('/')?.1.trim();
    total
        .parse::<u64>()
        .ok()
        .and_then(|_| HeaderValue::from_str(total).ok())
}

#[derive(Debug, Default)]
struct FfmpegProgressParser {
    pending_out_time_us: Option<u64>,
    saw_out_time_us: bool,
}

impl FfmpegProgressParser {
    /// Returns whether this is a machine-readable progress line and, at the end
    /// of a complete stanza, the newest output timestamp in milliseconds.
    fn consume_line(&mut self, line: &[u8]) -> (bool, Option<u64>) {
        let line = line.strip_suffix(b"\n").unwrap_or(line);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(separator) = line.iter().position(|byte| *byte == b'=') else {
            return (false, None);
        };
        let (key, value) = (&line[..separator], &line[separator + 1..]);
        if key == b"out_time_us" {
            self.saw_out_time_us = true;
            self.pending_out_time_us = parse_ffmpeg_timestamp(value);
            return (true, None);
        }
        // Older FFmpeg builds call the same microsecond value `out_time_ms`.
        if key == b"out_time_ms" {
            if !self.saw_out_time_us {
                self.pending_out_time_us = parse_ffmpeg_timestamp(value);
            }
            return (true, None);
        }
        if key == b"progress" {
            let timestamp = if value == b"continue" || value == b"end" {
                self.pending_out_time_us.map(|value| value / 1_000)
            } else {
                None
            };
            self.pending_out_time_us = None;
            self.saw_out_time_us = false;
            return (true, timestamp);
        }
        (is_ffmpeg_progress_key(key), None)
    }
}

fn parse_ffmpeg_timestamp(value: &[u8]) -> Option<u64> {
    std::str::from_utf8(value).ok()?.trim().parse().ok()
}

fn is_ffmpeg_progress_key(key: &[u8]) -> bool {
    key == b"frame"
        || key == b"fps"
        || key == b"bitrate"
        || key == b"total_size"
        || key == b"out_time"
        || key == b"dup_frames"
        || key == b"drop_frames"
        || key == b"speed"
        || key.starts_with(b"stream_")
}

fn retain_ffmpeg_diagnostic(retained: &mut Vec<u8>, line: &[u8]) {
    if line.len() >= MAX_FFMPEG_DIAGNOSTICS_BYTES {
        retained.clear();
        retained.extend_from_slice(&line[line.len() - MAX_FFMPEG_DIAGNOSTICS_BYTES..]);
        return;
    }
    retained.extend_from_slice(line);
    if retained.len() > MAX_FFMPEG_DIAGNOSTICS_BYTES {
        let excess = retained.len() - MAX_FFMPEG_DIAGNOSTICS_BYTES;
        retained.drain(..excess);
    }
}

fn append_ffmpeg_input_seek(command: &mut Command, start_offset_ms: u64) {
    if let Some(offset) = ffmpeg_input_seek(start_offset_ms) {
        command.arg("-ss").arg(offset);
    }
}

fn ffmpeg_input_seek(start_offset_ms: u64) -> Option<String> {
    (start_offset_ms > 0)
        .then(|| format!("{}.{:03}", start_offset_ms / 1_000, start_offset_ms % 1_000))
}

struct FfmpegLease {
    _permit: OwnedSemaphorePermit,
    active_consumers: usize,
}

impl FfmpegLease {
    async fn acquire(entry: &MediaEntry) -> Option<Self> {
        if let Some(lease) = Self::try_acquire(entry) {
            return Some(lease);
        }
        let consumers = entry.ffmpeg_consumers.clone();
        let wait = time::timeout(FFMPEG_CONSUMER_WAIT, consumers.clone().acquire_owned());
        let permit = tokio::select! {
            biased;
            _ = entry.cancellation.cancelled() => return None,
            result = wait => result.ok()?.ok()?,
        };
        if entry.cancellation.is_cancelled() || consumers.is_closed() {
            return None;
        }
        Some(Self::from_permit(consumers, permit))
    }

    fn try_acquire(entry: &MediaEntry) -> Option<Self> {
        if entry.cancellation.is_cancelled() || entry.ffmpeg_consumers.is_closed() {
            return None;
        }
        let consumers = entry.ffmpeg_consumers.clone();
        let permit = consumers.clone().try_acquire_owned().ok()?;
        if entry.cancellation.is_cancelled() || consumers.is_closed() {
            return None;
        }
        Some(Self::from_permit(consumers, permit))
    }

    fn from_permit(consumers: Arc<tokio::sync::Semaphore>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            _permit: permit,
            active_consumers: MAX_FFMPEG_CONSUMERS - consumers.available_permits(),
        }
    }
}

fn ffmpeg_stream(state: &AppState, entry: &MediaEntry, lease: FfmpegLease) -> Result<Body> {
    let token = &entry.token;
    let video = format!(
        "http://{}:{}/upstream/{token}/video",
        state.internal_http_ip(),
        state.web_port()
    );
    let audio = format!(
        "http://{}:{}/upstream/{token}/audio",
        state.internal_http_ip(),
        state.web_port()
    );
    let source = format!(
        "http://{}:{}/upstream/{token}/source",
        state.internal_http_ip(),
        state.web_port()
    );
    let mut command = Command::new(state.ffmpeg());
    command
        .arg("-nostdin")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-nostats")
        .arg("-stats_period")
        .arg("1")
        .arg("-progress")
        .arg("pipe:2");
    match &entry.input {
        MediaInput::Dash { .. } => {
            command
                .arg("-rw_timeout")
                .arg("30000000")
                .arg("-fflags")
                .arg("+genpts");
            append_ffmpeg_input_seek(&mut command, entry.start_offset_ms);
            command
                .arg("-i")
                .arg(video)
                .arg("-rw_timeout")
                .arg("30000000")
                .arg("-fflags")
                .arg("+genpts");
            append_ffmpeg_input_seek(&mut command, entry.start_offset_ms);
            command
                .arg("-i")
                .arg(audio)
                .arg("-map")
                .arg("0:v:0")
                .arg("-map")
                .arg("1:a:0");
        }
        MediaInput::Remux { format, .. } => {
            match format {
                RemuxFormat::Flv => command.arg("-f").arg("flv"),
                RemuxFormat::Hls => command.arg("-f").arg("hls"),
            };
            command
                .arg("-rw_timeout")
                .arg("30000000")
                .arg("-fflags")
                .arg("+genpts");
            append_ffmpeg_input_seek(&mut command, entry.start_offset_ms);
            command
                .arg("-i")
                .arg(source)
                .arg("-map")
                .arg("0:v:0?")
                .arg("-map")
                .arg("0:a:0?");
        }
        MediaInput::Progressive { .. } => unreachable!("direct media is not remuxed"),
    }
    command
        .arg("-c")
        .arg("copy")
        .arg("-shortest")
        .arg("-avoid_negative_ts")
        .arg("make_zero")
        .arg("-mpegts_flags")
        .arg("+resend_headers")
        .arg("-muxdelay")
        .arg("0")
        .arg("-muxpreload")
        .arg("0")
        .arg("-f")
        .arg("mpegts")
        .arg("pipe:1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.as_std_mut().creation_flags(0x0800_0000);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("cannot launch {}", state.ffmpeg().display()))?;
    let mut stdout = child
        .stdout
        .take()
        .context("FFmpeg stdout is unavailable")?;
    let stderr = child
        .stderr
        .take()
        .context("FFmpeg stderr is unavailable")?;
    let token_for_log = token.chars().take(8).collect::<String>();
    let cancellation = entry.cancellation.clone();
    let remux_progress = entry.remux_progress.clone();
    let progress_generation = remux_progress.begin_generation();
    let stderr_task = tokio::spawn(async move {
        let mut stderr = BufReader::new(stderr);
        let mut line = Vec::new();
        let mut retained = Vec::new();
        let mut parser = FfmpegProgressParser::default();
        loop {
            line.clear();
            match stderr.read_until(b'\n', &mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let (is_progress, out_time_ms) =
                        FfmpegProgressParser::consume_line(&mut parser, &line);
                    if let Some(out_time_ms) = out_time_ms {
                        remux_progress.update(progress_generation, out_time_ms);
                    }
                    if !is_progress {
                        retain_ffmpeg_diagnostic(&mut retained, &line);
                    }
                }
                Err(_) => break,
            }
        }
        retained
    });
    let stream = async_stream::stream! {
        let _lease = lease;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    let diagnostics = stderr_task.await.unwrap_or_default();
                    if !diagnostics.is_empty() {
                        debug!(
                            media_token = token_for_log,
                            stderr = %String::from_utf8_lossy(&diagnostics),
                            "cancelled FFmpeg merger diagnostics"
                        );
                    }
                    return;
                }
                read = stdout.read(&mut buffer) => match read {
                    Ok(0) => break,
                    Ok(length) => {
                        yield Ok::<Bytes, io::Error>(Bytes::copy_from_slice(&buffer[..length]));
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        }
        let status = child.wait().await;
        let diagnostics = stderr_task.await.unwrap_or_default();
        match status {
            Ok(status) if status.success() => {
                if !diagnostics.is_empty() {
                    debug!(
                        media_token = token_for_log,
                        stderr = %String::from_utf8_lossy(&diagnostics),
                        "FFmpeg merger diagnostics"
                    );
                }
            }
            Ok(status) => {
                warn!(
                    media_token = token_for_log,
                    %status,
                    stderr = %String::from_utf8_lossy(&diagnostics),
                    "FFmpeg merger exited unsuccessfully"
                );
                yield Err(io::Error::other(format!("FFmpeg exited with {status}")));
            }
            Err(error) => {
                warn!(
                    media_token = token_for_log,
                    %error,
                    stderr = %String::from_utf8_lossy(&diagnostics),
                    "cannot wait for FFmpeg merger"
                );
                yield Err(error);
            }
        }
    };
    Ok(Body::from_stream(stream))
}

fn dash_headers(entry: &MediaEntry, body: Body) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, entry.mime.as_str())
        .header(header::ACCEPT_RANGES, "none")
        .header("transferMode.dlna.org", "Streaming")
        .header(
            "contentFeatures.dlna.org",
            "DLNA.ORG_OP=00;DLNA.ORG_CI=1;DLNA.ORG_FLAGS=01700000000000000000000000000000",
        )
        .body(body)
        .unwrap()
}

fn same_ip(peer: IpAddr, expected: &str) -> bool {
    expected
        .parse::<IpAddr>()
        .is_ok_and(|expected| expected == peer)
}

fn simple(status: StatusCode, message: &str) -> Response<Body> {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )],
        message.to_owned(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renderer_ip_check_is_exact() {
        assert!(same_ip("10.0.0.2".parse().unwrap(), "10.0.0.2"));
        assert!(!same_ip("10.0.0.3".parse().unwrap(), "10.0.0.2"));
    }

    #[test]
    fn rewrites_every_hls_uri_attribute() {
        let line = "#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",X=1,URI=\"backup.key\"";
        let rewritten = rewrite_hls_uri_attributes(line, |uri| Ok(format!("proxy/{uri}")))
            .expect("valid HLS tag");
        assert_eq!(
            rewritten,
            "#EXT-X-KEY:METHOD=AES-128,URI=\"proxy/key.bin\",X=1,URI=\"proxy/backup.key\""
        );
    }

    #[test]
    fn rewrites_unquoted_case_insensitive_hls_uri_attributes() {
        let line =
            "#EXT-X-MEDIA:TYPE=AUDIO, uri = https://cdn.example/audio.m3u8 ,NAME=\"main, audio\"";
        let rewritten = rewrite_hls_uri_attributes(line, |uri| Ok(format!("proxy/{uri}")))
            .expect("valid HLS tag");
        assert_eq!(
            rewritten,
            "#EXT-X-MEDIA:TYPE=AUDIO, uri = proxy/https://cdn.example/audio.m3u8 ,NAME=\"main, audio\""
        );
    }

    #[test]
    fn extracts_full_length_from_range_probe() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-0/123456"),
        );
        assert_eq!(
            content_range_total(&headers).and_then(|value| value.to_str().ok().map(str::to_owned)),
            Some("123456".into())
        );
    }

    #[test]
    fn detects_hls_after_bom_and_whitespace() {
        assert_eq!(
            hls_prefix_status(b"\xef\xbb\xbf \r\n#EXTM3U\n", true),
            Some(true)
        );
        assert_eq!(hls_prefix_status(b"#EXT", false), None);
        assert_eq!(hls_prefix_status(b"\x47\x40\x00\x10", false), Some(false));
    }

    #[test]
    fn playlist_requests_do_not_forward_ranges() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-0"));
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/test"));
        let sanitized = headers_without_range(&headers);
        assert!(!sanitized.contains_key(header::RANGE));
        assert_eq!(sanitized.get(header::ACCEPT), headers.get(header::ACCEPT));
    }

    #[test]
    fn hls_lru_preserves_hot_and_current_playlist_resources() {
        let mut resources = HlsResourceStore::default();
        for resource in ["old-a", "old-b", "playlist"] {
            resources.urls.insert(
                resource.into(),
                format!("https://cdn.example.net/{resource}"),
            );
            resources.order.push_back(resource.into());
        }
        assert!(touch_hls_resource(&mut resources, "playlist").is_some());

        let mut batch = HlsRewriteBatch::default();
        for resource in ["new-a", "new-b"] {
            batch.order.push(resource.into());
            batch.urls.insert(
                resource.into(),
                format!("https://cdn.example.net/{resource}"),
            );
        }
        commit_hls_resource_batch(&mut resources, batch, Some("playlist"), 4)
            .expect("bounded batch should fit");

        assert_eq!(resources.urls.len(), 4);
        assert!(!resources.urls.contains_key("old-a"));
        assert!(resources.urls.contains_key("old-b"));
        assert!(resources.urls.contains_key("new-a"));
        assert!(resources.urls.contains_key("new-b"));
        assert!(resources.urls.contains_key("playlist"));
        assert_eq!(resources.order.back().map(String::as_str), Some("playlist"));
    }

    #[test]
    fn hls_batch_over_capacity_does_not_mutate_store() {
        let mut resources = HlsResourceStore::default();
        resources
            .urls
            .insert("playlist".into(), "https://cdn.example.net/master".into());
        resources.order.push_back("playlist".into());
        let original_urls = resources.urls.clone();
        let original_order = resources.order.clone();

        let mut batch = HlsRewriteBatch::default();
        for resource in ["one", "two"] {
            batch.order.push(resource.into());
            batch.urls.insert(
                resource.into(),
                format!("https://cdn.example.net/{resource}"),
            );
        }
        assert!(commit_hls_resource_batch(&mut resources, batch, Some("playlist"), 2).is_err());
        assert_eq!(resources.urls, original_urls);
        assert_eq!(resources.order, original_order);
    }

    #[tokio::test]
    async fn rewrites_bom_prefixed_hls_header_as_a_tag() {
        let config = crate::config::RuntimeConfig {
            web_listen: std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 8080),
            nva_listen: std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 9959),
            lelink_listen: std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 52288),
            advertise_ip: std::net::Ipv4Addr::new(192, 0, 2, 10),
            config_path: std::path::PathBuf::from("unused-config.json"),
            web_dir: std::path::PathBuf::from("web/dist"),
            ffmpeg: std::path::PathBuf::from("ffmpeg"),
            nva_name: "UniNVA".into(),
            dlna_name: "UniDLNA".into(),
            lelink_name: "UniLE".into(),
            device_uuid: uuid::Uuid::nil(),
            nva_device_uuid: uuid::Uuid::nil(),
            retired_nva_device_uuid: None,
            selected_udn: None,
            scan_interface_ids: Vec::new(),
        };
        let state = AppState::new(&config).expect("test state");
        let entry = MediaEntry {
            token: "token".into(),
            owner_session: "session".into(),
            input: MediaInput::Progressive {
                url: "https://cdn.example.net/video.mp4".into(),
            },
            mime: "video/mp4".into(),
            duration_ms: None,
            start_offset_ms: 0,
            created_unix_ms: 0,
            allowed_renderer_ip: "192.0.2.20".into(),
            gateway_address: "192.0.2.10".into(),
            cancellation: CancellationToken::new(),
            ffmpeg_consumers: Arc::new(tokio::sync::Semaphore::new(MAX_FFMPEG_CONSUMERS)),
            remux_progress: Arc::new(crate::state::RemuxProgress::default()),
            playback_clock: Arc::new(crate::state::PlaybackClock::default()),
            hls_resources: Arc::new(tokio::sync::RwLock::new(HlsResourceStore::default())),
        };
        let rewritten = rewrite_hls_playlist(
            &state,
            &entry,
            &Url::parse("https://cdn.example.net/master").unwrap(),
            "\u{feff} \t\r\n#EXTM3U\r\nsegment.ts\r\n",
            None,
        )
        .await
        .expect("valid playlist");
        assert!(rewritten.starts_with("#EXTM3U\nhttp://192.0.2.10:8080/upstream/token/hls/"));
        assert_eq!(entry.hls_resources.read().await.urls.len(), 1);
    }

    fn test_dash_entry() -> MediaEntry {
        MediaEntry {
            token: "token".into(),
            owner_session: "session".into(),
            input: MediaInput::Dash {
                video_url: "https://cdn.example.net/video.m4s".into(),
                video_backup_urls: Vec::new(),
                audio_url: "https://cdn.example.net/audio.m4s".into(),
                audio_backup_urls: Vec::new(),
            },
            mime: "video/mp2t".into(),
            duration_ms: None,
            start_offset_ms: 0,
            created_unix_ms: 0,
            allowed_renderer_ip: "192.0.2.20".into(),
            gateway_address: "192.0.2.10".into(),
            cancellation: CancellationToken::new(),
            ffmpeg_consumers: Arc::new(tokio::sync::Semaphore::new(MAX_FFMPEG_CONSUMERS)),
            remux_progress: Arc::new(crate::state::RemuxProgress::default()),
            playback_clock: Arc::new(crate::state::PlaybackClock::default()),
            hls_resources: Arc::new(tokio::sync::RwLock::new(HlsResourceStore::default())),
        }
    }

    #[test]
    fn parses_complete_ffmpeg_progress_stanzas_in_milliseconds() {
        let mut parser = FfmpegProgressParser::default();
        assert_eq!(parser.consume_line(b"frame=42\r\n"), (true, None));
        assert_eq!(
            parser.consume_line(b"out_time_us=1234567\r\n"),
            (true, None)
        );
        assert_eq!(
            parser.consume_line(b"progress=continue\r\n"),
            (true, Some(1_234))
        );
        assert_eq!(
            parser.consume_line(b"[http @ 0001] HTTP error 403 Forbidden\r\n"),
            (false, None)
        );
    }

    #[test]
    fn ffmpeg_progress_prefers_out_time_us_and_drops_invalid_stanzas() {
        let mut parser = FfmpegProgressParser::default();
        assert_eq!(parser.consume_line(b"out_time_ms=9000\n"), (true, None));
        assert_eq!(parser.consume_line(b"out_time_us=12000\n"), (true, None));
        assert_eq!(
            parser.consume_line(b"progress=continue\n"),
            (true, Some(12))
        );

        assert_eq!(parser.consume_line(b"out_time_us=-1\n"), (true, None));
        assert_eq!(parser.consume_line(b"progress=continue\n"), (true, None));
        assert_eq!(parser.consume_line(b"progress=unknown\n"), (true, None));
    }

    #[test]
    fn ffmpeg_input_seek_preserves_the_millisecond_offset() {
        assert_eq!(ffmpeg_input_seek(0), None);
        assert_eq!(ffmpeg_input_seek(42_999).as_deref(), Some("42.999"));
        assert_eq!(ffmpeg_input_seek(3_600_001).as_deref(), Some("3600.001"));
    }

    #[test]
    fn permits_two_bounded_ffmpeg_consumers() {
        let entry = test_dash_entry();
        let first = FfmpegLease::try_acquire(&entry).expect("first consumer");
        assert_eq!(first.active_consumers, 1);
        let second = FfmpegLease::try_acquire(&entry).expect("second consumer");
        assert_eq!(second.active_consumers, 2);
        assert!(FfmpegLease::try_acquire(&entry).is_none());

        drop(first);
        assert!(FfmpegLease::try_acquire(&entry).is_some());
    }

    #[test]
    fn remux_headers_explicitly_disable_byte_ranges() {
        let entry = test_dash_entry();
        let response = dash_headers(&entry, Body::empty());
        assert_eq!(
            response.headers().get(header::ACCEPT_RANGES),
            Some(&HeaderValue::from_static("none"))
        );
    }

    #[tokio::test]
    async fn waiting_ffmpeg_consumer_acquires_a_released_slot() {
        let entry = test_dash_entry();
        let first = FfmpegLease::try_acquire(&entry).expect("first consumer");
        let _second = FfmpegLease::try_acquire(&entry).expect("second consumer");
        let waiting_entry = entry.clone();
        let waiter =
            tokio::spawn(async move { FfmpegLease::acquire(&waiting_entry).await.is_some() });
        time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        drop(first);
        let acquired = time::timeout(std::time::Duration::from_millis(250), waiter)
            .await
            .expect("waiter should wake")
            .expect("waiter task should finish");
        assert!(acquired);
    }

    #[tokio::test]
    async fn waiting_ffmpeg_consumer_stops_when_media_is_cancelled() {
        let entry = test_dash_entry();
        let _first = FfmpegLease::try_acquire(&entry).expect("first consumer");
        let _second = FfmpegLease::try_acquire(&entry).expect("second consumer");
        let waiting_entry = entry.clone();
        let waiter =
            tokio::spawn(async move { FfmpegLease::acquire(&waiting_entry).await.is_some() });
        time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        entry.ffmpeg_consumers.close();
        entry.cancellation.cancel();
        let acquired = time::timeout(std::time::Duration::from_millis(250), waiter)
            .await
            .expect("cancelled waiter should wake")
            .expect("waiter task should finish");
        assert!(!acquired);
    }
}
