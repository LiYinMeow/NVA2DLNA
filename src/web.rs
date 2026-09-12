use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;

use crate::{dlna, media, state::AppState};

const MANAGEMENT_REQUEST_HEADER: &str = "x-nva2dlna-request";

#[derive(Serialize)]
struct StatusPayload {
    phase: String,
    nva_online: bool,
    target_udn: Option<String>,
    target_name: Option<String>,
    media_title: Option<String>,
    error: Option<String>,
}

#[derive(Serialize)]
struct DevicePayload {
    udn: String,
    friendly_name: String,
    location: String,
    model_name: Option<String>,
    online: bool,
}

#[derive(Deserialize)]
struct TargetPayload {
    udn: String,
}

pub async fn run(state: AppState, listen: std::net::SocketAddrV4, web_dir: PathBuf) -> Result<()> {
    let index = web_dir.join("index.html");
    let static_files = ServeDir::new(web_dir).fallback(ServeFile::new(index));
    let app = Router::new()
        .route("/healthz", get(|| async { Json(json!({"ok": true})) }))
        .route("/api/v1/status", get(status))
        .route("/api/v1/devices", get(devices))
        .route("/api/v1/discovery/scan", post(scan))
        .route("/api/v1/target", put(select_target))
        .route("/api/v1/session/stop", post(stop))
        .merge(media::routes())
        .fallback_service(static_files)
        .with_state(state);
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot bind web server to {listen}"))?;
    info!(%listen, "web interface ready");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("web server stopped")
}

async fn status(State(state): State<AppState>) -> Json<StatusPayload> {
    let selected_udn = state.selected_udn().await;
    let selected = match selected_udn.as_deref() {
        Some(udn) => state.renderer(udn).await,
        None => None,
    };
    let session = state.session().await;
    Json(StatusPayload {
        phase: session
            .as_ref()
            .map(|session| session.phase.clone())
            .unwrap_or_else(|| "idle".into()),
        nva_online: true,
        target_udn: selected_udn,
        target_name: selected
            .map(|renderer| renderer.friendly_name)
            .or_else(|| session.as_ref().map(|session| session.target_name.clone())),
        media_title: session.as_ref().map(|session| session.title.clone()),
        error: session.and_then(|session| session.error),
    })
}

async fn devices(State(state): State<AppState>) -> Json<Vec<DevicePayload>> {
    Json(
        state
            .public_renderers()
            .await
            .into_iter()
            .map(|item| DevicePayload {
                udn: item.renderer.udn,
                friendly_name: item.renderer.friendly_name,
                location: item.renderer.location,
                model_name: (!item.renderer.model_name.is_empty())
                    .then_some(item.renderer.model_name),
                online: true,
            })
            .collect(),
    )
}

async fn scan(State(state): State<AppState>, headers: HeaderMap) -> Result<StatusCode, ApiError> {
    require_management_request(&headers)?;
    dlna::scan(state).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn select_target(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<TargetPayload>,
) -> Result<StatusCode, ApiError> {
    require_management_request(&headers)?;
    dlna::select_target(state, payload.udn).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn stop(State(state): State<AppState>, headers: HeaderMap) -> Result<StatusCode, ApiError> {
    require_management_request(&headers)?;
    dlna::stop(state, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn require_management_request(headers: &HeaderMap) -> Result<(), ApiError> {
    if headers
        .get(MANAGEMENT_REQUEST_HEADER)
        .is_some_and(|value| value == "1")
    {
        Ok(())
    } else {
        Err(ApiError(anyhow!("missing management request header")))
    }
}

struct ApiError(anyhow::Error);

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": self.0.to_string()})),
        )
            .into_response()
    }
}
