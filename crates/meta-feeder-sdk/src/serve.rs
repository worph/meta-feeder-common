//! `serve_feeders` — the axum harness that exposes a set of [`FeederPlugin`]s
//! over the **feeder HTTP contract**. Every feeder sidecar binary is just
//! `serve_feeders(vec![MyPlugin::new()], state_dir, listen)`.
//!
//! # Contract (JSON over HTTP, mirrors meta-sort's container-plugin shape)
//!
//! - `GET  /manifest`                  → [`ManifestResponse`]
//! - `GET  /health`                    → [`HealthResponse`]
//! - `POST /query`                     → [`QueryRequest`] → [`QueryResponse`]
//! - `POST /compute`                   → [`ComputeRequest`] → [`ComputeResponse`]
//! - `GET  /fetch/{upstream_id}/{record_id}` → streamed bytes (or 404)
//! - `GET  /blob/{upstream_id}/{cid}`  → bytes (or 404)
//!
//! ## Bytes & the v1 simplification
//!
//! `/compute` returns each outcome's bytes **inline, base64-encoded**
//! ([`OutcomeDto::bytes_b64`]) — this reuses every existing `compute_outcomes`
//! impl verbatim (they already return bytes in `HashOutcome.bytes`) and keeps
//! the gateway core's auto-store a single round-trip. base64 adds ~33%; fine
//! for books/papers/posters. Large-file streaming (torznab multi-GiB) is a
//! documented follow-up — those outcomes are metadata-only anyway
//! (`bytes: None`), so they never ride this path.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::Engine as _;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::config::{self, CONFIG_PAGE_HTML};
use crate::plugin::{ConfigError, FeederPlugin, HashKind, HashOutcome};
use crate::query::{GatewayQuery, GatewaySearchEvent, GatewayWireError};
use crate::types::{DiscoveryRecord, GatewayError, PluginHealth};

// -------- wire DTOs --------------------------------------------------------

/// Serializable mirror of [`HashKind`] for the feeder HTTP boundary.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum HashKindDto {
    Midhash256,
    Sha2_256,
    BtV1File,
}

impl From<HashKind> for HashKindDto {
    fn from(k: HashKind) -> Self {
        match k {
            HashKind::Midhash256 => HashKindDto::Midhash256,
            HashKind::Sha2_256 => HashKindDto::Sha2_256,
            HashKind::BtV1File => HashKindDto::BtV1File,
        }
    }
}

impl From<HashKindDto> for HashKind {
    fn from(k: HashKindDto) -> Self {
        match k {
            HashKindDto::Midhash256 => HashKind::Midhash256,
            HashKindDto::Sha2_256 => HashKind::Sha2_256,
            HashKindDto::BtV1File => HashKind::BtV1File,
        }
    }
}

/// One plugin's capability surface in [`ManifestResponse`].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PluginManifest {
    pub id: String,
    pub served_file_types: Vec<String>,
    pub served_content_kinds: Vec<String>,
}

/// `GET /manifest` — the feeder's hosted plugins + their served types. The
/// gateway core reads this to register `RemoteFeederPlugin`s and to build the
/// capability heartbeat it broadcasts to meta-share.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ManifestResponse {
    pub feeder_version: String,
    pub plugins: Vec<PluginManifest>,
}

/// One plugin's liveness in [`HealthResponse`].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PluginHealthEntry {
    pub id: String,
    pub health: PluginHealth,
}

/// `GET /health` — `status` is `"ok"` iff every hosted plugin reports `Ok`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HealthResponse {
    pub status: String,
    pub plugins: Vec<PluginHealthEntry>,
}

/// `POST /query` request body.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QueryRequest {
    pub upstream_id: String,
    pub query: GatewayQuery,
    pub max_results: u32,
}

/// `POST /query` response body.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QueryResponse {
    pub records: Vec<DiscoveryRecord>,
}

/// `POST /compute` request body.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ComputeRequest {
    pub upstream_id: String,
    pub record_id: String,
}

/// One resolved outcome on the feeder wire. `bytes_b64` carries the full
/// payload base64-encoded when the plugin fetched bytes (`Sha2_256` full
/// store); `None` for metadata-only outcomes (the core's `(None, Some)`
/// branch — e.g. torznab midhash).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OutcomeDto {
    pub hash: String,
    pub hash_kind: HashKindDto,
    pub file_extension: Option<String>,
    pub record: Option<DiscoveryRecord>,
    pub bytes_b64: Option<String>,
}

impl OutcomeDto {
    fn from_outcome(o: HashOutcome) -> Self {
        OutcomeDto {
            hash: o.hash.0,
            hash_kind: o.hash_kind.into(),
            file_extension: o.file_extension,
            record: o.record,
            bytes_b64: o
                .bytes
                .map(|b| base64::engine::general_purpose::STANDARD.encode(&b)),
        }
    }
}

/// `POST /compute` response body.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ComputeResponse {
    pub outcomes: Vec<OutcomeDto>,
}

// -------- app state + error mapping ----------------------------------------

#[derive(Clone)]
struct AppState {
    plugins: Arc<HashMap<String, Arc<dyn FeederPlugin>>>,
    version: String,
    /// Feeder state dir. Per-plugin config persists at
    /// `state_dir/gateway/<id>/config.json` — the same per-plugin dir handed to
    /// `configure()`, so a UI save is read back on the next restart.
    state_dir: PathBuf,
}

impl AppState {
    fn plugin(&self, upstream_id: &str) -> Result<&Arc<dyn FeederPlugin>, Response> {
        self.plugins.get(upstream_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("no feeder plugin for upstream_id '{upstream_id}'"),
            )
                .into_response()
        })
    }

    /// `state_dir/gateway/<id>/config.json` — canonical per-plugin config file.
    fn config_path(&self, upstream_id: &str) -> PathBuf {
        self.state_dir
            .join("gateway")
            .join(upstream_id)
            .join("config.json")
    }

    /// The effective config as JSON: the persisted `config.json` if it exists,
    /// else the plugin's in-memory (env-seeded) config. Used as the redaction
    /// source for reads and the merge base for writes.
    fn effective_config(&self, upstream_id: &str) -> Value {
        if let Ok(bytes) = std::fs::read(self.config_path(upstream_id)) {
            if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                return v;
            }
        }
        self.plugins
            .get(upstream_id)
            .map(|p| p.config_values())
            .unwrap_or_else(|| serde_json::json!({}))
    }
}

/// Map an in-process [`GatewayError`] to an HTTP status + message. Mirrors the
/// status semantics the gateway's `RemoteFeederPlugin` will translate back
/// into a `GatewayError` on the core side.
fn gateway_error_response(e: GatewayError) -> Response {
    let (status, msg) = match e {
        GatewayError::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
        GatewayError::RateLimited { retry_after_s } => (
            StatusCode::TOO_MANY_REQUESTS,
            format!("rate limited; retry in {retry_after_s}s"),
        ),
        GatewayError::Transient(s) => (StatusCode::BAD_GATEWAY, s),
        GatewayError::Permanent(s) => (StatusCode::UNPROCESSABLE_ENTITY, s),
        GatewayError::Internal(s) => (StatusCode::INTERNAL_SERVER_ERROR, s.to_string()),
    };
    (status, msg).into_response()
}

// -------- handlers ---------------------------------------------------------

async fn manifest(State(state): State<AppState>) -> Json<ManifestResponse> {
    let mut plugins: Vec<PluginManifest> = state
        .plugins
        .values()
        .map(|p| PluginManifest {
            id: p.upstream_id().to_string(),
            served_file_types: p.served_file_types().iter().map(|s| s.to_string()).collect(),
            served_content_kinds: p
                .served_content_kinds()
                .iter()
                .map(|s| s.to_string())
                .collect(),
        })
        .collect();
    plugins.sort_by(|a, b| a.id.cmp(&b.id));
    Json(ManifestResponse {
        feeder_version: state.version.clone(),
        plugins,
    })
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let mut entries: Vec<PluginHealthEntry> = state
        .plugins
        .values()
        .map(|p| PluginHealthEntry {
            id: p.upstream_id().to_string(),
            health: p.health(),
        })
        .collect();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    let all_ok = entries
        .iter()
        .all(|e| matches!(e.health, PluginHealth::Ok));
    Json(HealthResponse {
        status: if all_ok { "ok" } else { "degraded" }.to_string(),
        plugins: entries,
    })
}

async fn query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, Response> {
    let plugin = state.plugin(&req.upstream_id)?;
    let records = plugin
        .handle_query(&req.query, req.max_results as usize)
        .await
        .map_err(gateway_error_response)?;
    Ok(Json(QueryResponse { records }))
}

/// `POST /query_stream` — the streaming analog of [`query`]. Emits the plugin's
/// [`FeederPlugin::handle_query_stream`] events as **NDJSON**: one JSON-encoded
/// [`GatewaySearchEvent`] per line (`application/x-ndjson`). The gateway core's
/// `RemoteFeederPlugin` consumes this and re-emits a `BoxStream<GatewaySearchEvent>`
/// into the libp2p streaming worker, preserving incremental enrichment (e.g.
/// torznab's late TMDB / poster / subtitle-availability patches). A plugin that
/// doesn't override `handle_query_stream` still works here — the trait default
/// replays `handle_query` as `Base* + Done`.
async fn query_stream(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Response, Response> {
    let plugin = state.plugin(&req.upstream_id)?;
    // The open can fail (e.g. the plugin can't reach its upstream at all); that
    // maps to an HTTP error status, same as `/query`. Per-event errors instead
    // ride the stream as a terminal `GatewaySearchEvent::Error` line.
    let events = plugin
        .handle_query_stream(&req.query, req.max_results as usize)
        .await
        .map_err(gateway_error_response)?;
    let body = events.map(|ev| {
        // Serialization of GatewaySearchEvent is infallible in practice; on the
        // off chance it fails, terminate with an Error line so the consumer sees
        // a clean terminal failure rather than a truncated stream.
        let mut line = serde_json::to_vec(&ev).unwrap_or_else(|e| {
            serde_json::to_vec(&GatewaySearchEvent::Error(GatewayWireError::Internal(
                format!("event serialize: {e}"),
            )))
            .unwrap_or_default()
        });
        line.push(b'\n');
        Ok::<_, std::convert::Infallible>(line)
    });
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(body),
    )
        .into_response())
}

async fn compute(
    State(state): State<AppState>,
    Json(req): Json<ComputeRequest>,
) -> Result<Json<ComputeResponse>, Response> {
    let plugin = state.plugin(&req.upstream_id)?;
    let outcomes = plugin
        .compute_outcomes(&req.record_id)
        .await
        .map_err(gateway_error_response)?;
    Ok(Json(ComputeResponse {
        outcomes: outcomes.into_iter().map(OutcomeDto::from_outcome).collect(),
    }))
}

async fn fetch(
    State(state): State<AppState>,
    Path((upstream_id, record_id)): Path<(String, String)>,
) -> Result<Response, Response> {
    let plugin = state.plugin(&upstream_id)?;
    match plugin
        .handle_fetch(&record_id)
        .await
        .map_err(gateway_error_response)?
    {
        Some(stream) => {
            // Map the plugin's GatewayError stream into an io::Error stream so
            // axum's Body::from_stream accepts it.
            let body_stream = stream.map(|chunk| {
                chunk.map_err(|e| std::io::Error::other(e.to_string()))
            });
            Ok(Body::from_stream(body_stream).into_response())
        }
        None => Err((StatusCode::NOT_FOUND, "no bytes for record").into_response()),
    }
}

async fn blob(
    State(state): State<AppState>,
    Path((upstream_id, cid)): Path<(String, String)>,
) -> Result<Response, Response> {
    let plugin = state.plugin(&upstream_id)?;
    match plugin.get_blob(&cid).await {
        Some(bytes) => Ok(bytes.into_response()),
        None => Err((StatusCode::NOT_FOUND, "no such blob").into_response()),
    }
}

// -------- config plane -----------------------------------------------------
//
// Generic, schema-driven per-plugin config. The page + the schema/values JSON
// are all served relative to the request path, so this works identically when
// hit directly on the feeder or reverse-proxied through the gateway.

/// `GET /config` — the schema-driven HTML form (static, plugin-agnostic).
async fn config_page() -> Html<&'static str> {
    Html(CONFIG_PAGE_HTML)
}

/// `GET /config/schema` — the single hosted plugin's declared config schema.
/// (A feeder hosts one plugin in practice; if it hosts several, the first by
/// id is used — the gateway proxies per upstream_id anyway.)
async fn config_schema(State(state): State<AppState>) -> Json<config::ConfigSchema> {
    let schema = sole_plugin(&state)
        .map(|p| p.config_schema())
        .unwrap_or_default();
    Json(schema)
}

/// `GET /config/values` — current values with secrets redacted to `<key>_set`.
async fn config_values_get(State(state): State<AppState>) -> Json<Value> {
    match sole_plugin(&state) {
        Some(p) => {
            let schema = p.config_schema();
            let eff = state.effective_config(p.upstream_id());
            Json(config::redact(&eff, &schema))
        }
        None => Json(serde_json::json!({})),
    }
}

/// `PUT /config/values` — merge the submitted values onto the stored config
/// (schema-aware; blank secrets keep their stored value), persist atomically,
/// and signal that a feeder restart is needed to apply them.
async fn config_values_put(
    State(state): State<AppState>,
    Json(incoming): Json<Value>,
) -> Result<Json<Value>, Response> {
    let plugin = sole_plugin(&state)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "feeder hosts no plugin").into_response())?;
    let id = plugin.upstream_id();
    let schema = plugin.config_schema();
    let base = state.effective_config(id);
    let merged = config::merge(&base, &incoming, &schema);

    let path = state.config_path(id);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")).into_response())?;
    }
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(&merged)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("serialize: {e}")).into_response())?;
    std::fs::write(&tmp, body)
        .and_then(|_| std::fs::rename(&tmp, &path))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write config: {e}")).into_response())?;
    info!(target: "meta-feeder", upstream_id = id, path = %path.display(), "feeder config saved (restart to apply)");
    Ok(Json(serde_json::json!({ "saved": true, "restart_required": true })))
}

/// `POST /enrich/callback` — sink for the enrichment plugins' completion
/// callbacks. The feeder drives those plugins (filename-parser, tmdb) during a
/// self-published resolve and reads results back by polling meta-core, so the
/// callback carries no load — this route exists only so the plugins' callback
/// POST gets a 200 instead of logging a connection failure. Body is ignored.
async fn enrich_callback() -> StatusCode {
    StatusCode::OK
}

/// A feeder hosts one plugin in the common case. Pick it (lowest id for
/// determinism if several are present).
fn sole_plugin(state: &AppState) -> Option<&Arc<dyn FeederPlugin>> {
    state
        .plugins
        .values()
        .min_by(|a, b| a.upstream_id().cmp(b.upstream_id()))
}

// -------- entry point ------------------------------------------------------

/// Build the feeder router for an already-configured plugin set. Exposed for
/// in-process tests (drive it without binding a socket).
pub fn router(
    plugins: HashMap<String, Arc<dyn FeederPlugin>>,
    version: String,
    state_dir: impl Into<PathBuf>,
) -> Router {
    let state = AppState {
        plugins: Arc::new(plugins),
        version,
        state_dir: state_dir.into(),
    };
    Router::new()
        .route("/manifest", get(manifest))
        .route("/health", get(health))
        .route("/query", post(query))
        .route("/query_stream", post(query_stream))
        .route("/compute", post(compute))
        .route("/fetch/:upstream_id/:record_id", get(fetch))
        .route("/blob/:upstream_id/:cid", get(blob))
        .route("/config", get(config_page))
        .route("/config/schema", get(config_schema))
        .route("/config/values", get(config_values_get).put(config_values_put))
        .route("/enrich/callback", post(enrich_callback))
        .with_state(state)
}

/// Configure each plugin (cache dir under `state_dir/gateway/<id>`), then serve
/// the feeder HTTP contract on `listen` until shutdown.
///
/// A plugin whose `configure()` returns [`ConfigError::MissingConfig`] is
/// **soft-skipped** (warn + dropped) — mirroring the gateway registry's
/// behaviour, so a misconfigured plugin doesn't take the whole feeder down.
/// Any other `ConfigError` aborts startup.
pub async fn serve_feeders(
    plugins: Vec<Box<dyn FeederPlugin>>,
    state_dir: impl Into<PathBuf>,
    listen: SocketAddr,
) -> anyhow::Result<()> {
    let state_dir = state_dir.into();
    let configured = configure_plugins(plugins, &state_dir)?;
    let version = env!("CARGO_PKG_VERSION").to_string();
    let app = router(configured, version, state_dir.clone());

    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(
        target: "meta-feeder",
        %listen,
        "feeder listening"
    );
    axum::serve(listener, app)
        .await
        .map_err(anyhow::Error::from)
}

/// Run each plugin's `configure()` against its per-plugin cache dir, applying
/// the soft-skip rule, and return the surviving plugins keyed by `upstream_id`.
/// Public so feeder binaries / tests can configure without serving.
pub fn configure_plugins(
    plugins: Vec<Box<dyn FeederPlugin>>,
    state_dir: &FsPath,
) -> anyhow::Result<HashMap<String, Arc<dyn FeederPlugin>>> {
    let mut out: HashMap<String, Arc<dyn FeederPlugin>> = HashMap::new();
    for mut plugin in plugins {
        let id = plugin.upstream_id();
        let cache_dir = state_dir.join("gateway").join(id);
        std::fs::create_dir_all(&cache_dir).map_err(|e| {
            anyhow::anyhow!("create cache dir {} for {id}: {e}", cache_dir.display())
        })?;
        match plugin.configure(&cache_dir) {
            Ok(()) => {
                let id = id.to_string();
                out.insert(id, Arc::from(plugin));
            }
            Err(ConfigError::MissingConfig { plugin: p, what }) => {
                warn!(
                    target: "meta-feeder",
                    upstream_id = p,
                    missing = what,
                    "feeder plugin skipped: required config not supplied; \
                     other plugins continue"
                );
            }
            Err(other) => return Err(anyhow::anyhow!(other)),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DiscoveryRecord;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use std::collections::BTreeMap;

    /// A feeder plugin that overrides `handle_query_stream` to emit a Base, then
    /// an EnrichPatch, then Done — so the NDJSON route is exercised on a real
    /// incremental stream (not just the collect-default).
    struct StubStreaming;

    #[async_trait]
    impl FeederPlugin for StubStreaming {
        fn upstream_id(&self) -> &'static str {
            "stub"
        }
        fn configure(&mut self, _cache_dir: &FsPath) -> Result<(), ConfigError> {
            Ok(())
        }
        async fn handle_query(
            &self,
            _q: &GatewayQuery,
            _n: usize,
        ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
            Ok(vec![])
        }
        async fn handle_query_stream(
            &self,
            _q: &GatewayQuery,
            _n: usize,
        ) -> Result<BoxStream<'static, GatewaySearchEvent>, GatewayError> {
            let rec = DiscoveryRecord {
                upstream_id: "stub".into(),
                record_id: "1".into(),
                fields: BTreeMap::new(),
            };
            let events = vec![
                GatewaySearchEvent::Base(rec),
                GatewaySearchEvent::EnrichPatch {
                    record_id: "1".into(),
                    set: BTreeMap::from([("title".to_string(), "X".to_string())]),
                    remove: vec![],
                },
                GatewaySearchEvent::Done,
            ];
            Ok(Box::pin(futures::stream::iter(events)))
        }
        async fn compute_outcomes(
            &self,
            _record_id: &str,
        ) -> Result<Vec<HashOutcome>, GatewayError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn query_stream_emits_ndjson_events_in_order() {
        let mut plugins: HashMap<String, Arc<dyn FeederPlugin>> = HashMap::new();
        plugins.insert("stub".to_string(), Arc::new(StubStreaming));
        let app = router(plugins, "test".to_string(), std::env::temp_dir());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let body = reqwest::Client::new()
            .post(format!("http://{addr}/query_stream"))
            .json(&serde_json::json!({
                "upstream_id": "stub",
                "query": {"raw_text":"x","free_text":"x","filters":{},"ranges":[],"negations":[]},
                "max_results": 5
            }))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let events: Vec<GatewaySearchEvent> = body
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("each NDJSON line is a GatewaySearchEvent"))
            .collect();

        assert_eq!(events.len(), 3, "body was: {body:?}");
        assert!(matches!(events[0], GatewaySearchEvent::Base(_)));
        assert!(matches!(events[1], GatewaySearchEvent::EnrichPatch { .. }));
        assert!(matches!(events[2], GatewaySearchEvent::Done));
    }
}
