//! Giphy bridge via the [Giphy API](https://developers.giphy.com/docs/api).
//!
//! - Search: `GET <base>/v1/gifs/search?api_key=<KEY>&q=<q>&limit=N`
//! - Fetch one: `GET <base>/v1/gifs/<id>?api_key=<KEY>`
//! - File download: the absolute URL from `images.original.url`.
//!
//! **API key required.** Missing/empty soft-skips the plugin at startup with a
//! clear `ConfigError::MissingConfig`.
//!
//! Ported from the gateway crate's `plugins/giphy.rs`. Difference from the
//! in-gateway version: a feeder can't reach the core bitswap blockstore, so
//! instead of fetching+seeding each result's preview and emitting a `preview`
//! CID, `handle_query` emits the raw upstream `preview_url`; the gateway core
//! fetches + seeds + rewrites it to a `preview` cid (plan §2c).

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use tracing::warn;

use meta_feeder_sdk::cache::MidhashCache;
use meta_feeder_sdk::common;
use meta_feeder_sdk::plugin::{upstream_id_field, ConfigError, FeederPlugin, GatewayQuery, HashOutcome};
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, PluginHealth};

/// Public Giphy API root.
const DEFAULT_BASE_URL: &str = "https://api.giphy.com";

/// HTTP timeout per upstream call.
const HTTP_TIMEOUT_SECS: u64 = 30;

/// Polite identification per Giphy's API guidelines.
const USER_AGENT: &str = concat!("meta-share/", env!("CARGO_PKG_VERSION"), " (gateway:giphy)");

/// Giphy gateway plugin. Holds the api_key after `configure()`.
pub struct GiphyPlugin {
    http: reqwest::Client,
    base_url: String,
    /// Operator-supplied Giphy API key. `None` before `configure()`.
    api_key: Option<String>,
    cache: Option<MidhashCache>,
}

impl Default for GiphyPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl GiphyPlugin {
    pub fn new() -> Self {
        Self::with_base_url(DEFAULT_BASE_URL.to_string())
    }

    /// Construct against a non-default base URL. The api_key is left `None` so
    /// `configure()` / `set_api_key` must supply it.
    pub fn with_base_url(base_url: String) -> Self {
        let http = common::build_http_client(HTTP_TIMEOUT_SECS, USER_AGENT, None);
        Self {
            http,
            base_url,
            api_key: None,
            cache: None,
        }
    }

    /// Set the api_key (from the feeder's env/config, or tests). Absent →
    /// `configure()` soft-skips the plugin.
    pub fn set_api_key(&mut self, key: String) {
        self.api_key = Some(key);
    }

    /// Test-only constructor: primes the api_key + base URL together.
    #[cfg(test)]
    pub fn with_api_key_and_base(api_key: String, base_url: String) -> Self {
        let mut p = Self::with_base_url(base_url);
        p.set_api_key(api_key);
        p
    }

    fn cache(&self) -> Result<&MidhashCache, GatewayError> {
        common::require_cache(self.cache.as_ref(), "giphy")
    }

    fn api_key(&self) -> Result<&str, GatewayError> {
        self.api_key.as_deref().ok_or_else(|| {
            GatewayError::Internal(anyhow::anyhow!(
                "giphy plugin api_key missing (configure() never called)"
            ))
        })
    }

    /// Build a Giphy API URL with the api_key baked in.
    fn api_url(&self, path: &str, extra: &str) -> Result<String, GatewayError> {
        let key = self.api_key()?;
        Ok(format!(
            "{}{}?api_key={}{}{}",
            self.base_url.trim_end_matches('/'),
            path,
            common::urlencode(key),
            if extra.is_empty() { "" } else { "&" },
            extra
        ))
    }

    async fn fetch_search(
        &self,
        query: &str,
        max_results: usize,
    ) -> Result<GiphyResponse, GatewayError> {
        let url = self.api_url(
            "/v1/gifs/search",
            &format!("q={}&limit={}", common::urlencode(query), max_results),
        )?;
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("GET {url}: {e}")))?;
        map_status(&resp)?;
        resp.json::<GiphyResponse>()
            .await
            .map_err(|e| GatewayError::Permanent(format!("decode giphy search response: {e}")))
    }

    async fn fetch_one(&self, record_id: &str) -> Result<GiphyGif, GatewayError> {
        let url = self.api_url(&format!("/v1/gifs/{}", common::urlencode(record_id)), "")?;
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("GET {url}: {e}")))?;
        map_status(&resp)?;
        let body: GiphySingleResponse = resp
            .json()
            .await
            .map_err(|e| GatewayError::Permanent(format!("decode giphy detail response: {e}")))?;
        body.data.ok_or(GatewayError::NotFound)
    }

    async fn fetch_gif_bytes(&self, url: &str) -> Result<bytes::Bytes, GatewayError> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("GET {url}: {e}")))?;
        map_status(&resp)?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| GatewayError::Transient(format!("read body {url}: {e}")))?;
        // Sanity check: GIF magic bytes are `GIF87a` or `GIF89a`. Guards
        // against Giphy returning an HTML error page with 200 status.
        if !bytes.starts_with(b"GIF8") {
            return Err(GatewayError::Permanent(format!(
                "giphy returned non-GIF body for {url} (CDN may have replaced the file)"
            )));
        }
        Ok(bytes)
    }
}

#[async_trait]
impl FeederPlugin for GiphyPlugin {
    fn upstream_id(&self) -> &'static str {
        "giphy"
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        if self.api_key.is_none() {
            return Err(ConfigError::MissingConfig {
                plugin: "giphy",
                what: "api_key (set it via the GIPHY_API_KEY env on the common-feeder)",
            });
        }
        self.cache = Some(common::open_midhash_cache(cache_dir, "giphy")?);
        Ok(())
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // Layer A early-return: Giphy only serves `image` / `gif`.
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }
        let q = query.free_text_or_star();
        let body = self.fetch_search(q, max_results).await?;
        // Emit the upstream `preview_gif` URL as a raw `preview_url` field. The
        // gateway core fetches it, seeds the bytes into its bitswap blockstore,
        // and rewrites it to a content-addressed `preview` cid (plan §2c) — a
        // feeder can't reach the core blockstore.
        let records: Vec<DiscoveryRecord> = body
            .data
            .into_iter()
            .filter_map(|g| {
                let preview_url = g
                    .images
                    .as_ref()
                    .and_then(|i| i.preview_gif.as_ref())
                    .map(|v| v.url.clone())
                    .filter(|s| !s.is_empty());
                let mut rec = into_discovery_record(g)?;
                if let Some(u) = preview_url {
                    rec.fields.insert("preview_url".to_string(), u);
                }
                Some(rec)
            })
            .take(max_results)
            .collect();
        Ok(records)
    }

    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let cache = self.cache()?;
        if let Some(hit) = common::cached_outcome(cache, record_id, "giphy")? {
            return Ok(hit);
        }
        let gif = self.fetch_one(record_id).await?;
        let url = gif
            .images
            .as_ref()
            .and_then(|m| m.original.as_ref())
            .map(|o| o.url.clone())
            .ok_or_else(|| {
                GatewayError::Permanent(format!(
                    "giphy record `{record_id}` has no images.original.url"
                ))
            })?;
        let bytes = self.fetch_gif_bytes(&url).await?;
        let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);

        common::store_midhash(cache, record_id, "giphy", &cid);

        let record = into_discovery_record(gif).ok_or_else(|| {
            GatewayError::Permanent(format!(
                "giphy record `{record_id}` lost fields between calls"
            ))
        })?;
        Ok(common::single_outcome(cid, bytes, record, Some("gif".to_string())))
    }

    fn health(&self) -> PluginHealth {
        match (self.cache.is_some(), self.api_key.is_some()) {
            (true, true) => PluginHealth::Ok,
            (false, _) => PluginHealth::Degraded {
                reason: "configure() not yet called".to_string(),
            },
            (true, false) => PluginHealth::Degraded {
                reason: "no api key — set GIPHY_API_KEY".to_string(),
            },
        }
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        &["image"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["gif"]
    }
}

/// Map an HTTP status to a `GatewayError`. Auth-shaped failures are permanent.
fn map_status(resp: &reqwest::Response) -> Result<(), GatewayError> {
    let status = resp.status();
    let code = status.as_u16();
    if code == 401 || code == 403 {
        warn!(target: "meta-share::gateway", upstream = "giphy", %status, "giphy auth failure");
        return Err(GatewayError::Permanent(format!(
            "giphy auth failed ({status}) — check the GIPHY_API_KEY"
        )));
    }
    common::map_status(resp)
}

// -- Giphy JSON shapes (subset we use) --------------------------------------

#[derive(Debug, Default, Deserialize)]
struct GiphyResponse {
    #[serde(default)]
    data: Vec<GiphyGif>,
}

#[derive(Debug, Default, Deserialize)]
struct GiphySingleResponse {
    data: Option<GiphyGif>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GiphyGif {
    id: Option<String>,
    title: Option<String>,
    url: Option<String>,
    username: Option<String>,
    rating: Option<String>,
    import_datetime: Option<String>,
    images: Option<GiphyImages>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GiphyImages {
    original: Option<GiphyImageVariant>,
    /// Small preview rendition surfaced as the `preview_url` field.
    preview_gif: Option<GiphyImageVariant>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GiphyImageVariant {
    url: String,
    width: Option<String>,
    height: Option<String>,
    size: Option<String>,
}

/// Convert a `GiphyGif` to a `DiscoveryRecord`. Returns `None` when the record
/// is missing an `id`.
fn into_discovery_record(gif: GiphyGif) -> Option<DiscoveryRecord> {
    let id = gif.id?;
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    if let Some(t) = gif.title.filter(|s| !s.is_empty()) {
        fields.insert("title".to_string(), t);
    } else {
        fields.insert("title".to_string(), format!("giphy/{id}"));
    }
    fields.insert("fileType".to_string(), "image".to_string());
    fields.insert("contentKind".to_string(), "gif".to_string());
    fields.insert(
        "sourceUrl".to_string(),
        gif.url.unwrap_or_else(|| format!("https://giphy.com/gifs/{id}")),
    );
    fields.insert("fileName".to_string(), format!("giphy-{id}.gif"));
    fields.insert(upstream_id_field("giphy"), id.clone());
    fields.insert("format".to_string(), "gif".to_string());
    fields.insert("mime".to_string(), "image/gif".to_string());
    if let Some(u) = gif.username.filter(|s| !s.is_empty()) {
        fields.insert("uploader".to_string(), u);
    }
    if let Some(r) = gif.rating.filter(|s| !s.is_empty()) {
        fields.insert("rating".to_string(), r);
    }
    if let Some(dt) = gif.import_datetime.filter(|s| !s.is_empty()) {
        fields.insert("imported".to_string(), dt.clone());
        if dt.len() >= 4 && dt[..4].chars().all(|c| c.is_ascii_digit()) {
            fields.insert("year".to_string(), dt[..4].to_string());
        }
    }
    if let Some(orig) = gif.images.and_then(|i| i.original) {
        if let Some(s) = orig.size.filter(|s| !s.is_empty()) {
            fields.insert("size".to_string(), s);
        }
        if let Some(w) = orig.width.filter(|s| !s.is_empty()) {
            fields.insert("width".to_string(), w);
        }
        if let Some(h) = orig.height.filter(|s| !s.is_empty()) {
            fields.insert("height".to_string(), h);
        }
    }
    Some(DiscoveryRecord {
        upstream_id: "giphy".to_string(),
        record_id: id,
        fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_SEARCH: &str = r#"{
      "data": [
        {
          "id": "ABCDEF123",
          "title": "Cat reaction",
          "url": "https://giphy.com/gifs/funny-cat-ABCDEF123",
          "username": "catlover",
          "rating": "g",
          "import_datetime": "2020-04-22 18:00:00",
          "images": {
            "original": {
              "url": "https://media.giphy.com/media/ABCDEF123/giphy.gif",
              "width": "480",
              "height": "270",
              "size": "1234567"
            },
            "preview_gif": {
              "url": "https://media.giphy.com/media/ABCDEF123/giphy-preview.gif"
            }
          }
        },
        {
          "id": "XYZ789",
          "title": "",
          "url": "https://giphy.com/gifs/XYZ789",
          "rating": "pg",
          "images": {
            "original": {
              "url": "https://media.giphy.com/media/XYZ789/giphy.gif",
              "size": "654321"
            }
          }
        }
      ],
      "pagination": { "total_count": 2, "count": 2, "offset": 0 },
      "meta": { "status": 200, "msg": "OK" }
    }"#;

    #[test]
    fn into_discovery_record_emits_required_fields() {
        let body: GiphyResponse = serde_json::from_str(SAMPLE_SEARCH).unwrap();
        let rec = into_discovery_record(body.data.into_iter().next().unwrap()).unwrap();
        assert_eq!(rec.upstream_id, "giphy");
        assert_eq!(rec.record_id, "ABCDEF123");
        assert_eq!(rec.fields.get("fileType").map(String::as_str), Some("image"));
        assert_eq!(rec.fields.get("contentKind").map(String::as_str), Some("gif"));
        assert_eq!(rec.fields.get("format").map(String::as_str), Some("gif"));
        assert_eq!(rec.fields.get("mime").map(String::as_str), Some("image/gif"));
        assert_eq!(
            rec.fields.get("sourceUrl").map(String::as_str),
            Some("https://giphy.com/gifs/funny-cat-ABCDEF123")
        );
        assert_eq!(
            rec.fields.get("fileName").map(String::as_str),
            Some("giphy-ABCDEF123.gif")
        );
        assert_eq!(rec.fields.get("giphyid").map(String::as_str), Some("ABCDEF123"));
        assert_eq!(rec.fields.get("title").map(String::as_str), Some("Cat reaction"));
        assert_eq!(rec.fields.get("uploader").map(String::as_str), Some("catlover"));
        assert_eq!(rec.fields.get("rating").map(String::as_str), Some("g"));
        assert_eq!(rec.fields.get("year").map(String::as_str), Some("2020"));
        assert_eq!(rec.fields.get("size").map(String::as_str), Some("1234567"));
    }

    #[test]
    fn empty_title_falls_back_to_id() {
        let body: GiphyResponse = serde_json::from_str(SAMPLE_SEARCH).unwrap();
        let rec = into_discovery_record(body.data.into_iter().nth(1).unwrap()).unwrap();
        assert_eq!(rec.record_id, "XYZ789");
        assert_eq!(rec.fields.get("title").map(String::as_str), Some("giphy/XYZ789"));
    }

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn configured_plugin_against(server: &MockServer) -> (GiphyPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin = GiphyPlugin::with_api_key_and_base("test-key".to_string(), server.uri());
        plugin.configure(dir.path()).expect("configure");
        (plugin, dir)
    }

    #[tokio::test]
    async fn handle_query_maps_search_to_discovery_records() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/gifs/search"))
            .and(query_param("api_key", "test-key"))
            .and(query_param("q", "cat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(SAMPLE_SEARCH)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("cat"), 10)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record_id, "ABCDEF123");
        // The preview rides as a raw URL for the core to seed.
        assert_eq!(
            records[0].fields.get("preview_url").map(String::as_str),
            Some("https://media.giphy.com/media/ABCDEF123/giphy-preview.gif")
        );
    }

    #[tokio::test]
    async fn handle_query_truncates_to_max_results() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/gifs/search"))
            .respond_with(ResponseTemplate::new(200).set_body_string(SAMPLE_SEARCH))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("cat"), 1)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn compute_outcomes_fetches_gif_and_caches() {
        let server = MockServer::start().await;
        let gif_bytes = b"GIF89a mock gif body\n".to_vec();
        let expected_cid = meta_feeder_sdk::hash::compute_ipfs_cid(&gif_bytes);
        let single = format!(
            r#"{{
              "data": {{
                "id": "ABCDEF123",
                "title": "Cat reaction",
                "url": "https://giphy.com/gifs/ABCDEF123",
                "images": {{
                  "original": {{
                    "url": "{base}/media/ABCDEF123/giphy.gif",
                    "size": "{sz}"
                  }}
                }}
              }},
              "meta": {{ "status": 200 }}
            }}"#,
            base = server.uri(),
            sz = gif_bytes.len()
        );
        Mock::given(method("GET"))
            .and(path("/v1/gifs/ABCDEF123"))
            .and(query_param("api_key", "test-key"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(single)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/media/ABCDEF123/giphy.gif"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(gif_bytes.clone())
                    .insert_header("content-type", "image/gif"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let outcome = plugin
            .compute_outcomes("ABCDEF123")
            .await
            .expect("compute")
            .into_iter()
            .next()
            .expect("single outcome");
        assert_eq!(outcome.hash.as_str(), expected_cid);
        assert_eq!(outcome.bytes.as_deref(), Some(gif_bytes.as_slice()));
        assert_eq!(outcome.file_extension.as_deref(), Some("gif"));
        let rec = outcome.record.expect("record present on fresh compute");
        assert_eq!(rec.record_id, "ABCDEF123");
    }

    #[tokio::test]
    async fn compute_outcomes_cache_hit_skips_http() {
        let server = MockServer::start().await;
        let (plugin, _dir) = configured_plugin_against(&server);
        plugin
            .cache
            .as_ref()
            .unwrap()
            .put_midhash("ABCDEF123", "bafyCACHED")
            .unwrap();
        let outcome = plugin
            .compute_outcomes("ABCDEF123")
            .await
            .expect("cache hit")
            .into_iter()
            .next()
            .expect("single outcome");
        assert_eq!(outcome.hash.as_str(), "bafyCACHED");
        assert!(outcome.bytes.is_none());
        assert!(outcome.record.is_none());
    }

    #[tokio::test]
    async fn compute_outcomes_rejects_non_gif_body() {
        let server = MockServer::start().await;
        let single = format!(
            r#"{{
              "data": {{
                "id": "ABCDEF123",
                "images": {{
                  "original": {{ "url": "{base}/blocked", "size": "100" }}
                }}
              }}
            }}"#,
            base = server.uri()
        );
        Mock::given(method("GET"))
            .and(path("/v1/gifs/ABCDEF123"))
            .respond_with(ResponseTemplate::new(200).set_body_string(single))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/blocked"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("<html>CDN takedown</html>")
                    .insert_header("content-type", "text/html"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("ABCDEF123")
            .await
            .expect_err("non-GIF body");
        assert!(matches!(err, GatewayError::Permanent(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn upstream_401_maps_to_permanent_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/gifs/search"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .handle_query(&GatewayQuery::from_free_text("anything"), 5)
            .await
            .expect_err("expected auth error");
        match err {
            GatewayError::Permanent(msg) => {
                assert!(msg.contains("auth failed") && msg.contains("GIPHY_API_KEY"), "msg: {msg}")
            }
            other => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn upstream_404_on_detail_maps_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/gifs/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("missing")
            .await
            .expect_err("expected NotFound");
        assert!(matches!(err, GatewayError::NotFound), "got {err:?}");
    }

    #[test]
    fn configure_without_api_key_soft_skips() {
        let dir = tempfile::tempdir().unwrap();
        let mut plugin = GiphyPlugin::new();
        match plugin.configure(dir.path()) {
            Err(ConfigError::MissingConfig { plugin, .. }) => assert_eq!(plugin, "giphy"),
            Err(other) => panic!("wrong error: {other:?}"),
            Ok(()) => panic!("expected MissingConfig"),
        }
    }
}
