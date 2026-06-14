//! Wikimedia Commons bridge via the [MediaWiki Action API](https://commons.wikimedia.org/w/api.php).
//!
//! - Search: `GET <base>/w/api.php?action=query&generator=search&gsrsearch=<q>&gsrnamespace=6&gsrlimit=N&prop=imageinfo&iiprop=url|mime|size|extmetadata&format=json`
//!   — one round-trip that returns both search hits and per-file `imageinfo`
//!   (direct URL, mime, size, license, artist, description). `srnamespace=6`
//!   pins results to the `File:` namespace so we only get media records.
//! - Fetch one: `GET <base>/w/api.php?action=query&titles=File:<title>&prop=imageinfo&...`
//!   — single-record metadata lookup used by `compute_outcomes`.
//! - File download: the absolute URL from `imageinfo[0].url`
//!   (typically `https://upload.wikimedia.org/wikipedia/commons/<hash>/<file>`).
//!
//! No auth, no API key. Anonymous use is fine for the read paths we use;
//! the API guide asks for a polite `User-Agent` (already supplied).
//!
//! **File-size cap.** Wikimedia Commons hosts files up to several GB
//! (long-form video). The auto-store path buffers the bytes into RAM
//! before writing to local meta-core's WebDAV — gigabytes there would
//! OOM the gateway peer. This plugin refuses anything over
//! [`MAX_FILE_BYTES`] (50 MB) with a permanent error, leaving streaming
//! support for a later feature.
//!
//! See `docs/gateway-feature.md` §4 for the v0 plugin role.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use meta_feeder_sdk::common;
use meta_feeder_sdk::cache::MidhashCache;
use meta_feeder_sdk::plugin::{upstream_id_field, ConfigError, FeederPlugin, GatewayQuery, HashOutcome};
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, PluginHealth};

/// Wikimedia Commons API root. Overridable by tests to point at a
/// `wiremock::MockServer`.
const DEFAULT_BASE_URL: &str = "https://commons.wikimedia.org";

/// HTTP timeout per upstream call. Generous to cover the occasional
/// upload-server cold-cache fetch for larger media files (image/video).
const HTTP_TIMEOUT_SECS: u64 = 60;

/// Polite identification — Wikimedia's API policy requires a meaningful
/// User-Agent so they can rate-limit per client.
const USER_AGENT: &str = concat!(
    "meta-share/",
    env!("CARGO_PKG_VERSION"),
    " (gateway:wikicommons)"
);

/// Hard ceiling on the file size we'll pull through `compute_outcomes`.
/// 50 MB comfortably covers all common-image, most audio, and small
/// video records on Commons; rejects the multi-GB long-form videos
/// that would OOM the gateway peer's buffer-to-RAM auto-store path.
/// Streaming support relaxes this when the auto-store pipeline goes
/// chunked (v0.3 territory).
const MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;

/// Thumbnail width (in pixels) requested via MediaWiki's `iiurlwidth`
/// parameter. 400 keeps the resulting JPEG/PNG in the ~30–80 KB range
/// per image — small enough to fetch + seed per search result without
/// blowing the search-latency budget, large enough that an inline card
/// preview doesn't look pixelated.
const PREVIEW_WIDTH_PX: u32 = 400;

/// Wikimedia Commons gateway plugin. Same structural shape as the other
/// fetch-capable plugins.
pub struct WikicommonsPlugin {
    http: reqwest::Client,
    base_url: String,
    cache: Option<MidhashCache>,
}

impl Default for WikicommonsPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl WikicommonsPlugin {
    pub fn new() -> Self {
        Self::with_base_url(DEFAULT_BASE_URL.to_string())
    }

    /// Override the API root for tests. The mock serves both
    /// `/w/api.php` (search + imageinfo) and the upload paths returned
    /// in the `imageinfo[0].url` field — the test fixture builds those
    /// URLs against `server.uri()` so the same host handles all calls.
    pub fn with_base_url(base_url: String) -> Self {
        let http = common::build_http_client(HTTP_TIMEOUT_SECS, USER_AGENT, None);
        Self {
            http,
            base_url,
            cache: None,
        }
    }

    fn cache(&self) -> Result<&MidhashCache, GatewayError> {
        common::require_cache(self.cache.as_ref(), "wikicommons")
    }

    /// Build the `/w/api.php` URL for one of our two query shapes:
    /// - search: `extra` = `generator=search&gsrsearch=<q>&gsrnamespace=6&gsrlimit=<n>`
    /// - single: `extra` = `titles=File:<title>`
    ///
    /// The common bits (`action=query&prop=imageinfo&iiprop=...&format=json`)
    /// live here so the two callers stay short.
    ///
    /// `iiurlwidth=<PREVIEW_WIDTH_PX>` asks MediaWiki to also include a
    /// thumbnail URL (`thumburl`) in each `imageinfo[0]`. `handle_query`
    /// surfaces that as a raw `preview_url` field; the gateway core fetches,
    /// seeds, and rewrites it to a `preview` cid (plan §2c). Audio/video
    /// records lack a thumbnail (MediaWiki omits `thumburl` for non-image
    /// mimes); those ship without a `preview_url`.
    fn api_url(&self, extra: &str) -> String {
        format!(
            "{}/w/api.php?action=query&prop=imageinfo&iiprop=url%7Cmime%7Csize%7Cextmetadata&iiurlwidth={}&format=json&formatversion=1&{}",
            self.base_url.trim_end_matches('/'),
            PREVIEW_WIDTH_PX,
            extra
        )
    }

    async fn fetch_api(&self, url: &str) -> Result<MediawikiResponse, GatewayError> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| GatewayError::Transient(format!("GET {url}: {e}")))?;
        common::map_status(&resp)?;
        resp.json::<MediawikiResponse>()
            .await
            .map_err(|e| GatewayError::Permanent(format!("decode mediawiki response: {e}")))
    }

    /// Free-text search on the File: namespace via `generator=search`.
    /// Single round-trip returns search hits and their `imageinfo` — no
    /// per-record lookup needed at search time.
    async fn search_files(
        &self,
        query: &str,
        max_results: usize,
    ) -> Result<Vec<MediawikiPage>, GatewayError> {
        let url = self.api_url(&format!(
            "generator=search&gsrsearch={}&gsrnamespace=6&gsrlimit={}",
            common::urlencode(query),
            max_results
        ));
        let body = self.fetch_api(&url).await?;
        Ok(body.into_ordered_pages())
    }

    /// Single-record lookup by canonical title. `record_id` is the title
    /// **without** the `File:` prefix (that's how we record it). We add
    /// the prefix back before the API call.
    async fn fetch_page(&self, record_id: &str) -> Result<MediawikiPage, GatewayError> {
        let url = self.api_url(&format!("titles=File:{}", common::urlencode(record_id)));
        let body = self.fetch_api(&url).await?;
        let pages = body.into_ordered_pages();
        let page = pages.into_iter().next().ok_or(GatewayError::NotFound)?;
        // MediaWiki returns a synthetic `{"missing": ""}` page for
        // unknown titles. Detect by absence of pageid + imageinfo.
        if page.imageinfo.as_deref().unwrap_or(&[]).is_empty() && page.pageid.is_none() {
            return Err(GatewayError::NotFound);
        }
        Ok(page)
    }

    /// Pull the binary file bytes from the absolute URL Wikimedia returned
    /// in `imageinfo[0].url` (full-file fetch for `compute_outcomes`) or
    /// `imageinfo[0].thumburl` (thumbnail fetch for `handle_query` →
    /// preview seed). The URL points at `upload.wikimedia.org` for real
    /// Commons content; the test fixture replaces it with the mock
    /// server's URL, so we just trust whatever the API gave us.
    async fn fetch_file(&self, url: &str) -> Result<bytes::Bytes, GatewayError> {
        common::fetch_bytes(&self.http, url).await
    }
}

#[async_trait]
impl FeederPlugin for WikicommonsPlugin {
    fn upstream_id(&self) -> &'static str {
        "wikicommons"
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        self.cache = Some(common::open_midhash_cache(cache_dir, "wikicommons")?);
        Ok(())
    }

    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        // Layer A early-return: a `fileType:` / `contentKind:` filter
        // wholly outside this plugin's served set can't ever match,
        // so don't burn an upstream API call. Wikicommons serves
        // image/audio/video/document/other and no content kinds, so
        // this only rejects a query like `contentKind:movie` or a
        // bespoke `fileType:archive` (none of which Commons answers).
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }
        // Layer A pushdown: if the user filtered by `fileType`, fold
        // the requested types into MediaWiki's `filetype:` CirrusSearch
        // keyword so the upstream returns the right kind in the first
        // place. Without this, broad-spectrum Commons returns jpgs for
        // a `fileType:video` query and the dispatcher post-filter has
        // to throw most of them away. See `mediawiki_filetype_clause`
        // for the meta-vocabulary → CirrusSearch value mapping.
        let base_q = query.free_text_or_star();
        let composed_q;
        let q = match mediawiki_filetype_clause(query) {
            Some(clause) => {
                composed_q = format!("{base_q} {clause}");
                composed_q.as_str()
            }
            None => base_q,
        };
        let pages = self.search_files(q, max_results).await?;
        // Emit each result's thumbnail URL as a raw `preview_url` field; the
        // gateway core fetches it, seeds the bytes into its bitswap blockstore,
        // and rewrites it to a content-addressed `preview` cid (plan §2c) — a
        // feeder can't reach the core blockstore. Non-image records (audio /
        // video / pdf) carry no thumbnail and ship preview-less.
        let records: Vec<DiscoveryRecord> = pages
            .into_iter()
            .filter_map(|p| {
                let thumb_url = p
                    .imageinfo
                    .as_ref()
                    .and_then(|v| v.first())
                    .and_then(|info| info.thumburl.clone());
                let mut rec = into_discovery_record(p)?;
                if let Some(u) = thumb_url {
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
        if let Some(hit) = common::cached_outcome(cache, record_id, "wikicommons")? {
            return Ok(hit);
        }

        let page = self.fetch_page(record_id).await?;
        let info = page
            .imageinfo
            .as_ref()
            .and_then(|v| v.first())
            .ok_or_else(|| {
                GatewayError::Permanent(format!(
                    "wikicommons record `{record_id}` has no imageinfo"
                ))
            })?;
        // Refuse oversized files before downloading so a multi-GB video
        // doesn't OOM the gateway peer. The 50 MB ceiling covers the
        // overwhelming majority of Commons records (images + small
        // audio/video); streaming support relaxes this later.
        if let Some(size) = info.size {
            if size > MAX_FILE_BYTES {
                return Err(GatewayError::Permanent(format!(
                    "wikicommons record `{record_id}` is {size} bytes — exceeds gateway plugin's \
                     {MAX_FILE_BYTES}-byte ceiling. Streaming auto-store would lift this; not in v0."
                )));
            }
        }
        let url = &info.url;
        let bytes = self.fetch_file(url).await?;
        // Defence-in-depth: even if `size` was None / missing in the
        // imageinfo, the downloaded body shouldn't exceed the same cap.
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(GatewayError::Permanent(format!(
                "wikicommons record `{record_id}` body was {} bytes — exceeds {MAX_FILE_BYTES} ceiling",
                bytes.len()
            )));
        }
        let cid = meta_feeder_sdk::hash::compute_ipfs_cid(&bytes);

        common::store_midhash(cache, record_id, "wikicommons", &cid);

        let file_extension = file_extension_for(record_id);
        let record = into_discovery_record(page).ok_or_else(|| {
            // We just successfully fetched a page; this should not
            // happen unless imageinfo went missing between API calls.
            GatewayError::Permanent(format!(
                "wikicommons record `{record_id}` lost imageinfo between calls"
            ))
        })?;
        Ok(common::single_outcome(cid, bytes, record, file_extension))
    }

    fn health(&self) -> PluginHealth {
        if self.cache.is_some() {
            PluginHealth::Ok
        } else {
            PluginHealth::Degraded {
                reason: "configure() not yet called".to_string(),
            }
        }
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        // Wikicommons holds heterogeneous mime types — `media_type_for`
        // buckets each result into one of meta-core's `fileType` values.
        // `other` covers everything we can't classify.
        &["image", "audio", "video", "document", "other"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        // No semantic refinement yet — wikicommons records carry only
        // the coarse mime bucket. Future: photo / illustration / gif
        // classification could land here.
        &[]
    }
}

// -- MediaWiki JSON shapes (subset we use) ----------------------------------

#[derive(Debug, Default, Deserialize)]
struct MediawikiResponse {
    #[serde(default)]
    query: Option<MediawikiQuery>,
}

impl MediawikiResponse {
    /// Pages come back as a `pages` map keyed by stringified pageid.
    /// `generator=search` adds an `index` field to each page reflecting
    /// the original search ranking — we sort on that so the caller sees
    /// hits in relevance order. Single-record lookups have no `index`
    /// and reach `into_ordered_pages` with at most one entry anyway.
    fn into_ordered_pages(self) -> Vec<MediawikiPage> {
        let Some(query) = self.query else {
            return Vec::new();
        };
        let mut pages: Vec<MediawikiPage> = query.pages.into_values().collect();
        pages.sort_by_key(|p| p.index.unwrap_or(u32::MAX));
        pages
    }
}

#[derive(Debug, Default, Deserialize)]
struct MediawikiQuery {
    #[serde(default)]
    pages: HashMap<String, MediawikiPage>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct MediawikiPage {
    pageid: Option<u64>,
    title: Option<String>,
    /// Search-rank position (1-based) — only populated when the page
    /// came from `generator=search`. Lower = more relevant; we sort on
    /// this so search results don't get scrambled by `HashMap` order.
    index: Option<u32>,
    imageinfo: Option<Vec<MediawikiImageInfo>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MediawikiImageInfo {
    url: String,
    #[serde(default)]
    mime: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    /// Scaled-down preview URL — only populated when the request
    /// included `iiurlwidth=<n>` AND the file is an image. For
    /// audio/video records MediaWiki omits this field even when
    /// `iiurlwidth` was set (no thumbnail exists), and we ship the
    /// record without a `preview` cid.
    #[serde(default)]
    thumburl: Option<String>,
    /// Wikimedia's extmetadata block — license, artist, description,
    /// date. Each entry is `{ "value": "…", "source": "…" }`; we keep
    /// only the value via the inner struct.
    #[serde(default)]
    extmetadata: Option<MediawikiExtMetadata>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct MediawikiExtMetadata {
    artist: Option<ExtValue>,
    license_short_name: Option<ExtValue>,
    image_description: Option<ExtValue>,
    date_time_original: Option<ExtValue>,
    credit: Option<ExtValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ExtValue {
    value: String,
}

/// Map a MediaWiki `page` (with imageinfo) into a `DiscoveryRecord`.
/// Returns `None` when the page has no imageinfo entry — happens for
/// deleted files where the page still exists, and for the synthetic
/// `missing=""` page MediaWiki emits for unknown titles.
fn into_discovery_record(page: MediawikiPage) -> Option<DiscoveryRecord> {
    let title = page.title?;
    // `record_id` is the title with the `File:` prefix stripped — that's
    // how the rest of the gateway tier refers to it (URL paths, library
    // entries, the synthetic `gateway:wikicommons:<id>` CID).
    let record_id = title
        .strip_prefix("File:")
        .map(str::to_string)
        .unwrap_or(title.clone());
    let info = page.imageinfo?.into_iter().next()?;
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    fields.insert("title".to_string(), record_id.clone());
    fields.insert("fileType".to_string(), media_type_for(info.mime.as_deref()));
    fields.insert(
        "sourceUrl".to_string(),
        format!(
            "https://commons.wikimedia.org/wiki/{}",
            encode_title_for_url(&title)
        ),
    );
    fields.insert("fileName".to_string(), record_id.clone());
    fields.insert(upstream_id_field("wikicommons"), record_id.clone());
    // The `preview` field (a content-addressed cid for an inline
    // thumbnail) is NOT set here — `handle_query` seeds the bytes into
    // the gateway's blockstore in parallel and attaches the cid
    // post-hoc. We deliberately don't emit an upstream URL field; the
    // design is "every byte reference is a cid", so dashboards never
    // need to know what `upload.wikimedia.org` is.
    if let Some(mime) = info.mime.as_deref() {
        fields.insert("mime".to_string(), mime.to_string());
    }
    if let Some(ext) = file_extension_for(&record_id) {
        fields.insert("format".to_string(), ext);
    }
    if let Some(size) = info.size {
        fields.insert("size".to_string(), size.to_string());
    }
    if let Some(em) = info.extmetadata {
        if let Some(v) = em.artist {
            fields.insert("artist".to_string(), v.value);
        }
        if let Some(v) = em.license_short_name {
            fields.insert("license".to_string(), v.value);
        }
        if let Some(v) = em.image_description {
            fields.insert("description".to_string(), v.value);
        }
        if let Some(v) = em.date_time_original {
            fields.insert("dateOriginal".to_string(), v.value);
        }
        if let Some(v) = em.credit {
            fields.insert("credit".to_string(), v.value);
        }
    }
    Some(DiscoveryRecord {
        upstream_id: "wikicommons".to_string(),
        record_id,
        fields,
    })
}

/// Bucket a MediaWiki mime into a meta-core `fileType` value
/// (vocabulary defined in
/// [docs/project-architecture/metadata-keys.md → `fileType`]).
/// `image/jpeg` → "image", `audio/ogg` → "audio", `video/webm` → "video",
/// `application/pdf` → "document", everything else → "other".
fn media_type_for(mime: Option<&str>) -> String {
    let Some(mime) = mime else {
        return "other".to_string();
    };
    if mime.starts_with("image/") {
        "image".to_string()
    } else if mime.starts_with("audio/") {
        "audio".to_string()
    } else if mime.starts_with("video/") {
        "video".to_string()
    } else if mime.starts_with("application/pdf") {
        "document".to_string()
    } else {
        "other".to_string()
    }
}

/// File extension from the canonical title. Mirror of arxiv / pubmed's
/// per-plugin hardcoded `"pdf"`, but Commons records carry heterogeneous
/// types — derive from the title (which Wikimedia normalises to include
/// the extension). Returns the lowercased extension without the dot, or
/// `None` if the title has no recognisable extension.
fn file_extension_for(record_id: &str) -> Option<String> {
    let dot = record_id.rfind('.')?;
    let ext = &record_id[dot + 1..];
    if ext.is_empty() || ext.len() > 5 || !ext.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(ext.to_ascii_lowercase())
}

/// Percent-encode a MediaWiki title for the wiki page URL.
/// Wikimedia uses underscores for spaces in URLs (the API normalises
/// spaces → underscores anyway), and percent-encodes the rest. We keep
/// it minimal: replace spaces, percent-encode everything else outside
/// the unreserved set.
fn encode_title_for_url(title: &str) -> String {
    let with_underscores: String = title
        .chars()
        .map(|c| if c == ' ' { '_' } else { c })
        .collect();
    let mut out = String::with_capacity(with_underscores.len());
    for c in with_underscores.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' | '/' | ':' => out.push(c),
            _ => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    out
}

/// Percent-encode a value for the query string. Same safe-set the other
/// plugins use; covers MediaWiki's search query syntax.
/// Map `query.filters["fileType"]` (our cohort vocabulary) into a
/// MediaWiki CirrusSearch `filetype:` clause that Commons' search
/// understands. Returns `None` when the query has no `fileType:`
/// filter, so the caller can leave the upstream query unmodified.
///
/// MediaWiki vocabulary (`filetype:` keyword on Commons): `bitmap`,
/// `drawing`, `audio`, `video`, `office`, `unknown`. Mapping from our
/// coarse `fileType` enum:
///
/// | meta-cohort `fileType` | MediaWiki `filetype:` value(s) |
/// |---|---|
/// | `image`    | `bitmap`, `drawing`               |
/// | `audio`    | `audio`                           |
/// | `video`    | `video`                           |
/// | `document` | `office`                          |
/// | (other)    | dropped — Commons has no concept  |
///
/// CirrusSearch syntax for OR-ing filetype values is **pipe-separated
/// inside a single keyword**: `filetype:bitmap|drawing`. Using
/// explicit `OR` between two `filetype:` keywords parses as an
/// impossible AND (a file can't be both `bitmap` AND `drawing`), so
/// emit the pipe form unconditionally.
fn mediawiki_filetype_clause(query: &GatewayQuery) -> Option<String> {
    let requested = query.filters.get("fileType")?;
    if requested.is_empty() {
        return None;
    }
    let mut mapped: Vec<&'static str> = Vec::new();
    for v in requested {
        let extra: &[&'static str] = match v.to_ascii_lowercase().as_str() {
            "image" => &["bitmap", "drawing"],
            "audio" => &["audio"],
            "video" => &["video"],
            "document" => &["office"],
            // `other`, `subtitle`, `archive`, … — no Commons equivalent.
            // Skip rather than emit a syntactically-broken clause; the
            // dispatcher's post-filter (Layer B) will drop anything
            // that comes back with the wrong fileType anyway.
            _ => &[],
        };
        for e in extra {
            if !mapped.iter().any(|m| m == e) {
                mapped.push(e);
            }
        }
    }
    if mapped.is_empty() {
        return None;
    }
    Some(format!("filetype:{}", mapped.join("|")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sample `generator=search` response with two image hits.
    /// Real Wikimedia API output, trimmed.
    const SAMPLE_SEARCH: &str = r#"{
      "batchcomplete": "",
      "query": {
        "pages": {
          "1": {
            "pageid": 1,
            "ns": 6,
            "title": "File:Sunrise over the sea.jpg",
            "index": 1,
            "imageinfo": [
              {
                "url": "https://upload.wikimedia.org/wikipedia/commons/a/ab/Sunrise_over_the_sea.jpg",
                "mime": "image/jpeg",
                "size": 234567,
                "extmetadata": {
                  "Artist":           { "value": "Jane Doe", "source": "commons" },
                  "LicenseShortName": { "value": "CC BY-SA 4.0", "source": "commons" },
                  "ImageDescription": { "value": "A sunrise photographed off the coast.", "source": "commons" },
                  "DateTimeOriginal": { "value": "2023-08-12 06:14:00", "source": "exif" },
                  "Credit":           { "value": "Own work", "source": "commons" }
                }
              }
            ]
          },
          "2": {
            "pageid": 2,
            "ns": 6,
            "title": "File:Echinacea_purpurea.jpg",
            "index": 2,
            "imageinfo": [
              {
                "url": "https://upload.wikimedia.org/wikipedia/commons/x/yz/Echinacea_purpurea.jpg",
                "mime": "image/jpeg",
                "size": 145000
              }
            ]
          }
        }
      }
    }"#;

    fn empty_gateway_query() -> GatewayQuery {
        GatewayQuery {
            raw_text: String::new(),
            free_text: String::new(),
            filters: std::collections::BTreeMap::new(),
            ranges: Vec::new(),
            negations: Vec::new(),
        }
    }

    #[test]
    fn mediawiki_filetype_clause_returns_none_for_unfiltered_query() {
        assert_eq!(mediawiki_filetype_clause(&empty_gateway_query()), None);
    }

    #[test]
    fn mediawiki_filetype_clause_maps_video() {
        let mut q = empty_gateway_query();
        q.filters.insert("fileType".into(), vec!["video".into()]);
        assert_eq!(
            mediawiki_filetype_clause(&q),
            Some("filetype:video".to_string())
        );
    }

    #[test]
    fn mediawiki_filetype_clause_maps_image_to_bitmap_and_drawing() {
        let mut q = empty_gateway_query();
        q.filters.insert("fileType".into(), vec!["image".into()]);
        assert_eq!(
            mediawiki_filetype_clause(&q),
            Some("filetype:bitmap|drawing".to_string())
        );
    }

    #[test]
    fn mediawiki_filetype_clause_ors_multiple_requested_types() {
        let mut q = empty_gateway_query();
        q.filters
            .insert("fileType".into(), vec!["video".into(), "audio".into()]);
        assert_eq!(
            mediawiki_filetype_clause(&q),
            Some("filetype:video|audio".to_string())
        );
    }

    #[test]
    fn mediawiki_filetype_clause_dedupes_overlapping_mappings() {
        let mut q = empty_gateway_query();
        // image → [bitmap, drawing]; document → [office]; both kept.
        q.filters.insert(
            "fileType".into(),
            vec!["image".into(), "document".into(), "image".into()],
        );
        assert_eq!(
            mediawiki_filetype_clause(&q),
            Some("filetype:bitmap|drawing|office".to_string())
        );
    }

    #[test]
    fn mediawiki_filetype_clause_drops_unmappable_values() {
        let mut q = empty_gateway_query();
        q.filters
            .insert("fileType".into(), vec!["archive".into(), "subtitle".into()]);
        assert_eq!(mediawiki_filetype_clause(&q), None);
    }

    #[test]
    fn parses_search_response_in_index_order() {
        let resp: MediawikiResponse = serde_json::from_str(SAMPLE_SEARCH).expect("parse");
        let pages = resp.into_ordered_pages();
        assert_eq!(pages.len(), 2);
        assert_eq!(
            pages[0].title.as_deref(),
            Some("File:Sunrise over the sea.jpg")
        );
        assert_eq!(
            pages[1].title.as_deref(),
            Some("File:Echinacea_purpurea.jpg")
        );
    }

    #[test]
    fn into_discovery_record_emits_required_fields() {
        let resp: MediawikiResponse = serde_json::from_str(SAMPLE_SEARCH).unwrap();
        let pages = resp.into_ordered_pages();
        let rec = into_discovery_record(pages.into_iter().next().unwrap()).expect("rec");
        assert_eq!(rec.upstream_id, "wikicommons");
        assert_eq!(rec.record_id, "Sunrise over the sea.jpg");
        // §6.6 conventions: fileType (mime bucket) + no contentKind (no
        // semantic refinement on wikicommons records yet).
        assert_eq!(
            rec.fields.get("fileType").map(String::as_str),
            Some("image")
        );
        assert!(!rec.fields.contains_key("contentKind"));
        assert_eq!(rec.fields.get("format").map(String::as_str), Some("jpg"));
        assert_eq!(
            rec.fields.get("sourceUrl").map(String::as_str),
            Some("https://commons.wikimedia.org/wiki/File:Sunrise_over_the_sea.jpg")
        );
        assert_eq!(
            rec.fields.get("fileName").map(String::as_str),
            Some("Sunrise over the sea.jpg")
        );
        // Canonical `<upstream_id>id` field.
        assert_eq!(
            rec.fields.get("wikicommonsid").map(String::as_str),
            Some("Sunrise over the sea.jpg")
        );
        // Extmetadata surface.
        assert_eq!(
            rec.fields.get("artist").map(String::as_str),
            Some("Jane Doe")
        );
        assert_eq!(
            rec.fields.get("license").map(String::as_str),
            Some("CC BY-SA 4.0")
        );
        assert_eq!(
            rec.fields.get("description").map(String::as_str),
            Some("A sunrise photographed off the coast.")
        );
    }

    #[test]
    fn media_type_buckets_match_mime() {
        assert_eq!(media_type_for(Some("image/jpeg")), "image");
        assert_eq!(media_type_for(Some("audio/ogg")), "audio");
        assert_eq!(media_type_for(Some("video/webm")), "video");
        assert_eq!(media_type_for(Some("application/pdf")), "document");
        assert_eq!(media_type_for(Some("application/octet-stream")), "other");
        assert_eq!(media_type_for(None), "other");
    }

    #[test]
    fn file_extension_for_handles_common_shapes() {
        assert_eq!(file_extension_for("Sunrise.jpg").as_deref(), Some("jpg"));
        assert_eq!(file_extension_for("Foo.OGG").as_deref(), Some("ogg"));
        assert_eq!(file_extension_for("Foo.webm").as_deref(), Some("webm"));
        // Too long to be an extension.
        assert!(file_extension_for("Foo.thisisalonglongtail").is_none());
        // No extension at all.
        assert!(file_extension_for("BareName").is_none());
        // Non-alphanumeric trailing — refused.
        assert!(file_extension_for("Foo.tar.gz?x").is_none());
    }

    #[test]
    fn encode_title_swaps_spaces_for_underscores() {
        assert_eq!(
            encode_title_for_url("File:Sunrise over the sea.jpg"),
            "File:Sunrise_over_the_sea.jpg"
        );
        assert_eq!(
            encode_title_for_url("File:Caf\u{00e9}.jpg"),
            "File:Caf%C3%A9.jpg"
        );
    }

    // -- HTTP integration tests against a wiremock server ----------------

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn configured_plugin_against(server: &MockServer) -> (WikicommonsPlugin, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut plugin = WikicommonsPlugin::with_base_url(server.uri());
        plugin.configure(dir.path()).expect("configure");
        (plugin, dir)
    }

    #[tokio::test]
    async fn handle_query_maps_search_to_discovery_records() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("generator", "search"))
            .and(query_param("gsrsearch", "sunrise"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(SAMPLE_SEARCH)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("sunrise"), 10)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record_id, "Sunrise over the sea.jpg");
    }

    #[tokio::test]
    async fn handle_query_truncates_to_max_results() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .respond_with(ResponseTemplate::new(200).set_body_string(SAMPLE_SEARCH))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let records = plugin
            .handle_query(&GatewayQuery::from_free_text("sunrise"), 1)
            .await
            .expect("handle_query");
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn compute_outcomes_fetches_file_and_caches() {
        let server = MockServer::start().await;
        // Build a single-record response that points at the mock's own
        // host, so the file fetch is intercepted too.
        let img_bytes = b"\xFF\xD8\xFF mock jpeg body for hashing\n".to_vec();
        let expected_cid = meta_feeder_sdk::hash::compute_ipfs_cid(&img_bytes);
        let single = format!(
            r#"{{
              "batchcomplete": "",
              "query": {{
                "pages": {{
                  "1": {{
                    "pageid": 1,
                    "ns": 6,
                    "title": "File:Sunrise over the sea.jpg",
                    "imageinfo": [
                      {{
                        "url": "{base}/wikipedia/commons/a/ab/Sunrise_over_the_sea.jpg",
                        "mime": "image/jpeg",
                        "size": {sz}
                      }}
                    ]
                  }}
                }}
              }}
            }}"#,
            base = server.uri(),
            sz = img_bytes.len()
        );
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .and(query_param("titles", "File:Sunrise over the sea.jpg"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(single)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/wikipedia/commons/a/ab/Sunrise_over_the_sea.jpg"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(img_bytes.clone())
                    .insert_header("content-type", "image/jpeg"),
            )
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let outcome = plugin
            .compute_outcomes("Sunrise over the sea.jpg")
            .await
            .expect("compute")
            .into_iter()
            .next()
            .expect("single outcome");
        assert_eq!(outcome.hash.as_str(), expected_cid);
        assert_eq!(outcome.bytes.as_deref(), Some(img_bytes.as_slice()));
        assert_eq!(outcome.file_extension.as_deref(), Some("jpg"));
        let rec = outcome.record.expect("record present on fresh compute");
        assert_eq!(rec.record_id, "Sunrise over the sea.jpg");
        assert_eq!(
            rec.fields.get("fileType").map(String::as_str),
            Some("image")
        );
    }

    #[tokio::test]
    async fn compute_outcomes_cache_hit_skips_http() {
        let server = MockServer::start().await;
        let (plugin, _dir) = configured_plugin_against(&server);
        plugin
            .cache
            .as_ref()
            .unwrap()
            .put_midhash("Sunrise.jpg", "bafyCACHED")
            .unwrap();
        let outcome = plugin
            .compute_outcomes("Sunrise.jpg")
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
    async fn compute_outcomes_refuses_oversized_records() {
        let server = MockServer::start().await;
        let too_big = MAX_FILE_BYTES + 1;
        let single = format!(
            r#"{{
              "batchcomplete": "",
              "query": {{
                "pages": {{
                  "1": {{
                    "pageid": 1,
                    "ns": 6,
                    "title": "File:Huge.webm",
                    "imageinfo": [
                      {{ "url": "{base}/foo.webm", "mime": "video/webm", "size": {sz} }}
                    ]
                  }}
                }}
              }}
            }}"#,
            base = server.uri(),
            sz = too_big
        );
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .respond_with(ResponseTemplate::new(200).set_body_string(single))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("Huge.webm")
            .await
            .expect_err("oversized");
        assert!(matches!(err, GatewayError::Permanent(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn upstream_404_maps_to_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/w/api.php"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let (plugin, _dir) = configured_plugin_against(&server);
        let err = plugin
            .compute_outcomes("Does-not-exist.jpg")
            .await
            .expect_err("expected NotFound");
        assert!(matches!(err, GatewayError::NotFound), "got {err:?}");
    }
}
