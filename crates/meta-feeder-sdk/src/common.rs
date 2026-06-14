//! Shared helpers for single-outcome source plugins (`arxiv`, `gutenberg`,
//! `pubmed`, `wikicommons`, `giphy`, `scihub`).
//!
//! These plugins are structurally near-identical: build a reqwest client, map
//! HTTP status → [`GatewayError`], and front a per-record midhash cache whose
//! hit/put boilerplate is the same everywhere. Only the duplicated scaffolding
//! lives here; the genuinely-different bodies stay in the plugin modules.

use std::path::Path;
use std::time::Duration;

use tracing::{debug, warn};

use crate::cache::MidhashCache;
use crate::plugin::{ConfigError, HashKind, HashOutcome};
use crate::types::{DiscoveryRecord, GatewayError, Hash};

/// Build the shared reqwest client every source plugin uses.
pub fn build_http_client(
    timeout_secs: u64,
    user_agent: &str,
    redirect: Option<reqwest::redirect::Policy>,
) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .user_agent(user_agent);
    if let Some(policy) = redirect {
        builder = builder.redirect(policy);
    }
    builder
        .build()
        .expect("reqwest client build is infallible with rustls-tls")
}

/// Map a non-success HTTP status to a [`GatewayError`].
///
/// - `404` → [`GatewayError::NotFound`]
/// - `429` → [`GatewayError::RateLimited`] (honours `Retry-After`, default 60s)
/// - `5xx` → [`GatewayError::Transient`]
/// - anything else → [`GatewayError::Permanent`]
pub fn map_status(resp: &reqwest::Response) -> Result<(), GatewayError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let code = status.as_u16();
    if code == 404 {
        return Err(GatewayError::NotFound);
    }
    if code == 429 {
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(60);
        return Err(GatewayError::RateLimited {
            retry_after_s: retry_after,
        });
    }
    if status.is_server_error() {
        return Err(GatewayError::Transient(format!(
            "upstream {} returned {status}",
            resp.url()
        )));
    }
    Err(GatewayError::Permanent(format!(
        "upstream {} returned {status}",
        resp.url()
    )))
}

/// Open the per-plugin redb midhash cache, wrapping the error with the plugin
/// name for a clean startup diagnostic.
pub fn open_midhash_cache(
    cache_dir: &Path,
    plugin: &'static str,
) -> Result<MidhashCache, ConfigError> {
    MidhashCache::open(cache_dir).map_err(|e| ConfigError::Other {
        plugin,
        source: anyhow::anyhow!("open redb cache: {e}"),
    })
}

/// Cache-hit fast path for single-outcome plugins.
///
/// On hit, returns `Some(vec![sparse outcome])` — hash only, no bytes/record —
/// so the core's `(_, None)` branch skips re-storing content. `Ok(None)` is a
/// cache miss; the caller proceeds to fetch + hash.
pub fn cached_outcome(
    cache: &MidhashCache,
    record_id: &str,
    upstream_id: &'static str,
) -> Result<Option<Vec<HashOutcome>>, GatewayError> {
    let Some(cached) = cache
        .get_midhash(record_id)
        .map_err(|e| GatewayError::Internal(anyhow::anyhow!("cache get: {e}")))?
    else {
        return Ok(None);
    };
    debug!(
        target: "meta-share::gateway",
        upstream = upstream_id,
        record_id,
        "hash cache hit"
    );
    Ok(Some(vec![HashOutcome {
        hash: Hash(cached),
        hash_kind: HashKind::Sha2_256,
        bytes: None,
        record: None,
        file_extension: None,
    }]))
}

/// Persist a freshly-computed `(record_id → cid)` mapping. A write failure is
/// non-fatal, so it is logged and swallowed.
pub fn store_midhash(cache: &MidhashCache, record_id: &str, upstream_id: &'static str, cid: &str) {
    if let Err(e) = cache.put_midhash(record_id, cid) {
        warn!(
            target: "meta-share::gateway",
            upstream = upstream_id,
            record_id,
            error = %e,
            "hash cache put failed (non-fatal)"
        );
    }
}

/// Resolve the per-plugin cache handle, or produce the standard "not
/// configured" internal error.
pub fn require_cache<'a>(
    cache: Option<&'a MidhashCache>,
    plugin: &str,
) -> Result<&'a MidhashCache, GatewayError> {
    cache.ok_or_else(|| {
        GatewayError::Internal(anyhow::anyhow!(
            "{plugin} plugin not configured (configure() never called)"
        ))
    })
}

/// Build the single-element outcome vec a single-outcome plugin returns on a
/// fresh (cache-miss) compute: the full bytes + record, a Sha2_256 IPFS CID,
/// and an optional file extension.
pub fn single_outcome(
    cid: String,
    bytes: bytes::Bytes,
    record: DiscoveryRecord,
    file_extension: Option<String>,
) -> Vec<HashOutcome> {
    vec![HashOutcome {
        hash: Hash(cid),
        hash_kind: HashKind::Sha2_256,
        bytes: Some(bytes),
        record: Some(record),
        file_extension,
    }]
}

/// Percent-encode a query-string value using the unreserved set
/// (RFC 3986 `A-Za-z0-9-_.~`) plus `+` for space.
pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{:02X}", other)),
        }
    }
    out
}

/// GET `url` and return the body bytes, mapping transport + status errors to
/// [`GatewayError`].
pub async fn fetch_bytes(http: &reqwest::Client, url: &str) -> Result<bytes::Bytes, GatewayError> {
    let resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| GatewayError::Transient(format!("GET {url}: {e}")))?;
    map_status(&resp)?;
    resp.bytes()
        .await
        .map_err(|e| GatewayError::Transient(format!("read body {url}: {e}")))
}

/// Like [`fetch_bytes`] but decodes the body as UTF-8 text.
pub async fn fetch_text(http: &reqwest::Client, url: &str) -> Result<String, GatewayError> {
    let resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| GatewayError::Transient(format!("GET {url}: {e}")))?;
    map_status(&resp)?;
    resp.text()
        .await
        .map_err(|e| GatewayError::Transient(format!("read body {url}: {e}")))
}

/// Decode a v1 BitTorrent infohash string into its raw 20 bytes.
///
/// Accepts 40 hex chars (case-insensitive) or 32 base32 chars (RFC 4648,
/// case-insensitive). Returns `None` for anything else.
pub fn decode_infohash(s: &str) -> Option<[u8; 20]> {
    let bytes = s.as_bytes();
    let raw: Vec<u8> = if bytes.len() == 40 && bytes.iter().all(u8::is_ascii_hexdigit) {
        let mut out = Vec::with_capacity(20);
        for pair in bytes.chunks(2) {
            out.push((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?);
        }
        out
    } else if bytes.len() == 32 && bytes.iter().all(u8::is_ascii_alphanumeric) {
        decode_base32_20(bytes)?
    } else {
        return None;
    };
    raw.try_into().ok()
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Decode 32 RFC-4648 base32 chars (case-insensitive, no padding) into the 20
/// bytes they encode.
fn decode_base32_20(chars: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(20);
    let mut buffer: u64 = 0;
    let mut bits: u32 = 0;
    for &c in chars {
        let val = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a',
            b'2'..=b'7' => c - b'2' + 26,
            _ => return None,
        } as u64;
        buffer = (buffer << 5) | val;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_infohash_hex_roundtrips() {
        let hex = "0123456789abcdef0123456789abcdef01234567";
        let raw = decode_infohash(hex).expect("hex decodes");
        assert_eq!(raw[0], 0x01);
        assert_eq!(raw[19], 0x67);
    }

    #[test]
    fn decode_infohash_is_case_insensitive_hex() {
        let lower = decode_infohash("aabbccddeeff00112233445566778899aabbccdd");
        let upper = decode_infohash("AABBCCDDEEFF00112233445566778899AABBCCDD");
        assert!(lower.is_some());
        assert_eq!(lower, upper);
    }

    #[test]
    fn decode_infohash_accepts_base32() {
        let raw = decode_infohash("MFRGGZDFMZTWQ2LKNNWG23TPOBYXE43U");
        assert!(raw.is_some());
    }

    #[test]
    fn decode_infohash_rejects_wrong_shape() {
        assert!(decode_infohash("tooshort").is_none());
        assert!(decode_infohash("zz23456789abcdef0123456789abcdef01234567").is_none());
        assert!(decode_infohash("").is_none());
    }
}
