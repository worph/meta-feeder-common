//! In-process types used by the gateway runtime, feeders, and plugins.
//!
//! `GatewayError` here is **distinct from** the wire-stable
//! `GatewayWireError` (see [`crate::query`]): this one can carry rich
//! internal context (anyhow chains, plugin specifics); the wire one is narrow
//! on purpose since adding variants is a protocol change. Conversion happens
//! at the protocol boundary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Content-addressed hash string carried over the wire. Renamed from
/// `Midhash` in the multi-outcome plugin contract migration — the name
/// `Midhash` lied for sha2-256 CIDs (which gateway plugins also emit
/// via `compute_ipfs_cid`). The carrier is generic; the hash family is
/// discriminated by `HashKind` on `HashOutcome`.
///
/// Shape: base32-lower 'b'-prefixed multibase CIDv1. The specific
/// multihash inside (`0x1000` for midhash256, `0x12` for sha2-256) is
/// determined by the producing plugin's hashing path.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash(pub String);

impl Hash {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Pre-hash identifier for a record discovered through a gateway.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiscoveryId {
    pub upstream_id: String,
    pub record_id: String,
}

/// Metadata-only search result. Distinct from meta-core's persistent record
/// (which is a flat `map[string]string` indexed by midhash); a
/// `DiscoveryRecord` only lives inside the gateway search/response pipeline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryRecord {
    pub upstream_id: String,
    pub record_id: String,
    pub fields: BTreeMap<String, String>,
}

/// In-process error returned by `FeederPlugin` methods.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("upstream transient error: {0}")]
    Transient(String),

    #[error("upstream permanent error: {0}")]
    Permanent(String),

    #[error("rate limited; retry in {retry_after_s}s")]
    RateLimited { retry_after_s: u32 },

    #[error("record not found upstream")]
    NotFound,

    #[error("internal: {0}")]
    Internal(#[from] anyhow::Error),
}

/// Byte stream returned by a plugin's `handle_fetch`.
pub type ByteStream = futures::stream::BoxStream<'static, Result<bytes::Bytes, GatewayError>>;

/// Plugin liveness snapshot surfaced via `/health`. Must not call upstream
/// — `health()` is invoked on every status poll.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum PluginHealth {
    Ok,
    Degraded { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_display_and_as_str_match() {
        let m = Hash("bafy123".to_string());
        assert_eq!(m.as_str(), "bafy123");
        assert_eq!(format!("{m}"), "bafy123");
    }

    #[test]
    fn discovery_record_roundtrips_through_serde_json() {
        let r = DiscoveryRecord {
            upstream_id: "scihub".to_string(),
            record_id: "10.1038/s41586-021-03819-2".to_string(),
            fields: BTreeMap::from_iter([
                ("title".to_string(), "AlphaFold".to_string()),
                ("year".to_string(), "2021".to_string()),
            ]),
        };
        let j = serde_json::to_string(&r).expect("serialize");
        let back: DiscoveryRecord = serde_json::from_str(&j).expect("deserialize");
        assert_eq!(r, back);
    }

    #[test]
    fn plugin_health_serialises_with_tag() {
        let ok = serde_json::to_value(PluginHealth::Ok).unwrap();
        assert_eq!(ok, serde_json::json!({"state": "ok"}));
        let bad = serde_json::to_value(PluginHealth::Degraded {
            reason: "upstream 5xx".into(),
        })
        .unwrap();
        assert_eq!(
            bad,
            serde_json::json!({"state": "degraded", "reason": "upstream 5xx"})
        );
    }
}
