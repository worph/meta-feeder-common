//! Structured query envelope + streaming-search event + wire-error types,
//! shared by feeders (their HTTP contract) and the gateway core (its libp2p
//! wire). These are pure serde data types — no libp2p, no transport.
//!
//! On the core side, `protocol.rs` re-uses these exact structs as the bincode
//! wire shape; the `wire_parity_*` tests there pin the byte encoding. Keep
//! field order / variant order / derive shape stable.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::types::{DiscoveryRecord, GatewayError};

/// Structured query envelope. Gateway plugins do best-effort pushdown to
/// upstream APIs with native query languages (`torznab cat=movie`,
/// `arxiv id_list=`).
///
/// **Authority rule:** `filters` / `ranges` / `negations` are authoritative;
/// `raw_text` is informational. A plugin that consumes both will double-filter
/// and under-match — pick one source per filter; structured wins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayQuery {
    /// Original user text (e.g. `"naruto contentKind:movie imdbid:tt0903747"`).
    /// Verbatim — never mutated by the conversion.
    pub raw_text: String,

    /// Bare-word leaves only, space-joined. The plugin's primary `q=` input
    /// for upstream search APIs that take a single search box.
    pub free_text: String,

    /// Structured filters keyed by field name (raw, unstemmed values).
    /// Multiple values per key = OR within a key; AND across keys.
    pub filters: BTreeMap<String, Vec<String>>,

    /// Numeric range filters (e.g. `movieYear:2020..2024`). Both bounds
    /// optional → unbounded on that side.
    pub ranges: Vec<RangeFilter>,

    /// Negation filters (e.g. `NOT genres:horror`, bare `NOT word`).
    pub negations: Vec<Negation>,
}

/// Numeric range filter inside [`GatewayQuery::ranges`]. Both bounds are
/// `Option<i64>` so open-ended ranges drop one bound to `None`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeFilter {
    pub field: String,
    pub lo: Option<i64>,
    pub hi: Option<i64>,
}

/// Negation entry inside [`GatewayQuery::negations`]. `field` is `Some(name)`
/// for typed negations (`NOT genres:horror`), `None` for bare-word negation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Negation {
    pub field: Option<String>,
    pub value: String,
}

impl GatewayQuery {
    /// Bare-text input to hand to a free-form upstream search API. Falls back
    /// to `"*"` when the conversion produced no free-text leaves so upstreams
    /// that 4xx on empty search return their top-ranked results instead.
    pub fn free_text_or_star(&self) -> &str {
        if self.free_text.trim().is_empty() {
            "*"
        } else {
            self.free_text.as_str()
        }
    }

    /// Construct a free-text-only `GatewayQuery` for in-process use (tests,
    /// debug routes). `raw_text` == `free_text`; structured fields stay empty.
    pub fn from_free_text(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            raw_text: text.clone(),
            free_text: text,
            filters: BTreeMap::new(),
            ranges: Vec::new(),
            negations: Vec::new(),
        }
    }
}

/// One frame in the streaming Search response.
///
/// A plugin emits `Base` records as soon as they're discovered, then
/// best-effort `EnrichPatch`/`Drop` events as enrichment lands, then a
/// terminal `Done`. The default `handle_query_stream` collects `handle_query`
/// and replays it as `Base*` + `Done`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewaySearchEvent {
    /// An un-enriched discovery record.
    Base(DiscoveryRecord),
    /// Incremental enrichment for a previously-sent `Base`, keyed by
    /// `record_id`. `set` fields are inserted/overwritten; `remove` deleted.
    EnrichPatch {
        record_id: String,
        set: BTreeMap<String, String>,
        remove: Vec<String>,
    },
    /// Retract a previously-emitted `Base` record entirely.
    Drop { record_id: String },
    /// Terminal success — no more frames follow.
    Done,
    /// Terminal error — no more frames follow.
    Error(GatewayWireError),
}

/// Wire-stable error shape. Distinct from in-process [`GatewayError`] because
/// adding a variant here is a wire-format change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum GatewayWireError {
    #[error("not found")]
    NotFound,
    #[error("rate limited; retry in {retry_after_s}s")]
    RateLimited { retry_after_s: u32 },
    #[error("upstream transient: {0}")]
    UpstreamTransient(String),
    #[error("upstream permanent: {0}")]
    UpstreamPermanent(String),
    /// The protocol was registered but no plugin claims this `upstream_id`.
    #[error("plugin not loaded")]
    PluginNotLoaded,
    /// Internal codec / decode failure on the *receiver* side.
    #[error("internal: {0}")]
    Internal(String),
}

impl From<GatewayError> for GatewayWireError {
    fn from(e: GatewayError) -> Self {
        match e {
            GatewayError::Transient(s) => GatewayWireError::UpstreamTransient(s),
            GatewayError::Permanent(s) => GatewayWireError::UpstreamPermanent(s),
            GatewayError::RateLimited { retry_after_s } => {
                GatewayWireError::RateLimited { retry_after_s }
            }
            GatewayError::NotFound => GatewayWireError::NotFound,
            // Internal errors are never leaked; map to a transient.
            GatewayError::Internal(_) => {
                GatewayWireError::UpstreamTransient("internal".to_string())
            }
        }
    }
}
