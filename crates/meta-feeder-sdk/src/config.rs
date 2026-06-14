//! Self-describing per-plugin configuration: schema, redaction, merge, and the
//! generic schema-driven HTML page the feeder serves at `/config`.
//!
//! The point of this module is that **the gateway carries no per-plugin config
//! knowledge** and **the SDK carries no per-plugin field names**. A plugin
//! declares its fields via [`FeederPlugin::config_schema`](crate::FeederPlugin::config_schema);
//! everything here (redaction of secrets, merge-on-save, and the rendered form)
//! is driven entirely by that schema. Add a feeder → it shows up with a working
//! config form and zero UI/gateway changes.
//!
//! ## Persistence & precedence
//!
//! Config lives at `cache_dir/config.json` (the per-plugin dir handed to
//! `configure()`). The plugin reads **file-or-env** at startup: env is the
//! first-boot seed and stays authoritative until the operator saves through the
//! UI, at which point `config.json` exists and wins. There is no runtime reload
//! — a save returns `restart_required: true` (mirrors the gateway's
//! `gateway-config.json` posture).
//!
//! ## Secrets
//!
//! Fields of kind [`FieldKind::Secret`] are **write-only**: never returned to
//! the page (redacted to a `<key>_set: bool`), and a blank incoming value on
//! save means "keep the stored secret".

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A plugin's full config surface — an ordered list of fields.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ConfigSchema {
    pub fields: Vec<ConfigField>,
}

impl ConfigSchema {
    /// Convenience: `true` when the plugin declares no configurable fields (the
    /// page then renders a "no configuration required" notice).
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// One configurable field. `record_array` fields nest `fields` for the
/// per-record sub-schema (e.g. torznab's `indexers`).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConfigField {
    /// JSON key in the config object.
    pub key: String,
    /// Human label for the form.
    pub label: String,
    #[serde(rename = "type")]
    pub kind: FieldKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(default)]
    pub required: bool,
    /// Sub-fields for [`FieldKind::RecordArray`]. The FIRST sub-field is treated
    /// as the record identifier for secret back-fill on merge (e.g. `name`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<ConfigField>>,
}

impl ConfigField {
    pub fn text(key: &str, label: &str) -> Self {
        Self::new(key, label, FieldKind::Text)
    }
    pub fn secret(key: &str, label: &str) -> Self {
        Self::new(key, label, FieldKind::Secret)
    }
    pub fn bool(key: &str, label: &str) -> Self {
        Self::new(key, label, FieldKind::Bool)
    }
    pub fn number(key: &str, label: &str) -> Self {
        Self::new(key, label, FieldKind::Number)
    }
    pub fn list(key: &str, label: &str) -> Self {
        Self::new(key, label, FieldKind::List)
    }
    pub fn record_array(key: &str, label: &str, fields: Vec<ConfigField>) -> Self {
        let mut f = Self::new(key, label, FieldKind::RecordArray);
        f.fields = Some(fields);
        f
    }
    fn new(key: &str, label: &str, kind: FieldKind) -> Self {
        Self {
            key: key.to_string(),
            label: label.to_string(),
            kind,
            help: None,
            required: false,
            fields: None,
        }
    }
    pub fn with_help(mut self, help: &str) -> Self {
        self.help = Some(help.to_string());
        self
    }
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }
}

/// Field widget / value type. Drives both rendering and the secret/merge rules.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    /// Free text → JSON string.
    Text,
    /// Write-only secret → JSON string; redacted on read, "blank keeps" on save.
    Secret,
    /// Checkbox → JSON bool.
    Bool,
    /// Numeric input → JSON number.
    Number,
    /// Comma/line list → JSON array of strings.
    List,
    /// Repeatable record → JSON array of objects keyed by nested `fields`.
    RecordArray,
}

fn is_blank(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        _ => false,
    }
}

/// Produce a UI-safe view of `values`: secret fields are dropped and replaced
/// with a `<key>_set` boolean; everything else passes through. Recurses into
/// `record_array` records for secret sub-fields.
pub fn redact(values: &Value, schema: &ConfigSchema) -> Value {
    let src = values.as_object().cloned().unwrap_or_default();
    Value::Object(redact_object(&src, &schema.fields))
}

fn redact_object(src: &Map<String, Value>, fields: &[ConfigField]) -> Map<String, Value> {
    let mut out = Map::new();
    for f in fields {
        match f.kind {
            FieldKind::Secret => {
                let set = !is_blank(src.get(&f.key));
                out.insert(format!("{}_set", f.key), Value::Bool(set));
            }
            FieldKind::RecordArray => {
                let subs = f.fields.as_deref().unwrap_or(&[]);
                let arr = src
                    .get(&f.key)
                    .and_then(Value::as_array)
                    .map(|recs| {
                        recs.iter()
                            .map(|r| {
                                let m = r.as_object().cloned().unwrap_or_default();
                                Value::Object(redact_object(&m, subs))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                out.insert(f.key.clone(), Value::Array(arr));
            }
            _ => {
                if let Some(v) = src.get(&f.key) {
                    out.insert(f.key.clone(), v.clone());
                }
            }
        }
    }
    out
}

/// Merge `incoming` (from the UI) onto `base` (the stored/effective config),
/// schema-aware. Returns the full, UNREDACTED object to persist:
/// - secret: blank/absent incoming keeps `base`; non-blank overwrites.
/// - text/number/bool/list: present in incoming overwrites; absent keeps base.
/// - record_array: incoming array replaces base, but blank secret sub-fields are
///   back-filled from the base record matched by the first sub-field (identifier).
pub fn merge(base: &Value, incoming: &Value, schema: &ConfigSchema) -> Value {
    let base_obj = base.as_object().cloned().unwrap_or_default();
    let inc_obj = incoming.as_object().cloned().unwrap_or_default();
    Value::Object(merge_object(&base_obj, &inc_obj, &schema.fields))
}

fn merge_object(
    base: &Map<String, Value>,
    inc: &Map<String, Value>,
    fields: &[ConfigField],
) -> Map<String, Value> {
    let mut out = base.clone();
    for f in fields {
        match f.kind {
            FieldKind::Secret => {
                if !is_blank(inc.get(&f.key)) {
                    out.insert(f.key.clone(), inc.get(&f.key).cloned().unwrap());
                }
                // blank/absent → keep base (no-op)
            }
            FieldKind::RecordArray => {
                let subs = f.fields.as_deref().unwrap_or(&[]);
                let id_key = subs.first().map(|s| s.key.clone());
                let base_recs = base.get(&f.key).and_then(Value::as_array).cloned();
                let inc_recs = match inc.get(&f.key).and_then(Value::as_array) {
                    Some(r) => r.clone(),
                    None => continue, // absent → keep base array untouched
                };
                let merged: Vec<Value> = inc_recs
                    .iter()
                    .map(|rec| {
                        let rec_obj = rec.as_object().cloned().unwrap_or_default();
                        // Find the matching base record by identifier sub-field.
                        let base_match = match (&id_key, base_recs.as_ref()) {
                            (Some(idk), Some(brs)) => brs
                                .iter()
                                .find(|br| br.get(idk) == rec_obj.get(idk))
                                .and_then(Value::as_object)
                                .cloned()
                                .unwrap_or_default(),
                            _ => Map::new(),
                        };
                        Value::Object(merge_object(&base_match, &rec_obj, subs))
                    })
                    .collect();
                out.insert(f.key.clone(), Value::Array(merged));
            }
            _ => {
                if let Some(v) = inc.get(&f.key) {
                    out.insert(f.key.clone(), v.clone());
                }
            }
        }
    }
    out
}

/// The generic, schema-driven config page served at `GET /config`. Self-contained
/// (inline CSS + JS, no external assets) so it loads identically when hit
/// directly on the feeder OR reverse-proxied through the gateway. All API calls
/// are relative to `location.pathname`, so the proxy mount prefix is irrelevant.
pub const CONFIG_PAGE_HTML: &str = include_str!("config_page.html");

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn torznab_like() -> ConfigSchema {
        ConfigSchema {
            fields: vec![
                ConfigField::secret("tmdb_token", "TMDB token"),
                ConfigField::bool("resolve_torrent_files", "Resolve .torrent"),
                ConfigField::record_array(
                    "indexers",
                    "Indexers",
                    vec![
                        ConfigField::text("name", "Name"),
                        ConfigField::secret("api_key", "API key"),
                    ],
                ),
            ],
        }
    }

    #[test]
    fn redact_hides_secrets_and_flags_set() {
        let v = json!({
            "tmdb_token": "abc",
            "resolve_torrent_files": true,
            "indexers": [{"name":"prowlarr","api_key":"k"}]
        });
        let r = redact(&v, &torznab_like());
        assert_eq!(r["tmdb_token_set"], json!(true));
        assert!(r.get("tmdb_token").is_none());
        assert_eq!(r["resolve_torrent_files"], json!(true));
        assert_eq!(r["indexers"][0]["name"], json!("prowlarr"));
        assert_eq!(r["indexers"][0]["api_key_set"], json!(true));
        assert!(r["indexers"][0].get("api_key").is_none());
    }

    #[test]
    fn merge_blank_secret_keeps_base() {
        let base = json!({"tmdb_token": "keepme"});
        let inc = json!({"tmdb_token": ""});
        let m = merge(&base, &inc, &torznab_like());
        assert_eq!(m["tmdb_token"], json!("keepme"));
    }

    #[test]
    fn merge_nonblank_secret_overwrites() {
        let base = json!({"tmdb_token": "old"});
        let inc = json!({"tmdb_token": "new"});
        let m = merge(&base, &inc, &torznab_like());
        assert_eq!(m["tmdb_token"], json!("new"));
    }

    #[test]
    fn merge_record_array_backfills_blank_secret_by_identifier() {
        let base = json!({"indexers":[{"name":"prowlarr","api_key":"secret"}]});
        // UI re-submits the indexer with a blank api_key → keep the stored one.
        let inc = json!({"indexers":[{"name":"prowlarr","api_key":""}]});
        let m = merge(&base, &inc, &torznab_like());
        assert_eq!(m["indexers"][0]["api_key"], json!("secret"));
        assert_eq!(m["indexers"][0]["name"], json!("prowlarr"));
    }

    #[test]
    fn merge_bool_overwrites_when_present() {
        let base = json!({"resolve_torrent_files": true});
        let inc = json!({"resolve_torrent_files": false});
        let m = merge(&base, &inc, &torznab_like());
        assert_eq!(m["resolve_torrent_files"], json!(false));
    }
}
