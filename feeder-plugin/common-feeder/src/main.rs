//! `common-feeder` — Wikimedia Commons + Giphy feeder sidecar.
//!
//! Wikimedia needs no config. Giphy is opt-in: it loads only when
//! `GIPHY_API_KEY` is set — otherwise its `configure()` returns `MissingConfig`
//! and the harness soft-skips it, so the feeder still serves Wikimedia.
//!
//! Env:
//! - `META_FEEDER_HTTP_LISTEN` — listen addr (default `0.0.0.0:8080`)
//! - `META_FEEDER_STATE_DIR`   — per-plugin cache root (default `/data/meta-feeder`)
//! - `GIPHY_API_KEY`           — Giphy API key (opt-in)
//! - `RUST_LOG`                — tracing filter (default `info`)

use std::net::SocketAddr;

use common_feeder::{giphy::GiphyPlugin, wikicommons::WikicommonsPlugin};
use meta_feeder_sdk::{serve_feeders, FeederPlugin};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let listen: SocketAddr = std::env::var("META_FEEDER_HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
    let state_dir =
        std::env::var("META_FEEDER_STATE_DIR").unwrap_or_else(|_| "/data/meta-feeder".to_string());

    let mut giphy = GiphyPlugin::new();
    if let Ok(key) = std::env::var("GIPHY_API_KEY") {
        if !key.trim().is_empty() {
            giphy.set_api_key(key);
        }
    }

    let plugins: Vec<Box<dyn FeederPlugin>> =
        vec![Box::new(WikicommonsPlugin::new()), Box::new(giphy)];
    serve_feeders(plugins, state_dir, listen).await
}
