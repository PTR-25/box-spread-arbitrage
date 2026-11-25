use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use deribit_tick_collector::config::BookInterval;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ArbConfig {
    pub scanner: ScannerConfig,
    pub rates: RatesConfig,
    pub fees: FeeConfig,
    pub replay: ReplayConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerConfig {
    /// Canonical market families (e.g. `BTC`, `ETH`, `BTC_USDC`, `SOL_USDC`, or `ALL`).
    pub markets: Vec<String>,
    pub book_interval: BookInterval,
    pub enable_box_spreads: bool,
    pub enable_parity: bool,
    pub enable_verticals: bool,
    pub enable_butterflies: bool,
    /// Minimum net post-fee total profit (USD/USDC across swept L2 depth).
    pub min_profit_usd: f64,
    /// Minimum net edge per unit expressed in basis points of spot index.
    pub min_edge_bps: f64,
    /// Skip expiries closer than this many minutes to 08:00 UTC settlement.
    pub min_tte_minutes: f64,
    /// Optional upper bound on days to expiry (0 = unlimited).
    pub max_dte_days: u64,
    /// Maximum allowed quote age in ms relative to current event time (0 = disabled).
    pub max_quote_age_ms: i64,
    /// Optional strike-index span cap for box/vertical pairs (0 = all pairs).
    pub max_strike_span: usize,
    /// Optional notional cap per opportunity in USD/USDC (0 = full L2 book).
    pub max_sweep_notional_usd: f64,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            markets: vec![
                "BTC".into(),
                "ETH".into(),
                "BTC_USDC".into(),
                "ETH_USDC".into(),
                "SOL_USDC".into(),
                "XRP_USDC".into(),
            ],
            book_interval: BookInterval::Raw,
            enable_box_spreads: true,
            enable_parity: true,
            enable_verticals: true,
            enable_butterflies: true,
            min_profit_usd: 0.25,
            min_edge_bps: 0.5,
            min_tte_minutes: 15.0,
            max_dte_days: 0,
            max_quote_age_ms: 60_000,
            max_strike_span: 0,
            max_sweep_notional_usd: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RatesConfig {
    pub usdc_benchmark_rate: f64,
    pub coin_benchmark_rate: f64,
}

impl Default for RatesConfig {
    fn default() -> Self {
        Self {
            usdc_benchmark_rate: 0.0,
            coin_benchmark_rate: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeeConfig {
    pub hold_to_expiry: bool,
    pub default_option_taker_rate: f64,
    pub default_future_taker_rate: f64,
    pub option_delivery_rate: f64,
    pub option_fee_cap_rate: f64,
    pub option_taker_override: Option<f64>,
    pub future_taker_override: Option<f64>,
}

impl Default for FeeConfig {
    fn default() -> Self {
        Self {
            hold_to_expiry: true,
            default_option_taker_rate: 0.0003,
            default_future_taker_rate: 0.00035,
            option_delivery_rate: 0.00015,
            option_fee_cap_rate: 0.125,
            option_taker_override: None,
            future_taker_override: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplayConfig {
    pub latency_horizons_ms: Vec<i64>,
    pub episodes_output: Option<PathBuf>,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            latency_horizons_ms: vec![5, 20, 50, 150],
            episodes_output: None,
        }
    }
}

impl ArbConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut cfg = match path {
            Some(p) => {
                let raw = std::fs::read_to_string(p)
                    .with_context(|| format!("failed to read config {}", p.display()))?;
                toml::from_str::<ArbConfig>(&raw)
                    .with_context(|| format!("failed to parse TOML {}", p.display()))?
            }
            None => ArbConfig::default(),
        };
        cfg.normalize();
        Ok(cfg)
    }

    pub fn normalize(&mut self) {
        for m in &mut self.scanner.markets {
            *m = m.trim().to_ascii_uppercase();
        }
        self.scanner.markets.retain(|m| !m.is_empty());
        if self.scanner.markets.is_empty() {
            self.scanner.markets = vec!["BTC".into(), "ETH".into(), "SOL_USDC".into()];
        }
        self.replay.latency_horizons_ms.sort_unstable();
        self.replay.latency_horizons_ms.dedup();
    }

    pub fn validate(&self) -> Result<()> {
        if !self.scanner.enable_box_spreads
            && !self.scanner.enable_parity
            && !self.scanner.enable_verticals
            && !self.scanner.enable_butterflies
        {
            bail!("at least one arbitrage strategy must be enabled");
        }
        if self.fees.option_fee_cap_rate < 0.0 || self.fees.option_delivery_rate < 0.0 {
            bail!("fee rates must be non-negative");
        }
        Ok(())
    }
}
