use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::DateTime;
use deribit_tick_collector::config::{AppConfig, TickerEmitPolicy, TickerMode};
use deribit_tick_collector::deribit::discovery::InstrumentDiscovery;
use deribit_tick_collector::deribit::manager::WsManager;
use deribit_tick_collector::model::MarketEvent;
use deribit_tick_collector::registry::InstrumentRegistry;
use deribit_tick_collector::telemetry::Telemetry;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::ArbConfig;
use crate::fees::FeeModel;
use crate::market_state::{AffectedTarget, ChainKey, MarketState};
use crate::strategies::box_spread::{mid_implied_coin_box_rate, mid_implied_usd_box_rate};
use crate::strategies::{
    ArbOpportunity, EpisodeKey, EvalContext, StrategyKind, re_evaluate_opportunity, scan_affected,
};

/// Build a zero-auth `deribit-tick-collector` streaming config tuned for live options arbitrage.
pub fn build_collector_config(cfg: &ArbConfig) -> AppConfig {
    let mut app = AppConfig::default();
    app.deribit.markets = cfg.scanner.markets.clone();
    app.deribit.options_book_interval = cfg.scanner.book_interval;
    app.deribit.futures_book_interval = cfg.scanner.book_interval;
    app.deribit.collect_options = true;
    app.deribit.collect_perpetuals = true;
    app.deribit.collect_dated_futures = true;
    app.deribit.collect_orderbook = true;
    app.deribit.collect_ticker = true;
    app.deribit.ticker_mode = TickerMode::Incremental;
    app.deribit.ticker_emit_policy = TickerEmitPolicy::InterestingOnly;
    app.deribit.collect_trades = false;
    app.deribit.collect_combo_trades = false;
    app.deribit.collect_indices = true;
    app.deribit.max_dte_days = if cfg.scanner.max_dte_days > 0 {
        Some(cfg.scanner.max_dte_days)
    } else {
        None
    };
    app.storage.hourly_book_checkpoints = false;
    app.telemetry.metrics_bind = String::new();
    app.normalize();
    app
}

#[derive(Debug, Clone)]
struct LiveEpisode {
    opened_ts_ms: i64,
    last_seen_ts_ms: i64,
    peak_profit_usd: f64,
    peak_edge_bps: f64,
    peak_units: f64,
    ticks: u64,
    last_opp: ArbOpportunity,
}

#[derive(Debug, Serialize)]
struct LiveEpisodeLog<'a> {
    event: &'static str,
    opened_ts_ms: i64,
    closed_ts_ms: Option<i64>,
    duration_ms: i64,
    peak_profit_usd: f64,
    peak_edge_bps: f64,
    peak_units: f64,
    ticks: u64,
    opportunity: &'a ArbOpportunity,
}

/// Run the live event-driven static options arbitrage scanner directly on top of
/// `deribit-tick-collector`'s multi-shard WebSocket engine (no API keys required).
pub async fn run_live_scanner(
    cfg: ArbConfig,
    duration_secs: Option<u64>,
    stats_interval_secs: u64,
) -> Result<()> {
    let collector_cfg = Arc::new(build_collector_config(&cfg));
    let registry = Arc::new(InstrumentRegistry::new());
    let telemetry = Arc::new(Telemetry::new());
    let discovery = Arc::new(InstrumentDiscovery::new(&collector_cfg.deribit.rest_url)?);

    let mut jsonl_writer = match cfg.replay.episodes_output.as_deref() {
        Some(path) => Some(open_jsonl_writer(path)?),
        None => None,
    };

    info!(
        markets = ?cfg.scanner.markets,
        book_interval = cfg.scanner.book_interval.as_str(),
        box_spreads = cfg.scanner.enable_box_spreads,
        parity = cfg.scanner.enable_parity,
        verticals = cfg.scanner.enable_verticals,
        butterflies = cfg.scanner.enable_butterflies,
        min_profit_usd = cfg.scanner.min_profit_usd,
        min_edge_bps = cfg.scanner.min_edge_bps,
        usdc_rate = cfg.rates.usdc_benchmark_rate,
        coin_rate = cfg.rates.coin_benchmark_rate,
        "starting live static-arbitrage scanner"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ingest_tx, mut ingest_rx) = mpsc::channel(collector_cfg.network.ingest_channel_capacity);

    let mut manager = WsManager::new(
        Arc::clone(&collector_cfg),
        Arc::clone(&registry),
        Arc::clone(&telemetry),
        Arc::clone(&discovery),
        ingest_tx.clone(),
        shutdown_rx.clone(),
    );
    manager
        .start()
        .await
        .context("failed to start Deribit WebSocket manager")?;

    let manager_handle = tokio::spawn(manager.run_lifecycle_loop());

    // Background shutdown watcher (SIGINT / SIGTERM / optional --duration-secs).
    let shutdown_tx_sig = shutdown_tx.clone();
    tokio::spawn(async move {
        wait_for_shutdown(duration_secs).await;
        let _ = shutdown_tx_sig.send(true);
    });

    let fees = FeeModel::new(cfg.fees.clone());
    let mut state = MarketState::new();
    let mut active: FxHashMap<EpisodeKey, LiveEpisode> = FxHashMap::default();
    let mut strategy_counts: BTreeMap<StrategyKind, u64> = BTreeMap::new();

    let mut total_events = 0u64;
    let mut book_updates = 0u64;
    let mut episodes_opened = 0u64;
    let mut episodes_closed = 0u64;
    let mut last_event_ts_ms = 0i64;

    let stats_period = Duration::from_secs(stats_interval_secs.max(1));
    let mut stats_tick = tokio::time::interval(stats_period);
    stats_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    stats_tick.tick().await; // consume immediate tick

    let mut shutdown_watch = shutdown_rx.clone();

    loop {
        tokio::select! {
            biased;
            _ = shutdown_watch.changed() => {
                if *shutdown_watch.borrow() {
                    break;
                }
            }
            _ = stats_tick.tick() => {
                info!(
                    events = total_events,
                    book_updates,
                    instruments = state.metas.len(),
                    live_books = state.books.len(),
                    chains = state.chains.len(),
                    active_episodes = active.len(),
                    episodes_opened,
                    episodes_closed,
                    "scanner heartbeat"
                );
            }
            maybe_ev = ingest_rx.recv() => {
                let Some(ev) = maybe_ev else {
                    break;
                };
                process_live_event(
                    &ev,
                    &mut state,
                    &cfg,
                    &fees,
                    &mut active,
                    &mut strategy_counts,
                    &mut total_events,
                    &mut book_updates,
                    &mut episodes_opened,
                    &mut episodes_closed,
                    &mut last_event_ts_ms,
                    &mut jsonl_writer,
                );
            }
        }
    }

    info!("stopping WebSocket shards and draining final market events...");
    let _ = shutdown_tx.send(true);
    let _ = manager_handle.await;
    drop(ingest_tx);

    while let Ok(ev) = ingest_rx.try_recv() {
        process_live_event(
            &ev,
            &mut state,
            &cfg,
            &fees,
            &mut active,
            &mut strategy_counts,
            &mut total_events,
            &mut book_updates,
            &mut episodes_opened,
            &mut episodes_closed,
            &mut last_event_ts_ms,
            &mut jsonl_writer,
        );
    }

    if let Some(w) = jsonl_writer.as_mut() {
        let _ = w.flush();
    }

    println!("\n=== Live Scan Summary ===");
    println!(
        "Events processed:  {} (book updates: {})",
        total_events, book_updates
    );
    println!(
        "Coverage:          {} instruments, {} live L2 books, {} expiry chains",
        state.metas.len(),
        state.books.len(),
        state.chains.len()
    );
    println!(
        "Episodes:          {} opened, {} closed, {} still open at exit",
        episodes_opened,
        episodes_closed,
        active.len()
    );
    if !strategy_counts.is_empty() {
        println!("By strategy:");
        for (strat, cnt) in &strategy_counts {
            println!("  {:<22} {:>6}", strat.as_str(), cnt);
        }
    }
    if !active.is_empty() {
        let mut open_list: Vec<&LiveEpisode> = active.values().collect();
        open_list.sort_by(|a, b| {
            b.last_opp
                .total_profit_usd
                .total_cmp(&a.last_opp.total_profit_usd)
        });
        println!("\nTop active opportunities at exit:");
        for ep in open_list.into_iter().take(10) {
            let age_ms = (last_event_ts_ms - ep.opened_ts_ms).max(0);
            println!("  {} (open {}ms)", ep.last_opp.summary_line(), age_ms);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_live_event(
    ev: &MarketEvent,
    state: &mut MarketState,
    cfg: &ArbConfig,
    fees: &FeeModel,
    active: &mut FxHashMap<EpisodeKey, LiveEpisode>,
    strategy_counts: &mut BTreeMap<StrategyKind, u64>,
    total_events: &mut u64,
    book_updates: &mut u64,
    episodes_opened: &mut u64,
    episodes_closed: &mut u64,
    last_event_ts_ms: &mut i64,
    jsonl_writer: &mut Option<BufWriter<std::fs::File>>,
) {
    *total_events += 1;
    let Some(target) = state.apply_event(ev) else {
        return;
    };
    *book_updates += 1;

    let (target_key, target_strike, now_ms) = match &target {
        AffectedTarget::OptionStrike {
            key,
            strike,
            exchange_ts_ms,
        } => (key, Some(*strike), *exchange_ts_ms),
        AffectedTarget::DatedFuture {
            key,
            exchange_ts_ms,
        } => (key, None, *exchange_ts_ms),
    };
    *last_event_ts_ms = (*last_event_ts_ms).max(now_ms);

    let found = scan_affected(state, cfg, fees, &target);
    let mut found_keys: FxHashSet<EpisodeKey> = FxHashSet::default();

    for opp in found {
        let key = opp.episode_key();
        found_keys.insert(key.clone());

        if let Some(ep) = active.get_mut(&key) {
            ep.last_seen_ts_ms = now_ms;
            ep.ticks += 1;
            if opp.total_profit_usd > ep.peak_profit_usd {
                ep.peak_profit_usd = opp.total_profit_usd;
            }
            if opp.bbo_edge_bps > ep.peak_edge_bps {
                ep.peak_edge_bps = opp.bbo_edge_bps;
            }
            if opp.executable_units > ep.peak_units {
                ep.peak_units = opp.executable_units;
            }
            ep.last_opp = opp;
        } else {
            *episodes_opened += 1;
            *strategy_counts.entry(opp.strategy).or_insert(0) += 1;
            info!("OPEN  {}", opp.summary_line());
            let ep = LiveEpisode {
                opened_ts_ms: now_ms,
                last_seen_ts_ms: now_ms,
                peak_profit_usd: opp.total_profit_usd,
                peak_edge_bps: opp.bbo_edge_bps,
                peak_units: opp.executable_units,
                ticks: 1,
                last_opp: opp,
            };
            if let Some(w) = jsonl_writer.as_mut() {
                let rec = LiveEpisodeLog {
                    event: "open",
                    opened_ts_ms: ep.opened_ts_ms,
                    closed_ts_ms: None,
                    duration_ms: 0,
                    peak_profit_usd: ep.peak_profit_usd,
                    peak_edge_bps: ep.peak_edge_bps,
                    peak_units: ep.peak_units,
                    ticks: ep.ticks,
                    opportunity: &ep.last_opp,
                };
                if let Ok(line) = serde_json::to_string(&rec) {
                    let _ = writeln!(w, "{line}");
                }
            }
            active.insert(key, ep);
        }
    }

    // Re-check active episodes touched by this tick to close vanished dislocations immediately.
    let to_check: Vec<EpisodeKey> = active
        .keys()
        .filter(|k| {
            !found_keys.contains(*k)
                && k.expiration_ts_ms == target_key.expiration_ts_ms
                && k.market.as_str() == target_key.market.as_ref()
                && match target_strike {
                    Some(s) => k.strike_bits.contains(&s.to_bits()),
                    None => k.strategy.uses_dated_future(),
                }
        })
        .cloned()
        .collect();

    for key in to_check {
        let Some(ep) = active.get(&key) else {
            continue;
        };
        if let Some(updated) = re_evaluate_opportunity(state, cfg, fees, &ep.last_opp, now_ms) {
            if let Some(ep_mut) = active.get_mut(&key) {
                ep_mut.last_seen_ts_ms = now_ms;
                ep_mut.last_opp = updated;
            }
        } else if let Some(closed) = active.remove(&key) {
            *episodes_closed += 1;
            let dur_ms = (now_ms - closed.opened_ts_ms).max(0);
            info!(
                "CLOSE [{}] {} {} K={:?} duration={}ms peak_profit=${:.2} peak_edge={:.1}bps ticks={}",
                closed.last_opp.strategy,
                closed.last_opp.market,
                closed.last_opp.expiry_iso(),
                closed.last_opp.strikes,
                dur_ms,
                closed.peak_profit_usd,
                closed.peak_edge_bps,
                closed.ticks
            );
            if let Some(w) = jsonl_writer.as_mut() {
                let rec = LiveEpisodeLog {
                    event: "close",
                    opened_ts_ms: closed.opened_ts_ms,
                    closed_ts_ms: Some(now_ms),
                    duration_ms: dur_ms,
                    peak_profit_usd: closed.peak_profit_usd,
                    peak_edge_bps: closed.peak_edge_bps,
                    peak_units: closed.peak_units,
                    ticks: closed.ticks,
                    opportunity: &closed.last_opp,
                };
                if let Ok(line) = serde_json::to_string(&rec) {
                    let _ = writeln!(w, "{line}");
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExpiryRateRow {
    pub market: String,
    pub is_inverse: bool,
    pub expiration_ts_ms: i64,
    pub dte_days: f64,
    pub spot_index: f64,
    /// USD-numeraire implied rate from 1:1 boxes:
    /// - Linear USDC: undiscounted (`~0%` on Deribit due to zero deposit rate on margin equity)
    /// - Inverse coin: `-(Box_1:1_coin * S_0 / W).ln() / T = ln(F_box / S_0) / T` (`≈ futures carry`)
    pub atm_box_usd_rate: Option<f64>,
    /// Coin-numeraire implied rate from inverse ratio boxes (`1 : K_low/K_high`, `None` for linear).
    pub atm_box_coin_rate: Option<f64>,
    pub box_pairs_sampled: usize,
    pub futures_carry_rate: Option<f64>,
    pub perp_funding_ann: Option<f64>,
    pub dvol: Option<f64>,
}

/// Connect briefly to Deribit, capture initial L2 snapshots + spot indices + DVOL + perp funding,
/// and print a live term-structure table of ATM box-implied financing rates vs. futures carry.
pub async fn run_rates_snapshot(cfg: ArbConfig, warmup_secs: u64) -> Result<Vec<ExpiryRateRow>> {
    let collector_cfg = Arc::new(build_collector_config(&cfg));
    let registry = Arc::new(InstrumentRegistry::new());
    let telemetry = Arc::new(Telemetry::new());
    let discovery = Arc::new(InstrumentDiscovery::new(&collector_cfg.deribit.rest_url)?);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ingest_tx, mut ingest_rx) = mpsc::channel(collector_cfg.network.ingest_channel_capacity);

    let mut manager = WsManager::new(
        Arc::clone(&collector_cfg),
        Arc::clone(&registry),
        Arc::clone(&telemetry),
        Arc::clone(&discovery),
        ingest_tx.clone(),
        shutdown_rx.clone(),
    );
    manager
        .start()
        .await
        .context("failed to start Deribit WebSocket manager")?;

    let manager_handle = tokio::spawn(manager.run_lifecycle_loop());
    let warmup = Duration::from_secs(warmup_secs.max(2));
    let deadline = tokio::time::Instant::now() + warmup;

    let mut state = MarketState::new();
    let mut latest_ts_ms = chrono::Utc::now().timestamp_millis();

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(remaining) => break,
            ev = ingest_rx.recv() => {
                let Some(ev) = ev else {
                    break;
                };
                if let MarketEvent::Book(ref b) = ev {
                    latest_ts_ms = latest_ts_ms.max(b.exchange_ts_ms);
                } else if let MarketEvent::Index(ref i) = ev {
                    latest_ts_ms = latest_ts_ms.max(i.exchange_ts_ms);
                }
                let _ = state.apply_event(&ev);
            }
        }
    }

    let _ = shutdown_tx.send(true);
    let _ = manager_handle.await;
    drop(ingest_tx);
    while let Ok(ev) = ingest_rx.try_recv() {
        let _ = state.apply_event(&ev);
    }

    let fees = FeeModel::new(cfg.fees.clone());
    let rows = compute_rate_term_structure(&state, &cfg, &fees, latest_ts_ms);
    print_rates_table(&rows);
    Ok(rows)
}

/// Compute ATM mid box-implied rates, dated-futures carry, perpetual funding, and DVOL per expiry.
pub fn compute_rate_term_structure(
    state: &MarketState,
    cfg: &ArbConfig,
    fees: &FeeModel,
    now_ms: i64,
) -> Vec<ExpiryRateRow> {
    let mut rows = Vec::new();
    for (ChainKey { .. }, chain) in &state.chains {
        let Some(ctx) = EvalContext::build(state, chain, cfg, fees, now_ms) else {
            continue;
        };
        let strikes: Vec<f64> = chain.strikes.keys().map(|k| k.0).collect();
        // Focus on near-ATM liquid strikes (within ±20% of spot) for clean implied rates.
        let atm_strikes: Vec<f64> = strikes
            .iter()
            .copied()
            .filter(|&k| k >= ctx.spot * 0.80 && k <= ctx.spot * 1.20)
            .collect();

        let mut usd_samples = Vec::new();
        let mut coin_samples = Vec::new();
        for i in 0..atm_strikes.len() {
            for j in (i + 1)..atm_strikes.len() {
                if (atm_strikes[j] - atm_strikes[i]) / ctx.spot < 0.05 {
                    continue;
                }
                if let Some(r) = mid_implied_usd_box_rate(&ctx, atm_strikes[i], atm_strikes[j])
                    && r.is_finite()
                    && (-1.0..=2.0).contains(&r)
                {
                    usd_samples.push(r);
                }
                if let Some(r) = mid_implied_coin_box_rate(&ctx, atm_strikes[i], atm_strikes[j])
                    && r.is_finite()
                    && (-1.0..=2.0).contains(&r)
                {
                    coin_samples.push(r);
                }
            }
        }
        usd_samples.sort_by(|a, b| a.total_cmp(b));
        coin_samples.sort_by(|a, b| a.total_cmp(b));
        let atm_box_usd_rate = if usd_samples.is_empty() {
            None
        } else {
            Some(usd_samples[usd_samples.len() / 2])
        };
        let atm_box_coin_rate = if coin_samples.is_empty() {
            None
        } else {
            Some(coin_samples[coin_samples.len() / 2])
        };

        let dvol = state
            .markets
            .get(chain.market.as_ref())
            .and_then(|m| m.dvol);

        rows.push(ExpiryRateRow {
            market: chain.market.to_string(),
            is_inverse: chain.is_inverse,
            expiration_ts_ms: chain.expiration_ts_ms,
            dte_days: ctx.tte_years * 365.0,
            spot_index: ctx.spot,
            atm_box_usd_rate,
            atm_box_coin_rate,
            box_pairs_sampled: usd_samples.len(),
            futures_carry_rate: ctx.futures_carry,
            perp_funding_ann: ctx.perp_funding_ann,
            dvol,
        });
    }
    rows
}

pub fn print_rates_table(rows: &[ExpiryRateRow]) {
    println!(
        "\n{:<10} {:<4} {:<10} {:>6} {:>10} {:>10} {:>10} {:>6} {:>10} {:>10} {:>6}",
        "MARKET",
        "TYPE",
        "EXPIRY",
        "DTE",
        "SPOT",
        "BOX_USD",
        "BOX_COIN",
        "PAIRS",
        "FUT_CARRY",
        "PERP_ANN",
        "DVOL"
    );
    println!("{}", "-".repeat(106));

    for r in rows {
        let exp = DateTime::from_timestamp_millis(r.expiration_ts_ms)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| r.expiration_ts_ms.to_string());
        let kind = if r.is_inverse { "INV" } else { "LIN" };
        let fmt_pct = |v: Option<f64>| match v {
            Some(x) => format!("{:>+8.2}%", x * 100.0),
            None => "      n/a".to_string(),
        };
        let dvol_s = match r.dvol {
            Some(v) => format!("{v:>5.1}"),
            None => "  n/a".to_string(),
        };
        println!(
            "{:<10} {:<4} {:<10} {:>6.1} {:>10.2} {:>10} {:>10} {:>6} {:>10} {:>10} {:>6}",
            r.market,
            kind,
            exp,
            r.dte_days,
            r.spot_index,
            fmt_pct(r.atm_box_usd_rate),
            fmt_pct(r.atm_box_coin_rate),
            r.box_pairs_sampled,
            fmt_pct(r.futures_carry_rate),
            fmt_pct(r.perp_funding_ann),
            dvol_s
        );
    }

    if rows.is_empty() {
        warn!("no expiries with sufficient live quotes; try increasing --warmup-secs");
    }
}

fn open_jsonl_writer(path: &Path) -> Result<BufWriter<std::fs::File>> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    Ok(BufWriter::new(file))
}

async fn wait_for_shutdown(duration_secs: Option<u64>) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut sig) = signal(SignalKind::terminate()) {
            sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    let timer = async {
        match duration_secs {
            Some(secs) => tokio::time::sleep(Duration::from_secs(secs)).await,
            None => std::future::pending::<()>().await,
        }
    };

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT"),
        _ = sigterm => info!("received SIGTERM"),
        _ = timer => info!("scan duration reached"),
    }
}
