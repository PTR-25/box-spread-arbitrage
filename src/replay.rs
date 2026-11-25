use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, TimestampMillisecondType};
use arrow_array::{
    Array, ArrayAccessor, BooleanArray, Float64Array, RecordBatch, StringArray, UInt8Array,
    UInt64Array,
};
use chrono::DateTime;
use deribit_tick_collector::model::{
    BookAction, BookLevelDelta, BookSide, BookUpdateEvent, IndexKind, IndexRow, InstrumentId,
    InstrumentKind, InstrumentMeta, OptionType,
};
use deribit_tick_collector::registry::{InstrumentRegistry, market_matches_filter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use smallvec::SmallVec;

use crate::config::ArbConfig;
use crate::fees::FeeModel;
use crate::market_state::{AffectedTarget, MarketState};
use crate::strategies::{
    ArbOpportunity, EpisodeKey, StrategyKind, re_evaluate_opportunity, scan_affected,
};

#[derive(Debug, Clone, Serialize)]
pub struct LatencyOutcome {
    pub latency_ms: i64,
    pub eval_ts_ms: i64,
    pub survived: bool,
    pub profit_usd: f64,
    pub edge_bps: f64,
    pub executable_units: f64,
    pub profit_retention: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EpisodeRecord {
    pub episode_id: usize,
    pub strategy: StrategyKind,
    pub market: String,
    pub is_inverse: bool,
    pub expiration_ts_ms: i64,
    pub strikes: Vec<f64>,
    pub opened_ts_ms: i64,
    pub closed_ts_ms: Option<i64>,
    pub duration_ms: i64,
    pub censored_at_end: bool,
    pub ticks_observed: u64,
    pub initial_profit_usd: f64,
    pub peak_profit_usd: f64,
    pub initial_edge_bps: f64,
    pub peak_edge_bps: f64,
    pub initial_units: f64,
    pub peak_units: f64,
    pub initial_opp: ArbOpportunity,
    pub latency_outcomes: Vec<LatencyOutcome>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategySummary {
    pub strategy: StrategyKind,
    pub episodes: usize,
    pub median_duration_ms: i64,
    pub p90_duration_ms: i64,
    pub max_duration_ms: i64,
    pub total_initial_profit_usd: f64,
    pub total_peak_profit_usd: f64,
    pub avg_peak_edge_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    pub latency_ms: i64,
    pub evaluated_episodes: usize,
    pub survived_episodes: usize,
    pub survival_rate: f64,
    pub avg_profit_retention: f64,
    pub total_post_latency_profit_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayReport {
    pub parquet_files_read: usize,
    pub instruments_loaded: usize,
    pub book_packets_replayed: usize,
    pub index_updates_replayed: usize,
    pub ticker_updates_replayed: usize,
    pub checkpoints_skipped: usize,
    pub start_ts_ms: Option<i64>,
    pub end_ts_ms: Option<i64>,
    pub episodes: Vec<EpisodeRecord>,
    pub by_strategy: Vec<StrategySummary>,
    pub latency_decay: Vec<LatencySummary>,
}

enum ReplayEvent {
    Index(IndexRow),
    Ticker {
        exchange_ts_ms: i64,
        instrument_id: InstrumentId,
        index_price: Option<f64>,
        funding_8h: Option<f64>,
        mark_price: Option<f64>,
    },
    Book(BookUpdateEvent),
}

impl ReplayEvent {
    fn sort_key(&self) -> (i64, u8, bool, u64, u32) {
        match self {
            Self::Index(i) => (i.exchange_ts_ms, 0, false, 0, 0),
            Self::Ticker {
                exchange_ts_ms,
                instrument_id,
                ..
            } => (*exchange_ts_ms, 1, false, 0, instrument_id.0),
            Self::Book(b) => (
                b.exchange_ts_ms,
                2,
                b.is_checkpoint,
                b.change_id,
                b.instrument_id.0,
            ),
        }
    }

    fn exchange_ts_ms(&self) -> i64 {
        match self {
            Self::Index(i) => i.exchange_ts_ms,
            Self::Ticker { exchange_ts_ms, .. } => *exchange_ts_ms,
            Self::Book(b) => b.exchange_ts_ms,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PendingCheck {
    eval_ts_ms: i64,
    episode_idx: usize,
    latency_ms: i64,
}

impl Ord for PendingCheck {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.eval_ts_ms, self.episode_idx, self.latency_ms).cmp(&(
            other.eval_ts_ms,
            other.episode_idx,
            other.latency_ms,
        ))
    }
}

impl PartialOrd for PendingCheck {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct BookPacketAcc {
    exchange_ts_ms: i64,
    instrument_id: InstrumentId,
    is_snapshot: bool,
    is_checkpoint: bool,
    change_id: u64,
    prev_change_id: Option<u64>,
    levels: SmallVec<[BookLevelDelta; 8]>,
}

/// Replay a `deribit-tick-collector` Hive-partitioned Parquet directory tick-by-tick,
/// reconstructing exact L2 order books, tracking every arbitrage episode's lifetime, and
/// simulating synthetic execution latencies across all configured horizons.
pub fn run_replay(data_dir: &Path, cfg: &ArbConfig) -> Result<ReplayReport> {
    let mut files = Vec::new();
    if data_dir.is_file() {
        files.push(data_dir.to_path_buf());
    } else {
        collect_parquet_files(data_dir, &mut files);
    }
    files.sort();

    let mut instrument_files = Vec::new();
    let mut book_files = Vec::new();
    let mut index_files = Vec::new();
    let mut ticker_files = Vec::new();

    for path in &files {
        let f = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| format!("failed to read parquet footer {}", path.display()))?;
        let schema = builder.schema();

        if schema.index_of("taker_commission").is_ok() && schema.index_of("expiration_ts").is_ok() {
            instrument_files.push(path.clone());
        } else if schema.index_of("change_id").is_ok() && schema.index_of("side").is_ok() {
            book_files.push(path.clone());
        } else if schema.index_of("index_name").is_ok() && schema.index_of("index_kind").is_ok() {
            index_files.push(path.clone());
        } else if schema.index_of("changed_mask").is_ok() && schema.index_of("funding_8h").is_ok() {
            ticker_files.push(path.clone());
        }
    }

    let registry = InstrumentRegistry::new();
    let mut state = MarketState::new();

    // 1. Load instrument catalog metadata first (exact taker_commission, min_trade_amount, strikes).
    for path in &instrument_files {
        load_instruments_file(path, &cfg.scanner.markets, &registry, &mut state)?;
    }

    // 2. Load indices, tickers, and grouped atomic book packets.
    let mut events: Vec<ReplayEvent> = Vec::new();
    let mut index_updates_replayed = 0usize;
    let mut ticker_updates_replayed = 0usize;

    for path in &index_files {
        let before = events.len();
        load_indices_file(path, &mut events)?;
        index_updates_replayed += events.len() - before;
    }

    for path in &ticker_files {
        let before = events.len();
        load_tickers_file(
            path,
            &cfg.scanner.markets,
            &registry,
            &mut state,
            &mut events,
        )?;
        ticker_updates_replayed += events.len() - before;
    }

    let mut book_packets: FxHashMap<(i64, InstrumentId, u64, bool), BookPacketAcc> =
        FxHashMap::default();
    for path in &book_files {
        load_book_updates_file(
            path,
            &cfg.scanner.markets,
            &registry,
            &mut state,
            &mut book_packets,
        )?;
    }

    for (_, acc) in book_packets {
        events.push(ReplayEvent::Book(BookUpdateEvent {
            received_ts_ns: acc.exchange_ts_ms * 1_000_000,
            exchange_ts_ms: acc.exchange_ts_ms,
            instrument_id: acc.instrument_id,
            is_snapshot: acc.is_snapshot,
            is_checkpoint: acc.is_checkpoint,
            change_id: acc.change_id,
            prev_change_id: acc.prev_change_id,
            levels: acc.levels,
        }));
    }

    events.sort_unstable_by_key(ReplayEvent::sort_key);

    let start_ts_ms = events.first().map(ReplayEvent::exchange_ts_ms);
    let end_ts_ms = events.last().map(ReplayEvent::exchange_ts_ms);

    // 3. Chronological tick-by-tick replay with episode tracking and latency-decay checks.
    let fees = FeeModel::new(cfg.fees.clone());
    let mut last_book_cid: FxHashMap<InstrumentId, u64> = FxHashMap::default();
    let mut active_episodes: FxHashMap<EpisodeKey, usize> = FxHashMap::default();
    let mut episodes: Vec<EpisodeRecord> = Vec::new();
    let mut pending_checks: BinaryHeap<Reverse<PendingCheck>> = BinaryHeap::new();

    let mut book_packets_replayed = 0usize;
    let mut checkpoints_skipped = 0usize;

    for ev in &events {
        let ev_ts = ev.exchange_ts_ms();

        // Flush every latency check strictly earlier than `ev_ts` against the pre-event book state.
        while let Some(Reverse(top)) = pending_checks.peek() {
            if top.eval_ts_ms >= ev_ts {
                break;
            }
            let check = pending_checks.pop().unwrap().0;
            evaluate_latency_check(&state, cfg, &fees, &mut episodes, &check);
        }

        match ev {
            ReplayEvent::Index(idx) => {
                let _ = state.apply_event(&deribit_tick_collector::model::MarketEvent::Index(
                    idx.clone(),
                ));
            }
            ReplayEvent::Ticker {
                exchange_ts_ms,
                instrument_id,
                index_price,
                funding_8h,
                mark_price,
            } => {
                if let Some(meta) = state.metas.get(instrument_id) {
                    let market = Arc::clone(&meta.market);
                    let is_perp = meta.kind == InstrumentKind::Perpetual;
                    let ctx = state.markets.entry(market).or_default();
                    if let Some(px) = *index_price
                        && px > 0.0
                        && *exchange_ts_ms >= ctx.spot_ts_ms
                    {
                        ctx.spot_index = Some(px);
                        ctx.spot_ts_ms = *exchange_ts_ms;
                    }
                    if is_perp {
                        if funding_8h.is_some() {
                            ctx.perp_funding_8h = *funding_8h;
                        }
                        if mark_price.is_some() {
                            ctx.perp_mark_price = *mark_price;
                        }
                    }
                }
            }
            ReplayEvent::Book(book_ev) => {
                if book_ev.is_checkpoint
                    && last_book_cid.get(&book_ev.instrument_id) == Some(&book_ev.change_id)
                {
                    // Skip redundant quiet-instrument hourly anchor already satisfied by prior hour.
                    checkpoints_skipped += 1;
                    continue;
                }
                last_book_cid.insert(book_ev.instrument_id, book_ev.change_id);
                book_packets_replayed += 1;

                let Some(target) = state.apply_book_update(book_ev) else {
                    continue;
                };

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

                let found = scan_affected(&state, cfg, &fees, &target);
                let mut found_keys: FxHashSet<EpisodeKey> = FxHashSet::default();

                for opp in found {
                    let key = opp.episode_key();
                    found_keys.insert(key.clone());

                    if let Some(&ep_idx) = active_episodes.get(&key) {
                        let ep = &mut episodes[ep_idx];
                        ep.ticks_observed += 1;
                        if opp.total_profit_usd > ep.peak_profit_usd {
                            ep.peak_profit_usd = opp.total_profit_usd;
                        }
                        if opp.bbo_edge_bps > ep.peak_edge_bps {
                            ep.peak_edge_bps = opp.bbo_edge_bps;
                        }
                        if opp.executable_units > ep.peak_units {
                            ep.peak_units = opp.executable_units;
                        }
                    } else {
                        let ep_idx = episodes.len();
                        episodes.push(EpisodeRecord {
                            episode_id: ep_idx,
                            strategy: opp.strategy,
                            market: opp.market.clone(),
                            is_inverse: opp.is_inverse,
                            expiration_ts_ms: opp.expiration_ts_ms,
                            strikes: opp.strikes.clone(),
                            opened_ts_ms: now_ms,
                            closed_ts_ms: None,
                            duration_ms: 0,
                            censored_at_end: false,
                            ticks_observed: 1,
                            initial_profit_usd: opp.total_profit_usd,
                            peak_profit_usd: opp.total_profit_usd,
                            initial_edge_bps: opp.bbo_edge_bps,
                            peak_edge_bps: opp.bbo_edge_bps,
                            initial_units: opp.executable_units,
                            peak_units: opp.executable_units,
                            initial_opp: opp,
                            latency_outcomes: Vec::new(),
                        });
                        active_episodes.insert(key, ep_idx);

                        for &dt in &cfg.replay.latency_horizons_ms {
                            if dt >= 0 {
                                pending_checks.push(Reverse(PendingCheck {
                                    eval_ts_ms: now_ms + dt,
                                    episode_idx: ep_idx,
                                    latency_ms: dt,
                                }));
                            }
                        }
                    }
                }

                // Close active episodes touched by this update that no longer pass arbitrage bounds.
                let touched_keys: Vec<EpisodeKey> = active_episodes
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

                for key in touched_keys {
                    let Some(&ep_idx) = active_episodes.get(&key) else {
                        continue;
                    };
                    let still_alive = re_evaluate_opportunity(
                        &state,
                        cfg,
                        &fees,
                        &episodes[ep_idx].initial_opp,
                        now_ms,
                    )
                    .is_some();
                    if !still_alive {
                        active_episodes.remove(&key);
                        let ep = &mut episodes[ep_idx];
                        ep.closed_ts_ms = Some(now_ms);
                        ep.duration_ms = (now_ms - ep.opened_ts_ms).max(0);
                    }
                }
            }
        }
    }

    // Flush remaining latency checks that fall within the recorded dataset timespan.
    let dataset_end = end_ts_ms.unwrap_or(0);
    while let Some(Reverse(check)) = pending_checks.pop() {
        if check.eval_ts_ms <= dataset_end {
            evaluate_latency_check(&state, cfg, &fees, &mut episodes, &check);
        }
    }

    // Finalize any episodes still open when the dataset ended.
    for (_, ep_idx) in active_episodes {
        let ep = &mut episodes[ep_idx];
        ep.censored_at_end = true;
        ep.duration_ms = (dataset_end - ep.opened_ts_ms).max(0);
    }

    for ep in &mut episodes {
        ep.latency_outcomes.sort_by_key(|o| o.latency_ms);
    }

    if let Some(out_path) = cfg.replay.episodes_output.as_deref() {
        write_episodes_jsonl(out_path, &episodes)?;
    }

    let by_strategy = aggregate_by_strategy(&episodes);
    let latency_decay = aggregate_latency_decay(&episodes, &cfg.replay.latency_horizons_ms);

    Ok(ReplayReport {
        parquet_files_read: files.len(),
        instruments_loaded: state.metas.len(),
        book_packets_replayed,
        index_updates_replayed,
        ticker_updates_replayed,
        checkpoints_skipped,
        start_ts_ms,
        end_ts_ms,
        episodes,
        by_strategy,
        latency_decay,
    })
}

fn evaluate_latency_check(
    state: &MarketState,
    cfg: &ArbConfig,
    fees: &FeeModel,
    episodes: &mut [EpisodeRecord],
    check: &PendingCheck,
) {
    let ep = &mut episodes[check.episode_idx];
    let re = re_evaluate_opportunity(state, cfg, fees, &ep.initial_opp, check.eval_ts_ms);
    let outcome = match re {
        Some(opp) => {
            let retention = if ep.initial_profit_usd > 0.0 {
                (opp.total_profit_usd / ep.initial_profit_usd).clamp(0.0, 10.0)
            } else {
                0.0
            };
            LatencyOutcome {
                latency_ms: check.latency_ms,
                eval_ts_ms: check.eval_ts_ms,
                survived: true,
                profit_usd: opp.total_profit_usd,
                edge_bps: opp.bbo_edge_bps,
                executable_units: opp.executable_units,
                profit_retention: retention,
            }
        }
        None => LatencyOutcome {
            latency_ms: check.latency_ms,
            eval_ts_ms: check.eval_ts_ms,
            survived: false,
            profit_usd: 0.0,
            edge_bps: 0.0,
            executable_units: 0.0,
            profit_retention: 0.0,
        },
    };
    ep.latency_outcomes.push(outcome);
}

fn aggregate_by_strategy(episodes: &[EpisodeRecord]) -> Vec<StrategySummary> {
    let mut grouped: BTreeMap<StrategyKind, Vec<&EpisodeRecord>> = BTreeMap::new();
    for ep in episodes {
        grouped.entry(ep.strategy).or_default().push(ep);
    }

    let mut out = Vec::new();
    for (strategy, list) in grouped {
        let mut durs: Vec<i64> = list.iter().map(|e| e.duration_ms).collect();
        durs.sort_unstable();
        let median_duration_ms = durs[durs.len() / 2];
        let p90_idx = ((durs.len() - 1) * 90) / 100;
        let p90_duration_ms = durs[p90_idx];
        let max_duration_ms = *durs.last().unwrap_or(&0);
        let total_initial_profit_usd = list.iter().map(|e| e.initial_profit_usd).sum();
        let total_peak_profit_usd = list.iter().map(|e| e.peak_profit_usd).sum();
        let avg_peak_edge_bps =
            list.iter().map(|e| e.peak_edge_bps).sum::<f64>() / (list.len() as f64);

        out.push(StrategySummary {
            strategy,
            episodes: list.len(),
            median_duration_ms,
            p90_duration_ms,
            max_duration_ms,
            total_initial_profit_usd,
            total_peak_profit_usd,
            avg_peak_edge_bps,
        });
    }
    out
}

fn aggregate_latency_decay(episodes: &[EpisodeRecord], horizons: &[i64]) -> Vec<LatencySummary> {
    let mut out = Vec::new();
    for &h in horizons {
        let mut evaluated = 0usize;
        let mut survived = 0usize;
        let mut sum_retention = 0.0;
        let mut sum_profit = 0.0;

        for ep in episodes {
            if let Some(o) = ep.latency_outcomes.iter().find(|o| o.latency_ms == h) {
                evaluated += 1;
                if o.survived {
                    survived += 1;
                }
                sum_retention += o.profit_retention;
                sum_profit += o.profit_usd;
            }
        }

        out.push(LatencySummary {
            latency_ms: h,
            evaluated_episodes: evaluated,
            survived_episodes: survived,
            survival_rate: if evaluated > 0 {
                survived as f64 / evaluated as f64
            } else {
                0.0
            },
            avg_profit_retention: if evaluated > 0 {
                sum_retention / evaluated as f64
            } else {
                0.0
            },
            total_post_latency_profit_usd: sum_profit,
        });
    }
    out
}

pub fn print_replay_report(report: &ReplayReport) {
    let span_str = match (report.start_ts_ms, report.end_ts_ms) {
        (Some(s), Some(e)) => {
            let s_dt = DateTime::from_timestamp_millis(s)
                .map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
                .unwrap_or_else(|| s.to_string());
            let e_dt = DateTime::from_timestamp_millis(e)
                .map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
                .unwrap_or_else(|| e.to_string());
            let secs = (e - s).max(0) as f64 / 1000.0;
            format!("{s_dt} -> {e_dt} ({secs:.2}s)")
        }
        _ => "empty dataset".to_string(),
    };

    println!("\n=== Tick-by-Tick Static Arbitrage Replay Report ===");
    println!("Parquet files:     {}", report.parquet_files_read);
    println!("Timespan:          {span_str}");
    println!("Instruments:       {}", report.instruments_loaded);
    println!(
        "Events replayed:   {} book packets ({} checkpoints skipped), {} indices, {} tickers",
        report.book_packets_replayed,
        report.checkpoints_skipped,
        report.index_updates_replayed,
        report.ticker_updates_replayed
    );
    println!("Episodes detected: {}", report.episodes.len());

    if !report.by_strategy.is_empty() {
        println!(
            "\n{:<22} {:>8} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
            "STRATEGY",
            "EPISODES",
            "MED_MS",
            "P90_MS",
            "MAX_MS",
            "INIT_PNL($)",
            "PEAK_PNL($)",
            "AVG_BPS"
        );
        println!("{}", "-".repeat(100));
        for s in &report.by_strategy {
            println!(
                "{:<22} {:>8} {:>10} {:>10} {:>10} {:>12.2} {:>12.2} {:>10.2}",
                s.strategy.as_str(),
                s.episodes,
                s.median_duration_ms,
                s.p90_duration_ms,
                s.max_duration_ms,
                s.total_initial_profit_usd,
                s.total_peak_profit_usd,
                s.avg_peak_edge_bps
            );
        }
    }

    if !report.episodes.is_empty() && !report.latency_decay.is_empty() {
        println!(
            "\n{:<14} {:>10} {:>10} {:>11} {:>14} {:>16}",
            "LATENCY_MS", "EVALUATED", "SURVIVED", "SURVIVAL_%", "AVG_RETENTION", "POST_LAT_PNL($)"
        );
        println!("{}", "-".repeat(80));
        for l in &report.latency_decay {
            println!(
                "{:<14} {:>10} {:>10} {:>10.1}% {:>13.1}% {:>16.2}",
                format!("{} ms", l.latency_ms),
                l.evaluated_episodes,
                l.survived_episodes,
                l.survival_rate * 100.0,
                l.avg_profit_retention * 100.0,
                l.total_post_latency_profit_usd
            );
        }
    }

    if !report.episodes.is_empty() {
        let mut top: Vec<&EpisodeRecord> = report.episodes.iter().collect();
        top.sort_by(|a, b| b.peak_profit_usd.total_cmp(&a.peak_profit_usd));
        println!("\nTop episodes by peak post-fee profit:");
        for ep in top.into_iter().take(10) {
            let status = if ep.censored_at_end {
                " (open@end)"
            } else {
                ""
            };
            println!(
                "  #{} {} dur={}ms{} peak=${:.2} ticks={}",
                ep.episode_id,
                ep.initial_opp.summary_line(),
                ep.duration_ms,
                status,
                ep.peak_profit_usd,
                ep.ticks_observed
            );
        }
    }
}

fn write_episodes_jsonl(path: &Path, episodes: &[EpisodeRecord]) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut w = BufWriter::new(file);
    for ep in episodes {
        let line = serde_json::to_string(ep)?;
        writeln!(w, "{line}")?;
    }
    w.flush()?;
    Ok(())
}

fn load_instruments_file(
    path: &Path,
    markets: &[String],
    registry: &InstrumentRegistry,
    state: &mut MarketState,
) -> Result<()> {
    let f = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)?
        .with_batch_size(65_536)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let Some(names) = extract_string_column(&batch, "instrument_name") else {
            continue;
        };
        let Some(mkts) = extract_string_column(&batch, "market") else {
            continue;
        };
        let Some(bases) = extract_string_column(&batch, "base_currency") else {
            continue;
        };
        let Some(quotes) = extract_string_column(&batch, "quote_currency") else {
            continue;
        };
        let Some(settles) = extract_string_column(&batch, "settlement_currency") else {
            continue;
        };
        let Some(periods) = extract_string_column(&batch, "settlement_period") else {
            continue;
        };
        let opt_types = extract_string_column(&batch, "option_type");
        let states = extract_string_column(&batch, "state");

        let schema = batch.schema();
        let kind_col = batch
            .column(schema.index_of("kind")?)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .context("kind column")?;
        let exp_col = batch
            .column(schema.index_of("expiration_ts")?)
            .as_primitive::<TimestampMillisecondType>();
        let cre_col = batch
            .column(schema.index_of("creation_ts")?)
            .as_primitive::<TimestampMillisecondType>();
        let strike_col = batch
            .column(schema.index_of("strike")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("strike column")?;
        let tick_col = batch
            .column(schema.index_of("tick_size")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("tick_size column")?;
        let min_amt_col = batch
            .column(schema.index_of("min_trade_amount")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("min_trade_amount column")?;
        let cs_col = batch
            .column(schema.index_of("contract_size")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("contract_size column")?;
        let maker_col = batch
            .column(schema.index_of("maker_commission")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("maker_commission column")?;
        let taker_col = batch
            .column(schema.index_of("taker_commission")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("taker_commission column")?;
        let active_col = batch
            .column(schema.index_of("is_active")?)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .context("is_active column")?;

        for i in 0..batch.num_rows() {
            let (Some(name), Some(mkt)) = (names[i].as_deref(), mkts[i].as_deref()) else {
                continue;
            };
            if !market_matches_filter(mkt, markets) {
                continue;
            }
            let Some(kind) = InstrumentKind::from_u8(kind_col.value(i)) else {
                continue;
            };
            let option_type =
                opt_types
                    .as_ref()
                    .and_then(|v| v[i].as_deref())
                    .and_then(|s| match s {
                        "call" => Some(OptionType::Call),
                        "put" => Some(OptionType::Put),
                        _ => None,
                    });

            let meta = InstrumentMeta {
                id: InstrumentId(0),
                deribit_instrument_id: None,
                instrument_name: Arc::from(name),
                market: Arc::from(mkt),
                base_currency: Arc::from(bases[i].as_deref().unwrap_or(mkt)),
                quote_currency: Arc::from(quotes[i].as_deref().unwrap_or("USD")),
                settlement_currency: Arc::from(settles[i].as_deref().unwrap_or(mkt)),
                settlement_period: Arc::from(periods[i].as_deref().unwrap_or("unknown")),
                kind,
                creation_ts_ms: cre_col.value(i),
                expiration_ts_ms: exp_col.value(i),
                strike: if strike_col.is_null(i) {
                    None
                } else {
                    Some(strike_col.value(i))
                },
                option_type,
                tick_size: tick_col.value(i),
                min_trade_amount: min_amt_col.value(i),
                contract_size: cs_col.value(i),
                maker_commission: maker_col.value(i),
                taker_commission: taker_col.value(i),
                is_active: active_col.value(i),
                state: Arc::from(
                    states
                        .as_ref()
                        .and_then(|v| v[i].as_deref())
                        .unwrap_or("open"),
                ),
                recorded_ts_ns: 0,
            };

            let id = registry.upsert(meta);
            if let Some(m) = registry.get_meta(id) {
                state.register_instrument(m);
            }
        }
    }
    Ok(())
}

fn load_indices_file(path: &Path, events: &mut Vec<ReplayEvent>) -> Result<()> {
    let f = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)?
        .with_batch_size(65_536)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let Some(names) = extract_string_column(&batch, "index_name") else {
            continue;
        };
        let schema = batch.schema();
        let ts_col = batch
            .column(schema.index_of("exchange_ts")?)
            .as_primitive::<TimestampMillisecondType>();
        let kind_col = batch
            .column(schema.index_of("index_kind")?)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .context("index_kind column")?;
        let val_col = batch
            .column(schema.index_of("value")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("value column")?;

        for (i, name_opt) in names.iter().enumerate().take(batch.num_rows()) {
            let Some(name) = name_opt.as_deref() else {
                continue;
            };
            let kind = match kind_col.value(i) {
                0 => IndexKind::Price,
                1 => IndexKind::Volatility,
                _ => continue,
            };
            let ts = ts_col.value(i);
            events.push(ReplayEvent::Index(IndexRow {
                received_ts_ns: ts * 1_000_000,
                exchange_ts_ms: ts,
                index_name: Arc::from(name),
                kind,
                value: val_col.value(i),
            }));
        }
    }
    Ok(())
}

fn load_tickers_file(
    path: &Path,
    markets: &[String],
    registry: &InstrumentRegistry,
    state: &mut MarketState,
    events: &mut Vec<ReplayEvent>,
) -> Result<()> {
    let f = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)?
        .with_batch_size(65_536)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let Some(names) = extract_string_column(&batch, "instrument_name") else {
            continue;
        };
        let schema = batch.schema();
        let ts_col = batch
            .column(schema.index_of("exchange_ts")?)
            .as_primitive::<TimestampMillisecondType>();
        let idx_col = batch
            .column(schema.index_of("index_price")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("index_price column")?;
        let fund_col = batch
            .column(schema.index_of("funding_8h")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("funding_8h column")?;
        let mark_col = batch
            .column(schema.index_of("mark_price")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("mark_price column")?;

        for (i, name_opt) in names.iter().enumerate().take(batch.num_rows()) {
            let Some(name) = name_opt.as_deref() else {
                continue;
            };
            let Some(id) = resolve_filtered_instrument(name, markets, registry, state) else {
                continue;
            };
            let index_price = if idx_col.is_null(i) {
                None
            } else {
                Some(idx_col.value(i))
            };
            let funding_8h = if fund_col.is_null(i) {
                None
            } else {
                Some(fund_col.value(i))
            };
            let mark_price = if mark_col.is_null(i) {
                None
            } else {
                Some(mark_col.value(i))
            };
            if index_price.is_none() && funding_8h.is_none() && mark_price.is_none() {
                continue;
            }
            events.push(ReplayEvent::Ticker {
                exchange_ts_ms: ts_col.value(i),
                instrument_id: id,
                index_price,
                funding_8h,
                mark_price,
            });
        }
    }
    Ok(())
}

fn load_book_updates_file(
    path: &Path,
    markets: &[String],
    registry: &InstrumentRegistry,
    state: &mut MarketState,
    packets: &mut FxHashMap<(i64, InstrumentId, u64, bool), BookPacketAcc>,
) -> Result<()> {
    let f = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)?
        .with_batch_size(65_536)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let Some(names) = extract_string_column(&batch, "instrument_name") else {
            continue;
        };
        let schema = batch.schema();
        let ts_col = batch
            .column(schema.index_of("exchange_ts")?)
            .as_primitive::<TimestampMillisecondType>();
        let snap_col = batch
            .column(schema.index_of("is_snapshot")?)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .context("is_snapshot column")?;
        let ckpt_col = schema
            .index_of("is_checkpoint")
            .ok()
            .and_then(|idx| batch.column(idx).as_any().downcast_ref::<BooleanArray>());
        let cid_col = batch
            .column(schema.index_of("change_id")?)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("change_id column")?;
        let prev_col = batch
            .column(schema.index_of("prev_change_id")?)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("prev_change_id column")?;
        let side_col = batch
            .column(schema.index_of("side")?)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .context("side column")?;
        let act_col = batch
            .column(schema.index_of("action")?)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .context("action column")?;
        let px_col = batch
            .column(schema.index_of("price")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("price column")?;
        let amt_col = batch
            .column(schema.index_of("amount")?)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("amount column")?;

        for (i, name_opt) in names.iter().enumerate().take(batch.num_rows()) {
            let Some(name) = name_opt.as_deref() else {
                continue;
            };
            let Some(id) = resolve_filtered_instrument(name, markets, registry, state) else {
                continue;
            };

            let ts = ts_col.value(i);
            let cid = cid_col.value(i);
            let is_checkpoint = ckpt_col.is_some_and(|c| c.value(i));
            let pkt = packets
                .entry((ts, id, cid, is_checkpoint))
                .or_insert_with(|| BookPacketAcc {
                    exchange_ts_ms: ts,
                    instrument_id: id,
                    is_snapshot: snap_col.value(i),
                    is_checkpoint,
                    change_id: cid,
                    prev_change_id: if prev_col.is_null(i) {
                        None
                    } else {
                        Some(prev_col.value(i))
                    },
                    levels: SmallVec::new(),
                });

            let side = BookSide::from_u8(side_col.value(i)).unwrap_or(BookSide::Empty);
            let action = BookAction::from_u8(act_col.value(i)).unwrap_or(BookAction::Empty);
            let price = if px_col.is_null(i) {
                f64::NAN
            } else {
                px_col.value(i)
            };
            pkt.levels.push(BookLevelDelta {
                side,
                action,
                price,
                amount: amt_col.value(i),
            });
        }
    }
    Ok(())
}

fn resolve_filtered_instrument(
    name: &str,
    markets: &[String],
    registry: &InstrumentRegistry,
    state: &mut MarketState,
) -> Option<InstrumentId> {
    if let Some(&id) = state.by_name.get(name) {
        return Some(id);
    }
    let id = registry.get_or_register_from_name(name)?;
    let meta = registry.get_meta(id)?;
    if !market_matches_filter(&meta.market, markets) {
        return None;
    }
    state.register_instrument(meta);
    Some(id)
}

fn collect_parquet_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out);
        } else if path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
        {
            out.push(path);
        }
    }
}

fn extract_string_column(batch: &RecordBatch, col_name: &str) -> Option<Vec<Option<String>>> {
    let idx = batch.schema().index_of(col_name).ok()?;
    let col = batch.column(idx);
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Some(
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_string())
                    }
                })
                .collect(),
        );
    }
    if let Some(dict) = col.as_dictionary_opt::<Int32Type>() {
        let typed = dict.downcast_dict::<StringArray>()?;
        return Some(
            (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        None
                    } else {
                        Some(typed.value(i).to_string())
                    }
                })
                .collect(),
        );
    }
    None
}
