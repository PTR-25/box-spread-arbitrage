use std::sync::Arc;

use deribit_tick_collector::config::StorageConfig;
use deribit_tick_collector::model::{
    BookAction, BookLevelDelta, BookSide, BookUpdateEvent, IndexKind, IndexRow, InstrumentId,
    InstrumentKind, InstrumentMeta, MarketEvent, OptionType,
};
use deribit_tick_collector::registry::InstrumentRegistry;
use deribit_tick_collector::storage::writer::run_storage_engine;
use deribit_tick_collector::telemetry::Telemetry;
use smallvec::SmallVec;
use tempfile::tempdir;
use tokio::sync::mpsc;

use box_spread_arbitrage::config::ArbConfig;
use box_spread_arbitrage::replay::run_replay;
use box_spread_arbitrage::strategies::StrategyKind;

fn option_meta(
    registry: &InstrumentRegistry,
    name: &str,
    strike: f64,
    opt_type: OptionType,
    exp_ts_ms: i64,
    now_ms: i64,
) -> InstrumentMeta {
    let meta = InstrumentMeta {
        id: InstrumentId(0),
        deribit_instrument_id: None,
        instrument_name: Arc::from(name),
        market: Arc::from("SOL_USDC"),
        base_currency: Arc::from("SOL"),
        quote_currency: Arc::from("USDC"),
        settlement_currency: Arc::from("USDC"),
        settlement_period: Arc::from("month"),
        kind: InstrumentKind::Option,
        creation_ts_ms: now_ms - 86_400_000,
        expiration_ts_ms: exp_ts_ms,
        strike: Some(strike),
        option_type: Some(opt_type),
        tick_size: 0.05,
        min_trade_amount: 10.0,
        contract_size: 1.0,
        maker_commission: 0.0001,
        taker_commission: 0.0003,
        is_active: true,
        state: Arc::from("open"),
        recorded_ts_ns: now_ms * 1_000_000,
    };
    let id = registry.upsert(meta);
    (*registry.get_meta(id).unwrap()).clone()
}

fn book_ev(
    id: InstrumentId,
    ts_ms: i64,
    is_snapshot: bool,
    is_checkpoint: bool,
    cid: u64,
    prev_cid: Option<u64>,
    levels: &[(BookSide, BookAction, f64, f64)],
) -> MarketEvent {
    let mut lvls: SmallVec<[BookLevelDelta; 8]> = SmallVec::new();
    for &(side, action, price, amount) in levels {
        lvls.push(BookLevelDelta {
            side,
            action,
            price,
            amount,
        });
    }
    MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: ts_ms * 1_000_000,
        exchange_ts_ms: ts_ms,
        instrument_id: id,
        is_snapshot,
        is_checkpoint,
        change_id: cid,
        prev_change_id: prev_cid,
        levels: lvls,
    })
}

#[tokio::test]
async fn end_to_end_parquet_replay_tracks_exact_lifetime_and_latency_decay() {
    let dir = tempdir().unwrap();
    let base_ts = 1_760_000_000_000_i64;
    let exp_ts = base_ts + 30 * 86_400_000;

    let registry = Arc::new(InstrumentRegistry::new());
    let telemetry = Arc::new(Telemetry::new());
    let storage_cfg = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        flush_interval_secs: 60,
        rotation_interval_secs: 3600,
        hourly_book_checkpoints: false,
        ..StorageConfig::default()
    };

    let (tx, rx) = mpsc::channel(256);
    let writer_handle = tokio::spawn(run_storage_engine(
        storage_cfg,
        Arc::clone(&registry),
        telemetry,
        rx,
        None,
    ));

    let cl = option_meta(
        &registry,
        "SOL_USDC-25NOV26-140-C",
        140.0,
        OptionType::Call,
        exp_ts,
        base_ts,
    );
    let pl = option_meta(
        &registry,
        "SOL_USDC-25NOV26-140-P",
        140.0,
        OptionType::Put,
        exp_ts,
        base_ts,
    );
    let ch = option_meta(
        &registry,
        "SOL_USDC-25NOV26-160-C",
        160.0,
        OptionType::Call,
        exp_ts,
        base_ts,
    );
    let ph = option_meta(
        &registry,
        "SOL_USDC-25NOV26-160-P",
        160.0,
        OptionType::Put,
        exp_ts,
        base_ts,
    );

    // 1. Instrument catalog & spot index
    for m in [&cl, &pl, &ch, &ph] {
        tx.send(MarketEvent::Instrument(m.clone())).await.unwrap();
    }
    tx.send(MarketEvent::Index(IndexRow {
        received_ts_ns: base_ts * 1_000_000,
        exchange_ts_ms: base_ts,
        index_name: Arc::from("sol_usdc"),
        kind: IndexKind::Price,
        value: 150.0,
    }))
    .await
    .unwrap();

    // 2. Fair initial L2 snapshots at t = base_ts (Long box cost = 20.4 > 20.0 -> zero arb)
    tx.send(book_ev(
        cl.id,
        base_ts,
        true,
        false,
        1,
        None,
        &[
            (BookSide::Bid, BookAction::New, 19.8, 100.0),
            (BookSide::Ask, BookAction::New, 20.2, 100.0),
        ],
    ))
    .await
    .unwrap();
    tx.send(book_ev(
        pl.id,
        base_ts,
        true,
        false,
        1,
        None,
        &[
            (BookSide::Bid, BookAction::New, 4.0, 100.0),
            (BookSide::Ask, BookAction::New, 4.2, 100.0),
        ],
    ))
    .await
    .unwrap();
    tx.send(book_ev(
        ch.id,
        base_ts,
        true,
        false,
        1,
        None,
        &[
            (BookSide::Bid, BookAction::New, 4.0, 100.0),
            (BookSide::Ask, BookAction::New, 4.2, 100.0),
        ],
    ))
    .await
    .unwrap();
    tx.send(book_ev(
        ph.id,
        base_ts,
        true,
        false,
        1,
        None,
        &[
            (BookSide::Bid, BookAction::New, 8.0, 100.0),
            (BookSide::Ask, BookAction::New, 8.2, 100.0),
        ],
    ))
    .await
    .unwrap();

    // 3. Inject transient BoxLong mispricing at t0 = base_ts + 1000ms (C140 ask drops to 18.0 for 50 SOL)
    // Cost = 18.0 - 4.0 - 4.0 + 8.2 = 18.2 vs 20.0 payoff => +1.80 pre-fee (~1.575 post-fee)
    let t0 = base_ts + 1000;
    tx.send(book_ev(
        cl.id,
        t0,
        false,
        false,
        2,
        Some(1),
        &[(BookSide::Ask, BookAction::New, 18.0, 50.0)],
    ))
    .await
    .unwrap();

    // 4. Redundant checkpoint at t0 + 10ms with same change_id=1 on pl (should be skipped cleanly)
    tx.send(book_ev(
        pl.id,
        t0 + 10,
        true,
        true,
        1,
        None,
        &[
            (BookSide::Bid, BookAction::New, 4.0, 100.0),
            (BookSide::Ask, BookAction::New, 4.2, 100.0),
        ],
    ))
    .await
    .unwrap();

    // 5. Arbitrageur lifts the 18.0 ask at t0 + 35ms (episode lasts exactly 35 ms!)
    tx.send(book_ev(
        cl.id,
        t0 + 35,
        false,
        false,
        3,
        Some(2),
        &[(BookSide::Ask, BookAction::Delete, 18.0, 0.0)],
    ))
    .await
    .unwrap();

    // 6. Advance dataset clock past 150ms horizon so all latency horizons [5, 20, 50, 150] are evaluated
    tx.send(book_ev(
        ph.id,
        t0 + 250,
        false,
        false,
        2,
        Some(1),
        &[(BookSide::Bid, BookAction::Change, 8.0, 90.0)],
    ))
    .await
    .unwrap();

    drop(tx);
    writer_handle.await.unwrap().unwrap();

    // Run box-spread-arbitrage Parquet replay!
    let jsonl_path = dir.path().join("episodes.jsonl");
    let mut cfg = ArbConfig::default();
    cfg.scanner.markets = vec!["SOL_USDC".into()];
    cfg.scanner.enable_parity = false;
    cfg.scanner.enable_verticals = false;
    cfg.scanner.enable_butterflies = false;
    cfg.rates.usdc_benchmark_rate = 0.0;
    cfg.replay.latency_horizons_ms = vec![5, 20, 50, 150];
    cfg.replay.episodes_output = Some(jsonl_path.clone());

    let report = run_replay(dir.path(), &cfg).unwrap();
    assert_eq!(report.instruments_loaded, 4);
    assert_eq!(report.checkpoints_skipped, 1);
    assert_eq!(report.episodes.len(), 1);

    let ep = &report.episodes[0];
    assert_eq!(ep.strategy, StrategyKind::BoxLong);
    assert_eq!(ep.opened_ts_ms, t0);
    assert_eq!(ep.closed_ts_ms, Some(t0 + 35));
    assert_eq!(ep.duration_ms, 35);
    assert!(!ep.censored_at_end);
    assert!((ep.initial_units - 50.0).abs() < 1e-9);
    assert!(ep.initial_profit_usd > 75.0);

    // Check synthetic latency decay across [5, 20, 50, 150] ms!
    assert_eq!(ep.latency_outcomes.len(), 4);
    assert_eq!(ep.latency_outcomes[0].latency_ms, 5);
    assert!(ep.latency_outcomes[0].survived);
    assert!((ep.latency_outcomes[0].profit_retention - 1.0).abs() < 1e-6);

    assert_eq!(ep.latency_outcomes[1].latency_ms, 20);
    assert!(ep.latency_outcomes[1].survived);
    assert!((ep.latency_outcomes[1].profit_retention - 1.0).abs() < 1e-6);

    assert_eq!(ep.latency_outcomes[2].latency_ms, 50);
    assert!(!ep.latency_outcomes[2].survived);
    assert_eq!(ep.latency_outcomes[2].profit_usd, 0.0);

    assert_eq!(ep.latency_outcomes[3].latency_ms, 150);
    assert!(!ep.latency_outcomes[3].survived);
    assert_eq!(ep.latency_outcomes[3].profit_usd, 0.0);

    // Verify JSONL output parses cleanly
    let jsonl = std::fs::read_to_string(&jsonl_path).unwrap();
    let lines: Vec<&str> = jsonl.lines().collect();
    assert_eq!(lines.len(), 1);
    let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["duration_ms"].as_i64(), Some(35));
}
