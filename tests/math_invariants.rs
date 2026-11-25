use std::sync::Arc;

use deribit_tick_collector::model::{
    BookAction, BookLevelDelta, BookSide, BookUpdateEvent, IndexKind, IndexRow, InstrumentId,
    InstrumentKind, InstrumentMeta, MarketEvent, OptionType,
};
use smallvec::SmallVec;

use box_spread_arbitrage::config::{ArbConfig, FeeConfig};
use box_spread_arbitrage::fees::FeeModel;
use box_spread_arbitrage::market_state::MarketState;
use box_spread_arbitrage::strategies::{StrategyKind, scan_all};

const NOW_MS: i64 = 1_760_000_000_000;
const EXP_MS: i64 = NOW_MS + 30 * 86_400_000; // 30 days to expiry

#[allow(clippy::too_many_arguments)]
fn make_meta(
    id: u32,
    name: &str,
    market: &str,
    base: &str,
    settle: &str,
    kind: InstrumentKind,
    strike: Option<f64>,
    option_type: Option<OptionType>,
    min_trade_amount: f64,
    taker_commission: f64,
) -> Arc<InstrumentMeta> {
    Arc::new(InstrumentMeta {
        id: InstrumentId(id),
        deribit_instrument_id: None,
        instrument_name: Arc::from(name),
        market: Arc::from(market),
        base_currency: Arc::from(base),
        quote_currency: Arc::from(if settle == "USDC" { "USDC" } else { "USD" }),
        settlement_currency: Arc::from(settle),
        settlement_period: Arc::from("month"),
        kind,
        creation_ts_ms: NOW_MS - 86_400_000,
        expiration_ts_ms: EXP_MS,
        strike,
        option_type,
        tick_size: 0.0001,
        min_trade_amount,
        contract_size: if settle == "USDC" { 1.0 } else { 10.0 },
        maker_commission: 0.0001,
        taker_commission,
        is_active: true,
        state: Arc::from("open"),
        recorded_ts_ns: NOW_MS * 1_000_000,
    })
}

fn push_snapshot(
    state: &mut MarketState,
    id: u32,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    ts_ms: i64,
    cid: u64,
) {
    let mut levels: SmallVec<[BookLevelDelta; 8]> = SmallVec::new();
    for &(price, amount) in bids {
        levels.push(BookLevelDelta {
            side: BookSide::Bid,
            action: BookAction::New,
            price,
            amount,
        });
    }
    for &(price, amount) in asks {
        levels.push(BookLevelDelta {
            side: BookSide::Ask,
            action: BookAction::New,
            price,
            amount,
        });
    }
    state.apply_book_update(&BookUpdateEvent {
        received_ts_ns: ts_ms * 1_000_000,
        exchange_ts_ms: ts_ms,
        instrument_id: InstrumentId(id),
        is_snapshot: true,
        is_checkpoint: false,
        change_id: cid,
        prev_change_id: None,
        levels,
    });
}

fn push_index(state: &mut MarketState, index_name: &str, value: f64, ts_ms: i64) {
    state.apply_event(&MarketEvent::Index(IndexRow {
        received_ts_ns: ts_ms * 1_000_000,
        exchange_ts_ms: ts_ms,
        index_name: Arc::from(index_name),
        kind: IndexKind::Price,
        value,
    }));
}

#[test]
fn fair_linear_and_inverse_chains_produce_zero_false_positives() {
    let mut state = MarketState::new();
    let cfg = ArbConfig::default();
    let fees = FeeModel::new(cfg.fees.clone());

    let t = (EXP_MS - NOW_MS) as f64 / (365.0 * 86_400_000.0);
    let df_usdc = (-cfg.rates.usdc_benchmark_rate * t).exp();

    // --- 1. Fair linear SOL_USDC chain (spot = 150, F = 152) ---
    let fut_sol = 152.0;
    let strikes_sol = [(140.0, 15.0), (150.0, 9.0), (160.0, 5.0)];
    state.register_instrument(make_meta(
        1,
        "SOL_USDC-25NOV26",
        "SOL_USDC",
        "SOL",
        "USDC",
        InstrumentKind::Future,
        None,
        None,
        1.0,
        0.00035,
    ));
    push_snapshot(
        &mut state,
        1,
        &[(151.9, 500.0)],
        &[(152.1, 500.0)],
        NOW_MS,
        1,
    );

    let mut next_id = 10u32;
    for &(k, call_mid) in &strikes_sol {
        // Put-call parity: C - P = df_usdc * (F - K) => P = C - df_usdc * (F - K)
        let put_mid = call_mid - df_usdc * (fut_sol - k);
        assert!(put_mid > 0.0);
        state.register_instrument(make_meta(
            next_id,
            &format!("SOL_USDC-25NOV26-{k}-C"),
            "SOL_USDC",
            "SOL",
            "USDC",
            InstrumentKind::Option,
            Some(k),
            Some(OptionType::Call),
            10.0,
            0.0003,
        ));
        push_snapshot(
            &mut state,
            next_id,
            &[(call_mid - 0.10, 100.0)],
            &[(call_mid + 0.10, 100.0)],
            NOW_MS,
            1,
        );
        next_id += 1;

        state.register_instrument(make_meta(
            next_id,
            &format!("SOL_USDC-25NOV26-{k}-P"),
            "SOL_USDC",
            "SOL",
            "USDC",
            InstrumentKind::Option,
            Some(k),
            Some(OptionType::Put),
            10.0,
            0.0003,
        ));
        push_snapshot(
            &mut state,
            next_id,
            &[(put_mid - 0.10, 100.0)],
            &[(put_mid + 0.10, 100.0)],
            NOW_MS,
            1,
        );
        next_id += 1;
    }
    push_index(&mut state, "sol_usdc", 150.0, NOW_MS);

    // --- 2. Fair inverse BTC chain (spot = 90_000, F = 91_000, r_coin = 0.0) ---
    let fut_btc = 91_000.0;
    // Convex inverse call prices in BTC: C(80k)=0.160, C(90k)=0.075, C(100k)=0.030
    // Put-call parity in coin (r_coin=0): C - P = 1 - K/F => P = C - (1 - K/F)
    let strikes_btc = [(80_000.0, 0.160), (90_000.0, 0.075), (100_000.0, 0.030)];
    state.register_instrument(make_meta(
        2,
        "BTC-25NOV26",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Future,
        None,
        None,
        10.0,
        0.00035,
    ));
    push_snapshot(
        &mut state,
        2,
        &[(90_990.0, 500_000.0)],
        &[(91_010.0, 500_000.0)],
        NOW_MS,
        1,
    );

    for &(k, call_mid) in &strikes_btc {
        let put_mid = call_mid - (1.0 - k / fut_btc);
        assert!(put_mid > 0.0);
        state.register_instrument(make_meta(
            next_id,
            &format!("BTC-25NOV26-{k}-C"),
            "BTC",
            "BTC",
            "BTC",
            InstrumentKind::Option,
            Some(k),
            Some(OptionType::Call),
            0.1,
            0.0003,
        ));
        push_snapshot(
            &mut state,
            next_id,
            &[(call_mid - 0.001, 10.0)],
            &[(call_mid + 0.001, 10.0)],
            NOW_MS,
            1,
        );
        next_id += 1;

        state.register_instrument(make_meta(
            next_id,
            &format!("BTC-25NOV26-{k}-P"),
            "BTC",
            "BTC",
            "BTC",
            InstrumentKind::Option,
            Some(k),
            Some(OptionType::Put),
            0.1,
            0.0003,
        ));
        push_snapshot(
            &mut state,
            next_id,
            &[(put_mid - 0.001, 10.0)],
            &[(put_mid + 0.001, 10.0)],
            NOW_MS,
            1,
        );
        next_id += 1;
    }
    push_index(&mut state, "btc_usd", 90_000.0, NOW_MS);

    let opps = scan_all(&state, &cfg, &fees, NOW_MS);
    assert!(
        opps.is_empty(),
        "expected 0 false positives on fair chains, got: {opps:?}"
    );
}

#[test]
fn linear_usdc_box_sweeps_multilevel_l2_depth_and_quantizes_size() {
    let mut state = MarketState::new();
    let mut cfg = ArbConfig::default();
    cfg.scanner.enable_parity = false;
    cfg.scanner.enable_verticals = false;
    cfg.scanner.enable_butterflies = false;
    cfg.rates.usdc_benchmark_rate = 0.0; // exact PV = K_high - K_low = 20.0 USDC
    let fees = FeeModel::new(cfg.fees.clone());

    // SOL_USDC strikes K1=140, K2=160, spot=150, min_trade_amount=10.0 SOL.
    // Long box cost = ask(C140) - bid(P140) - bid(C160) + ask(P160)
    // Set L1: 18.0 - 4.0 - 4.0 + 8.0 = 18.0 USDC (< 20.0 payoff => ~2.00 edge pre-fee)
    // Set L2 on C140 ask: 19.0 => cost = 19.0 (< 20.0 payoff => ~1.00 edge pre-fee)
    // Set L3 on C140 ask: 20.5 => unprofitable (must stop before L3!)
    // Available C140 ask depth: 25 SOL @ L1, 20 SOL @ L2 (total 45 SOL -> quantized to 40 SOL!)
    state.register_instrument(make_meta(
        10,
        "SOL_USDC-25NOV26-140-C",
        "SOL_USDC",
        "SOL",
        "USDC",
        InstrumentKind::Option,
        Some(140.0),
        Some(OptionType::Call),
        10.0,
        0.0003,
    ));
    state.register_instrument(make_meta(
        11,
        "SOL_USDC-25NOV26-140-P",
        "SOL_USDC",
        "SOL",
        "USDC",
        InstrumentKind::Option,
        Some(140.0),
        Some(OptionType::Put),
        10.0,
        0.0003,
    ));
    state.register_instrument(make_meta(
        12,
        "SOL_USDC-25NOV26-160-C",
        "SOL_USDC",
        "SOL",
        "USDC",
        InstrumentKind::Option,
        Some(160.0),
        Some(OptionType::Call),
        10.0,
        0.0003,
    ));
    state.register_instrument(make_meta(
        13,
        "SOL_USDC-25NOV26-160-P",
        "SOL_USDC",
        "SOL",
        "USDC",
        InstrumentKind::Option,
        Some(160.0),
        Some(OptionType::Put),
        10.0,
        0.0003,
    ));

    push_snapshot(
        &mut state,
        10,
        &[(17.0, 100.0)],
        &[(18.0, 25.0), (19.0, 20.0), (20.5, 100.0)],
        NOW_MS,
        1,
    );
    push_snapshot(&mut state, 11, &[(4.0, 100.0)], &[(5.0, 100.0)], NOW_MS, 1);
    push_snapshot(&mut state, 12, &[(4.0, 100.0)], &[(5.0, 100.0)], NOW_MS, 1);
    push_snapshot(&mut state, 13, &[(7.0, 100.0)], &[(8.0, 100.0)], NOW_MS, 1);
    push_index(&mut state, "sol_usdc", 150.0, NOW_MS);

    let opps = scan_all(&state, &cfg, &fees, NOW_MS);
    assert_eq!(opps.len(), 1);
    let opp = &opps[0];
    assert_eq!(opp.strategy, StrategyKind::BoxLong);
    assert!(!opp.is_inverse);
    // 25 (L1) + 20 (L2) = 45 -> quantized down to 40.0 SOL (multiple of min_trade_amount=10.0)
    assert!((opp.executable_units - 40.0).abs() < 1e-9);
    // C140 VWAP = (25 * 18.0 + 15 * 19.0) / 40.0 = 18.375
    assert!((opp.legs[0].vwap_price - 18.375).abs() < 1e-9);
    // Fees per 1 SOL: 4 option taker legs * (0.0003 * 150 = 0.045) + 2 ITM delivery * (0.00015 * 150 = 0.0225) = 0.225 USDC
    let expected_fee_per_unit = 4.0 * (0.0003 * 150.0) + 2.0 * (0.00015 * 150.0);
    assert!((opp.total_fees_usd - 40.0 * expected_fee_per_unit).abs() < 1e-6);
    // Profit = 25 * (2.0 - 0.225) + 15 * (1.0 - 0.225) = 44.375 + 11.625 = 56.0 USDC
    let expected_profit =
        25.0 * (2.0 - expected_fee_per_unit) + 15.0 * (1.0 - expected_fee_per_unit);
    assert!((opp.total_profit_usd - expected_profit).abs() < 1e-6);
    assert!(opp.implied_rate.unwrap() > 0.5);
}

#[test]
fn inverse_btc_ratio_box_is_spot_invariant_and_detects_exact_coin_edge() {
    // Verify analytical spot invariance of inverse ratio box:
    // +1 C(K1) - 1 P(K1) - m C(K2) + m P(K2) with m = K1 / K2 pays (1 - K1/K2) coin for ALL S_T > 0!
    let k1 = 80_000.0_f64;
    let k2 = 100_000.0_f64;
    let m = k1 / k2; // 0.8
    let expected_coin_payoff = 1.0 - m; // 0.2 BTC

    for &st in &[50_000.0_f64, 80_000.0, 90_000.0, 100_000.0, 150_000.0] {
        let c1 = (1.0 - k1 / st).max(0.0);
        let p1 = (k1 / st - 1.0).max(0.0);
        let c2 = (1.0 - k2 / st).max(0.0);
        let p2 = (k2 / st - 1.0).max(0.0);
        let payoff = (c1 - p1) - m * (c2 - p2);
        assert!(
            (payoff - expected_coin_payoff).abs() < 1e-12,
            "inverse ratio box failed spot invariance at S_T={st}: got {payoff}"
        );
    }

    let mut state = MarketState::new();
    let mut cfg = ArbConfig::default();
    cfg.scanner.enable_parity = false;
    cfg.scanner.enable_verticals = false;
    cfg.scanner.enable_butterflies = false;
    cfg.rates.coin_benchmark_rate = 0.0;
    let fees = FeeModel::new(cfg.fees.clone());

    state.register_instrument(make_meta(
        10,
        "BTC-25NOV26-80000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k1),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        11,
        "BTC-25NOV26-80000-P",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k1),
        Some(OptionType::Put),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        12,
        "BTC-25NOV26-100000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k2),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        13,
        "BTC-25NOV26-100000-P",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k2),
        Some(OptionType::Put),
        0.1,
        0.0003,
    ));

    // Fair BTC option prices at F=90k: C(80k)-P(80k) = 1 - 80/90 = 0.111111, C(100k)-P(100k) = -0.111111
    // Cost = 0.111111 - 0.8 * (-0.111111) = 0.200000 BTC (exact zero arb).
    // Discount C(80k) ask by 0.01 BTC => cost = 0.19 BTC vs 0.20 BTC guaranteed payoff!
    push_snapshot(
        &mut state,
        10,
        &[(0.145, 5.0)],
        &[(0.1511111111, 2.0)],
        NOW_MS,
        1,
    );
    push_snapshot(&mut state, 11, &[(0.050, 5.0)], &[(0.055, 5.0)], NOW_MS, 1);
    push_snapshot(&mut state, 12, &[(0.030, 5.0)], &[(0.035, 5.0)], NOW_MS, 1);
    push_snapshot(
        &mut state,
        13,
        &[(0.135, 5.0)],
        &[(0.1411111111, 5.0)],
        NOW_MS,
        1,
    );
    push_index(&mut state, "btc_usd", 90_000.0, NOW_MS);

    let opps = scan_all(&state, &cfg, &fees, NOW_MS);
    assert_eq!(opps.len(), 1);
    let opp = &opps[0];
    assert_eq!(opp.strategy, StrategyKind::BoxLong);
    assert!(opp.is_inverse);
    // quantum = max(0.1, 0.1 / 0.8) = 0.125 BTC; 2.0 / 0.125 = 16 exact quanta => 2.0 BTC
    assert!((opp.executable_units - 2.0).abs() < 1e-9);
    // Fee per 1.0 unit in BTC:
    // exec: (1 + 1 + 0.8 + 0.8) * 0.0003 = 3.6 * 0.0003 = 0.00108 BTC
    // delivery (2 ITM legs weight 1 + 0.8): 1.8 * 0.00015 = 0.00027 BTC
    // total fee = 0.00135 BTC; gross edge = 0.01 BTC => net edge = 0.00865 BTC = $778.50/BTC
    let expected_net_btc = 0.01 - (3.6 * 0.0003 + 1.8 * 0.00015);
    assert!((opp.bbo_edge_usd - expected_net_btc * 90_000.0).abs() < 1e-3);
    assert!((opp.total_profit_usd - 2.0 * expected_net_btc * 90_000.0).abs() < 1e-2);
}

#[test]
fn inverse_conversion_and_reversal_respect_usd_future_notional_ratio() {
    let mut state = MarketState::new();
    let mut cfg = ArbConfig::default();
    cfg.scanner.enable_box_spreads = false;
    cfg.scanner.enable_verticals = false;
    cfg.scanner.enable_butterflies = false;
    cfg.rates.coin_benchmark_rate = 0.0;
    let fees = FeeModel::new(cfg.fees.clone());

    let strike = 90_000.0;
    state.register_instrument(make_meta(
        1,
        "BTC-25NOV26",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Future,
        None,
        None,
        10.0,
        0.00035,
    ));
    state.register_instrument(make_meta(
        2,
        "BTC-25NOV26-90000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(strike),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        3,
        "BTC-25NOV26-90000-P",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(strike),
        Some(OptionType::Put),
        0.1,
        0.0003,
    ));

    // Dated future at F=90_000 with ONLY 180_000 USD on ask -> caps conversion size at 180_000 / 90_000 = 2.0 BTC!
    // Fair parity at K=F=90k: C - P = 1 - K/F = 0.
    // Set C bid = 0.080, P ask = 0.070 => conversion (-C + P + Long 90k USD future) earns 0.010 BTC pre-fee!
    push_snapshot(
        &mut state,
        1,
        &[(89_990.0, 500_000.0)],
        &[(90_000.0, 180_000.0)],
        NOW_MS,
        1,
    );
    push_snapshot(&mut state, 2, &[(0.080, 5.0)], &[(0.085, 5.0)], NOW_MS, 1);
    push_snapshot(&mut state, 3, &[(0.065, 5.0)], &[(0.070, 5.0)], NOW_MS, 1);
    push_index(&mut state, "btc_usd", 90_000.0, NOW_MS);

    let opps = scan_all(&state, &cfg, &fees, NOW_MS);
    assert_eq!(opps.len(), 1);
    let opp = &opps[0];
    assert_eq!(opp.strategy, StrategyKind::Conversion);
    assert!(opp.is_inverse);
    assert!((opp.executable_units - 2.0).abs() < 1e-9);
    assert!((opp.legs[2].ratio - strike).abs() < 1e-9);
}

#[test]
fn verticals_and_butterflies_trigger_cleanly_on_linear_and_inverse() {
    let mut state = MarketState::new();
    let mut cfg = ArbConfig::default();
    cfg.scanner.enable_box_spreads = false;
    cfg.scanner.enable_parity = false;
    cfg.rates.usdc_benchmark_rate = 0.0;
    cfg.rates.coin_benchmark_rate = 0.0;
    let fees = FeeModel::new(cfg.fees.clone());

    // Non-uniform inverse BTC strikes: 80k, 90k, 105k (w1 = 15/25 = 0.6, w3 = 10/25 = 0.4)
    // Concave middle strike violation: bid(C90) = 0.110 > 0.6*ask(C80)=0.6*0.140 + 0.4*ask(C105)=0.4*0.030 = 0.096
    // Credit = 0.014 BTC per 1.0 unit of middle leg!
    // Also trigger Call Monotonicity violation on SOL_USDC: ask(C140) = 5.0 < bid(C160) = 6.0!
    state.register_instrument(make_meta(
        10,
        "BTC-25NOV26-80000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(80_000.0),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        11,
        "BTC-25NOV26-90000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(90_000.0),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        12,
        "BTC-25NOV26-105000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(105_000.0),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    push_snapshot(&mut state, 10, &[(0.135, 5.0)], &[(0.140, 5.0)], NOW_MS, 1);
    push_snapshot(&mut state, 11, &[(0.110, 5.0)], &[(0.115, 5.0)], NOW_MS, 1);
    push_snapshot(&mut state, 12, &[(0.025, 5.0)], &[(0.030, 5.0)], NOW_MS, 1);
    push_index(&mut state, "btc_usd", 90_000.0, NOW_MS);

    state.register_instrument(make_meta(
        20,
        "SOL_USDC-25NOV26-140-C",
        "SOL_USDC",
        "SOL",
        "USDC",
        InstrumentKind::Option,
        Some(140.0),
        Some(OptionType::Call),
        10.0,
        0.0003,
    ));
    state.register_instrument(make_meta(
        21,
        "SOL_USDC-25NOV26-160-C",
        "SOL_USDC",
        "SOL",
        "USDC",
        InstrumentKind::Option,
        Some(160.0),
        Some(OptionType::Call),
        10.0,
        0.0003,
    ));
    push_snapshot(&mut state, 20, &[(4.5, 50.0)], &[(5.0, 50.0)], NOW_MS, 1);
    push_snapshot(&mut state, 21, &[(6.0, 50.0)], &[(6.5, 50.0)], NOW_MS, 1);
    push_index(&mut state, "sol_usdc", 150.0, NOW_MS);

    let opps = scan_all(&state, &cfg, &fees, NOW_MS);
    assert!(
        opps.iter()
            .any(|o| o.strategy == StrategyKind::ButterflyCall && o.market == "BTC"),
        "expected inverse BTC ButterflyCall, got {opps:?}"
    );
    assert!(
        opps.iter()
            .any(|o| o.strategy == StrategyKind::VerticalCallMonotonicity && o.market == "SOL_USDC"),
        "expected linear SOL_USDC VerticalCallMonotonicity, got {opps:?}"
    );
}

#[test]
fn fee_cap_and_hold_to_expiry_behave_exactly() {
    let model = FeeModel::new(FeeConfig::default());
    // Deep OTM linear option (premium = 0.10 USDC, spot = 150.0):
    // base = 0.0003 * 150 = 0.045, cap = 0.125 * 0.10 = 0.0125 -> capped at 0.0125!
    assert!((model.linear_option_exec_fee(0.10, 150.0, 0.0003) - 0.0125).abs() < 1e-12);
    // Deep OTM inverse option (premium = 0.001 BTC):
    // base = 0.0003, cap = 0.125 * 0.001 = 0.000125 -> capped at 0.000125!
    assert!((model.inverse_option_exec_fee(0.001, 0.0003) - 0.000125).abs() < 1e-12);

    let no_deliv = FeeModel::new(FeeConfig {
        hold_to_expiry: false,
        ..Default::default()
    });
    assert_eq!(no_deliv.linear_option_delivery_fee(150.0, 2.0), 0.0);
    assert_eq!(no_deliv.inverse_option_delivery_fee(1.8), 0.0);
}

#[test]
fn inverse_1to1_box_carries_usd_implied_rate_and_hedges_with_width_usd_future() {
    // Verify analytical spot invariance of 1:1 inverse box + W-USD dated future:
    // +1 C(K1) - 1 P(K1) - 1 C(K2) + 1 P(K2) pays W/S_T coin (= W USD at expiry).
    // Adding Long W USD dated future @ F pays W*(1/F - 1/S_T) coin.
    // Combined coin payoff = W/F for ALL S_T > 0, so in USD terms Box_USD = (W/F)*S_0 = W*exp(-rT)!
    let s0 = 90_000.0_f64;
    let f = 91_000.0_f64;
    let k1 = 80_000.0_f64;
    let k2 = 100_000.0_f64;
    let width = k2 - k1; // 20_000 USD

    for &st in &[50_000.0_f64, 80_000.0, 90_000.0, 100_000.0, 150_000.0] {
        let c1 = (1.0 - k1 / st).max(0.0);
        let p1 = (k1 / st - 1.0).max(0.0);
        let c2 = (1.0 - k2 / st).max(0.0);
        let p2 = (k2 / st - 1.0).max(0.0);
        let opt_coin = (c1 - p1) - (c2 - p2);
        // In USD terms at expiry, 1:1 inverse box pays exactly W = K2 - K1 USD:
        assert!((opt_coin * st - width).abs() < 1e-9);
        // Hedged with Long W USD future @ F, coin payoff is exact W / F:
        let fut_coin = width * (1.0 / f - 1.0 / st);
        assert!((opt_coin + fut_coin - width / f).abs() < 1e-12);
    }

    // Verify fair 1:1 inverse box implies exact futures-carry USD rate `ln(F / S_0) / T`
    // while ratio coin box implies exact `0.0%` coin rate!
    let mut state = MarketState::new();
    let mut cfg = ArbConfig::default();
    cfg.scanner.enable_parity = false;
    cfg.scanner.enable_verticals = false;
    cfg.scanner.enable_butterflies = false;
    let fees = FeeModel::new(cfg.fees.clone());

    state.register_instrument(make_meta(
        1,
        "BTC-25NOV26",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Future,
        None,
        None,
        10.0,
        0.00035,
    ));
    state.register_instrument(make_meta(
        10,
        "BTC-25NOV26-80000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k1),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        11,
        "BTC-25NOV26-80000-P",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k1),
        Some(OptionType::Put),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        12,
        "BTC-25NOV26-100000-C",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k2),
        Some(OptionType::Call),
        0.1,
        0.0003,
    ));
    state.register_instrument(make_meta(
        13,
        "BTC-25NOV26-100000-P",
        "BTC",
        "BTC",
        "BTC",
        InstrumentKind::Option,
        Some(k2),
        Some(OptionType::Put),
        0.1,
        0.0003,
    ));

    // Fair BTC option prices at F=91_000 (r_coin=0): C(K) - P(K) = 1 - K/F
    let c1_fair = 0.170;
    let p1_fair = c1_fair - (1.0 - k1 / f);
    let c2_fair = 0.035;
    let p2_fair = c2_fair - (1.0 - k2 / f);

    // Future has 40_000 USD on ask -> caps 5-leg 1:1 box size at 40_000 / 20_000 = 2.0 BTC!
    push_snapshot(
        &mut state,
        1,
        &[(f - 5.0, 200_000.0)],
        &[(f, 40_000.0)],
        NOW_MS,
        1,
    );
    push_snapshot(
        &mut state,
        10,
        &[(c1_fair - 0.0005, 5.0)],
        &[(c1_fair + 0.0005, 5.0)],
        NOW_MS,
        1,
    );
    push_snapshot(
        &mut state,
        11,
        &[(p1_fair - 0.0005, 5.0)],
        &[(p1_fair + 0.0005, 5.0)],
        NOW_MS,
        1,
    );
    push_snapshot(
        &mut state,
        12,
        &[(c2_fair - 0.0005, 5.0)],
        &[(c2_fair + 0.0005, 5.0)],
        NOW_MS,
        1,
    );
    push_snapshot(
        &mut state,
        13,
        &[(p2_fair - 0.0005, 5.0)],
        &[(p2_fair + 0.0005, 5.0)],
        NOW_MS,
        1,
    );
    push_index(&mut state, "btc_usd", s0, NOW_MS);

    let chain = state.chains.values().next().unwrap();
    let ctx =
        box_spread_arbitrage::strategies::EvalContext::build(&state, chain, &cfg, &fees, NOW_MS)
            .unwrap();
    let t = ctx.tte_years;
    let expected_carry = (f / s0).ln() / t;
    let r_usd =
        box_spread_arbitrage::strategies::box_spread::mid_implied_usd_box_rate(&ctx, k1, k2)
            .unwrap();
    let r_coin =
        box_spread_arbitrage::strategies::box_spread::mid_implied_coin_box_rate(&ctx, k1, k2)
            .unwrap();
    assert!(
        (r_usd - expected_carry).abs() < 1e-9,
        "1:1 inverse box USD rate {r_usd} must equal futures carry {expected_carry}"
    );
    assert!(
        r_coin.abs() < 1e-9,
        "ratio inverse box coin rate {r_coin} must equal 0.0"
    );

    // Now discount C(K1) ask by 0.01 BTC => triggers 5-leg BoxInverseUsdLong capped at 2.0 BTC by W=20k future depth!
    push_snapshot(
        &mut state,
        10,
        &[(c1_fair - 0.015, 5.0)],
        &[(c1_fair - 0.010, 5.0)],
        NOW_MS,
        2,
    );
    let opps = scan_all(&state, &cfg, &fees, NOW_MS);
    let usd_box = opps
        .iter()
        .find(|o| o.strategy == StrategyKind::BoxInverseUsdLong)
        .expect("expected 5-leg BoxInverseUsdLong");
    assert!((usd_box.executable_units - 2.0).abs() < 1e-9);
    assert_eq!(usd_box.legs.len(), 5);
    assert!((usd_box.legs[4].ratio - width).abs() < 1e-9);
    assert!(usd_box.implied_rate.unwrap() > expected_carry);
}
