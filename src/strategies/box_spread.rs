use crate::book::{LegSide, LegSpec, sweep_multi_leg};
use crate::strategies::{ArbOpportunity, EvalContext, StrategyKind, fresh_leg, strike_pair};

/// Scan all box spread pairs `(K_low, K_high)` touching strike index `pos`.
pub fn scan_around_strike(
    ctx: &EvalContext<'_>,
    strikes: &[f64],
    pos: usize,
    out: &mut Vec<ArbOpportunity>,
) {
    let span = ctx.cfg.scanner.max_strike_span;
    let start = if span > 0 {
        pos.saturating_sub(span)
    } else {
        0
    };
    let end = if span > 0 {
        (pos + span + 1).min(strikes.len())
    } else {
        strikes.len()
    };

    for &k_low in &strikes[start..pos] {
        evaluate_pair(ctx, k_low, strikes[pos], out);
    }
    for &k_high in &strikes[(pos + 1)..end] {
        evaluate_pair(ctx, strikes[pos], k_high, out);
    }
}

/// Scan all pairs `(K_low, K_high)` in the expiry chain.
pub fn scan_all_pairs(ctx: &EvalContext<'_>, strikes: &[f64], out: &mut Vec<ArbOpportunity>) {
    let span = ctx.cfg.scanner.max_strike_span;
    for i in 0..strikes.len() {
        let j_end = if span > 0 {
            (i + span + 1).min(strikes.len())
        } else {
            strikes.len()
        };
        for j in (i + 1)..j_end {
            evaluate_pair(ctx, strikes[i], strikes[j], out);
        }
    }
}

/// Scan all pairs `(K_low, K_high)` for the 5-leg 1:1 inverse USD-payoff box hedged against the
/// matched-expiry dated future (dispatched on inverse dated-future ticks).
pub fn scan_all_inverse_usd_pairs(
    ctx: &EvalContext<'_>,
    strikes: &[f64],
    out: &mut Vec<ArbOpportunity>,
) {
    if !ctx.chain.is_inverse || ctx.chain.dated_future.is_none() {
        return;
    }
    let span = ctx.cfg.scanner.max_strike_span;
    for i in 0..strikes.len() {
        let j_end = if span > 0 {
            (i + span + 1).min(strikes.len())
        } else {
            strikes.len()
        };
        for j in (i + 1)..j_end {
            evaluate_inverse_usd_pair(ctx, strikes[i], strikes[j], out);
        }
    }
}

/// Evaluate box spread opportunities on `(k_low, k_high)`:
///
/// 1. **Linear USDC options (`!is_inverse`)**:
///    - Equal 1:1 underlying units across all 4 legs.
///    - Guaranteed expiry payoff = `W = K_high - K_low` USDC (exact for any terminal spot `S_T`).
///    - Because Deribit pays 0% on USDC margin equity and short proceeds are offset in
///      `equity = cash + OptionsMtM`, linear USDC mark prices trade undiscounted (`r ≈ 0`).
/// 2. **Inverse coin-settled options (`is_inverse`, BTC/ETH)** — TWO distinct box structures:
///    - **Coin-Numeraire Ratio Box (`BoxLong` / `BoxShort`, 4 legs):**
///      `1.0` unit at `K_low`, `m = K_low / K_high` units at `K_high`.
///      Terminal payoff = `(1 - K_low/S_T) - (K_low/K_high)*(1 - K_high/S_T) = 1 - K_low/K_high`
///      coin (spot-invariant coin bond; measures pure coin lending rate `r_coin ≈ 0%`).
///    - **USD-Numeraire 1:1 Inverse Box + Netted `W`-USD Future (`BoxInverseUsdLong` / `BoxInverseUsdShort`, 5 legs):**
///      Equal 1:1 units across all 4 option legs yields `W / S_T` coin at expiry = exact `W` USD
///      (`(W/S_T) * S_T = W`), so its fair coin price is `W / F` and its USD value today is
///      `(W / F) * S_0 = W * exp(-r_usd * T)` carrying the positive USD/futures-carry implied rate!
///      Hedging with `W = K_high - K_low` USD of the matched-expiry inverse dated future locks in
///      exact coin payoff `W / F` with zero `S_T` risk (and much smaller future notional than
///      trading separate `Reversal(K_low) + Conversion(K_high)` which requires `K_low + K_high` USD).
pub fn evaluate_pair(
    ctx: &EvalContext<'_>,
    k_low: f64,
    k_high: f64,
    out: &mut Vec<ArbOpportunity>,
) {
    evaluate_native_pair(ctx, k_low, k_high, out);
    if ctx.chain.is_inverse {
        evaluate_inverse_usd_pair(ctx, k_low, k_high, out);
    }
}

fn evaluate_native_pair(
    ctx: &EvalContext<'_>,
    k_low: f64,
    k_high: f64,
    out: &mut Vec<ArbOpportunity>,
) {
    if k_low <= 0.0 || k_high <= k_low {
        return;
    }
    let Some(p_low) = strike_pair(ctx.chain, k_low) else {
        return;
    };
    let Some(p_high) = strike_pair(ctx.chain, k_high) else {
        return;
    };
    let (Some(cl_id), Some(pl_id), Some(ch_id), Some(ph_id)) =
        (p_low.call, p_low.put, p_high.call, p_high.put)
    else {
        return;
    };

    let (
        Some((cl_b, cl_t, cl_min)),
        Some((pl_b, pl_t, pl_min)),
        Some((ch_b, ch_t, ch_min)),
        Some((ph_b, ph_t, ph_min)),
    ) = (
        fresh_leg(ctx, cl_id),
        fresh_leg(ctx, pl_id),
        fresh_leg(ctx, ch_id),
        fresh_leg(ctx, ph_id),
    )
    else {
        return;
    };

    let is_inverse = ctx.chain.is_inverse;
    let m_high = if is_inverse { k_low / k_high } else { 1.0 };
    let discount = (-ctx.benchmark_rate * ctx.tte_years).exp();
    let payoff_native = if is_inverse {
        1.0 - (k_low / k_high)
    } else {
        k_high - k_low
    };
    let pv_native = discount * payoff_native;
    let unit_to_usd = if is_inverse { ctx.spot } else { 1.0 };

    // Minimum discrete package quantum across all 4 legs.
    let quantum = cl_min.max(pl_min).max(ch_min / m_high).max(ph_min / m_high);
    let max_units = ctx.max_units();

    // Delivery fee bound for the 2 ITM legs at expiry (worst-case weight: 1 + m_high).
    let deliv_native = if is_inverse {
        ctx.fees.inverse_option_delivery_fee(1.0 + m_high)
    } else {
        ctx.fees.linear_option_delivery_fee(ctx.spot, 2.0)
    };

    // --- 1. LONG BOX: +C(K_low) -P(K_low) -m*C(K_high) +m*P(K_high) ---
    let long_legs = [
        LegSpec {
            book: cl_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: pl_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: ch_b,
            side: LegSide::Sell,
            ratio: m_high,
        },
        LegSpec {
            book: ph_b,
            side: LegSide::Buy,
            ratio: m_high,
        },
    ];

    if let Some(sw) = sweep_multi_leg(&long_legs, quantum, max_units, |px| {
        let cost_pre = (px[0] - px[1]) - m_high * (px[2] - px[3]);
        let exec_fee = if is_inverse {
            ctx.fees.inverse_option_exec_fee(px[0], cl_t)
                + ctx.fees.inverse_option_exec_fee(px[1], pl_t)
                + m_high
                    * (ctx.fees.inverse_option_exec_fee(px[2], ch_t)
                        + ctx.fees.inverse_option_exec_fee(px[3], ph_t))
        } else {
            ctx.fees.linear_option_exec_fee(px[0], ctx.spot, cl_t)
                + ctx.fees.linear_option_exec_fee(px[1], ctx.spot, pl_t)
                + ctx.fees.linear_option_exec_fee(px[2], ctx.spot, ch_t)
                + ctx.fees.linear_option_exec_fee(px[3], ctx.spot, ph_t)
        };
        let fee_native = exec_fee + deliv_native;
        let edge_native = pv_native - (cost_pre + fee_native);
        (edge_native * unit_to_usd, fee_native * unit_to_usd)
    }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
    {
        let vwap_cost_pre = (sw.vwap_prices[0] - sw.vwap_prices[1])
            - m_high * (sw.vwap_prices[2] - sw.vwap_prices[3]);
        let vwap_fee_native = (sw.total_fees_usd / sw.total_units) / unit_to_usd;
        let cost_net_native = vwap_cost_pre + vwap_fee_native;
        let implied_rate = if cost_net_native > 0.0 && payoff_native > 0.0 && ctx.tte_years > 0.0 {
            Some(-(cost_net_native / payoff_native).ln() / ctx.tte_years)
        } else {
            None
        };

        out.push(ArbOpportunity {
            strategy: StrategyKind::BoxLong,
            market: ctx.chain.market.to_string(),
            is_inverse,
            expiration_ts_ms: ctx.chain.expiration_ts_ms,
            detected_ts_ms: ctx.now_ms,
            tte_years: ctx.tte_years,
            spot_index: ctx.spot,
            strikes: vec![k_low, k_high],
            bbo_edge_usd: sw.bbo_edge_usd,
            bbo_edge_bps: (sw.bbo_edge_usd / ctx.spot) * 10_000.0,
            executable_units: sw.total_units,
            total_profit_usd: sw.total_profit_usd,
            total_fees_usd: sw.total_fees_usd,
            levels_swept: sw.levels_swept,
            implied_rate,
            futures_carry_rate: ctx.futures_carry,
            perp_funding_ann: ctx.perp_funding_ann,
            legs: vec![
                ctx.leg_fill(
                    cl_id,
                    LegSide::Buy,
                    1.0,
                    sw.bbo_prices[0],
                    sw.vwap_prices[0],
                ),
                ctx.leg_fill(
                    pl_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[1],
                    sw.vwap_prices[1],
                ),
                ctx.leg_fill(
                    ch_id,
                    LegSide::Sell,
                    m_high,
                    sw.bbo_prices[2],
                    sw.vwap_prices[2],
                ),
                ctx.leg_fill(
                    ph_id,
                    LegSide::Buy,
                    m_high,
                    sw.bbo_prices[3],
                    sw.vwap_prices[3],
                ),
            ],
        });
    }

    // --- 2. SHORT BOX: -C(K_low) +P(K_low) +m*C(K_high) -m*P(K_high) ---
    let short_legs = [
        LegSpec {
            book: cl_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: pl_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: ch_b,
            side: LegSide::Buy,
            ratio: m_high,
        },
        LegSpec {
            book: ph_b,
            side: LegSide::Sell,
            ratio: m_high,
        },
    ];

    if let Some(sw) = sweep_multi_leg(&short_legs, quantum, max_units, |px| {
        let proceeds_pre = (px[0] - px[1]) - m_high * (px[2] - px[3]);
        let exec_fee = if is_inverse {
            ctx.fees.inverse_option_exec_fee(px[0], cl_t)
                + ctx.fees.inverse_option_exec_fee(px[1], pl_t)
                + m_high
                    * (ctx.fees.inverse_option_exec_fee(px[2], ch_t)
                        + ctx.fees.inverse_option_exec_fee(px[3], ph_t))
        } else {
            ctx.fees.linear_option_exec_fee(px[0], ctx.spot, cl_t)
                + ctx.fees.linear_option_exec_fee(px[1], ctx.spot, pl_t)
                + ctx.fees.linear_option_exec_fee(px[2], ctx.spot, ch_t)
                + ctx.fees.linear_option_exec_fee(px[3], ctx.spot, ph_t)
        };
        let fee_native = exec_fee + deliv_native;
        let edge_native = (proceeds_pre - fee_native) - pv_native;
        (edge_native * unit_to_usd, fee_native * unit_to_usd)
    }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
    {
        let vwap_proc_pre = (sw.vwap_prices[0] - sw.vwap_prices[1])
            - m_high * (sw.vwap_prices[2] - sw.vwap_prices[3]);
        let vwap_fee_native = (sw.total_fees_usd / sw.total_units) / unit_to_usd;
        let proc_net_native = vwap_proc_pre - vwap_fee_native;
        let implied_rate = if proc_net_native > 0.0 && payoff_native > 0.0 && ctx.tte_years > 0.0 {
            Some(-(proc_net_native / payoff_native).ln() / ctx.tte_years)
        } else {
            None
        };

        out.push(ArbOpportunity {
            strategy: StrategyKind::BoxShort,
            market: ctx.chain.market.to_string(),
            is_inverse,
            expiration_ts_ms: ctx.chain.expiration_ts_ms,
            detected_ts_ms: ctx.now_ms,
            tte_years: ctx.tte_years,
            spot_index: ctx.spot,
            strikes: vec![k_low, k_high],
            bbo_edge_usd: sw.bbo_edge_usd,
            bbo_edge_bps: (sw.bbo_edge_usd / ctx.spot) * 10_000.0,
            executable_units: sw.total_units,
            total_profit_usd: sw.total_profit_usd,
            total_fees_usd: sw.total_fees_usd,
            levels_swept: sw.levels_swept,
            implied_rate,
            futures_carry_rate: ctx.futures_carry,
            perp_funding_ann: ctx.perp_funding_ann,
            legs: vec![
                ctx.leg_fill(
                    cl_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[0],
                    sw.vwap_prices[0],
                ),
                ctx.leg_fill(
                    pl_id,
                    LegSide::Buy,
                    1.0,
                    sw.bbo_prices[1],
                    sw.vwap_prices[1],
                ),
                ctx.leg_fill(
                    ch_id,
                    LegSide::Buy,
                    m_high,
                    sw.bbo_prices[2],
                    sw.vwap_prices[2],
                ),
                ctx.leg_fill(
                    ph_id,
                    LegSide::Sell,
                    m_high,
                    sw.bbo_prices[3],
                    sw.vwap_prices[3],
                ),
            ],
        });
    }
}

/// Evaluate the 5-leg 1:1 inverse box (`+C(K1) -P(K1) -C(K2) +P(K2)`) hedged with `W = K2 - K1`
/// USD notional of the matched-expiry inverse dated future.
///
/// Terminal coin payoff:
/// - Options (1:1): `(1 - K1/S_T) - (1 - K2/S_T) = W / S_T` coin (`= W` USD at expiry!)
/// - Long `W` USD future @ `F_ask`: `W * (1/F_ask - 1/S_T)` coin
/// - Combined coin payoff: `W / F_ask` coin (exact zero `S_T` risk!)
///
/// In USD terms today: `Box_USD = Box_coin * S_0 ≈ (W / F) * S_0 = W * exp(-r_usd * T)`.
pub fn evaluate_inverse_usd_pair(
    ctx: &EvalContext<'_>,
    k_low: f64,
    k_high: f64,
    out: &mut Vec<ArbOpportunity>,
) {
    if !ctx.chain.is_inverse || k_low <= 0.0 || k_high <= k_low {
        return;
    }
    let Some(fut_id) = ctx.chain.dated_future else {
        return;
    };
    let (Some(p_low), Some(p_high)) = (
        strike_pair(ctx.chain, k_low),
        strike_pair(ctx.chain, k_high),
    ) else {
        return;
    };
    let (Some(cl_id), Some(pl_id), Some(ch_id), Some(ph_id)) =
        (p_low.call, p_low.put, p_high.call, p_high.put)
    else {
        return;
    };
    let (
        Some((cl_b, cl_t, cl_min)),
        Some((pl_b, pl_t, pl_min)),
        Some((ch_b, ch_t, ch_min)),
        Some((ph_b, ph_t, ph_min)),
        Some((f_b, f_t, f_min)),
    ) = (
        fresh_leg(ctx, cl_id),
        fresh_leg(ctx, pl_id),
        fresh_leg(ctx, ch_id),
        fresh_leg(ctx, ph_id),
        fresh_leg(ctx, fut_id),
    )
    else {
        return;
    };

    let width = k_high - k_low;
    let discount_coin = (-ctx.benchmark_rate * ctx.tte_years).exp();
    let quantum = cl_min
        .max(pl_min)
        .max(ch_min)
        .max(ph_min)
        .max(f_min / width);
    let max_units = ctx.max_units();
    // Exactly 2 of the 4 equal-weight option legs finish ITM at expiry.
    let deliv_coin = ctx.fees.inverse_option_delivery_fee(2.0);

    // --- 1. LONG 1:1 INVERSE BOX + LONG W USD FUTURE ---
    let long_legs = [
        LegSpec {
            book: cl_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: pl_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: ch_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: ph_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: f_b,
            side: LegSide::Buy,
            ratio: width,
        },
    ];

    if let Some(sw) = sweep_multi_leg(&long_legs, quantum, max_units, |px| {
        let f_ask = px[4];
        let cost_pre = (px[0] - px[1]) - (px[2] - px[3]);
        let opt_fee = ctx.fees.inverse_option_exec_fee(px[0], cl_t)
            + ctx.fees.inverse_option_exec_fee(px[1], pl_t)
            + ctx.fees.inverse_option_exec_fee(px[2], ch_t)
            + ctx.fees.inverse_option_exec_fee(px[3], ph_t)
            + deliv_coin;
        let fut_fee = ctx.fees.inverse_future_exec_fee(width, f_ask, f_t);
        let total_fee = opt_fee + fut_fee;
        let pv_coin = discount_coin * (width / f_ask);
        let edge_coin = pv_coin - (cost_pre + total_fee);
        (edge_coin * ctx.spot, total_fee * ctx.spot)
    }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
    {
        let vwap_cost_pre =
            (sw.vwap_prices[0] - sw.vwap_prices[1]) - (sw.vwap_prices[2] - sw.vwap_prices[3]);
        let vwap_opt_fee = ctx.fees.inverse_option_exec_fee(sw.vwap_prices[0], cl_t)
            + ctx.fees.inverse_option_exec_fee(sw.vwap_prices[1], pl_t)
            + ctx.fees.inverse_option_exec_fee(sw.vwap_prices[2], ch_t)
            + ctx.fees.inverse_option_exec_fee(sw.vwap_prices[3], ph_t)
            + deliv_coin;
        let usd_cost_net = (vwap_cost_pre + vwap_opt_fee) * ctx.spot;
        let implied_rate = if usd_cost_net > 0.0 && ctx.tte_years > 0.0 {
            Some(-(usd_cost_net / width).ln() / ctx.tte_years)
        } else {
            None
        };

        out.push(ArbOpportunity {
            strategy: StrategyKind::BoxInverseUsdLong,
            market: ctx.chain.market.to_string(),
            is_inverse: true,
            expiration_ts_ms: ctx.chain.expiration_ts_ms,
            detected_ts_ms: ctx.now_ms,
            tte_years: ctx.tte_years,
            spot_index: ctx.spot,
            strikes: vec![k_low, k_high],
            bbo_edge_usd: sw.bbo_edge_usd,
            bbo_edge_bps: (sw.bbo_edge_usd / ctx.spot) * 10_000.0,
            executable_units: sw.total_units,
            total_profit_usd: sw.total_profit_usd,
            total_fees_usd: sw.total_fees_usd,
            levels_swept: sw.levels_swept,
            implied_rate,
            futures_carry_rate: ctx.futures_carry,
            perp_funding_ann: ctx.perp_funding_ann,
            legs: vec![
                ctx.leg_fill(
                    cl_id,
                    LegSide::Buy,
                    1.0,
                    sw.bbo_prices[0],
                    sw.vwap_prices[0],
                ),
                ctx.leg_fill(
                    pl_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[1],
                    sw.vwap_prices[1],
                ),
                ctx.leg_fill(
                    ch_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[2],
                    sw.vwap_prices[2],
                ),
                ctx.leg_fill(
                    ph_id,
                    LegSide::Buy,
                    1.0,
                    sw.bbo_prices[3],
                    sw.vwap_prices[3],
                ),
                ctx.leg_fill(
                    fut_id,
                    LegSide::Buy,
                    width,
                    sw.bbo_prices[4],
                    sw.vwap_prices[4],
                ),
            ],
        });
    }

    // --- 2. SHORT 1:1 INVERSE BOX + SHORT W USD FUTURE ---
    let short_legs = [
        LegSpec {
            book: cl_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: pl_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: ch_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: ph_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: f_b,
            side: LegSide::Sell,
            ratio: width,
        },
    ];

    if let Some(sw) = sweep_multi_leg(&short_legs, quantum, max_units, |px| {
        let f_bid = px[4];
        let proc_pre = (px[0] - px[1]) - (px[2] - px[3]);
        let opt_fee = ctx.fees.inverse_option_exec_fee(px[0], cl_t)
            + ctx.fees.inverse_option_exec_fee(px[1], pl_t)
            + ctx.fees.inverse_option_exec_fee(px[2], ch_t)
            + ctx.fees.inverse_option_exec_fee(px[3], ph_t)
            + deliv_coin;
        let fut_fee = ctx.fees.inverse_future_exec_fee(width, f_bid, f_t);
        let total_fee = opt_fee + fut_fee;
        let pv_coin = discount_coin * (width / f_bid);
        let edge_coin = (proc_pre - total_fee) - pv_coin;
        (edge_coin * ctx.spot, total_fee * ctx.spot)
    }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
    {
        let vwap_proc_pre =
            (sw.vwap_prices[0] - sw.vwap_prices[1]) - (sw.vwap_prices[2] - sw.vwap_prices[3]);
        let vwap_opt_fee = ctx.fees.inverse_option_exec_fee(sw.vwap_prices[0], cl_t)
            + ctx.fees.inverse_option_exec_fee(sw.vwap_prices[1], pl_t)
            + ctx.fees.inverse_option_exec_fee(sw.vwap_prices[2], ch_t)
            + ctx.fees.inverse_option_exec_fee(sw.vwap_prices[3], ph_t)
            + deliv_coin;
        let usd_proc_net = (vwap_proc_pre - vwap_opt_fee) * ctx.spot;
        let implied_rate = if usd_proc_net > 0.0 && ctx.tte_years > 0.0 {
            Some(-(usd_proc_net / width).ln() / ctx.tte_years)
        } else {
            None
        };

        out.push(ArbOpportunity {
            strategy: StrategyKind::BoxInverseUsdShort,
            market: ctx.chain.market.to_string(),
            is_inverse: true,
            expiration_ts_ms: ctx.chain.expiration_ts_ms,
            detected_ts_ms: ctx.now_ms,
            tte_years: ctx.tte_years,
            spot_index: ctx.spot,
            strikes: vec![k_low, k_high],
            bbo_edge_usd: sw.bbo_edge_usd,
            bbo_edge_bps: (sw.bbo_edge_usd / ctx.spot) * 10_000.0,
            executable_units: sw.total_units,
            total_profit_usd: sw.total_profit_usd,
            total_fees_usd: sw.total_fees_usd,
            levels_swept: sw.levels_swept,
            implied_rate,
            futures_carry_rate: ctx.futures_carry,
            perp_funding_ann: ctx.perp_funding_ann,
            legs: vec![
                ctx.leg_fill(
                    cl_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[0],
                    sw.vwap_prices[0],
                ),
                ctx.leg_fill(
                    pl_id,
                    LegSide::Buy,
                    1.0,
                    sw.bbo_prices[1],
                    sw.vwap_prices[1],
                ),
                ctx.leg_fill(
                    ch_id,
                    LegSide::Buy,
                    1.0,
                    sw.bbo_prices[2],
                    sw.vwap_prices[2],
                ),
                ctx.leg_fill(
                    ph_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[3],
                    sw.vwap_prices[3],
                ),
                ctx.leg_fill(
                    fut_id,
                    LegSide::Sell,
                    width,
                    sw.bbo_prices[4],
                    sw.vwap_prices[4],
                ),
            ],
        });
    }
}

/// Compute mid-market implied USD financing rate from a 1:1 box spread:
/// - **Linear USDC (`!is_inverse`)**: `-(mid_cost_usdc / W).ln() / T` (undiscounted on Deribit -> `~0%`).
/// - **Inverse coin (`is_inverse`)**: the 1:1 inverse box pays `W / S_T` coin (`= W` USD at expiry),
///   so its coin cost is `~W / F` and its USD cost today is `mid_cost_coin * S_0 ≈ W * (S_0 / F) = W * exp(-r_usd * T)`.
///   Hence `r_usd = -(mid_cost_coin * S_0 / W).ln() / T = ln(F_box / S_0) / T`!
pub fn mid_implied_usd_box_rate(ctx: &EvalContext<'_>, k_low: f64, k_high: f64) -> Option<f64> {
    if k_low <= 0.0 || k_high <= k_low || ctx.tte_years <= 0.0 || ctx.spot <= 0.0 {
        return None;
    }
    let p_low = strike_pair(ctx.chain, k_low)?;
    let p_high = strike_pair(ctx.chain, k_high)?;
    let (cl_id, pl_id, ch_id, ph_id) = (p_low.call?, p_low.put?, p_high.call?, p_high.put?);

    let mid = |id| -> Option<f64> {
        let (b, _) = ctx.state.best_bid(id)?;
        let (a, _) = ctx.state.best_ask(id)?;
        if b > 0.0 && a > 0.0 {
            Some(0.5 * (b + a))
        } else {
            None
        }
    };

    let (cl, pl, ch, ph) = (mid(cl_id)?, mid(pl_id)?, mid(ch_id)?, mid(ph_id)?);
    let mid_cost_native = (cl - pl) - (ch - ph);
    if mid_cost_native <= 0.0 {
        return None;
    }
    let mid_cost_usd = if ctx.chain.is_inverse {
        mid_cost_native * ctx.spot
    } else {
        mid_cost_native
    };
    let width = k_high - k_low;
    Some(-(mid_cost_usd / width).ln() / ctx.tte_years)
}

/// Compute mid-market implied coin-numeraire box rate from the inverse ratio box (`1 : K_low/K_high`),
/// whose constant terminal payoff is `1 - K_low/K_high` coin (`None` for linear USDC chains).
pub fn mid_implied_coin_box_rate(ctx: &EvalContext<'_>, k_low: f64, k_high: f64) -> Option<f64> {
    if !ctx.chain.is_inverse || k_low <= 0.0 || k_high <= k_low || ctx.tte_years <= 0.0 {
        return None;
    }
    let p_low = strike_pair(ctx.chain, k_low)?;
    let p_high = strike_pair(ctx.chain, k_high)?;
    let (cl_id, pl_id, ch_id, ph_id) = (p_low.call?, p_low.put?, p_high.call?, p_high.put?);

    let mid = |id| -> Option<f64> {
        let (b, _) = ctx.state.best_bid(id)?;
        let (a, _) = ctx.state.best_ask(id)?;
        if b > 0.0 && a > 0.0 {
            Some(0.5 * (b + a))
        } else {
            None
        }
    };

    let (cl, pl, ch, ph) = (mid(cl_id)?, mid(pl_id)?, mid(ch_id)?, mid(ph_id)?);
    let m_high = k_low / k_high;
    let payoff_coin = 1.0 - m_high;
    let mid_cost_coin = (cl - pl) - m_high * (ch - ph);
    if mid_cost_coin <= 0.0 {
        return None;
    }
    Some(-(mid_cost_coin / payoff_coin).ln() / ctx.tte_years)
}
