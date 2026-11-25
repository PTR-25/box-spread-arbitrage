use crate::book::{LegSide, LegSpec, sweep_multi_leg};
use crate::strategies::{ArbOpportunity, EvalContext, StrategyKind, fresh_leg, strike_pair};

/// Scan vertical spread monotonicity & payoff-cap violations touching strike index `pos`.
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

/// Evaluate static vertical spread no-arbitrage bounds for `k_low < k_high`:
///
/// 1. **Call Monotonicity (`C(K_low) >= C(K_high)`):**
///    Buy `C(K_low)` + Sell `C(K_high)` has guaranteed payoff `>= 0` for all `S_T` (on both linear
///    and inverse options!). If `bid(C_high) - ask(C_low) - fees > 0`, it yields instant free cash.
/// 2. **Put Monotonicity (`P(K_high) >= P(K_low)`):**
///    Buy `P(K_high)` + Sell `P(K_low)` has guaranteed payoff `>= 0` for all `S_T`.
///    If `bid(P_low) - ask(P_high) - fees > 0`, it yields instant free cash.
/// 3. **Call Spread Payoff Cap:**
///    - Linear: `C(K_low) - C(K_high) <= exp(-rT) * (K_high - K_low)`
///    - Inverse ratio (`1*C(K_low) - m*C(K_high)`, `m = K_low/K_high`):
///      max terminal payoff in coin is `1 - K_low/K_high` (as `S_T -> inf`).
/// 4. **Put Spread Payoff Cap:**
///    - Linear: `P(K_high) - P(K_low) <= exp(-rT) * (K_high - K_low)`
///      (Note: for inverse puts, `(K_high - K_low)/S_T` is unbounded as `S_T -> 0`, so only linear puts have a finite cap.)
pub fn evaluate_pair(
    ctx: &EvalContext<'_>,
    k_low: f64,
    k_high: f64,
    out: &mut Vec<ArbOpportunity>,
) {
    if k_low <= 0.0 || k_high <= k_low {
        return;
    }
    let (Some(p_low), Some(p_high)) = (
        strike_pair(ctx.chain, k_low),
        strike_pair(ctx.chain, k_high),
    ) else {
        return;
    };

    let is_inverse = ctx.chain.is_inverse;
    let unit_to_usd = if is_inverse { ctx.spot } else { 1.0 };
    let discount = (-ctx.benchmark_rate * ctx.tte_years).exp();
    let max_units = ctx.max_units();

    // --- Calls: monotonicity & payoff cap ---
    if let (Some(cl_id), Some(ch_id)) = (p_low.call, p_high.call)
        && let (Some((cl_b, cl_t, cl_min)), Some((ch_b, ch_t, ch_min))) =
            (fresh_leg(ctx, cl_id), fresh_leg(ctx, ch_id))
    {
        let quantum = cl_min.max(ch_min);
        let deliv_2 = if is_inverse {
            ctx.fees.inverse_option_delivery_fee(2.0)
        } else {
            ctx.fees.linear_option_delivery_fee(ctx.spot, 2.0)
        };

        // 1) Call monotonicity: +C(K_low) -C(K_high) for positive upfront credit
        let mono_legs = [
            LegSpec {
                book: cl_b,
                side: LegSide::Buy,
                ratio: 1.0,
            },
            LegSpec {
                book: ch_b,
                side: LegSide::Sell,
                ratio: 1.0,
            },
        ];
        if let Some(sw) = sweep_multi_leg(&mono_legs, quantum, max_units, |px| {
            let exec = if is_inverse {
                ctx.fees.inverse_option_exec_fee(px[0], cl_t)
                    + ctx.fees.inverse_option_exec_fee(px[1], ch_t)
            } else {
                ctx.fees.linear_option_exec_fee(px[0], ctx.spot, cl_t)
                    + ctx.fees.linear_option_exec_fee(px[1], ctx.spot, ch_t)
            };
            let fee = exec + deliv_2;
            let edge = (px[1] - px[0]) - fee;
            (edge * unit_to_usd, fee * unit_to_usd)
        }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
        {
            out.push(ArbOpportunity {
                strategy: StrategyKind::VerticalCallMonotonicity,
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
                implied_rate: None,
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
                        ch_id,
                        LegSide::Sell,
                        1.0,
                        sw.bbo_prices[1],
                        sw.vwap_prices[1],
                    ),
                ],
            });
        }

        // 2) Call spread payoff cap: Sell C(K_low) + Buy m*C(K_high) for more than max discounted liability
        let m_high = if is_inverse { k_low / k_high } else { 1.0 };
        let max_payoff = if is_inverse {
            1.0 - (k_low / k_high)
        } else {
            k_high - k_low
        };
        let pv_cap = discount * max_payoff;
        let cap_quantum = cl_min.max(ch_min / m_high);
        let cap_deliv = if is_inverse {
            ctx.fees.inverse_option_delivery_fee(1.0 + m_high)
        } else {
            deliv_2
        };

        let cap_legs = [
            LegSpec {
                book: cl_b,
                side: LegSide::Sell,
                ratio: 1.0,
            },
            LegSpec {
                book: ch_b,
                side: LegSide::Buy,
                ratio: m_high,
            },
        ];
        if let Some(sw) = sweep_multi_leg(&cap_legs, cap_quantum, max_units, |px| {
            let exec = if is_inverse {
                ctx.fees.inverse_option_exec_fee(px[0], cl_t)
                    + m_high * ctx.fees.inverse_option_exec_fee(px[1], ch_t)
            } else {
                ctx.fees.linear_option_exec_fee(px[0], ctx.spot, cl_t)
                    + ctx.fees.linear_option_exec_fee(px[1], ctx.spot, ch_t)
            };
            let fee = exec + cap_deliv;
            let edge = (px[0] - m_high * px[1]) - fee - pv_cap;
            (edge * unit_to_usd, fee * unit_to_usd)
        }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
        {
            out.push(ArbOpportunity {
                strategy: StrategyKind::VerticalCallCap,
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
                implied_rate: None,
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
                        ch_id,
                        LegSide::Buy,
                        m_high,
                        sw.bbo_prices[1],
                        sw.vwap_prices[1],
                    ),
                ],
            });
        }
    }

    // --- Puts: monotonicity & (linear) payoff cap ---
    if let (Some(pl_id), Some(ph_id)) = (p_low.put, p_high.put)
        && let (Some((pl_b, pl_t, pl_min)), Some((ph_b, ph_t, ph_min))) =
            (fresh_leg(ctx, pl_id), fresh_leg(ctx, ph_id))
    {
        let quantum = pl_min.max(ph_min);
        let deliv_2 = if is_inverse {
            ctx.fees.inverse_option_delivery_fee(2.0)
        } else {
            ctx.fees.linear_option_delivery_fee(ctx.spot, 2.0)
        };

        // 3) Put monotonicity: +P(K_high) -P(K_low) for positive upfront credit
        let mono_legs = [
            LegSpec {
                book: ph_b,
                side: LegSide::Buy,
                ratio: 1.0,
            },
            LegSpec {
                book: pl_b,
                side: LegSide::Sell,
                ratio: 1.0,
            },
        ];
        if let Some(sw) = sweep_multi_leg(&mono_legs, quantum, max_units, |px| {
            let exec = if is_inverse {
                ctx.fees.inverse_option_exec_fee(px[0], ph_t)
                    + ctx.fees.inverse_option_exec_fee(px[1], pl_t)
            } else {
                ctx.fees.linear_option_exec_fee(px[0], ctx.spot, ph_t)
                    + ctx.fees.linear_option_exec_fee(px[1], ctx.spot, pl_t)
            };
            let fee = exec + deliv_2;
            let edge = (px[1] - px[0]) - fee;
            (edge * unit_to_usd, fee * unit_to_usd)
        }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
        {
            out.push(ArbOpportunity {
                strategy: StrategyKind::VerticalPutMonotonicity,
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
                implied_rate: None,
                futures_carry_rate: ctx.futures_carry,
                perp_funding_ann: ctx.perp_funding_ann,
                legs: vec![
                    ctx.leg_fill(
                        ph_id,
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
                ],
            });
        }

        // 4) Linear Put spread payoff cap: Sell P(K_high) + Buy P(K_low) for > exp(-rT)*(K_high - K_low)
        if !is_inverse {
            let pv_cap = discount * (k_high - k_low);
            let cap_legs = [
                LegSpec {
                    book: ph_b,
                    side: LegSide::Sell,
                    ratio: 1.0,
                },
                LegSpec {
                    book: pl_b,
                    side: LegSide::Buy,
                    ratio: 1.0,
                },
            ];
            if let Some(sw) = sweep_multi_leg(&cap_legs, quantum, max_units, |px| {
                let exec = ctx.fees.linear_option_exec_fee(px[0], ctx.spot, ph_t)
                    + ctx.fees.linear_option_exec_fee(px[1], ctx.spot, pl_t);
                let fee = exec + deliv_2;
                let edge = (px[0] - px[1]) - fee - pv_cap;
                (edge, fee)
            }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
            {
                out.push(ArbOpportunity {
                    strategy: StrategyKind::VerticalPutCap,
                    market: ctx.chain.market.to_string(),
                    is_inverse: false,
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
                    implied_rate: None,
                    futures_carry_rate: ctx.futures_carry,
                    perp_funding_ann: ctx.perp_funding_ann,
                    legs: vec![
                        ctx.leg_fill(
                            ph_id,
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
                    ],
                });
            }
        }
    }
}
