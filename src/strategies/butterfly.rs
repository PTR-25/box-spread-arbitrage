use deribit_tick_collector::model::InstrumentId;

use crate::book::{LegSide, LegSpec, sweep_multi_leg};
use crate::strategies::{ArbOpportunity, EvalContext, StrategyKind, fresh_leg, strike_pair};

/// Scan the at-most-3 adjacent strike triples `(K_{i-1}, K_i, K_{i+1})` containing `pos` (O(1)!).
pub fn scan_around_strike(
    ctx: &EvalContext<'_>,
    strikes: &[f64],
    pos: usize,
    out: &mut Vec<ArbOpportunity>,
) {
    if strikes.len() < 3 {
        return;
    }
    if pos >= 2 {
        evaluate_triple(ctx, strikes[pos - 2], strikes[pos - 1], strikes[pos], out);
    }
    if pos >= 1 && pos + 1 < strikes.len() {
        evaluate_triple(ctx, strikes[pos - 1], strikes[pos], strikes[pos + 1], out);
    }
    if pos + 2 < strikes.len() {
        evaluate_triple(ctx, strikes[pos], strikes[pos + 1], strikes[pos + 2], out);
    }
}

pub fn scan_all_triples(ctx: &EvalContext<'_>, strikes: &[f64], out: &mut Vec<ArbOpportunity>) {
    if strikes.len() < 3 {
        return;
    }
    for i in 1..(strikes.len() - 1) {
        evaluate_triple(ctx, strikes[i - 1], strikes[i], strikes[i + 1], out);
    }
}

/// Evaluate Call and Put Butterfly convexity violations (risk-neutral density non-negativity):
///
/// For `K1 < K2 < K3`, define convex weights:
///   `w1 = (K3 - K2) / (K3 - K1)`,  `w3 = (K2 - K1) / (K3 - K1)`  (`w1 + w3 = 1`, `w1*K1 + w3*K3 = K2`).
///
/// Because both linear payoffs `max(S_T - K, 0)` / `max(K - S_T, 0)` AND inverse coin payoffs
/// `max(1 - K/S_T, 0)` / `max(K/S_T - 1, 0)` are convex functions of `K`, the long butterfly
///   `+w1 * O(K1) - 1.0 * O(K2) + w3 * O(K3)`
/// has guaranteed terminal payoff `>= 0` for ALL `S_T > 0`!
///
/// Therefore, whenever `bid(O(K2)) - w1 * ask(O(K1)) - w3 * ask(O(K3)) - fees > 0`, executing the
/// long butterfly locks in strictly positive risk-free cash upfront with zero expiry downside.
pub fn evaluate_triple(
    ctx: &EvalContext<'_>,
    k1: f64,
    k2: f64,
    k3: f64,
    out: &mut Vec<ArbOpportunity>,
) {
    if !(k1 > 0.0 && k2 > k1 && k3 > k2) {
        return;
    }
    let (Some(p1), Some(p2), Some(p3)) = (
        strike_pair(ctx.chain, k1),
        strike_pair(ctx.chain, k2),
        strike_pair(ctx.chain, k3),
    ) else {
        return;
    };

    let w1 = (k3 - k2) / (k3 - k1);
    let w3 = (k2 - k1) / (k3 - k1);

    if let (Some(c1), Some(c2), Some(c3)) = (p1.call, p2.call, p3.call) {
        eval_one_butterfly(
            ctx,
            StrategyKind::ButterflyCall,
            [k1, k2, k3],
            [c1, c2, c3],
            w1,
            w3,
            out,
        );
    }
    if let (Some(pt1), Some(pt2), Some(pt3)) = (p1.put, p2.put, p3.put) {
        eval_one_butterfly(
            ctx,
            StrategyKind::ButterflyPut,
            [k1, k2, k3],
            [pt1, pt2, pt3],
            w1,
            w3,
            out,
        );
    }
}

fn eval_one_butterfly(
    ctx: &EvalContext<'_>,
    strategy: StrategyKind,
    strikes: [f64; 3],
    ids: [InstrumentId; 3],
    w1: f64,
    w3: f64,
    out: &mut Vec<ArbOpportunity>,
) {
    let (Some((b1, t1, m1)), Some((b2, t2, m2)), Some((b3, t3, m3))) = (
        fresh_leg(ctx, ids[0]),
        fresh_leg(ctx, ids[1]),
        fresh_leg(ctx, ids[2]),
    ) else {
        return;
    };

    let is_inverse = ctx.chain.is_inverse;
    let unit_to_usd = if is_inverse { ctx.spot } else { 1.0 };
    // Ensure wing legs satisfy `min_trade_amount` when scaled by w1 and w3.
    let quantum = m2.max(m1 / w1).max(m3 / w3);
    let max_units = ctx.max_units();
    // Conservative ITM expiry delivery weight across the 3 legs (total long+short weight = 2.0).
    let deliv = if is_inverse {
        ctx.fees.inverse_option_delivery_fee(2.0)
    } else {
        ctx.fees.linear_option_delivery_fee(ctx.spot, 2.0)
    };

    let legs = [
        LegSpec {
            book: b1,
            side: LegSide::Buy,
            ratio: w1,
        },
        LegSpec {
            book: b2,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: b3,
            side: LegSide::Buy,
            ratio: w3,
        },
    ];

    if let Some(sw) = sweep_multi_leg(&legs, quantum, max_units, |px| {
        let exec = if is_inverse {
            w1 * ctx.fees.inverse_option_exec_fee(px[0], t1)
                + ctx.fees.inverse_option_exec_fee(px[1], t2)
                + w3 * ctx.fees.inverse_option_exec_fee(px[2], t3)
        } else {
            w1 * ctx.fees.linear_option_exec_fee(px[0], ctx.spot, t1)
                + ctx.fees.linear_option_exec_fee(px[1], ctx.spot, t2)
                + w3 * ctx.fees.linear_option_exec_fee(px[2], ctx.spot, t3)
        };
        let fee = exec + deliv;
        let credit = px[1] - (w1 * px[0] + w3 * px[2]);
        let edge = credit - fee;
        (edge * unit_to_usd, fee * unit_to_usd)
    }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
    {
        out.push(ArbOpportunity {
            strategy,
            market: ctx.chain.market.to_string(),
            is_inverse,
            expiration_ts_ms: ctx.chain.expiration_ts_ms,
            detected_ts_ms: ctx.now_ms,
            tte_years: ctx.tte_years,
            spot_index: ctx.spot,
            strikes: strikes.to_vec(),
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
                    ids[0],
                    LegSide::Buy,
                    w1,
                    sw.bbo_prices[0],
                    sw.vwap_prices[0],
                ),
                ctx.leg_fill(
                    ids[1],
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[1],
                    sw.vwap_prices[1],
                ),
                ctx.leg_fill(
                    ids[2],
                    LegSide::Buy,
                    w3,
                    sw.bbo_prices[2],
                    sw.vwap_prices[2],
                ),
            ],
        });
    }
}
