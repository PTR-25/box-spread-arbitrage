use crate::book::{LegSide, LegSpec, sweep_multi_leg};
use crate::strategies::{ArbOpportunity, EvalContext, StrategyKind, fresh_leg, strike_pair};

/// Scan Conversions & Reversals (Put-Call Parity against the live matched-expiry dated future).
///
/// Exact Deribit settlement identities:
/// - **Linear USDC (`!is_inverse`)** with linear dated future (`amount` in underlying units):
///   - **Conversion** (`-C(K) + P(K) + F`):
///     Terminal payoff = `(K - S_T) + (S_T - F_ask) = K - F_ask` USDC.
///     Discounted net edge = `(bid(C) - ask(P) - f_opt) + exp(-rT) * (K - F_ask - f_fut)`.
///   - **Reversal** (`+C(K) - P(K) - F`):
///     Terminal payoff = `(S_T - K) + (F_bid - S_T) = F_bid - K` USDC.
///     Discounted net edge = `exp(-rT) * (F_bid - K - f_fut) - (ask(C) - bid(P) + f_opt)`.
/// - **Inverse BTC/ETH (`is_inverse`)** with inverse dated future (`amount` in USD notional, ratio = `K` USD per 1 coin):
///   - **Conversion** (`-1 C(K) + 1 P(K) + Long K USD future @ F_ask`):
///     Terminal payoff in coin = `(K/S_T - 1) + K*(1/F_ask - 1/S_T) = K/F_ask - 1` coin (exact zero spot risk!).
///     Discounted net edge (coin) = `(bid(C) - ask(P) - f_opt) + exp(-r_coin*T)*(K/F_ask - 1) - f_fut`.
///   - **Reversal** (`+1 C(K) - 1 P(K) + Short K USD future @ F_bid`):
///     Terminal payoff in coin = `(1 - K/S_T) + K*(1/S_T - 1/F_bid) = 1 - K/F_bid` coin (exact zero spot risk!).
///     Discounted net edge (coin) = `exp(-r_coin*T)*(1 - K/F_bid) - (ask(C) - bid(P) + f_opt + f_fut)`.
pub fn scan_strike(ctx: &EvalContext<'_>, strike: f64, out: &mut Vec<ArbOpportunity>) {
    let Some(fut_id) = ctx.chain.dated_future else {
        return;
    };
    let Some(pair) = strike_pair(ctx.chain, strike) else {
        return;
    };
    let (Some(c_id), Some(p_id)) = (pair.call, pair.put) else {
        return;
    };

    let (Some((c_b, c_t, c_min)), Some((p_b, p_t, p_min)), Some((f_b, f_t, f_min))) = (
        fresh_leg(ctx, c_id),
        fresh_leg(ctx, p_id),
        fresh_leg(ctx, fut_id),
    ) else {
        return;
    };

    let is_inverse = ctx.chain.is_inverse;
    let discount = (-ctx.benchmark_rate * ctx.tte_years).exp();
    let unit_to_usd = if is_inverse { ctx.spot } else { 1.0 };
    // For inverse dated futures, book amount is in USD and 1 coin of options is hedged by `strike` USD.
    let fut_ratio = if is_inverse { strike } else { 1.0 };
    let quantum = c_min.max(p_min).max(f_min / fut_ratio);
    let max_units = ctx.max_units();

    // At most 1 of the 2 option legs (C or P) finishes ITM at expiry.
    let deliv_native = if is_inverse {
        ctx.fees.inverse_option_delivery_fee(1.0)
    } else {
        ctx.fees.linear_option_delivery_fee(ctx.spot, 1.0)
    };

    // --- 1. CONVERSION: -C(K) +P(K) +Future ---
    let conv_legs = [
        LegSpec {
            book: c_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: p_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: f_b,
            side: LegSide::Buy,
            ratio: fut_ratio,
        },
    ];

    if let Some(sw) = sweep_multi_leg(&conv_legs, quantum, max_units, |px| {
        let (c_bid, p_ask, f_ask) = (px[0], px[1], px[2]);
        if is_inverse {
            let opt_fee = ctx.fees.inverse_option_exec_fee(c_bid, c_t)
                + ctx.fees.inverse_option_exec_fee(p_ask, p_t)
                + deliv_native;
            let fut_fee = ctx.fees.inverse_future_exec_fee(strike, f_ask, f_t);
            let total_fee = opt_fee + fut_fee;
            let edge_coin = (c_bid - p_ask) + discount * (strike / f_ask - 1.0) - total_fee;
            (edge_coin * unit_to_usd, total_fee * unit_to_usd)
        } else {
            let opt_fee = ctx.fees.linear_option_exec_fee(c_bid, ctx.spot, c_t)
                + ctx.fees.linear_option_exec_fee(p_ask, ctx.spot, p_t)
                + deliv_native;
            let fut_fee = ctx.fees.linear_future_exec_fee(f_ask, f_t);
            let total_fee = opt_fee + fut_fee;
            let edge_usdc = (c_bid - p_ask) + discount * (strike - f_ask) - total_fee;
            (edge_usdc, total_fee)
        }
    }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
    {
        out.push(ArbOpportunity {
            strategy: StrategyKind::Conversion,
            market: ctx.chain.market.to_string(),
            is_inverse,
            expiration_ts_ms: ctx.chain.expiration_ts_ms,
            detected_ts_ms: ctx.now_ms,
            tte_years: ctx.tte_years,
            spot_index: ctx.spot,
            strikes: vec![strike],
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
                    c_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[0],
                    sw.vwap_prices[0],
                ),
                ctx.leg_fill(p_id, LegSide::Buy, 1.0, sw.bbo_prices[1], sw.vwap_prices[1]),
                ctx.leg_fill(
                    fut_id,
                    LegSide::Buy,
                    fut_ratio,
                    sw.bbo_prices[2],
                    sw.vwap_prices[2],
                ),
            ],
        });
    }

    // --- 2. REVERSAL: +C(K) -P(K) -Future ---
    let rev_legs = [
        LegSpec {
            book: c_b,
            side: LegSide::Buy,
            ratio: 1.0,
        },
        LegSpec {
            book: p_b,
            side: LegSide::Sell,
            ratio: 1.0,
        },
        LegSpec {
            book: f_b,
            side: LegSide::Sell,
            ratio: fut_ratio,
        },
    ];

    if let Some(sw) = sweep_multi_leg(&rev_legs, quantum, max_units, |px| {
        let (c_ask, p_bid, f_bid) = (px[0], px[1], px[2]);
        if is_inverse {
            let opt_fee = ctx.fees.inverse_option_exec_fee(c_ask, c_t)
                + ctx.fees.inverse_option_exec_fee(p_bid, p_t)
                + deliv_native;
            let fut_fee = ctx.fees.inverse_future_exec_fee(strike, f_bid, f_t);
            let total_fee = opt_fee + fut_fee;
            let edge_coin = discount * (1.0 - strike / f_bid) - (c_ask - p_bid) - total_fee;
            (edge_coin * unit_to_usd, total_fee * unit_to_usd)
        } else {
            let opt_fee = ctx.fees.linear_option_exec_fee(c_ask, ctx.spot, c_t)
                + ctx.fees.linear_option_exec_fee(p_bid, ctx.spot, p_t)
                + deliv_native;
            let fut_fee = ctx.fees.linear_future_exec_fee(f_bid, f_t);
            let total_fee = opt_fee + fut_fee;
            let edge_usdc = discount * (f_bid - strike) - (c_ask - p_bid) - total_fee;
            (edge_usdc, total_fee)
        }
    }) && ctx.passes_thresholds(sw.bbo_edge_usd, sw.total_profit_usd)
    {
        out.push(ArbOpportunity {
            strategy: StrategyKind::Reversal,
            market: ctx.chain.market.to_string(),
            is_inverse,
            expiration_ts_ms: ctx.chain.expiration_ts_ms,
            detected_ts_ms: ctx.now_ms,
            tte_years: ctx.tte_years,
            spot_index: ctx.spot,
            strikes: vec![strike],
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
                ctx.leg_fill(c_id, LegSide::Buy, 1.0, sw.bbo_prices[0], sw.vwap_prices[0]),
                ctx.leg_fill(
                    p_id,
                    LegSide::Sell,
                    1.0,
                    sw.bbo_prices[1],
                    sw.vwap_prices[1],
                ),
                ctx.leg_fill(
                    fut_id,
                    LegSide::Sell,
                    fut_ratio,
                    sw.bbo_prices[2],
                    sw.vwap_prices[2],
                ),
            ],
        });
    }
}
