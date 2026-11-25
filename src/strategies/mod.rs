pub mod box_spread;
pub mod butterfly;
pub mod parity;
pub mod vertical;

use std::fmt;

use chrono::DateTime;
use deribit_tick_collector::model::{InstrumentId, OrderedPrice};
use serde::Serialize;

use crate::book::LegSide;
use crate::config::ArbConfig;
use crate::fees::FeeModel;
use crate::market_state::{AffectedTarget, ExpiryChain, MS_PER_YEAR, MarketState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    BoxLong,
    BoxShort,
    /// 1:1 inverse box (USD payoff `W = K_high - K_low`, coin payoff `W/S_T`) hedged with
    /// `W` USD notional of the matched-expiry inverse dated future (net coin payoff `W/F_ask`).
    BoxInverseUsdLong,
    /// Short 1:1 inverse box hedged with short `W` USD notional of the matched-expiry inverse
    /// dated future (net coin liability `W/F_bid`).
    BoxInverseUsdShort,
    Conversion,
    Reversal,
    VerticalCallMonotonicity,
    VerticalPutMonotonicity,
    VerticalCallCap,
    VerticalPutCap,
    ButterflyCall,
    ButterflyPut,
}

impl StrategyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoxLong => "box_long",
            Self::BoxShort => "box_short",
            Self::BoxInverseUsdLong => "box_inv_usd_long",
            Self::BoxInverseUsdShort => "box_inv_usd_short",
            Self::Conversion => "conversion",
            Self::Reversal => "reversal",
            Self::VerticalCallMonotonicity => "vertical_call_mono",
            Self::VerticalPutMonotonicity => "vertical_put_mono",
            Self::VerticalCallCap => "vertical_call_cap",
            Self::VerticalPutCap => "vertical_put_cap",
            Self::ButterflyCall => "butterfly_call",
            Self::ButterflyPut => "butterfly_put",
        }
    }

    /// True if this strategy includes the matched-expiry dated future as a leg.
    pub fn uses_dated_future(self) -> bool {
        matches!(
            self,
            Self::Conversion | Self::Reversal | Self::BoxInverseUsdLong | Self::BoxInverseUsdShort
        )
    }
}

impl fmt::Display for StrategyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Unique key identifying a persistent arbitrage opportunity episode across ticks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EpisodeKey {
    pub strategy: StrategyKind,
    pub market: String,
    pub expiration_ts_ms: i64,
    /// Ordered strikes serialized as bit-exact `u64` keys for exact hashing.
    pub strike_bits: Vec<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LegFill {
    pub instrument_name: String,
    #[serde(skip_serializing)]
    pub instrument_id: InstrumentId,
    pub side: String,
    pub ratio: f64,
    pub bbo_price: f64,
    pub vwap_price: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArbOpportunity {
    pub strategy: StrategyKind,
    pub market: String,
    pub is_inverse: bool,
    pub expiration_ts_ms: i64,
    pub detected_ts_ms: i64,
    pub tte_years: f64,
    pub spot_index: f64,
    pub strikes: Vec<f64>,
    /// L1 top-of-book net edge per 1.0 unit (USD/USDC).
    pub bbo_edge_usd: f64,
    /// L1 net edge in basis points of spot index (`10_000 * bbo_edge_usd / spot_index`).
    pub bbo_edge_bps: f64,
    /// Total discrete executable size across all profitable L2 levels (underlying units).
    pub executable_units: f64,
    /// Total net post-fee profit across swept L2 depth (USD/USDC).
    pub total_profit_usd: f64,
    /// Total fees (execution + ITM delivery) across swept L2 depth (USD/USDC).
    pub total_fees_usd: f64,
    pub levels_swept: usize,
    /// Box-implied annualized financing rate (for box spreads).
    pub implied_rate: Option<f64>,
    /// Live matched-expiry dated future annualized carry rate.
    pub futures_carry_rate: Option<f64>,
    /// Live annualized 8h perpetual funding rate.
    pub perp_funding_ann: Option<f64>,
    pub legs: Vec<LegFill>,
}

impl ArbOpportunity {
    pub fn episode_key(&self) -> EpisodeKey {
        EpisodeKey {
            strategy: self.strategy,
            market: self.market.clone(),
            expiration_ts_ms: self.expiration_ts_ms,
            strike_bits: self.strikes.iter().map(|s| s.to_bits()).collect(),
        }
    }

    pub fn expiry_iso(&self) -> String {
        DateTime::from_timestamp_millis(self.expiration_ts_ms)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| self.expiration_ts_ms.to_string())
    }

    pub fn summary_line(&self) -> String {
        let strikes_str = self
            .strikes
            .iter()
            .map(|s| format!("{s}"))
            .collect::<Vec<_>>()
            .join("/");
        let rate_info = match (self.implied_rate, self.futures_carry_rate) {
            (Some(r), Some(fc)) => {
                format!(" r_box={:.2}% fut_carry={:.2}%", r * 100.0, fc * 100.0)
            }
            (Some(r), None) => format!(" r_box={:.2}%", r * 100.0),
            _ => String::new(),
        };
        format!(
            "[{}] {} {} K=[{}] profit=${:.2} (bbo=${:.4}/u, {:.1}bps) size={:.4} lvls={}{}",
            self.strategy,
            self.market,
            self.expiry_iso(),
            strikes_str,
            self.total_profit_usd,
            self.bbo_edge_usd,
            self.bbo_edge_bps,
            self.executable_units,
            self.levels_swept,
            rate_info
        )
    }
}

pub struct EvalContext<'a> {
    pub state: &'a MarketState,
    pub chain: &'a ExpiryChain,
    pub cfg: &'a ArbConfig,
    pub fees: &'a FeeModel,
    pub now_ms: i64,
    pub tte_years: f64,
    pub spot: f64,
    pub benchmark_rate: f64,
    pub futures_carry: Option<f64>,
    pub perp_funding_ann: Option<f64>,
}

impl<'a> EvalContext<'a> {
    pub fn build(
        state: &'a MarketState,
        chain: &'a ExpiryChain,
        cfg: &'a ArbConfig,
        fees: &'a FeeModel,
        now_ms: i64,
    ) -> Option<Self> {
        let tte_ms = chain.expiration_ts_ms - now_ms;
        if tte_ms <= 0 {
            return None;
        }
        let tte_minutes = tte_ms as f64 / 60_000.0;
        if tte_minutes < cfg.scanner.min_tte_minutes {
            return None;
        }
        if cfg.scanner.max_dte_days > 0 {
            let max_ms = (cfg.scanner.max_dte_days as i64) * 86_400_000;
            if tte_ms > max_ms {
                return None;
            }
        }
        let tte_years = tte_ms as f64 / MS_PER_YEAR;
        let spot = state.spot_index(&chain.market)?;
        let futures_carry = state.futures_carry_rate(chain, spot, tte_years);
        let perp_funding_ann = state.perp_funding_annualized(&chain.market);
        let benchmark_rate = if chain.is_inverse {
            cfg.rates.coin_benchmark_rate
        } else {
            cfg.rates.usdc_benchmark_rate
        };

        Some(Self {
            state,
            chain,
            cfg,
            fees,
            now_ms,
            tte_years,
            spot,
            benchmark_rate,
            futures_carry,
            perp_funding_ann,
        })
    }

    pub fn max_units(&self) -> f64 {
        if self.cfg.scanner.max_sweep_notional_usd > 0.0 && self.spot > 0.0 {
            self.cfg.scanner.max_sweep_notional_usd / self.spot
        } else {
            f64::INFINITY
        }
    }

    pub fn passes_thresholds(&self, bbo_edge_usd: f64, total_profit_usd: f64) -> bool {
        if total_profit_usd < self.cfg.scanner.min_profit_usd {
            return false;
        }
        let edge_bps = (bbo_edge_usd / self.spot) * 10_000.0;
        edge_bps >= self.cfg.scanner.min_edge_bps
    }

    pub fn leg_fill(
        &self,
        id: InstrumentId,
        side: LegSide,
        ratio: f64,
        bbo_price: f64,
        vwap_price: f64,
    ) -> LegFill {
        let name = self
            .state
            .metas
            .get(&id)
            .map(|m| m.instrument_name.to_string())
            .unwrap_or_else(|| format!("id:{}", id.0));
        LegFill {
            instrument_name: name,
            instrument_id: id,
            side: side.as_str().to_string(),
            ratio,
            bbo_price,
            vwap_price,
        }
    }
}

/// Incremental scanner dispatched on each affected option strike or dated future tick.
pub fn scan_affected(
    state: &MarketState,
    cfg: &ArbConfig,
    fees: &FeeModel,
    target: &AffectedTarget,
) -> Vec<ArbOpportunity> {
    let mut out = Vec::new();
    match target {
        AffectedTarget::OptionStrike {
            key,
            strike,
            exchange_ts_ms,
        } => {
            let Some(chain) = state.chains.get(key) else {
                return out;
            };
            let Some(ctx) = EvalContext::build(state, chain, cfg, fees, *exchange_ts_ms) else {
                return out;
            };
            let ordered_strikes: Vec<f64> = chain.strikes.keys().map(|k| k.0).collect();
            let Some(pos) = ordered_strikes
                .iter()
                .position(|&k| k.to_bits() == strike.to_bits())
            else {
                return out;
            };

            if cfg.scanner.enable_parity {
                parity::scan_strike(&ctx, *strike, &mut out);
            }
            if cfg.scanner.enable_box_spreads {
                box_spread::scan_around_strike(&ctx, &ordered_strikes, pos, &mut out);
            }
            if cfg.scanner.enable_verticals {
                vertical::scan_around_strike(&ctx, &ordered_strikes, pos, &mut out);
            }
            if cfg.scanner.enable_butterflies {
                butterfly::scan_around_strike(&ctx, &ordered_strikes, pos, &mut out);
            }
        }
        AffectedTarget::DatedFuture {
            key,
            exchange_ts_ms,
        } => {
            if !cfg.scanner.enable_parity && !cfg.scanner.enable_box_spreads {
                return out;
            }
            let Some(chain) = state.chains.get(key) else {
                return out;
            };
            let Some(ctx) = EvalContext::build(state, chain, cfg, fees, *exchange_ts_ms) else {
                return out;
            };
            if cfg.scanner.enable_parity {
                for k in chain.strikes.keys() {
                    parity::scan_strike(&ctx, k.0, &mut out);
                }
            }
            if cfg.scanner.enable_box_spreads && chain.is_inverse {
                let ordered_strikes: Vec<f64> = chain.strikes.keys().map(|k| k.0).collect();
                box_spread::scan_all_inverse_usd_pairs(&ctx, &ordered_strikes, &mut out);
            }
        }
    }
    out
}

/// Full sweep across every active chain and enabled strategy.
pub fn scan_all(
    state: &MarketState,
    cfg: &ArbConfig,
    fees: &FeeModel,
    now_ms: i64,
) -> Vec<ArbOpportunity> {
    let mut out = Vec::new();
    for chain in state.chains.values() {
        let Some(ctx) = EvalContext::build(state, chain, cfg, fees, now_ms) else {
            continue;
        };
        let ordered_strikes: Vec<f64> = chain.strikes.keys().map(|k| k.0).collect();
        if cfg.scanner.enable_parity {
            for &k in &ordered_strikes {
                parity::scan_strike(&ctx, k, &mut out);
            }
        }
        if cfg.scanner.enable_box_spreads {
            box_spread::scan_all_pairs(&ctx, &ordered_strikes, &mut out);
        }
        if cfg.scanner.enable_verticals {
            vertical::scan_all_pairs(&ctx, &ordered_strikes, &mut out);
        }
        if cfg.scanner.enable_butterflies {
            butterfly::scan_all_triples(&ctx, &ordered_strikes, &mut out);
        }
    }
    out
}

/// Re-evaluate an exact previously detected opportunity at a later timestamp (used for
/// episode-lifetime tracking and synthetic latency-decay simulation).
pub fn re_evaluate_opportunity(
    state: &MarketState,
    cfg: &ArbConfig,
    fees: &FeeModel,
    opp: &ArbOpportunity,
    now_ms: i64,
) -> Option<ArbOpportunity> {
    let key = crate::market_state::ChainKey {
        market: std::sync::Arc::from(opp.market.as_str()),
        expiration_ts_ms: opp.expiration_ts_ms,
    };
    let chain = state.chains.get(&key)?;
    let ctx = EvalContext::build(state, chain, cfg, fees, now_ms)?;
    let mut tmp = Vec::new();

    match opp.strategy {
        StrategyKind::BoxLong
        | StrategyKind::BoxShort
        | StrategyKind::BoxInverseUsdLong
        | StrategyKind::BoxInverseUsdShort
            if opp.strikes.len() == 2 =>
        {
            box_spread::evaluate_pair(&ctx, opp.strikes[0], opp.strikes[1], &mut tmp);
        }
        StrategyKind::Conversion | StrategyKind::Reversal if opp.strikes.len() == 1 => {
            parity::scan_strike(&ctx, opp.strikes[0], &mut tmp);
        }
        StrategyKind::VerticalCallMonotonicity
        | StrategyKind::VerticalPutMonotonicity
        | StrategyKind::VerticalCallCap
        | StrategyKind::VerticalPutCap
            if opp.strikes.len() == 2 =>
        {
            vertical::evaluate_pair(&ctx, opp.strikes[0], opp.strikes[1], &mut tmp);
        }
        StrategyKind::ButterflyCall | StrategyKind::ButterflyPut if opp.strikes.len() == 3 => {
            butterfly::evaluate_triple(
                &ctx,
                opp.strikes[0],
                opp.strikes[1],
                opp.strikes[2],
                &mut tmp,
            );
        }
        _ => return None,
    }

    tmp.into_iter().find(|c| c.strategy == opp.strategy)
}

/// Helper to fetch a fresh L2 book + taker rate + min_trade_amount for an instrument.
pub(crate) fn fresh_leg<'a>(
    ctx: &'a EvalContext<'a>,
    id: InstrumentId,
) -> Option<(&'a deribit_tick_collector::model::LiveBook, f64, f64)> {
    let book = ctx.state.books.get(&id)?;
    if !ctx
        .state
        .is_quote_fresh(book, ctx.now_ms, ctx.cfg.scanner.max_quote_age_ms)
    {
        return None;
    }
    let meta = ctx.state.metas.get(&id)?;
    Some((book, meta.taker_commission, meta.min_trade_amount))
}

pub(crate) fn strike_pair(
    chain: &ExpiryChain,
    strike: f64,
) -> Option<&crate::market_state::StrikePair> {
    chain.strikes.get(&OrderedPrice(strike))
}
