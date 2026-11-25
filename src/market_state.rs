use std::collections::BTreeMap;
use std::sync::Arc;

use deribit_tick_collector::model::{
    BookUpdateEvent, IndexKind, InstrumentId, InstrumentKind, InstrumentMeta, LiveBook,
    MarketEvent, OptionType, OrderedPrice,
};
use deribit_tick_collector::registry::deribit_index_name;
use rustc_hash::FxHashMap;

pub const MS_PER_YEAR: f64 = 365.0 * 86_400_000.0;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChainKey {
    pub market: Arc<str>,
    pub expiration_ts_ms: i64,
}

#[derive(Debug, Clone)]
pub struct StrikePair {
    pub strike: f64,
    pub call: Option<InstrumentId>,
    pub put: Option<InstrumentId>,
}

#[derive(Debug, Clone)]
pub struct ExpiryChain {
    pub market: Arc<str>,
    pub expiration_ts_ms: i64,
    pub is_inverse: bool,
    pub strikes: BTreeMap<OrderedPrice, StrikePair>,
    pub dated_future: Option<InstrumentId>,
}

impl ExpiryChain {
    pub fn new(market: Arc<str>, expiration_ts_ms: i64, is_inverse: bool) -> Self {
        Self {
            market,
            expiration_ts_ms,
            is_inverse,
            strikes: BTreeMap::new(),
            dated_future: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketContext {
    pub spot_index: Option<f64>,
    pub spot_ts_ms: i64,
    pub dvol: Option<f64>,
    pub perp_funding_8h: Option<f64>,
    pub perp_mark_price: Option<f64>,
}

/// Identifies the minimal subset of chains/strikes affected by an incoming book update
/// so incremental scanning runs in O(N) / O(1) instead of O(N^2).
#[derive(Debug, Clone)]
pub enum AffectedTarget {
    OptionStrike {
        key: ChainKey,
        strike: f64,
        exchange_ts_ms: i64,
    },
    DatedFuture {
        key: ChainKey,
        exchange_ts_ms: i64,
    },
}

#[derive(Debug, Default)]
pub struct MarketState {
    pub metas: FxHashMap<InstrumentId, Arc<InstrumentMeta>>,
    pub by_name: FxHashMap<Arc<str>, InstrumentId>,
    pub books: FxHashMap<InstrumentId, LiveBook>,
    pub chains: BTreeMap<ChainKey, ExpiryChain>,
    pub markets: FxHashMap<Arc<str>, MarketContext>,
    index_to_markets: FxHashMap<String, Vec<Arc<str>>>,
    vol_index_to_markets: FxHashMap<String, Vec<Arc<str>>>,
}

impl MarketState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_instrument(&mut self, meta: Arc<InstrumentMeta>) {
        let id = meta.id;
        let is_inverse = meta
            .base_currency
            .eq_ignore_ascii_case(&meta.settlement_currency);

        let idx_name = deribit_index_name(&meta.market);
        let mkts = self.index_to_markets.entry(idx_name).or_default();
        if !mkts.iter().any(|m| m.as_ref() == meta.market.as_ref()) {
            mkts.push(Arc::clone(&meta.market));
        }
        // DVOL indices are published per base asset (e.g. `btc_usd`, `eth_usd`) and apply to both
        // inverse (`BTC`) and linear (`BTC_USDC`) chains sharing the same underlying.
        let vol_idx = format!("{}_usd", meta.base_currency.to_ascii_lowercase());
        let vol_mkts = self.vol_index_to_markets.entry(vol_idx).or_default();
        if !vol_mkts.iter().any(|m| m.as_ref() == meta.market.as_ref()) {
            vol_mkts.push(Arc::clone(&meta.market));
        }
        self.markets.entry(Arc::clone(&meta.market)).or_default();

        match meta.kind {
            InstrumentKind::Option => {
                if let (Some(strike), Some(opt_type)) = (meta.strike, meta.option_type)
                    && meta.expiration_ts_ms > 0
                {
                    let key = ChainKey {
                        market: Arc::clone(&meta.market),
                        expiration_ts_ms: meta.expiration_ts_ms,
                    };
                    let chain = self.chains.entry(key).or_insert_with(|| {
                        ExpiryChain::new(
                            Arc::clone(&meta.market),
                            meta.expiration_ts_ms,
                            is_inverse,
                        )
                    });
                    let pair = chain
                        .strikes
                        .entry(OrderedPrice(strike))
                        .or_insert_with(|| StrikePair {
                            strike,
                            call: None,
                            put: None,
                        });
                    match opt_type {
                        OptionType::Call => pair.call = Some(id),
                        OptionType::Put => pair.put = Some(id),
                    }
                }
            }
            InstrumentKind::Future => {
                if meta.expiration_ts_ms > 0 {
                    let key = ChainKey {
                        market: Arc::clone(&meta.market),
                        expiration_ts_ms: meta.expiration_ts_ms,
                    };
                    let chain = self.chains.entry(key).or_insert_with(|| {
                        ExpiryChain::new(
                            Arc::clone(&meta.market),
                            meta.expiration_ts_ms,
                            is_inverse,
                        )
                    });
                    chain.dated_future = Some(id);
                }
            }
            InstrumentKind::Perpetual
            | InstrumentKind::OptionCombo
            | InstrumentKind::FutureCombo => {}
        }

        self.by_name.insert(Arc::clone(&meta.instrument_name), id);
        self.metas.insert(id, meta);
    }

    pub fn apply_book_update(&mut self, ev: &BookUpdateEvent) -> Option<AffectedTarget> {
        if ev.is_snapshot {
            self.books
                .entry(ev.instrument_id)
                .and_modify(|b| b.reset_snapshot(ev.change_id, ev.exchange_ts_ms, &ev.levels))
                .or_insert_with(|| {
                    LiveBook::from_snapshot(ev.change_id, ev.exchange_ts_ms, &ev.levels)
                });
        } else {
            let book = self.books.get_mut(&ev.instrument_id)?;
            book.apply_delta(ev.change_id, ev.exchange_ts_ms, &ev.levels);
        }

        let meta = self.metas.get(&ev.instrument_id)?;
        match meta.kind {
            InstrumentKind::Option => Some(AffectedTarget::OptionStrike {
                key: ChainKey {
                    market: Arc::clone(&meta.market),
                    expiration_ts_ms: meta.expiration_ts_ms,
                },
                strike: meta.strike?,
                exchange_ts_ms: ev.exchange_ts_ms,
            }),
            InstrumentKind::Future => Some(AffectedTarget::DatedFuture {
                key: ChainKey {
                    market: Arc::clone(&meta.market),
                    expiration_ts_ms: meta.expiration_ts_ms,
                },
                exchange_ts_ms: ev.exchange_ts_ms,
            }),
            _ => None,
        }
    }

    pub fn apply_event(&mut self, ev: &MarketEvent) -> Option<AffectedTarget> {
        match ev {
            MarketEvent::Instrument(meta) => {
                self.register_instrument(Arc::new(meta.clone()));
                None
            }
            MarketEvent::Book(book_ev) => self.apply_book_update(book_ev),
            MarketEvent::Index(idx) => {
                match idx.kind {
                    IndexKind::Price => {
                        if let Some(mkts) =
                            self.index_to_markets.get(idx.index_name.as_ref()).cloned()
                        {
                            for m in mkts {
                                let ctx = self.markets.entry(m).or_default();
                                ctx.spot_index = Some(idx.value);
                                ctx.spot_ts_ms = idx.exchange_ts_ms;
                            }
                        }
                    }
                    IndexKind::Volatility => {
                        if let Some(mkts) = self
                            .vol_index_to_markets
                            .get(idx.index_name.as_ref())
                            .cloned()
                        {
                            for m in mkts {
                                let ctx = self.markets.entry(m).or_default();
                                ctx.dvol = Some(idx.value);
                            }
                        }
                    }
                }
                None
            }
            MarketEvent::Ticker(t) => {
                if let Some(meta) = self.metas.get(&t.instrument_id) {
                    let ctx = self.markets.entry(Arc::clone(&meta.market)).or_default();
                    if let Some(idx_px) = t.index_price
                        && idx_px > 0.0
                        && t.exchange_ts_ms >= ctx.spot_ts_ms
                    {
                        ctx.spot_index = Some(idx_px);
                        ctx.spot_ts_ms = t.exchange_ts_ms;
                    }
                    if meta.kind == InstrumentKind::Perpetual {
                        if t.funding_8h.is_some() {
                            ctx.perp_funding_8h = t.funding_8h;
                        }
                        if t.mark_price.is_some() {
                            ctx.perp_mark_price = t.mark_price;
                        }
                    }
                }
                None
            }
            MarketEvent::Trade(_) | MarketEvent::Capture(_) => None,
        }
    }

    /// Compute live annualized dated-future carry rate `ln(F_mid / S_0) / T` if a matched dated
    /// future book has both bid and ask.
    pub fn futures_carry_rate(&self, chain: &ExpiryChain, spot: f64, t_years: f64) -> Option<f64> {
        if spot <= 0.0 || t_years <= 0.0 {
            return None;
        }
        let fut_id = chain.dated_future?;
        let book = self.books.get(&fut_id)?;
        let bid = book.bids.iter().next_back().map(|(p, _)| p.0)?;
        let ask = book.asks.iter().next().map(|(p, _)| p.0)?;
        if bid <= 0.0 || ask <= 0.0 {
            return None;
        }
        let mid = 0.5 * (bid + ask);
        // Ignore severely illiquid stub quotes (spread > 5% of mid) so carry estimates stay clean.
        if (ask - bid) / mid > 0.05 {
            return None;
        }
        Some((mid / spot).ln() / t_years)
    }

    pub fn perp_funding_annualized(&self, market: &str) -> Option<f64> {
        let f8h = self.markets.get(market)?.perp_funding_8h?;
        Some(f8h * 3.0 * 365.0)
    }

    pub fn spot_index(&self, market: &str) -> Option<f64> {
        self.markets
            .get(market)
            .and_then(|c| c.spot_index)
            .filter(|&p| p > 0.0)
    }

    pub fn is_quote_fresh(&self, book: &LiveBook, now_ms: i64, max_age_ms: i64) -> bool {
        if max_age_ms <= 0 {
            return true;
        }
        (now_ms - book.last_exchange_ts_ms).abs() <= max_age_ms
    }

    pub fn best_bid(&self, id: InstrumentId) -> Option<(f64, f64)> {
        self.books
            .get(&id)?
            .bids
            .iter()
            .next_back()
            .map(|(p, &a)| (p.0, a))
    }

    pub fn best_ask(&self, id: InstrumentId) -> Option<(f64, f64)> {
        self.books
            .get(&id)?
            .asks
            .iter()
            .next()
            .map(|(p, &a)| (p.0, a))
    }
}
