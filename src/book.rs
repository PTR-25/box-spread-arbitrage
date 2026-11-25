use deribit_tick_collector::model::LiveBook;
use smallvec::SmallVec;

/// Execution direction on an individual leg's L2 order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegSide {
    /// Lift resting asks (ascending price).
    Buy,
    /// Hit resting bids (descending price).
    Sell,
}

impl LegSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

/// Specification of one leg participating in a multi-leg L2 sweep.
#[derive(Clone, Copy)]
pub struct LegSpec<'a> {
    pub book: &'a LiveBook,
    pub side: LegSide,
    /// Raw book amount consumed per 1.0 unit of the multi-leg package.
    /// - Linear/inverse options: `1.0` (or `m = K1/K2` for ratio inverse box high legs, `w1`/`w3` for butterflies)
    /// - Linear USDC dated futures: `1.0` (book amount is in underlying units)
    /// - Inverse USD dated futures: `K` (book amount is in USD notional, hedging 1.0 coin of options at strike `K`)
    pub ratio: f64,
}

/// Output of sweeping all profitable price levels across a multi-leg L2 order book package.
#[derive(Debug, Clone)]
pub struct SweepResult {
    /// Top-of-book (L1) net edge per 1.0 package unit in USD/USDC.
    pub bbo_edge_usd: f64,
    /// Total executable package units across all profitable levels (quantized to `quantum` if > 0).
    pub total_units: f64,
    /// Total net post-fee profit in USD/USDC across all swept levels.
    pub total_profit_usd: f64,
    /// Total fees in USD/USDC across all swept levels.
    pub total_fees_usd: f64,
    /// Size-weighted average fill price (VWAP) on each leg.
    pub vwap_prices: SmallVec<[f64; 5]>,
    /// Top-of-book prices on each leg.
    pub bbo_prices: SmallVec<[f64; 5]>,
    /// Total book levels consumed across all legs.
    pub levels_swept: usize,
}

struct LevelCursor {
    levels: SmallVec<[(f64, f64); 16]>,
    idx: usize,
    rem: f64,
}

impl LevelCursor {
    fn from_book(book: &LiveBook, side: LegSide) -> Option<Self> {
        let mut levels: SmallVec<[(f64, f64); 16]> = SmallVec::new();
        match side {
            LegSide::Buy => {
                for (px, &amt) in &book.asks {
                    if px.0 > 0.0 && amt > 0.0 {
                        levels.push((px.0, amt));
                    }
                }
            }
            LegSide::Sell => {
                for (px, &amt) in book.bids.iter().rev() {
                    if px.0 > 0.0 && amt > 0.0 {
                        levels.push((px.0, amt));
                    }
                }
            }
        }
        let first_rem = levels.first()?.1;
        Some(Self {
            levels,
            idx: 0,
            rem: first_rem,
        })
    }

    #[inline]
    fn price(&self) -> Option<f64> {
        self.levels.get(self.idx).map(|(p, _)| *p)
    }

    #[inline]
    fn consume(&mut self, qty: f64) -> bool {
        self.rem -= qty;
        if self.rem <= 1e-12 {
            self.idx += 1;
            if let Some(&(_, next_amt)) = self.levels.get(self.idx) {
                self.rem = next_amt;
            } else {
                self.rem = 0.0;
            }
            true
        } else {
            false
        }
    }
}

/// Sweep full multi-level L2 depth across 2..=4 legs simultaneously.
///
/// `eval_marginal(prices)` returns `(net_edge_usd_per_unit, fee_usd_per_unit)` for the current
/// marginal leg prices. Sweeping terminates immediately as soon as marginal edge `<= 0.0` because
/// deeper book levels are monotonically worse on every leg.
pub fn sweep_multi_leg<F>(
    legs: &[LegSpec<'_>],
    quantum: f64,
    max_units: f64,
    mut eval_marginal: F,
) -> Option<SweepResult>
where
    F: FnMut(&[f64]) -> (f64, f64),
{
    let n = legs.len();
    if n == 0 || legs.iter().any(|l| l.ratio <= 0.0) {
        return None;
    }

    // Fast path: check BBO first without allocating level vectors if top-of-book is unprofitable!
    let mut bbo_prices: SmallVec<[f64; 5]> = SmallVec::with_capacity(n);
    for leg in legs {
        let px = match leg.side {
            LegSide::Buy => leg
                .book
                .asks
                .iter()
                .find(|(p, a)| p.0 > 0.0 && **a > 0.0)
                .map(|(p, _)| p.0)?,
            LegSide::Sell => leg
                .book
                .bids
                .iter()
                .rev()
                .find(|(p, a)| p.0 > 0.0 && **a > 0.0)
                .map(|(p, _)| p.0)?,
        };
        bbo_prices.push(px);
    }

    let (bbo_edge_usd, _) = eval_marginal(&bbo_prices);
    if !bbo_edge_usd.is_finite() || bbo_edge_usd <= 0.0 {
        return None;
    }

    // BBO is profitable -> materialize L2 cursors and sweep every profitable marginal slice.
    let mut cursors: SmallVec<[LevelCursor; 5]> = SmallVec::with_capacity(n);
    for leg in legs {
        cursors.push(LevelCursor::from_book(leg.book, leg.side)?);
    }

    let cap = if max_units > 0.0 && max_units.is_finite() {
        max_units
    } else {
        f64::INFINITY
    };

    struct Slice {
        units: f64,
        edge_per_unit: f64,
        fee_per_unit: f64,
        prices: SmallVec<[f64; 5]>,
    }

    let mut slices: SmallVec<[Slice; 8]> = SmallVec::new();
    let mut cur_prices: SmallVec<[f64; 5]> = SmallVec::with_capacity(n);
    let mut raw_total_units = 0.0;
    let mut levels_swept = n;

    while raw_total_units + 1e-12 < cap {
        cur_prices.clear();
        let mut slice_units = cap - raw_total_units;
        for (i, c) in cursors.iter().enumerate() {
            let Some(px) = c.price() else {
                slice_units = 0.0;
                break;
            };
            cur_prices.push(px);
            slice_units = slice_units.min(c.rem / legs[i].ratio);
        }
        if cur_prices.len() < n || slice_units <= 1e-12 {
            break;
        }

        let (edge_per_unit, fee_per_unit) = eval_marginal(&cur_prices);
        if !edge_per_unit.is_finite() || edge_per_unit <= 0.0 {
            break;
        }

        slices.push(Slice {
            units: slice_units,
            edge_per_unit,
            fee_per_unit,
            prices: cur_prices.clone(),
        });
        raw_total_units += slice_units;

        for (i, c) in cursors.iter_mut().enumerate() {
            if c.consume(slice_units * legs[i].ratio) && c.price().is_some() {
                levels_swept += 1;
            }
        }
    }

    // Apply discrete trade-size quantization (e.g. min_trade_amount) accurately across slices.
    let target_units = if quantum > 0.0 {
        let q = (raw_total_units / quantum).floor() * quantum;
        if q <= 0.0 {
            return None;
        }
        q
    } else {
        if raw_total_units <= 0.0 {
            return None;
        }
        raw_total_units
    };

    let mut remaining = target_units;
    let mut total_profit_usd = 0.0;
    let mut total_fees_usd = 0.0;
    let mut vwap_num: SmallVec<[f64; 5]> = SmallVec::from_elem(0.0, n);

    for s in &slices {
        if remaining <= 1e-12 {
            break;
        }
        let take = remaining.min(s.units);
        total_profit_usd += take * s.edge_per_unit;
        total_fees_usd += take * s.fee_per_unit;
        for i in 0..n {
            vwap_num[i] += take * s.prices[i];
        }
        remaining -= take;
    }

    let mut vwap_prices: SmallVec<[f64; 5]> = SmallVec::with_capacity(n);
    for num in vwap_num {
        vwap_prices.push(num / target_units);
    }

    Some(SweepResult {
        bbo_edge_usd,
        total_units: target_units,
        total_profit_usd,
        total_fees_usd,
        vwap_prices,
        bbo_prices,
        levels_swept,
    })
}
