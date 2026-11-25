use crate::config::FeeConfig;

/// Exact Deribit execution & expiry delivery fee model for linear USDC and inverse coin contracts.
///
/// Key Deribit rules modeled accurately:
/// - Linear USDC option execution fee per 1 underlying unit (in USDC):
///   `min(taker_rate * index_price, cap_rate * max(premium, 0))`
/// - Inverse coin option execution fee per 1 underlying unit (in coin, e.g. BTC/ETH):
///   `min(taker_rate, cap_rate * max(premium_coin, 0))`
/// - Expiry delivery fee applies **only to ITM legs** (OTM options expire at zero fee):
///   - Box spread (4 legs): at most 2 legs finish ITM for any terminal spot `S_T`
///   - Conversion / reversal (2 option legs + future): at most 1 option leg finishes ITM
///   - Vertical spread (2 option legs): at most 2 legs finish ITM (conservative bound)
///   - Butterfly (3 strikes, total weight 2): at most weighted 1.0–2.0 legs finish ITM
/// - Dated future execution fee:
///   - Linear USDC future: `future_taker_rate * future_price` USDC per 1 underlying unit
///   - Inverse future of USD notional `N` at price `F`: `future_taker_rate * (N / F)` coin
#[derive(Debug, Clone)]
pub struct FeeModel {
    pub cfg: FeeConfig,
}

impl FeeModel {
    pub fn new(cfg: FeeConfig) -> Self {
        Self { cfg }
    }

    #[inline]
    pub fn effective_option_taker(&self, instrument_taker: f64) -> f64 {
        if let Some(ov) = self.cfg.option_taker_override {
            return ov;
        }
        if instrument_taker > 0.0 {
            instrument_taker
        } else {
            self.cfg.default_option_taker_rate
        }
    }

    #[inline]
    pub fn effective_future_taker(&self, instrument_taker: f64) -> f64 {
        if let Some(ov) = self.cfg.future_taker_override {
            return ov;
        }
        if instrument_taker > 0.0 {
            instrument_taker
        } else {
            self.cfg.default_future_taker_rate
        }
    }

    /// Linear USDC option taker execution fee per 1 unit of underlying (in USDC).
    #[inline]
    pub fn linear_option_exec_fee(
        &self,
        premium_usdc: f64,
        spot_usdc: f64,
        inst_taker: f64,
    ) -> f64 {
        let rate = self.effective_option_taker(inst_taker);
        let base = rate * spot_usdc.max(0.0);
        let cap = self.cfg.option_fee_cap_rate * premium_usdc.max(0.0);
        base.min(cap)
    }

    /// Linear USDC option expiry delivery fee upper bound per ITM unit (in USDC).
    #[inline]
    pub fn linear_option_delivery_fee(&self, spot_usdc: f64, itm_leg_weight: f64) -> f64 {
        if !self.cfg.hold_to_expiry {
            return 0.0;
        }
        self.cfg.option_delivery_rate * spot_usdc.max(0.0) * itm_leg_weight
    }

    /// Inverse option taker execution fee per 1 unit of underlying (in coin, e.g. BTC/ETH).
    #[inline]
    pub fn inverse_option_exec_fee(&self, premium_coin: f64, inst_taker: f64) -> f64 {
        let rate = self.effective_option_taker(inst_taker);
        let cap = self.cfg.option_fee_cap_rate * premium_coin.max(0.0);
        rate.min(cap)
    }

    /// Inverse option expiry delivery fee upper bound per ITM unit (in coin).
    #[inline]
    pub fn inverse_option_delivery_fee(&self, itm_leg_weight: f64) -> f64 {
        if !self.cfg.hold_to_expiry {
            return 0.0;
        }
        self.cfg.option_delivery_rate * itm_leg_weight
    }

    /// Linear USDC dated future execution fee per 1 unit of underlying (in USDC).
    #[inline]
    pub fn linear_future_exec_fee(&self, future_price_usdc: f64, inst_taker: f64) -> f64 {
        self.effective_future_taker(inst_taker) * future_price_usdc.max(0.0)
    }

    /// Inverse dated future execution fee for `notional_usd` entered at `future_price_usd` (in coin).
    #[inline]
    pub fn inverse_future_exec_fee(
        &self,
        notional_usd: f64,
        future_price_usd: f64,
        inst_taker: f64,
    ) -> f64 {
        if future_price_usd <= 0.0 {
            return 0.0;
        }
        self.effective_future_taker(inst_taker) * (notional_usd / future_price_usd)
    }
}
