"""
Defines data classes for representing different types of arbitrage opportunities,
including Box Spreads and Synthetic Forwards.
"""

import math
import datetime
import time
import logging
from typing import Dict, Any, Optional

from config import settings # For RISK_FREE_RATE
from arb.utils.fees import FeeCalculator # Import FeeCalculator

logger = logging.getLogger(__name__)

class ArbitrageOpportunity:
    """
    Base class for arbitrage opportunities.

    Attributes:
        coin (str): The underlying coin symbol (e.g., 'SOL').
        strike (float): The strike price of the option(s).
        expiry (datetime.date): The expiry date of the option(s).
        underlying_price (float): The current price of the underlying asset.
        call_order (Dict[str, Any]): Dictionary containing call option order book data.
        put_order (Dict[str, Any]): Dictionary containing put option order book data.
        T (float): Time to expiry in years.
        threshold (float): The minimum profit threshold for the opportunity.
        theoretical_forward (float): Theoretical forward price based on put-call parity.
    """
    def __init__(self, coin: str, strike: float, expiry: datetime.date,
                 underlying_price: float, call_order: Dict[str, Any],
                 put_order: Dict[str, Any], T: float, threshold: float):
        self.coin = coin
        self.strike = strike
        self.expiry = expiry
        self.underlying_price = underlying_price
        self.call_order = call_order
        self.put_order = put_order
        self.T = T
        self.threshold = threshold
        # Calculate theoretical forward using Black-76/Put-Call Parity: F = S - K * exp(-rT) is incorrect for options
        # Correct parity: C - P = exp(-rT) * (F - K) -> F = K + exp(rT)*(C-P)
        # Or simpler for this context: C - P should equal discounted (Spot - Strike)
        # Theoretical value of (Call - Put) = Discounted (Spot - Strike)
        # Note: RISK_FREE_RATE is loaded from settings
        self.theoretical_diff = math.exp(-settings.RISK_FREE_RATE * T) * (underlying_price - strike)

    def check_opportunity(self):
        """Checks if the conditions for this arbitrage opportunity are met."""
        raise NotImplementedError("Subclass must implement check_opportunity")

    def print_details(self):
        """Prints the details of the arbitrage opportunity."""
        raise NotImplementedError("Subclass must implement print_details")

class BoxArbitrage:
    """
    Constructs and analyzes a box spread arbitrage opportunity for both buying
    (long box) and selling (short box) scenarios.

    A box spread involves four options with the same expiry but two different
    strikes (K_low, K_high).

    Long Box (Buy Spread):
    - Buy Call @ K_low (Ask)
    - Sell Put @ K_low (Bid)
    - Sell Call @ K_high (Bid)
    - Buy Put @ K_high (Ask)
    Profit if: Theoretical Value > Buy Cost

    Short Box (Sell Spread):
    - Sell Call @ K_low (Bid)
    - Buy Put @ K_low (Ask)
    - Buy Call @ K_high (Ask)
    - Sell Put @ K_high (Bid)
    Profit if: Sell Proceeds > Theoretical Value

    The theoretical value at initiation is the present value of the guaranteed
    expiry payoff (K_high - K_low): exp(-rT) * (K_high - K_low).

    Handles differences between inverse (BTC/ETH) and linear (USDC) options.

    Attributes:
        coin (str): Underlying coin symbol.
        expiry (datetime.date): Option expiry date.
        K_low (float): Lower strike price.
        K_high (float): Higher strike price.
        contract_size (float): The number of underlying units per contract (multiplier).
        T (float): Time to expiry in years.
        threshold (float): Minimum profit potential threshold (absolute value).
        underlying_price (float): Current underlying index price.
        hold_to_expiry (bool): Flag indicating if settlement fees should be included.
        is_inverse (bool): True if options are inverse type (BTC/ETH).

        call_low_data, put_low_data, call_high_data, put_high_data (Dict): Raw ticker data.

        theoretical_value (Optional[float]): PV of box payoff (per contract for linear, total USD for inverse).

        # Long Box Metrics
        buy_cost_pre_fee (Optional[float]): Cost to buy the box before fees.
        buy_total_fees (Optional[float]): Total fees for buying the box.
        buy_cost_post_fee (Optional[float]): Cost to buy the box including fees.
        buy_profit_potential (Optional[float]): Potential profit if buying the box.

        # Short Box Metrics
        sell_proceeds_pre_fee (Optional[float]): Proceeds from selling the box before fees.
        sell_total_fees (Optional[float]): Total fees for selling the box.
        sell_proceeds_post_fee (Optional[float]): Proceeds from selling the box after fees.
        sell_profit_potential (Optional[float]): Potential profit if selling the box.

        # Opportunity Summary
        opportunity_type (Optional[str]): 'buy', 'sell', or None.
        opportunity_profit (Optional[float]): Profit potential of the identified opportunity.

        max_deployable_usd (Optional[float]): Estimated max USD/USDC value based on liquidity.
        benchmark (Dict): Timing data for initialization steps.
    """
    def __init__(self, coin: str, expiry: datetime.date, K_low: float, K_high: float,
                 call_low_order: Dict[str, Any], put_low_order: Dict[str, Any],
                 call_high_order: Dict[str, Any], put_high_order: Dict[str, Any],
                 T: float, threshold: float, underlying_price: float,
                 contract_size: float,
                 hold_to_expiry: bool = True):

        overall_start = time.perf_counter()
        self.benchmark: Dict[str, float] = {}

        # --- Input Validation ---
        if not all([coin, expiry, K_low > 0, K_high > K_low, T >= 0, underlying_price > 0, contract_size > 0]):
             raise ValueError("Invalid input parameters for BoxArbitrage (incl. contract_size)")
        required_keys = ['best_ask', 'best_bid', 'best_ask_amount', 'best_bid_amount', 'instrument_name']
        order_data_list = [call_low_order, put_low_order, call_high_order, put_high_order]
        for order_dict in order_data_list:
             # Allow None for price/amount initially, but check before use
             if not all(key in order_dict for key in required_keys):
                  raise ValueError(f"Missing required keys in order data: {order_dict}")
             # Add more specific type checks if needed before calculations

        self.coin = coin
        self.expiry = expiry
        self.K_low = K_low
        self.K_high = K_high
        self.contract_size = contract_size
        self.T = T
        self.threshold = threshold # Absolute profit threshold
        self.underlying_price = underlying_price
        self.hold_to_expiry = hold_to_expiry
        self.is_inverse = coin in {"BTC", "ETH"}

        # Store raw data
        self.call_low_data = call_low_order
        self.put_low_data = put_low_order
        self.call_high_data = call_high_order
        self.put_high_data = put_high_order

        # --- Initialize result attributes ---
        self.theoretical_value: Optional[float] = None
        self.buy_cost_pre_fee: Optional[float] = None
        self.buy_total_fees: Optional[float] = None
        self.buy_cost_post_fee: Optional[float] = None
        self.buy_profit_potential: Optional[float] = None
        self.sell_proceeds_pre_fee: Optional[float] = None
        self.sell_total_fees: Optional[float] = None
        self.sell_proceeds_post_fee: Optional[float] = None
        self.sell_profit_potential: Optional[float] = None
        self.opportunity_type: Optional[str] = None
        self.opportunity_profit: Optional[float] = None
        self.max_deployable_usd: Optional[float] = None

        # --- Perform Calculations ---
        try:
            self._calculate_theoretical_value()
            if self.is_inverse:
                self._calculate_inverse_box_metrics()
            else:
                self._calculate_linear_box_metrics()

            self._calculate_profit_potentials()
            self._determine_opportunity()
            self._calculate_max_deployable() # Calculate deployable last

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error during BoxArbitrage calculation setup for {coin} {expiry} {K_low}/{K_high}: {e}", exc_info=False) # Log less verbose
            # Attributes remain None if calculation fails early
        except Exception as e:
            logger.exception(f"Unexpected error during BoxArbitrage init for {coin} {expiry} {K_low}/{K_high}: {e}")

        self.benchmark['total_init_time'] = time.perf_counter() - overall_start
        logger.debug(f"BoxArbitrage initialized for {coin} {expiry} {K_low}/{K_high} (CS:{self.contract_size}) in {self.benchmark.get('total_init_time', 0)*1000:.3f}ms")


    def _calculate_theoretical_value(self):
        """Calculates the theoretical present value of the box payoff."""
        payoff = self.K_high - self.K_low
        pv_payoff_per_unit = math.exp(-settings.RISK_FREE_RATE * self.T) * payoff

        if self.is_inverse:
             # Theoretical value is the PV of the USD payoff
             self.theoretical_value = pv_payoff_per_unit
        else:
             # Theoretical value is the PV of the USDC payoff PER CONTRACT
             self.theoretical_value = pv_payoff_per_unit * self.contract_size

        self.benchmark['theoretical_calc'] = time.perf_counter() # Add benchmark point


    @staticmethod
    def _compute_inverse_option_fee(premium_base: float) -> float:
        """ Calculates taker fee for one inverse option contract (in base currency units). """
        # Based on previous simple interpretation: min(0.0003, 0.125 * premium)
        # Premium must be non-negative for cap calculation
        if premium_base < 0: premium_base = 0.0
        fee_from_underlying = 0.0003 # In base currency (e.g., BTC)
        fee_cap = 0.125 * premium_base
        # Ignoring settlement fee for simplicity in inverse case
        return min(fee_from_underlying, fee_cap)

    def _calculate_inverse_box_metrics(self):
        """Calculates metrics for an inverse options box spread."""
        t_start = time.perf_counter()
        mult_high = self.K_low / self.K_high # Multiplier for high strike legs

        # --- Buy Box Calculation (Cost in USD) ---
        try:
            cl_ask = self.call_low_data['best_ask']
            pl_bid = self.put_low_data['best_bid']
            ch_bid = self.call_high_data['best_bid']
            ph_ask = self.put_high_data['best_ask']

            cost_low_spread_usd = (cl_ask - pl_bid) * self.underlying_price
            proceeds_high_spread_usd = (ch_bid - ph_ask) * self.underlying_price # Note: Can be negative
            self.buy_cost_pre_fee = cost_low_spread_usd - (mult_high * proceeds_high_spread_usd)

            # Fees for buying the box
            fee_cl_ask_usd = self._compute_inverse_option_fee(cl_ask) * self.underlying_price
            fee_pl_bid_usd = self._compute_inverse_option_fee(pl_bid) * self.underlying_price
            fee_ch_bid_usd = self._compute_inverse_option_fee(ch_bid) * self.underlying_price
            fee_ph_ask_usd = self._compute_inverse_option_fee(ph_ask) * self.underlying_price

            self.buy_total_fees = fee_cl_ask_usd + fee_pl_bid_usd + mult_high * (fee_ch_bid_usd + fee_ph_ask_usd)
            self.buy_cost_post_fee = self.buy_cost_pre_fee + self.buy_total_fees
        except (KeyError, TypeError) as e:
             logger.error(f"Missing data for inverse buy box calc: {e}")
             self.buy_cost_pre_fee = self.buy_total_fees = self.buy_cost_post_fee = None


        # --- Sell Box Calculation (Proceeds in USD) ---
        try:
            cl_bid = self.call_low_data['best_bid']
            pl_ask = self.put_low_data['best_ask']
            ch_ask = self.call_high_data['best_ask']
            ph_bid = self.put_high_data['best_bid']

            proceeds_low_spread_usd = (cl_bid - pl_ask) * self.underlying_price # Can be negative
            cost_high_spread_usd = (ch_ask - ph_bid) * self.underlying_price
            self.sell_proceeds_pre_fee = proceeds_low_spread_usd - (mult_high * cost_high_spread_usd)

             # Fees for selling the box
            fee_cl_bid_usd = self._compute_inverse_option_fee(cl_bid) * self.underlying_price
            fee_pl_ask_usd = self._compute_inverse_option_fee(pl_ask) * self.underlying_price
            fee_ch_ask_usd = self._compute_inverse_option_fee(ch_ask) * self.underlying_price
            fee_ph_bid_usd = self._compute_inverse_option_fee(ph_bid) * self.underlying_price

            self.sell_total_fees = fee_cl_bid_usd + fee_pl_ask_usd + mult_high * (fee_ch_ask_usd + fee_ph_bid_usd)
            self.sell_proceeds_post_fee = self.sell_proceeds_pre_fee - self.sell_total_fees
        except (KeyError, TypeError) as e:
             logger.error(f"Missing data for inverse sell box calc: {e}")
             self.sell_proceeds_pre_fee = self.sell_total_fees = self.sell_proceeds_post_fee = None

        self.benchmark['inverse_metrics_calc'] = time.perf_counter() - t_start


    def _calculate_linear_box_metrics(self):
        """Calculates metrics for a linear (USDC) options box spread (per contract)."""
        t_start = time.perf_counter()
        cs = self.contract_size

        # --- Buy Box Calculation (Cost per contract) ---
        try:
            cl_ask = self.call_low_data['best_ask']
            pl_bid = self.put_low_data['best_bid']
            ch_bid = self.call_high_data['best_bid']
            ph_ask = self.put_high_data['best_ask']

            cost_low_spread_unit = cl_ask - pl_bid
            proceeds_high_spread_unit = ch_bid - ph_ask
            self.buy_cost_pre_fee = (cost_low_spread_unit - proceeds_high_spread_unit) * cs

            # Fees for buying the box (per contract)
            fee_cl_ask = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(cl_ask, self.underlying_price, cs, self.hold_to_expiry)
            fee_pl_bid = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(pl_bid, self.underlying_price, cs, self.hold_to_expiry)
            fee_ch_bid = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(ch_bid, self.underlying_price, cs, self.hold_to_expiry)
            fee_ph_ask = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(ph_ask, self.underlying_price, cs, self.hold_to_expiry)
            self.buy_total_fees = fee_cl_ask + fee_pl_bid + fee_ch_bid + fee_ph_ask
            self.buy_cost_post_fee = self.buy_cost_pre_fee + self.buy_total_fees
        except (KeyError, TypeError, ValueError) as e:
             logger.error(f"Missing/invalid data for linear buy box calc: {e}")
             self.buy_cost_pre_fee = self.buy_total_fees = self.buy_cost_post_fee = None

        # --- Sell Box Calculation (Proceeds per contract) ---
        try:
            cl_bid = self.call_low_data['best_bid']
            pl_ask = self.put_low_data['best_ask']
            ch_ask = self.call_high_data['best_ask']
            ph_bid = self.put_high_data['best_bid']

            proceeds_low_spread_unit = cl_bid - pl_ask
            cost_high_spread_unit = ch_ask - ph_bid
            self.sell_proceeds_pre_fee = (proceeds_low_spread_unit - cost_high_spread_unit) * cs

            # Fees for selling the box (per contract)
            fee_cl_bid = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(cl_bid, self.underlying_price, cs, self.hold_to_expiry)
            fee_pl_ask = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(pl_ask, self.underlying_price, cs, self.hold_to_expiry)
            fee_ch_ask = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(ch_ask, self.underlying_price, cs, self.hold_to_expiry)
            fee_ph_bid = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(ph_bid, self.underlying_price, cs, self.hold_to_expiry)
            self.sell_total_fees = fee_cl_bid + fee_pl_ask + fee_ch_ask + fee_ph_bid
            self.sell_proceeds_post_fee = self.sell_proceeds_pre_fee - self.sell_total_fees
        except (KeyError, TypeError, ValueError) as e:
             logger.error(f"Missing/invalid data for linear sell box calc: {e}")
             self.sell_proceeds_pre_fee = self.sell_total_fees = self.sell_proceeds_post_fee = None

        self.benchmark['linear_metrics_calc'] = time.perf_counter() - t_start


    def _calculate_profit_potentials(self):
        """Calculates potential profit for buying and selling the box."""
        if self.theoretical_value is None: return # Cannot calculate if theoretical is unknown

        if self.buy_cost_post_fee is not None:
            self.buy_profit_potential = self.theoretical_value - self.buy_cost_post_fee
        else:
             self.buy_profit_potential = None

        if self.sell_proceeds_post_fee is not None:
             self.sell_profit_potential = self.sell_proceeds_post_fee - self.theoretical_value
        else:
             self.sell_profit_potential = None

        self.benchmark['profit_potential_calc'] = time.perf_counter()


    def _determine_opportunity(self):
        """Determines if a buy or sell opportunity exists above the threshold."""
        self.opportunity_type = None
        self.opportunity_profit = 0.0

        if self.buy_profit_potential is not None and self.buy_profit_potential > self.threshold:
            self.opportunity_type = 'buy'
            self.opportunity_profit = self.buy_profit_potential
            logger.debug(f"Buy opportunity found: Profit {self.opportunity_profit:.4f} > Threshold {self.threshold:.4f}")

        # Check sell only if buy wasn't chosen (or choose the better one?)
        # For now, prioritize buy if both are profitable? Or check sell regardless. Let's check sell if buy isn't > threshold.
        elif self.sell_profit_potential is not None and self.sell_profit_potential > self.threshold:
            self.opportunity_type = 'sell'
            self.opportunity_profit = self.sell_profit_potential
            logger.debug(f"Sell opportunity found: Profit {self.opportunity_profit:.4f} > Threshold {self.threshold:.4f}")

        self.benchmark['opportunity_check'] = time.perf_counter()


    def check_opportunity(self) -> bool:
        """Checks if a profitable arbitrage opportunity (buy or sell) was identified."""
        return self.opportunity_type is not None

    def _calculate_max_deployable(self):
        """Calculates estimated max deployable USD/USDC value."""
        t_start = time.perf_counter()
        try:
            if self.is_inverse:
                # Uses per-unit prices and amounts, convert to USD
                cl_ask_usd_val = self.call_low_data['best_ask_amount'] * (self.call_low_data['best_ask'] * self.underlying_price)
                pl_bid_usd_val = self.put_low_data['best_bid_amount'] * (self.put_low_data['best_bid'] * self.underlying_price)
                ch_bid_usd_val = self.call_high_data['best_bid_amount'] * (self.call_high_data['best_bid'] * self.underlying_price)
                ph_ask_usd_val = self.put_high_data['best_ask_amount'] * (self.put_high_data['best_ask'] * self.underlying_price)
                # Consider also liquidity for selling the box? Depends on which side is executed.
                # Let's use the liquidity relevant for BUYING the box as a proxy for now.
                self.max_deployable_usd = min(cl_ask_usd_val, pl_bid_usd_val, ch_bid_usd_val, ph_ask_usd_val)
            else:
                # Linear: Amount is UNDERLYING UNITS, Price is per unit, Value is USDC
                # Value = Amount (Units) * Price_Per_Unit
                # --- CORRECTED CALCULATION ---
                val_cl_ask = self.call_low_data["best_ask_amount"] * self.call_low_data["best_ask"]
                val_pl_bid = self.put_low_data["best_bid_amount"] * self.put_low_data["best_bid"]
                val_ch_bid = self.call_high_data["best_bid_amount"] * self.call_high_data["best_bid"]
                val_ph_ask = self.put_high_data["best_ask_amount"] * self.put_high_data["best_ask"]
                # --- END CORRECTION ---
                self.max_deployable_usd = min(val_cl_ask, val_pl_bid, val_ch_bid, val_ph_ask)
        except (TypeError, KeyError, ValueError) as e:
             logger.error(f"Error calculating deployable USD: {e}")
             self.max_deployable_usd = 0.0 # Default to 0 on error
        self.benchmark['deployable_calc'] = time.perf_counter() - t_start

    def print_details(self):
        """Prints a formatted summary of the box spread analysis for both buy and sell."""
        unit = "USD" if self.is_inverse else "USDC"
        cost_unit = "USD" if self.is_inverse else f"USDC Per Contract (Size:{self.contract_size})"

        logger.info(f"\n--- [Box Spread Analysis] {self.coin} Options ---")
        logger.info(f"Expiry: {self.expiry} (T = {self.T:.4f} years)")
        logger.info(f"Strikes: Low={self.K_low}, High={self.K_high}")
        logger.info(f"Underlying Index Price: {self.underlying_price:.4f} {unit}")
        logger.info(f"Option Type: {'Inverse' if self.is_inverse else 'Linear (USDC)'} | Contract Size: {self.contract_size}")

        logger.info("-- Leg Details (Price Per Unit & Contracts Available) --")
        logger.info(f"  Call Low ({self.K_low} C): Ask={self.call_low_data.get('best_ask', 'N/A'):.4f} (Amt:{self.call_low_data.get('best_ask_amount', 'N/A')}), Bid={self.call_low_data.get('best_bid', 'N/A'):.4f} (Amt:{self.call_low_data.get('best_bid_amount', 'N/A')})")
        logger.info(f"  Put Low  ({self.K_low} P): Ask={self.put_low_data.get('best_ask', 'N/A'):.4f} (Amt:{self.put_low_data.get('best_ask_amount', 'N/A')}), Bid={self.put_low_data.get('best_bid', 'N/A'):.4f} (Amt:{self.put_low_data.get('best_bid_amount', 'N/A')})")
        logger.info(f"  Call High({self.K_high} C): Ask={self.call_high_data.get('best_ask', 'N/A'):.4f} (Amt:{self.call_high_data.get('best_ask_amount', 'N/A')}), Bid={self.call_high_data.get('best_bid', 'N/A'):.4f} (Amt:{self.call_high_data.get('best_bid_amount', 'N/A')})")
        logger.info(f"  Put High ({self.K_high} P): Ask={self.put_high_data.get('best_ask', 'N/A'):.4f} (Amt:{self.put_high_data.get('best_ask_amount', 'N/A')}), Bid={self.put_high_data.get('best_bid', 'N/A'):.4f} (Amt:{self.put_high_data.get('best_bid_amount', 'N/A')})")

        logger.info(f"-- Theoretical Value ({cost_unit}) --")
        logger.info(f"  PV of (K_high - K_low): {self.theoretical_value:.4f}" if self.theoretical_value is not None else "  PV of (K_high - K_low): N/A")

        logger.info(f"-- Buy Box Analysis ({cost_unit}) --")
        logger.info(f"  Cost (Pre-Fee): {self.buy_cost_pre_fee:.4f}" if self.buy_cost_pre_fee is not None else "  Cost (Pre-Fee): N/A")
        logger.info(f"  Total Fees:     {self.buy_total_fees:.4f}" if self.buy_total_fees is not None else "  Total Fees: N/A")
        logger.info(f"  Cost (Post-Fee):{self.buy_cost_post_fee:.4f}" if self.buy_cost_post_fee is not None else "  Cost (Post-Fee): N/A")
        logger.info(f"  Profit Potential (Theoretical - Cost): {self.buy_profit_potential:.4f}" if self.buy_profit_potential is not None else "  Profit Potential: N/A")

        logger.info(f"-- Sell Box Analysis ({cost_unit}) --")
        logger.info(f"  Proceeds (Pre-Fee): {self.sell_proceeds_pre_fee:.4f}" if self.sell_proceeds_pre_fee is not None else "  Proceeds (Pre-Fee): N/A")
        logger.info(f"  Total Fees:         {self.sell_total_fees:.4f}" if self.sell_total_fees is not None else "  Total Fees: N/A")
        logger.info(f"  Proceeds (Post-Fee):{self.sell_proceeds_post_fee:.4f}" if self.sell_proceeds_post_fee is not None else "  Proceeds (Post-Fee): N/A")
        logger.info(f"  Profit Potential (Proceeds - Theoretical): {self.sell_profit_potential:.4f}" if self.sell_profit_potential is not None else "  Profit Potential: N/A")

        logger.info("-- Opportunity Summary --")
        logger.info(f"  Threshold (Absolute Profit): {self.threshold:.4f}")
        if self.opportunity_type:
            logger.info(f"  Identified Opportunity: {self.opportunity_type.upper()} BOX")
            logger.info(f"  Estimated Profit:       {self.opportunity_profit:.4f} {unit}{'/Contract' if not self.is_inverse else ''}")
        else:
            logger.info("  No opportunity found above threshold.")

        logger.info(f"Max Deployable Value (Est. Total {unit}): {self.max_deployable_usd:.2f}" if self.max_deployable_usd is not None else "Max Deployable Value: N/A")

        logger.debug("Benchmark timings (milliseconds):")
        for key, t in self.benchmark.items():
            logger.debug(f"  {key}: {t*1000:.4f}")
        logger.info("---------------------------------------------------")
