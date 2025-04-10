"""
Scans for box spread arbitrage opportunities using real-time market data.
"""

import time
import logging
import datetime
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional

# Use MultiWSMarketDataFetcher for ticker data
from arb.api import HTTPMarketDataFetcher, MultiWSMarketDataFetcher
from arb.models import BoxArbitrage
from arb.utils.globals import INDEX_PRICES # Access shared index prices

logger = logging.getLogger(__name__)

class BoxArbitrageScannerWS:
    """
    Scans for box spread opportunities for a specific set of coins using
    real-time WebSocket ticker data.

    Uses a MultiWSMarketDataFetcher instance to get ticker updates and the
    global INDEX_PRICES dictionary for underlying prices.
    """
    def __init__(self,
                 ws_fetcher: MultiWSMarketDataFetcher,
                 currency: str, # Base currency of options (e.g., USDC)
                 coins: List[str], # Coins to scan within this currency (e.g., ['SOL', 'XRP'])
                 opportunity_threshold: float = 0.0, # Minimum profit threshold (e.g., $0.0 per contract/USD)   
                 hold_to_expiry: bool = True,
                 instruments: Optional[List[Dict[str, Any]]] = None):
        """
        Initializes the scanner.

        Args:
            ws_fetcher: An instance of MultiWSMarketDataFetcher providing ticker data.
            currency: The currency denomination of the options (e.g., 'USDC').
            coins: List of coin symbols to scan within this scanner instance.
            hold_to_expiry: Whether to include settlement fees in calculations.
            instruments: Pre-fetched list of relevant instrument dictionaries. If None,
                         it attempts to fetch them (though usually provided by main).
        """
        if not isinstance(ws_fetcher, MultiWSMarketDataFetcher):
             raise TypeError("ws_fetcher must be an instance of MultiWSMarketDataFetcher")

        self.ws_fetcher = ws_fetcher
        self.currency = currency
        self.coins_to_scan = coins
        self.opportunity_threshold = opportunity_threshold # Store threshold
        self.hold_to_expiry = hold_to_expiry

        if instruments is None:
            logger.warning(f"Scanner for {currency}/{coins} initialized without pre-fetched instruments. Fetching...")
            # This might fetch more than needed if multiple scanners are created
            self.instruments = HTTPMarketDataFetcher.get_instruments(currency)
        else:
            self.instruments = instruments

        # Group instruments relevant ONLY to the coins this scanner handles
        self.coin_groups = self._filter_and_group_instruments(self.instruments, self.coins_to_scan)
        logger.info(f"Scanner initialized for coins {coins}. Found relevant instruments: {sum(len(v) for v in self.coin_groups.values())}")

        # Statistics/Tracking
        self.skipped_boxes_missing_data = 0
        self.skipped_boxes_zero_price = 0
        self.total_boxes_checked = 0


    def _filter_and_group_instruments(self, all_instruments, target_coins):
        """Filters instruments for target coins and groups them."""
        target_set = set(target_coins)
        filtered = []
        for inst in all_instruments:
            name = inst.get('instrument_name')
            if not name: continue
            try:
                if "_" in name.split('-')[0]: coin = name.split('_')[0].upper()
                else: coin = name.split('-')[0].upper()
                if coin in target_set:
                    filtered.append(inst)
            except Exception:
                 logger.warning(f"Could not parse coin for instrument {name} during filtering.")
        return HTTPMarketDataFetcher.group_options_by_coin(filtered)


    def find_opportunities(self) -> List[Tuple[float, BoxArbitrage]]:
        """
        Performs one scan cycle to find box spread opportunities exceeding the threshold.

        Iterates through expiries and adjacent strikes for the configured coins,
        fetches ticker data, validates it, creates BoxArbitrage instances,
        and checks if a profitable opportunity (buy or sell) exists above the threshold.

        Returns:
            List[Tuple[float, BoxArbitrage]]: A list of tuples, where each tuple contains
                the identified profit potential (float) and the BoxArbitrage object.
                The list is sorted by profit potential descending in the main loop.
        """
        overall_start = time.perf_counter()
        # --- MODIFIED: Store profit potential directly ---
        opportunities: List[Tuple[float, BoxArbitrage]] = []
        self.total_boxes_checked = 0
        self.skipped_boxes_missing_data = 0
        self.skipped_boxes_zero_price = 0
        scan_benchmarks = {} # Track time per coin

        current_index_prices = INDEX_PRICES.copy() # Get snapshot of global prices

        for coin in self.coins_to_scan:
            coin_start_time = time.perf_counter()
            coin_instruments = self.coin_groups.get(coin, [])
            if not coin_instruments:
                logger.debug(f"No instruments found for coin {coin} in this scanner.")
                continue

            underlying_price = current_index_prices.get(coin)
            if underlying_price is None:
                logger.warning(f"Skipping scan for {coin}: Underlying index price not available.")
                continue
            if underlying_price <= 0:
                 logger.warning(f"Skipping scan for {coin}: Underlying index price is not positive ({underlying_price}).")
                 continue

            # Group by strike and expiry for this coin
            grouped_options = HTTPMarketDataFetcher.group_options_by_strike_and_expiry(coin_instruments)
            expiry_groups = defaultdict(dict)
            for (strike, expiry), option_pair in grouped_options.items():
                # Ensure both call and put exist for the strike/expiry
                if option_pair.get("call") and option_pair.get("put"):
                    expiry_groups[expiry][strike] = option_pair

            # Iterate through expiries and strike pairs
            today = datetime.date.today()
            for expiry, strikes_dict in expiry_groups.items():
                if expiry <= today: # Skip expired or same-day expiry
                    continue

                T_days = (expiry - today).days
                if T_days <= 0: continue # Should be covered by above, but safety check
                T = T_days / 365.25 # Time to expiry in years

                strikes = sorted(strikes_dict.keys())
                num_strikes = len(strikes)
                for i in range(num_strikes - 1): # Outer loop for K_low
                    for j in range(i + 1, num_strikes): # Inner loop for K_high (j > i)
                        K_low = strikes[i]
                        K_high = strikes[j]
                        lower_pair = strikes_dict[K_low]
                        higher_pair = strikes_dict[K_high]
                        self.total_boxes_checked += 1

                        # Fetch latest ticker data for all four legs
                        cl_name = lower_pair['call']['instrument_name']
                        pl_name = lower_pair['put']['instrument_name']
                        ch_name = higher_pair['call']['instrument_name']
                        ph_name = higher_pair['put']['instrument_name']

                        cl_ticker = self.ws_fetcher.get_ticker(cl_name)
                        pl_ticker = self.ws_fetcher.get_ticker(pl_name)
                        ch_ticker = self.ws_fetcher.get_ticker(ch_name)
                        ph_ticker = self.ws_fetcher.get_ticker(ph_name)

                        # --- Get Contract Size ---
                        # Assume contract size is the same for call and put at the same strike/expiry
                        # Get it from one of the instruments (e.g., lower call)
                        contract_size = lower_pair['call'].get('contract_size')
                        if contract_size is None or contract_size <= 0:
                            logger.warning(f"Skipping box {coin} {expiry} {K_low}/{K_high}: Missing or invalid contract_size ({contract_size}) in instrument data for {lower_pair['call']['instrument_name']}")
                            self.skipped_boxes_missing_data += 1 # Count as missing data
                            continue
                        try:
                            # Ensure contract_size is a number (float or int)
                            contract_size = float(contract_size)
                        except (ValueError, TypeError):
                            logger.warning(f"Skipping box {coin} {expiry} {K_low}/{K_high}: Invalid contract_size format ({contract_size}) for {lower_pair['call']['instrument_name']}")
                            self.skipped_boxes_missing_data += 1
                            continue
                        # --- End Get Contract Size ---
                        
                        # --- Data Validation ---
                        tickers = [cl_ticker, pl_ticker, ch_ticker, ph_ticker]
                        if any(t is None for t in tickers):
                            self.skipped_boxes_missing_data += 1
                            logger.debug(f"Skipping box {coin} {expiry} {K_low}/{K_high}: Missing ticker data.")
                            continue

                        required_fields = ["best_ask_price", "best_bid_price", "best_ask_amount", "best_bid_amount"]
                        has_missing_fields = False
                        has_zero_price = False
                        for ticker in tickers:
                            if not all(field in ticker and ticker[field] is not None for field in required_fields):
                                has_missing_fields = True
                                break
                            if ticker["best_ask_price"] <= 0 or ticker["best_bid_price"] <= 0:
                                has_zero_price = True
                                break
                        if has_missing_fields:
                            self.skipped_boxes_missing_data += 1
                            logger.debug(f"Skipping box {coin} {expiry} {K_low}/{K_high}: Ticker missing required fields.")
                            continue
                        if has_zero_price:
                            self.skipped_boxes_zero_price += 1
                            logger.debug(f"Skipping box {coin} {expiry} {K_low}/{K_high}: Ticker has zero bid/ask price.")
                            continue
                        # --- End Validation ---


                        # Create BoxArbitrage instance - use 0 threshold for finding all deviations
                        try:
                            # Prepare order dicts with consistent keys expected by BoxArbitrage
                            call_low_order_data = {
                                "best_ask": cl_ticker["best_ask_price"],
                                "best_bid": cl_ticker["best_bid_price"],
                                "best_ask_amount": cl_ticker["best_ask_amount"], # RAW amount (underlying units)
                                "best_bid_amount": cl_ticker["best_bid_amount"],
                                "instrument_name": cl_name
                            }
                            put_low_order_data = {
                                "best_ask": pl_ticker["best_ask_price"],
                                "best_bid": pl_ticker["best_bid_price"],
                                "best_ask_amount": pl_ticker["best_ask_amount"],
                                "best_bid_amount": pl_ticker["best_bid_amount"],
                                "instrument_name": pl_name
                            }
                            call_high_order_data = {
                                "best_ask": ch_ticker["best_ask_price"],
                                "best_bid": ch_ticker["best_bid_price"],
                                "best_ask_amount": ch_ticker["best_ask_amount"],
                                "best_bid_amount": ch_ticker["best_bid_amount"],
                                "instrument_name": ch_name
                            }
                            put_high_order_data = {
                                "best_ask": ph_ticker["best_ask_price"],
                                "best_bid": ph_ticker["best_bid_price"],
                                "best_ask_amount": ph_ticker["best_ask_amount"],
                                "best_bid_amount": ph_ticker["best_bid_amount"],
                                "instrument_name": ph_name
                            }

                            box = BoxArbitrage(
                                coin=coin, expiry=expiry, K_low=K_low, K_high=K_high,
                                call_low_order=call_low_order_data,
                                put_low_order=put_low_order_data,
                                call_high_order=call_high_order_data,
                                put_high_order=put_high_order_data,
                                T=T,
                                threshold=self.opportunity_threshold,
                                underlying_price=underlying_price,
                                contract_size = contract_size,
                                hold_to_expiry=self.hold_to_expiry
                            )

                            # --- MODIFIED: Use BoxArbitrage's opportunity check ---
                            if box.check_opportunity():
                                # Opportunity found (either buy or sell profit > threshold)
                                # Append the actual calculated profit and the box object
                                if box.opportunity_profit is not None: # Should not be None if check_opportunity is True
                                    opportunities.append((box.opportunity_profit, box))
                                    logger.debug(f"Found opportunity: {box.opportunity_type} box for {coin} {expiry} {K_low}/{K_high}, Profit: {box.opportunity_profit:.4f}")
                                else:
                                    # This case should ideally not happen if check_opportunity is True
                                    logger.warning(f"Box check_opportunity was True but opportunity_profit is None for {coin} {expiry} {K_low}/{K_high}")
                            # --- END MODIFICATION ---

                        except (ValueError, TypeError, KeyError) as e:
                            logger.error(f"Error creating BoxArbitrage for {coin} {expiry} {K_low}/{K_high}: {e}")
                        except Exception as e:
                            logger.exception(f"Unexpected error processing box {coin} {expiry} {K_low}/{K_high}: {e}")

            scan_benchmarks[coin] = time.perf_counter() - coin_start_time

        overall_duration = time.perf_counter() - overall_start
        logger.info(
            f"Scan completed in {overall_duration*1000:.2f}ms. "
            f"Checked: {self.total_boxes_checked}, Found: {len(opportunities)}, "
            f"Skipped (Data): {self.skipped_boxes_missing_data}, Skipped (ZeroPx): {self.skipped_boxes_zero_price}"
        )
        times_ms = {c: f"{t * 1000:.2f}" for c, t in scan_benchmarks.items()}
        logger.debug(f"Per-coin scan times (ms): {times_ms}")
        return opportunities

    # Optional: Add a simple scan method if needed outside main loop
    # def scan(self):
    #     start_time = time.perf_counter()
    #     opps = self.find_opportunities()
    #     end_time = time.perf_counter()
    #     if not opps:
    #         logger.info("No box spread opportunities found in this scan.")
    #         return
    #     # Sort by absolute difference magnitude
    #     sorted_opps = sorted(opps, key=lambda x: abs(x[0]), reverse=True)
    #     logger.info("=== Top 3 Box Spread Opportunities Found ===")
    #     for diff, box in sorted_opps[:3]:
    #         box.print_details() # Assumes BoxArbitrage has print_details
    #     logger.info(f"Scan duration: {(end_time - start_time)*1000:.2f}ms")