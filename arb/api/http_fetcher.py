"""
Module for fetching initial public market data via Deribit HTTP REST API.
"""

import requests
import logging
import datetime
import json
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple

# Assuming settings are loaded elsewhere and accessible if needed,
# but DERIBIT_API_URL is defined in config.settings
from config import settings

logger = logging.getLogger(__name__)

class HTTPMarketDataFetcher:
    """
    Provides static methods to fetch public data like instruments and index prices
    using Deribit's HTTP REST API.
    """

    @staticmethod
    def get_instruments(currency: str) -> List[Dict[str, Any]]:
        """
        Fetches active option instruments for a given currency.

        Args:
            currency (str): The currency (e.g., 'BTC', 'ETH', 'USDC').

        Returns:
            List[Dict[str, Any]]: A list of instrument dictionaries, or empty list on error.
        """
        url = f"{settings.DERIBIT_API_URL}/public/get_instruments"
        params = {"currency": currency, "kind": "option", "expired": "false"}
        logger.debug(f"Fetching instruments from {url} with params: {params}")
        try:
            response = requests.get(url, params=params, timeout=10) # Increased timeout slightly
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            if 'result' not in data:
                 logger.error(f"Error fetching instruments for {currency}: 'result' key missing in response.")
                 return []
            instruments = data['result']
            logger.info(f"Fetched {len(instruments)} active option instruments for {currency}.")
            return instruments
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Error fetching instruments for {currency}: {e}")
            return []
        except json.JSONDecodeError as e:
             logger.error(f"JSON Decode Error fetching instruments for {currency}: {e}")
             return []
        except Exception as e:
            logger.exception(f"Unexpected error fetching instruments for {currency}: {e}")
            return []

    @staticmethod
    def get_underlying_spot(coin: str) -> Optional[float]:
        """
        Fetches the current index price for a given underlying coin.

        Handles the naming convention (e.g., 'btc_usd', 'sol_usd').

        Args:
            coin (str): The underlying coin symbol (e.g., 'BTC', 'SOL').

        Returns:
            Optional[float]: The index price, or None on error.
        """
        index_name = coin.lower() + "_usd"
        params = {"index_name": index_name}
        url = f"{settings.DERIBIT_API_URL}/public/get_index_price"
        logger.debug(f"Fetching index price from {url} with params: {params}")
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            if 'result' not in data or 'index_price' not in data['result']:
                 logger.error(f"Error fetching index price for {coin}: 'result' or 'index_price' key missing.")
                 return None
            index_price = data['result'].get('index_price')
            if index_price is None:
                 logger.warning(f"Index price for {coin} ({index_name}) is null in response.")
                 return None
            logger.info(f"Fetched underlying index price for {coin} ({index_name}): {index_price}")
            return float(index_price) # Ensure it's a float
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Error fetching underlying spot for {coin}: {e}")
            return None
        except json.JSONDecodeError as e:
             logger.error(f"JSON Decode Error fetching underlying spot for {coin}: {e}")
             return None
        except (ValueError, TypeError) as e:
             logger.error(f"Data type error processing index price for {coin}: {e}")
             return None
        except Exception as e:
            logger.exception(f"Unexpected error fetching underlying spot for {coin}: {e}")
            return None

    @staticmethod
    def parse_instrument_name(instrument_name: str) -> Tuple[Optional[float], Optional[datetime.date], Optional[str]]:
        """
        Parses a Deribit option instrument name into strike, expiry, and type.

        Handles different naming conventions (e.g., BTC- vs SOL_USDC-).
        Handles XRP decimal notation ('d').

        Args:
            instrument_name (str): The full instrument name.

        Returns:
            Tuple[Optional[float], Optional[datetime.date], Optional[str]]:
                strike price, expiry date, option type ('C' or 'P'), or (None, None, None) on error.
        """
        try:
            parts = instrument_name.split('-')
            if len(parts) < 4: # Allow for potential future parts, but need at least 4
                logger.warning(f"Could not parse instrument name (expected >= 4 parts): {instrument_name}")
                return None, None, None

            # Determine coin (handle SOL_USDC- style)
            if "_" in parts[0]:
                coin = parts[0].split('_')[0].upper()
            else:
                coin = parts[0].upper()

            # Expiry Date (second part)
            try:
                expiry = datetime.datetime.strptime(parts[1], "%d%b%y").date()
            except ValueError:
                 logger.error(f"Could not parse expiry date '{parts[1]}' in {instrument_name}")
                 return None, None, None

            # Strike Price (third part) - handle XRP 'd' decimal
            strike_str = parts[2]
            try:
                if coin == "XRP" and 'd' in strike_str:
                    strike = float(strike_str.replace('d', '.'))
                else:
                    strike = float(strike_str)
            except ValueError:
                 logger.error(f"Could not parse strike price '{strike_str}' in {instrument_name}")
                 return None, None, None

            # Option Type (fourth part)
            option_type = parts[3].upper()
            if option_type not in ['C', 'P']:
                 logger.warning(f"Unexpected option type '{option_type}' in {instrument_name}")
                 # Allow it for now, but might indicate parsing issue or new type
                 # return None, None, None # Stricter parsing

            return strike, expiry, option_type

        except Exception as e:
            logger.exception(f"Unexpected error parsing instrument name {instrument_name}: {e}")
            return None, None, None

    @staticmethod
    def group_options_by_strike_and_expiry(instruments: List[Dict[str, Any]]) -> Dict[Tuple[float, datetime.date], Dict[str, Any]]:
        """
        Groups a list of instrument dictionaries by (strike, expiry) pair.

        Args:
            instruments (List[Dict[str, Any]]): List of instrument data from get_instruments.

        Returns:
            Dict[Tuple[float, datetime.date], Dict[str, Any]]:
                A dictionary where keys are (strike, expiry) tuples and values are
                dictionaries like {"call": instrument_dict, "put": instrument_dict}.
        """
        options_dict = defaultdict(lambda: {"call": None, "put": None})
        parsed_count = 0
        skipped_count = 0
        for inst in instruments:
            instrument_name = inst.get('instrument_name')
            if not instrument_name:
                skipped_count += 1
                continue

            strike, expiry, option_type = HTTPMarketDataFetcher.parse_instrument_name(instrument_name)
            if strike is None or expiry is None or option_type is None:
                skipped_count += 1
                continue

            key = (strike, expiry)
            if option_type == 'C': # Standardize to 'C' and 'P'
                options_dict[key]["call"] = inst
                parsed_count +=1
            elif option_type == 'P':
                options_dict[key]["put"] = inst
                parsed_count += 1
            # else: ignore unexpected types if parse_instrument_name allows them

        logger.debug(f"Grouped {parsed_count} instruments by strike/expiry, skipped {skipped_count}.")
        return options_dict

    @staticmethod
    def group_options_by_coin(instruments: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Groups a list of instrument dictionaries by the underlying coin.

        Args:
            instruments (List[Dict[str, Any]]): List of instrument data.

        Returns:
            Dict[str, List[Dict[str, Any]]]:
                A dictionary where keys are coin symbols (str) and values are lists
                of instrument dictionaries for that coin.
        """
        coin_groups = defaultdict(list)
        for inst in instruments:
            name = inst.get('instrument_name')
            if not name:
                continue
            try:
                # Determine coin (handle SOL_USDC- style)
                if "_" in name.split('-')[0]:
                    coin = name.split('_')[0].upper()
                else:
                    coin = name.split('-')[0].upper()
                coin_groups[coin].append(inst)
            except IndexError:
                 logger.warning(f"Could not determine coin for instrument: {name}")
            except Exception as e:
                 logger.exception(f"Error grouping instrument by coin {name}: {e}")

        logger.debug(f"Grouped instruments into {len(coin_groups)} coin groups.")
        return coin_groups