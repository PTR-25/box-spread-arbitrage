"""
Module for handling public Deribit WebSocket subscriptions (tickers, index prices).

These connections do not require authentication.
"""

import asyncio
import websockets
import json
import logging
import time
from typing import List, Dict, Any, Optional

from config import settings
# Import the shared global dictionary for index prices
from arb.utils.globals import INDEX_PRICES

logger = logging.getLogger(__name__)

# --- Index Price Subscription ---

async def subscribe_index_prices(coins: List[str]):
    """
    Connects to Deribit WS and subscribes to index price channels for given coins.
    Updates the global INDEX_PRICES dictionary. Runs indefinitely until cancelled.

    Args:
        coins (List[str]): List of coin symbols (e.g., ['BTC', 'ETH', 'SOL']).
    """
    ws_url = settings.DERIBIT_WS_URL
    channels = [f"deribit_price_index.{coin.lower()}_usd" for coin in coins]
    if not channels:
        logger.warning("subscribe_index_prices called with no coins.")
        return

    while True: # Outer loop for reconnection
        try:
            logger.info(f"Connecting to {ws_url} for index price subscription...")
            async with websockets.connect(ws_url, ping_interval=None) as ws:
                logger.info(f"Connected for index prices. Subscribing to: {channels}")
                subscription_msg = {
                    "jsonrpc": "2.0",
                    "id": 9001, # Unique ID for this subscription request
                    "method": "public/subscribe",
                    "params": {"channels": channels}
                }
                await ws.send(json.dumps(subscription_msg))

                # Handle initial subscription confirmation (optional)
                # try:
                #     conf = await asyncio.wait_for(ws.recv(), timeout=5.0)
                #     logger.debug(f"Index subscription confirmation: {conf}")
                # except asyncio.TimeoutError:
                #     logger.warning("Timeout waiting for index subscription confirmation.")

                # Process incoming messages
                async for message in ws:
                    try:
                        data = json.loads(message)
                        logger.debug(f"Index WS RECV: {data}")

                        # Handle test requests (heartbeat mechanism)
                        if data.get("method") == "test_request":
                            test_msg = {
                                "jsonrpc": "2.0",
                                "id": data.get("id", 9099), # Echo ID if possible
                                "method": "public/test",
                                "params": {}
                            }
                            await ws.send(json.dumps(test_msg))
                            logger.debug("Index WS: Responded to test_request")
                            continue

                        # Process subscription updates
                        params = data.get("params", {})
                        channel = params.get("channel", "")
                        if channel.startswith("deribit_price_index"):
                            price_data = params.get("data", {})
                            index_price = price_data.get("price")
                            # Extract coin (e.g., "SOL" from "deribit_price_index.sol_usd")
                            try:
                                coin = channel.split('.')[1].split('_')[0].upper()
                            except IndexError:
                                logger.warning(f"Could not parse coin from index channel: {channel}")
                                continue

                            if index_price is not None:
                                try:
                                    INDEX_PRICES[coin] = float(index_price)
                                    logger.debug(f"Updated global index price for {coin}: {INDEX_PRICES[coin]}")
                                except (ValueError, TypeError):
                                     logger.error(f"Invalid index price format for {coin}: {index_price}")
                            else:
                                logger.warning(f"Received null index price for {coin} on channel {channel}")

                    except json.JSONDecodeError:
                        logger.error(f"Index WS: Failed to decode JSON: {message}")
                    except Exception as e:
                        logger.exception(f"Index WS: Error processing message: {e}")

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.InvalidStatusCode, OSError) as e:
            logger.error(f"Index WS connection error: {e}. Reconnecting in 5 seconds...")
        except asyncio.CancelledError:
             logger.info("Index price subscription task cancelled.")
             break # Exit the loop if cancelled
        except Exception as e:
            logger.exception(f"Index WS unexpected error: {e}. Reconnecting in 5 seconds...")

        await asyncio.sleep(5) # Wait before reconnecting


# --- Ticker Data Subscription ---

class WSMarketDataFetcher:
    """
    Manages a single public WebSocket connection to subscribe to ticker data
    for a batch of instruments. Handles heartbeats and reconnections.
    """
    def __init__(self, instruments: List[str], heartbeat_interval: int = 30):
        """
        Args:
            instruments (List[str]): List of instrument names to subscribe tickers for.
            heartbeat_interval (int): Interval in seconds for setting heartbeat. Min 10.
        """
        if not instruments:
             raise ValueError("WSMarketDataFetcher requires a list of instruments.")
        self.instruments = instruments
        self.heartbeat_interval = max(10, heartbeat_interval) # Enforce minimum
        self.ticker_cache: Dict[str, Dict[str, Any]] = {}
        self.ws_url = settings.DERIBIT_WS_URL
        self._stop_event = asyncio.Event() # For graceful shutdown if needed
        self._connection_id = f"TickerWS-{instruments[0].split('-')[0][:3]}-{len(instruments)}" # Simple ID for logging
        logger.info(f"[{self._connection_id}] Initialized for {len(instruments)} instruments.")

    async def _set_heartbeat(self, ws):
        """Sends the command to set the heartbeat interval."""
        heartbeat_msg = {
            "jsonrpc": "2.0",
            "id": hash(self._connection_id) + 100, # Semi-unique ID
            "method": "public/set_heartbeat",
            "params": {"interval": self.heartbeat_interval}
        }
        try:
            await ws.send(json.dumps(heartbeat_msg))
            logger.info(f"[{self._connection_id}] Heartbeat set to {self.heartbeat_interval} seconds.")
        except Exception as e:
             logger.error(f"[{self._connection_id}] Failed to send set_heartbeat message: {e}")


    async def _subscribe_channels(self, ws):
        """Subscribes to the ticker channels for the assigned instruments."""
        channels = [f"ticker.{inst}.100ms" for inst in self.instruments]
        if not channels:
            logger.warning(f"[{self._connection_id}] No channels to subscribe to.")
            return

        subscription_msg = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": hash(self._connection_id) + 101, # Semi-unique ID
            "params": {"channels": channels}
        }
        try:
            await ws.send(json.dumps(subscription_msg))
            logger.info(f"[{self._connection_id}] Subscribed to {len(channels)} ticker channels (100ms updates).")
        except Exception as e:
            logger.error(f"[{self._connection_id}] Failed to send subscribe message: {e}")

    async def _process_messages(self, ws):
        """Listens for and processes incoming messages on the WebSocket."""
        async for message in ws:
            # start_time = time.perf_counter() # Benchmarking removed for clarity
            try:
                msg = json.loads(message)
                logger.debug(f"[{self._connection_id}] RECV: {msg}")

                # Handle test requests (heartbeat mechanism)
                if msg.get("method") == "test_request":
                    test_msg = {
                        "jsonrpc": "2.0",
                        "id": msg.get("id", hash(self._connection_id) + 999), # Echo ID if possible
                        "method": "public/test",
                        "params": {}
                    }
                    await ws.send(json.dumps(test_msg))
                    logger.debug(f"[{self._connection_id}] Responded to test_request")
                    continue

                # Process subscription updates
                if msg.get("method") == "subscription":
                    params = msg.get("params", {})
                    channel = params.get("channel")
                    data = params.get("data", {})
                    if channel and channel.startswith("ticker.") and data:
                        try:
                            # Extract instrument name (e.g., BTC-PERPETUAL from ticker.BTC-PERPETUAL.100ms)
                            instrument = channel.split('.')[1]
                            # Update cache - ensure data is a dict
                            #if "SOL_USDC-30MAY25-100-P" in instrument: # Example instrument
                            #    logger.info(f"RAW TICKER DATA for {instrument}: {json.dumps(data)}")
                            if isinstance(data, dict):
                                self.ticker_cache[instrument] = data
                            else:
                                 logger.warning(f"[{self._connection_id}] Received non-dict data for ticker {instrument}: {type(data)}")
                        except IndexError:
                            logger.warning(f"[{self._connection_id}] Could not parse instrument from ticker channel: {channel}")

            except json.JSONDecodeError:
                logger.error(f"[{self._connection_id}] Failed to decode JSON: {message}")
            except Exception as e:
                logger.exception(f"[{self._connection_id}] Error processing message: {e}")
            # end_time = time.perf_counter()
            # processing_latency = (end_time - start_time) * 1000
            # logger.debug(f"[{self._connection_id}] Processed WS message in {processing_latency:.2f}ms")


    async def run(self):
        """Runs the WebSocket connection loop, handling reconnections."""
        while not self._stop_event.is_set():
            try:
                logger.info(f"[{self._connection_id}] Connecting to {self.ws_url}...")
                async with websockets.connect(self.ws_url, ping_interval=None) as ws:
                    logger.info(f"[{self._connection_id}] Connected.")
                    # Setup connection specifics
                    await self._set_heartbeat(ws)
                    await self._subscribe_channels(ws)
                    # Listen for messages until connection closes or stop is requested
                    await self._process_messages(ws)

            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.InvalidStatusCode, OSError) as e:
                logger.error(f"[{self._connection_id}] Connection error: {e}.")
            except asyncio.CancelledError:
                 logger.info(f"[{self._connection_id}] Run task cancelled.")
                 break # Exit loop if cancelled
            except Exception as e:
                logger.exception(f"[{self._connection_id}] Unexpected error in run loop: {e}")

            if not self._stop_event.is_set():
                 logger.info(f"[{self._connection_id}] Reconnecting in 5 seconds...")
                 await asyncio.sleep(5)
            else:
                 logger.info(f"[{self._connection_id}] Stop requested, not reconnecting.")

        logger.info(f"[{self._connection_id}] Run loop finished.")


    def get_ticker(self, instrument: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the latest cached ticker data for a specific instrument.

        Args:
            instrument (str): The instrument name.

        Returns:
            Optional[Dict[str, Any]]: The ticker data dictionary, or None if not found.
        """
        return self.ticker_cache.get(instrument, None)

    async def stop(self):
        """Signals the run loop to stop."""
        logger.info(f"[{self._connection_id}] Stop requested.")
        self._stop_event.set()


class MultiWSMarketDataFetcher:
    """
    Aggregates multiple WSMarketDataFetcher instances if the number of
    instruments exceeds MAX_INSTRUMENTS_PER_WS. Provides a unified interface.
    """
    def __init__(self, instruments: List[str], heartbeat_interval: int = 30):
        """
        Args:
            instruments (List[str]): The full list of instrument names.
            heartbeat_interval (int): Heartbeat interval for underlying fetchers.
        """
        self.fetchers: List[WSMarketDataFetcher] = []
        self._tasks: List[asyncio.Task] = []
        max_instruments = settings.MAX_INSTRUMENTS_PER_WS

        if not instruments:
             logger.warning("MultiWSMarketDataFetcher initialized with no instruments.")
             return

        for i in range(0, len(instruments), max_instruments):
            batch = instruments[i:i+max_instruments]
            if batch: # Ensure batch is not empty
                fetcher = WSMarketDataFetcher(batch, heartbeat_interval)
                self.fetchers.append(fetcher)
        logger.info(f"Initialized MultiWSMarketDataFetcher with {len(self.fetchers)} underlying fetcher(s).")

    async def run(self):
        """Starts the run loops for all underlying fetchers concurrently."""
        if not self.fetchers:
            logger.warning("MultiWSMarketDataFetcher has no fetchers to run.")
            return

        logger.info(f"Starting {len(self.fetchers)} market data fetcher task(s)...")
        self._tasks = [asyncio.create_task(fetcher.run()) for fetcher in self.fetchers]
        # Wait for all tasks to complete (e.g., if they are stopped)
        # This might run indefinitely if fetchers don't stop
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("MultiWSMarketDataFetcher run finished (all tasks completed or cancelled).")


    def get_ticker(self, instrument: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the latest ticker data for an instrument from the appropriate
        underlying fetcher. Returns the one with the latest timestamp if multiple
        fetchers somehow have the same instrument (shouldn't happen with proper batching).

        Args:
            instrument (str): The instrument name.

        Returns:
            Optional[Dict[str, Any]]: The latest ticker data, or None.
        """
        latest_ticker = None
        latest_time = 0
        for fetcher in self.fetchers:
            # Check if the instrument belongs to this fetcher's batch
            # Optimization: Could pre-map instruments to fetchers if performance critical
            ticker = fetcher.get_ticker(instrument)
            if ticker:
                timestamp = ticker.get("timestamp", 0)
                # Check timestamp for freshness (optional, assumes cache has latest)
                if timestamp > latest_time:
                    latest_time = timestamp
                    latest_ticker = ticker
                # Since instruments should be unique per fetcher, we can often break early
                # break # Uncomment if absolutely sure instruments are uniquely assigned

        return latest_ticker

    async def stop(self):
        """Stops all underlying fetcher tasks by cancelling them."""
        logger.info("Stopping all underlying market data fetchers by cancelling tasks...")

        if not self._tasks:
            logger.warning("No market data fetcher tasks found to stop.")
            return

        # 1. Cancel the tasks running the fetcher.run() loops
        cancelled_count = 0
        for task in self._tasks:
            if task and not task.done():
                logger.debug(f"Cancelling market data fetcher task: {task.get_name()}")
                task.cancel()
                cancelled_count += 1
        logger.info(f"Requested cancellation for {cancelled_count} market data fetcher task(s).")

        # 2. Wait for the tasks to finish cancellation WITH A TIMEOUT
        logger.info(f"Waiting up to 5 seconds for {len(self._tasks)} market data fetcher tasks to finish cancellation...")
        try:
            # --- ADD TIMEOUT HERE ---
            results = await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=5.0
            )
            # --- END TIMEOUT ---
            logger.info("Market data fetcher task gathering complete within timeout.")

            # Optional: Log results if needed (as before)
            # for i, result in enumerate(results): ...

        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for market data fetcher tasks to finish cancellation. Proceeding with shutdown.")
            # Log which tasks might still be running
            for i, task in enumerate(self._tasks):
                 if task and not task.done():
                      logger.warning(f"  - Task {task.get_name()} may not have terminated cleanly.")
        except Exception as e:
             logger.exception(f"Error during market data fetcher task gathering: {e}")


        logger.info("Market data fetcher stop sequence finished.")
        # Note: Even if tasks didn't terminate, the main script will continue shutdown.