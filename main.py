"""
Main entry point for the Deribit Arbitrage Bot application.

Initializes components, starts data fetching, runs the scanning loop,
and potentially executes arbitrage opportunities.
"""

import asyncio
import logging
import os
import signal # For graceful shutdown
import time
import websockets

# Load configuration first

from dotenv import load_dotenv # <--- IMPORT load_dotenv
# --- Load environment variables from .env file ---
# Load .env file located in the project root directory BEFORE importing settings
dotenv_path = os.path.join(os.path.dirname(__file__), '.env') # Explicitly point to .env in the same dir as main.py
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)
    print(f"Loaded environment variables from: {dotenv_path}") # Optional: confirmation
else:
    print("Warning: .env file not found. Relying on system environment variables.")
# -------------------------------------------------

# Now import settings AFTER .env has been loaded
from config import settings # <--- Import settings AFTER load_dotenv()
# Import core components
from arb.api import (
    DeribitAPIClient,
    HTTPMarketDataFetcher,
    MultiWSMarketDataFetcher,
    subscribe_index_prices,
    DeribitAPIError,
)
from arb.services import BoxArbitrageScannerWS
from arb.utils import setup_logging # Import logging setup

# Setup logging as early as possible
# Read log level from environment variable or default to INFO
log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
setup_logging(level=log_level)

# Get the main logger for this script
logger = logging.getLogger(__name__)

# --- Global flag for graceful shutdown ---
shutdown_requested = False
shutdown_event = asyncio.Event()

def handle_shutdown_signal(sig, frame):
    """Sets the shutdown event when a signal is received."""
    logger.warning(f"Received signal {sig}. Initiating graceful shutdown...")
    shutdown_event.set()

async def run_client_connection(client: DeribitAPIClient, stop_event: asyncio.Event):
    reconnect_attempts = 0
    max_reconnect_attempts = 5
    reconnect_delay = 5

    while not stop_event.is_set():
        websocket_protocol = None  # Reset for each attempt
        try:
            logger.info(f"Attempting to connect to {settings.DERIBIT_WS_URL} (Attempt {reconnect_attempts + 1})...")
            async with websockets.connect(settings.DERIBIT_WS_URL, ping_interval=30) as connection_object:
                logger.info(f"Object yielded by 'async with connect': type={type(connection_object)}")
                if (hasattr(connection_object, 'send') and
                        hasattr(connection_object, 'recv') and
                        hasattr(connection_object, 'close')):
                    websocket_protocol = connection_object
                    logger.info(f"Usable WebSocket protocol object obtained (ID: {getattr(websocket_protocol, 'id', 'N/A')}).")
                else:
                    logger.error(f"Object yielded by connect (type: {type(connection_object)}) doesn't have required methods.")
                    continue  # Skip to reconnect logic

                reconnect_attempts = 0  # Reset attempts

                await client.set_websocket(websocket_protocol)

                # Wait for connection to be OPEN
                from websockets.protocol import State
                timeout = 5.0  # increased timeout
                start = time.monotonic()
                while True:
                    async with client._ws_lock:
                        ws = client._ws
                    logger.debug(f"Polling: current websocket state: {ws.state if ws else 'None'}")
                    if ws and ws.state == State.OPEN:
                        logger.info("WebSocket is OPEN.")
                        break
                    if time.monotonic() - start > timeout:
                        logger.error("Timeout waiting for WebSocket to become OPEN.")
                        break
                    await asyncio.sleep(0.1)

                async with client._ws_lock:
                    ws = client._ws
                if not ws or ws.state != State.OPEN:
                    logger.error("WebSocket did not become OPEN in time; closing connection and retrying.")
                    await client.set_websocket(None)
                    continue

                # **Start the listener loop in background so it processes incoming messages**
                listener_task = asyncio.create_task(client.run_listener_loop())

                # Now attempt authentication while the listener is running
                if await client.authenticate():
                    logger.info(f"Authentication successful for WSID {getattr(websocket_protocol, 'id', 'N/A')}.")
                    # Optionally, you can await the listener loop if you want to run it to completion:
                    await listener_task
                    logger.info(f"Listener loop finished for WSID {getattr(websocket_protocol, 'id', 'N/A')}.")
                else:
                    logger.error(f"Authentication failed for WSID {getattr(websocket_protocol, 'id', 'N/A')}. Cancelling listener and closing connection.")
                    listener_task.cancel()
                    await client.set_websocket(None)

            logger.debug("Exited 'async with' block.")
            if client.is_connected:
                logger.warning("Clearing client websocket state after 'async with' exit.")
                await client.set_websocket(None)

        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.InvalidStatusCode,
                websockets.exceptions.InvalidHandshake,
                OSError) as e:
            logger.error(f"WebSocket connection error: {e}. Attempting reconnect...")
            await client.set_websocket(None)
        except asyncio.CancelledError:
            logger.info("Client connection task cancelled.")
            await client.set_websocket(None)
            break
        except Exception as e:
            logger.exception(f"Unexpected error in connection loop: {e}. Attempting reconnect...")
            await client.set_websocket(None)

        # --- Reconnect Logic ---
        if not stop_event.is_set():
            reconnect_attempts += 1
            if reconnect_attempts >= max_reconnect_attempts:
                logger.critical(f"Max reconnect attempts ({max_reconnect_attempts}) reached. Stopping connection attempts.")
                stop_event.set()
                break
            logger.info(f"Waiting {reconnect_delay} seconds before reconnecting...")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay)
                if stop_event.is_set():
                    logger.info("Stop requested during reconnect delay.")
                    break
            except asyncio.TimeoutError:
                pass

    logger.info("Client connection loop finished.")



async def main_runner():
    """Main asynchronous execution function."""
    global shutdown_requested

    api_client = None
    market_data_fetcher = None
    all_tasks = [] # Keep track of all background tasks

    try:
        # --- Initialize API Client ---
        if not settings.CLIENT_ID or not settings.CLIENT_SECRET:
             logger.critical("CRITICAL: Deribit Client ID or Secret not configured in environment variables.")
             logger.critical("Please set DERIBIT_CLIENT_ID and DERIBIT_CLIENT_SECRET.")
             return # Cannot proceed without credentials

        api_client = DeribitAPIClient(
            client_id=settings.CLIENT_ID,
            client_secret=settings.CLIENT_SECRET,
        )

        # --- Start Background Data Fetching ---
        logger.info("Starting background data fetching tasks...")
        
        # Start the NEW external connection loop for the API client
        client_connection_task = asyncio.create_task(
            run_client_connection(api_client, shutdown_event)
        )
        all_tasks.append(client_connection_task)
        
        # Start index price subscription
        index_price_task = asyncio.create_task(subscribe_index_prices(settings.COINS_FOR_INDEX_PRICE))

        # Fetch initial instrument list
        all_instruments = []
        for curr in settings.FETCH_INSTRUMENT_CURRENCIES:
            instruments = HTTPMarketDataFetcher.get_instruments(curr)
            all_instruments.extend(instruments)
        logger.info(f"Total instruments fetched initially: {len(all_instruments)}")

        # Filter instruments for desired coins
        filtered_instruments_dict = {
            inst["instrument_name"]: inst for inst in all_instruments
            if inst.get("instrument_name") and (
                (inst["instrument_name"].split('_')[0].upper() if "_" in inst["instrument_name"].split('-')[0]
                 else inst["instrument_name"].split('-')[0].upper())
                in settings.DESIRED_COINS_SET
            )
        }
        filtered_instruments = list(filtered_instruments_dict.values())
        instrument_names = list(filtered_instruments_dict.keys())
        logger.info(f"Filtered instruments for desired coins: {len(filtered_instruments)}")

        if not instrument_names:
             logger.warning("No relevant instruments found for desired coins. Scanner may not find opportunities.")
             # Consider exiting or handling this case differently

        # Start public ticker data fetcher
        market_data_fetcher = MultiWSMarketDataFetcher(instrument_names, heartbeat_interval=30)
        market_data_task = asyncio.create_task(market_data_fetcher.run())
        all_tasks.append(market_data_task)
        logger.info("Waiting 5 seconds for market data caches to populate...")
        await asyncio.sleep(5)

        # --- Wait for initial connection and auth ---
        logger.info("Waiting up to 15 seconds for initial API client connection and authentication...")
        try:
            # Wait until connected and authenticated, or timeout
            async with asyncio.timeout(15):
                 while not api_client.is_authenticated:
                      if shutdown_event.is_set(): # Check if shutdown requested during wait
                           raise asyncio.CancelledError("Shutdown requested during startup.")
                      if not client_connection_task.done(): # Check if connection task failed early
                           await asyncio.sleep(0.5)
                      else:
                           logger.error("Client connection task finished unexpectedly during startup.")
                           raise ConnectionError("Client connection task failed.")
        except TimeoutError:
             logger.critical("API client did not authenticate within timeout. Exiting.")
             return # Exit if initial auth fails
        except (ConnectionError, asyncio.CancelledError) as e:
             logger.critical(f"API client startup failed: {e}. Exiting.")
             return

        logger.info("API client ready. Initializing scanners...")

        # --- Initialize Scanners ---
        logger.info("Initializing arbitrage scanners...")
        coin_groups = HTTPMarketDataFetcher.group_options_by_coin(filtered_instruments)
        scanners = {}
        for coin in settings.DESIRED_COINS_SET:
            inst_list_for_coin = coin_groups.get(coin, [])
            if not inst_list_for_coin:
                logger.debug(f"No instruments for scanner for coin {coin}.")
                continue
            # Assuming all desired coins use USDC options for this example config
            scanner_currency = "USDC"
            scanners[coin] = BoxArbitrageScannerWS(
                ws_fetcher=market_data_fetcher,
                currency=scanner_currency,
                coins=[coin], # Each scanner instance focuses on one coin
                opportunity_threshold=settings.OPPORTUNITY_THRESHOLD,
                hold_to_expiry=True, # Configurable?
                instruments=inst_list_for_coin
            )
            logger.info(f"Created scanner for {coin} options (instruments: {len(inst_list_for_coin)}).")

        if not scanners:
             logger.critical("No scanners were initialized. Check instrument fetching and desired coins. Exiting.")
             # Clean up background tasks before exiting
             index_price_task.cancel()
             market_data_task.cancel()
             await asyncio.gather(index_price_task, market_data_task, return_exceptions=True)
             return

        # --- Main Scan Loop ---
        logger.info("Starting main scan loop...")
        while not shutdown_event.is_set():
            cycle_start = time.perf_counter()

            # Check API connection status (optional, errors during send handle it too)
            if not api_client.is_authenticated:
                 logger.warning("API Client appears disconnected. Waiting for reconnection loop...")
                 # Don't try to reconnect here, let the run_client_connection task handle it
                 await asyncio.sleep(settings.RECONNECT_DELAY) # Wait before next cycle check
                 continue

            logger.debug("--- Scan Cycle Start ---")

            # Run scanners concurrently
            scan_tasks = [
                asyncio.to_thread(scanner.find_opportunities)
                for scanner in scanners.values()
                # Add check if scanner is ready or has data if needed
            ]
            if not scan_tasks:
                 logger.warning("No active scanners to run this cycle.")
                 await asyncio.sleep(1); continue # Avoid busy-waiting

            scan_results = await asyncio.gather(*scan_tasks, return_exceptions=True)

            # Aggregate opportunities
            all_opps = []
            for i, result in enumerate(scan_results):
                scanner_coin = list(scanners.keys())[i] # Get coin associated with result
                if isinstance(result, Exception):
                    logger.error(f"Error running scanner for {scanner_coin}: {result}")
                elif isinstance(result, list):
                    all_opps.extend(result)
                else:
                     logger.warning(f"Unexpected result type from scanner for {scanner_coin}: {type(result)}")


            if all_opps:
                # Sort by absolute difference magnitude (potential profit/loss)
                sorted_opps = sorted(all_opps, key=lambda x: abs(x[0]), reverse=True)

                # Log top opportunities
                logger.info("--- Top 3 Potential Box Spread Opportunities ---")
                for diff, box in sorted_opps[:3]:
                    box.print_details()  # Print full details

            else:
                logger.info("No box spread opportunities found in this scan cycle.")

            # --- Cycle End ---
            cycle_end = time.perf_counter()
            logger.debug(f"--- Scan cycle finished in {(cycle_end - cycle_start)*1000:.2f}ms ---")
            sleep_time = 1.0
            # Use wait_for on the shutdown event for faster exit during sleep
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
                if shutdown_event.is_set(): break # Exit loop if event set during sleep
            except TimeoutError:
                pass # Sleep finished normally

    except asyncio.CancelledError:
        logger.info("Main runner task cancelled.")
    except Exception as e:
        logger.exception("Unhandled exception in main_runner:")
    finally:
        logger.info("Initiating shutdown...")
        shutdown_event.set() # Ensure event is set for all tasks

        # Gracefully shut down background tasks
        if 'market_data_fetcher' in locals() and market_data_fetcher: # Check if initialized
             logger.info("Stopping market data fetcher...")
             await market_data_fetcher.stop() # Stops underlying WS fetchers

        # Cancel remaining tasks (client connection, index prices)
        logger.info("Cancelling remaining background tasks...")
        # Use the 'all_tasks' list we maintained
        tasks_to_cancel = []
        if 'client_connection_task' in locals() and client_connection_task and not client_connection_task.done():
             tasks_to_cancel.append(client_connection_task)
        if 'index_price_task' in locals() and index_price_task and not index_price_task.done():
             tasks_to_cancel.append(index_price_task)
        # The market_data_task should finish due to market_data_fetcher.stop()
        # but we await it later anyway.

        for task in tasks_to_cancel:
             task.cancel()

        # Wait for all tasks to finish
        logger.info("Waiting for background tasks to complete...")
        tasks_to_await = []
        if 'client_connection_task' in locals() and client_connection_task: tasks_to_await.append(client_connection_task)
        if 'index_price_task' in locals() and index_price_task: tasks_to_await.append(index_price_task)
        if 'market_data_task' in locals() and market_data_task: tasks_to_await.append(market_data_task)

        if tasks_to_await:
             # Wait for tasks, return_exceptions=True prevents gather from stopping if one task errors on cancel
             await asyncio.gather(*tasks_to_await, return_exceptions=True)

        logger.info("Shutdown complete.")


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown_signal)  # Handle Ctrl+C
    signal.signal(signal.SIGTERM, handle_shutdown_signal) # Handle kill/systemd stop

    logger.info("Starting Deribit Arbitrage Bot...")
    try:
        asyncio.run(main_runner())
    except KeyboardInterrupt:
        # This might be caught by the signal handler already
        logger.info("KeyboardInterrupt caught in __main__.")
    except Exception as e:
         logger.critical(f"Critical error preventing startup: {e}", exc_info=True)

    logger.info("Deribit Arbitrage Bot finished.")