import asyncio
import datetime
import math
import json
import time
import logging
from collections import defaultdict
import requests
import websockets
import random # Added for placeholder simulation
import uuid
import hashlib
import hmac

# ----------------------
# Configuration constants
# ----------------------
DERIBIT_API_URL = "https://www.deribit.com/api/v2"
RISK_FREE_RATE = 0.08
TRADE_USD = 100         # Minimum USD liquidity threshold for consideration
MAX_INSTRUMENTS = 500   # Maximum number of instruments to subscribe to
INDEX_PRICES = {}

# Configure logging to include timestamps and level information.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
# Get logger specific to the client
api_logger = logging.getLogger("DeribitAPIClient")
# Get logger for the main script logic (or use the root logger)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Functional Deribit Authenticated API Client (WebSocket)
# -------------------------------------------------------------------
class DeribitAPIClient:
    """
    An asynchronous Deribit API client using WebSockets and client_signature authentication.
    Handles connection, authentication, sending requests, and receiving responses/notifications.
    """
    MAX_RECONNECT_ATTEMPTS = 5
    RECONNECT_DELAY = 5 # seconds

    def __init__(self, client_id: str, client_secret: str, ws_url: str = "wss://www.deribit.com/ws/api/v2"):
        if not client_id or not client_secret:
            raise ValueError("Client ID and Client Secret cannot be empty.")
        self.client_id = client_id
        self.client_secret = client_secret
        self.ws_url = ws_url

        self._ws: websockets.WebSocketClientProtocol | None = None
        self._listener_task: asyncio.Task | None = None
        self._heartbeat_responder_task: asyncio.Task | None = None # Task to respond to pings
        self._connection_lock = asyncio.Lock()
        self._pending_requests: dict[int, asyncio.Future] = {}
        self._request_id_counter: int = 1
        self._is_authenticated: bool = False
        self._is_connected: bool = False
        self._reconnect_attempts: int = 0
        self._disconnect_requested: bool = False

        api_logger.info(f"API Client Initialized for {client_id[:5]}... URL: {ws_url}")

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.open and self._is_connected

    @property
    def is_authenticated(self) -> bool:
        return self.is_connected and self._is_authenticated

    async def connect(self):
        """Establishes the WebSocket connection and authenticates."""
        async with self._connection_lock:
            if self.is_connected:
                api_logger.warning("Already connected.")
                return
            self._disconnect_requested = False
            self._reconnect_attempts = 0
            await self._establish_connection()

    async def _establish_connection(self):
        """Internal method to connect, start listener, and authenticate."""
        while self._reconnect_attempts < self.MAX_RECONNECT_ATTEMPTS and not self._disconnect_requested:
            try:
                api_logger.info(f"Attempting to connect (Attempt {self._reconnect_attempts + 1}/{self.MAX_RECONNECT_ATTEMPTS})...")
                self._ws = await websockets.connect(self.ws_url, ping_interval=None) # Disable automatic pings, handle manually if needed
                self._is_connected = True
                self._is_authenticated = False # Reset auth status on new connection
                api_logger.info("WebSocket connection established.")

                # Start background tasks
                self._listener_task = asyncio.create_task(self._listen())
                # Start heartbeat responder (optional but good practice)
                # await self._setup_heartbeat() # Let's rely on test_request for now

                # Authenticate
                if await self.authenticate():
                    api_logger.info("Authentication successful.")
                    self._reconnect_attempts = 0 # Reset attempts on success
                    return # Successful connection and authentication
                else:
                    api_logger.error("Authentication failed after connection.")
                    await self.disconnect(reconnecting=True) # Disconnect if auth fails
                    # Don't return, loop will retry if attempts remain

            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.InvalidStatusCode, OSError) as e:
                api_logger.error(f"Connection failed: {e}")
                await self._handle_disconnect() # Clean up before retry/exit

            except Exception as e:
                api_logger.exception(f"Unexpected error during connection attempt: {e}")
                await self._handle_disconnect() # Clean up before retry/exit

            self._reconnect_attempts += 1
            if not self._disconnect_requested and self._reconnect_attempts < self.MAX_RECONNECT_ATTEMPTS:
                api_logger.info(f"Waiting {self.RECONNECT_DELAY} seconds before next connection attempt...")
                await asyncio.sleep(self.RECONNECT_DELAY)

        if not self.is_connected:
             api_logger.error(f"Failed to connect after {self.MAX_RECONNECT_ATTEMPTS} attempts.")
             # Optionally raise an exception here if connection is critical
             # raise ConnectionError("Failed to connect to Deribit API")


    async def _listen(self):
        """Background task to listen for messages and dispatch them."""
        api_logger.info("Listener task started.")
        try:
            async for message in self._ws:
                try:
                    data = json.loads(message)
                    api_logger.debug(f"RECV: {data}")

                    if 'method' in data:
                        if data['method'] == 'subscription':
                            await self._handle_subscription(data['params'])
                        elif data['method'] == 'heartbeat':
                            await self._handle_heartbeat(data['params'])
                        elif data['method'] == 'test_request':
                             # Respond immediately to test_request
                             await self._send_test_response()
                        else:
                            api_logger.warning(f"Received unhandled method: {data['method']}")
                    elif 'id' in data:
                        await self._handle_response(data)
                    else:
                        api_logger.warning(f"Received message without 'id' or 'method': {data}")

                except json.JSONDecodeError:
                    api_logger.error(f"Failed to decode JSON message: {message}")
                except Exception as e:
                    api_logger.exception(f"Error processing received message: {e}")

        except websockets.exceptions.ConnectionClosedError as e:
            api_logger.warning(f"Listener task stopped: Connection closed (Code: {e.code}, Reason: {e.reason})")
        except asyncio.CancelledError:
             api_logger.info("Listener task cancelled.")
        except Exception as e:
            api_logger.exception(f"Listener task stopped unexpectedly: {e}")
        finally:
            api_logger.info("Listener task finished.")
            # Trigger reconnection logic if disconnect wasn't requested
            if not self._disconnect_requested:
                asyncio.create_task(self._handle_disconnect())


    async def _handle_response(self, response: dict):
        """Handles JSON-RPC responses, matching them to pending requests."""
        req_id = response.get('id')
        if req_id in self._pending_requests:
            future = self._pending_requests.pop(req_id)
            if not future.cancelled():
                if 'error' in response:
                    api_logger.error(f"API Error for request ID {req_id}: {response['error']}")
                    future.set_exception(DeribitAPIError(response['error']))
                elif 'result' in response:
                    future.set_result(response['result'])
                else:
                     api_logger.error(f"Invalid response format for request ID {req_id}: {response}")
                     future.set_exception(Exception(f"Invalid response format: {response}"))
            else:
                 api_logger.warning(f"Received response for already cancelled request ID {req_id}")
        else:
            api_logger.warning(f"Received response with unknown request ID: {req_id}")

    async def _handle_subscription(self, params: dict):
        """Handles subscription notifications (placeholder)."""
        channel = params.get('channel')
        data = params.get('data')
        api_logger.debug(f"Subscription update on channel '{channel}': {data}")
        # In a full implementation, you might put this data onto a queue
        # or call registered callbacks based on the channel name.
        # For the arbitrage bot, we primarily rely on direct ticker fetching,
        # but this handler is necessary for a complete client.

    async def _handle_heartbeat(self, params: dict):
        """Handles heartbeat notifications."""
        # api_logger.debug(f"Heartbeat received: {params}")
        pass # Usually no action needed unless monitoring latency

    async def _send_test_response(self):
        """Sends a response to the server's test_request."""
        try:
            await self._send_json({
                "jsonrpc": "2.0",
                "method": "public/test",
                "params": {}
                # No ID needed for test response according to common practice,
                # but if server expects one matching test_request, adjust here.
                # Let's send without ID first.
            })
            api_logger.debug("Sent public/test response.")
        except Exception as e:
            api_logger.error(f"Failed to send public/test response: {e}")

    async def _setup_heartbeat(self, interval: int = 30):
         """Sets up server-side heartbeats (optional)."""
         if interval < 10:
             interval = 10
         api_logger.info(f"Setting heartbeat interval to {interval} seconds.")
         try:
             await self.send_private_request("public/set_heartbeat", {"interval": interval})
         except Exception as e:
             api_logger.error(f"Failed to set heartbeat: {e}")


    async def authenticate(self) -> bool:
        """Performs authentication using client_signature."""
        if not self.is_connected:
            api_logger.error("Cannot authenticate: Not connected.")
            return False
        if self.is_authenticated:
            api_logger.warning("Already authenticated.")
            return True

        try:
            timestamp = int(time.time() * 1000)
            nonce = uuid.uuid4().hex[:16] # Generate a random nonce
            data = "" # Optional data field for signature

            string_to_sign = f"{timestamp}\n{nonce}\n{data}"

            signature = hmac.new(
                self.client_secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()

            auth_params = {
                "grant_type": "client_signature",
                "client_id": self.client_id,
                "timestamp": timestamp,
                "signature": signature,
                "nonce": nonce,
                "data": data,
                # Request necessary scopes
                "scope": "account:read trade:read trade:read_write" # Adjust scopes as needed
            }

            api_logger.info("Sending authentication request...")
            response = await self.send_public_request("public/auth", auth_params) # Auth is public initially

            # Check response (send_public_request handles basic errors)
            if response and 'access_token' in response:
                self._is_authenticated = True
                api_logger.info(f"Authentication successful. Scope: {response.get('scope')}")
                return True
            else:
                api_logger.error(f"Authentication failed. Response: {response}")
                return False

        except DeribitAPIError as e:
             api_logger.error(f"Authentication API Error: {e}")
             return False
        except Exception as e:
            api_logger.exception(f"Unexpected error during authentication: {e}")
            return False

    async def _send_json(self, request: dict):
        """Sends a JSON request over the WebSocket."""
        if not self.is_connected:
            raise ConnectionError("Cannot send request: Not connected.")
        try:
            message = json.dumps(request)
            api_logger.debug(f"SEND: {message}")
            await self._ws.send(message)
        except websockets.exceptions.ConnectionClosedError as e:
             api_logger.error(f"Connection closed while trying to send: {e}")
             # Trigger reconnection logic
             asyncio.create_task(self._handle_disconnect())
             raise ConnectionError("Connection closed") from e
        except Exception as e:
             api_logger.exception(f"Error sending JSON message: {e}")
             raise

    async def _send_request(self, method: str, params: dict, is_private: bool = True) -> dict:
        """Internal method to send requests and wait for responses."""
        if is_private and not self.is_authenticated:
            raise PermissionError("Cannot send private request: Not authenticated.")
        if not self.is_connected:
             # Attempt to reconnect if not connected
             api_logger.warning("Not connected. Attempting to reconnect before sending request...")
             await self.connect()
             if not self.is_connected:
                  raise ConnectionError("Failed to reconnect. Cannot send request.")
             if is_private and not self.is_authenticated:
                  raise PermissionError("Reconnected but failed to authenticate. Cannot send private request.")


        req_id = self._request_id_counter
        self._request_id_counter += 1

        request = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params
        }

        future = asyncio.get_event_loop().create_future()
        self._pending_requests[req_id] = future

        try:
            await self._send_json(request)
            # Wait for the response with a timeout
            result = await asyncio.wait_for(future, timeout=10.0) # 10 second timeout
            return result
        except asyncio.TimeoutError:
            api_logger.error(f"Request ID {req_id} ({method}) timed out.")
            # Remove the pending request entry if it's still there
            self._pending_requests.pop(req_id, None)
            raise TimeoutError(f"Request {req_id} ({method}) timed out")
        except Exception as e:
             # Ensure future is cleaned up on other errors during send/wait
             self._pending_requests.pop(req_id, None)
             # Don't wrap ConnectionError again if it came from _send_json
             if not isinstance(e, ConnectionError):
                 api_logger.exception(f"Error during request {req_id} ({method}): {e}")
             raise # Re-raise the original exception

    # --- Public API Method Wrappers ---
    async def send_public_request(self, method: str, params: dict = None) -> dict:
        """Sends a public API request."""
        return await self._send_request(method, params or {}, is_private=False)

    # --- Private API Method Wrappers ---
    async def send_private_request(self, method: str, params: dict = None) -> dict:
        """Sends a private API request (requires authentication)."""
        return await self._send_request(method, params or {}, is_private=True)

    async def buy(self, **params) -> dict:
        """Places a buy order. Ensures time_in_force is set if provided."""
        # FOK logic is now handled in ArbitrageExecutor, client just sends
        return await self.send_private_request("private/buy", params)

    async def sell(self, **params) -> dict:
        """Places a sell order. Ensures time_in_force is set if provided."""
        # FOK logic is now handled in ArbitrageExecutor, client just sends
        return await self.send_private_request("private/sell", params)

    async def cancel(self, order_id: str) -> dict:
        """Cancels a specific order by ID."""
        return await self.send_private_request("private/cancel", {"order_id": order_id})

    async def get_order_state(self, order_id: str) -> dict:
         """Retrieves the state of a specific order."""
         # Note: Response format might differ slightly from buy/sell response.
         # The result will likely be the order object itself.
         return await self.send_private_request("private/get_order_state", {"order_id": order_id})

    async def get_position(self, instrument_name: str) -> dict:
        """Retrieves position details for a specific instrument."""
        return await self.send_private_request("private/get_position", {"instrument_name": instrument_name})

    async def get_account_summary(self, currency: str, extended: bool = False) -> dict:
        """Retrieves account summary for a currency."""
        return await self.send_private_request("private/get_account_summary", {"currency": currency, "extended": extended})

    # --- Connection Management ---
    async def disconnect(self, reconnecting=False):
        """Closes the WebSocket connection and cleans up tasks."""
        async with self._connection_lock:
            if not reconnecting:
                self._disconnect_requested = True # Prevent automatic reconnection attempts
                api_logger.info("Disconnect requested by user.")
            else:
                 api_logger.info("Disconnecting (part of reconnect/failure handling).")

            if self._listener_task and not self._listener_task.done():
                self._listener_task.cancel()
                try:
                    await self._listener_task
                except asyncio.CancelledError:
                    pass # Expected
                except Exception as e:
                     api_logger.error(f"Error during listener task cancellation: {e}")
                self._listener_task = None

            # Cancel any other background tasks if they exist (e.g., heartbeat)

            if self._ws and self._ws.open:
                try:
                    await self._ws.close()
                    api_logger.info("WebSocket connection closed.")
                except Exception as e:
                    api_logger.error(f"Error closing WebSocket: {e}")

            self._ws = None
            self._is_connected = False
            self._is_authenticated = False

            # Clear pending requests - they will fail anyway
            for req_id, future in self._pending_requests.items():
                 if not future.done():
                      future.set_exception(ConnectionAbortedError(f"Connection closed while request {req_id} was pending"))
            self._pending_requests.clear()

    async def _handle_disconnect(self):
         """Handles cleanup and potential reconnection logic after a disconnect."""
         api_logger.warning("Handling disconnect...")
         await self.disconnect(reconnecting=True) # Ensure clean state
         # Reconnection is handled by the loop in _establish_connection


# --- Custom Exception for API Errors ---
class DeribitAPIError(Exception):
    """Custom exception for errors returned by the Deribit API."""
    def __init__(self, error_data: dict):
        self.code = error_data.get('code')
        self.message = error_data.get('message', 'Unknown API error')
        self.data = error_data.get('data')
        super().__init__(f"Deribit API Error {self.code}: {self.message} {self.data or ''}")

# -------------------------------------------------------------------
# HTTPMarketDataFetcher: Fetch instruments, underlying prices, and group instruments.
# -------------------------------------------------------------------
class HTTPMarketDataFetcher:
    @staticmethod
    def get_instruments(currency):
        url = f"{DERIBIT_API_URL}/public/get_instruments"
        params = {"currency": currency, "kind": "option", "expired": "false"}
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            instruments = response.json()['result']
            logger.info(f"Fetched {len(instruments)} instruments for {currency}.")
            return instruments
        except Exception as e:
            logger.error(f"Error fetching instruments for {currency}: {e}")
            return []
    
    @staticmethod
    def get_underlying_spot(coin):
        """
        For BTC and ETH options (settled in the base coin), the index is in USD.
        For altcoin options (e.g. SOL, XRP, BNB, etc.), settlement is USDC.
        """
        index_name = coin.lower() + "_usd"
        params = {"index_name": index_name}
        url = f"{DERIBIT_API_URL}/public/get_index_price"
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()['result']
            index_price = data.get('index_price')
            logger.info(f"Underlying index for {coin}: {index_price}")
            return index_price
        except Exception as e:
            logger.error(f"Error fetching underlying spot for {coin}: {e}")
            return None

    @staticmethod
    def parse_instrument_name(instrument_name):
        try:
            parts = instrument_name.split('-')
            if len(parts) != 4:
                return None, None, None
            expiry = datetime.datetime.strptime(parts[1], "%d%b%y").date()
            strike_str = parts[2]
            # If underscore present, coin is before it; else use first dash token.
            if "_" in instrument_name:
                coin = instrument_name.split('_')[0].upper()
            else:
                coin = instrument_name.split('-')[0].upper()
            strike = float(strike_str.replace('d', '.')) if (coin == "XRP" and 'd' in strike_str) else float(strike_str)
            option_type = parts[3].upper()
            return strike, expiry, option_type
        except Exception as e:
            logger.error(f"Error parsing instrument name {instrument_name}: {e}")
            return None, None, None

    @staticmethod
    def group_options_by_strike_and_expiry(instruments):
        options_dict = defaultdict(lambda: {"call": None, "put": None})
        for inst in instruments:
            instrument_name = inst['instrument_name']
            strike, expiry, option_type = HTTPMarketDataFetcher.parse_instrument_name(instrument_name)
            if strike is None or expiry is None or option_type is None:
                continue
            key = (strike, expiry)
            if option_type in ['C', 'CALL']:
                options_dict[key]["call"] = inst
            elif option_type in ['P', 'PUT']:
                options_dict[key]["put"] = inst
        return options_dict

    @staticmethod
    def group_options_by_coin(instruments):
        coin_groups = defaultdict(list)
        for inst in instruments:
            name = inst['instrument_name']
            if "_" in name:
                coin = name.split('_')[0].upper()
            else:
                coin = name.split('-')[0].upper()
            coin_groups[coin].append(inst)
        return coin_groups

# -------------------------------------------------------------------
# MultiWSMarketDataFetcher: Aggregates WS fetchers.
# -------------------------------------------------------------------

async def subscribe_index_prices(coins):
    ws_url = "wss://www.deribit.com/ws/api/v2"
    async with websockets.connect(ws_url) as ws:
        channels = [f"deribit_price_index.{coin.lower()}_usd" for coin in coins]
        subscription_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/subscribe",
            "params": {"channels": channels}
        }
        await ws.send(json.dumps(subscription_msg))
        logger.info(f"Subscribed to index channels: {channels}")
        # Use async for to iterate over messages until the connection is closed.
        async for message in ws:
            try:
                data = json.loads(message)
                params = data.get("params", {})
                channel = params.get("channel", "")
                if channel.startswith("deribit_price_index"):
                    # Extract coin (e.g., "sol" from "deribit_price_index.sol_usd")
                    coin = channel.split('.')[1].split('_')[0].upper()
                    index_price = params.get("data", {}).get("price")
                    if index_price:
                        INDEX_PRICES[coin] = index_price
                        logger.debug(f"Updated index price for {coin}: {index_price}")
                    else:
                        logger.warning(f"Failed to get index price for {coin}")
            except Exception as e:
                logger.error(f"Error processing index price message: {e}")

class MultiWSMarketDataFetcher:
    def __init__(self, instruments, heartbeat_interval=10):
        self.fetchers = []
        for i in range(0, len(instruments), MAX_INSTRUMENTS):
            batch = instruments[i:i+MAX_INSTRUMENTS]
            fetcher = WSMarketDataFetcher(batch, heartbeat_interval)
            self.fetchers.append(fetcher)
    
    async def run(self):
        await asyncio.gather(*(fetcher.run() for fetcher in self.fetchers))
    
    def get_ticker(self, instrument):
        latest_ticker = None
        latest_time = 0
        for fetcher in self.fetchers:
            ticker = fetcher.get_ticker(instrument)
            if ticker and ticker.get("timestamp", 0) > latest_time:
                latest_time = ticker["timestamp"]
                latest_ticker = ticker
        return latest_ticker

# -------------------------------------------------------------------
# WSMarketDataFetcher: Subscribes using "ticker.{instrument_name}.100ms" endpoint.
# -------------------------------------------------------------------

class WSMarketDataFetcher:
    def __init__(self, instruments, heartbeat_interval=10):
        self.instruments = instruments
        self.heartbeat_interval = heartbeat_interval
        self.ticker_cache = {}
        self.ws_url = "wss://www.deribit.com/ws/api/v2"
    
    async def set_heartbeat(self, ws):
        heartbeat_msg = {
            "jsonrpc": "2.0",
            "id": 100,
            "method": "public/set_heartbeat",
            "params": {"interval": self.heartbeat_interval}
        }
        await ws.send(json.dumps(heartbeat_msg))
        logger.info(f"Heartbeat set to {self.heartbeat_interval} seconds.")
    
    async def subscribe_channels(self, ws):
        channels = [f"ticker.{inst}.100ms" for inst in self.instruments]
        subscription_msg = {
            "jsonrpc": "2.0",
            "id": 101,
            "method": "public/subscribe",
            "params": {"channels": channels}
        }
        await ws.send(json.dumps(subscription_msg))
        logger.info(f"Subscribed to {len(channels)} ticker channels (100ms updates).")
    
    async def process_messages(self, ws):
        async for message in ws:
            start_time = time.perf_counter()
            try:
                msg = json.loads(message)
                if "method" in msg:
                    method = msg["method"]
                    if method == "test_request":
                        test_msg = {
                            "jsonrpc": "2.0",
                            "id": msg.get("id", 999),
                            "method": "public/test",
                            "params": {}
                        }
                        await ws.send(json.dumps(test_msg))
                    elif method == "subscription":
                        params = msg.get("params", {})
                        channel = params.get("channel")
                        data = params.get("data", {})
                        if channel and data:
                            parts = channel.split(".")
                            if len(parts) >= 3:
                                instrument = parts[1]
                                self.ticker_cache[instrument] = data
            except Exception as e:
                logger.error("Error processing WS message: %s", e)
            end_time = time.perf_counter()
            processing_latency = (end_time - start_time) * 1000
            logger.debug(f"Processed WS message in {processing_latency:.2f}ms")
    
    async def run(self):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    logger.info("Connected to Deribit WS.")
                    await self.set_heartbeat(ws)
                    await self.subscribe_channels(ws)
                    await self.process_messages(ws)
            except Exception as e:
                logger.error("WS connection error: %s", e)
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
    
    def get_ticker(self, instrument):
        return self.ticker_cache.get(instrument, None)

# -------------------------------------------------------------------
# FeeCalculator: Computes taker fees for USDC linear options.
# -------------------------------------------------------------------

class FeeCalculator:
    """
    Computes execution and settlement fees per USDC linear option leg.
    For USDC options, combo discounts are not applied.
    """
    
    @staticmethod
    def compute_usdc_linear_option_taker_fee_leg(premium, index_price, hold_to_expiry=True):
        execution_fee = min(0.0005 * index_price, 0.125 * premium)
        settlement_fee = 0.0
        if hold_to_expiry:
            settlement_fee = min(0.00015 * index_price, 0.125 * premium)
        return execution_fee + settlement_fee
    
    @staticmethod
    def apply_combo_discount(fee_buy, fee_sell):
        # For USDC options, no combo discount is applied.
        return fee_buy + fee_sell

# -------------------------------------------------------------------
# Arbitrage Opportunity Classes (Synthetic Long/Short)
# -------------------------------------------------------------------

class ArbitrageOpportunity:
    """
    Base class for arbitrage opportunities. Computes theoretical forward via Black-76 parity.
    """
    def __init__(self, coin, strike, expiry, underlying_price, call_order, put_order, T, threshold):
        self.coin = coin
        self.strike = strike
        self.expiry = expiry
        self.underlying_price = underlying_price
        self.call_order = call_order
        self.put_order = put_order
        self.T = T
        self.threshold = threshold
        self.theoretical_forward = math.exp(-RISK_FREE_RATE * T) * (underlying_price - strike)
    
    def check_opportunity(self):
        raise NotImplementedError("Subclass must implement check_opportunity")
    
    def print_details(self):
        raise NotImplementedError("Subclass must implement print_details")

class SyntheticLongArbitrage(ArbitrageOpportunity):
    """Synthetic Long: Buy call at ask, sell put at bid."""
    def __init__(self, *args, hold_to_expiry=True, **kwargs):
        super().__init__(*args, **kwargs)
        call_ask = self.call_order['best_ask']
        put_bid = self.put_order['best_bid']
        self.synthetic_long = call_ask - put_bid
        fee_call = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(call_ask, self.underlying_price, hold_to_expiry)
        fee_put  = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(put_bid, self.underlying_price, hold_to_expiry)
        total_fee = fee_call + fee_put
        self.synthetic_long_with_fees = self.synthetic_long - total_fee
    
    def check_opportunity(self):
        return (self.theoretical_forward - self.synthetic_long_with_fees) > self.threshold
    
    def print_details(self):
        diff = self.theoretical_forward - self.synthetic_long_with_fees
        pct = (diff / self.theoretical_forward * 100) if self.theoretical_forward != 0 else 0
        logger.info(f"\n[Arbitrage Opportunity: Synthetic Long] {self.coin} Options")
        logger.info(f"Strike: {self.strike}, Expiry: {self.expiry} (T = {self.T:.3f} years)")
        logger.info(f"Index Price: {self.underlying_price}")
        logger.info(f"Call Ask: {self.call_order['best_ask']}, Put Bid: {self.put_order['best_bid']}")
        logger.info(f"Synthetic Long (pre-fee): {self.synthetic_long:.4f}")
        logger.info(f"Synthetic Long (post-fee): {self.synthetic_long_with_fees:.4f}")
        logger.info(f"Theoretical Forward: {self.theoretical_forward:.4f}")
        logger.info(f"Difference: {diff:.4f} ({pct:.2f}%)")

class SyntheticShortArbitrage(ArbitrageOpportunity):
    """Synthetic Short: Sell call at bid, buy put at ask."""
    def __init__(self, *args, hold_to_expiry=True, **kwargs):
        super().__init__(*args, **kwargs)
        call_bid = self.call_order['best_bid']
        put_ask = self.put_order['best_ask']
        self.synthetic_short = call_bid - put_ask
        fee_call = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(call_bid, self.underlying_price, hold_to_expiry)
        fee_put  = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(put_ask, self.underlying_price, hold_to_expiry)
        total_fee = fee_call + fee_put
        self.synthetic_short_with_fees = self.synthetic_short - total_fee
    
    def check_opportunity(self):
        return (self.synthetic_short_with_fees - self.theoretical_forward) > self.threshold
    
    def print_details(self):
        diff = self.synthetic_short_with_fees - self.theoretical_forward
        pct = (diff / self.theoretical_forward * 100) if self.theoretical_forward != 0 else 0
        logger.info(f"\n[Arbitrage Opportunity: Synthetic Short] {self.coin} Options")
        logger.info(f"Strike: {self.strike}, Expiry: {self.expiry} (T = {self.T:.3f} years)")
        logger.info(f"Index Price: {self.underlying_price}")
        logger.info(f"Call Bid: {self.call_order['best_bid']}, Put Ask: {self.put_order['best_ask']}")
        logger.info(f"Synthetic Short (pre-fee): {self.synthetic_short:.4f}")
        logger.info(f"Synthetic Short (post-fee): {self.synthetic_short_with_fees:.4f}")
        logger.info(f"Theoretical Forward: {self.theoretical_forward:.4f}")
        logger.info(f"Difference: {diff:.4f} ({pct:.2f}%)")

# -------------------------------------------------------------------
# BoxArbitrage: Constructs box spread arbitrage opportunities.
# -------------------------------------------------------------------
class BoxArbitrage:
    """
    Constructs a box spread arbitrage opportunity.
    
    For inverse options (BTC/ETH), the replication is normalized:
      - Buy 1 unit of the lower-strike PCP portfolio.
      - Sell (K_low/K_high) units of the higher-strike PCP portfolio.
    The resulting (normalized) net cost is compared to the normalized theoretical payoff,
    which is exp(-r*T) * (K_high - K_low) / K_high.
    
    For USDC (or linear) options, one contract per leg is used.
    Comprehensive benchmarking is added to record the latency of each step.
    """
    def __init__(self, coin, expiry, K_low, K_high,
                 call_low_order, put_low_order, call_high_order, put_high_order,
                 T, threshold, underlying_price, hold_to_expiry=True):
        overall_start = time.perf_counter()
        self.benchmark = {}  # Dictionary to hold timing data

        self.coin = coin
        self.expiry = expiry
        self.K_low = K_low
        self.K_high = K_high
        self.call_low_order = call_low_order
        self.put_low_order = put_low_order
        self.call_high_order = call_high_order
        self.put_high_order = put_high_order
        self.T = T
        self.threshold = threshold
        self.underlying_price = underlying_price
        self.hold_to_expiry = hold_to_expiry

        if coin in {"BTC", "ETH"}:
            branch_start = time.perf_counter()
            # For inverse options, normalize replication.
            self.multiplier_low = 1
            self.multiplier_high = K_low / K_high

            # --- Conversion of option premiums to USD ---
            t_conv_start = time.perf_counter()
            call_low_ask_usd  = self.call_low_order['best_ask'] * underlying_price
            put_low_bid_usd   = self.put_low_order['best_bid'] * underlying_price
            call_high_bid_usd = self.call_high_order['best_bid'] * underlying_price
            put_high_ask_usd  = self.put_high_order['best_ask'] * underlying_price
            self.benchmark['conversion'] = time.perf_counter() - t_conv_start

            # --- Net cost computation ---
            t_net_start = time.perf_counter()
            net_cost_usd = (self.multiplier_low * (call_low_ask_usd - put_low_bid_usd)
                            - self.multiplier_high * (call_high_bid_usd - put_high_ask_usd))
            self.net_cost = net_cost_usd
            self.benchmark['net_cost'] = time.perf_counter() - t_net_start

            # --- Fees computation ---
            t_fees_start = time.perf_counter()
            fee_buy_call_low_coin  = self.multiplier_low * self.compute_btc_eth_option_fee(self.call_low_order['best_ask'], underlying_price)
            fee_buy_put_high_coin  = self.multiplier_high * self.compute_btc_eth_option_fee(self.put_high_order['best_ask'], underlying_price)
            fee_sell_put_low_coin  = self.multiplier_low * self.compute_btc_eth_option_fee(self.put_low_order['best_bid'], underlying_price)
            fee_sell_call_high_coin = self.multiplier_high * self.compute_btc_eth_option_fee(self.call_high_order['best_bid'], underlying_price)

            fee_buy_call_low_usd  = fee_buy_call_low_coin * underlying_price
            fee_buy_put_high_usd  = fee_buy_put_high_coin * underlying_price
            fee_sell_put_low_usd  = fee_sell_put_low_coin * underlying_price
            fee_sell_call_high_usd = fee_sell_call_high_coin * underlying_price

            group_buy_fee_usd  = fee_buy_call_low_usd + fee_buy_put_high_usd
            group_sell_fee_usd = fee_sell_put_low_usd + fee_sell_call_high_usd

            total_fee_usd = group_sell_fee_usd if group_buy_fee_usd < group_sell_fee_usd else group_buy_fee_usd
            self.combo_fee = total_fee_usd
            net_cost_with_fees_usd = net_cost_usd - total_fee_usd
            self.benchmark['fees'] = time.perf_counter() - t_fees_start

            # --- Normalization ---
            t_norm_start = time.perf_counter()
            self.normalized_net_cost = net_cost_with_fees_usd / K_high
            self.normalized_theoretical_cost = math.exp(-RISK_FREE_RATE * T) * (K_high - K_low) / K_high
            self.normalized_delta = ((self.normalized_net_cost - self.normalized_theoretical_cost) /
                                       self.normalized_theoretical_cost)
            self.net_cost_with_fees = self.normalized_net_cost
            self.theoretical_cost = self.normalized_theoretical_cost
            self.benchmark['normalization'] = time.perf_counter() - t_norm_start

            # --- Deployable liquidity computation ---
            t_deploy_start = time.perf_counter()
            leg1_value = self.multiplier_low * (self.call_low_order['best_ask'] * underlying_price *
                                                 self.call_low_order.get("size", 0))
            leg2_value = self.multiplier_low * (self.put_low_order['best_bid'] * underlying_price *
                                                 self.put_low_order.get("size", 0))
            leg3_value = self.multiplier_high * (self.call_high_order['best_bid'] * underlying_price *
                                                  self.call_high_order.get("size", 0))
            leg4_value = self.multiplier_high * (self.put_high_order['best_ask'] * underlying_price *
                                                  self.put_high_order.get("size", 0))
            self.max_deployable_usd = min(leg1_value, leg2_value, leg3_value, leg4_value)
            self.benchmark['deployable'] = time.perf_counter() - t_deploy_start

            self.benchmark['branch_total'] = time.perf_counter() - branch_start

        else:
            branch_start = time.perf_counter()
            self.multiplier_low = 1
            self.multiplier_high = 1

            t_net_start = time.perf_counter()
            self.net_cost = (self.call_low_order['best_ask'] - self.put_low_order['best_bid']) - \
                            (self.call_high_order['best_bid'] - self.put_high_order['best_ask'])
            self.benchmark['net_cost'] = time.perf_counter() - t_net_start

            t_fees_start = time.perf_counter()
            fee_buy_call_low = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(
                self.call_low_order['best_ask'], underlying_price, hold_to_expiry)
            fee_sell_put_low = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(
                self.put_low_order['best_bid'], underlying_price, hold_to_expiry)
            fee_sell_call_high = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(
                self.call_high_order['best_bid'], underlying_price, hold_to_expiry)
            fee_buy_put_high = FeeCalculator.compute_usdc_linear_option_taker_fee_leg(
                self.put_high_order['best_ask'], underlying_price, hold_to_expiry)
            total_fee = fee_buy_call_low + fee_buy_put_high + fee_sell_put_low + fee_sell_call_high
            self.combo_fee = total_fee
            self.net_cost_with_fees = self.net_cost - self.combo_fee
            self.benchmark['fees'] = time.perf_counter() - t_fees_start

            t_theoretical_start = time.perf_counter()
            self.theoretical_cost = math.exp(-RISK_FREE_RATE * T) * (K_high - K_low)
            self.benchmark['theoretical'] = time.perf_counter() - t_theoretical_start

            t_deploy_start = time.perf_counter()
            call_low_value = self.call_low_order["best_ask"] * self.call_low_order["best_ask_amount"]
            put_low_value = self.put_low_order["best_bid"] * self.put_low_order["best_bid_amount"]
            call_high_value = self.call_high_order["best_bid"] * self.call_high_order["best_bid_amount"]
            put_high_value = self.put_high_order["best_ask"] * self.put_high_order["best_ask_amount"]
            self.max_deployable_usd = min(call_low_value, put_low_value, call_high_value, put_high_value)
            self.benchmark['deployable'] = time.perf_counter() - t_deploy_start

            self.benchmark['branch_total'] = time.perf_counter() - branch_start

        self.benchmark['total_init_time'] = time.perf_counter() - overall_start

    @staticmethod
    def compute_btc_eth_option_fee(premium, underlying_price):
        fee_from_underlying = 0.0003
        fee_cap = 0.125 * premium
        return min(fee_from_underlying, fee_cap)

    def check_opportunity(self):
        if self.coin in {"BTC", "ETH"}:
            return abs(self.net_cost_with_fees - self.theoretical_cost) > self.threshold
        else:
            return abs(self.net_cost_with_fees - self.theoretical_cost) > self.threshold

    def print_details(self):
        logger.info(f"\n[Arbitrage Opportunity: Box Spread] {self.coin} Options")
        logger.info(f"Expiry: {self.expiry} (T = {self.T:.3f} years)")
        logger.info(f"Lower Strike: {self.K_low}, Higher Strike: {self.K_high}")
        logger.info("--- Leg Details ---")
        if self.coin in {"BTC", "ETH"}:
            logger.info(f"Buy {self.multiplier_low} unit(s) at strike {self.K_low}:")
            logger.info(f"  Call Low Ask: {self.call_low_order['best_ask']}, Put Low Bid: {self.put_low_order['best_bid']}")
            logger.info(f"Sell {self.multiplier_high} unit(s) at strike {self.K_high}:")
            logger.info(f"  Call High Bid: {self.call_high_order['best_bid']}, Put High Ask: {self.put_high_order['best_ask']}")
            logger.info(f"Normalized Net Cost (after fees): {self.net_cost_with_fees:.4f}")
            logger.info(f"Normalized Theoretical Box Cost (Risk-free payoff): {self.theoretical_cost:.4f}")
            diff = self.net_cost_with_fees - self.theoretical_cost
            pct = (diff / self.theoretical_cost * 100) if self.theoretical_cost != 0 else 0
            logger.info(f"Difference: {diff:.4f} ({pct:.2f}%)")
        else:
            logger.info(f"Buy 1 unit at strike {self.K_low} and sell 1 unit at strike {self.K_high}:")
            logger.info(f"  Call Low Ask: {self.call_low_order['best_ask']}, Put Low Bid: {self.put_low_order['best_bid']}")
            logger.info(f"  Call High Bid: {self.call_high_order['best_bid']}, Put High Ask: {self.put_high_order['best_ask']}")
            logger.info(f"Net Cost (after fees): {self.net_cost_with_fees:.4f}")
            logger.info(f"Theoretical Box Cost (Risk-free payoff): {self.theoretical_cost:.4f}")
            diff = self.net_cost_with_fees - self.theoretical_cost
            pct = (diff / self.theoretical_cost * 100) if self.theoretical_cost != 0 else 0
            logger.info(f"Difference: {diff:.4f} ({pct:.2f}%)")
            logger.info(f"Maximum deployable value (USD): {self.max_deployable_usd:.2f}")
        # Print benchmark timings.
        logger.info("Benchmark timings (miliseconds):")
        for key, t in self.benchmark.items():
            logger.info(f"  {key}: {t*1000:.6f}")

# -------------------------------------------------------------------
# BoxArbitrageScannerWS: Scans for box spread opportunities across coins.
# -------------------------------------------------------------------
class BoxArbitrageScannerWS:
    def __init__(self, ws_fetcher, currency, coins=('SOL', 'XRP', 'BNB'), hold_to_expiry=True, instruments=None):
        self.ws_fetcher = ws_fetcher
        self.currency = currency
        self.coins = coins
        self.hold_to_expiry = hold_to_expiry
        if instruments is None:
            self.instruments = HTTPMarketDataFetcher.get_instruments(currency)
        else:
            self.instruments = instruments
        self.coin_groups = HTTPMarketDataFetcher.group_options_by_coin(self.instruments)
        self.skipped_boxes = []
        self.total_boxes_checked = 0

    def find_opportunities(self):
        overall_start = time.perf_counter()
        benchmark = {}  # To record per-coin processing times.
        coin_timings = {}
        opportunities = []
        for coin in self.coins:
            coin_start = time.perf_counter()
            coin_instruments = self.coin_groups.get(coin, [])
            if not coin_instruments:
                continue
            underlying_coin = coin
            underlying_price = INDEX_PRICES.get(underlying_coin)
            if underlying_price is None:
                continue
            grouped_options = HTTPMarketDataFetcher.group_options_by_strike_and_expiry(coin_instruments)
            expiry_groups = defaultdict(dict)
            for (strike, expiry), option_pair in grouped_options.items():
                if option_pair.get("call") and option_pair.get("put"):
                    expiry_groups[expiry][strike] = option_pair
            for expiry, strikes_dict in expiry_groups.items():
                strikes = sorted(strikes_dict.keys())
                today = datetime.date.today()
                T_days = (expiry - today).days
                if T_days <= 0:
                    continue
                T = T_days / 365.25
                for i in range(len(strikes) - 1):
                    K_low = strikes[i]
                    K_high = strikes[i + 1]
                    lower_option = strikes_dict[K_low]
                    higher_option = strikes_dict[K_high]
                    
                    call_low_ticker = self.ws_fetcher.get_ticker(lower_option['call']['instrument_name'])
                    put_low_ticker  = self.ws_fetcher.get_ticker(lower_option['put']['instrument_name'])
                    call_high_ticker = self.ws_fetcher.get_ticker(higher_option['call']['instrument_name'])
                    put_high_ticker  = self.ws_fetcher.get_ticker(higher_option['put']['instrument_name'])
                    
                    # Skip if any ticker is missing or has zero bid/ask.
                    if (call_low_ticker is None or put_low_ticker is None or 
                        call_high_ticker is None or put_high_ticker is None):
                        self.skipped_boxes.append((coin, expiry, K_low, K_high))
                        continue
                    
                    if (not call_low_ticker.get("best_ask_price") or call_low_ticker.get("best_ask_price") == 0 or
                        not call_low_ticker.get("best_bid_price") or call_low_ticker.get("best_bid_price") == 0 or
                        not put_low_ticker.get("best_ask_price") or put_low_ticker.get("best_ask_price") == 0 or
                        not put_low_ticker.get("best_bid_price") or put_low_ticker.get("best_bid_price") == 0 or
                        not call_high_ticker.get("best_ask_price") or call_high_ticker.get("best_ask_price") == 0 or
                        not call_high_ticker.get("best_bid_price") or call_high_ticker.get("best_bid_price") == 0 or
                        not put_high_ticker.get("best_ask_price") or put_high_ticker.get("best_ask_price") == 0 or
                        not put_high_ticker.get("best_bid_price") or put_high_ticker.get("best_bid_price") == 0):
                        self.skipped_boxes.append((coin, expiry, K_low, K_high))
                        continue
                    
                    threshold = 0  # For benchmarking, threshold is set to zero.
                    box = BoxArbitrage(
                        coin, expiry, K_low, K_high,
                        call_low_order = {
                            "best_ask": call_low_ticker.get("best_ask_price"),
                            "best_bid": call_low_ticker.get("best_bid_price"),
                            "best_ask_amount": call_low_ticker.get("best_ask_amount", 0)
                        },
                        put_low_order = {
                            "best_bid": put_low_ticker.get("best_bid_price"),
                            "best_ask": put_low_ticker.get("best_ask_price"),
                            "best_bid_amount": put_low_ticker.get("best_bid_amount", 0)
                        },
                        call_high_order = {
                            "best_bid": call_high_ticker.get("best_bid_price"),
                            "best_ask": call_high_ticker.get("best_ask_price"),
                            "best_bid_amount": call_high_ticker.get("best_bid_amount", 0)
                        },
                        put_high_order = {
                            "best_ask": put_high_ticker.get("best_ask_price"),
                            "best_bid": put_high_ticker.get("best_bid_price"),
                            "best_ask_amount": put_high_ticker.get("best_ask_amount", 0)
                        },
                        T = T,
                        threshold = threshold,
                        underlying_price = underlying_price,
                        hold_to_expiry = self.hold_to_expiry
                    )
                    self.total_boxes_checked += 1
                    if box.check_opportunity():
                        diff = abs(box.net_cost_with_fees - box.theoretical_cost)
                        opportunities.append((diff, box))
            coin_end = time.perf_counter()
            coin_timings[coin] = coin_end - coin_start
        benchmark['coin_processing'] = coin_timings
        benchmark['find_total'] = time.perf_counter() - overall_start
        logger.info(f"Find opportunities benchmark: {benchmark}")
        logger.info(f"Box scan completed in {benchmark['find_total']*1000:.2f}ms. Checked {self.total_boxes_checked} pairs.")
        return opportunities

    def scan(self):
        start_time = time.perf_counter()
        opps = self.find_opportunities()
        end_time = time.perf_counter()
        if not opps:
            logger.info("No box spread opportunities found in this scan.")
            return
        sorted_opps = sorted(opps, key=lambda x: x[0], reverse=True)
        logger.info("=== Top 3 Box Spread Opportunities Across All Instruments ===")
        for diff, box in sorted_opps[:3]:
            box.print_details()
        logger.info(f"Scan duration: {(end_time - start_time)*1000:.2f}ms")


# -------------------------------------------------------------------
# ArbitrageExecutor: Executes box spread trades
# -------------------------------------------------------------------
class ArbitrageExecutor:
    def __init__(self, api_client: DeribitAPIClient, max_contracts_per_trade: int = 5):
        """
        Initializes the ArbitrageExecutor.

        Args:
            api_client: An instance of the authenticated DeribitAPIClient.
            max_contracts_per_trade: The maximum number of contracts for any single leg.
                                      Prevents excessively large trades.
        """
        if not isinstance(api_client, DeribitAPIClient):
             # Basic check, replace DeribitAPIClient with your actual client class name
             raise TypeError("api_client must be an instance of DeribitAPIClient or compatible")
        self.api_client = api_client
        self.max_contracts_per_trade = max_contracts_per_trade
        logger.info(f"ArbitrageExecutor initialized with max_contracts_per_trade={max_contracts_per_trade}")

    async def execute_box(self, box: BoxArbitrage):
        """
        Attempts to execute a box spread arbitrage opportunity.

        Uses Fill-Or-Kill (FOK) orders for each leg to attempt atomic execution.
        Logs the outcome, including warnings if execution is partial (which is a risk).

        Args:
            box: The BoxArbitrage object representing the opportunity.
        """
        start_time = time.perf_counter()
        trade_id = str(uuid.uuid4()) # Unique ID for this trade attempt
        logger.info(f"[Trade Attempt {trade_id}] Initiating box spread execution for {box.coin} {box.expiry} {box.K_low}/{box.K_high}")

        # --- 1. Determine Direction ---
        # If actual net cost (selling high spread, buying low spread) > theoretical cost,
        # we want to *capture* that difference by doing the trade: SELL the box spread.
        # Selling the box = Sell Low Strike Spread + Buy High Strike Spread
        # Sell Low Strike Spread = Sell Call Low + Buy Put Low
        # Buy High Strike Spread = Buy Call High + Sell Put High
        #
        # If actual net cost < theoretical cost, the opportunity is to BUY the box spread.
        # Buying the box = Buy Low Strike Spread + Sell High Strike Spread
        # Buy Low Strike Spread = Buy Call Low + Sell Put Low
        # Sell High Strike Spread = Sell Call High + Buy Put High
        #
        # The 'BoxArbitrage' class calculates `net_cost_with_fees` based on:
        # (Buy Call Low Ask - Sell Put Low Bid) - (Sell Call High Bid - Buy Put High Ask)
        # This represents the cost of *buying* the box.

        is_buy_box = box.theoretical_cost > box.net_cost_with_fees
        if is_buy_box:
            logger.info(f"[Trade Attempt {trade_id}] Target: BUY Box (Theoretical: {box.theoretical_cost:.4f} > Net Cost: {box.net_cost_with_fees:.4f})")
            # Buy Low Strike Call (@ Ask)
            # Sell Low Strike Put (@ Bid)
            # Sell High Strike Call (@ Bid)
            # Buy High Strike Put (@ Ask)
            leg1 = {"action": "buy", "instrument": box.call_low_order['instrument_name'], "price": box.call_low_order['best_ask'], "available_amount": box.call_low_order['best_ask_amount']}
            leg2 = {"action": "sell", "instrument": box.put_low_order['instrument_name'], "price": box.put_low_order['best_bid'], "available_amount": box.put_low_order['best_bid_amount']}
            leg3 = {"action": "sell", "instrument": box.call_high_order['instrument_name'], "price": box.call_high_order['best_bid'], "available_amount": box.call_high_order['best_bid_amount']}
            leg4 = {"action": "buy", "instrument": box.put_high_order['instrument_name'], "price": box.put_high_order['best_ask'], "available_amount": box.put_high_order['best_ask_amount']}
        else:
            logger.info(f"[Trade Attempt {trade_id}] Target: SELL Box (Net Cost: {box.net_cost_with_fees:.4f} > Theoretical: {box.theoretical_cost:.4f})")
            # Sell Low Strike Call (@ Bid)
            # Buy Low Strike Put (@ Ask)
            # Buy High Strike Call (@ Ask)
            # Sell High Strike Put (@ Bid)
            leg1 = {"action": "sell", "instrument": box.call_low_order['instrument_name'], "price": box.call_low_order['best_bid'], "available_amount": box.call_low_order['best_bid_amount']}
            leg2 = {"action": "buy", "instrument": box.put_low_order['instrument_name'], "price": box.put_low_order['best_ask'], "available_amount": box.put_low_order['best_ask_amount']}
            leg3 = {"action": "buy", "instrument": box.call_high_order['instrument_name'], "price": box.call_high_order['best_ask'], "available_amount": box.call_high_order['best_ask_amount']}
            leg4 = {"action": "sell", "instrument": box.put_high_order['instrument_name'], "price": box.put_high_order['best_bid'], "available_amount": box.put_high_order['best_bid_amount']}

        legs = [leg1, leg2, leg3, leg4]

        # --- 2. Determine Contracts to Trade ---
        # Find the minimum available contracts across all legs at the target prices
        try:
            min_available_contracts = math.floor(min(leg['available_amount'] for leg in legs))
        except (TypeError, ValueError):
             logger.error(f"[Trade Attempt {trade_id}] Invalid or missing contract amounts in box data. Aborting.")
             return None # Indicate failure

        if min_available_contracts <= 0:
            logger.warning(f"[Trade Attempt {trade_id}] Insufficient liquidity (0 contracts) available on at least one leg. Aborting.")
            return None

        contracts_to_trade = math.floor(min(min_available_contracts, self.max_contracts_per_trade))

        if contracts_to_trade <= 0:
             logger.warning(f"[Trade Attempt {trade_id}] Calculated contracts to trade is 0 (check max_contracts_per_trade). Aborting.")
             return None

        logger.info(f"[Trade Attempt {trade_id}] Attempting to trade {contracts_to_trade} contracts per leg (Min Available: {min_available_contracts}, Max Allowed: {self.max_contracts_per_trade}).")

        # --- 3. Prepare and Send Orders ---
        order_tasks = []
        label_prefix = f"box_{box.coin[:3]}_{box.expiry.strftime('%y%m%d')}_{int(box.K_low)}_{int(box.K_high)}_{trade_id[:4]}" # Short unique label

        for i, leg in enumerate(legs):
            order_label = f"{label_prefix}_{i+1}"
            params = {
                "instrument_name": leg['instrument'],
                "contracts": contracts_to_trade, # Use contracts for linear options
                "type": "limit",
                "price": leg['price'],
                "time_in_force": "fill_or_kill", # Crucial for atomicity attempt
                "label": order_label,
                # "reduce_only": False # Typically not needed for arb entry
                # "post_only": False # We want to take liquidity immediately
            }

            if leg['action'] == "buy":
                task = asyncio.create_task(self.api_client.buy(**params))
            else: # sell
                task = asyncio.create_task(self.api_client.sell(**params))
            order_tasks.append(task)

        order_results = []
        try:
            # Send all orders concurrently
            results = await asyncio.gather(*order_tasks, return_exceptions=True)
            order_results = results # Store results or exceptions
        except Exception as e:
            logger.error(f"[Trade Attempt {trade_id}] Unexpected error during asyncio.gather for orders: {e}")
            # Need to attempt cancellation if some tasks might have started
            # (This part is tricky, gather might fail before all tasks run)
            # For simplicity, we proceed to check results, cancellation logic below handles partials

        # --- 4. Process Results and Check Atomicity ---
        filled_orders = []
        failed_orders = []
        order_details = {} # Store details for logging/potential cancellation

        for i, result in enumerate(order_results):
             leg_label = f"{label_prefix}_{i+1}"
             if isinstance(result, Exception):
                 logger.error(f"[Trade Attempt {trade_id}] Leg {i+1} ({legs[i]['action']} {legs[i]['instrument']}) failed: {result}")
                 failed_orders.append({"label": leg_label, "instrument": legs[i]['instrument'], "reason": str(result)})
                 order_details[leg_label] = {"status": "exception", "id": None}
             elif 'error' in result: # Should be caught by API client, but double-check
                 logger.error(f"[Trade Attempt {trade_id}] Leg {i+1} ({legs[i]['action']} {legs[i]['instrument']}) API Error: {result['error']}")
                 failed_orders.append({"label": leg_label, "instrument": legs[i]['instrument'], "reason": result['error']})
                 order_details[leg_label] = {"status": "api_error", "id": None}
             elif 'order' in result:
                 order_info = result['order']
                 order_id = order_info.get('order_id')
                 order_state = order_info.get('order_state')
                 filled_amount = order_info.get('filled_amount', 0) # Use filled_amount from response
                 order_details[leg_label] = {"status": order_state, "id": order_id, "filled": filled_amount}

                 logger.info(f"[Trade Attempt {trade_id}] Leg {i+1} ({leg_label}): ID {order_id}, State: {order_state}, Filled: {filled_amount}")

                 # Check if FOK order filled completely as expected
                 # Note: 'filled_amount' for options is in contracts
                 if order_state == 'filled' and filled_amount == contracts_to_trade:
                     filled_orders.append(order_info)
                 elif order_state == 'filled' and filled_amount != contracts_to_trade:
                      logger.warning(f"[Trade Attempt {trade_id}] Leg {i+1} ({leg_label}): State is 'filled' but filled_amount ({filled_amount}) != requested ({contracts_to_trade}). This shouldn't happen with FOK.")
                      # Treat as failure for safety, may need cancellation attempt
                      failed_orders.append({"label": leg_label, "instrument": legs[i]['instrument'], "reason": f"Partial Fill ({filled_amount}/{contracts_to_trade}) despite FOK?"})
                 elif order_state == 'cancelled' or order_state == 'rejected' or order_state == 'expired': # FOK failure states
                     failed_orders.append({"label": leg_label, "instrument": legs[i]['instrument'], "reason": f"Order state: {order_state}"})
                 else: # Unexpected state like 'open' (shouldn't happen with FOK) or 'untriggered'
                     logger.warning(f"[Trade Attempt {trade_id}] Leg {i+1} ({leg_label}): Unexpected state {order_state}. Treating as failure.")
                     failed_orders.append({"label": leg_label, "instrument": legs[i]['instrument'], "reason": f"Unexpected state: {order_state}"})
             else:
                 logger.error(f"[Trade Attempt {trade_id}] Leg {i+1} ({leg_label}): Invalid response format: {result}")
                 failed_orders.append({"label": leg_label, "instrument": legs[i]['instrument'], "reason": "Invalid response format"})
                 order_details[leg_label] = {"status": "invalid_response", "id": None}


        # --- 5. Final Outcome and Cleanup Attempt ---
        exec_time = time.perf_counter() - start_time
        if len(filled_orders) == 4 and len(failed_orders) == 0:
            # SUCCESS! All 4 legs filled via FOK.
            logger.info(f"✅ [Trade Success {trade_id}] Box spread successfully executed in {exec_time:.3f}s.")
            # Log fill details
            for order in filled_orders:
                 logger.info(f"  -> Filled: {order.get('direction')} {order.get('contracts')} {order.get('instrument_name')} @ {order.get('average_price', order.get('price'))} (ID: {order.get('order_id')})")
            return order_details # Indicate success

        else:
            # FAILURE or PARTIAL EXECUTION
            logger.error(f"❌ [Trade Failed {trade_id}] Box spread execution failed or partial ({len(filled_orders)}/4 filled) in {exec_time:.3f}s.")
            for failure in failed_orders:
                 logger.error(f"  -> Failed Leg: {failure['label']} ({failure['instrument']}) - Reason: {failure['reason']}")

            # --- Attempt to cancel any orders that might be open or were filled ---
            # This is crucial to avoid unwanted naked positions, but cancellation itself isn't guaranteed.
            logger.warning(f"[Trade Attempt {trade_id}] Attempting to cancel any non-failed/filled orders...")
            cancel_tasks = []
            for label, details in order_details.items():
                 # Attempt cancel if ID exists and status wasn't already failed/cancelled/rejected
                 if details['id'] and details['status'] not in ['cancelled', 'rejected', 'expired', 'exception', 'api_error', 'invalid_response']:
                     logger.info(f"   - Adding cancel task for Order ID: {details['id']} (Label: {label}, Status: {details['status']})")
                     cancel_tasks.append(asyncio.create_task(self.api_client.cancel(details['id'])))
                 elif not details['id']:
                     logger.warning(f"   - Cannot cancel {label} - Order ID unknown (Status: {details['status']})")

            if cancel_tasks:
                try:
                    cancel_results = await asyncio.gather(*cancel_tasks, return_exceptions=True)
                    for res in cancel_results:
                        if isinstance(res, Exception):
                            logger.error(f"   - Cancel attempt failed: {res}")
                        elif 'error' in res:
                             logger.error(f"   - Cancel attempt API Error: {res['error']}")
                        elif 'order_state' in res:
                             logger.info(f"   - Cancel result for {res.get('order_id')}: {res.get('order_state')}")
                        else:
                             logger.warning(f"   - Unknown cancel result: {res}")
                except Exception as e:
                    logger.error(f"[Trade Attempt {trade_id}] Error during cancellation gather: {e}")
            else:
                 logger.info("[Trade Attempt {trade_id}] No orders required cancellation attempts.")

            logger.critical(f"[Trade ATTENTION {trade_id}] Manual intervention may be required to check final position state!")
            return order_details # Indicate failure, return partial results


# -------------------------------------------------------------------
# Placeholder for Deribit Authenticated API Client
# (Replace with your actual implementation using aiohttp/websockets)
# -------------------------------------------------------------------
class DeribitAPIClient:
    """
    Placeholder for an authenticated Deribit API client.
    Handles authentication and provides methods for private API calls.
    Requires proper implementation using websockets or aiohttp.
    """
    def __init__(self, client_id, client_secret, ws_url="wss://www.deribit.com/ws/api/v2"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.ws_url = ws_url
        self.ws = None
        self.request_id_counter = int(time.time() * 1000)
        self.pending_requests = {}
        # In a real implementation, you'd handle connection, authentication,
        # message routing, and response correlation.

        logger.info("API Client Initialized (Placeholder). Requires implementation.")

    async def _send_request(self, method, params):
        # --- THIS IS A SIMPLIFIED PLACEHOLDER ---
        # A real implementation needs:
        # 1. Robust WebSocket connection management (connect, reconnect)
        # 2. Authentication flow (public/auth)
        # 3. Proper request ID generation and response matching
        # 4. Handling of responses, errors, and subscriptions
        request_id = self.request_id_counter
        self.request_id_counter += 1

        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        }

        # Simulate sending and receiving (replace with actual ws.send/recv logic)
        logger.debug(f"API SEND (Placeholder): {json.dumps(request)}")
        # --- Replace simulation with actual WebSocket interaction ---
        await asyncio.sleep(0.1) # Simulate network latency

        # Simulate possible outcomes (highly simplified)
        if method == "private/buy" or method == "private/sell":
            # Simulate FOK success/failure randomly for demo
            if random.random() < 0.8: # 80% chance of simulated success
                 simulated_response = {
                     "jsonrpc": "2.0",
                     "id": request_id,
                     "result": {
                         "order": {
                             "order_id": f"sim_{uuid.uuid4()}",
                             "instrument_name": params.get("instrument_name"),
                             "amount": params.get("amount"),
                             "contracts": params.get("contracts"),
                             "price": params.get("price"),
                             "order_state": "filled", # Simulate FOK success
                             "filled_amount": params.get("contracts"), # Assuming contracts used
                             "time_in_force": "fill_or_kill",
                             "direction": method.split('/')[-1],
                         },
                         "trades": [] # Simulate no immediate trades shown in basic response
                     }
                 }
                 logger.info(f"Simulated successful FOK order: {simulated_response['result']['order']['order_id']}")
            else:
                # Simulate FOK failure (cancelled)
                simulated_response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "order": {
                            "order_id": f"sim_{uuid.uuid4()}",
                            "instrument_name": params.get("instrument_name"),
                            "amount": params.get("amount"),
                            "contracts": params.get("contracts"),
                            "price": params.get("price"),
                            "order_state": "cancelled", # Simulate FOK failure
                            "filled_amount": 0,
                            "time_in_force": "fill_or_kill",
                            "direction": method.split('/')[-1],
                            "cancel_reason": "fill_or_kill"
                        },
                        "trades": []
                    }
                }
                logger.warning(f"Simulated FAILED FOK order: {simulated_response['result']['order']['order_id']}")
        elif method == "private/cancel":
             simulated_response = {
                 "jsonrpc": "2.0",
                 "id": request_id,
                 "result": { # Simulate successful cancel confirmation
                      "order_id": params.get("order_id"),
                      "order_state": "cancelled",
                      # ... other fields ...
                 }
             }
             logger.info(f"Simulated successful cancel for: {params.get('order_id')}")
        else:
             simulated_response = {
                 "jsonrpc": "2.0",
                 "id": request_id,
                 "error": {"code": -1, "message": "Method not simulated"}
             }

        # --- End Placeholder Simulation ---
        logger.debug(f"API RECV (Placeholder): {json.dumps(simulated_response)}")

        if 'error' in simulated_response:
             logger.error(f"API Error for request {request_id} ({method}): {simulated_response['error']}")
             raise Exception(f"API Error: {simulated_response['error'].get('message', 'Unknown error')}")
        return simulated_response['result']

    async def buy(self, **params):
        # Ensure FOK for atomicity attempt
        params['time_in_force'] = 'fill_or_kill'
        return await self._send_request("private/buy", params)

    async def sell(self, **params):
        # Ensure FOK for atomicity attempt
        params['time_in_force'] = 'fill_or_kill'
        return await self._send_request("private/sell", params)

    async def cancel(self, order_id):
        return await self._send_request("private/cancel", {"order_id": order_id})

    async def get_order_state(self, order_id):
         # Placeholder - Real client would query using private/get_order_state
         logger.warning("get_order_state is a placeholder, returning 'unknown'")
         await asyncio.sleep(0.05)
         return {"order_state": "unknown"} # Replace with actual API call

# -------------------------------------------------------------------
# Updated Main Loop Integration (Example)
# -------------------------------------------------------------------
async def main():
    # --- Configuration ---
    API_KEY = os.getenv("CLIENT_ID_1") # Load securely!
    API_SECRET = os.getenv("CLIENT_SECRET_1") # Load securely!
    MAX_USD_PER_BOX_TRADE_LEG = 2 # Example: Limit trade size
    EXECUTION_ENABLED = False # SAFETY SWITCH! Set to True to enable live trading attempts
    execution_attempted = False # Flag to control single execution.

    currencies = ["USDC"]
    coins_for_index = ["SOL", "XRP", "BNB", "PAXG"]
    desired_coins = {"SOL", "XRP", "BNB", "PAXG"}
    all_instruments = []

    # --- Setup ---
    logging.basicConfig(
        level=logging.INFO, # Use DEBUG for more verbose logs if needed
        format="%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] %(message)s", # Added logger name
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Check for real keys if execution is intended
    if EXECUTION_ENABLED and (API_KEY == "placeholder_key" or API_SECRET == "placeholder_secret"):
        logger.error("Execution is enabled, but API Key/Secret are placeholders! Disabling execution.")
        EXECUTION_ENABLED = False
        
    # Initialize API Client (Replace Placeholder)
    api_client = DeribitAPIClient(API_KEY, API_SECRET) # Replace with your actual client

    # Start background tasks
    asyncio.create_task(subscribe_index_prices(coins_for_index))

    for curr in currencies:
        instruments = HTTPMarketDataFetcher.get_instruments(curr)
        logger.info(f"Fetched {len(instruments)} instruments for {curr}.")
        all_instruments.extend(instruments)
    logger.info(f"Total instruments fetched: {len(all_instruments)}")

    filtered_instruments_dict = {inst["instrument_name"]: inst for inst in all_instruments
                                 if ((inst["instrument_name"].split('_')[0].upper() if "_" in inst["instrument_name"]
                                      else inst["instrument_name"].split('-')[0].upper()) in desired_coins)}
    filtered_instruments = list(filtered_instruments_dict.values())
    logger.info(f"Filtered instruments (unique): {len(filtered_instruments)}")

    instrument_names = [inst["instrument_name"] for inst in filtered_instruments]

    # Setup WS Fetcher
    multi_ws_fetcher = MultiWSMarketDataFetcher(instrument_names, heartbeat_interval=10)
    asyncio.create_task(multi_ws_fetcher.run())
    logger.info("Waiting 5 seconds for ticker caches to build up...")
    await asyncio.sleep(5)

    # Initialize Scanners
    coin_groups = HTTPMarketDataFetcher.group_options_by_coin(filtered_instruments)
    scanners = {}
    for coin in desired_coins:
         inst_list_for_coin = coin_groups.get(coin, [])
         if not inst_list_for_coin:
             continue
         scanner_currency = "USDC"
         scanners[coin] = BoxArbitrageScannerWS(
             multi_ws_fetcher,
             currency=scanner_currency,
             coins=[coin],
             hold_to_expiry=True,
             instruments=inst_list_for_coin
         )
         logger.info(f"Created scanner for {coin} options (instruments: {len(inst_list_for_coin)}).")

    # --- Execution Instance ---
    # CHANGED: Pass max_usd_per_trade
    executor = ArbitrageExecutor(api_client, MAX_USD_PER_BOX_TRADE_LEG)

    # --- Main Scan and Execute Loop ---
    executed_opportunities = set() # Still useful for tracking attempts even if only one is allowed

    while True:
        cycle_start = time.perf_counter()

        if execution_attempted:
            logger.debug("--- Starting scan cycle (Observation Mode Only) ---")
        else:
            logger.info("--- Starting scan cycle (Execution Enabled for First Opportunity) ---")


        # [ ... Scanning logic remains the same: get prices, run scanners ... ]
        current_index_prices = INDEX_PRICES.copy()
        tasks = []
        scanner_coins = []
        for coin, scanner in scanners.items():
             if coin in current_index_prices:
                 tasks.append(asyncio.to_thread(scanner.find_opportunities))
                 scanner_coins.append(coin)
             else:
                 logger.warning(f"Skipping scan for {coin} - No index price available.")

        if not tasks:
             logger.info("No scanners ready (missing index prices?). Sleeping.")
             await asyncio.sleep(1)
             continue

        results = await asyncio.gather(*tasks)
        all_opps = [opp for sublist in results for opp in sublist]


        if all_opps:
            sorted_opps = sorted(all_opps, key=lambda x: abs(x[1].net_cost_with_fees - x[1].theoretical_cost), reverse=True)

            # --- Execute Top Opportunity ONLY IF ENABLED and NOT ALREADY ATTEMPTED ---
            # MODIFIED: Added 'and not execution_attempted'
            if EXECUTION_ENABLED and not execution_attempted:
                logger.info(f"=== Found {len(sorted_opps)} potential opportunities this cycle (Execution ON) ===")
                executed_in_cycle = False # Renamed to avoid confusion with outer flag
                for diff, box in sorted_opps:
                    opp_key = (box.coin, box.expiry, box.K_low, box.K_high)

                    if opp_key in executed_opportunities:
                        logger.debug(f"Skipping already processed opportunity key: {opp_key}")
                        continue

                    logger.info(f"Top opportunity found for execution attempt: {box.coin} {box.expiry} {box.K_low}/{box.K_high}, Diff: {diff:.4f}")
                    box.print_details()

                    # *** EXECUTE THE TRADE ***
                    try:
                        execution_result = await executor.execute_box(box)
                        executed_opportunities.add(opp_key) # Mark as processed this run

                        if execution_result: # Check if execution was attempted (even if failed)
                             logger.info(f"Execution attempt completed for {opp_key}. Result summary: {execution_result}")
                             # SET THE FLAG TO PREVENT FURTHER EXECUTIONS
                             execution_attempted = True
                             logger.warning(">>> First execution attempt complete. Disabling further executions. Entering Observation Mode. <<<")
                             executed_in_cycle = True
                             break # Stop trying other opportunities in this cycle

                        else:
                             # execute_box returned None (e.g., pre-trade check failed like 0 liquidity)
                             logger.warning(f"Execution attempt aborted early for {opp_key} (likely pre-check failure). Will consider next opportunity.")
                             # Do NOT set execution_attempted = True here, as no API calls were made yet

                    except Exception as e:
                         logger.exception(f"!!! CRITICAL ERROR during execute_box call for {opp_key}: {e}")
                         executed_opportunities.add(opp_key) # Mark as processed
                         # SET THE FLAG on critical error during execution to prevent retries
                         execution_attempted = True
                         logger.warning(">>> Critical error during execution attempt. Disabling further executions. Entering Observation Mode. <<<")
                         executed_in_cycle = True
                         break # Stop trying this cycle

                if not executed_in_cycle and not execution_attempted:
                     logger.info("No opportunities met execution criteria this cycle. Execution remains enabled for next cycle.")

            else:
                 # Log top opportunities without executing (either disabled or already attempted)
                 if execution_attempted:
                      log_level = logging.DEBUG # Be less verbose if just observing
                      prefix = "Observation Mode"
                 else:
                      log_level = logging.INFO
                      prefix = "Execution Disabled"

                 logger.log(log_level, f"--- Top 3 Potential Box Spread Opportunities ({prefix}) ---")
                 for diff, box in sorted_opps[:3]:
                      # Use print_details logic but maybe at DEBUG level if observing
                      if execution_attempted:
                           logger.debug(f"Found Opp ({prefix}): {box.coin} {box.expiry} {box.K_low}/{box.K_high} Diff={diff:.4f}")
                           # box.print_details() # Maybe too verbose for every cycle in observation
                      else:
                            box.print_details()


        else:
            logger.info("No box spread opportunities found in this scan cycle.")

        cycle_end = time.perf_counter()
        logger.debug(f"--- Scan cycle finished in {(cycle_end - cycle_start)*1000:.2f}ms ---")

        sleep_time = 0.5 if not all_opps else 1.0
        logger.debug(f"Sleeping for {sleep_time} seconds...")
        await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    # Load API Keys securely here before running main!
    # e.g., from environment variables or a config file
    # os.environ['DERIBIT_API_KEY'] = "..."
    # os.environ['DERIBIT_API_SECRET'] = "..."

    # Add a check for keys before starting
    import os
    if not os.getenv("CLIENT_ID_1") or not os.getenv("CLIENT_SECRET_1"):
         # Use placeholder keys if not found, but WARN and keep execution disabled
         print("WARNING: API Key/Secret not found in environment variables. Using placeholders.")
         print("Ensure EXECUTION_ENABLED is False in main() unless using real keys!")
         # Assign dummy values so the script can run for analysis, but executor won't work
         os.environ['CLIENT_ID_1'] = 'placeholder_key'
         os.environ['CLIENT_SECRET_1'] = 'placeholder_secret'

    # Make sure to set EXECUTION_ENABLED = True in main() ONLY when ready for live attempts
    # with a fully implemented and tested DeribitAPIClient and valid keys.

    try:
        asyncio.run(main()) # Use asyncio.run for modern Python
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
    except Exception as e:
        logger.exception("Unhandled exception in main loop:")