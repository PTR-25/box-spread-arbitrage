# deribit_arb/api/client.py
"""
Module containing the authenticated Deribit WebSocket API client logic.
Connection management loop is handled externally using async with.
"""

import asyncio
import websockets
import json
import time
import hmac
import hashlib
import logging
import uuid
from typing import Optional, Dict, Any
from websockets.protocol import State
from .exceptions import DeribitAPIError
from websockets.protocol import State
logger = logging.getLogger(__name__)

class DeribitAPIClient:
    """
    Holds Deribit API credentials, authentication state, pending requests,
    and methods for interacting with the API.

    The actual WebSocket connection and listening loop are managed externally
    to correctly use 'async with websockets.connect'.

    Args:
        client_id (str): Deribit API Client ID.
        client_secret (str): Deribit API Client Secret.
    """
    def __init__(self, client_id: str, client_secret: str):
        if not client_id or not client_secret:
            raise ValueError("Client ID and Client Secret cannot be empty.")
        self.client_id = client_id
        self.client_secret = client_secret

        # --- State managed by this class ---
        self._ws: Optional[websockets.WebSocketClientProtocol] = None # Holds the *active* protocol object
        self._ws_lock = asyncio.Lock() # Lock specifically for accessing/modifying _ws
        self._auth_lock = asyncio.Lock() # Lock for modifying _is_authenticated
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._request_id_counter: int = 1
        self._is_authenticated: bool = False

        logger.info(f"API Client Logic Initialized for {client_id[:5]}...")

    # --- Connection State Management (Called Externally) ---

    async def set_websocket(self, ws: Optional[websockets.WebSocketClientProtocol]):
        async with self._ws_lock:
            if ws:
                # Use duck typing to check if the object has the required methods.
                if (hasattr(ws, "send") and hasattr(ws, "recv") and hasattr(ws, "close")):
                    logger.info(f"Active WebSocket protocol object set (ID: {getattr(ws, 'id', 'N/A')}).")
                    self._ws = ws
                    async with self._auth_lock:
                        self._is_authenticated = False
                else:
                    logger.error(f"Attempted to set invalid websocket object type: {type(ws)}")
                    self._ws = None
                    async with self._auth_lock:
                        self._is_authenticated = False
            else:
                # Clearing the websocket
                if self._ws:
                    logger.warning(f"Clearing active WebSocket protocol object (ID: {getattr(self._ws, 'id', 'N/A')}).")
                    # Fail pending requests
                    pending = list(self._pending_requests.items())
                    self._pending_requests.clear()
                    for req_id, future in pending:
                        if not future.done():
                            future.set_exception(ConnectionAbortedError(
                                f"Connection closed while request {req_id} was pending"))
                    logger.debug(f"Failed {len(pending)} pending requests due to disconnect.")
                self._ws = None
                async with self._auth_lock:
                    self._is_authenticated = False


    # --- Properties ---

    @property
    def is_connected(self) -> bool:
        ws = self._ws
        return ws is not None and ws.state == State.OPEN


    @property
    def is_authenticated(self) -> bool:
        """Check if the client considers itself connected and authenticated."""
        # Reading _is_authenticated might not strictly need a lock
        # Let's try without lock first.
        return self.is_connected and self._is_authenticated

    # --- Core Logic Methods ---

    async def run_listener_loop(self):
        """
        Listens for messages on the currently set websocket.
        This should be run by the external connection loop once authenticated.
        Exits when the websocket connection is closed.
        """
        # Get the websocket reference *once* at the start of the loop run
        async with self._ws_lock:
            current_ws = self._ws
            
        if not current_ws or current_ws.state != State.OPEN:
            logger.error("Listener loop called but client is not connected.")
            return

        ws_id = current_ws.id
        logger.info(f"Listener loop started for WebSocket ID: {ws_id}")
        try:
            async for message in current_ws:
                try:
                    data = json.loads(message)
                    logger.debug(f"RECV (WSID {ws_id}): {json.dumps(data)}")

                    if 'method' in data:
                        method = data['method']; params = data.get('params', {})
                        if method == 'subscription': await self._handle_subscription(params)
                        elif method == 'heartbeat': await self._handle_heartbeat(params)
                        elif method == 'test_request': await self._send_test_response(current_ws) # Pass ws
                        else: logger.warning(f"Received unhandled method: {method}")
                    elif 'id' in data: await self._handle_response(data)
                    else: logger.warning(f"Received message without 'id' or 'method': {data}")

                except json.JSONDecodeError: logger.error(f"Failed to decode JSON message: {message}")
                except Exception as e: logger.exception(f"Error processing received message: {e}")

        except websockets.exceptions.ConnectionClosedOK:
             logger.info(f"Listener loop stopped: Connection closed normally (WSID {ws_id}).")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"Listener loop stopped: Connection closed with error (WSID {ws_id}, Code: {e.code}, Reason: {e.reason})")
        except asyncio.CancelledError:
             logger.info(f"Listener loop explicitly cancelled (WSID {ws_id}).")
             # Don't re-raise, allow external loop to handle cancellation if needed
        except Exception as e:
            logger.exception(f"Listener loop stopped unexpectedly (WSID {ws_id}): {e}")
        finally:
            logger.info(f"Listener loop finished cleanup (WSID {ws_id}).")
            # Signal that this connection is gone by clearing the reference
            # Use the main set_websocket method for consistency and locking
            await self.set_websocket(None)


    async def _handle_response(self, response: dict):
        """Handles JSON-RPC responses, matching them to pending requests."""
        # [ ... Same as before ... ]
        req_id = response.get('id');
        if req_id is None: logger.warning(f"Received response-like message with no ID: {response}"); return
        future = self._pending_requests.pop(req_id, None)
        if future:
            if not future.done():
                if 'error' in response: error_obj = DeribitAPIError(response['error']); logger.error(f"API Error for request ID {req_id}: {error_obj}"); future.set_exception(error_obj)
                elif 'result' in response: future.set_result(response['result'])
                else: err_msg = f"Invalid response format for request ID {req_id}: {response}"; logger.error(err_msg); future.set_exception(Exception(err_msg))
            else: logger.warning(f"Received response for request ID {req_id}, but future was already done.")
        else: logger.warning(f"Received response with unknown or timed-out request ID: {req_id}")

    async def _handle_subscription(self, params: dict):
        """Handles subscription notifications."""
        # [ ... Same as before ... ]
        channel = params.get('channel');
        if not channel: logger.warning(f"Received subscription message with no channel: {params}"); return
        logger.debug(f"Subscription update received for channel '{channel}'.")

    async def _handle_heartbeat(self, params: dict):
        """Handles heartbeat notifications."""
        # [ ... Same as before ... ]
        hb_type = params.get('type');
        if hb_type == 'test_request':
             logger.warning("Received heartbeat of type 'test_request', responding.")
             # Need the current websocket to respond
             async with self._ws_lock:
                 ws = self._ws
             if ws and ws.state == State.OPEN:
                 await self._send_test_response(ws)
             else:
                 logger.warning("Cannot respond to test_request, websocket not available.")

    async def _send_test_response(self, ws: websockets.WebSocketClientProtocol):
        """Sends a response to the server's test_request using the provided websocket."""
        request = {"jsonrpc": "2.0", "method": "public/test", "params": {}};
        try:
            # Use the passed 'ws' object directly
            await self._send_json_on_ws(request, ws) # Use helper
            logger.debug(f"Sent public/test response on WSID {ws.id}.")
        except ConnectionError:
             logger.warning(f"Could not send test response on WSID {ws.id}, connection likely closed.")
        except Exception as e:
            logger.error(f"Failed to send public/test response on WSID {ws.id}: {e}")

    async def authenticate(self) -> bool:
        async with self._ws_lock:
            ws = self._ws

        if not ws or ws.state != State.OPEN:
            logger.error("Cannot authenticate: Not connected (state is not OPEN).")
            return False

        async with self._auth_lock:
            # No need to check ws.close here; the above check is sufficient.
            try:
                timestamp = int(time.time() * 1000)
                nonce = uuid.uuid4().hex[:8]
                data = ""
                string_to_sign = f"{timestamp}\n{nonce}\n{data}"
                signature = hmac.new(self.client_secret.encode('utf-8'),
                                    string_to_sign.encode('utf-8'),
                                    hashlib.sha256).hexdigest()
                auth_params = {
                    "grant_type": "client_signature",
                    "client_id": self.client_id,
                    "timestamp": timestamp,
                    "signature": signature,
                    "nonce": nonce,
                    "data": data,
                }

                logger.info(f"Sending authentication request on WSID {getattr(ws, 'id', 'N/A')}...")
                response = await self.send_public_request("public/auth", auth_params)

                if response and 'access_token' in response:
                    self._is_authenticated = True
                    logger.info(f"Authentication successful on WSID {getattr(ws, 'id', 'N/A')}. Scope: {response.get('scope')}")
                    return True
                else:
                    logger.error(f"Authentication failed on WSID {getattr(ws, 'id', 'N/A')}: No access token in response. Response: {response}")
                    self._is_authenticated = False
                    return False

            except Exception as e:
                logger.exception(f"Unexpected error during authentication on WSID {getattr(ws, 'id', 'N/A')}: {e}")
                self._is_authenticated = False
                return False


            
    async def _send_json_on_ws(self, request: dict, ws: websockets.WebSocketClientProtocol):
        """Internal helper to send a JSON request on a specific WebSocket object."""
        if not ws or ws.state != State.OPEN:
             raise ConnectionError("Cannot send request: Provided WebSocket is not connected.")
        try:
            message = json.dumps(request)
            logger.debug(f"SEND (WSID {ws.id}): {message}")
            await ws.send(message)
        except websockets.exceptions.ConnectionClosed as e:
             logger.error(f"Connection closed while trying to send (WSID {ws.id}): {e}")
             # Don't trigger reconnect here, external loop handles it
             raise ConnectionError("Connection closed") from e
        except Exception as e:
             logger.exception(f"Error sending JSON message (WSID {ws.id}): {e}")
             raise

    async def _send_request(self, method: str, params: dict, is_private: bool = True) -> dict:
        """
        Core internal method to prepare requests and wait for responses.
        Uses the currently active websocket stored in self._ws.
        """
        # Get current websocket reference under lock
        async with self._ws_lock:
            current_ws = self._ws
            # Check connection status while holding lock
            if not current_ws or current_ws.state != State.OPEN:
                raise ConnectionError(f"Cannot send request {method}: Not connected.")


            # Check authentication status while holding lock
            if is_private:
                 async with self._auth_lock:
                      if not self._is_authenticated:
                           raise PermissionError(f"Cannot send private request {method}: Not authenticated.")

        # --- Proceed with sending on the obtained 'current_ws' ---
        req_id = self._request_id_counter; self._request_id_counter += 1
        request = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        future = asyncio.get_event_loop().create_future(); self._pending_requests[req_id] = future

        try:
            # Use the helper to send on the specific websocket object
            await self._send_json_on_ws(request, current_ws)
            result = await asyncio.wait_for(future, timeout=15.0)
            return result
        except asyncio.TimeoutError:
            logger.error(f"Request ID {req_id} ({method}) timed out.")
            self._pending_requests.pop(req_id, None)
            if not future.done(): future.cancel();
            raise TimeoutError(f"Request {req_id} ({method}) timed out")
        except Exception as e:
             self._pending_requests.pop(req_id, None);
             if not future.done(): future.set_exception(e)
             if not isinstance(e, (ConnectionError, PermissionError, DeribitAPIError, TimeoutError)):
                 logger.exception(f"Unexpected error during request {req_id} ({method}): {e}")
             raise

    # --- Public/Private Wrappers ---
    async def send_public_request(self, method: str, params: Optional[dict] = None) -> dict:
        return await self._send_request(method, params or {}, is_private=False)
    async def send_private_request(self, method: str, params: Optional[dict] = None) -> dict:
        return await self._send_request(method, params or {}, is_private=True)

    # --- Specific API Methods ---
    async def buy(self, **params) -> dict: return await self.send_private_request("private/buy", params)
    async def sell(self, **params) -> dict: return await self.send_private_request("private/sell", params)
    async def cancel(self, order_id: str) -> dict: return await self.send_private_request("private/cancel", {"order_id": order_id})
    async def get_order_state(self, order_id: str) -> dict: return await self.send_private_request("private/get_order_state", {"order_id": order_id})
    async def get_position(self, instrument_name: str) -> dict: return await self.send_private_request("private/get_position", {"instrument_name": instrument_name})
    async def get_account_summary(self, currency: str, extended: bool = False) -> dict: return await self.send_private_request("private/get_account_summary", {"currency": currency, "extended": extended})

    # --- Shutdown Signal ---
    # No disconnect method needed here, external loop handles closing via 'async with'