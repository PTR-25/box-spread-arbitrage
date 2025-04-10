"""
Deribit API Interaction Package.

Provides classes and functions for interacting with both public and private
Deribit API endpoints via HTTP and WebSockets.
"""

from .exceptions import DeribitAPIError
from .client import DeribitAPIClient
from .http_fetcher import HTTPMarketDataFetcher
from .public_ws import WSMarketDataFetcher, MultiWSMarketDataFetcher, subscribe_index_prices

__all__ = [
    "DeribitAPIError",
    "DeribitAPIClient",
    "HTTPMarketDataFetcher",
    "WSMarketDataFetcher",
    "MultiWSMarketDataFetcher",
    "subscribe_index_prices",
]