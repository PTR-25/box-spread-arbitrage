"""
Module for holding shared global state.

Warning: Use global state sparingly. It can make testing and reasoning about
         the application harder. Consider passing state explicitly or using
         dedicated state management classes if complexity grows.
"""

from typing import Dict

# Dictionary to store the latest index prices fetched by the background task.
# Key: Coin symbol (str, e.g., "SOL"), Value: Price (float)
INDEX_PRICES: Dict[str, float] = {}