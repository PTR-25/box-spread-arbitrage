"""
Data models representing financial concepts like arbitrage opportunities.
"""
from .arbitrage import ArbitrageOpportunity, BoxArbitrage

__all__ = [
    "ArbitrageOpportunity",
    "BoxArbitrage",
]