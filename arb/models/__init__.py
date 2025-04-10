"""
Data models representing financial concepts like arbitrage opportunities.
"""
from .arbitrage import ArbitrageOpportunity, SyntheticLongArbitrage, SyntheticShortArbitrage, BoxArbitrage

__all__ = [
    "ArbitrageOpportunity",
    "SyntheticLongArbitrage",
    "SyntheticShortArbitrage",
    "BoxArbitrage",
]