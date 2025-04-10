"""
Configuration settings for the Deribit Arbitrage Bot.

Loads sensitive information like API keys from environment variables.
Defines application constants.
"""

import os
import logging
from dotenv import load_dotenv # <--- IMPORT load_dotenv

logger = logging.getLogger(__name__)

# --- Load environment variables from .env file ---
# Determine project root directory (assumes config/settings.py is 2 levels below root)
current_file_dir = os.path.dirname(os.path.abspath(__file__))          # .../box-spread-arbitrage/config
project_root = os.path.abspath(os.path.join(current_file_dir, ".."))   # .../box-spread-arbitrage
dotenv_path = os.path.join(project_root, ".env")

# print(f"Loaded environment variables from: {dotenv_path}")  # Optional for debugging

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)
else:
    logger.warning(f".env file not found at expected path: {dotenv_path}")
    
    
# --- Deribit API Configuration ---
DERIBIT_API_URL = os.getenv("DERIBIT_API_URL", "https://www.deribit.com/api/v2")
DERIBIT_WS_URL = os.getenv("DERIBIT_WS_URL", "wss://www.deribit.com/ws/api/v2")

# Load API Credentials securely from environment variables
# Replace 'YOUR_CLIENT_ID' and 'YOUR_CLIENT_SECRET' with your actual env var names if different
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# --- Arbitrage Strategy Configuration ---
RISK_FREE_RATE = float(os.getenv("RISK_FREE_RATE", 0.08)) # Annual risk-free rate for theoretical calculations
OPPORTUNITY_THRESHOLD = float(os.getenv("OPPORTUNITY_THRESHOLD", 0.0)) # Default: $0.0
# --- Market Data Configuration ---
# Max instruments per public WebSocket connection (adjust based on performance/limits)
MAX_INSTRUMENTS_PER_WS = int(os.getenv("MAX_INSTRUMENTS_PER_WS", 500))
# List of coins for which to fetch index prices
COINS_FOR_INDEX_PRICE = os.getenv("COINS_FOR_INDEX_PRICE", "SOL,XRP,BNB,PAXG").split(',')
# Set of coins for which to fetch options and scan for arbitrage
DESIRED_COINS_SET = set(os.getenv("DESIRED_COINS_SET", "SOL,XRP,BNB,PAXG").split(','))
# Currencies for which to fetch option instruments initially (usually USDC for desired altcoins)
FETCH_INSTRUMENT_CURRENCIES = os.getenv("FETCH_INSTRUMENT_CURRENCIES", "USDC").split(',')

# --- Execution Configuration ---
# SAFETY SWITCH: Set to "true" (case-insensitive) via env var to enable execution attempts
EXECUTION_ENABLED_STR = os.getenv("EXECUTION_ENABLED", "False").lower()
EXECUTION_ENABLED = EXECUTION_ENABLED_STR == 'true'

# Maximum target USD value per leg of a box trade. Actual size limited by liquidity.
MAX_USD_PER_BOX_TRADE_LEG = float(os.getenv("MAX_USD_PER_BOX_TRADE_LEG", 500.0))

# --- Validation ---
if EXECUTION_ENABLED and (not CLIENT_ID or not CLIENT_SECRET):
    logger.warning("EXECUTION_ENABLED is True, but DERIBIT_CLIENT_ID or DERIBIT_CLIENT_SECRET are not set.")
    logger.warning("Execution will likely fail. Disabling execution for safety.")
    EXECUTION_ENABLED = False

if MAX_USD_PER_BOX_TRADE_LEG <= 0:
     raise ValueError("MAX_USD_PER_BOX_TRADE_LEG must be positive.")

logger.info("Configuration loaded:")
logger.info(f"  DERIBIT_API_URL: {DERIBIT_API_URL}")
logger.info(f"  DERIBIT_WS_URL: {DERIBIT_WS_URL}")
logger.info(f"  CLIENT_ID Set: {'Yes' if CLIENT_ID else 'No'}")
logger.info(f"  RISK_FREE_RATE: {RISK_FREE_RATE}")
logger.info(f"  MAX_INSTRUMENTS_PER_WS: {MAX_INSTRUMENTS_PER_WS}")
logger.info(f"  COINS_FOR_INDEX_PRICE: {COINS_FOR_INDEX_PRICE}")
logger.info(f"  DESIRED_COINS_SET: {DESIRED_COINS_SET}")
logger.info(f"  FETCH_INSTRUMENT_CURRENCIES: {FETCH_INSTRUMENT_CURRENCIES}")
logger.info(f"  EXECUTION_ENABLED: {EXECUTION_ENABLED}")
logger.info(f"  MAX_USD_PER_BOX_TRADE_LEG: {MAX_USD_PER_BOX_TRADE_LEG}")