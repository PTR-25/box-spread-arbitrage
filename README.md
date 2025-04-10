# Deribit Box Spread Arbitrage Scanner

## Overview

This project implements an asynchronous Python application designed to scan the Deribit cryptocurrency derivatives exchange for potential **Box Spread arbitrage opportunities** on linear (USDC-settled) options markets (e.g., SOL, XRP, BNB, PAXG).

It connects to Deribit's public WebSocket API to receive real-time market data (ticker snapshots), calculates the theoretical value of box spreads across various strike combinations and expiries, compares it to the current market cost (including bid ask spreads and estimated fees), and logs potential opportunities that exceed a defined profit threshold.

**Note:** This version focuses solely on **scanning and identifying** potential opportunities. It **does not** include automated trade execution functionality.

## Key Features

*   **Real-time Data:** Connects to Deribit via WebSockets for low-latency market data.
*   **Multi-Asset Scanning:** Configurable to scan multiple USDC-settled underlying assets (e.g., SOL, XRP, BNB, PAXG) concurrently.
*   **Comprehensive Box Spread Analysis:**
    *   Evaluates *all* valid combinations of strike prices (K_low, K_high) for each available expiry date.
    *   Calculates theoretical box value based on risk-free interest rate and time to expiry.
    *   Calculates the net cost/proceeds for both buying (long) and selling (short) the box spread, using current Best Bid/Offer (BBO) data.
    *   Incorporates Deribit's fee structure (taker execution + settlement) for linear options, accounting for contract multipliers.
    *   Identifies potential arbitrage if the difference between market cost/proceeds and theoretical value exceeds a configurable threshold.
*   **Contract Multiplier Handling:** Correctly accounts for Deribit's contract multipliers (e.g., 10 for SOL, 1000 for XRP) in value and fee calculations. Even though the UI of Deribit shows the per coin price, the actual value is multiplied by the contract multiplier. For example, if on the UI it is shown that one SOL-{maturity}-{strike}-{C/P} costs 5.65 USD, since it is the per coin value, it actually costs 5.65 * {sol_contract_multiplier} = 5.65 * 10 = 56.5 USD.
*   **Asynchronous Architecture:** Built using Python's `asyncio` for efficient handling of concurrent network I/O and tasks.
*   **Modular Design:** Code is structured into logical packages (api, models, services, utils, config) for readability and maintainability.
*   **Configuration:** Key parameters (API URLs, assets to scan, threshold, etc.) are configurable via environment variables or a `.env` file.
*   **Robust Connection Management:** Includes automatic reconnection logic for WebSocket connections.
*   **Graceful Shutdown:** Handles `Ctrl+C` (SIGINT) and SIGTERM for clean termination of background tasks.
## Project Structure

```
box-spread-arbitrage/
├── config/                 # Configuration files
│   ├── __init__.py
│   └── settings.py         # Loads settings & API keys from environment variables
├── arb/            # Main application package
│   ├── __init__.py
│   ├── api/                # Modules for interacting with Deribit API
│   │   ├── __init__.py
│   │   ├── client.py         # Authenticated WebSocket client
│   │   ├── exceptions.py     # Custom API exceptions
│   │   ├── http_fetcher.py   # Public data fetching via HTTP
│   │   └── public_ws.py      # Public data fetching via WebSocket
│   ├── models/             # Data models (e.g., Arbitrage opportunities)
│   │   ├── __init__.py
│   │   └── arbitrage.py
│   ├── services/           # Core logic (scanning, execution)
│   │   ├── __init__.py
│   │   └── scanner.py        # Opportunity scanning logic
│   └── utils/              # Utility modules
│       ├── __init__.py
│       ├── fees.py           # Fee calculation logic
│       ├── globals.py        # Shared global state (e.g., index prices)
│       └── logging_config.py # Logging setup
├── main.py                 # Main application entry point
├── requirements.txt        # Python dependencies
└── README.md               # This file
```
## Technical Details & Considerations

*   **Data Feed:** The scanner currently uses the `ticker.{instrument_name}.100ms` WebSocket channel by default. This provides reliable Best Bid/Offer (BBO) snapshots every 100ms, suitable for this strategy. Integration with the `book.{instrument_name}.*` channel is possible (code exists within `public_ws.py`) but requires careful state management and verification of amount units.
*   **API Amounts:** Based on testing and documentation review, the `best_ask/bid_amount` fields from the Deribit `ticker.*` WebSocket API represent the quantity in the **underlying asset** (e.g., number of SOL), not the number of contracts. The code accounts for this when calculating total values and determining tradeable contract sizes (though execution is currently disabled).
*   **Market Efficiency & Liquidity:** Box spread arbitrage opportunities are typically rare and fleeting in efficient markets, especially for liquid assets like BTC/ETH. While USDC altcoin options might be less efficient, they often suffer from low liquidity (wide spreads, zero bids/asks), making theoretical opportunities difficult or impossible to execute profitably. The scanner logs indicate when potential boxes are skipped due to missing quotes.
*   **Competition:** Be aware that numerous professional market makers and high-frequency trading firms operate on Deribit, employing sophisticated, low-latency strategies to capture arbitrage opportunities. Competing purely on speed is generally not feasible with this type of setup.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd box-spread-arbitrage
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    Create a `.env` file in the project root directory (copy from `.env.example`) or set system environment variables. **No API keys are strictly required for the scanner-only version**, but URLs might need adjustment for testnet vs. mainnet.

    **`.env` Example:**
    ```dotenv
    # .env file

    # --- URLs (Use testnet for development/testing) ---
    # DERIBIT_WS_URL="wss://test.deribit.com/ws/api/v2"
    # DERIBIT_API_URL="https://test.deribit.com/api/v2"
    DERIBIT_WS_URL="wss://www.deribit.com/ws/api/v2"
    DERIBIT_API_URL="https://www.deribit.com/api/v2"

    # --- Scanner Configuration ---
    # Comma-separated list of coins for index price subscription
    COINS_FOR_INDEX_PRICE="SOL,XRP,BNB,PAXG"
    # Comma-separated list of coins to actively scan for box spreads
    DESIRED_COINS_SET="SOL,XRP,BNB,PAXG"
    # Base currency for fetching instruments (usually USDC for these coins)
    FETCH_INSTRUMENT_CURRENCIES="USDC"
    # Minimum profit potential (per contract for linear) to log as opportunity
    OPPORTUNITY_THRESHOLD="0.10" # e.g., $0.10

    # --- Other Settings ---
    RISK_FREE_RATE="0.05" # Assumed annual risk-free rate for theoretical value
    LOG_LEVEL="INFO" # Use DEBUG for more verbose output
    # MAX_INSTRUMENTS_PER_WS="500" # Max instruments per public WS connection

    # --- Execution (DISABLED for Scanner-Only Version) ---
    # EXECUTION_ENABLED="False"
    # MAX_USD_PER_BOX_TRADE_LEG="500.0"
    # DERIBIT_CLIENT_ID=""
    # DERIBIT_CLIENT_SECRET=""
    ```

## Running the Scanner

Ensure your virtual environment is activated.

```bash
python main.py
```
The application will:

1. Load configuration.

2. Start background tasks to fetch index prices and ticker/book data via WebSockets.

3. Initialize scanners for the configured coins.

4. Enter the main loop, continuously:

4.1 Running the scanners to find potential box spread opportunities based on the latest market data.

4.2 Logging summary statistics for each scan cycle.

4.3 Logging details of any identified opportunities that exceed the OPPORTUNITY_THRESHOLD.

## Stopping the Scanner
Press Ctrl+C in the terminal. The application will catch the signal and attempt a graceful shutdown of all background tasks and connections.

## Disclaimer
This software is for educational and informational purposes only. Cryptocurrency derivatives trading involves significant risk. Market data can be delayed or inaccurate. Calculations may contain errors. This software does not provide financial advice. Use at your own risk. The authors are not responsible for any decisions made or losses incurred based on the output of this scanner.

