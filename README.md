# Deribit Box-Spread Arbitrage Scanner

Asynchronous Python scanner for box-spread mispricings on Deribit's linear (USDC-settled) options markets (e.g. SOL, XRP, BNB, PAXG).

The scanner streams live Best Bid/Offer (BBO) snapshots from Deribit's WebSocket API, evaluates all strike pairs `(K_low, K_high)` per expiry for both long and short boxes, deducts taker and settlement fees, and logs candidates above a configured profit threshold. **Scanner only — no automated order execution.**

## Pricing & fee mechanics

- **Box valuation.** Compares the executable 4-leg BBO cost (long box: buy `K_low` call + sell `K_low` put + sell `K_high` call + buy `K_high` put; short box: inverse) against the discounted terminal payoff `(K_high - K_low) * exp(-rT)`.
- **Contract multipliers & sizes.** Deribit linear option quotes are per unit of the underlying while contract notional scales by the contract multiplier (e.g. `10` for SOL, `1000` for XRP), and `ticker.*` bid/ask amounts are reported in underlying units. Both are normalized before computing contract P&L and fees.
- **Deribit fee schedule.** Models taker execution (`min(0.05% × index, 12.5% × premium)`) and optional expiry settlement (`min(0.015% × index, 12.5% × premium)`) per leg in `arb/utils/fees.py`.
- **Data feed.** Subscribes to `ticker.<instrument>.100ms` across sharded public WebSocket connections (`MAX_INSTRUMENTS_PER_WS`, default `500`), alongside live index-price feeds.

## Layout

| Path | What |
|---|---|
| `main.py` | async entry point, WebSocket lifecycle, concurrent scan loop, graceful shutdown |
| `config/settings.py` | environment configuration and defaults |
| `arb/api/http_fetcher.py` | instrument discovery via Deribit REST API |
| `arb/api/public_ws.py` | sharded public WebSocket market-data and index-price fetchers |
| `arb/api/client.py` | authenticated Deribit WebSocket client |
| `arb/models/arbitrage.py` | box-spread valuation and opportunity formatting |
| `arb/services/scanner.py` | per-coin strike-pair scanning and filter statistics |
| `arb/utils/fees.py` | Deribit USDC linear option execution and settlement fee calculator |

## Setup & run

```bash
git clone https://github.com/PTR-25/box-spread-arbitrage.git
cd box-spread-arbitrage
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file in the repo root (or export the variables). `main.py` authenticates a Deribit client session on startup using `CLIENT_ID` and `CLIENT_SECRET` (testnet credentials work with the testnet URLs):

```dotenv
DERIBIT_WS_URL="wss://www.deribit.com/ws/api/v2"
DERIBIT_API_URL="https://www.deribit.com/api/v2"
CLIENT_ID="your_client_id"
CLIENT_SECRET="your_client_secret"

COINS_FOR_INDEX_PRICE="SOL,XRP,BNB,PAXG"
DESIRED_COINS_SET="SOL,XRP,BNB,PAXG"
FETCH_INSTRUMENT_CURRENCIES="USDC"
OPPORTUNITY_THRESHOLD="0.10"
RISK_FREE_RATE="0.05"
LOG_LEVEL="INFO"
```

Run the scanner (`Ctrl+C` triggers graceful shutdown):

```bash
python main.py
```

## Notes

- Genuine box-spread arbitrage is rare on liquid options; on thinner USDC altcoin chains, wide spreads and missing quotes typically dominate apparent dislocations (skipped boxes are counted in scanner logs).
- Built for market-structure study rather than latency competition. Not financial advice.
