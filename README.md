# box-spread-arbitrage

[![CI](https://github.com/PTR-25/box-spread-arbitrage/actions/workflows/ci.yml/badge.svg)](https://github.com/PTR-25/box-spread-arbitrage/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Event-driven Rust **static options arbitrage scanner**, **implied financing term-structure analyzer**, and **tick-by-tick Parquet backtester** for **Deribit** — covering both **inverse coin-settled** (`BTC`, `ETH`) and **linear USDC-settled** (`BTC_USDC`, `ETH_USDC`, `SOL_USDC`, `XRP_USDC`, `AVAX_USDC`, `TRX_USDC`, etc.) options and dated futures.

Built directly on top of [`deribit-tick-collector`](https://github.com/PTR-25/deribit-tick-collector) for zero-auth multi-shard WebSocket ingestion, exact L2 order book state reconstruction, live contract metadata (`taker_commission`, `min_trade_amount`), and Hive-partitioned Parquet v2 replay.

---

## Capabilities

- **Four Static No-Arbitrage Strategies** (linear USDC & inverse coin-settled):
  1. **Box Spreads (`box_long`, `box_short`, `box_inv_usd_long`, `box_inv_usd_short`)**:
     - Linear USDC 1:1 boxes (payoff $W = K_2 - K_1$ USDC, undiscounted on Deribit since USDC equity earns 0% and `equity = cash + OptionsMtM` prevents borrowing).
     - Inverse **coin-numeraire ratio boxes** ($m = K_1 / K_2$, constant payoff $1 - K_1/K_2$ coin, measures pure coin lending rate $r_{\text{coin}} \approx 0\%$).
     - Inverse **USD-numeraire 1:1 boxes** (coin payoff $W / S_T$ = exact $W$ USD at expiry, fair coin cost $W / F$ $\implies$ USD cost $(W/F)\cdot S_0 = W e^{-r_{\text{USD}} T}$ carrying the positive USD/futures-carry implied rate!), hedged into zero-$S_T$-risk coin arbitrage with $W = K_2 - K_1$ USD of the matched-expiry inverse dated future.
  2. **Conversions & Reversals (`conversion`, `reversal`)** — exact put-call parity against live matched-expiry dated futures (handling linear underlying-unit future books and inverse USD-notional future books with exact hedge ratio $K$ USD per 1 coin of options).
  3. **Vertical Spreads (`vertical_call_mono`, `vertical_put_mono`, `vertical_call_cap`, `vertical_put_cap`)** — strike monotonicity and maximum payoff-cap violations across all strike pairs.
  4. **Butterflies (`butterfly_call`, `butterfly_put`)** — call and put convexity bounds (risk-neutral density non-negativity) on non-uniform adjacent strike triples.
- **Full Multi-Level L2 Order Book Sweeping**:
  - Fast-path zero-allocation BBO rejection when top-of-book is unprofitable.
  - Full L2 depth sweeping across 2–4 legs simultaneously with arbitrary leg ratios, discrete `min_trade_amount` quantization, exact VWAP fill prices, and cumulative post-fee USD profit.
- **Exact Deribit Fee & Margin Mechanics**:
  - Live per-instrument `taker_commission` and `min_trade_amount` pulled from Deribit instrument metadata.
  - Exact 12.5% option premium fee cap (`min(taker_rate * index, 12.5% * premium)` for linear; `min(taker_rate, 12.5% * premium_coin)` for inverse).
  - Expiry delivery fees charged **only on ITM legs** (1.5 bps; OTM legs expire at zero fee; ITM option delivery nets opposing dated futures on conversion/reversal).
  - Defaults `usdc_benchmark_rate = 0.0` and `coin_benchmark_rate = 0.0` to match Deribit's zero-interest margin mechanics (avoiding phantom short-box "arbitrage" from locked short-option proceeds), while allowing custom rates for yield-bearing collateral / financing-carry studies.
- **Tick-by-Tick Parquet Replay & Synthetic Latency-Decay Simulator**:
  - Replays datasets recorded by `deribit-tick-collector` (`dataset=instruments`, `book_updates`, `indices`, `tickers`) in exact chronological `(exchange_ts, change_id)` order, automatically deduplicating redundant hourly quiet-instrument checkpoints.
  - Tracks every arbitrage **episode** from exact opening millisecond to closing millisecond (`duration_ms`, peak profit, peak edge bps, ticks observed).
  - Evaluates every detected episode against configurable **synthetic execution latency horizons** (default `[5, 20, 50, 150]` ms) to measure survival rate, profit retention, and realistic post-latency PnL.
- **Live Implied-Rate Term Structure (`rates`)**:
  - Snapshots ATM median mid-market box-implied financing rates alongside live dated-futures carry ($\frac{1}{T}\ln(F/S)$), annualized 8h perpetual funding, and 30-day DVOL across all inverse and linear expiries.

---

## Mathematical Formulations

### 1. Box Spreads

For strikes $K_1 < K_2$ with width $W = K_2 - K_1$ at time to expiry $T$ (to exact 08:00 UTC settlement):

- **Linear USDC (`BTC_USDC`, `ETH_USDC`, `SOL_USDC`, `XRP_USDC`, …):**
  - Long box: $+C(K_1) - P(K_1) - C(K_2) + P(K_2)$ (1:1 across all 4 legs).
  - Guaranteed expiry payoff for any $S_T$: $W = K_2 - K_1$ USDC.
  - Because Deribit pays 0% deposit interest on USDC equity and `equity = cash + OptionsMtM` locks short-option proceeds against mark liability, linear USDC boxes trade undiscounted ($r_{\text{usdc}} \approx 0\%$).
  - ITM delivery fee weight: exactly $2.0$ legs for every $S_T$.

- **Inverse Coin-Settled (`BTC`, `ETH`) — Coin-Numeraire Ratio Box (`box_long` / `box_short`, 4 legs):**
  - Ratio $m = K_1 / K_2$; long box $+1\cdot C(K_1) - 1\cdot P(K_1) - m\cdot C(K_2) + m\cdot P(K_2)$.
  - Terminal coin payoff for **every** $S_T > 0$:
    $$\left(1 - \frac{K_1}{S_T}\right) - \frac{K_1}{K_2}\left(1 - \frac{K_2}{S_T}\right) = 1 - \frac{K_1}{K_2}\;\text{coin}$$
    Zero spot-price risk in coin; measures the pure coin rate $r_{\text{coin}} \approx 0\%$. ITM delivery weight: $1 + m$.

- **Inverse Coin-Settled (`BTC`, `ETH`) — USD-Numeraire 1:1 Box (`box_inv_usd_long` / `box_inv_usd_short`, 5 legs):**
  - Equal 1:1 option legs $+C(K_1) - P(K_1) - C(K_2) + P(K_2)$ yield $\frac{W}{S_T}$ coin at expiry, which equals **exact $W$ USD** ($\frac{W}{S_T} \cdot S_T = W$).
  - By no-arbitrage against the matched-expiry inverse dated future $F$, its fair coin cost is $\frac{W}{F}$, so its USD value today is:
    $$\text{Box}_{1:1,\;\text{USD}} = \text{Box}_{1:1,\;\text{coin}} \cdot S_0 = \frac{W}{F} \cdot S_0 = W e^{-r_{\text{USD}} T}$$
    yielding the positive USD-implied interest rate $r_{\text{USD}} = -\frac{1}{T}\ln\!\left(\frac{\text{Box}_{1:1,\;\text{coin}}\cdot S_0}{W}\right) \approx \frac{1}{T}\ln(F/S_0)$ (futures carry)!
  - Hedging with **$W$ USD** of the matched-expiry inverse dated future (`ratio = W`) produces exact risk-free coin payoff $\frac{W}{S_T} + W(\frac{1}{F} - \frac{1}{S_T}) = \frac{W}{F}$ coin — requiring only $W = K_2 - K_1$ USD of future notional instead of $K_1 + K_2$ USD for separate `Reversal(K_1) + Conversion(K_2)`.

### 2. Conversions & Reversals (Put-Call Parity vs. Dated Futures)

- **Linear USDC** (dated future book amounts in underlying units, `ratio = 1.0`):
  - Conversion ($-C(K) + P(K) + F$): expiry payoff $K - F_{\text{ask}}$ USDC.
  - Reversal ($+C(K) - P(K) - F$): expiry payoff $F_{\text{bid}} - K$ USDC.
- **Inverse Coin-Settled** (dated future book amounts in USD notional, `ratio = K` USD per 1 coin of options):
  - Conversion ($-1\cdot C(K) + 1\cdot P(K) + \text{Long } K\text{ USD future}$):
    $$\left(\frac{K}{S_T} - 1\right) + K\left(\frac{1}{F_{\text{ask}}} - \frac{1}{S_T}\right) = \frac{K}{F_{\text{ask}}} - 1\;\text{coin}$$
  - Reversal ($+1\cdot C(K) - 1\cdot P(K) + \text{Short } K\text{ USD future}$):
    $$\left(1 - \frac{K}{S_T}\right) + K\left(\frac{1}{S_T} - \frac{1}{F_{\text{bid}}}\right) = 1 - \frac{K}{F_{\text{bid}}}\;\text{coin}$$

### 3. Verticals & Butterflies

- **Monotonicity ($K_1 < K_2$):** $C(K_1) \ge C(K_2)$ and $P(K_2) \ge P(K_1)$ hold on both linear and inverse contracts.
- **Payoff Caps:** Linear call & put spreads are bounded by $e^{-rT}(K_2 - K_1)$. Inverse ratio call spreads ($1\cdot C(K_1) - \frac{K_1}{K_2} C(K_2)$) are strictly bounded by $e^{-r_{\text{coin}} T}(1 - K_1/K_2)$ coin.
- **Butterfly Convexity ($K_1 < K_2 < K_3$):** With $w_1 = \frac{K_3 - K_2}{K_3 - K_1}$ and $w_3 = \frac{K_2 - K_1}{K_3 - K_1}$ ($w_1 + w_3 = 1$, $w_1 K_1 + w_3 K_3 = K_2$), because $1/S_T > 0$ factors out of inverse payoffs, the long butterfly $+w_1 O(K_1) - O(K_2) + w_3 O(K_3)$ has non-negative expiry payoff for all $S_T > 0$ on **both** linear and inverse calls and puts.

---

## Quick Start

### Build

```bash
cargo build --release
```

No Deribit API keys are required — all public market data streams connect directly over unauthenticated WebSocket shards.

### 1. Live Term Structure Snapshot (`rates`)

```bash
./target/release/box-spread-arbitrage rates --markets BTC,ETH,BTC_USDC,SOL_USDC --warmup-secs 6
```

Sample output captured live from Deribit mainnet (notice inverse `BOX_USD` tracks `FUT_CARRY` at `+3.4%–+5.8%` while inverse `BOX_COIN` and linear `BTC_USDC` `BOX_USD` sit near `0%`):

```text
MARKET     TYPE EXPIRY        DTE       SPOT    BOX_USD   BOX_COIN  PAIRS  FUT_CARRY   PERP_ANN   DVOL
----------------------------------------------------------------------------------------------------------
BTC        INV  2026-12-25   30.1   59110.33     +3.65%     -0.45%     74     +4.22%     +0.00%   53.1
BTC        INV  2027-01-29   65.1   59110.33     +3.38%     -0.84%    191     +4.34%     +0.00%   53.1
BTC        INV  2027-03-26  121.1   59110.33     +5.26%     +0.60%     64     +4.61%     +0.00%   53.1
BTC        INV  2027-06-25  212.1   59110.33     +5.03%     -0.05%     64     +5.05%     +0.00%   53.1
BTC        INV  2027-09-24  303.1   59110.33     +5.78%     +0.54%     64     +5.19%     +0.00%   53.1
BTC_USDC   LIN  2026-12-25   30.1   59110.33     +0.70%        n/a     74     +4.17%     +0.02%   53.1
BTC_USDC   LIN  2027-03-26  121.1   59110.33     +0.16%        n/a     64     +4.67%     +0.02%   53.1
ETH        INV  2026-12-25   30.1    1657.74     +2.63%     +0.45%     37     +2.29%     +0.00%   66.1
ETH        INV  2027-03-26  121.1    1657.74     +3.38%     -0.18%     15     +3.57%     +0.00%   66.1
ETH        INV  2027-06-25  212.1    1657.74     +3.82%     -0.21%     15     +4.05%     +0.00%   66.1
ETH        INV  2027-09-24  303.1    1657.74     +4.60%     +0.26%     15     +4.32%     +0.00%   66.1
SOL_USDC   LIN  2026-12-25   30.1      68.66     +4.64%        n/a    113        n/a     +0.04%    n/a
SOL_USDC   LIN  2027-03-26  121.1      68.66     +0.62%        n/a    101        n/a     +0.04%    n/a
```

### 2. Live Event-Driven Scanner (`live`)

```bash
# Pure static arbitrage across inverse & linear markets (r = 0.0 default)
./target/release/box-spread-arbitrage live \
  --markets BTC,ETH,BTC_USDC,SOL_USDC \
  --book-interval raw \
  --min-profit-usd 0.25 \
  --min-edge-bps 0.5 \
  --output-jsonl ./episodes-live.jsonl

# Or model synthetic USDC financing carry against a 4.5% collateral yield benchmark
./target/release/box-spread-arbitrage live \
  --markets BTC_USDC,SOL_USDC \
  --usdc-rate 0.045 \
  --no-parity --no-verticals --no-butterflies
```

### 3. Record + Backtest with `deribit-tick-collector` (`replay`)

Record a tick-by-tick dataset using [`deribit-tick-collector`](https://github.com/PTR-25/deribit-tick-collector):

```bash
deribit-tick-collector collect \
  --markets BTC,ETH,BTC_USDC,SOL_USDC \
  --data-dir ./deribit-data \
  --duration-secs 3600
```

Replay the recorded Hive-partitioned Parquet v2 directory tick-by-tick with episode lifetime tracking and synthetic latency-decay analysis:

```bash
./target/release/box-spread-arbitrage replay ./deribit-data \
  --markets BTC,ETH,BTC_USDC,SOL_USDC \
  --latencies 5,20,50,150 \
  --output-jsonl ./replay-episodes.jsonl
```

---

## Configuration

Copy [`config.example.toml`](config.example.toml) and pass `--config my-config.toml` to any subcommand. CLI flags always override TOML values.

| Section | Key | Default | Description |
|---|---|---|---|
| `[scanner]` | `markets` | `["BTC","ETH","BTC_USDC","ETH_USDC","SOL_USDC","XRP_USDC"]` | Canonical market families (or `["ALL"]`) |
| `[scanner]` | `book_interval` | `"raw"` | Live L2 granularity: `"raw"`, `"100ms"`, `"agg2"` |
| `[scanner]` | `min_profit_usd` | `0.25` | Minimum total post-fee swept L2 profit (USD/USDC) |
| `[scanner]` | `min_edge_bps` | `0.5` | Minimum L1 net edge in basis points of spot index |
| `[scanner]` | `min_tte_minutes` | `15.0` | Skip expiries too close to 08:00 UTC settlement |
| `[scanner]` | `max_quote_age_ms` | `60000` | Cross-leg staleness guard (`0` disables) |
| `[rates]` | `usdc_benchmark_rate` | `0.0` | Annualized USDC discount rate (`0.0` matches Deribit zero-interest margining) |
| `[rates]` | `coin_benchmark_rate` | `0.0` | Annualized coin lending rate for inverse options |
| `[fees]` | `hold_to_expiry` | `true` | Include 1.5 bps ITM-only expiry delivery fees |
| `[fees]` | `option_fee_cap_rate` | `0.125` | Deribit 12.5% premium fee cap |
| `[replay]` | `latency_horizons_ms` | `[5, 20, 50, 150]` | Synthetic execution latencies evaluated per episode |

---

## Architecture

```text
src/
├── main.rs                 # CLI (`live`, `replay`, `rates`)
├── config.rs               # TOML & CLI configuration + validation
├── fees.rs                 # Exact Deribit linear/inverse taker, 12.5% cap & ITM delivery fees
├── book.rs                 # Zero-alloc fast-path multi-leg L2 sweeper + discrete quantization
├── market_state.rs         # Live/replayed chain index, L2 books, spot/DVOL, futures carry, perp funding
├── live.rs                 # Live multi-shard WS scanner (via deribit-tick-collector) & `rates` snapshot
├── replay.rs               # Chronological Parquet v2 replayer, episode tracker & latency-decay simulator
└── strategies/
    ├── mod.rs              # O(N)/O(1) incremental dispatch, scan_all, re_evaluate_opportunity
    ├── box_spread.rs       # Linear 1:1 & inverse ratio (K_low/K_high) boxes + mid implied rate
    ├── parity.rs           # Linear & inverse Conversions / Reversals vs. dated futures
    ├── vertical.rs         # Call/put monotonicity & payoff-cap bounds
    └── butterfly.rs        # Call/put convexity bounds on adjacent strike triples
```

---

## Testing

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

Integration tests verify:
- Zero false positives across fair linear (`SOL_USDC`) and inverse (`BTC`) option + future chains.
- Multi-level L2 sweeping, exact VWAP calculation, and discrete `min_trade_amount` quantization.
- Analytical spot invariance of inverse ratio boxes ($m = K_1/K_2$) across arbitrary $S_T$.
- Inverse conversion/reversal USD-notional future hedging (`fut_ratio = K`).
- Vertical monotonicity/caps and non-uniform butterfly convexity on both linear and inverse contracts.
- End-to-end Parquet v2 write (via `deribit-tick-collector`'s storage engine) -> chronological replay -> exact 35 ms episode lifetime -> exact `[5, 20, 50, 150]` ms latency survival/decay verification.

---

## License

MIT — see [LICENSE](LICENSE).
