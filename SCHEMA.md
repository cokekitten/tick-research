# Normalized schema (v1)

`python -m tick.normalize` turns the raw append log into Parquet, one file per table x venue-market
x UTC hour: `data/norm/<table>/<venue>-<market>/<YYYYMMDD-HH>.parquet`.
The raw log is never modified; normalization is re-runnable and versioned (`schema_version` in the
Parquet metadata).

## Coverage

All stablecoin-quoted (USDT / USDC / FDUSD / USD1) linear perpetuals and spot markets on Binance,
Bybit and OKX — no intersection filter. Instrument lists are refreshed hourly; new listings get a
connection automatically. 2026-10-12: 787 + 825 + 858 + 476 + 485 + 717 = 4,148 markets
(including ~690 tradfi perps/spot: stocks, ETFs, commodities, FX).

## Units (the same on every venue)

| field | meaning |
|---|---|
| `px` | quote currency per **1 coin**. Binance/Bybit `1000PEPE` prices are divided by 1000. |
| `qty` | **coins**. OKX SWAP contracts x `ctVal`; `1000`-prefixed sizes x 1000. |
| `recv_ns` | local wall-clock receive time (ns). |
| `avail_ns` | earliest local time the row could be known (book only). **Use this, not recv_ns, for causal features.** |
| `ex_ms` | exchange event time (ms) as published; null where the venue publishes none (Binance spot bookTicker). |
| side / `trade.side` | `1` = bid / taker buy, `-1` = ask / taker sell. |
| `liq.pos_side` | side of the **liquidated position**: `1` = a long was closed, `-1` = a short. |

Cross-venue joins use `instruments.coin_id`, never the raw ticker: tickers collide
(Binance `CATUSDT` is Caterpillar stock, `1000CATUSDT` the CAT memecoin; OKX `QNT-USDT-SWAP` is a stock
perp while every other venue's `QNT` is Quant). `coin_id = base` for crypto, `base@tradfi` otherwise.
Coins with the same ticker on two venues are still assumed to be the same asset (not verified).

## Tables

**book** — L2 level updates. Rows sharing `(sym, chain, upd)` form one book event.
`kind`: 0 delta, 1 websocket snapshot (replaces the book), 2 REST snapshot (Binance anchor).
`qty == 0` deletes the level. `chain` changes on every resync; never carry a book across chains.

| venue | depth | cadence | continuity rule |
|---|---|---|---|
| Binance perp | full-book diffs, anchored on a 500-level REST snapshot | 100ms | `pu == previous u` |
| Binance spot | full-book diffs, anchored on a 500-level REST snapshot | 100ms | `U == previous u + 1` |
| Bybit perp / spot | 200 levels | 100ms (spot 100–400ms on quiet books) | `u == previous u + 1` |
| OKX perp / spot | 400 levels | 100ms | `prevSeqId == previous seqId` |

Binance diffs are buffered until the REST snapshot arrives, then replayed; replayed diffs get
`avail_ns` = snapshot receive time. Rows before the first anchor are dropped.
Levels beyond the venue's depth window can be stale: compare depth across venues within the top
200 levels only.

**bbo** — native best bid/ask feed: Binance `bookTicker` (tick-by-tick), Bybit `orderbook.1`
(10–20ms), OKX `bbo-tbt` (tick-by-tick). `upd` is the venue's book update id, so a bbo row can be
matched to the book event that produced it (Binance `u`, Bybit `seq`, OKX `seqId`).

**trade** — individual fills on all venues (Binance `@trade`, Bybit `publicTrade`, OKX `trades-all`).
`flag`: Binance `X` (`MARKET`, `RPI`, `NA` = not a regular trade), Bybit `RPI`/`BT` (block trade),
OKX `src<n>` for non-default sources. Filter `flag` before counting trades.

**liq** — liquidations.

| venue | feed (`feed`) | completeness | `px_type` |
|---|---|---|---|
| Binance perp | `!forceOrder@arr` (`throttled`) | at most 1 order per symbol per second: **undercounts** | `avg_fill` |
| Bybit perp | `allLiquidation` (`all_500ms`) | all, batched every 500ms | `bankruptcy` |
| OKX perp | `liquidation-orders` (`okx_swap`) | all SWAP | `bankruptcy` |

Bankruptcy price is not an execution price. Binance liquidation intensity is not comparable to the
other two venues in absolute terms; use it as a presence / direction signal.

**deriv** — long format `(sym, field, value, ref_ms)`.

| field | Binance perp | Bybit perp | OKX perp |
|---|---|---|---|
| `mark` | markPrice 1s | tickers | mark-price |
| `index` | markPrice 1s | tickers | index-tickers (mapped to every SWAP on that index) |
| `funding_rate` (ref = next funding time) | markPrice | tickers | funding-rate |
| `funding_rate_next` / `funding_rate_settled` | — | — | funding-rate |
| `oi_coin` | REST poll every ~2 min | tickers | open-interest (`oiCcy`) |

**instruments** — the instrument records parsed from the lists logged in the same run, with units
(`mult`, `ct_val`), `coin_id`, `asset`, tick/lot, listing time. Use the latest row at or before the
data's `recv_ns`.

**conn** — collector events (connect / close / error / gap / registry) and `book_gap` from replay.
Use it to build data-validity masks: no feature may bridge a gap or a reconnect.

## Verified on capture (2026-10-12, 3 minutes, all 4,148 markets)

* Rebuilt best bid/ask from `book` equals the native `bbo` feed at the same update id:
  37,678 / 37,678 events across all six venue-markets (30 busiest symbols each).
* No sequence gaps; every Binance REST snapshot fell inside its diff stream and anchored.
* 379 coin_ids on >= 4 venue-markets: normalized mids agree within 5% except two thin spot books.
* OKX `books` sends `checksum: 0` on every message at present, so it cannot be used for validation.
