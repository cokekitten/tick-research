# Normalized schema (v2)

`python -m tick.normalize` turns the raw segments into Parquet:
`data/norm/v2/<table>/<venue>-<market>/<YYYYMMDD-HH>-<part>.parquet` (UTC hour; `part` increments
if an hour is written in more than one go). The raw log is never modified; normalization is
re-runnable, rebuilds a venue-market from all its raw segments, and is versioned (`schema_version`
in the Parquet metadata and in the path).

## Raw segments

`data/raw/<venue>-<market>/<run_id>-<seg>.jsonl.zst`. Every collector start is a new `run_id`
(`<UTC start>-<pid>`); segments roll every 15 minutes or 2 GiB and are never reopened. The open
segment is `.partial` and is renamed when closed cleanly. A `.partial` left behind means the writer
was killed: it is readable up to its last complete frame and is only read with `--include-partial`,
which also records a `segment_unsealed` event. A damaged tail is reported as `segment_damaged` and
never affects other segments. Book state is keyed by `(run_id, connection, symbol)`, so a later run
never continues an earlier run's book.

## Coverage

All USDT-quoted spot markets and USDT-margined linear perpetuals on Binance, Bybit and OKX — no
intersection filter. Other quote currencies (USDC, FDUSD, USD1, fiat) are not captured; widen
`registry.STABLES` to add them. Instrument lists are refreshed hourly; new listings get a connection
automatically. 2026-10-12: perps 746 + 790 + 485, spot 506 + 390 + 419 = 3,336 markets
(including ~810 tradfi perps/spot: stocks, ETFs, commodities, FX).

## Units (the same on every venue)

| field | meaning |
|---|---|
| `px` | quote currency per **1 coin**. Binance/Bybit `1000PEPE` prices are divided by 1000. |
| `qty` | **coins**. OKX SWAP contracts x `ctVal`; `1000`-prefixed sizes x 1000. |
| `px_raw`, `qty_raw` | the exchange's own decimal strings, unconverted (book and trade). Use them for exact audit; the float columns are for features. |
| `recv_ns` | local wall-clock receive time (ns). |
| `avail_ns` | earliest local time the row could be known (book only). **Use this, not recv_ns, for causal features.** |
| `ex_ms` | exchange event time (ms) as published; null where the venue publishes none (Binance spot bookTicker). |
| side / `trade.side` | `1` = bid / taker buy, `-1` = ask / taker sell. |
| `liq.pos_side` | side of the **liquidated position**: `1` = a long was closed, `-1` = a short. |

Rows for an instrument without logged metadata are never unit-converted: they go to the
`quarantine` table with the reason and a pointer to the raw line (`<segment>#<n>`). Venue-wide feeds
(OKX liquidations, Binance mark price) also carry out-of-scope contracts such as inverse swaps; those
are skipped or quarantined as `out_of_scope`.

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

Binance: each REST snapshot is requested for one connection (`for_conn`) and only anchors that
connection's diff stream. It works whichever arrives first (snapshot before the first diff on quiet
markets, or diffs buffered until the snapshot). Replayed diffs get `avail_ns` = snapshot receive
time. Rows before the first anchor are dropped; after a gap the chain stays closed until the next
snapshot for that connection (the collector requests one immediately).
Levels beyond the venue's depth window can be stale: compare depth across venues within the top
200 levels only.

**bbo** — native best bid/ask feed: Binance `bookTicker` (tick-by-tick), Bybit `orderbook.1`
(10–20ms), OKX `bbo-tbt` (tick-by-tick). `upd` is the venue's book update id, so a bbo row can be
matched to the book event that produced it (Binance `u`, Bybit `seq`, OKX `seqId`).

**trade** — individual fills on all venues (Binance `@trade`, Bybit `publicTrade`, OKX `trades-all`).
`flag`: Binance `X` (`MARKET`, `RPI`, `NA` = not a regular trade), Bybit `RPI`/`BT` (block trade),
OKX `src<n>` for non-default sources. Filter `flag` before counting trades.

**price_limit** — dynamic order price limits: `buy_max` (highest allowed buy price), `sell_min`
(lowest allowed sell price), `enabled` (OKX only; null on Bybit, which does not publish it).
`enabled=false` comes with null limits. Bybit `priceLimit` and OKX `price-limit` on spot and perps;
Binance publishes no such feed. A price inside the limits is not a guarantee of execution.

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

**conn** — collector events (connect / close / error / gap / sub_rejected / task_error /
registry / stop) plus `book_gap`, `segment_damaged` and `segment_unsealed` from replay, keyed by
`run_id` + `conn`. Use it to build data-validity masks: no feature may bridge a gap, a reconnect or
a run boundary.

**quarantine** — rows that were not normalized: `no_metadata`, `out_of_scope`, or `parse:<error>`,
with `source = <segment>#<line n>` pointing at the raw record.

## Verified on capture (2026-10-12, 120 seconds, all 3,336 USDT markets)

* Rebuilt best bid/ask **price and size** from `book` equals the native `bbo` feed at the same
  update id: 26,963 / 26,963 events across all six venue-markets (30 busiest symbols each).
* No sequence gaps; 363 Binance anchors, none missed.
* 327 coin_ids on >= 4 venue-markets: normalized mids agree within 5% except one thin spot book.
* `tick.verify` exits 0 only if every check passes, 1 on any failure or too few samples, 2 if there
  is no data. A short capture can PASS; that says nothing about the long-run coverage.
* OKX `books` sends `checksum: 0` on every message at present, so it cannot be used for validation.

## Not covered (on purpose, for now)

* Depth views (cumulative depth by bps / VWAP by notional with coverage masks) belong to the
  feature step, not the archive.
* Per-channel health intervals, a central rate-limit scheduler, manifests with content hashes and
  resumable checkpoints. The collector logs every connect / close / gap / reject event, which is
  enough to derive validity masks offline.
* RPI books, Bybit 20ms/1000-level books, more venues.
