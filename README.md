# tick-research

Public market-data capture for the wick (接针) research plan. v0 = raw append log + measurement.
No API keys, read-only public endpoints, no trading.

```bash
uv sync
.venv/bin/python -m tick.collect --top 50 --duration 120   # 2-minute smoke run
.venv/bin/python -m tick.stats                              # density / gaps / latency / storage
.venv/bin/python -m tick.collect --top 259                  # run until Ctrl-C / SIGTERM
```

## What is captured

Coins listed as USDT linear perpetuals on all three venues, ranked by Binance 24h quote volume.

| venue | book | trades | other |
|---|---|---|---|
| Binance USD-M | `@depth@100ms` diff (`/public/stream`) + REST `depth?limit=1000` anchor on connect and every 30 min | `@aggTrade` (`/market/stream`) | `!forceOrder@arr`, `!markPrice@arr@1s` |
| Bybit linear | `orderbook.50` (20ms snapshot/delta) | `publicTrade` | `allLiquidation`, `tickers` |
| OKX SWAP | `books` (400 levels, 100ms) | `trades` | `funding-rate`, `open-interest`, `mark-price`, `liquidation-orders` |

Binance USD-M websocket streams are split by category: depth only flows on `/public/stream`,
aggTrade / forceOrder / markPrice only on `/market/stream`. The legacy `/stream` endpoint accepts
all subscriptions but silently delivers depth only (verified 2026-10-12).

## Raw log format

`data/raw/<venue>/<YYYYMMDD-HH UTC>.jsonl.zst`, append-only, a zstd frame closed every ~1s.

```json
{"k":"ws|rest|evt","c":conn_id,"n":seq,"w":wall_ns,"m":mono_ns, ...,"d":<exchange payload verbatim>}
```

* `w` / `m`: local wall-clock and monotonic receive time. REST lines also carry request start `t0w`/`t0m`.
* `evt`: connect / close / error / handshake_rejected / stop. Gaps are detected offline from these + sequence ids.
* `data/meta/universe-*.json`: the market list and contract multipliers used for the run.

Nothing is parsed, deduplicated or dropped on the write path. Dedup (e.g. the author's
"same market, same 5s bucket, identical row" rule) belongs to a later, versioned packing step.

## Safety

* HTTP 418/429 from REST or a websocket handshake stops the whole collector (shared IP limit).
* Binance REST weight is read from `x-mbx-used-weight-1m`; snapshots back off above 1200.
* Stops when disk free space drops below `--min-free-gb` (default 15).
* Websockets and REST bypass the macOS system proxy on purpose: latency must be from our own vantage point.

## Known limitations of v0

* Laptop vantage point: clock offset to exchanges ~0.6–0.75s and REST RTT ~0.7–1s were measured here,
  so receive-latency numbers are not representative. Production capture needs a server near the
  matching engines with chrony.
* Same-ticker matching only (1000PEPE -> PEPE); same ticker on two venues is not verified to be the same asset.
* No spot markets, no Gate/Bitget/KuCoin/MEXC/Hyperliquid yet.
* No book reconstruction yet; `stats.py` only checks sequence continuity.
