# tick-research

Public market-data capture for the wick (接针) research plan. v0 = raw append log + measurement.
No API keys, read-only public endpoints, no trading.

```bash
uv sync
.venv/bin/python -m tick.run --duration 180      # all six venue-markets, 3 minutes
.venv/bin/python -m tick.normalize               # raw log -> data/norm/*.parquet (see SCHEMA.md)
.venv/bin/python -m tick.verify                  # book rebuild vs BBO feed, cross-venue units
.venv/bin/python -m tick.run                     # run until Ctrl-C / SIGTERM
```

## What is captured

Every USDT-quoted spot market and USDT-margined linear perpetual on Binance, Bybit and OKX (3,336
markets on 2026-10-12), one collector process per venue-market. Per market: L2 book (>=200 levels, 100ms),
native BBO (tick-by-tick or 10-20ms), individual trades; perps also liquidations, mark, index,
funding and open interest. Units, ids and per-venue differences are documented in `SCHEMA.md`.

Measured on a laptop, all 3,336 USDT markets: ~22k messages/s, ~67% of one CPU core across six
processes, event-loop lag <=1ms, **~117 GB/day zstd** raw log (book ~1/3, BBO ~1/2 of bytes).

Binance USD-M websocket streams are split by category: depth, bookTicker and trade only flow on
`/public/stream`; markPrice and forceOrder only on `/market/stream`. The legacy `/stream` endpoint
accepts every subscription but silently delivers depth only (verified 2026-10-12).

## Raw log format

`data/raw/<venue>-<market>/<YYYYMMDD-HH UTC>.jsonl.zst`, append-only, a zstd frame closed every ~1s.

```json
{"k":"ws|rest|evt","c":conn_id,"n":seq,"w":wall_ns,"m":mono_ns, ...,"d":<exchange payload verbatim>}
```

* `w` / `m`: local wall-clock and monotonic receive time. REST lines also carry request start `t0w`/`t0m`.
* `evt`: connect / close / error / handshake_rejected / stop. Gaps are detected offline from these + sequence ids.
* Instrument lists are logged as REST bodies (`\"ep\":\"instruments\"`) at start and every hour; `data/meta/instruments-*.json` is a readable copy.

Nothing is parsed, deduplicated or dropped on the write path. Dedup (e.g. the author's
"same market, same 5s bucket, identical row" rule) belongs to a later, versioned packing step.

## Safety

* A rate-limit answer (HTTP 418/429, Bybit 403 / retCode 10006, OKX 50011) from REST or a websocket
  handshake writes `data/meta/STOP-<venue>`; every collector of that venue stops and refuses to
  start until the file is removed by hand.
* Binance REST weight is read from `x-mbx-used-weight-1m`; snapshot pacing slows down above a threshold.
* Websocket handshakes are staggered per venue; OKX perp and spot share its per-IP connection rate.
* Stops when disk free space drops below `--min-free-gb` (default 15).
* Websockets and REST bypass the macOS system proxy on purpose: latency must be from our own vantage point.

## Known limitations

* Laptop vantage point: clock offset to exchanges ~0.6–0.75s and REST RTT ~0.7–1s were measured here,
  so receive-latency numbers are not representative. Production capture needs a server near the
  matching engines with chrony.
* Same ticker on two venues is assumed to be the same asset (crypto vs tradfi collisions are separated via `coin_id`).
* No Gate/Bitget/KuCoin/MEXC/Hyperliquid yet.
* Binance liquidations are throttled by the exchange (at most one per symbol per second).

