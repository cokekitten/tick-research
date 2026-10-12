"""v0 collector: public market data for USDT linear perps on Binance / Bybit / OKX.

Every received message is written verbatim (raw append log) with local wall + monotonic
receive time and connection id. Nothing is parsed or dropped on the write path.

    python -m tick.collect --top 50 --duration 120

Read-only public endpoints; no API keys. Stops the whole process on HTTP 418/429.
"""
import argparse
import asyncio
import json
import os
import random
import shutil
import signal
import time

import websockets

from .common import RateLimited, RawWriter, as_json_text, http_get
from .universe import build_universe


class Ctx:
    def __init__(self, root: str, min_free_gb: float):
        self.root = root
        self.min_free = min_free_gb * 1e9
        self.stop = asyncio.Event()
        self.fatal: str | None = None
        self._conn = 0
        self.writers: dict[str, RawWriter] = {}
        self.msgs: dict[str, int] = {}

    def conn_id(self) -> int:
        self._conn += 1
        return self._conn

    def writer(self, ex: str) -> RawWriter:
        if ex not in self.writers:
            self.writers[ex] = RawWriter(self.root, ex)
            self.msgs[ex] = 0
        return self.writers[ex]

    def halt(self, why: str):
        if not self.fatal:
            self.fatal = why
        self.stop.set()


async def sleep_or_stop(ctx: Ctx, secs: float):
    try:
        await asyncio.wait_for(ctx.stop.wait(), timeout=secs)
    except asyncio.TimeoutError:
        pass


async def ws_loop(ctx: Ctx, ex: str, name: str, url: str, on_open, app_ping=None, ping_every=20.0,
                  start_delay=0.0):
    """Keep one websocket alive forever; record every message; reconnect with backoff."""
    w = ctx.writer(ex)
    backoff = 1.0
    await sleep_or_stop(ctx, start_delay)  # stagger handshakes: per-IP connection rate limits
    while not ctx.stop.is_set():
        conn = ctx.conn_id()
        try:
            async with websockets.connect(url, proxy=None, max_size=64 << 20, open_timeout=15,
                                          ping_interval=20, ping_timeout=30, close_timeout=5) as ws:
                w.event(conn, {"type": "connect", "name": name, "url": url})
                await on_open(ws, conn)
                backoff = 1.0
                pinger = asyncio.create_task(_pinger(ws, app_ping, ping_every)) if app_ping else None
                try:
                    async for msg in ws:
                        w.write("ws", conn, time.time_ns(), time.monotonic_ns(), as_json_text(msg))
                        ctx.msgs[ex] += 1
                        if ctx.stop.is_set():
                            break
                finally:
                    if pinger:
                        pinger.cancel()
                w.event(conn, {"type": "close", "name": name, "code": ws.close_code, "reason": ws.close_reason})
        except RateLimited as e:
            ctx.halt(str(e))
        except websockets.exceptions.InvalidStatus as e:
            code = e.response.status_code
            w.event(conn, {"type": "handshake_rejected", "name": name, "status": code})
            if code in (418, 429):
                ctx.halt(f"{ex} websocket handshake HTTP {code}")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - recorded, then reconnect
            w.event(conn, {"type": "error", "name": name, "err": f"{type(e).__name__}: {e}"[:500]})
        if ctx.stop.is_set():
            break
        await sleep_or_stop(ctx, backoff + random.random())
        backoff = min(backoff * 2, 30.0)


async def _pinger(ws, payload: str, every: float):
    while True:
        await asyncio.sleep(every)
        await ws.send(payload)


async def rest_record(ctx: Ctx, ex: str, url: str, extra: dict):
    """GET url off-loop, write verbatim body with request/response timestamps."""
    status, body, (t0w, t0m, t1w, t1m), hdr = await asyncio.to_thread(http_get, url, 15.0)
    ex_extra = {**extra, "status": status, "t0w": t0w, "t0m": t0m}
    if "x-mbx-used-weight-1m" in hdr:
        ex_extra["wt"] = int(hdr["x-mbx-used-weight-1m"])
    ctx.writer(ex).write("rest", 0, t1w, t1m, as_json_text(body), ex_extra)
    return ex_extra


# ---------------------------------------------------------------- Binance USD-M
class Binance:
    # USD-M streams are split by category (verified 2026-10-12): depth only flows on /public,
    # aggTrade / forceOrder / markPrice only on /market. The legacy /stream endpoint accepts
    # every subscription but silently delivers depth only.
    WS_PUBLIC = "wss://fstream.binance.com/public/stream?streams="
    WS_MARKET = "wss://fstream.binance.com/market/stream?streams="
    REST = "https://fapi.binance.com"

    def __init__(self, ctx: Ctx, uni: list, shard: int, resnap_min: float):
        self.ctx, self.shard, self.resnap = ctx, shard, resnap_min * 60
        self.syms = [u["binance"]["sym"] for u in uni]
        self.snapq: asyncio.Queue = asyncio.Queue()

    def tasks(self):
        out = []
        for i in range(0, len(self.syms), self.shard):
            part = self.syms[i:i + self.shard]
            depth = [f"{s.lower()}@depth@100ms" for s in part]
            trades = [f"{s.lower()}@aggTrade" for s in part]

            async def on_open(ws, conn, part=part):
                for s in part:  # anchor snapshots requested after the diff stream is live
                    self.snapq.put_nowait((s, conn, "connect"))

            async def noop(ws, conn):
                return None

            k = i // self.shard
            out.append(ws_loop(self.ctx, "binance", f"binance-depth-{k}", self.WS_PUBLIC + "/".join(depth), on_open,
                               start_delay=k * 1.0))
            out.append(ws_loop(self.ctx, "binance", f"binance-trade-{k}", self.WS_MARKET + "/".join(trades), noop,
                               start_delay=k * 1.0 + 0.5))
        out.append(ws_loop(self.ctx, "binance", "binance-global",
                           self.WS_MARKET + "!forceOrder@arr/!markPrice@arr@1s", lambda ws, conn: asyncio.sleep(0)))
        out += [self._snapshots(), self._resnap_cycle()]
        return out

    async def _snapshots(self):
        """Depth snapshots, one per 1.5s (weight 20 each -> ~800/min of the 2400/min IP budget)."""
        while not self.ctx.stop.is_set():
            try:
                sym, conn, why = await asyncio.wait_for(self.snapq.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                info = await rest_record(self.ctx, "binance", f"{self.REST}/fapi/v1/depth?symbol={sym}&limit=1000",
                                         {"ep": "depth", "sym": sym, "for_conn": conn, "why": why})
                if info.get("wt", 0) > 1200:
                    self.ctx.writer("binance").event(0, {"type": "weight_backoff", "wt": info["wt"]})
                    await sleep_or_stop(self.ctx, 30)
            except RateLimited as e:
                self.ctx.writer("binance").event(0, {"type": "rate_limited", "err": str(e)})
                self.ctx.halt(str(e))
                return
            except Exception as e:  # noqa: BLE001
                self.ctx.writer("binance").event(0, {"type": "snapshot_error", "sym": sym, "err": str(e)[:300]})
            await sleep_or_stop(self.ctx, 1.5)

    async def _resnap_cycle(self):
        while not self.ctx.stop.is_set():
            await sleep_or_stop(self.ctx, self.resnap)
            for s in self.syms:
                self.snapq.put_nowait((s, 0, "periodic"))


# ---------------------------------------------------------------- Bybit v5 linear
class Bybit:
    WS = "wss://stream.bybit.com/v5/public/linear"

    def __init__(self, ctx: Ctx, uni: list, shard: int, depth: int):
        self.ctx, self.shard, self.depth = ctx, shard, depth
        self.syms = [u["bybit"]["sym"] for u in uni]

    def tasks(self):
        out = []
        for i in range(0, len(self.syms), self.shard):
            part = self.syms[i:i + self.shard]
            topics = []
            for s in part:
                topics += [f"orderbook.{self.depth}.{s}", f"publicTrade.{s}", f"allLiquidation.{s}", f"tickers.{s}"]

            async def on_open(ws, conn, topics=topics):
                for j in range(0, len(topics), 10):
                    await ws.send(json.dumps({"op": "subscribe", "args": topics[j:j + 10]}))

            out.append(ws_loop(self.ctx, "bybit", f"bybit-{i // self.shard}", self.WS, on_open,
                               app_ping='{"op":"ping"}', ping_every=20, start_delay=(i // self.shard) * 0.5))
        return out


# ---------------------------------------------------------------- OKX v5 public
class Okx:
    WS = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(self, ctx: Ctx, uni: list, shard: int):
        self.ctx, self.shard = ctx, shard
        self.insts = [u["okx"]["instId"] for u in uni]

    def tasks(self):
        out = []
        for i in range(0, len(self.insts), self.shard):
            part = self.insts[i:i + self.shard]
            args = []
            for inst in part:
                args += [{"channel": c, "instId": inst} for c in ("books", "trades", "funding-rate", "open-interest", "mark-price")]
            if i == 0:
                args.append({"channel": "liquidation-orders", "instType": "SWAP"})

            async def on_open(ws, conn, args=args):
                for j in range(0, len(args), 20):
                    await ws.send(json.dumps({"op": "subscribe", "args": args[j:j + 20]}))

            out.append(ws_loop(self.ctx, "okx", f"okx-{i // self.shard}", self.WS, on_open,
                               app_ping="ping", ping_every=20, start_delay=(i // self.shard) * 0.5))
        return out


# ---------------------------------------------------------------- housekeeping
TIME_URLS = {
    "binance": "https://fapi.binance.com/fapi/v1/time",
    "bybit": "https://api.bybit.com/v5/market/time",
    "okx": "https://www.okx.com/api/v5/public/time",
}


async def clock_probe(ctx: Ctx, exchanges: list, every: float = 60.0):
    """Server-time probes to estimate exchange-vs-local clock offset over time."""
    while not ctx.stop.is_set():
        for ex in exchanges:
            try:
                await rest_record(ctx, ex, TIME_URLS[ex], {"ep": "time"})
            except RateLimited as e:
                ctx.halt(str(e))
                return
            except Exception as e:  # noqa: BLE001
                ctx.writer(ex).event(0, {"type": "time_error", "err": str(e)[:300]})
        await sleep_or_stop(ctx, every)


async def guards(ctx: Ctx, duration: float):
    t0 = time.monotonic()
    last = dict(ctx.msgs)
    while not ctx.stop.is_set():
        await sleep_or_stop(ctx, 10)
        free = shutil.disk_usage(ctx.root).free
        if free < ctx.min_free:
            ctx.halt(f"disk free {free / 1e9:.1f}GB < {ctx.min_free / 1e9:.0f}GB")
        el = time.monotonic() - t0
        rates = " ".join(f"{ex}={ctx.msgs[ex]}(+{(ctx.msgs[ex] - last.get(ex, 0)) / 10:.0f}/s)" for ex in ctx.msgs)
        raw = sum(w.raw_bytes for w in ctx.writers.values()) / 1e6
        print(f"[{el:7.0f}s] {rates} raw={raw:.1f}MB free={free / 1e9:.0f}GB", flush=True)
        last = dict(ctx.msgs)
        if duration and el >= duration:
            ctx.stop.set()


async def amain(a):
    os.makedirs(os.path.join(a.root, "meta"), exist_ok=True)
    ctx = Ctx(a.root, a.min_free_gb)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, ctx.stop.set)

    uni, counts = await asyncio.to_thread(build_universe, a.top)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    with open(os.path.join(a.root, "meta", f"universe-{stamp}.json"), "w") as f:
        json.dump({"ts": stamp, "counts": counts, "top": a.top, "bybit_depth": a.bybit_depth, "universe": uni}, f, indent=1)
    print(f"universe: {counts} -> top {len(uni)}: {' '.join(u['base'] for u in uni[:12])} ...", flush=True)

    exchanges = [e.strip() for e in a.exchanges.split(",") if e.strip()]
    tasks = []
    if "binance" in exchanges:
        tasks += Binance(ctx, uni, a.shard, a.resnap_min).tasks()
    if "bybit" in exchanges:
        tasks += Bybit(ctx, uni, a.shard, a.bybit_depth).tasks()
    if "okx" in exchanges:
        tasks += Okx(ctx, uni, a.shard).tasks()
    tasks += [clock_probe(ctx, exchanges), guards(ctx, a.duration)]

    running = [asyncio.create_task(t) for t in tasks]
    await ctx.stop.wait()
    await asyncio.sleep(0.5)  # let in-flight writes land
    for t in running:
        t.cancel()
    await asyncio.gather(*running, return_exceptions=True)
    for ex, w in ctx.writers.items():
        w.event(0, {"type": "stop", "fatal": ctx.fatal})
        w.close()
    print(f"stopped. fatal={ctx.fatal} msgs={ctx.msgs}", flush=True)
    return 2 if ctx.fatal else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--top", type=int, default=50, help="coins listed on all three, ranked by Binance 24h quote volume")
    ap.add_argument("--duration", type=float, default=0, help="seconds; 0 = run until SIGINT/SIGTERM")
    ap.add_argument("--root", default="data")
    ap.add_argument("--exchanges", default="binance,bybit,okx")
    ap.add_argument("--shard", type=int, default=25, help="symbols per websocket connection")
    ap.add_argument("--bybit-depth", type=int, default=50, choices=[1, 50, 200, 1000])
    ap.add_argument("--resnap-min", type=float, default=30, help="Binance periodic REST snapshot interval")
    ap.add_argument("--min-free-gb", type=float, default=15, help="stop when disk free space drops below")
    raise SystemExit(asyncio.run(amain(ap.parse_args())))


if __name__ == "__main__":
    main()
