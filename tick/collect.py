"""v1 collector: one process per venue x market (use `python -m tick.run` to launch all six).

    python -m tick.collect --venue binance --market perp --duration 120

Captures every stablecoin-quoted (USDT/USDC/FDUSD/USD1) linear perpetual or spot market of the venue:
  book    L2 >=200 levels at ~100ms, kept continuous: a sequence gap triggers an immediate resync
  bbo     best bid/ask, tick-by-tick or 10-20ms (Binance bookTicker / Bybit orderbook.1 / OKX bbo-tbt)
  trade   individual fills (Binance @trade / Bybit publicTrade / OKX trades-all)
  limit   dynamic price limits (Bybit priceLimit, OKX price-limit; Binance has no public feed)
  perp    liquidations, mark / index / funding / open interest
Every message is logged verbatim with local wall + monotonic receive time (see tick.common.RawWriter).
Instrument lists are re-fetched hourly; new listings get their own connection.

Read-only public endpoints, no API keys. A rate-limit response writes data/meta/STOP-<venue> and stops
the process; every collector of that venue stops when it sees the file, and refuses to start while it
exists (delete it by hand after checking).
"""
import argparse
import asyncio
import concurrent.futures
import itertools
import os
import random
import shutil
import signal
import time
from dataclasses import dataclass, field

import orjson
import websockets
import websockets.exceptions

from .common import RateLimited, RawWriter, as_json_text, http_get, new_run_id
from .wsraw import RawWsConn
from .registry import INSTRUMENT_URLS, PARSERS, fetch_volumes

TIME_URLS = {
    ("binance", "perp"): "https://fapi.binance.com/fapi/v1/time",
    ("binance", "spot"): "https://api.binance.com/api/v3/time",
    ("bybit", "perp"): "https://api.bybit.com/v5/market/time",
    ("bybit", "spot"): "https://api.bybit.com/v5/market/time",
    ("okx", "perp"): "https://www.okx.com/api/v5/public/time",
    ("okx", "spot"): "https://www.okx.com/api/v5/public/time",
}


def stop_files(root, venue):
    return [os.path.join(root, "meta", "STOP-all"), os.path.join(root, "meta", f"STOP-{venue}")]


class Ctx:
    def __init__(self, a):
        self.root, self.venue, self.market = a.root, a.venue, a.market
        self.stream = a.venue + "-" + a.market + ("-" + a.tag if a.tag else "")
        self.raw_ws = getattr(a, "raw_ws", False)
        self.min_free = a.min_free_gb * 1e9
        self.stop = asyncio.Event()
        self.fatal: str | None = None
        self._conn = itertools.count(1)
        self.run_id = new_run_id()
        self.w = RawWriter(a.root, self.stream, self.run_id)
        self.msgs = 0
        self.rest_sem = asyncio.Semaphore(8)
        self.bg: set = set()
        self.crash: str | None = None

    def spawn(self, coro, name: str):
        """Fire-and-forget task whose failure is logged instead of vanishing."""
        t = asyncio.create_task(coro, name=name)
        self.bg.add(t)
        t.add_done_callback(self._bg_done)
        return t

    def _bg_done(self, t: asyncio.Task):
        self.bg.discard(t)
        if t.cancelled():
            return
        e = t.exception()
        if e is not None:
            self.w.event(0, {"type": "task_error", "task": t.get_name(), "err": repr(e)[:300]})

    def conn_id(self) -> int:
        return next(self._conn)

    def halt(self, why: str):
        if not self.fatal:
            self.fatal = why
            self.w.event(0, {"type": "fatal", "why": why})
        self.stop.set()

    def rate_limited(self, why: str):
        path = stop_files(self.root, self.venue)[1]
        with open(path, "a") as f:
            f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {self.venue}-{self.market}: {why}\n")
        self.halt(f"rate limited: {why}")


async def sleep_or_stop(ctx: Ctx, secs: float):
    if secs <= 0:
        return
    try:
        await asyncio.wait_for(ctx.stop.wait(), timeout=secs)
    except asyncio.TimeoutError:
        pass


async def rest_record(ctx: Ctx, url: str, extra: dict, timeout: float = 15.0):
    """GET url off-loop and log the verbatim body with request/response timestamps."""
    async with ctx.rest_sem:
        status, body, (t0w, t0m, t1w, t1m), hdr = await asyncio.to_thread(http_get, url, timeout)
    ex = {**extra, "status": status, "t0w": t0w, "t0m": t0m}
    if "x-mbx-used-weight-1m" in hdr:
        ex["wt"] = int(hdr["x-mbx-used-weight-1m"])
    ctx.w.write("rest", 0, t1w, t1m, as_json_text(body), ex)
    return ex, body


# ---------------------------------------------------------------- websocket plumbing
@dataclass
class Spec:
    name: str
    url: str
    topics: list                      # what this connection carries, for the connect event
    subscribe: list = field(default_factory=list)   # messages sent after connect
    app_ping: str | None = None
    check: object = None              # check(state, ws, msg) for live sequence checks
    on_open: object = None            # on_open(conn) -> None


class ConnState:
    def __init__(self, ctx: Ctx, conn: int, spec: Spec):
        self.ctx, self.conn, self.spec = ctx, conn, spec
        self.last: dict = {}
        self.last_resync: dict = {}
        self.resyncs = 0
        self.tasks: set = set()

    def allow_resync(self, key, min_gap=5.0) -> bool:
        now = time.monotonic()
        if now - self.last_resync.get(key, -1e9) < min_gap:
            return False
        self.last_resync[key] = now
        self.resyncs += 1
        return True

    def send_later(self, ws, payloads):
        async def go():
            for p in payloads:
                await ws.send(p)
        t = asyncio.create_task(go())
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)


def _is_reject(msg) -> bool:
    """Bybit {"success":false}, OKX {"event":"error"}, Binance {"error":...} on control messages."""
    if isinstance(msg, bytes):
        msg = msg.decode("utf-8", "replace")
    return '"success":false' in msg or '"event":"error"' in msg or msg.startswith('{"error"')


def _is_rate_reject(msg) -> bool:
    if isinstance(msg, bytes):
        msg = msg.decode("utf-8", "replace")
    low = msg.lower()
    return "too many" in low or '"code":"60014"' in msg or '"code":"63999"' in msg or "rate limit" in low


# WebSocket handshakes run here, not in the default executor: REST polling can occupy every
# default worker (rest_sem allows 8 concurrent calls, the default pool is cpu+4 = 8 on a 4-core
# box), which left reconnects queued behind HTTP for tens of seconds.
_HS_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=16, thread_name_prefix="wshs")


async def _raw_ws_session(ctx: Ctx, spec: Spec, st: "ConnState", conn: int, check_errors_box: list):
    """Same session as ws_loop's body, but the socket is ours and every message carries the kernel's
    packet arrival time (field `kw`), which stays accurate even when the process is starved of CPU."""
    w = ctx.w
    scheme, rest = spec.url.split("://", 1)
    host_path = rest.split("/", 1)
    host = host_path[0]
    path = "/" + (host_path[1] if len(host_path) > 1 else "")
    host, _, port_s = host.partition(":")
    port = int(port_s) if port_s else (443 if scheme == "wss" else 80)
    ws = RawWsConn(asyncio.get_running_loop(), host, path, port)
    await asyncio.get_running_loop().run_in_executor(_HS_POOL, ws.start)
    try:
        w.event(conn, {"type": "connect", "name": spec.name, "url": spec.url[:300], "topics": spec.topics,
                       "transport": "raw+kernel_ts"})
        for m in spec.subscribe:
            await ws.send(m)
        if spec.on_open:
            spec.on_open(conn)
        pinger = asyncio.create_task(_pinger(ws, spec.app_ping, 20)) if spec.app_ping else None
        last_yield = time.monotonic()
        try:
            async for payload, kts, dts in ws.drain():
                if payload.startswith(b"WARN: backlog"):
                    w.event(conn, {"type": "rawws_backlog", "name": spec.name, "detail": payload.decode()})
                    continue
                now = time.monotonic_ns()
                extra = {"kw": kts} if kts else None
                w.write("ws", conn, kts or dts, now, payload, extra)   # bytes：零解码零重编码
                ctx.msgs += 1
                if len(payload) < 600:          # 拒绝/控制消息都很短，长消息无需解码
                    text = payload.decode("utf-8", "replace")
                    if _is_reject(text):
                        w.event(conn, {"type": "sub_rejected", "name": spec.name, "msg": text[:400]})
                        if _is_rate_reject(text):
                            ctx.rate_limited(f"websocket reject: {text[:200]}")
                if spec.check:
                    try:
                        spec.check(st, ws, payload)
                    except Exception as e:  # noqa: BLE001
                        check_errors_box[0] += 1
                        if check_errors_box[0] <= 5:
                            w.event(conn, {"type": "check_error", "name": spec.name, "err": repr(e)[:300]})
                if ctx.stop.is_set():
                    break
                # Yield to the event loop regularly. At high load the inbox is never empty, so
                # without this the loop starves every other task — reconnects waited out their
                # backoff and then another 40 s, and loop_lag reached minutes.
                now_m = time.monotonic()
                if now_m - last_yield > 0.05:
                    last_yield = now_m
                    await asyncio.sleep(0)
        finally:
            if pinger:
                pinger.cancel()
            for t in list(st.tasks):
                t.cancel()
        w.event(conn, {"type": "close", "name": spec.name, "reason": ws.close_reason, "resyncs": st.resyncs})
    finally:
        ws.close()


async def _pinger(ws, payload: str, every: float):
    while True:
        await asyncio.sleep(every)
        await ws.send(payload)


async def ws_loop(ctx: Ctx, spec: Spec, start_delay: float = 0.0):
    """Keep one websocket alive; log every message; reconnect with jittered backoff."""
    w = ctx.w
    backoff = 1.0
    await sleep_or_stop(ctx, start_delay)
    while not ctx.stop.is_set():
        conn = ctx.conn_id()
        st = ConnState(ctx, conn, spec)
        check_errors = 0
        started = time.monotonic()
        try:
            if ctx.raw_ws:
                check_errors_box = [check_errors]
                await _raw_ws_session(ctx, spec, st, conn, check_errors_box)
                check_errors = check_errors_box[0]
                if ctx.stop.is_set():
                    break
                await sleep_or_stop(ctx, backoff + random.uniform(0, 3))
                backoff = min(backoff * 2, 30.0)
                continue
            async with websockets.connect(spec.url, proxy=None, max_size=64 << 20, open_timeout=15,
                                          ping_interval=20, ping_timeout=30, close_timeout=5) as ws:
                w.event(conn, {"type": "connect", "name": spec.name, "url": spec.url[:300], "topics": spec.topics})
                for m in spec.subscribe:
                    await ws.send(m)
                if spec.on_open:
                    spec.on_open(conn)
                backoff = 1.0
                pinger = asyncio.create_task(_pinger(ws, spec.app_ping, 20)) if spec.app_ping else None
                try:
                    async for msg in ws:
                        w.write("ws", conn, time.time_ns(), time.monotonic_ns(), as_json_text(msg))
                        ctx.msgs += 1
                        if len(msg) < 600 and _is_reject(msg):
                            w.event(conn, {"type": "sub_rejected", "name": spec.name, "msg": msg[:400]})
                            if _is_rate_reject(msg):
                                ctx.rate_limited(f"websocket reject: {msg[:200]}")
                        if spec.check:
                            try:
                                spec.check(st, ws, msg)
                            except Exception as e:  # noqa: BLE001 - never let a checker kill capture
                                check_errors += 1
                                if check_errors <= 5:
                                    w.event(conn, {"type": "check_error", "name": spec.name, "err": repr(e)[:300]})
                        if ctx.stop.is_set():
                            break
                finally:
                    if pinger:
                        pinger.cancel()
                    for t in list(st.tasks):
                        t.cancel()
                w.event(conn, {"type": "close", "name": spec.name, "code": ws.close_code,
                               "reason": ws.close_reason, "resyncs": st.resyncs})
        except RateLimited as e:
            ctx.rate_limited(str(e))
        except websockets.exceptions.InvalidStatus as e:
            code = e.response.status_code
            w.event(conn, {"type": "handshake_rejected", "name": spec.name, "status": code})
            if code in (418, 429) or (code == 403 and ctx.venue == "bybit"):
                ctx.rate_limited(f"websocket handshake HTTP {code}")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - recorded, then reconnect
            w.event(conn, {"type": "error", "name": spec.name, "err": f"{type(e).__name__}: {e}"[:500]})
        # A session that stayed up even briefly ended for an external reason (the venue cycles
        # connections); reconnect promptly. Without this reset the raw path doubles the delay on
        # every drop and, since connection lifetimes are often under 60 s, it never recovers —
        # measured reconnect delays reached 122 s (1+2+4+8+16+32+60) and left minute-long holes.
        if time.monotonic() - started > 20.0:
            backoff = 1.0
        if ctx.stop.is_set():
            break
        await sleep_or_stop(ctx, backoff + random.uniform(0, 3))
        backoff = min(backoff * 2, 30.0)


def shards(items: list, size: int) -> list[list]:
    """Round-robin so every connection gets a mix of busy and quiet markets."""
    n = max(1, -(-len(items) // size))
    out = [[] for _ in range(n)]
    for i, it in enumerate(items):
        out[i % n].append(it)
    return out


# ---------------------------------------------------------------- Binance
class Snapshots:
    """Binance REST depth snapshots: gap > connect > periodic, paced by request weight."""

    def __init__(self, ctx: Ctx, url_fmt: str, weight: int, budget_per_min: int, backoff_used: int):
        self.ctx, self.url_fmt, self.backoff_used = ctx, url_fmt, backoff_used
        self.interval = 60.0 * weight / budget_per_min
        self.q: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.pending: dict = {}
        self.seq = itertools.count()

    def request(self, sym: str, conn: int, why: str):
        """One pending request per symbol; a request for a newer connection always replaces an older one."""
        prio = {"gap": 0, "connect": 1, "new": 1}.get(why, 2)
        cur = self.pending.get(sym)
        if cur is not None and cur[0] <= prio and cur[2] >= conn:
            return
        self.pending[sym] = (prio, why, conn)
        self.q.put_nowait((prio, next(self.seq), sym))

    async def run(self):
        while not self.ctx.stop.is_set():
            try:
                prio, _, sym = await asyncio.wait_for(self.q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            cur = self.pending.get(sym)
            if cur is None or cur[0] != prio:
                continue  # superseded by another request for this symbol
            del self.pending[sym]
            self.ctx.spawn(self._one(sym, cur[1], cur[2]), f"snapshot:{sym}")
            await sleep_or_stop(self.ctx, self.interval)

    async def _one(self, sym, why, conn):
        try:
            info, _ = await rest_record(self.ctx, self.url_fmt.format(sym=sym),
                                        {"ep": "depth", "sym": sym, "why": why, "for_conn": conn,
                                         "req": next(self.seq)})
            if info.get("wt", 0) > self.backoff_used:
                self.ctx.w.event(0, {"type": "weight_backoff", "wt": info["wt"]})
                self.interval *= 1.5
        except RateLimited as e:
            self.ctx.rate_limited(str(e))
        except Exception as e:  # noqa: BLE001
            self.ctx.w.event(0, {"type": "snapshot_error", "sym": sym, "err": repr(e)[:300]})
            self.request(sym, conn, "retry")


def _prefix_has(msg, needle: str, window: int) -> bool:
    """Cheap early-out that works on both the raw path (bytes) and the websockets fallback (str)."""
    if isinstance(msg, bytes):
        return needle.encode() in msg[:window]
    return needle in msg[:window]


def binance_depth_check(snaps: Snapshots, spot: bool):
    def check(st: ConnState, ws, msg):
        if not _prefix_has(msg, "@depth", 72):
            return
        x = orjson.loads(msg)["data"]
        s = x["s"]
        prev = st.last.get(s)
        if prev is not None and (x["U"] != prev + 1 if spot else x["pu"] != prev):
            st.ctx.w.event(st.conn, {"type": "gap", "sym": s, "prev_u": prev, "U": x["U"], "pu": x.get("pu")})
            snaps.request(s, st.conn, "gap")
        st.last[s] = x["u"]
    return check


class Binance:
    def __init__(self, ctx: Ctx, a):
        self.ctx, self.a, self.spot = ctx, a, a.market == "spot"
        if self.spot:
            self.ws = "wss://stream.binance.com:9443/stream?streams="
            self.snaps = Snapshots(ctx, "https://api.binance.com/api/v3/depth?symbol={sym}&limit=500",
                                   weight=25, budget_per_min=2400, backoff_used=3000)
            self.resnap_s = 3600
        else:
            self.ws = "wss://fstream.binance.com/public/stream?streams="
            self.market_ws = "wss://fstream.binance.com/market/stream?streams="
            self.snaps = Snapshots(ctx, "https://fapi.binance.com/fapi/v1/depth?symbol={sym}&limit=500",
                                   weight=10, budget_per_min=900, backoff_used=1500)
            self.resnap_s = 1800
        self.syms: list = []
        self.sym_conn: dict = {}   # symbol -> connection currently carrying its diff stream
        self.k = itertools.count()

    def specs(self, recs, why="connect"):
        out = []
        for part in shards([r["sym"] for r in recs], self.a.shard):
            streams = []
            for s in part:
                streams += [f"{s.lower()}@depth@100ms", f"{s.lower()}@trade"]
                if self.a.bbo:
                    streams.append(f"{s.lower()}@bookTicker")
            self.syms += part

            def on_open(conn, part=part):
                for s in part:  # snapshot after the diff stream is live; replay is done offline
                    self.sym_conn[s] = conn
                    self.snaps.request(s, conn, why)

            out.append(Spec(f"binance-{self.a.market}-{next(self.k)}", self.ws + "/".join(streams), streams,
                            check=binance_depth_check(self.snaps, self.spot), on_open=on_open))
        return out

    def global_specs(self):
        if self.spot:
            return []
        st = ["!markPrice@arr@1s", "!forceOrder@arr"]
        return [Spec("binance-perp-market", self.market_ws + "/".join(st), st)]

    def tasks(self):
        out = [self.snaps.run(), self._resnap()]
        if not self.spot:
            out.append(self._oi_poll())
        return out

    async def _resnap(self):
        while not self.ctx.stop.is_set():
            await sleep_or_stop(self.ctx, self.resnap_s)
            for s in list(self.syms):
                self.snaps.request(s, self.sym_conn.get(s, 0), "periodic")

    async def _oi_poll(self, period=120.0):
        """Open interest has no Binance websocket; poll every symbol every `period` seconds (weight 1)."""
        while not self.ctx.stop.is_set():
            syms = list(self.syms)
            gap = period / max(1, len(syms))
            for s in syms:
                if self.ctx.stop.is_set():
                    return
                self.ctx.spawn(self._oi(s), f"oi:{s}")
                await asyncio.sleep(gap)

    async def _oi(self, s):
        try:
            await rest_record(self.ctx, f"https://fapi.binance.com/fapi/v1/openInterest?symbol={s}", {"ep": "oi", "sym": s})
        except RateLimited as e:
            self.ctx.rate_limited(str(e))
        except Exception as e:  # noqa: BLE001
            self.ctx.w.event(0, {"type": "oi_error", "sym": s, "err": repr(e)[:200]})


# ---------------------------------------------------------------- Bybit
def bybit_book_check(st: ConnState, ws, msg):
    if not _prefix_has(msg, '"orderbook.200.', 40):
        return
    d = orjson.loads(msg)
    topic, u = d["topic"], d["data"]["u"]
    if d.get("type") == "snapshot":
        st.last[topic] = u
        return
    prev = st.last.get(topic)
    if prev is not None and u == prev + 1:
        st.last[topic] = u
        return
    st.last.pop(topic, None)
    st.ctx.w.event(st.conn, {"type": "gap", "topic": topic, "prev_u": prev, "u": u})
    if st.allow_resync(topic):
        st.send_later(ws, [orjson.dumps({"op": "unsubscribe", "args": [topic]}).decode(),
                           orjson.dumps({"op": "subscribe", "args": [topic]}).decode()])


class Bybit:
    def __init__(self, ctx: Ctx, a):
        self.ctx, self.a = ctx, a
        self.url = f"wss://stream.bybit.com/v5/public/{'spot' if a.market == 'spot' else 'linear'}"
        self.k = itertools.count()

    def specs(self, recs, why="connect"):
        out = []
        for part in shards([r["sym"] for r in recs], self.a.shard):
            topics = []
            for s in part:
                topics += [f"orderbook.200.{s}", f"publicTrade.{s}"]
                if self.a.bbo:
                    topics.append(f"orderbook.1.{s}")
                topics.append(f"priceLimit.{s}")
                if self.a.market == "perp":
                    topics += [f"allLiquidation.{s}", f"tickers.{s}"]
            subs = [orjson.dumps({"op": "subscribe", "args": topics[i:i + 10]}).decode() for i in range(0, len(topics), 10)]
            out.append(Spec(f"bybit-{self.a.market}-{next(self.k)}", self.url, topics, subs,
                            app_ping='{"op":"ping"}', check=bybit_book_check))
        return out

    def global_specs(self):
        return []

    def tasks(self):
        return []


# ---------------------------------------------------------------- OKX
def okx_book_check(st: ConnState, ws, msg):
    if not _prefix_has(msg, '"channel":"books"', 60) or not _prefix_has(msg, '"data"', 200):
        return
    d = orjson.loads(msg)
    inst = d["arg"]["instId"]
    for x in d["data"]:
        if d.get("action") == "snapshot":
            st.last[inst] = x["seqId"]
            continue
        prev = st.last.get(inst)
        if prev is not None and x["prevSeqId"] == prev:
            st.last[inst] = x["seqId"]
            continue
        st.last.pop(inst, None)
        st.ctx.w.event(st.conn, {"type": "gap", "inst": inst, "prev_seq": prev, "prevSeqId": x["prevSeqId"]})
        if st.allow_resync(inst):
            arg = {"channel": "books", "instId": inst}
            st.send_later(ws, [orjson.dumps({"op": "unsubscribe", "args": [arg]}).decode(),
                               orjson.dumps({"op": "subscribe", "args": [arg]}).decode()])
        return


class Okx:
    PUB = "wss://ws.okx.com:8443/ws/v5/public"
    BIZ = "wss://ws.okx.com:8443/ws/v5/business"

    def __init__(self, ctx: Ctx, a):
        self.ctx, self.a = ctx, a
        self.k = itertools.count()
        self.indexes: set = set()
        self.liq_done = False

    @staticmethod
    def _subs(args):
        return [orjson.dumps({"op": "subscribe", "args": args[i:i + 20]}).decode() for i in range(0, len(args), 20)]

    def specs(self, recs, why="connect"):
        out = []
        perp = self.a.market == "perp"
        by_sym = {r["sym"]: r for r in recs}
        for part in shards(list(by_sym), self.a.shard):
            pub, biz = [], []
            for inst in part:
                pub.append({"channel": "books", "instId": inst})
                if self.a.bbo:
                    pub.append({"channel": "bbo-tbt", "instId": inst})
                pub.append({"channel": "price-limit", "instId": inst})
                biz.append({"channel": "trades-all", "instId": inst})
                if perp:
                    pub += [{"channel": c, "instId": inst} for c in ("funding-rate", "open-interest", "mark-price")]
                    idx = by_sym[inst].get("index_id")
                    if idx and idx not in self.indexes:
                        self.indexes.add(idx)
                        pub.append({"channel": "index-tickers", "instId": idx})
            if perp and not self.liq_done:  # one venue-wide liquidation feed
                self.liq_done = True
                pub.append({"channel": "liquidation-orders", "instType": "SWAP"})
            k = next(self.k)
            out.append(Spec(f"okx-{self.a.market}-pub-{k}", self.PUB, pub, self._subs(pub), app_ping="ping",
                            check=okx_book_check))
            out.append(Spec(f"okx-{self.a.market}-biz-{k}", self.BIZ, biz, self._subs(biz), app_ping="ping"))
        return out

    def global_specs(self):
        return []

    def tasks(self):
        return []


VENUES = {"binance": Binance, "bybit": Bybit, "okx": Okx}
# seconds between connection handshakes (OKX: 3 new connections/s per IP, shared by perp + spot)
STAGGER = {"binance": 1.0, "bybit": 0.5, "okx": 0.8}


# ---------------------------------------------------------------- registry, clock, guards
async def load_instruments(ctx: Ctx, why: str):
    """Fetch and log instrument lists (the normalizer re-parses these logged bodies)."""
    parse = PARSERS[(ctx.venue, ctx.market)]
    recs = []
    for url in INSTRUMENT_URLS[(ctx.venue, ctx.market)]:
        cursor = ""
        while True:
            u = url + (f"&cursor={cursor}" if cursor else "")
            _, body = await rest_record(ctx, u, {"ep": "instruments", "why": why}, timeout=30)
            d = orjson.loads(body)
            recs += parse(d)
            cursor = ((d.get("result") or {}).get("nextPageCursor") or "") if ctx.venue == "bybit" else ""
            if not cursor:
                break
    return recs


async def registry_refresh(ctx: Ctx, venue_obj, known: set, every: float, launch):
    while not ctx.stop.is_set():
        await sleep_or_stop(ctx, every)
        if ctx.stop.is_set():
            return
        try:
            recs = await load_instruments(ctx, "refresh")
        except RateLimited as e:
            ctx.rate_limited(str(e))
            return
        except Exception as e:  # noqa: BLE001
            ctx.w.event(0, {"type": "registry_error", "err": repr(e)[:300]})
            continue
        now = {r["sym"] for r in recs}
        new = [r for r in recs if r["sym"] not in known]
        gone = sorted(known - now)
        ctx.w.event(0, {"type": "registry", "count": len(now), "new": [r["sym"] for r in new], "gone": gone})
        if new:
            known.update(r["sym"] for r in new)
            for sp in venue_obj.specs(new, why="new"):
                launch(sp, 0.0)


async def clock_probe(ctx: Ctx, every: float = 60.0):
    url = TIME_URLS[(ctx.venue, ctx.market)]
    while not ctx.stop.is_set():
        try:
            await rest_record(ctx, url, {"ep": "time"})
        except RateLimited as e:
            ctx.rate_limited(str(e))
            return
        except Exception as e:  # noqa: BLE001
            ctx.w.event(0, {"type": "time_error", "err": repr(e)[:300]})
        await sleep_or_stop(ctx, every)


async def guards(ctx: Ctx, duration: float):
    t0 = last_t = time.monotonic()
    last_cpu, last_msgs, max_lag = time.process_time(), 0, 0.0
    files = stop_files(ctx.root, ctx.venue)
    tick = 0
    while not ctx.stop.is_set():
        t = time.monotonic()
        await sleep_or_stop(ctx, 1.0)
        max_lag = max(max_lag, time.monotonic() - t - 1.0)
        tick += 1
        if any(os.path.exists(f) for f in files):
            ctx.halt("stop file present")
        if tick % 10:
            continue
        now, cpu = time.monotonic(), time.process_time()
        free = shutil.disk_usage(ctx.root).free
        if free < ctx.min_free:
            ctx.halt(f"disk free {free / 1e9:.1f}GB < {ctx.min_free / 1e9:.0f}GB")
        dt = now - last_t
        print(f"[{now - t0:7.0f}s] msgs={ctx.msgs} +{(ctx.msgs - last_msgs) / dt:.0f}/s "
              f"cpu={100 * (cpu - last_cpu) / dt:.0f}% loop_lag_max={1000 * max_lag:.0f}ms "
              f"raw={ctx.w.raw_bytes / 1e6:.0f}MB free={free / 1e9:.0f}GB", flush=True)
        last_t, last_cpu, last_msgs, max_lag = now, cpu, ctx.msgs, 0.0
        if duration and now - t0 >= duration:
            ctx.stop.set()


async def amain(a):
    os.makedirs(os.path.join(a.root, "meta"), exist_ok=True)
    ctx = Ctx(a)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, ctx.stop.set)
    blockers = [f for f in stop_files(a.root, a.venue) if os.path.exists(f)]
    if blockers:
        print(f"refusing to start: {blockers} exists (check the reason, then delete it)", flush=True)
        return 3

    try:
        recs = await load_instruments(ctx, "startup")
        vols = await asyncio.to_thread(fetch_volumes, a.venue, a.market)
    except RateLimited as e:
        ctx.rate_limited(str(e))
        ctx.w.close()
        return 2
    recs.sort(key=lambda r: vols.get(r["sym"], 0.0), reverse=True)
    recs = recs[a.skip:]
    if a.mod:   # rank modulo: the busiest markets are spread over the shards instead of grouping
        recs = [r for i, r in enumerate(recs) if i % a.mod == a.rem]
    if a.limit:
        recs = recs[:a.limit]
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    with open(os.path.join(a.root, "meta", f"instruments-{ctx.stream}-{stamp}.json"), "wb") as f:
        f.write(orjson.dumps({"ts": stamp, "count": len(recs), "bbo": a.bbo, "shard": a.shard,
                              "markets": [{**r, "qv24h": vols.get(r["sym"])} for r in recs]}, option=orjson.OPT_INDENT_2))
    print(f"{ctx.stream}: {len(recs)} markets, top: {' '.join(r['sym'] for r in recs[:8])}", flush=True)

    venue_obj = VENUES[a.venue](ctx, a)
    running: set = set()
    stagger = STAGGER[a.venue]
    offset = itertools.count()

    def critical_done(t: asyncio.Task):
        if t.cancelled() or ctx.stop.is_set():
            return
        e = t.exception()
        ctx.crash = f"{t.get_name()} died: {e!r}"[:300] if e else f"{t.get_name()} exited unexpectedly"
        ctx.w.event(0, {"type": "task_died", "task": t.get_name(), "err": ctx.crash})
        ctx.stop.set()

    def launch(spec: Spec, delay: float | None = None):
        d = (next(offset) * stagger + (0.4 if a.market == "spot" and a.venue == "okx" else 0.0)) if delay is None else delay
        t = asyncio.create_task(ws_loop(ctx, spec, d), name=f"ws:{spec.name}")
        t.add_done_callback(critical_done)
        running.add(t)

    for sp in venue_obj.global_specs() + venue_obj.specs(recs):
        launch(sp)
    known = {r["sym"] for r in recs}
    named = [(c.cr_code.co_name if hasattr(c, "cr_code") else "task", c) for c in venue_obj.tasks()]
    named += [("clock_probe", clock_probe(ctx)), ("guards", guards(ctx, a.duration)),
              ("registry", registry_refresh(ctx, venue_obj, known, a.registry_min * 60, launch))]
    for name, coro in named:
        t = asyncio.create_task(coro, name=name)
        t.add_done_callback(critical_done)
        running.add(t)

    await ctx.stop.wait()
    await asyncio.sleep(0.5)  # let in-flight writes land
    for t in list(running) + list(ctx.bg):  # stop websockets and background REST before closing the log
        t.cancel()
    await asyncio.gather(*running, *ctx.bg, return_exceptions=True)
    ctx.w.event(0, {"type": "stop", "fatal": ctx.fatal, "crash": ctx.crash})
    ctx.w.close()
    print(f"stopped. fatal={ctx.fatal} crash={ctx.crash} msgs={ctx.msgs}", flush=True)
    if ctx.fatal:
        return 2   # rate limit / disk / stop file: supervisor must not restart
    return 1 if ctx.crash else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--venue", required=True, choices=list(VENUES))
    ap.add_argument("--market", required=True, choices=["perp", "spot"])
    ap.add_argument("--duration", type=float, default=0, help="seconds; 0 = until SIGINT/SIGTERM")
    ap.add_argument("--root", default="data")
    ap.add_argument("--limit", type=int, default=0, help="only the N busiest markets (0 = all)")
    ap.add_argument("--skip", type=int, default=0, help="skip the N busiest markets first (sharding)")
    ap.add_argument("--mod", type=int, default=0, help="shard by rank: keep markets where rank %% MOD == REM")
    ap.add_argument("--rem", type=int, default=0, help="this shard's remainder, used with --mod")
    ap.add_argument("--tag", default="", help="suffix for this shard's raw stream name")
    ap.add_argument("--raw-ws", action="store_true",
                    help="own the socket: timestamp with the kernel's packet arrival time (SO_TIMESTAMPNS)")
    ap.add_argument("--uvloop", action="store_true", help="use uvloop's C event loop instead of asyncio's")
    ap.add_argument("--shard", type=int, default=50, help="markets per websocket connection")
    ap.add_argument("--bbo", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--registry-min", type=float, default=60, help="instrument list refresh interval")
    ap.add_argument("--min-free-gb", type=float, default=15)
    args = ap.parse_args()
    if args.uvloop:
        import uvloop
        uvloop.install()
    raise SystemExit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
