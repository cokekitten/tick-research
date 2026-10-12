"""Raw segments -> one common schema (Parquet), per venue-market per UTC hour.

    python -m tick.normalize [--root data] [--only binance-perp,okx-spot] [--include-partial]

Output: data/norm/v2/<table>/<venue>-<market>/<YYYYMMDD-HH>-<part>.parquet (see SCHEMA.md); a re-run
rebuilds the venue-market from its raw segments. Rules that make
the venues comparable:

* px  = quote currency per 1 coin   (Binance/Bybit "1000PEPE" price is divided by 1000)
* qty = coins                        (OKX SWAP contracts x ctVal; 1000-prefixed sizes x 1000)
  The exchange's own strings are kept in px_raw / qty_raw.
* Rows whose instrument has no logged metadata are NOT converted: they go to the `quarantine` table.
* trade.side = aggressor side; liq.pos_side = side of the liquidated position.
* book rows are level updates (qty 0 = delete); rows sharing (sym, chain, upd) form one book event.
* avail_ns = earliest local time a book row could be known. Replayed Binance diffs only become known
  when their REST anchor arrived. Training and replay must use avail_ns, never recv_ns.

Book state is keyed by (run_id, connection id, symbol), so connection ids reused by a later run never
mix, and a REST snapshot is only used for the connection it was requested for.
The whole capture of one venue-market is replayed in order (segments are processed sequentially and
book state carries across hour boundaries); output is flushed in batches of --batch-rows.
"""
import argparse
import os
import shutil
import time
from collections import defaultdict

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from .common import read_lines, segment_files
from .registry import PARSERS

SCHEMA_VERSION = 2

S, I8, I32, I64, F64, B = pa.string(), pa.int8(), pa.int32(), pa.int64(), pa.float64(), pa.bool_()
COLS = {
    "book": [("recv_ns", I64), ("avail_ns", I64), ("ex_ms", I64), ("sym", S), ("chain", I64), ("upd", I64),
             ("kind", I8), ("side", I8), ("px", F64), ("qty", F64), ("px_raw", S), ("qty_raw", S)],
    "bbo": [("recv_ns", I64), ("ex_ms", I64), ("sym", S), ("upd", I64), ("bid_px", F64), ("bid_qty", F64),
            ("ask_px", F64), ("ask_qty", F64), ("src", I8)],
    "trade": [("recv_ns", I64), ("ex_ms", I64), ("sym", S), ("trade_id", S), ("px", F64), ("qty", F64),
              ("side", I8), ("flag", S), ("px_raw", S), ("qty_raw", S)],
    "liq": [("recv_ns", I64), ("ex_ms", I64), ("sym", S), ("pos_side", I8), ("px", F64), ("px_type", S),
            ("qty", F64), ("filled_qty", F64), ("feed", S)],
    "deriv": [("recv_ns", I64), ("ex_ms", I64), ("sym", S), ("field", S), ("value", F64), ("ref_ms", I64)],
    "price_limit": [("recv_ns", I64), ("ex_ms", I64), ("sym", S), ("buy_max", F64), ("sell_min", F64),
                    ("enabled", B)],
    "conn": [("recv_ns", I64), ("run_id", S), ("conn", I32), ("event", S), ("detail", S)],
    "instruments": [("recv_ns", I64), ("sym", S), ("coin_id", S), ("asset", S), ("asset_raw", S),
                    ("base_raw", S), ("base", S), ("mult", F64), ("ct_val", F64), ("quote", S), ("settle", S),
                    ("ctype", S), ("status", S), ("tick", S), ("lot", S), ("listed_ms", I64),
                    ("fund_int_h", F64), ("index_id", S)],
    "quarantine": [("recv_ns", I64), ("sym", S), ("reason", S), ("source", S)],
}
# book.kind: 0 delta, 1 websocket snapshot (replaces the book), 2 REST snapshot (Binance anchor)
# side: 1 bid / buy, -1 ask / sell ; liq.pos_side: 1 long liquidated, -1 short liquidated
# bbo.src: 1 native BBO feed


class Unknown(Exception):
    """Instrument without logged metadata: the row must not be unit-converted."""


class Meta:
    """Instrument units by symbol, from the instrument lists the collector logged (latest seen wins)."""

    def __init__(self, venue, market):
        self.venue, self.market = venue, market
        self.by_sym: dict = {}
        self.index_to_syms: dict = defaultdict(list)

    def load(self, body: dict, out, recv_ns):
        for r in PARSERS[(self.venue, self.market)](body):
            self.by_sym[r["sym"]] = r
            if r.get("index_id") and r["sym"] not in self.index_to_syms[r["index_id"]]:
                self.index_to_syms[r["index_id"]].append(r["sym"])
            row = {k: r.get(k) for k, _ in COLS["instruments"] if k != "recv_ns"}
            for k in ("mult", "ct_val", "fund_int_h"):
                if row[k] is not None:
                    row[k] = float(row[k])
            if row["listed_ms"] is not None:
                row["listed_ms"] = int(row["listed_ms"])
            out.add("instruments", recv_ns=recv_ns, **row)

    def _r(self, sym):
        r = self.by_sym.get(sym)
        if r is None:
            raise Unknown(sym)
        return r

    def px(self, sym, v):
        return float(v) / self._r(sym)["mult"]

    def qty(self, sym, v):
        r = self._r(sym)
        return float(v) * r["mult"] * r["ct_val"]


class Out:
    """Column buffers flushed to one Parquet file per table x UTC hour, in row groups."""

    def __init__(self, root, vm, batch_rows):
        self.root, self.vm, self.batch_rows = root, vm, batch_rows
        self.rows = {t: defaultdict(list) for t in COLS}
        self.count = {t: 0 for t in COLS}
        self.writers: dict = {}   # (table, hour) -> (ParquetWriter, tmp path)
        self.parts: dict = defaultdict(int)   # (table, hour) -> files already written
        self.hour = None
        self.last_w = 0
        self.total = defaultdict(int)

    def set_hour(self, recv_ns):
        self.last_w = recv_ns
        hour = time.strftime("%Y%m%d-%H", time.gmtime(recv_ns / 1e9))
        if hour != self.hour:
            self.flush_all()
            for key in [k for k in self.writers if k[1] != hour]:
                self._close(key)
            self.hour = hour

    def add(self, table, **kw):
        cols = self.rows[table]
        for name, _ in COLS[table]:
            cols[name].append(kw.get(name))
        self.count[table] += 1
        if self.count[table] >= self.batch_rows:
            self._flush(table)

    def _flush(self, table):
        if not self.count[table]:
            return
        cols = self.rows[table]
        schema = pa.schema(COLS[table], metadata={"schema_version": str(SCHEMA_VERSION), "source": self.vm})
        tbl = pa.table({n: pa.array(cols[n], type=ty) for n, ty in COLS[table]}, schema=schema)
        key = (table, self.hour)
        w = self.writers.get(key)
        if w is None:
            d = os.path.join(self.root, "norm", f"v{SCHEMA_VERSION}", table, self.vm)
            os.makedirs(d, exist_ok=True)
            self.parts[key] += 1
            path = os.path.join(d, f"{self.hour}-{self.parts[key]:02d}.parquet.tmp")
            w = self.writers[key] = (pq.ParquetWriter(path, schema, compression="zstd", compression_level=3), path)
        w[0].write_table(tbl)
        self.total[table] += tbl.num_rows
        self.rows[table] = defaultdict(list)
        self.count[table] = 0

    def _close(self, key):
        w, path = self.writers.pop(key)
        w.close()
        os.replace(path, path[: -len(".tmp")])  # publish only complete files

    def flush_all(self):
        for t in COLS:
            self._flush(t)

    def close(self):
        self.flush_all()
        for key in list(self.writers):
            self._close(key)


# ---------------------------------------------------------------- book chains
class ChainIds:
    def __init__(self):
        self.i = 0

    def __call__(self):
        self.i += 1
        return self.i


class BinanceBooks:
    """Per (run, conn, symbol): buffer diffs until the REST snapshot requested for that connection
    arrives, then anchor and replay. Works whichever arrives first.

    Futures: drop diffs with u < L; the first applied diff must have U <= L <= u; then pu == previous u.
    Spot:    drop diffs with u <= L; the first applied diff must have U <= L+1 <= u; then U == previous u + 1.
    A break closes the chain and buffers again until the next snapshot for this connection.
    A periodic snapshot on a healthy chain is logged (kind 2 rows are not emitted twice).
    """

    MAX_BUF = 20000

    def __init__(self, out: Out, meta: Meta, spot: bool, chain_ids):
        self.out, self.meta, self.spot, self.chain_ids = out, meta, spot, chain_ids
        self.state: dict = {}     # (run, conn, sym) -> state
        self.stats = defaultdict(int)

    def _st(self, run, conn, sym):
        k = (run, conn, sym)
        st = self.state.get(k)
        if st is None:
            st = self.state[k] = {"chain": None, "last_u": None, "buf": [], "snap": None, "first": False}
        return st

    def _straddles(self, x, L):
        return x["U"] <= L + 1 <= x["u"] if self.spot else x["U"] <= L <= x["u"]

    def _stale(self, x, L):
        return x["u"] <= L if self.spot else x["u"] < L

    def _next_ok(self, x, last):
        return x["U"] == last + 1 if self.spot else x["pu"] == last

    def _emit(self, rec_w, avail, x, sym, chain, kind):
        for side, rows in ((1, x["b"]), (-1, x["a"])):
            for p, q in rows:
                self.out.add("book", recv_ns=rec_w, avail_ns=avail, ex_ms=x.get("E"), sym=sym, chain=chain,
                             upd=x["u"], kind=kind, side=side, px=self.meta.px(sym, p), qty=self.meta.qty(sym, q),
                             px_raw=p, qty_raw=q)

    def diff(self, run, rec, x):
        sym = x["s"]
        st = self._st(run, rec["c"], sym)
        if st["chain"] is None:
            st["buf"].append((rec["w"], x))
            if len(st["buf"]) > self.MAX_BUF:
                st["buf"] = st["buf"][-self.MAX_BUF // 2:]
                self.stats["buffer_trimmed"] += 1
            if st["snap"] is not None:   # snapshot arrived before any usable diff
                self._anchor(st, sym, *st["snap"])
            return
        if st["first"]:   # anchored on a snapshot newer than every buffered diff
            L = st["snap_L"]
            if self._stale(x, L):
                return
            if not self._straddles(x, L):
                self._break(run, rec, st, sym, x)
                return
            st["first"] = False
        elif not self._next_ok(x, st["last_u"]):
            self._break(run, rec, st, sym, x)
            return
        self._emit(rec["w"], rec["w"], x, sym, st["chain"], 0)
        st["last_u"] = x["u"]

    def _break(self, run, rec, st, sym, x):
        self.stats["chain_break"] += 1
        self.out.add("conn", recv_ns=rec["w"], run_id=run, conn=rec["c"], event="book_gap", detail=sym)
        st.update(chain=None, first=False, snap=None, buf=[(rec["w"], x)])

    def snapshot(self, run, rec, d):
        sym, conn = rec.get("sym"), rec.get("for_conn")
        if conn in (None, 0):
            self.stats["snapshot_no_conn"] += 1
            return
        st = self._st(run, conn, sym)
        if st["chain"] is not None:
            self.stats["snapshot_on_healthy_chain"] += 1
            return
        self._anchor(st, sym, rec, d)

    def _anchor(self, st, sym, rec, d):
        L = d["lastUpdateId"]
        buf = [(w, x) for w, x in st["buf"] if not self._stale(x, L)]
        if buf and not self._straddles(buf[0][1], L):
            # buffered stream starts after the snapshot: wait for a newer snapshot
            self.stats["snapshot_too_old"] += 1
            st["snap"] = None
            return
        chain = self.chain_ids()
        avail = rec["w"]
        self._emit(rec["w"], avail, {"u": L, "E": d.get("E") or d.get("T"), "b": d["bids"], "a": d["asks"]},
                   sym, chain, 2)
        last = None
        for w, x in buf:
            if last is not None and not self._next_ok(x, last):
                self.stats["chain_break_in_buffer"] += 1
                st.update(chain=None, first=False, snap=None, buf=[])
                return
            self._emit(w, max(w, avail), x, sym, chain, 0)   # known only once the anchor arrived
            last = x["u"]
        if last is None:
            st.update(chain=chain, first=True, snap_L=L, last_u=None, buf=[], snap=None)
        else:
            st.update(chain=chain, first=False, last_u=last, buf=[], snap=None)
        self.stats["anchored"] += 1

    def remember_snapshot(self, run, rec, d):
        """Snapshot that arrived before its stream had any diff: keep it until diffs show up."""
        sym, conn = rec.get("sym"), rec.get("for_conn")
        if conn in (None, 0):
            return False
        st = self._st(run, conn, sym)
        if st["chain"] is None and not st["buf"]:
            st["snap"] = (rec, d)
            self.stats["snapshot_before_stream"] += 1
            return True
        return False


class SeqBooks:
    """Bybit / OKX: websocket snapshot + deltas with a continuity id; a break waits for a new snapshot."""

    def __init__(self, out, meta, chain_ids):
        self.out, self.meta, self.chain_ids = out, meta, chain_ids
        self.state: dict = {}
        self.stats = defaultdict(int)

    def apply(self, run, rec, sym, snap, cont_ok, new_id, ex_ms, upd, bids, asks):
        key = (run, rec["c"], sym)
        st = self.state.get(key)
        if snap:
            st = self.state[key] = {"chain": self.chain_ids(), "id": new_id}
            kind = 1
        else:
            if st is None or st["chain"] is None or not cont_ok(st["id"]):
                if st is not None and st["chain"] is not None:
                    self.stats["chain_break"] += 1
                    self.out.add("conn", recv_ns=rec["w"], run_id=run, conn=rec["c"], event="book_gap", detail=sym)
                self.state[key] = {"chain": None, "id": None}
                return
            st["id"] = new_id
            kind = 0
        for side, rows in ((1, bids), (-1, asks)):
            for r in rows:
                self.out.add("book", recv_ns=rec["w"], avail_ns=rec["w"], ex_ms=ex_ms, sym=sym, chain=st["chain"],
                             upd=upd, kind=kind, side=side, px=self.meta.px(sym, r[0]), qty=self.meta.qty(sym, r[1]),
                             px_raw=r[0], qty_raw=r[1])


# ---------------------------------------------------------------- per-venue handlers
def handle_binance(run, rec, d, meta, out, books: BinanceBooks):
    if rec["k"] == "rest":
        ep = rec.get("ep")
        if ep == "depth" and isinstance(d, dict) and "lastUpdateId" in d:
            meta._r(rec["sym"])
            if not books.remember_snapshot(run, rec, d):
                books.snapshot(run, rec, d)
        elif ep == "oi" and isinstance(d, dict) and "openInterest" in d:
            s = d["symbol"]
            out.add("deriv", recv_ns=rec["w"], ex_ms=d.get("time"), sym=s, field="oi_coin",
                    value=meta.qty(s, d["openInterest"]))
        return
    stream, x = d.get("stream", ""), d.get("data")
    if x is None:
        return
    if "@depth" in stream:
        meta._r(x["s"])
        books.diff(run, rec, x)
    elif stream.endswith("@bookTicker"):
        s = x["s"]
        out.add("bbo", recv_ns=rec["w"], ex_ms=x.get("T") or x.get("E"), sym=s, upd=x.get("u"),
                bid_px=meta.px(s, x["b"]), bid_qty=meta.qty(s, x["B"]), ask_px=meta.px(s, x["a"]),
                ask_qty=meta.qty(s, x["A"]), src=1)
    elif stream.endswith("@trade"):
        s = x["s"]
        out.add("trade", recv_ns=rec["w"], ex_ms=x["T"], sym=s, trade_id=str(x["t"]), px=meta.px(s, x["p"]),
                qty=meta.qty(s, x["q"]), side=-1 if x["m"] else 1, flag=x.get("X"), px_raw=x["p"], qty_raw=x["q"])
    elif stream == "!forceOrder@arr":
        o = x["o"]
        s = o["s"]
        # order side SELL closes a long; ap = average fill price
        out.add("liq", recv_ns=rec["w"], ex_ms=o["T"], sym=s, pos_side=1 if o["S"] == "SELL" else -1,
                px=meta.px(s, o["ap"]), px_type="avg_fill", qty=meta.qty(s, o["q"]),
                filled_qty=meta.qty(s, o["z"]), feed="throttled")
    elif stream.startswith("!markPrice"):
        for m in x:
            s = m["s"]
            if s not in meta.by_sym:   # venue-wide array includes out-of-scope contracts
                continue
            ts = m["E"]
            out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=s, field="mark", value=meta.px(s, m["p"]))
            out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=s, field="index", value=meta.px(s, m["i"]))
            if m.get("r") not in (None, ""):
                out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=s, field="funding_rate", value=float(m["r"]),
                        ref_ms=m.get("T"))


def handle_bybit(run, rec, d, meta, out, books: SeqBooks):
    if rec["k"] != "ws":
        return
    topic = d.get("topic", "")
    if topic.startswith("orderbook.200."):
        x = d["data"]
        u = x["u"]
        books.apply(run, rec, x["s"], d.get("type") == "snapshot", lambda prev: u == prev + 1, u,
                    d.get("cts") or d.get("ts"), x.get("seq", u), x.get("b", []), x.get("a", []))
    elif topic.startswith("orderbook.1."):
        x = d["data"]
        s = x["s"]
        b, a = (x.get("b") or [[None, None]])[0], (x.get("a") or [[None, None]])[0]
        out.add("bbo", recv_ns=rec["w"], ex_ms=d.get("cts") or d.get("ts"), sym=s, upd=x.get("seq"),
                bid_px=meta.px(s, b[0]) if b[0] else None, bid_qty=meta.qty(s, b[1]) if b[1] else None,
                ask_px=meta.px(s, a[0]) if a[0] else None, ask_qty=meta.qty(s, a[1]) if a[1] else None, src=1)
    elif topic.startswith("publicTrade."):
        for t in d["data"]:
            s = t["s"]
            flag = "RPI" if t.get("RPI") else ("BT" if t.get("BT") else None)
            out.add("trade", recv_ns=rec["w"], ex_ms=t["T"], sym=s, trade_id=str(t["i"]), px=meta.px(s, t["p"]),
                    qty=meta.qty(s, t["v"]), side=1 if t["S"] == "Buy" else -1, flag=flag, px_raw=t["p"],
                    qty_raw=t["v"])
    elif topic.startswith("allLiquidation."):
        for t in d["data"]:
            s = t["s"]
            # Bybit: S = side of the liquidated POSITION (Buy = long liquidated); p = bankruptcy price
            out.add("liq", recv_ns=rec["w"], ex_ms=t["T"], sym=s, pos_side=1 if t["S"] == "Buy" else -1,
                    px=meta.px(s, t["p"]), px_type="bankruptcy", qty=meta.qty(s, t["v"]), feed="all_500ms")
    elif topic.startswith("priceLimit."):
        x = d["data"]
        s = x["symbol"]
        out.add("price_limit", recv_ns=rec["w"], ex_ms=d.get("ts"), sym=s, buy_max=meta.px(s, x["buyLmt"]),
                sell_min=meta.px(s, x["sellLmt"]), enabled=None)
    elif topic.startswith("tickers."):
        x = d["data"]
        s = x["symbol"]
        ts = d.get("ts")
        for f, key, conv in (("mark", "markPrice", "px"), ("index", "indexPrice", "px"),
                             ("funding_rate", "fundingRate", None), ("oi_coin", "openInterest", "qty")):
            v = x.get(key)
            if v in (None, ""):
                continue
            val = meta.px(s, v) if conv == "px" else meta.qty(s, v) if conv == "qty" else float(v)
            out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=s, field=f, value=val,
                    ref_ms=int(x["nextFundingTime"]) if f == "funding_rate" and x.get("nextFundingTime") else None)


def handle_okx(run, rec, d, meta, out, books: SeqBooks):
    if rec["k"] != "ws" or not isinstance(d, dict) or "data" not in d:
        return
    arg = d.get("arg", {})
    ch = arg.get("channel")
    if ch == "books":
        inst = arg["instId"]
        for x in d["data"]:
            books.apply(run, rec, inst, d.get("action") == "snapshot",
                        lambda prev, x=x: x["prevSeqId"] == prev, x["seqId"], int(x["ts"]), x["seqId"],
                        [r[:2] for r in x.get("bids", [])], [r[:2] for r in x.get("asks", [])])
    elif ch == "bbo-tbt":
        inst = arg["instId"]
        for x in d["data"]:
            b, a = (x.get("bids") or [[None, None]])[0], (x.get("asks") or [[None, None]])[0]
            out.add("bbo", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=inst, upd=x.get("seqId"),
                    bid_px=meta.px(inst, b[0]) if b[0] else None, bid_qty=meta.qty(inst, b[1]) if b[1] else None,
                    ask_px=meta.px(inst, a[0]) if a[0] else None, ask_qty=meta.qty(inst, a[1]) if a[1] else None,
                    src=1)
    elif ch == "trades-all":
        for t in d["data"]:
            inst = t["instId"]
            out.add("trade", recv_ns=rec["w"], ex_ms=int(t["ts"]), sym=inst, trade_id=str(t["tradeId"]),
                    px=meta.px(inst, t["px"]), qty=meta.qty(inst, t["sz"]), side=1 if t["side"] == "buy" else -1,
                    flag=None if t.get("source") in (None, "0") else f"src{t['source']}", px_raw=t["px"],
                    qty_raw=t["sz"])
    elif ch == "liquidation-orders":
        for g in d["data"]:
            inst = g["instId"]
            if inst not in meta.by_sym:   # venue-wide feed includes out-of-scope (e.g. inverse) contracts
                out.add("quarantine", recv_ns=rec["w"], sym=inst, reason="out_of_scope", source="liquidation-orders")
                continue
            for x in g.get("details", []):
                ps = x.get("posSide")
                pos = 1 if ps == "long" else -1 if ps == "short" else (1 if x.get("side") == "sell" else -1)
                out.add("liq", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=inst, pos_side=pos,
                        px=meta.px(inst, x["bkPx"]), px_type="bankruptcy", qty=meta.qty(inst, x["sz"]),
                        feed="okx_swap")
    elif ch == "price-limit":
        for x in d["data"]:
            inst = x["instId"]
            # enabled=false comes with empty limits: a real "no limit" state, not missing data
            out.add("price_limit", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=inst,
                    buy_max=meta.px(inst, x["buyLmt"]) if x.get("buyLmt") else None,
                    sell_min=meta.px(inst, x["sellLmt"]) if x.get("sellLmt") else None, enabled=x.get("enabled"))
    elif ch == "mark-price":
        for x in d["data"]:
            out.add("deriv", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=x["instId"], field="mark",
                    value=meta.px(x["instId"], x["markPx"]))
    elif ch == "index-tickers":
        for x in d["data"]:
            for inst in meta.index_to_syms.get(x["instId"], []):
                out.add("deriv", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=inst, field="index",
                        value=float(x["idxPx"]))
    elif ch == "funding-rate":
        for x in d["data"]:
            inst = x["instId"]
            meta._r(inst)
            ts = int(x["ts"])
            out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=inst, field="funding_rate",
                    value=float(x["fundingRate"]), ref_ms=int(x["fundingTime"]))
            if x.get("nextFundingRate") not in (None, ""):
                out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=inst, field="funding_rate_next",
                        value=float(x["nextFundingRate"]), ref_ms=int(x["nextFundingTime"]))
            if x.get("settFundingRate") not in (None, ""):
                out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=inst, field="funding_rate_settled",
                        value=float(x["settFundingRate"]),
                        ref_ms=int(x["prevFundingTime"]) if x.get("prevFundingTime") else None)
    elif ch == "open-interest":
        for x in d["data"]:
            inst = x["instId"]
            meta._r(inst)
            out.add("deriv", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=inst, field="oi_coin", value=float(x["oiCcy"]))


HANDLERS = {"binance": handle_binance, "bybit": handle_bybit, "okx": handle_okx}


def run_id_of(path: str) -> str:
    """data/raw/<vm>/<run_id>-<seg>.jsonl.zst -> run_id (run ids contain one '-': stamp-pid)."""
    name = os.path.basename(path).split(".")[0]
    return name.rsplit("-", 1)[0]


def normalize_vm(root: str, vm: str, batch_rows: int = 200_000, include_partial: bool = False):
    venue, market = vm.split("-")
    files = segment_files(root, vm, include_partial)
    meta = Meta(venue, market)
    chain_ids = ChainIds()
    out = Out(root, vm, batch_rows)
    books = BinanceBooks(out, meta, market == "spot", chain_ids) if venue == "binance" else SeqBooks(out, meta, chain_ids)
    handler = HANDLERS[venue]
    report = {"segments": len(files), "damaged": [], "unsealed": [], "quarantined": 0, "errors": defaultdict(int)}
    for path in files:
        run = run_id_of(path)
        src = os.path.basename(path)
        lines = read_lines(path)
        unsealed = path.endswith(".partial")
        while True:
            try:
                line = next(lines)
            except StopIteration as stop:
                n, err = stop.value or (0, None)
                if err:
                    report["damaged"].append((src, n, err))
                    out.add("conn", recv_ns=out.last_w, run_id=run, conn=0, event="segment_damaged",
                            detail=f"{src} after {n} lines: {err}"[:500])
                if unsealed:   # writer was killed, or is still running: data ends here for this run
                    report["unsealed"].append(src)
                    out.add("conn", recv_ns=out.last_w, run_id=run, conn=0, event="segment_unsealed",
                            detail=f"{src} after {n} lines")
                break
            try:
                rec = orjson.loads(line)
            except orjson.JSONDecodeError:
                report["errors"]["bad_json"] += 1
                continue
            out.set_hour(rec["w"])
            d = rec["d"]
            if rec["k"] == "evt":
                out.add("conn", recv_ns=rec["w"], run_id=run, conn=rec["c"], event=d.get("type"),
                        detail=orjson.dumps({k: v for k, v in d.items() if k not in ("type", "topics")}).decode()[:500])
                continue
            if rec["k"] == "rest" and rec.get("ep") == "instruments":
                meta.load(d, out, rec["w"])
                continue
            try:
                handler(run, rec, d, meta, out, books)
            except Unknown as e:
                report["quarantined"] += 1
                out.add("quarantine", recv_ns=rec["w"], sym=str(e), reason="no_metadata", source=f"{src}#{rec['n']}")
            except (KeyError, TypeError, ValueError, IndexError) as e:
                report["errors"][f"{type(e).__name__}"] += 1
                out.add("quarantine", recv_ns=rec["w"], sym=None, reason=f"parse:{type(e).__name__}:{e}"[:200],
                        source=f"{src}#{rec['n']}")
    out.close()
    report["rows"] = dict(out.total)
    report["books"] = dict(books.stats)
    report["errors"] = dict(report["errors"])
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="data")
    ap.add_argument("--only", default="")
    ap.add_argument("--batch-rows", type=int, default=200_000)
    ap.add_argument("--include-partial", action="store_true", help="also read the segment still being written")
    a = ap.parse_args()
    raw = os.path.join(a.root, "raw")
    vms = sorted(v for v in os.listdir(raw) if os.path.isdir(os.path.join(raw, v))) if os.path.isdir(raw) else []
    if a.only:
        vms = [v for v in vms if v in a.only.split(",")]
    if not vms:
        raise SystemExit(f"no raw data under {raw}")
    t0 = time.time()
    for vm in vms:
        t = time.time()
        for table in COLS:
            shutil.rmtree(os.path.join(a.root, "norm", f"v{SCHEMA_VERSION}", table, vm), ignore_errors=True)
        r = normalize_vm(a.root, vm, a.batch_rows, a.include_partial)
        print(f"{vm:14} {time.time() - t:6.1f}s segments={r['segments']} rows={r['rows']} books={r['books']} "
              f"quarantined={r['quarantined']} errors={r['errors']} damaged={r['damaged']} unsealed={r['unsealed']}",
              flush=True)
    print(f"done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
