"""Raw append log -> one common schema (Parquet), per venue-market per UTC hour.

    python -m tick.normalize [--root data] [--only binance-perp,okx-spot]

Output: data/norm/<table>/<venue>-<market>/<YYYYMMDD-HH>.parquet
Tables and units are documented in SCHEMA.md. The rules that make the venues comparable:

* px  = quote currency per 1 coin   (Binance/Bybit "1000PEPE" price is divided by 1000)
* qty = coins                        (OKX SWAP contracts x ctVal; 1000-prefixed sizes x 1000)
* trade.side   = aggressor side (buy = taker bought)
* liq.pos_side = side of the position that was liquidated (long = a long was closed by a sell)
* book rows are level updates (qty 0 = delete); a row group sharing (chain, upd) is one book event
* avail_ns     = earliest local time the row could be known. For Binance the diff stream cannot be
                 applied until its REST anchor snapshot arrived, so replayed diffs get the snapshot's
                 receive time. Training and replay must use avail_ns, never recv_ns.

Instrument metadata comes from the instrument lists logged by the collector in the same run, so a
contract-size change during capture is picked up from the time it was logged.
"""
import argparse
import glob
import os
import time
from collections import defaultdict
from compression import zstd

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from .registry import PARSERS

SCHEMA_VERSION = 1

COLS = {
    "book": [("recv_ns", pa.int64()), ("avail_ns", pa.int64()), ("ex_ms", pa.int64()), ("sym", pa.string()),
             ("chain", pa.int32()), ("upd", pa.int64()), ("kind", pa.int8()), ("side", pa.int8()),
             ("px", pa.float64()), ("qty", pa.float64())],
    "bbo": [("recv_ns", pa.int64()), ("ex_ms", pa.int64()), ("sym", pa.string()), ("upd", pa.int64()), ("bid_px", pa.float64()),
            ("bid_qty", pa.float64()), ("ask_px", pa.float64()), ("ask_qty", pa.float64()), ("src", pa.int8())],
    "trade": [("recv_ns", pa.int64()), ("ex_ms", pa.int64()), ("sym", pa.string()), ("trade_id", pa.string()),
              ("px", pa.float64()), ("qty", pa.float64()), ("side", pa.int8()), ("flag", pa.string())],
    "liq": [("recv_ns", pa.int64()), ("ex_ms", pa.int64()), ("sym", pa.string()), ("pos_side", pa.int8()),
            ("px", pa.float64()), ("px_type", pa.string()), ("qty", pa.float64()), ("filled_qty", pa.float64()),
            ("feed", pa.string())],
    "deriv": [("recv_ns", pa.int64()), ("ex_ms", pa.int64()), ("sym", pa.string()), ("field", pa.string()),
              ("value", pa.float64()), ("ref_ms", pa.int64())],
    "conn": [("recv_ns", pa.int64()), ("conn", pa.int32()), ("event", pa.string()), ("detail", pa.string())],
    "instruments": [("recv_ns", pa.int64()), ("sym", pa.string()), ("coin_id", pa.string()), ("asset", pa.string()),
                    ("asset_raw", pa.string()), ("base_raw", pa.string()), ("base", pa.string()), ("mult", pa.float64()),
                    ("ct_val", pa.float64()), ("quote", pa.string()), ("settle", pa.string()), ("ctype", pa.string()),
                    ("status", pa.string()), ("tick", pa.string()), ("lot", pa.string()), ("listed_ms", pa.int64()),
                    ("fund_int_h", pa.float64()), ("index_id", pa.string())],
}
# book.kind: 0 = delta, 1 = snapshot row from the websocket, 2 = REST snapshot row
# book.side / bbo / trade.side: 1 = bid / buy, -1 = ask / sell ; liq.pos_side: 1 = long, -1 = short
# bbo.src: 1 = native BBO feed, 2 = derived from L2 (not produced in v1)


class Meta:
    """Instrument units by symbol, taken from logged instrument lists (latest at or before use)."""

    def __init__(self, venue, market):
        self.venue, self.market = venue, market
        self.by_sym: dict = {}
        self.index_to_syms: dict = defaultdict(list)

    def load(self, body: dict, out=None, recv_ns=None):
        for r in PARSERS[(self.venue, self.market)](body):
            self.by_sym[r["sym"]] = r
            if out is not None:
                out.add("instruments", recv_ns=recv_ns, **{k: r.get(k) for k, _ in COLS["instruments"] if k != "recv_ns"})
            if r.get("index_id"):
                if r["sym"] not in self.index_to_syms[r["index_id"]]:
                    self.index_to_syms[r["index_id"]].append(r["sym"])

    def px(self, sym, v):
        r = self.by_sym.get(sym)
        return float(v) / (r["mult"] if r else 1)

    def qty(self, sym, v):
        r = self.by_sym.get(sym)
        return float(v) * ((r["mult"] * r["ct_val"]) if r else 1)

    def known(self, sym):
        return sym in self.by_sym


class Out:
    def __init__(self):
        self.rows = {t: defaultdict(list) for t in COLS}
        self.missing_meta = defaultdict(int)

    def add(self, table, **kw):
        if table == "instruments":
            for k in ("mult", "ct_val", "fund_int_h"):
                if kw.get(k) is not None:
                    kw[k] = float(kw[k])
            if kw.get("listed_ms") is not None:
                kw["listed_ms"] = int(kw["listed_ms"])
        cols = self.rows[table]
        for name, _ in COLS[table]:
            cols[name].append(kw.get(name))

    def flush(self, root, vm, hour):
        written = {}
        for t, cols in self.rows.items():
            if not cols or not cols[COLS[t][0][0]]:
                continue
            schema = pa.schema(COLS[t], metadata={"schema_version": str(SCHEMA_VERSION), "source": vm})
            tbl = pa.table({n: pa.array(cols[n], type=ty) for n, ty in COLS[t]}, schema=schema)
            d = os.path.join(root, "norm", t, vm)
            os.makedirs(d, exist_ok=True)
            path = os.path.join(d, f"{hour}.parquet")
            pq.write_table(tbl, path, compression="zstd", compression_level=3)
            written[t] = tbl.num_rows
        return written


# ---------------------------------------------------------------- Binance book anchoring
class BinanceBooks:
    """Per (connection, symbol): buffer diffs until a REST snapshot arrives, then anchor and replay.

    Futures: drop diffs with u < lastUpdateId; the first applied diff must have U <= L <= u
    (it may straddle L); afterwards each diff's pu must equal the previous u.
    Spot: drop u <= L; first applied must have U <= L + 1 <= u; afterwards U == previous u + 1.
    A break closes the chain; diffs are buffered again until the next snapshot (requested by the
    collector on every gap) anchors a new chain. Rows before an anchor are never emitted.
    """

    def __init__(self, out: Out, meta: Meta, spot: bool, chain_ids):
        self.out, self.meta, self.spot, self.chain_ids = out, meta, spot, chain_ids
        # sym -> dict(conn, chain, last_u, buf, snap_L, first)
        #   chain None: buffering;  first True: anchored on a snapshot, next diff must straddle snap_L
        self.state: dict = {}
        self.stats = defaultdict(int)

    def _emit(self, rec_w, avail, x, sym, chain, kind):
        upd = x["u"]
        for side, rows in ((1, x["b"]), (-1, x["a"])):
            for p, q in rows:
                self.out.add("book", recv_ns=rec_w, avail_ns=avail, ex_ms=x.get("E"), sym=sym, chain=chain,
                             upd=upd, kind=kind, side=side, px=self.meta.px(sym, p), qty=self.meta.qty(sym, q))

    def _straddles(self, x, L) -> bool:
        return x["U"] <= L + 1 <= x["u"] if self.spot else x["U"] <= L <= x["u"]

    def _stale(self, x, L) -> bool:
        return x["u"] <= L if self.spot else x["u"] < L

    def diff(self, rec, x):
        sym = x["s"]
        st = self.state.get(sym)
        if st is None or st["conn"] != rec["c"]:
            st = self.state[sym] = {"conn": rec["c"], "chain": None, "last_u": None, "buf": [], "first": False}
        if st["chain"] is None:
            st["buf"].append((rec["w"], x))
            if len(st["buf"]) > 20000:  # no snapshot for a long time; keep memory bounded
                st["buf"] = st["buf"][-10000:]
                self.stats["buffer_trimmed"] += 1
            return
        if st["first"]:  # anchored on a snapshot that already contained every buffered diff
            L = st["snap_L"]
            if self._stale(x, L):
                return
            if not self._straddles(x, L):
                self._break(rec, st, sym, x)
                return
            st["first"] = False
            self._emit(rec["w"], rec["w"], x, sym, st["chain"], 0)
            st["last_u"] = x["u"]
            return
        ok = (x["U"] == st["last_u"] + 1) if self.spot else (x["pu"] == st["last_u"])
        if not ok:
            self._break(rec, st, sym, x)
            return
        self._emit(rec["w"], rec["w"], x, sym, st["chain"], 0)
        st["last_u"] = x["u"]

    def _break(self, rec, st, sym, x):
        self.stats["chain_break"] += 1
        self.out.add("conn", recv_ns=rec["w"], conn=rec["c"], event="book_gap", detail=sym)
        st.update(chain=None, first=False, buf=[(rec["w"], x)])

    def snapshot(self, rec, d):
        sym = rec.get("sym")
        st = self.state.get(sym)
        L = d["lastUpdateId"]
        if st is None:
            self.stats["snapshot_without_stream"] += 1
            return
        if st["chain"] is not None:
            self.stats["periodic_snapshot"] += 1  # chain healthy; logged raw rows remain for audit
            return
        buf = [(w, x) for w, x in st["buf"] if not self._stale(x, L)]
        if buf and not self._straddles(buf[0][1], L):
            # Snapshot is older than the oldest buffered diff: the stream started after it.
            self.stats["snapshot_too_old"] += 1
            return
        chain = next(self.chain_ids)
        avail = rec["w"]
        snap_x = {"u": L, "E": d.get("E") or d.get("T"), "b": d["bids"], "a": d["asks"]}
        self._emit(rec["w"], avail, snap_x, sym, chain, 2)
        last = None
        for w, x in buf:
            if last is not None:
                good = (x["U"] == last + 1) if self.spot else (x["pu"] == last)
                if not good:
                    self.stats["chain_break_in_buffer"] += 1
                    st.update(chain=None, first=False, buf=[])
                    return
            self._emit(w, max(w, avail), x, sym, chain, 0)  # replayed diffs are known only from avail
            last = x["u"]
        if last is None:  # snapshot already covers every buffered diff; next live diff must straddle L
            st.update(chain=chain, first=True, snap_L=L, last_u=None, buf=[])
        else:
            st.update(chain=chain, first=False, last_u=last, buf=[])
        self.stats["anchored"] += 1


# ---------------------------------------------------------------- per-venue handlers
def side_from_buyer_maker(m: bool) -> int:
    return -1 if m else 1  # buyer is maker -> taker sold


def handle_binance(rec, d, meta, out, books: BinanceBooks, spot):
    if rec["k"] == "rest":
        ep = rec.get("ep")
        if ep == "depth":
            books.snapshot(rec, d)
        elif ep == "oi" and isinstance(d, dict) and "openInterest" in d:
            s = d["symbol"]
            out.add("deriv", recv_ns=rec["w"], ex_ms=d.get("time"), sym=s, field="oi_coin",
                    value=meta.qty(s, d["openInterest"]))
        return
    stream, x = d.get("stream", ""), d.get("data")
    if x is None:
        return
    if "@depth" in stream:
        books.diff(rec, x)
    elif stream.endswith("@bookTicker"):
        s = x["s"]
        out.add("bbo", recv_ns=rec["w"], ex_ms=x.get("T") or x.get("E"), sym=s, upd=x.get("u"), bid_px=meta.px(s, x["b"]),
                bid_qty=meta.qty(s, x["B"]), ask_px=meta.px(s, x["a"]), ask_qty=meta.qty(s, x["A"]), src=1)
    elif stream.endswith("@trade"):
        s = x["s"]
        flag = x.get("X")
        if flag in ("NA",):  # non-trade records on the futures trade stream (e.g. insurance fund)
            flag = "NA"
        out.add("trade", recv_ns=rec["w"], ex_ms=x["T"], sym=s, trade_id=str(x["t"]), px=meta.px(s, x["p"]),
                qty=meta.qty(s, x["q"]), side=side_from_buyer_maker(x["m"]), flag=flag)
    elif stream == "!forceOrder@arr":
        o = x["o"]
        s = o["s"]
        # order side SELL closes a long; p = order price (bankruptcy-based limit), ap = average fill
        out.add("liq", recv_ns=rec["w"], ex_ms=o["T"], sym=s, pos_side=1 if o["S"] == "SELL" else -1,
                px=meta.px(s, o["ap"]), px_type="avg_fill", qty=meta.qty(s, o["q"]),
                filled_qty=meta.qty(s, o["z"]), feed="throttled")
    elif stream.startswith("!markPrice"):
        for m in x:
            s = m["s"]
            ts = m["E"]
            out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=s, field="mark", value=meta.px(s, m["p"]))
            out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=s, field="index", value=meta.px(s, m["i"]))
            if m.get("r") not in (None, ""):
                out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=s, field="funding_rate", value=float(m["r"]),
                        ref_ms=m.get("T"))


class BybitBooks:
    def __init__(self, out, meta, chain_ids):
        self.out, self.meta, self.chain_ids = out, meta, chain_ids
        self.state: dict = {}
        self.stats = defaultdict(int)

    def msg(self, rec, d):
        x = d["data"]
        s = x["s"]
        key = (rec["c"], s)
        snap = d.get("type") == "snapshot"
        st = self.state.get(key)
        if snap:
            st = self.state[key] = {"chain": next(self.chain_ids), "u": x["u"]}
            kind = 1
        else:
            if st is None or st["chain"] is None or x["u"] != st["u"] + 1:
                if st is not None and st["chain"] is not None:
                    self.stats["chain_break"] += 1
                    self.out.add("conn", recv_ns=rec["w"], conn=rec["c"], event="book_gap", detail=s)
                self.state[key] = {"chain": None, "u": None}
                return
            st["u"] = x["u"]
            kind = 0
        for side, rows in ((1, x.get("b", [])), (-1, x.get("a", []))):
            for p, q in rows:
                self.out.add("book", recv_ns=rec["w"], avail_ns=rec["w"], ex_ms=d.get("cts") or d.get("ts"), sym=s,
                             chain=st["chain"], upd=x.get("seq", x["u"]), kind=kind, side=side,
                             px=self.meta.px(s, p), qty=self.meta.qty(s, q))


def handle_bybit(rec, d, meta, out, books: BybitBooks, spot):
    if rec["k"] != "ws":
        return
    topic = d.get("topic", "")
    if topic.startswith("orderbook.200."):
        books.msg(rec, d)
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
                    qty=meta.qty(s, t["v"]), side=1 if t["S"] == "Buy" else -1, flag=flag)
    elif topic.startswith("allLiquidation."):
        for t in d["data"]:
            s = t["s"]
            # Bybit: S = side of the liquidated POSITION (Buy = long liquidated); p = bankruptcy price
            out.add("liq", recv_ns=rec["w"], ex_ms=t["T"], sym=s, pos_side=1 if t["S"] == "Buy" else -1,
                    px=meta.px(s, t["p"]), px_type="bankruptcy", qty=meta.qty(s, t["v"]), feed="all_500ms")
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


class OkxBooks:
    def __init__(self, out, meta, chain_ids):
        self.out, self.meta, self.chain_ids = out, meta, chain_ids
        self.state: dict = {}
        self.stats = defaultdict(int)

    def msg(self, rec, d):
        inst = d["arg"]["instId"]
        key = (rec["c"], inst)
        for x in d["data"]:
            snap = d.get("action") == "snapshot"
            st = self.state.get(key)
            if snap:
                st = self.state[key] = {"chain": next(self.chain_ids), "seq": x["seqId"]}
                kind = 1
            else:
                if st is None or st["chain"] is None or x["prevSeqId"] != st["seq"]:
                    if st is not None and st["chain"] is not None:
                        self.stats["chain_break"] += 1
                        self.out.add("conn", recv_ns=rec["w"], conn=rec["c"], event="book_gap", detail=inst)
                    self.state[key] = {"chain": None, "seq": None}
                    continue
                st["seq"] = x["seqId"]
                kind = 0
            for side, rows in ((1, x.get("bids", [])), (-1, x.get("asks", []))):
                for r in rows:
                    self.out.add("book", recv_ns=rec["w"], avail_ns=rec["w"], ex_ms=int(x["ts"]), sym=inst,
                                 chain=st["chain"], upd=x["seqId"], kind=kind, side=side,
                                 px=self.meta.px(inst, r[0]), qty=self.meta.qty(inst, r[1]))


def handle_okx(rec, d, meta, out, books: OkxBooks, spot):
    if rec["k"] != "ws" or not isinstance(d, dict) or "data" not in d:
        return
    arg = d.get("arg", {})
    ch = arg.get("channel")
    if ch == "books":
        books.msg(rec, d)
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
                    flag=None if t.get("source") in (None, "0") else f"src{t['source']}")
    elif ch == "liquidation-orders":
        for g in d["data"]:
            inst = g["instId"]
            for x in g.get("details", []):
                # OKX: posSide is the liquidated position side; in net mode posSide is "net" -> use order side
                ps = x.get("posSide")
                pos = 1 if ps == "long" else -1 if ps == "short" else (1 if x.get("side") == "sell" else -1)
                out.add("liq", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=inst, pos_side=pos,
                        px=meta.px(inst, x["bkPx"]), px_type="bankruptcy", qty=meta.qty(inst, x["sz"]),
                        feed="okx_swap")
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
            ts = int(x["ts"])
            out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=inst, field="funding_rate",
                    value=float(x["fundingRate"]), ref_ms=int(x["fundingTime"]))
            if x.get("nextFundingRate") not in (None, ""):
                out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=inst, field="funding_rate_next",
                        value=float(x["nextFundingRate"]), ref_ms=int(x["nextFundingTime"]))
            if x.get("settFundingRate") not in (None, ""):
                out.add("deriv", recv_ns=rec["w"], ex_ms=ts, sym=inst, field="funding_rate_settled",
                        value=float(x["settFundingRate"]), ref_ms=int(x["prevFundingTime"]) if x.get("prevFundingTime") else None)
    elif ch == "open-interest":
        for x in d["data"]:
            out.add("deriv", recv_ns=rec["w"], ex_ms=int(x["ts"]), sym=x["instId"], field="oi_coin",
                    value=float(x["oiCcy"]))


HANDLERS = {"binance": handle_binance, "bybit": handle_bybit, "okx": handle_okx}


def counter():
    i = 0
    while True:
        i += 1
        yield i


def normalize_vm(root: str, vm: str, done_hours: set | None = None):
    venue, market = vm.split("-")
    spot = market == "spot"
    files = sorted(glob.glob(os.path.join(root, "raw", vm, "*.jsonl.zst")))
    meta = Meta(venue, market)
    chain_ids = counter()
    report = {"files": 0, "rows": defaultdict(int), "stats": {}}
    handler = HANDLERS[venue]
    for path in files:
        hour = os.path.basename(path).split(".")[0]
        out = Out()
        books = (BinanceBooks(out, meta, spot, chain_ids) if venue == "binance"
                 else BybitBooks(out, meta, chain_ids) if venue == "bybit" else OkxBooks(out, meta, chain_ids))
        if hasattr(normalize_vm, "_carry") and normalize_vm._carry.get(vm):  # carry book state across hours
            books.state = normalize_vm._carry[vm]
            books.out = out
        try:
            fh = zstd.open(path, "rb")
            for line in fh:
                try:
                    rec = orjson.loads(line)
                except orjson.JSONDecodeError:
                    continue
                d = rec["d"]
                if rec["k"] == "evt":
                    out.add("conn", recv_ns=rec["w"], conn=rec["c"], event=d.get("type"),
                            detail=orjson.dumps({k: v for k, v in d.items() if k not in ("type", "topics")}).decode()[:500])
                    continue
                if rec["k"] == "rest" and rec.get("ep") == "instruments":
                    meta.load(d, out, rec["w"])
                    continue
                handler(rec, d, meta, out, books, spot)
        except (EOFError, zstd.ZstdError):
            pass  # trailing frame still open (file being written)
        normalize_vm._carry = getattr(normalize_vm, "_carry", {})
        normalize_vm._carry[vm] = books.state
        written = out.flush(root, vm, hour)
        report["files"] += 1
        for t, n in written.items():
            report["rows"][t] += n
        for k, v in books.stats.items():
            report["stats"][k] = report["stats"].get(k, 0) + v
    report["rows"] = dict(report["rows"])
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default="data")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    vms = [os.path.basename(p) for p in sorted(glob.glob(os.path.join(a.root, "raw", "*")))]
    if a.only:
        vms = [v for v in vms if v in a.only.split(",")]
    t0 = time.time()
    for vm in vms:
        t = time.time()
        r = normalize_vm(a.root, vm)
        print(f"{vm:14} {time.time() - t:6.1f}s rows={r['rows']} books={r['stats']}", flush=True)
    print(f"done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
