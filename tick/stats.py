"""Measure what the raw append log actually contains.

    python -m tick.stats [--root data]

Answers, from our own capture:
  * density per market: book messages/s vs level rows/s (the "what is one row" question)
  * how much the author's "same market, same 5s bucket, identical to previous row" rule removes,
    at message granularity and at level granularity
  * book update intervals per market (is a sub-second lag grid backed by data?)
  * sequence continuity, reconnects, receive latency vs exchange timestamps, clock offset
  * storage per day (raw JSON vs zstd on disk)
"""
import argparse
import glob
import json
import os
import statistics as st
import time
from collections import defaultdict
from compression import zstd

AUTHOR = {"book_rows_per_s_per_market": 2.23, "trades_per_s_per_market": 0.40}


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def iter_lines(path):
    try:
        with zstd.open(path, "rt") as f:
            for line in f:
                yield line
    except (EOFError, zstd.ZstdError):
        return  # file still being written; ignore the open frame


class Mkt:
    __slots__ = ("msgs", "levels", "trades", "fills", "first", "last", "times", "dedup_msg", "noop_lvl",
                 "prev_key", "prev_bucket", "lvl", "empty")

    def __init__(self):
        self.msgs = self.levels = self.trades = self.fills = self.dedup_msg = self.noop_lvl = self.empty = 0
        self.first = self.last = None
        self.times = []
        self.prev_key = self.prev_bucket = None
        self.lvl = {}  # (side, price) -> (qty, bucket)

    def book(self, t_ms, bids, asks, snapshot):
        self.msgs += 1
        self.times.append(t_ms)
        bucket = int(t_ms // 5000)
        key = (tuple(map(tuple, bids)), tuple(map(tuple, asks)))
        if not bids and not asks:
            self.empty += 1
        if not snapshot and key == self.prev_key and bucket == self.prev_bucket:
            self.dedup_msg += 1
        self.prev_key, self.prev_bucket = key, bucket
        n = 0
        for side, rows in (("b", bids), ("a", asks)):
            for r in rows:
                n += 1
                k = (side, r[0])
                old = self.lvl.get(k)
                if not snapshot and old is not None and old[0] == r[1] and old[1] == bucket:
                    self.noop_lvl += 1
                self.lvl[k] = (r[1], bucket)
        self.levels += n


class Ex:
    def __init__(self, name):
        self.name = name
        self.mk = defaultdict(Mkt)
        self.liq = 0
        self.lat = []          # local receive - exchange event time (ms), book msgs
        self.offsets = []      # server - local (ms) from time probes
        self.rtts = []
        self.gaps = 0
        self.chain_resets = 0
        self.conns = set()
        self.events = defaultdict(int)
        self.raw_bytes = 0
        self.zst_bytes = 0
        self.w_first = self.w_last = None
        self.snapshots_rest = 0
        self.seq = {}          # (conn, sym) -> last seq marker

    def seen(self, w):
        self.w_first = w if self.w_first is None else min(self.w_first, w)
        self.w_last = w if self.w_last is None else max(self.w_last, w)


def handle_binance(E, rec, d):
    if rec["k"] == "rest":
        if rec.get("ep") == "time":
            mid = (rec["t0w"] + rec["w"]) / 2e6
            E.offsets.append(d["serverTime"] - mid)
            E.rtts.append((rec["m"] - rec["t0m"]) / 1e6)
        elif rec.get("ep") == "depth":
            E.snapshots_rest += 1
        return
    stream, x = d.get("stream", ""), d.get("data")
    if stream.endswith("@depth@100ms"):
        s = x["s"]
        E.mk[s].book(x["E"], x["b"], x["a"], False)
        E.lat.append(rec["w"] / 1e6 - x["E"])
        k = (rec["c"], s)
        if k in E.seq:
            if x["pu"] != E.seq[k]:
                E.gaps += 1
        else:
            E.chain_resets += 1
        E.seq[k] = x["u"]
    elif stream.endswith("@aggTrade"):
        m = E.mk[x["s"]]
        m.trades += 1
        m.fills += x["l"] - x["f"] + 1
    elif stream == "!forceOrder@arr":
        E.liq += 1


def handle_bybit(E, rec, d):
    if rec["k"] == "rest":
        if rec.get("ep") == "time":
            mid = (rec["t0w"] + rec["w"]) / 2e6
            E.offsets.append(int(d["result"]["timeNano"]) / 1e6 - mid)
            E.rtts.append((rec["m"] - rec["t0m"]) / 1e6)
        return
    topic = d.get("topic", "")
    if topic.startswith("orderbook."):
        x = d["data"]
        s = x["s"]
        snap = d.get("type") == "snapshot"
        E.mk[s].book(d["ts"], x.get("b", []), x.get("a", []), snap)
        E.lat.append(rec["w"] / 1e6 - d["ts"])
        k = (rec["c"], s)
        if snap or k not in E.seq:
            E.chain_resets += 1
        elif x["u"] != E.seq[k] + 1:
            E.gaps += 1
        E.seq[k] = x["u"]
    elif topic.startswith("publicTrade."):
        m = E.mk[topic.split(".", 1)[1]]
        m.trades += len(d["data"])
        m.fills += len(d["data"])
    elif topic.startswith("allLiquidation."):
        E.liq += len(d.get("data", []))


def handle_okx(E, rec, d):
    if rec["k"] == "rest":
        if rec.get("ep") == "time":
            mid = (rec["t0w"] + rec["w"]) / 2e6
            E.offsets.append(int(d["data"][0]["ts"]) - mid)
            E.rtts.append((rec["m"] - rec["t0m"]) / 1e6)
        return
    if not isinstance(d, dict) or "arg" not in d or "data" not in d:
        return
    ch = d["arg"].get("channel")
    if ch == "books":
        s = d["arg"]["instId"]
        for x in d["data"]:
            snap = d.get("action") == "snapshot"
            bids = [r[:2] for r in x.get("bids", [])]
            asks = [r[:2] for r in x.get("asks", [])]
            E.mk[s].book(int(x["ts"]), bids, asks, snap)
            E.lat.append(rec["w"] / 1e6 - int(x["ts"]))
            k = (rec["c"], s)
            if snap or k not in E.seq:
                E.chain_resets += 1
            elif x.get("prevSeqId") != E.seq[k]:
                E.gaps += 1
            E.seq[k] = x.get("seqId")
    elif ch == "trades":
        for x in d["data"]:
            m = E.mk[x["instId"]]
            m.trades += 1
            m.fills += int(x.get("count", 1) or 1)
    elif ch == "liquidation-orders":
        for x in d["data"]:
            E.liq += len(x.get("details", []))


HANDLERS = {"binance": handle_binance, "bybit": handle_bybit, "okx": handle_okx}


def analyze(root):
    out = {}
    for ex, handler in HANDLERS.items():
        files = sorted(glob.glob(os.path.join(root, "raw", ex, "*.jsonl.zst")))
        if not files:
            continue
        E = Ex(ex)
        for p in files:
            E.zst_bytes += os.path.getsize(p)
            for line in iter_lines(p):
                E.raw_bytes += len(line.encode())
                rec = json.loads(line)
                E.seen(rec["w"])
                if rec["k"] == "evt":
                    E.events[rec["d"].get("type", "?")] += 1
                    continue
                E.conns.add(rec["c"])
                handler(E, rec, rec["d"])
        out[ex] = E
    return out


def summarize(E):
    dur = max(1e-9, (E.w_last - E.w_first) / 1e9)
    books = [m for m in E.mk.values() if m.msgs]
    msgs = sum(m.msgs for m in books)
    levels = sum(m.levels for m in books)
    trades = sum(m.trades for m in E.mk.values())
    fills = sum(m.fills for m in E.mk.values())
    nm = max(1, len(books))
    per_mkt_msg = [m.msgs / dur for m in books]
    per_mkt_lvl = [m.levels / dur for m in books]
    med_int = []
    for m in books:
        if len(m.times) > 2:
            ts = sorted(m.times)
            med_int.append(st.median(b - a for a, b in zip(ts, ts[1:])))
    return {
        "duration_s": round(dur, 1),
        "markets": len(books),
        "book_msgs_per_s_per_mkt": round(msgs / dur / nm, 2),
        "level_rows_per_s_per_mkt": round(levels / dur / nm, 2),
        "levels_per_msg": round(levels / max(1, msgs), 1),
        "per_mkt_msg_s_p10_p50_p90": [round(pct(per_mkt_msg, p), 2) for p in (10, 50, 90)],
        "per_mkt_lvl_s_p10_p50_p90": [round(pct(per_mkt_lvl, p), 1) for p in (10, 50, 90)],
        "median_book_interval_ms_p10_p50_p90": [pct(med_int, p) for p in (10, 50, 90)],
        "trade_msgs_per_s_per_mkt": round(trades / dur / nm, 2),
        "fills_per_s_per_mkt": round(fills / dur / nm, 2),
        "author_5s_dedup_msg_pct": round(100 * sum(m.dedup_msg for m in books) / max(1, msgs), 2),
        "author_5s_dedup_level_pct": round(100 * sum(m.noop_lvl for m in books) / max(1, levels), 2),
        "empty_book_msgs_pct": round(100 * sum(m.empty for m in books) / max(1, msgs), 2),
        "liquidations": E.liq,
        "seq_gaps": E.gaps,
        "chain_starts": E.chain_resets,
        "connections": len(E.conns - {0}),
        "events": dict(E.events),
        "rest_depth_snapshots": E.snapshots_rest,
        "clock_offset_ms_median": round(st.median(E.offsets), 1) if E.offsets else None,
        # local receive - exchange event time, corrected by the measured clock offset
        "recv_lat_ms_corr_p50_p99": (
            [round(pct(E.lat, p) + st.median(E.offsets), 1) for p in (50, 99)] if E.lat and E.offsets else None),
        "rest_rtt_ms_median": round(st.median(E.rtts), 1) if E.rtts else None,
        "raw_gb_per_day": round(E.raw_bytes / dur * 86400 / 1e9, 2),
        "zstd_gb_per_day": round(E.zst_bytes / dur * 86400 / 1e9, 2),
        "zstd_ratio": round(E.raw_bytes / max(1, E.zst_bytes), 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data")
    a = ap.parse_args()
    res = {ex: summarize(E) for ex, E in analyze(a.root).items()}
    os.makedirs(os.path.join(a.root, "meta"), exist_ok=True)
    path = os.path.join(a.root, "meta", f"stats-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    with open(path, "w") as f:
        json.dump({"author_reference": AUTHOR, "exchanges": res}, f, indent=1)
    keys = list(next(iter(res.values())).keys()) if res else []
    w = max(map(len, keys)) if keys else 10
    print(f"{'':{w}}  " + "  ".join(f"{ex:>22}" for ex in res))
    for k in keys:
        print(f"{k:{w}}  " + "  ".join(f"{json.dumps(res[ex][k], ensure_ascii=False):>22}" for ex in res))
    print(f"\nauthor (82h assumption): {AUTHOR}\nsaved {path}")


if __name__ == "__main__":
    main()
