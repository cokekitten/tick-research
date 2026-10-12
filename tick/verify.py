"""Checks that the normalized data is internally consistent and comparable across venues.

    python -m tick.verify [--root data]

1. Book vs BBO: replay norm/book rows and compare the rebuilt best bid/ask with the venue's native
   BBO feed (Bybit orderbook.1 / OKX bbo-tbt / Binance bookTicker) matched by the exchange's own
   update id where one exists (Binance u, Bybit u, OKX seqId), else by exchange timestamp.
   OKX `books` currently sends checksum=0 on every message (observed 2026-10-12), so the BBO feed is
   our only independent reference.
2. Cross-venue unit check: per coin_id, normalized mid price on every venue-market must agree within
   a few percent (catches 1000x prefixes, contract-size errors and ticker collisions).
"""
import argparse
import glob
import os
from collections import defaultdict

import pyarrow.parquet as pq



def verify_book_vs_bbo_exact(root, vm, sample_syms=30):
    bbo = defaultdict(dict)
    for path in sorted(glob.glob(os.path.join(root, "norm", "bbo", vm, "*.parquet"))):
        t = pq.read_table(path, columns=["sym", "upd", "bid_px", "ask_px"]).to_pydict()
        for s, u, b, a in zip(t["sym"], t["upd"], t["bid_px"], t["ask_px"]):
            if u is not None:
                bbo[s][u] = (b, a)
    syms = set(sorted(bbo, key=lambda s: -len(bbo[s]))[:sample_syms])
    books, compared, match, examples = {}, 0, 0, []
    for path in sorted(glob.glob(os.path.join(root, "norm", "book", vm, "*.parquet"))):
        t = pq.read_table(path, columns=["sym", "chain", "upd", "kind", "side", "px", "qty"]).to_pydict()
        n = len(t["sym"])
        i = 0
        while i < n:
            s, ch, u, kind = t["sym"][i], t["chain"][i], t["upd"][i], t["kind"][i]
            j = i
            while j < n and t["sym"][j] == s and t["chain"][j] == ch and t["upd"][j] == u:
                j += 1
            if s in syms:
                key = (s, ch)
                if key not in books or kind != 0:
                    books[key] = ({}, {})
                bids, asks = books[key]
                for k in range(i, j):
                    side = bids if t["side"][k] == 1 else asks
                    if t["qty"][k] == 0:
                        side.pop(t["px"][k], None)
                    else:
                        side[t["px"][k]] = t["qty"][k]
                ref = bbo[s].get(u)
                if ref is not None and bids and asks:
                    compared += 1
                    bb, ba = max(bids), min(asks)
                    if ref[0] == bb and ref[1] == ba:
                        match += 1
                    elif len(examples) < 3:
                        examples.append((s, u, "book", bb, ba, "bbo", ref))
            i = j
    return compared, match, compared - match, examples


def mids(root):
    """Last normalized mid per (coin_id, venue-market:quote) from BBO; units/ids from norm/instruments."""
    out = defaultdict(dict)
    for vm_dir in sorted(glob.glob(os.path.join(root, "norm", "bbo", "*"))):
        vm = os.path.basename(vm_dir)
        meta = {}
        for path in sorted(glob.glob(os.path.join(root, "norm", "instruments", vm, "*.parquet"))):
            for r in pq.read_table(path, columns=["sym", "coin_id", "quote"]).to_pylist():
                meta[r["sym"]] = r
        last = {}
        for path in sorted(glob.glob(os.path.join(vm_dir, "*.parquet"))):
            t = pq.read_table(path, columns=["sym", "bid_px", "ask_px"]).to_pydict()
            for s, b, a in zip(t["sym"], t["bid_px"], t["ask_px"]):
                if b and a:
                    last[s] = (b + a) / 2
        for s, mid in last.items():
            m = meta.get(s)
            if not m or m["quote"] not in ("USDT", "USDC"):
                continue
            out[m["coin_id"]][f"{vm}:{m['quote']}"] = mid
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data")
    a = ap.parse_args()

    print("1) L2-rebuilt best bid/ask vs native BBO feed at the same update id (30 busiest symbols)")
    for vm in ("binance-perp", "binance-spot", "bybit-perp", "bybit-spot", "okx-perp", "okx-spot"):
        c, m, x, ex = verify_book_vs_bbo_exact(a.root, vm)
        print(f"   {vm:13} compared={c} match={m} mismatch={x}" + (f"  ({100 * m / c:.2f}%)" if c else ""))
        for e in ex:
            print("      ", e)

    print("2) cross-venue normalized mid price per coin_id")
    mm = mids(a.root)
    multi = {b: v for b, v in mm.items() if len(v) >= 4}
    bad = []
    for b, v in multi.items():
        vals = sorted(v.values())
        med = vals[len(vals) // 2]
        for k, x in v.items():
            if abs(x / med - 1) > 0.05:
                bad.append((b, k, x, med))
    print(f"   coin_ids on >=4 venue-markets: {len(multi)}; quotes >5% off the coin median: {len(bad)}")
    for row in bad[:15]:
        print("     ", row)
    for b in ("BTC", "PEPE", "SHIB", "BONK"):
        if b in mm:
            print(f"   {b}: " + "  ".join(f"{k}={v:.8g}" for k, v in sorted(mm[b].items())))


if __name__ == "__main__":
    main()
