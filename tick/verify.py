"""Checks on normalized data. Exit code 0 = all checks PASS, 1 = any FAIL, 2 = nothing to check.

    python -m tick.verify [--root data] [--min-compared 100]

1. Book vs BBO: replay norm/book and compare the rebuilt best bid/ask (price AND size) with the venue's
   native BBO feed at the same update id (Binance u, Bybit seq, OKX seqId). Uses the 30 busiest
   symbols per venue-market. FAIL on any mismatch; INCONCLUSIVE if fewer than --min-compared events.
   OKX `books` currently sends checksum=0, so the BBO feed is the only independent reference.
2. Cross-venue units: per coin_id, normalized mids on every venue-market must agree within 5%
   (catches 1000x prefixes, contract-size errors, ticker collisions). Reported; a >50% outlier FAILs.
3. Quarantine and damaged segments are counted and listed; they never count as passed data.
"""
import argparse
import glob
import os
from collections import defaultdict

import pyarrow.parquet as pq

from .normalize import SCHEMA_VERSION

VMS = ("binance-perp", "binance-spot", "bybit-perp", "bybit-spot", "okx-perp", "okx-spot")


def files(root, table, vm):
    return sorted(glob.glob(os.path.join(root, "norm", f"v{SCHEMA_VERSION}", table, vm, "*.parquet")))


def book_vs_bbo(root, vm, sample_syms=30):
    bbo = defaultdict(dict)
    for path in files(root, "bbo", vm):
        t = pq.read_table(path, columns=["sym", "upd", "bid_px", "bid_qty", "ask_px", "ask_qty"]).to_pydict()
        for s, u, bp, bq, ap, aq in zip(t["sym"], t["upd"], t["bid_px"], t["bid_qty"], t["ask_px"], t["ask_qty"]):
            if u is not None:
                bbo[s][u] = (bp, bq, ap, aq)
    syms = set(sorted(bbo, key=lambda s: -len(bbo[s]))[:sample_syms])
    books, compared, match, examples = {}, 0, 0, []
    for path in files(root, "book", vm):
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
                    got = (bb, bids[bb], ba, asks[ba])
                    if all(abs(x - y) <= 1e-9 * max(1.0, abs(y)) for x, y in zip(got, ref)):
                        match += 1
                    elif len(examples) < 3:
                        examples.append({"sym": s, "upd": u, "book": got, "bbo": ref})
            i = j
    return compared, match, examples


def mids(root):
    out = defaultdict(dict)
    for vm in VMS:
        meta = {}
        for path in files(root, "instruments", vm):
            for r in pq.read_table(path, columns=["sym", "coin_id", "quote"]).to_pylist():
                meta[r["sym"]] = r
        last = {}
        for path in files(root, "bbo", vm):
            t = pq.read_table(path, columns=["sym", "bid_px", "ask_px"]).to_pydict()
            for s, b, a in zip(t["sym"], t["bid_px"], t["ask_px"]):
                if b and a:
                    last[s] = (b + a) / 2
        for s, mid in last.items():
            m = meta.get(s)
            if m:
                out[m["coin_id"]][f"{vm}:{m['quote']}"] = mid
    return out


def count_rows(root, table, vm):
    return sum(pq.ParquetFile(p).metadata.num_rows for p in files(root, table, vm))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data")
    ap.add_argument("--min-compared", type=int, default=100)
    a = ap.parse_args()
    present = [vm for vm in VMS if files(a.root, "book", vm)]
    if not present:
        print(f"NO DATA: nothing under {a.root}/norm/v{SCHEMA_VERSION}")
        raise SystemExit(2)
    results = []

    print("1) rebuilt best bid/ask (price and size) vs native BBO at the same update id")
    for vm in present:
        c, m, ex = book_vs_bbo(a.root, vm)
        status = "FAIL" if m < c else ("INCONCLUSIVE" if c < a.min_compared else "PASS")
        results.append(status)
        print(f"   {status:12} {vm:13} compared={c} match={m}")
        for e in ex:
            print(f"      {e}")

    print("2) cross-venue normalized mid per coin_id (>=4 venue-markets)")
    mm = mids(a.root)
    multi = {b: v for b, v in mm.items() if len(v) >= 4}
    off5, off50 = [], []
    for b, v in multi.items():
        vals = sorted(v.values())
        med = vals[len(vals) // 2]
        for k, x in v.items():
            r = abs(x / med - 1)
            if r > 0.5:
                off50.append((b, k, x, med))
            elif r > 0.05:
                off5.append((b, k, x, med))
    status = "FAIL" if off50 else ("INCONCLUSIVE" if not multi else "PASS")
    results.append(status)
    print(f"   {status:12} coin_ids={len(multi)} off>5%={len(off5)} off>50%={len(off50)}")
    for row in (off50 + off5)[:10]:
        print(f"      {row}")

    print("3) quarantine / damaged segments")
    for vm in present:
        q = count_rows(a.root, "quarantine", vm)
        dmg = uns = 0
        for path in files(a.root, "conn", vm):
            t = pq.read_table(path, columns=["event"]).to_pydict()
            dmg += sum(1 for e in t["event"] if e == "segment_damaged")
            uns = sum(1 for e in t["event"] if e == "segment_unsealed")
        print(f"   {vm:13} quarantined_rows={q} damaged_segments={dmg} unsealed_segments={uns}")

    final = "FAIL" if "FAIL" in results else ("INCONCLUSIVE" if "INCONCLUSIVE" in results else "PASS")
    print(f"RESULT: {final}")
    raise SystemExit(0 if final == "PASS" else 1)


if __name__ == "__main__":
    main()
