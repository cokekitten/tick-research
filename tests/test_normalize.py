"""Normalizer: Binance anchoring in both orders, connection isolation, unit quarantine, replay timing."""
import glob
import os

import orjson
import pyarrow.parquet as pq

from tick.common import RawWriter
from tick.normalize import normalize_vm

T0 = 1_791_800_000_000_000_000  # ns


class Log:
    """Write a synthetic raw segment with controlled receive times."""

    def __init__(self, root, vm, run="20261012T000000Z-1"):
        self.w = RawWriter(str(root), vm, run)
        self.t = T0

    def _w(self, kind, conn, d, extra=None, dt_ms=10):
        self.t += dt_ms * 1_000_000
        self.w.write(kind, conn, self.t, self.t, orjson.dumps(d).decode(), extra)
        return self.t

    def instruments(self, body):
        return self._w("rest", 0, body, {"ep": "instruments", "status": 200, "t0w": self.t, "t0m": self.t})

    def ws(self, conn, d):
        return self._w("ws", conn, d)

    def rest(self, d, **extra):
        return self._w("rest", 0, d, {"status": 200, "t0w": self.t, "t0m": self.t, **extra})

    def close(self):
        self.w.close()


def read(root, table, vm):
    rows = []
    for p in sorted(glob.glob(os.path.join(str(root), "norm", "v2", table, vm, "*.parquet"))):
        rows += pq.read_table(p).to_pylist()
    return rows


BN_PERP_INFO = {"symbols": [
    {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT", "contractType": "PERPETUAL",
     "status": "TRADING", "underlyingType": "COIN", "filters": []},
    {"symbol": "1000PEPEUSDT", "baseAsset": "1000PEPE", "quoteAsset": "USDT", "marginAsset": "USDT",
     "contractType": "PERPETUAL", "status": "TRADING", "underlyingType": "COIN", "filters": []},
]}


def depth(sym, U, u, pu, b=(), a=()):
    return {"stream": f"{sym.lower()}@depth@100ms",
            "data": {"e": "depthUpdate", "E": 1, "s": sym, "U": U, "u": u, "pu": pu, "b": list(b), "a": list(a)}}


def snap(L, b, a):
    return {"lastUpdateId": L, "E": 1, "T": 1, "bids": b, "asks": a}


def test_binance_diff_first_then_snapshot(tmp_path):
    L = Log(tmp_path, "binance-perp")
    L.instruments(BN_PERP_INFO)
    L.ws(1, depth("BTCUSDT", 90, 99, 89, b=[["100", "1"]]))          # stale: u < snapshot L
    t_diff = L.ws(1, depth("BTCUSDT", 100, 105, 99, b=[["101", "2"]]))  # straddles L=100
    t_snap = L.rest(snap(100, [["100", "5"]], [["102", "1"]]), ep="depth", sym="BTCUSDT", for_conn=1)
    L.ws(1, depth("BTCUSDT", 106, 110, 105, a=[["102", "0"]]))
    L.close()
    r = normalize_vm(str(tmp_path), "binance-perp")
    assert r["books"]["anchored"] == 1
    book = read(tmp_path, "book", "binance-perp")
    assert [(x["kind"], x["px"]) for x in book] == [(2, 100.0), (2, 102.0), (0, 101.0), (0, 102.0)]
    replayed = book[2]
    assert replayed["recv_ns"] == t_diff and replayed["avail_ns"] == t_snap   # known only after the anchor


def test_binance_snapshot_first_then_diff(tmp_path):
    L = Log(tmp_path, "binance-perp")
    L.instruments(BN_PERP_INFO)
    L.rest(snap(100, [["100", "5"]], [["102", "1"]]), ep="depth", sym="BTCUSDT", for_conn=1)
    L.ws(1, depth("BTCUSDT", 98, 100, 97, b=[["100", "6"]]))          # straddles L=100 (U <= L <= u)
    L.ws(1, depth("BTCUSDT", 101, 103, 100, b=[["99", "1"]]))
    L.close()
    r = normalize_vm(str(tmp_path), "binance-perp")
    assert r["books"]["anchored"] == 1
    book = read(tmp_path, "book", "binance-perp")
    assert [x["kind"] for x in book] == [2, 2, 0, 0]


def test_binance_snapshot_for_other_connection_is_not_used(tmp_path):
    L = Log(tmp_path, "binance-perp")
    L.instruments(BN_PERP_INFO)
    L.ws(2, depth("BTCUSDT", 100, 105, 99, b=[["101", "2"]]))
    L.rest(snap(100, [["100", "5"]], [["102", "1"]]), ep="depth", sym="BTCUSDT", for_conn=1)   # for old conn 1
    L.close()
    r = normalize_vm(str(tmp_path), "binance-perp")
    assert "anchored" not in r["books"]
    assert read(tmp_path, "book", "binance-perp") == []


def test_binance_gap_breaks_chain_until_next_snapshot(tmp_path):
    L = Log(tmp_path, "binance-perp")
    L.instruments(BN_PERP_INFO)
    L.ws(1, depth("BTCUSDT", 100, 105, 99, b=[["101", "2"]]))
    L.rest(snap(100, [["100", "5"]], [["102", "1"]]), ep="depth", sym="BTCUSDT", for_conn=1)
    L.ws(1, depth("BTCUSDT", 110, 112, 108, b=[["90", "1"]]))          # pu 108 != 105: gap
    L.ws(1, depth("BTCUSDT", 113, 115, 112, b=[["91", "1"]]))          # buffered, not emitted
    L.rest(snap(114, [["95", "5"]], [["96", "1"]]), ep="depth", sym="BTCUSDT", for_conn=1, why="gap")
    L.close()
    r = normalize_vm(str(tmp_path), "binance-perp")
    assert r["books"]["chain_break"] == 1 and r["books"]["anchored"] == 2
    book = read(tmp_path, "book", "binance-perp")
    chains = sorted({x["chain"] for x in book})
    assert len(chains) == 2
    assert 90.0 not in [x["px"] for x in book]                         # the diff across the gap is dropped
    gaps = [c for c in read(tmp_path, "conn", "binance-perp") if c["event"] == "book_gap"]
    assert len(gaps) == 1


def test_units_and_unknown_symbol_quarantined(tmp_path):
    L = Log(tmp_path, "binance-perp")
    L.instruments(BN_PERP_INFO)
    L.ws(1, {"stream": "1000pepeusdt@trade", "data": {"e": "trade", "s": "1000PEPEUSDT", "t": 1, "p": "0.0042",
                                                      "q": "500", "T": 1, "m": False, "X": "MARKET"}})
    L.ws(1, {"stream": "newcoinusdt@trade", "data": {"e": "trade", "s": "NEWCOINUSDT", "t": 2, "p": "1",
                                                     "q": "100", "T": 1, "m": True, "X": "MARKET"}})
    L.close()
    r = normalize_vm(str(tmp_path), "binance-perp")
    trades = read(tmp_path, "trade", "binance-perp")
    assert len(trades) == 1
    t = trades[0]
    assert abs(t["px"] - 0.0000042) < 1e-15 and t["qty"] == 500_000.0      # per coin, in coins
    assert t["px_raw"] == "0.0042" and t["qty_raw"] == "500"
    q = read(tmp_path, "quarantine", "binance-perp")
    assert r["quarantined"] == 1 and q[0]["sym"] == "NEWCOINUSDT" and q[0]["reason"] == "no_metadata"


OKX_SWAP_INFO = {"data": [
    {"instId": "BTC-USDT-SWAP", "ctType": "linear", "settleCcy": "USDT", "state": "live", "ctVal": "0.01",
     "ctMult": "1", "uly": "BTC-USDT", "instCategory": "1"},
]}


def test_okx_contracts_to_coins_and_inverse_liquidation_out_of_scope(tmp_path):
    L = Log(tmp_path, "okx-perp")
    L.instruments(OKX_SWAP_INFO)
    L.ws(1, {"arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"},
             "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": "84000", "sz": "100", "side": "sell", "ts": "1"}]})
    L.ws(1, {"arg": {"channel": "liquidation-orders", "instType": "SWAP"},
             "data": [{"instId": "BTC-USD-SWAP", "details": [{"bkPx": "84000", "sz": "100", "posSide": "long",
                                                              "side": "sell", "ts": "1"}]}]})
    L.close()
    normalize_vm(str(tmp_path), "okx-perp")
    trades = read(tmp_path, "trade", "okx-perp")
    assert trades[0]["qty"] == 1.0 and trades[0]["side"] == -1          # 100 contracts x 0.01 BTC
    assert read(tmp_path, "liq", "okx-perp") == []
    assert read(tmp_path, "quarantine", "okx-perp")[0]["reason"] == "out_of_scope"


def test_book_state_carries_across_segments_and_runs_do_not_mix(tmp_path):
    info = {"result": {"list": [{"symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT",
                                 "contractType": "LinearPerpetual", "status": "Trading"}]}}

    def ob(u, typ, b):
        return {"topic": "orderbook.200.BTCUSDT", "type": typ, "ts": 1, "cts": 1,
                "data": {"s": "BTCUSDT", "b": b, "a": [], "u": u, "seq": u}}

    w = RawWriter(str(tmp_path), "bybit-perp", "20261012T000000Z-1", roll_bytes=1)   # every line its own segment
    t = T0
    for kind, d, extra in [("rest", info, {"ep": "instruments"}), ("ws", ob(1, "snapshot", [["100", "1"]]), None),
                           ("ws", ob(2, "delta", [["101", "1"]]), None)]:
        t += 1_000_000
        w.write(kind, 1, t, t, orjson.dumps(d).decode(), extra)
    w.close()
    # a later run reuses connection id 1 and sends a delta without a snapshot: must not continue run 1's chain
    w = RawWriter(str(tmp_path), "bybit-perp", "20261012T010000Z-2")
    w.write("ws", 1, t + 1_000_000, t + 1_000_000, orjson.dumps(ob(3, "delta", [["102", "1"]])).decode())
    w.close()
    normalize_vm(str(tmp_path), "bybit-perp")
    px = [x["px"] for x in read(tmp_path, "book", "bybit-perp")]
    assert px == [100.0, 101.0]


def test_okx_price_limit_disabled_is_a_state_not_an_error(tmp_path):
    L = Log(tmp_path, "okx-perp")
    L.instruments(OKX_SWAP_INFO)
    L.ws(1, {"arg": {"channel": "price-limit", "instId": "BTC-USDT-SWAP"},
             "data": [{"instId": "BTC-USDT-SWAP", "buyLmt": "", "sellLmt": "", "enabled": False, "ts": "1"}]})
    L.ws(1, {"arg": {"channel": "price-limit", "instId": "BTC-USDT-SWAP"},
             "data": [{"instId": "BTC-USDT-SWAP", "buyLmt": "84500", "sellLmt": "83500", "enabled": True, "ts": "2"}]})
    L.close()
    r = normalize_vm(str(tmp_path), "okx-perp")
    rows = read(tmp_path, "price_limit", "okx-perp")
    assert r["quarantined"] == 0 and not r["errors"]
    assert [(x["buy_max"], x["sell_min"], x["enabled"]) for x in rows] == [(None, None, False), (84500.0, 83500.0, True)]
