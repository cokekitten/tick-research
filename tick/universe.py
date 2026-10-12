"""Pick the coins listed as USDT linear perps on all of Binance / Bybit / OKX.

v0 matches by normalized base ticker only (1000PEPE -> PEPE). It does NOT verify that
the same ticker is the same asset on every exchange; the multipliers are recorded so
later stages can normalize prices/quantities.
"""
import re

from .common import http_json

_MULT = re.compile(r"^(1000000|100000|10000|1000|100)(?=[A-Z])")


def norm_base(base: str):
    m = _MULT.match(base)
    return (base[m.end():], int(m.group(1))) if m else (base, 1)


def build_universe(top: int):
    bi = http_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
    vol = {t["symbol"]: float(t["quoteVolume"]) for t in http_json("https://fapi.binance.com/fapi/v1/ticker/24hr")}
    binance = {}
    for s in bi["symbols"]:
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            b, mult = norm_base(s["baseAsset"])
            binance[b] = {"sym": s["symbol"], "mult": mult}

    bybit, cursor = {}, ""
    while True:
        url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
        r = http_json(url + (f"&cursor={cursor}" if cursor else ""))
        for s in r["result"]["list"]:
            if s.get("quoteCoin") == "USDT" and s.get("status") == "Trading" and s.get("contractType") == "LinearPerpetual":
                b, mult = norm_base(s["baseCoin"])
                bybit[b] = {"sym": s["symbol"], "mult": mult}
        cursor = r["result"].get("nextPageCursor") or ""
        if not cursor:
            break

    okx = {}
    for s in http_json("https://www.okx.com/api/v5/public/instruments?instType=SWAP")["data"]:
        if s.get("settleCcy") == "USDT" and s.get("state") == "live" and s["instId"].endswith("-USDT-SWAP"):
            b, mult = norm_base(s["instId"].split("-")[0])
            okx[b] = {"instId": s["instId"], "mult": mult, "ctVal": s.get("ctVal")}

    common = [b for b in binance if b in bybit and b in okx]
    common.sort(key=lambda b: vol.get(binance[b]["sym"], 0.0), reverse=True)
    uni = [
        {"base": b, "binance": binance[b], "bybit": bybit[b], "okx": okx[b],
         "binance_qv24h": vol.get(binance[b]["sym"], 0.0)}
        for b in common[:top]
    ]
    counts = {"binance": len(binance), "bybit": len(bybit), "okx": len(okx), "common": len(common)}
    return uni, counts
