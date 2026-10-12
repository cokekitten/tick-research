"""Instrument lists for every venue x market, parsed into one common record.

The same parse functions are used live (collector) and offline (normalizer, from the logged
REST bodies), so units are always derived from the metadata that was current at the time.

Record fields
  venue, market      binance|bybit|okx, perp|spot
  sym                exchange symbol / instId
  base_raw, base     exchange base ticker; base with denomination prefix removed (1000PEPE -> PEPE)
  mult               coins per quoted unit from the prefix (1000PEPE -> 1000); price / mult = per coin
  ct_val             coins per contract for venues that quote size in contracts (OKX SWAP), else 1
  quote, settle      quote currency, settlement currency
  ctype, status      contract type / market status as reported by the exchange
  tick, lot          price tick and size step as strings, in the exchange's own units
  listed_ms          listing time if the exchange publishes it
  fund_int_h         funding interval hours if published in instrument metadata
  index_id           OKX index id (uly) used by index-tickers, else None
  asset              crypto | tradfi (stocks, ETFs, commodities, FX, indexes listed as perps)
  asset_raw          the exchange's own class field (Binance underlyingType, Bybit symbolType,
                     OKX instCategory), kept for audit
  coin_id            cross-venue join key: base for crypto, f"{base}@tradfi" otherwise. Tickers collide
                     (Binance CATUSDT is Caterpillar stock, 1000CATUSDT is the CAT memecoin), so joins
                     across venues must use coin_id, never the raw ticker.
Normalized units everywhere downstream: px = quote per 1 coin, qty = coins (= qty * mult * ct_val).
"""
import re

import orjson

from .common import http_get

# Only USDT-quoted spot and USDT-margined linear perps are captured (one quote currency, no
# USDT/USDC basis to model). Widen this tuple to add other stablecoin quotes.
STABLES = ("USDT",)
_MULT = re.compile(r"^(1000000|100000|10000|1000|100|1M)(?=[A-Z])")


def norm_base(raw: str):
    m = _MULT.match(raw)
    if not m:
        return raw, 1
    g = m.group(1)
    return raw[m.end():], 1_000_000 if g == "1M" else int(g)


def _flt(filters, ftype, key):
    for f in filters or ():
        if f.get("filterType") == ftype:
            return f.get(key)
    return None


def _rec(**kw):
    base_raw = kw["base_raw"]
    base, mult = norm_base(base_raw) if kw.pop("prefix", True) else (base_raw, 1)
    out = dict(venue=None, market=None, sym=None, base_raw=base_raw, base=base, mult=mult, ct_val=1.0,
               quote=None, settle=None, ctype=None, status=None, tick=None, lot=None, listed_ms=None,
               fund_int_h=None, index_id=None, asset="crypto", asset_raw=None)
    out.update(kw)
    out["base"], out["mult"] = base, mult
    out["coin_id"] = base if out["asset"] == "crypto" else f"{base}@tradfi"
    return out


# Exchange class fields that mean "not a crypto asset" (verified against live metadata 2026-10-12).
# Binance PREMARKET contracts are pre-launch tokens without a spot reference; kept apart as well.
_BN_CRYPTO = {"COIN", None}
_BY_TRADFI = {"stock", "ETF", "commodity", "forex", "index"}
_OKX_CRYPTO = {"1", None, ""}  # instCategory 1 = crypto; 3 = stocks, 4 = commodities


# ---------------------------------------------------------------- parse (body -> records)
def parse_binance_perp(d):
    out = []
    for s in d.get("symbols", []):
        if s.get("contractType") not in ("PERPETUAL", "TRADIFI_PERPETUAL"):
            continue
        if s.get("quoteAsset") not in STABLES or s.get("status") not in ("TRADING", "PENDING_TRADING"):
            continue
        ut = s.get("underlyingType")
        tradfi = s["contractType"] == "TRADIFI_PERPETUAL" or ut not in _BN_CRYPTO
        out.append(_rec(venue="binance", market="perp", sym=s["symbol"], base_raw=s["baseAsset"],
                        asset="tradfi" if tradfi else "crypto", asset_raw=ut,
                        quote=s["quoteAsset"], settle=s.get("marginAsset"), ctype=s["contractType"],
                        status=s["status"], tick=_flt(s.get("filters"), "PRICE_FILTER", "tickSize"),
                        lot=_flt(s.get("filters"), "LOT_SIZE", "stepSize"), listed_ms=s.get("onboardDate")))
    return out


def parse_binance_spot(d):
    out = []
    for s in d.get("symbols", []):
        if s.get("quoteAsset") not in STABLES or s.get("status") not in ("TRADING", "PRE_TRADING"):
            continue
        if "SPOT" not in (s.get("permissions") or ["SPOT"]) and not s.get("isSpotTradingAllowed", True):
            continue
        out.append(_rec(venue="binance", market="spot", sym=s["symbol"], base_raw=s["baseAsset"],
                        quote=s["quoteAsset"], settle=s["quoteAsset"], ctype="SPOT", status=s["status"],
                        tick=_flt(s.get("filters"), "PRICE_FILTER", "tickSize"),
                        lot=_flt(s.get("filters"), "LOT_SIZE", "stepSize")))
    return out


def parse_bybit_perp(d):
    out = []
    for s in d.get("result", {}).get("list", []):
        if s.get("contractType") != "LinearPerpetual" or s.get("settleCoin") not in STABLES:
            continue
        if s.get("status") not in ("Trading", "PreLaunch"):
            continue
        fi = s.get("fundingInterval")
        st = s.get("symbolType") or None
        out.append(_rec(venue="bybit", market="perp", sym=s["symbol"], base_raw=s["baseCoin"],
                        asset="tradfi" if st in _BY_TRADFI else "crypto", asset_raw=st,
                        quote=s.get("quoteCoin"), settle=s.get("settleCoin"), ctype=s["contractType"],
                        status=s["status"], tick=(s.get("priceFilter") or {}).get("tickSize"),
                        lot=(s.get("lotSizeFilter") or {}).get("qtyStep"),
                        listed_ms=int(s["launchTime"]) if s.get("launchTime") else None,
                        fund_int_h=int(fi) / 60 if fi else None))
    return out


def parse_bybit_spot(d):
    out = []
    for s in d.get("result", {}).get("list", []):
        if s.get("quoteCoin") not in STABLES or s.get("status") != "Trading":
            continue
        st = s.get("symbolType") or None
        out.append(_rec(venue="bybit", market="spot", sym=s["symbol"], base_raw=s["baseCoin"],
                        asset="tradfi" if st in _BY_TRADFI else "crypto", asset_raw=st,
                        quote=s["quoteCoin"], settle=s["quoteCoin"], ctype="SPOT", status=s["status"],
                        tick=(s.get("priceFilter") or {}).get("tickSize"),
                        lot=(s.get("lotSizeFilter") or {}).get("basePrecision")))
    return out


def parse_okx_perp(d):
    out = []
    for s in d.get("data", []):
        if s.get("ctType") != "linear" or s.get("settleCcy") not in STABLES:
            continue
        if s.get("state") not in ("live", "preopen"):
            continue
        base_raw, quote = s["instId"].split("-")[:2]
        ct = float(s.get("ctVal") or 1) * float(s.get("ctMult") or 1)
        cat = s.get("instCategory")
        out.append(_rec(venue="okx", market="perp", sym=s["instId"], base_raw=base_raw, prefix=False, ct_val=ct,
                        asset="crypto" if cat in _OKX_CRYPTO else "tradfi", asset_raw=cat,
                        quote=quote, settle=s["settleCcy"], ctype="SWAP", status=s["state"],
                        tick=s.get("tickSz"), lot=s.get("lotSz"),
                        listed_ms=int(s["listTime"]) if s.get("listTime") else None,
                        index_id=s.get("uly") or s.get("instFamily")))
    return out


def parse_okx_spot(d):
    out = []
    for s in d.get("data", []):
        if s.get("quoteCcy") not in STABLES or s.get("state") not in ("live", "preopen"):
            continue
        cat = s.get("instCategory")
        out.append(_rec(venue="okx", market="spot", sym=s["instId"], base_raw=s["baseCcy"], prefix=False,
                        asset="crypto" if cat in _OKX_CRYPTO else "tradfi", asset_raw=cat,
                        quote=s["quoteCcy"], settle=s["quoteCcy"], ctype="SPOT", status=s["state"],
                        tick=s.get("tickSz"), lot=s.get("lotSz"),
                        listed_ms=int(s["listTime"]) if s.get("listTime") else None))
    return out


PARSERS = {
    ("binance", "perp"): parse_binance_perp, ("binance", "spot"): parse_binance_spot,
    ("bybit", "perp"): parse_bybit_perp, ("bybit", "spot"): parse_bybit_spot,
    ("okx", "perp"): parse_okx_perp, ("okx", "spot"): parse_okx_spot,
}

INSTRUMENT_URLS = {
    ("binance", "perp"): ["https://fapi.binance.com/fapi/v1/exchangeInfo"],
    ("binance", "spot"): ["https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"],
    ("bybit", "perp"): ["https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"],
    ("bybit", "spot"): ["https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000"],
    ("okx", "perp"): ["https://www.okx.com/api/v5/public/instruments?instType=SWAP"],
    ("okx", "spot"): ["https://www.okx.com/api/v5/public/instruments?instType=SPOT"],
}


# ---------------------------------------------------------------- fetch (live)
def fetch_instruments(venue: str, market: str):
    """Returns (records, pages) where pages = [(url, body_bytes, (t0w, t0m, t1w, t1m))]."""
    parse = PARSERS[(venue, market)]
    recs, pages = [], []
    for url in INSTRUMENT_URLS[(venue, market)]:
        cursor = ""
        while True:
            u = url + (f"&cursor={cursor}" if cursor else "")
            _, body, ts, _ = http_get(u, 20)
            d = orjson.loads(body)
            pages.append((u, body, ts))
            recs += parse(d)
            cursor = (d.get("result") or {}).get("nextPageCursor") or "" if venue == "bybit" else ""
            if not cursor:
                break
    return recs, pages


def fetch_volumes(venue: str, market: str) -> dict:
    """24h quote volume per symbol; only used to order subscriptions and snapshot priority."""
    if venue == "binance":
        url = ("https://fapi.binance.com/fapi/v1/ticker/24hr" if market == "perp"
               else "https://api.binance.com/api/v3/ticker/24hr?type=MINI")
        return {t["symbol"]: float(t.get("quoteVolume") or 0) for t in orjson.loads(http_get(url, 20)[1])}
    if venue == "bybit":
        cat = "linear" if market == "perp" else "spot"
        d = orjson.loads(http_get(f"https://api.bybit.com/v5/market/tickers?category={cat}", 20)[1])
        return {t["symbol"]: float(t.get("turnover24h") or 0) for t in d["result"]["list"]}
    typ = "SWAP" if market == "perp" else "SPOT"
    d = orjson.loads(http_get(f"https://www.okx.com/api/v5/market/tickers?instType={typ}", 20)[1])
    out = {}
    for t in d["data"]:
        v = float(t.get("volCcy24h") or 0)
        out[t["instId"]] = v * float(t.get("last") or 0) if typ == "SWAP" else v
    return out
