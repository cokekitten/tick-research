"""Shared helpers: direct REST fetch with rate-limit detection, raw append-log writer."""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from compression import zstd

UA = "tick-research/0.2"
# Bypass the macOS system proxy: latency must be measured from our own vantage point.
_DIRECT = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class RateLimited(Exception):
    """418/429 (or a venue's equivalent). Every process sharing that venue's limit must stop."""


def http_get(url: str, timeout: float = 10.0):
    """Returns (status, body, (t0_wall_ns, t0_mono_ns, t1_wall_ns, t1_mono_ns), headers)."""
    # Some symbols are not ASCII (Binance lists e.g. 龙虾USDT); percent-encode, keep URL syntax.
    url = urllib.parse.quote(url, safe=":/?&=%-._~,[]")
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    t0w, t0m = time.time_ns(), time.monotonic_ns()
    try:
        with _DIRECT.open(req, timeout=timeout) as r:
            body, status, hdr = r.read(), r.status, {k.lower(): v for k, v in r.headers.items()}
    except urllib.error.HTTPError as e:
        if e.code in (418, 429):
            raise RateLimited(f"HTTP {e.code} {url}") from e
        if e.code == 403 and "bybit.com" in url:  # Bybit answers IP rate limiting with 403
            raise RateLimited(f"HTTP 403 {url}") from e
        raise
    if b'"retCode":10006' in body[:120] or b'"code":"50011"' in body[:120]:  # Bybit / OKX too many visits
        raise RateLimited(f"rate-limit body {url}")
    return status, body, (t0w, t0m, time.time_ns(), time.monotonic_ns()), hdr


def as_json_text(raw) -> str:
    """Keep the exchange payload verbatim when it is a single-line JSON value; else quote it."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    s = raw.strip()
    if s[:1] in "{[" and "\n" not in s:
        return s
    return json.dumps(raw, ensure_ascii=False)


class RawWriter:
    """Append-only zstd JSONL, one file per stream name per UTC hour.

    Line: {"k":kind,"c":conn_id,"n":seq,"w":wall_ns,"m":mono_ns,<extra>,"d":<verbatim payload>}
      kind: ws (websocket message) | rest (REST response) | evt (collector event)
    A zstd frame is closed every ~1s, so a crash loses at most ~1s and files stay readable.
    """

    def __init__(self, root: str, name: str, level: int = 3, flush_s: float = 1.0):
        self.dir = os.path.join(root, "raw", name)
        os.makedirs(self.dir, exist_ok=True)
        self.level, self.flush_s = level, flush_s
        self.f, self.hour = None, None
        self.n = 0
        self.raw_bytes = 0
        self._last_flush = time.monotonic()

    def _rotate(self, wall_ns: int):
        hour = time.strftime("%Y%m%d-%H", time.gmtime(wall_ns / 1e9))
        if hour != self.hour:
            if self.f:
                self.f.close()
            self.f = zstd.ZstdFile(os.path.join(self.dir, f"{hour}.jsonl.zst"), "a", level=self.level)
            self.hour = hour

    def write(self, kind: str, conn: int, wall_ns: int, mono_ns: int, data_text: str, extra: dict | None = None):
        self._rotate(wall_ns)
        self.n += 1
        ex = "".join(f',"{k}":{json.dumps(v)}' for k, v in (extra or {}).items())
        line = f'{{"k":"{kind}","c":{conn},"n":{self.n},"w":{wall_ns},"m":{mono_ns}{ex},"d":{data_text}}}\n'.encode()
        self.raw_bytes += len(line)
        self.f.write(line)
        now = time.monotonic()
        if now - self._last_flush >= self.flush_s:
            self.f.flush(zstd.ZstdFile.FLUSH_FRAME)
            self._last_flush = now

    def event(self, conn: int, payload: dict):
        self.write("evt", conn, time.time_ns(), time.monotonic_ns(), json.dumps(payload, ensure_ascii=False))

    def close(self):
        if self.f:
            self.f.close()
            self.f = None
