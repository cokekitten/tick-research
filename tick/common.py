"""Shared helpers: direct REST fetch with rate-limit detection, segmented raw append log."""
import fcntl
import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from compression import zstd

UA = "tick-research/0.3"
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
    _check_rate_body(url, body)
    return status, body, (t0w, t0m, time.time_ns(), time.monotonic_ns()), hdr


def _check_rate_body(url: str, body: bytes):
    """Venue rate-limit codes that arrive with HTTP 200 (Bybit retCode 10006, OKX code 50011)."""
    if not body or body[:1] != b"{":
        return
    try:
        d = json.loads(body)
    except ValueError:
        return
    if not isinstance(d, dict):
        return
    if d.get("retCode") == 10006 or str(d.get("code")) == "50011":
        raise RateLimited(f"rate-limit body {d.get('retCode') or d.get('code')} {url}")


def as_json_text(raw) -> str:
    """Keep the exchange payload verbatim when it is a single-line JSON value; else quote it."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    s = raw.strip()
    if s[:1] in "{[" and "\n" not in s:
        return s
    return json.dumps(raw, ensure_ascii=False)


def new_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + f"-{os.getpid()}"


class RawWriter:
    """Append-only zstd JSONL in segments: data/raw/<stream>/<run_id>-<seg>.jsonl.zst

    * Every process start opens new files (its own run_id); a segment is never reopened, so a crash
      can only damage the tail of the segment that was open at that moment.
    * Segments roll every `roll_s` seconds (aligned to the wall clock) or `roll_bytes` of raw JSON.
      The open segment is named `.partial`; it is renamed to `.jsonl.zst` once closed cleanly.
      A leftover `.partial` therefore means a crash; readers decode it up to the last complete frame.
    * A zstd frame is closed every `flush_s` seconds and the data handed to the OS. That bounds the loss
      on a process crash to ~flush_s; it is not fsync'd, so an OS crash or power loss can lose more.
    * An exclusive lock on data/raw/<stream>/.lock refuses a second writer for the same stream.

    Line: {"k":kind,"c":conn_id,"n":seq,"w":wall_ns,"m":mono_ns,<extra>,"d":<verbatim payload>}
      kind: ws (websocket message) | rest (REST response) | evt (collector event)
    """

    def __init__(self, root: str, name: str, run_id: str, level: int = 3, flush_s: float = 1.0,
                 roll_s: int = 900, roll_bytes: int = 2 << 30):
        self.dir = os.path.join(root, "raw", name)
        os.makedirs(self.dir, exist_ok=True)
        self._lock = open(os.path.join(self.dir, ".lock"), "w")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"another collector is already writing {self.dir}") from None
        self._lock.write(f"{socket.gethostname()} pid={os.getpid()} run={run_id}\n")
        self._lock.flush()
        self.run_id, self.level, self.flush_s = run_id, level, flush_s
        self.roll_s, self.roll_bytes = roll_s, roll_bytes
        self.f = None
        self.path = None
        self.seg = 0
        self.seg_bytes = 0
        self.seg_until = 0
        self.n = 0
        self.raw_bytes = 0
        self._last_flush = time.monotonic()

    def _open(self, wall_ns: int):
        self._seal()
        self.seg += 1
        base = os.path.join(self.dir, f"{self.run_id}-{self.seg:05d}")
        self.path = base + ".partial"
        self.f = zstd.ZstdFile(self.path, "w", level=self.level)
        self.seg_bytes = 0
        now_s = wall_ns // 1_000_000_000
        self.seg_until = (now_s // self.roll_s + 1) * self.roll_s

    def _seal(self):
        if not self.f:
            return
        self.f.close()
        os.replace(self.path, self.path[: -len(".partial")] + ".jsonl.zst")
        self.f = None

    def write(self, kind: str, conn: int, wall_ns: int, mono_ns: int, data, extra: dict | None = None):
        """Append one record. `data` is the payload as raw bytes (spliced in directly — the hot path
        does no decode/re-encode) or str (events, encoded here). Byte-identical to the old str path
        for valid UTF-8, which all exchange payloads are."""
        if isinstance(data, str):
            data = data.encode()
        if self.f is None or wall_ns // 1_000_000_000 >= self.seg_until or self.seg_bytes >= self.roll_bytes:
            self._open(wall_ns)
        self.n += 1
        ex = "".join(f',"{k}":{json.dumps(v)}' for k, v in (extra or {}).items())
        head = f'{{"k":"{kind}","c":{conn},"n":{self.n},"w":{wall_ns},"m":{mono_ns}{ex},"d":'.encode()
        line = head + data + b"}\n"
        self.raw_bytes += len(line)
        self.seg_bytes += len(line)
        self.f.write(line)
        now = time.monotonic()
        if now - self._last_flush >= self.flush_s:
            self.f.flush(zstd.ZstdFile.FLUSH_FRAME)
            self._last_flush = now

    def event(self, conn: int, payload: dict):
        self.write("evt", conn, time.time_ns(), time.monotonic_ns(), json.dumps(payload, ensure_ascii=False))

    def close(self):
        self._seal()
        self._lock.close()


def segment_files(root: str, name: str, include_partial: bool = False) -> list[str]:
    """Raw segments of one stream in write order (run_id sorts by start time, then segment number)."""
    d = os.path.join(root, "raw", name)
    if not os.path.isdir(d):
        return []
    names = [f for f in os.listdir(d) if f.endswith(".jsonl.zst") or (include_partial and f.endswith(".partial"))]
    return [os.path.join(d, f) for f in sorted(names)]


def read_lines(path: str):
    """Yield raw lines; a damaged tail (crash) stops the file, it never affects other segments.

    Returns via StopIteration value: (lines_read, error_or_None)."""
    n = 0
    try:
        with zstd.open(path, "rb") as fh:
            for line in fh:
                if line.endswith(b"\n"):
                    n += 1
                    yield line
    except (EOFError, zstd.ZstdError) as e:
        return n, f"{type(e).__name__}: {e}"
    return n, None
