"""Minimal WebSocket client that timestamps every message with the kernel's packet arrival time.

Why: the `websockets` library reads the socket with recv(), which discards ancillary data, so a message
can only be timestamped when the event loop happens to run. Under CPU pressure (8 collectors on 4
cores) that is hundreds of ms — up to 2.5s — after the bytes actually arrived.

Here we own the socket:
  * SO_TIMESTAMPNS makes the kernel stamp every packet on receipt (softirq context, independent of
    whether our process is scheduled), and recvmsg() hands that timestamp back.
  * TLS is driven manually through MemoryBIO so the *ciphertext* is read with recvmsg.
  * WebSocket frames are parsed here (server-to-client frames are unmasked, so it is cheap).
All frames completed by one read share that read's kernel timestamp — they arrived in the same packet.

    python -m tick.wsraw --path '/stream?streams=btcusdt@depth@100ms' --seconds 10
"""
import argparse
import asyncio
import base64
import collections
import os
import select
import socket
import ssl
import struct
import threading
import time

SOL_SOCKET = 1
SO_TIMESTAMPNS = 35  # Linux; not exposed as a constant by the socket module
SCM_TIMESTAMPNS = 35

OP_CONT, OP_TEXT, OP_BIN, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA


def split_frames(buf: bytes):
    """Return ([ (opcode, payload, total_len) ... ], remainder). Server frames are unmasked."""
    out, i, n = [], 0, len(buf)
    while n - i >= 2:
        b0, b1 = buf[i], buf[i + 1]
        opcode, ln = b0 & 0x0F, b1 & 0x7F
        j = i + 2
        if ln == 126:
            if n - j < 2:
                break
            ln = struct.unpack_from(">H", buf, j)[0]
            j += 2
        elif ln == 127:
            if n - j < 8:
                break
            ln = struct.unpack_from(">Q", buf, j)[0]
            j += 8
        if b1 & 0x80:  # masked: server frames should not be, but skip the key if they are
            if n - j < 4:
                break
            j += 4
        if n - j < ln:
            break
        out.append((opcode, buf[j:j + ln], (j + ln) - i))
        i = j + ln
    return out, buf[i:]


class RawWs:
    def __init__(self, host: str, path: str, port: int = 443, timeout: float = 15.0):
        self.host, self.path, self.port = host, path, port
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.kernel_ts = True
        try:
            self.sock.setsockopt(SOL_SOCKET, SO_TIMESTAMPNS, 1)
        except OSError:
            self.kernel_ts = False
        self.sock.setblocking(False)
        self.inbio, self.outbio = ssl.MemoryBIO(), ssl.MemoryBIO()
        ctx = ssl.create_default_context()
        ctx.check_hostname, ctx.verify_mode = True, ssl.CERT_REQUIRED
        self.ssl = ctx.wrap_bio(self.inbio, self.outbio, server_hostname=host)
        self.buf = b""
        self.last_ts = None

    # ---------------- transport ----------------
    def _pump_out(self):
        while self.outbio.pending:
            self.sock.sendall(self.outbio.read())

    def _recv_cipher(self):
        try:
            data, anc, _fl, _ad = self.sock.recvmsg(65536, 256)
        except BlockingIOError:
            return b"", None
        ts = None
        for level, typ, cdata in anc:
            if typ in (SCM_TIMESTAMPNS, 36) and len(cdata) >= 16:
                sec, nsec = struct.unpack("qq", cdata[:16])
                ts = sec * 1_000_000_000 + nsec

        return data, ts

    def _decrypt_into_buf(self, data: bytes):
        self.inbio.write(data)
        try:
            while True:
                plain = self.ssl.read()
                if not plain:
                    break
                self.buf += plain
        except ssl.SSLWantReadError:
            pass

    def handshake(self):
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET {self.path} HTTP/1.1\r\nHost: {self.host}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
        deadline = time.monotonic() + 20
        while True:  # TLS
            try:
                self.ssl.do_handshake()
                break
            except ssl.SSLWantReadError:
                self._pump_out()
                if time.monotonic() > deadline:
                    raise TimeoutError("tls handshake")
                select.select([self.sock], [], [], 1.0)
                data, _ = self._recv_cipher()
                if data:
                    self.inbio.write(data)
        self._pump_out()
        self.ssl.write(req.encode())
        self._pump_out()
        while b"\r\n\r\n" not in self.buf:  # HTTP 101
            if time.monotonic() > deadline:
                raise TimeoutError("ws handshake")
            select.select([self.sock], [], [], 1.0)
            data, _ = self._recv_cipher()
            if data:
                self._decrypt_into_buf(data)
        head, self.buf = self.buf.split(b"\r\n\r\n", 1)
        if b" 101 " not in head.split(b"\r\n")[0]:
            raise RuntimeError(f"upgrade failed: {head.split(chr(13).encode())[0][:120]!r}")

    def _frame(self, opcode: int, payload: bytes):
        header = bytearray([0x80 | opcode])
        ln = len(payload)
        if ln < 126:
            header.append(0x80 | ln)
        elif ln < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", ln)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", ln)
        mask = os.urandom(4)
        header += mask
        masked = bytearray(payload)
        for i in range(len(masked)):
            masked[i] ^= mask[i & 3]
        self.ssl.write(bytes(header) + bytes(masked))
        self._pump_out()

    def send_text(self, text: str):
        self._frame(OP_TEXT, text.encode())

    # ---------------- messages ----------------
    def messages(self, idle_timeout: float = 60.0):
        """Yield (payload_bytes, kernel_arrival_ns|None, dequeue_ns)."""
        while True:
            frames, self.buf = split_frames(self.buf)
            if not frames:
                r, _, _ = select.select([self.sock], [], [], idle_timeout)
                if not r:
                    raise TimeoutError("idle")
                data, ts = self._recv_cipher()
                if not data:
                    raise ConnectionError("eof")
                if ts:
                    self.last_ts = ts
                self._decrypt_into_buf(data)
                continue
            now = time.time_ns()
            for opcode, payload, _ln in frames:
                if opcode in (OP_TEXT, OP_BIN):
                    yield payload, self.last_ts, now
                elif opcode == OP_PING:
                    self._frame(OP_PONG, payload)
                elif opcode == OP_CLOSE:
                    raise ConnectionError("server closed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="fstream.binance.com")
    ap.add_argument("--port", type=int, default=443)
    ap.add_argument("--path", default="/stream?streams=btcusdt@depth@100ms/ethusdt@trade")
    ap.add_argument("--seconds", type=float, default=10)
    a = ap.parse_args()
    ws = RawWs(a.host, a.path, a.port)
    ws.handshake()
    print(f"握手完成 kernel_ts={ws.kernel_ts}")
    t0, n, lags = time.monotonic(), 0, []
    try:
        for payload, kts, dts in ws.messages(idle_timeout=5):
            n += 1
            if kts:
                lags.append((dts - kts) / 1e6)
            if time.monotonic() - t0 > a.seconds:
                break
    except (TimeoutError, ConnectionError) as e:
        print("结束:", e)
    lags.sort()
    print(f"收到 {n} 条消息")
    if lags:
        print("取出时刻-内核收包时刻: 中位 %.3fms 90分位 %.3fms 最大 %.3fms"
              % (lags[len(lags) // 2], lags[int(len(lags) * 0.9)], lags[-1]))


if __name__ == "__main__":
    main()


class RawWsConn:
    """Blocking kernel-timestamped connection driven from a thread, consumed by asyncio.

    The reader thread only receives, stamps (kernel arrival time) and queues; the event loop writes.
    A slow writer therefore no longer distorts the timestamps — which is the whole point: under CPU
    pressure the old path stamped messages up to 2.5s late.
    """

    def __init__(self, loop, host, path, port=443, queue_warn=200_000):
        self.loop = loop
        self.host, self.path, self.port = host, path, port
        self.queue_warn = queue_warn
        self.inbox: collections.deque = collections.deque()
        self.outbox: collections.deque = collections.deque()
        self.wake = asyncio.Event()
        self.closed = False
        self.error: Exception | None = None
        self.close_code = None
        self.close_reason = None
        self.thread: threading.Thread | None = None
        self.ws: RawWs | None = None
        self.msgs = 0
        self.warned = False

    def start(self, timeout=25.0):
        self.thread = threading.Thread(target=self._run, name=f"rawws-{self.host}", daemon=True)
        self.thread.start()
        # handshake happens in the thread; wait for it (or the error) before returning
        t0 = time.monotonic()
        while self.ws is None and self.error is None and not self.closed:
            if time.monotonic() - t0 > timeout:
                self.error = TimeoutError("handshake")
                break
            time.sleep(0.005)
        if self.error:
            raise self.error

    def _run(self):
        try:
            self.ws = RawWs(self.host, self.path, self.port)
            self.ws.handshake()
            self._loop()
        except Exception as e:  # noqa: BLE001 - surfaced to the event loop
            self.error = e
        finally:
            self.closed = True
            self._notify()

    def _notify(self):
        try:
            self.loop.call_soon_threadsafe(self.wake.set)
        except RuntimeError:
            pass

    def _loop(self):
        sock = self.ws.sock
        buf = b""
        while not self.closed:
            while self.outbox:  # subscriptions, pings, pongs
                self.ws.send_text(self.outbox.popleft())
            r, _, _ = select.select([sock], [], [], 0.5)
            if not r:
                continue
            data, ts = self.ws._recv_cipher()
            if not data:
                self.close_reason = "eof"
                return
            if ts:
                self.ws.last_ts = ts
            self.ws._decrypt_into_buf(data)
            frames, self.ws.buf = split_frames(self.ws.buf)
            now = None
            for opcode, payload, _ln in frames:
                if opcode in (OP_TEXT, OP_BIN):
                    if now is None:
                        now = time.time_ns()
                    self.inbox.append((payload, self.ws.last_ts, now))
                    self.msgs += 1
                elif opcode == OP_PING:
                    self.ws._frame(OP_PONG, payload)
                elif opcode == OP_CLOSE:
                    self.close_reason = "server closed"
                    return
            if len(self.inbox) > self.queue_warn and not self.warned:
                self.warned = True
                self.inbox.append((f"WARN: backlog {len(self.inbox)}".encode(), None, None))
            self._notify()

    async def send(self, text: str):
        self.outbox.append(text)

    async def drain(self):
        """Yield (payload, kernel_ts, dequeue_ts) as they become available."""
        while True:
            while self.inbox:
                yield self.inbox.popleft()
            if self.closed:
                if self.error:
                    raise self.error
                raise ConnectionError(self.close_reason or "closed")
            self.wake.clear()
            await self.wake.wait()

    def close(self):
        self.closed = True
