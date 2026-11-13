"""Launch one collector process per venue x market and supervise them.

    python -m tick.run --duration 300            # all six, 5 minutes
    python -m tick.run --only binance-perp,okx-spot --limit 20

Each child logs to data/logs/<venue>-<market>.log. A child that exits because of a rate limit
(exit code 2 + data/meta/STOP-<venue>) makes the other collectors of that venue stop by themselves
when they see the stop file; the venue is then retried with exponential backoff (15 min, 30 min,
... up to 4 h) and the stop file is cleared right before each retry. A run that stays up for
RATE_RESET_S clears the backoff. data/meta/STOP-all is an operator flag: it is never cleared and
never retried. Other crashes are restarted with exponential backoff (5s, 10s, ... up to 5 min), at
most --max-restarts times per hour.
"""
import argparse
import os
import signal
import subprocess
import sys
import time

# binance-perp is split into shards: its stream is the largest and one process cannot keep up
# with its receive timestamps (event-loop lag grew past 200ms at ~18k msg/s).
ALL = ["binance-perp-a", "binance-perp-b", "binance-perp-c",
       "binance-spot", "bybit-perp", "bybit-spot", "okx-perp", "okx-spot"]
SHARDS = {"binance-perp": 3}

# A venue-wide stop is usually an IP-level block that clears on its own (Bybit answers its IP
# limiting with HTTP 403). Before 2026-11-11 the stop was final: bybit lost 2.5 days of data after
# one 403 until someone deleted the file by hand. Retry instead, but never hammer a blocked venue.
RATE_BASE_S = 15 * 60
RATE_MAX_S = 4 * 3600
RATE_RESET_S = 6 * 3600


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", default=",".join(ALL))
    ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("--root", default="data")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--no-bbo", action="store_true")
    ap.add_argument("--no-raw-ws", action="store_true",
                    help="fall back to the websockets library (timestamps get delayed under load)")
    ap.add_argument("--max-restarts", type=int, default=6, help="per collector per hour before giving up")
    a = ap.parse_args()

    jobs = [j.strip() for j in a.only.split(",") if j.strip()]
    os.makedirs(os.path.join(a.root, "logs"), exist_ok=True)
    procs: dict[str, subprocess.Popen] = {}
    done: set = set()
    stopping = False
    started_at: dict[str, float] = {}
    rate_tries: dict[str, int] = {}
    meta = os.path.join(a.root, "meta")

    def start(job):
        parts = job.split("-")
        venue, market = parts[0], parts[1]
        tag = parts[2] if len(parts) > 2 else ""
        cmd = [sys.executable, "-m", "tick.collect", "--venue", venue, "--market", market, "--root", a.root]
        if tag:
            n = SHARDS[f"{venue}-{market}"]
            cmd += ["--tag", tag, "--mod", str(n), "--rem", str("abc".index(tag))]
        if a.duration:
            cmd += ["--duration", str(a.duration)]
        if a.limit:
            cmd += ["--limit", str(a.limit)]
        if a.no_bbo:
            cmd.append("--no-bbo")
        if not a.no_raw_ws:
            cmd.append("--raw-ws")   # kernel packet-arrival timestamps (SO_TIMESTAMPNS)
        log = open(os.path.join(a.root, "logs", f"{job}.log"), "a")
        procs[job] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        started_at[job] = time.monotonic()
        print(f"started {job} pid={procs[job].pid}", flush=True)

    def venue_of(job):
        return job.split("-")[0]

    def clear_venue_stop(venue):
        """Delete the auto-written stop file so a scheduled retry can start at all."""
        path = os.path.join(meta, f"STOP-{venue}")
        if os.path.exists(path):
            os.remove(path)
            print(f"cleared {path} for the scheduled retry", flush=True)

    def stop(*_):
        nonlocal stopping
        stopping = True
        for p in procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    for i, j in enumerate(jobs):
        start(j)
        time.sleep(2)  # spread startup REST bursts

    restart_at: dict[str, float] = {}
    recent: dict[str, list] = {j: [] for j in jobs}   # monotonic times of restarts, last hour
    while len(done) < len(jobs):
        time.sleep(1)
        now = time.monotonic()
        for job, p in list(procs.items()):
            if job in done:
                continue
            if stopping or a.duration:
                if p.poll() is not None:
                    print(f"{job} exited code={p.returncode}", flush=True)
                    done.add(job)
                continue
            if job in restart_at:          # checked before poll(): an exited child may still have a retry pending
                if now < restart_at[job]:
                    continue
                del restart_at[job]
                recent[job].append(now)
                if p.returncode in (2, 3):
                    clear_venue_stop(venue_of(job))
                start(job)
                continue
            if p.poll() is None:
                continue
            code = p.returncode
            if code in (0, 2, 3):
                venue = venue_of(job)
                if os.path.exists(os.path.join(meta, "STOP-all")):
                    print(f"{job} exited code={code}; STOP-all is set, not retrying", flush=True)
                    done.add(job)
                    continue
                if code == 0:
                    print(f"{job} exited cleanly", flush=True)
                    done.add(job)
                    continue
                if now - started_at.get(job, now) >= RATE_RESET_S:
                    rate_tries[venue] = 0     # last run was long enough to prove the block cleared
                tries = rate_tries.get(venue, 0)
                delay = min(RATE_BASE_S * 2 ** tries, RATE_MAX_S)
                rate_tries[venue] = tries + 1
                print(f"{job} exited code={code} (rate limit / stop file); "
                      f"{venue} retry #{tries + 1} in {delay}s", flush=True)
                restart_at[job] = now + delay
                continue
            if job not in restart_at:
                recent[job] = [t for t in recent[job] if now - t < 3600]
                if len(recent[job]) >= a.max_restarts:
                    print(f"{job} crashed code={code}; {len(recent[job])} restarts in the last hour, giving up",
                          flush=True)
                    done.add(job)
                    continue
                delay = min(5 * 2 ** len(recent[job]), 300)
                print(f"{job} crashed code={code}; restart in {delay}s", flush=True)
                restart_at[job] = now + delay
    print("all collectors stopped", flush=True)


if __name__ == "__main__":
    main()
