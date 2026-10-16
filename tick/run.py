"""Launch one collector process per venue x market and supervise them.

    python -m tick.run --duration 300            # all six, 5 minutes
    python -m tick.run --only binance-perp,okx-spot --limit 20

Each child logs to data/logs/<venue>-<market>.log. A child that exits because of a rate limit
(exit code 2 + data/meta/STOP-<venue>) is not restarted, and the other collectors of that venue
stop by themselves when they see the stop file. Other crashes are restarted with exponential backoff
(5s, 10s, ... up to 5 min), at most --max-restarts times per hour.
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
        print(f"started {job} pid={procs[job].pid}", flush=True)

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
            if job in done or p.poll() is None:
                continue
            code = p.returncode
            # 0 = finished, 2 = rate limit / disk / stop file, 3 = refused to start: never restart
            if stopping or a.duration or code in (0, 2, 3):
                print(f"{job} exited code={code}", flush=True)
                done.add(job)
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
            elif now >= restart_at[job]:
                del restart_at[job]
                recent[job].append(now)
                start(job)
    print("all collectors stopped", flush=True)


if __name__ == "__main__":
    main()
