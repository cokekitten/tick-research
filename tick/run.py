"""Launch one collector process per venue x market and supervise them.

    python -m tick.run --duration 300            # all six, 5 minutes
    python -m tick.run --only binance-perp,okx-spot --limit 20

Each child logs to data/logs/<venue>-<market>.log. A child that exits because of a rate limit
(exit code 2 + data/meta/STOP-<venue>) is not restarted, and the other collectors of that venue
stop by themselves when they see the stop file. Other crashes are restarted after 30s.
"""
import argparse
import os
import signal
import subprocess
import sys
import time

ALL = ["binance-perp", "binance-spot", "bybit-perp", "bybit-spot", "okx-perp", "okx-spot"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", default=",".join(ALL))
    ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("--root", default="data")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--no-bbo", action="store_true")
    a = ap.parse_args()

    jobs = [j.strip() for j in a.only.split(",") if j.strip()]
    os.makedirs(os.path.join(a.root, "logs"), exist_ok=True)
    procs: dict[str, subprocess.Popen] = {}
    done: set = set()
    stopping = False

    def start(job):
        venue, market = job.split("-")
        cmd = [sys.executable, "-m", "tick.collect", "--venue", venue, "--market", market, "--root", a.root]
        if a.duration:
            cmd += ["--duration", str(a.duration)]
        if a.limit:
            cmd += ["--limit", str(a.limit)]
        if a.no_bbo:
            cmd.append("--no-bbo")
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
    while len(done) < len(jobs):
        time.sleep(1)
        for job, p in list(procs.items()):
            if job in done or p.poll() is None:
                continue
            code = p.returncode
            if stopping or a.duration or code in (0, 2, 3):
                print(f"{job} exited code={code}", flush=True)
                done.add(job)
                continue
            if job not in restart_at:
                print(f"{job} crashed code={code}; restart in 30s", flush=True)
                restart_at[job] = time.time() + 30
            elif time.time() >= restart_at.pop(job):
                start(job)
    print("all collectors stopped", flush=True)


if __name__ == "__main__":
    main()
