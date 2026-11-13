"""Supervisor restart policy and verify exit codes."""
import subprocess
import sys


def test_verify_without_data_is_not_a_pass(tmp_path):
    r = subprocess.run([sys.executable, "-m", "tick.verify", "--root", str(tmp_path)], capture_output=True, text=True)
    assert r.returncode == 2 and "NO DATA" in r.stdout


def test_supervisor_restarts_crashed_collector(tmp_path, monkeypatch):
    """Crashes (exit 1) back off in seconds; a rate-limit exit (2) waits RATE_BASE_S, then retries."""
    import tick.run as run

    starts = []

    class FakeProc:
        def __init__(self, code):
            self.code, self.pid, self.returncode = code, len(starts), None

        def poll(self):
            self.returncode = self.code
            return self.code

        def send_signal(self, _):
            pass

    codes = {"binance-perp": [1, 1, 0], "okx-perp": [2, 0]}

    def fake_popen(cmd, **_):
        job = f"{cmd[cmd.index('--venue') + 1]}-{cmd[cmd.index('--market') + 1]}"
        starts.append(job)
        return FakeProc(codes[job].pop(0))

    clock = [0.0]
    (tmp_path / "meta").mkdir()
    stop = tmp_path / "meta" / "STOP-okx"
    stop.write_text("2026-11-11T06:14:14Z okx-spot: HTTP 403\n")
    monkeypatch.setattr(run.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    monkeypatch.setattr(run.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(sys, "argv", ["run", "--only", "binance-perp,okx-perp", "--root", str(tmp_path)])
    run.main()
    assert starts.count("binance-perp") == 3      # crashed twice, restarted twice, then finished
    assert starts.count("okx-perp") == 2          # rate-limited once, retried once, then finished
    assert clock[0] >= 5 + 10                     # crash backoff was honoured
    assert clock[0] >= run.RATE_BASE_S            # the rate-limit retry waited the full base delay
    assert not stop.exists()                      # a due retry clears the venue stop file


def test_stop_all_blocks_the_rate_limit_retry(tmp_path, monkeypatch):
    """STOP-all is an operator flag: it must never be cleared or retried."""
    import tick.run as run

    starts = []

    class FakeProc:
        def __init__(self, code):
            self.code, self.pid, self.returncode = code, len(starts), None

        def poll(self):
            self.returncode = self.code
            return self.code

        def send_signal(self, _):
            pass

    def fake_popen(cmd, **_):
        job = f"{cmd[cmd.index('--venue') + 1]}-{cmd[cmd.index('--market') + 1]}"
        starts.append(job)
        return FakeProc(2)

    clock = [0.0]
    (tmp_path / "meta").mkdir()
    (tmp_path / "meta" / "STOP-all").write_text("manual")
    monkeypatch.setattr(run.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    monkeypatch.setattr(run.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(sys, "argv", ["run", "--only", "okx-perp", "--root", str(tmp_path)])
    run.main()
    assert starts.count("okx-perp") == 1          # not retried
    assert (tmp_path / "meta" / "STOP-all").exists()
