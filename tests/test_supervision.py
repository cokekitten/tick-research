"""Supervisor restart policy and verify exit codes."""
import subprocess
import sys


def test_verify_without_data_is_not_a_pass(tmp_path):
    r = subprocess.run([sys.executable, "-m", "tick.verify", "--root", str(tmp_path)], capture_output=True, text=True)
    assert r.returncode == 2 and "NO DATA" in r.stdout


def test_supervisor_restarts_crashed_collector(tmp_path, monkeypatch):
    """A collector that crashes (exit 1) is restarted after the backoff; exit 2 is never restarted."""
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

    codes = {"binance-perp": [1, 1, 0], "okx-perp": [2]}

    def fake_popen(cmd, **_):
        job = f"{cmd[cmd.index('--venue') + 1]}-{cmd[cmd.index('--market') + 1]}"
        starts.append(job)
        return FakeProc(codes[job].pop(0))

    clock = [0.0]
    monkeypatch.setattr(run.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(run.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    monkeypatch.setattr(run.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(sys, "argv", ["run", "--only", "binance-perp,okx-perp", "--root", str(tmp_path)])
    run.main()
    assert starts.count("binance-perp") == 3      # crashed twice, restarted twice, then finished
    assert starts.count("okx-perp") == 1          # rate-limit exit: not restarted
    assert clock[0] >= 30 + 60                    # exponential backoff was honoured
