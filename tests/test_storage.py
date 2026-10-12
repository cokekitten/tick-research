"""Raw segment writer: crash safety, rolling, single writer."""
import os

import pytest
from compression import zstd

from tick.common import RawWriter, read_lines, segment_files


def lines_of(path):
    gen = read_lines(path)
    out = []
    while True:
        try:
            out.append(next(gen))
        except StopIteration as stop:
            return out, stop.value


def test_each_start_gets_new_segment_and_seals_on_close(tmp_path):
    for run in ("r1-1", "r2-2"):
        w = RawWriter(str(tmp_path), "x-perp", run)
        w.event(0, {"type": "connect"})
        w.close()
    files = segment_files(str(tmp_path), "x-perp")
    assert [os.path.basename(f) for f in files] == ["r1-1-00001.jsonl.zst", "r2-2-00001.jsonl.zst"]
    assert not [f for f in os.listdir(tmp_path / "raw" / "x-perp") if f.endswith(".partial")]


def test_crash_damages_only_its_own_segment(tmp_path):
    w = RawWriter(str(tmp_path), "x-perp", "r1-1", flush_s=0)
    for i in range(100):
        w.event(0, {"type": "tick", "i": i})
    path = w.path
    w.f.write(b'{"k":"evt","truncated":' * 100)   # unflushed bytes in an open frame
    w.f.flush(zstd.ZstdFile.FLUSH_BLOCK)
    w.f._fp.close()                                   # simulate kill -9: frame never ends
    w.f = None
    w._lock.close()
    # restart: a new run writes a new segment; the old .partial stays as evidence
    w2 = RawWriter(str(tmp_path), "x-perp", "r2-2")
    w2.event(0, {"type": "connect"})
    w2.close()

    lines, (n, err) = lines_of(path)
    assert n == 100 and err is not None             # everything before the crash is readable
    good = segment_files(str(tmp_path), "x-perp")
    assert len(good) == 1
    lines2, (n2, err2) = lines_of(good[0])
    assert n2 == 1 and err2 is None                  # the new run is untouched by the old tail
    assert segment_files(str(tmp_path), "x-perp", include_partial=True)[0] == path


def test_rolls_by_size(tmp_path):
    w = RawWriter(str(tmp_path), "x-spot", "r1-1", roll_bytes=2000)
    for i in range(200):
        w.event(0, {"type": "tick", "i": i, "pad": "x" * 50})
    w.close()
    files = segment_files(str(tmp_path), "x-spot")
    assert len(files) > 5
    total = sum(len(lines_of(f)[0]) for f in files)
    assert total == 200


def test_second_writer_same_stream_refused(tmp_path):
    w = RawWriter(str(tmp_path), "x-perp", "r1-1")
    with pytest.raises(SystemExit):
        RawWriter(str(tmp_path), "x-perp", "r2-2")
    w.close()
    RawWriter(str(tmp_path), "x-perp", "r3-3").close()   # free again after close
