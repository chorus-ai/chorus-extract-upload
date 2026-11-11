"""Streaming (paged) upload: journal paging, bounded memory, worker feeding.

Uses a real v2 journal in SQLite; network transfers are replaced with fakes.
"""
import asyncio
import sqlite3
import threading
import time
import tracemalloc
import types
from pathlib import Path

import pytest

from chorus_upload import local_ops, upload_ops, perf_counter
from chorus_upload.journaldb_ops import JournalDispatcher, JournalTableV2

MB = 2**20
GROUP_SIZES = [100, MB + 1, 2**23 + 1, 2**25 + 1, 2**28 + 1]  # one per size group


def make_journal(path, rows):
    """rows: iterable of (filename, size, modality, active, uploaded). Returns db path str."""
    db = str(path)
    JournalTableV2.create_journal_table(db)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO srcpaths (SRC_PATH) VALUES ('P001/Waveforms')")
        c.execute("INSERT INTO modalities (MODALITY) VALUES ('waveforms')")
        c.execute("INSERT INTO modalities (MODALITY) VALUES ('images')")
        c.execute("INSERT INTO versions (VERSION) VALUES ('20250101')")
        c.execute("INSERT INTO uploads (UPLOAD_DT) VALUES ('20240101')")
        mod_id = {"waveforms": 1, "images": 2}
        c.executemany(
            "INSERT INTO journal_v2 (SRC_PATH_ID, FILENAME, MODALITY_ID, SRC_MODTIME_us, SIZE, MD5,"
            " TIME_VALID_us, TIME_INVALID_us, UPLOAD_DT_ID, VERSION_ID) VALUES (1,?,?,0,?,?,0,?,?,1)",
            ((fn, mod_id[mod], size, "0" * 32, None if active else 1, 1 if uploaded else None)
             for (fn, size, mod, active, uploaded) in rows))
    return db


def pending_ids(db, active=True):
    with sqlite3.connect(db) as c:
        inv = "IS NULL" if active else "IS NOT NULL"
        return {r[0] for r in c.execute(
            f"SELECT FILE_ID FROM journal_v2 WHERE UPLOAD_DT_ID IS NULL AND TIME_INVALID_us {inv} AND MODALITY_ID = 1")}


@pytest.fixture
def mixed_db(tmp_path):
    # pending active waveforms of all sizes, plus rows that must be excluded.
    rows = [(f"f{i}.dat", GROUP_SIZES[i % 5], "waveforms", True, False) for i in range(2500)]
    rows += [(f"up{i}.dat", 100, "waveforms", True, True) for i in range(50)]      # already uploaded
    rows += [(f"img{i}.dat", 100, "images", True, False) for i in range(50)]       # other modality
    rows += [(f"del{i}.dat", 100, "waveforms", False, False) for i in range(70)]   # inactive
    return make_journal(tmp_path / "journal.db", rows)


def test_iter_upload_pages_yields_each_pending_file_once(mixed_db):
    expected = {fid for fid in pending_ids(mixed_db)}
    seen = []
    for lo, hi in [(0, MB), (MB, 2**23), (2**23, 2**25), (2**25, 2**28), (2**28, float('inf'))]:
        for page in local_ops.iter_upload_pages(mixed_db, ["waveforms"], 128, min_size=lo, max_size=hi):
            assert 0 < len(page) <= 128
            for fn, info in page:
                assert lo <= info['size'] < hi
                assert set(info) == {'file_id', 'size', 'md5', 'version', 'central_path'}
                assert fn.startswith("P001/Waveforms/") and info['central_path']
                seen.append(info['file_id'])
    assert len(seen) == len(set(seen)) and set(seen) == expected


def test_paging_stable_while_pages_are_marked_uploaded(mixed_db):
    expected = pending_ids(mixed_db)
    seen = []
    for page in local_ops.iter_upload_pages(mixed_db, ["waveforms"], 100):
        seen += [info['file_id'] for _, info in page]
        JournalDispatcher.mark_as_uploaded_with_duration(
            mixed_db, [("20250202", 0.0, 0.0, info['file_id']) for _, info in page])
    assert sorted(seen) == sorted(expected) and pending_ids(mixed_db) == set()


def test_count_pending_by_size(mixed_db):
    bounds = [(g[1], g[2]) for g in upload_ops.UPLOAD_SIZE_GROUPS.values()]
    stats = JournalDispatcher.count_pending_by_size(mixed_db, ["waveforms"], bounds)
    assert stats == [(500, 500 * s) for s in GROUP_SIZES]


def test_deleted_ids_include_every_inactive_version_of_a_path(tmp_path):
    # same path invalidated three times; all three ids must be marked (the old dict-based
    # listing kept only the last id per path).
    rows = [("same.dat", 100, "waveforms", False, False)] * 3 + [("same.dat", 100, "waveforms", True, False)]
    db = make_journal(tmp_path / "journal.db", rows)
    ids = [fid for page in local_ops.iter_pending_deleted_ids(db, ["waveforms"], 2) for fid in page]
    assert sorted(ids) == [1, 2, 3]


def test_ordered_pages_caps_in_file_order_then_orders():
    pages = [[(f"a{i}", {'size': s}) for i, s in enumerate([1, 5, 3])],
             [(f"b{i}", {'size': s}) for i, s in enumerate([9, 2])]]
    out = list(upload_ops._ordered_pages(iter(pages), 4, 'lpt'))
    assert [[kv[1]['size'] for kv in p] for p in out] == [[5, 3, 1], [9]]
    out = list(upload_ops._ordered_pages(iter(pages), None, 'interleave'))
    assert [[kv[1]['size'] for kv in p] for p in out] == [[5, 1, 3], [9, 2]]


def test_iter_upload_pages_memory_is_bounded_by_page(tmp_path):
    n, page_rows = 200_000, 1000
    db = make_journal(tmp_path / "journal.db",
                      ((f"file_{i:08d}.dat", 100, "waveforms", True, False) for i in range(n)))
    tracemalloc.start()
    try:
        count = 0
        for page in local_ops.iter_upload_pages(db, ["waveforms"], page_rows):
            count += len(page)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert count == n
    # a full list of n info dicts is ~60+ MB; a page is ~1 MB.
    assert peak < 10 * MB, f"peak {peak / MB:.1f} MB"


class _PageFeed:
    """Page iterator that records how far the producer runs ahead of completed uploads."""

    def __init__(self, n, page_rows):
        self.n, self.page_rows = n, page_rows
        self.pulled = 0
        self.done = 0
        self.lock = threading.Lock()
        self.ahead_at_pull = []   # outstanding (pulled - done) when each later page is pulled

    def __iter__(self):
        for start in range(0, self.n, self.page_rows):
            with self.lock:
                if start > 0:
                    self.ahead_at_pull.append(self.pulled - self.done)
                page = [(f"f{i}", {'file_id': i, 'size': 1, 'version': 'v'})
                        for i in range(start, min(start + self.page_rows, self.n))]
                self.pulled += len(page)
            yield page

    def complete(self):
        with self.lock:
            self.done += 1


class _FakeFS:
    """Hashable stand-in for a dated destination FileSystemHelper."""
    def __init__(self, root, **kw):
        self.root = root


def _fake_dest(client):
    return types.SimpleNamespace(client=client, root=Path("dest"), internal_host=None)


def test_async_upload_bounded_and_workers_fed(monkeypatch):
    concurrency, step = 8, 5
    page_rows = step * concurrency
    n = 20 * page_rows + 7
    feed = _PageFeed(n, page_rows)
    flushed, active, peak = [], [0], [0]

    class FakeAz:
        service_client = types.SimpleNamespace(url="u", credential=None, api_version=None)

    class FakeAsyncClient:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    async def fake_upload(src, fn, info, dated, **kw):
        active[0] += 1
        peak[0] = max(peak[0], active[0])
        await asyncio.sleep(0.001)
        active[0] -= 1
        feed.complete()
        return (fn, info, upload_ops.sync_state.MATCHED, dated, 0.0, 0.0)

    monkeypatch.setattr(upload_ops, "AzureBlobClient", FakeAz)
    monkeypatch.setattr(upload_ops, "AsyncBlobServiceClient", FakeAsyncClient)
    monkeypatch.setattr(upload_ops, "AioHttpTransport", lambda **kw: None)
    monkeypatch.setattr(upload_ops, "aiohttp", types.SimpleNamespace(
        TCPConnector=lambda **kw: None, ClientSession=lambda **kw: None))
    monkeypatch.setattr(upload_ops, "FileSystemHelper", _FakeFS)
    monkeypatch.setattr(upload_ops, "_async_upload_and_verify", fake_upload)
    monkeypatch.setattr(upload_ops.JournalDispatcher, "mark_as_uploaded_with_duration",
                        lambda db, args: flushed.append(list(args)))

    update_args, _, _ = upload_ops._async_upload(
        types.SimpleNamespace(is_cloud=False, root=Path("src")), _fake_dest(FakeAz()),
        iter(feed), "db", "20250202", [], step,
        perf_counter.PerformanceCounter(total_file_count=n), concurrency,
        queue_size=page_rows)

    ids = [a[3] for batch in flushed for a in batch] + [a[3] for a in update_args]
    assert sorted(ids) == list(range(n))
    assert all(len(b) == step for b in flushed)
    assert peak[0] == concurrency                      # all workers busy, never more
    # memory bound: never more than queue + one page outstanding when pulling.
    assert max(feed.ahead_at_pull) <= 2 * page_rows
    # not starved: a full set of work is still queued whenever the next page is read.
    assert min(feed.ahead_at_pull) >= concurrency


def test_thread_upload_bounded_and_complete(monkeypatch):
    nuploads, step = 4, 3
    page_rows = step * nuploads
    n = 10 * page_rows + 5
    feed = _PageFeed(n, page_rows)
    flushed, active, peak = [], [0], [0]
    lock = threading.Lock()

    def fake_upload(src, fn, info, dated, **kw):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.002)
        with lock:
            active[0] -= 1
        feed.complete()
        return (fn, info, upload_ops.sync_state.MATCHED, dated, 0.0, 0.0)

    monkeypatch.setattr(upload_ops.storage_helper, "_clone_client", lambda c, pool_size=None: None)
    monkeypatch.setattr(upload_ops, "FileSystemHelper", _FakeFS)
    monkeypatch.setattr(upload_ops, "_upload_and_verify", fake_upload)
    monkeypatch.setattr(upload_ops.JournalDispatcher, "mark_as_uploaded_with_duration",
                        lambda db, args: flushed.append(list(args)))

    update_args, _, _ = upload_ops._thread_upload(
        types.SimpleNamespace(is_cloud=False, root=Path("src")), _fake_dest(None),
        iter(feed), "db", "20250202", [], step,
        perf_counter.PerformanceCounter(total_file_count=n), nuploads, 1)

    ids = [a[3] for batch in flushed for a in batch] + [a[3] for a in update_args]
    assert sorted(ids) == list(range(n))
    assert peak[0] == nuploads
    assert max(feed.ahead_at_pull) <= 2 * nuploads + page_rows


def test_upload_files_streams_caps_and_marks_deleted(tmp_path, monkeypatch):
    rows = [(f"f{i}.dat", GROUP_SIZES[i % 5], "waveforms", True, False) for i in range(53)]
    rows += [("gone.dat", 100, "waveforms", False, False)] * 4
    db = make_journal(tmp_path / "journal.db", rows)
    dest = tmp_path / "dest"
    (dest / "20250101").mkdir(parents=True)

    def fake_upload(src, fn, info, dated, **kw):
        return (fn, info, upload_ops.sync_state.MATCHED, dated, 0.0, 0.0)
    monkeypatch.setattr(upload_ops, "_upload_and_verify", fake_upload)
    monkeypatch.setattr(upload_ops.storage_helper, "_clone_client", lambda c, pool_size=None: None)

    src_fs = upload_ops.FileSystemHelper(str(tmp_path / "src"))
    dest_fs = upload_ops.FileSystemHelper(str(dest))

    _, remaining = upload_ops.upload_files(src_fs, dest_fs, ["waveforms"], db, max_num_files=20, page_size=2)
    assert remaining == 0
    assert len(pending_ids(db)) == 53 - 20
    assert pending_ids(db, active=False) == set()      # all 4 inactive ids marked

    _, remaining = upload_ops.upload_files(src_fs, dest_fs, ["waveforms"], db, page_size=2)
    assert remaining is None and pending_ids(db) == set()


# ---------------- per-file failures must not abort a group

def test_async_upload_and_verify_contains_copy_and_verify_errors(tmp_path):
    (tmp_path / "a.dat").write_bytes(b"x")
    info = {'central_path': 'c/a.dat', 'size': 1, 'md5': 'm'}

    class Src:
        root = tmp_path
        def __init__(self, fail):
            self.fail = fail
        async def async_copy_file_to(self, **kw):
            if self.fail:
                raise RuntimeError("copy boom")

    class Dest:
        async def async_get_metadata(self, **kw):
            raise TimeoutError("verify boom")

    for src in (Src(True), Src(False)):   # copy failure, then verify failure
        res = asyncio.run(upload_ops._async_upload_and_verify(src, "a.dat", info, Dest()))
        assert res[2] == upload_ops.sync_state.MISSING_DEST


def test_upload_and_verify_contains_copy_and_verify_errors(tmp_path, monkeypatch):
    (tmp_path / "a.dat").write_bytes(b"x")
    info = {'central_path': 'c/a.dat', 'size': 1, 'md5': 'm', 'file_id': 1}

    class Src:
        root = tmp_path
        def __init__(self, fail):
            self.fail = fail
        def copy_file_to(self, **kw):
            if self.fail:
                raise RuntimeError("copy boom")

    def boom(**kw):
        raise TimeoutError("verify boom")
    monkeypatch.setattr(upload_ops.FileSystemHelper, "get_metadata", staticmethod(boom))
    dest = types.SimpleNamespace(root=tmp_path / "dest")
    for src in (Src(True), Src(False)):
        res = upload_ops._upload_and_verify(src, "a.dat", info, dest)
        assert res[2] == upload_ops.sync_state.MISSING_DEST


def test_thread_group_logs_the_conn_per_file_it_uses(monkeypatch, caplog):
    import logging
    captured = {}

    def fake_thread_upload(src, dest, pages, db, dt, update_args, step, perf, nuploads, per_file,
                           verbose=False, *, group_perf=None):
        list(pages)
        captured['per_file'] = per_file
        return update_args, perf, set()
    monkeypatch.setattr(upload_ops, "_thread_upload", fake_thread_upload)
    caplog.set_level(logging.INFO, logger="chorus_upload.upload_ops")

    upload_ops._parallel_upload(
        types.SimpleNamespace(is_cloud=False), types.SimpleNamespace(client=None),
        lambda lo, hi, n: iter([[("f", {'size': 2**26, 'file_id': 1})]]),
        {'xlarge': (1, 2**26)}, "db", "dt", [], 10,
        perf_counter.PerformanceCounter(total_file_count=1), nthreads=16)
    msg = next(r.getMessage() for r in caplog.records if "xlarge group, thread" in r.getMessage())
    assert f"× {captured['per_file']} conn/file" in msg


def test_async_verify_failed_check_does_not_stop_run_and_spills(monkeypatch, tmp_path):
    class FakeAz:
        service_client = types.SimpleNamespace(url="u", credential=None, api_version=None)

    class FakeAsyncClient:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class FakeDest:
        def __init__(self, root, **kw): pass
        async def async_get_metadata(self, path, **kw):
            if path == "bad":
                raise TimeoutError("network")
            if path.startswith("nomd5"):
                return {'size': 1, 'md5': None}
            return {'size': 1, 'md5': 'm'}

    monkeypatch.setattr(upload_ops, "AzureBlobClient", FakeAz)
    monkeypatch.setattr(upload_ops, "AsyncBlobServiceClient", FakeAsyncClient)
    monkeypatch.setattr(upload_ops, "AioHttpTransport", lambda **kw: None)
    monkeypatch.setattr(upload_ops, "aiohttp", types.SimpleNamespace(
        TCPConnector=lambda **kw: None, ClientSession=lambda **kw: None))
    monkeypatch.setattr(upload_ops, "FileSystemHelper", FakeDest)
    monkeypatch.setattr(upload_ops, "convert_local_to_central_path", lambda fn, **kw: fn)

    items = [(i, f"f{i}", 1, "m", "waveforms") for i in range(30)]
    items += [(100, "bad", 1, "m", "waveforms")]
    items += [(200 + i, f"nomd5_{i}", 1, None, "waveforms") for i in range(5)]
    pages = [items[i:i + 8] for i in range(0, len(items), 8)]
    spill = upload_ops._DownloadSpill(str(tmp_path / "spill.tmp"), buffer_rows=2)
    dated = types.SimpleNamespace(client=FakeAz(), root=Path("v1"), internal_host=None)

    n, mm, mi, _, failed = upload_ops._async_verify(dated, iter(pages), 4, needs_download=spill)
    assert (n, mm, mi, failed) == (30, [], [], ["bad"])
    assert spill.count(0, float('inf')) == 5
    spill.close()
