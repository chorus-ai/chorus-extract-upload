"""Journal update / listing behavior, pinned before and after the streaming refactor.

Real files in a temp dir and a real v2 journal; no network.
"""
import os
import sqlite3

import pytest

from chorus_upload import local_ops
from chorus_upload.storage_helper import FileSystemHelper


def write(root, rel, data, mtime=None):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    if mtime is not None:
        os.utime(p, (mtime, mtime))


def journal_rows(db):
    """{path: [(active, version), ...]} sorted by FILE_ID."""
    with sqlite3.connect(db) as c:
        rows = c.execute(
            "SELECT srcpaths.SRC_PATH || '/' || FILENAME, TIME_INVALID_us IS NULL, versions.VERSION, FILE_ID "
            "FROM journal_v2 LEFT JOIN srcpaths ON srcpaths.id = journal_v2.SRC_PATH_ID "
            "LEFT JOIN versions ON versions.id = journal_v2.VERSION_ID ORDER BY FILE_ID").fetchall()
    out = {}
    for path, active, ver, _ in rows:
        out.setdefault(path, []).append((bool(active), ver))
    return out


@pytest.fixture
def site(tmp_path):
    src = tmp_path / "src"
    for rel in ["P001/Waveforms/a.dat", "P001/Waveforms/b.dat", "P001/Waveforms/c.dat",
                "P002/Waveforms/d.dat", "P002/Waveforms/sub/f.dat"]:
        write(src, rel, rel.encode(), mtime=1_700_000_000)
    return src, str(tmp_path / "journal.db")


def run_update(src, db, version, mode):
    local_ops.update_journal(FileSystemHelper(str(src)), ["waveforms"], db,
                             journaling_mode=mode, version=version, page_size=2)


@pytest.mark.parametrize("mode", ["full", "append"])
def test_update_journal_add_keep_modify_delete(site, mode):
    src, db = site
    run_update(src, db, "v1", mode)
    first = journal_rows(db)
    assert all(v == [(True, "v1")] for v in first.values()) and len(first) == 5

    (src / "P001/Waveforms/b.dat").unlink()                                # deleted
    write(src, "P001/Waveforms/c.dat", b"changed!", mtime=1_700_000_500)    # modified
    write(src, "P002/Waveforms/e.dat", b"new", mtime=1_700_000_000)        # added
    run_update(src, db, "v2", mode)

    rows = journal_rows(db)
    assert rows["P001/Waveforms/a.dat"] == [(True, "v1")]
    assert rows["P002/Waveforms/d.dat"] == [(True, "v1")]
    assert rows["P002/Waveforms/sub/f.dat"] == [(True, "v1")]
    assert rows["P001/Waveforms/c.dat"] == [(False, "v1"), (True, "v2")]
    assert rows["P002/Waveforms/e.dat"] == [(True, "v2")]
    # only full/snapshot journaling inactivates files missing from disk.
    assert rows["P001/Waveforms/b.dat"] == [(mode == "append", "v1")]


def test_update_journal_unchanged_rerun_is_noop(site):
    src, db = site
    run_update(src, db, "v1", "full")
    before = journal_rows(db)
    run_update(src, db, "v2", "full")
    assert journal_rows(db) == before


def test_list_files_with_info_keeps_every_inactive_id(site):
    src, db = site
    run_update(src, db, "v1", "full")
    for i in range(3):   # c.dat invalidated three times -> three inactive ids
        write(src, "P001/Waveforms/c.dat", b"x" * (i + 10), mtime=1_700_001_000 + i)
        run_update(src, db, f"v{i + 2}", "full")
    _, active, inactive = local_ops.list_files_with_info(db, modalities=["waveforms"])
    assert len(inactive["P001/Waveforms/c.dat"]) == 3
    assert set(active) == {"P001/Waveforms/a.dat", "P001/Waveforms/b.dat", "P001/Waveforms/c.dat",
                           "P002/Waveforms/d.dat", "P002/Waveforms/sub/f.dat"}


def test_update_journal_removes_snapshot_file(site):
    src, db = site
    run_update(src, db, "v1", "full")
    run_update(src, db, "v2", "full")
    assert not [p for p in os.listdir(os.path.dirname(db)) if p.endswith(".tmp")]


# ---------------- list command (streamed)

def test_list_streaming_matches_full_listing(site):
    from chorus_upload.journaldb_ops import JournalDispatcher
    src, db = site
    run_update(src, db, "v1", "full")
    write(src, "P003/Waveforms/g.dat", b"g", mtime=1_700_000_000)
    run_update(src, db, "v2", "full")

    by_ver, active, _ = local_ops.list_files_with_info(db, modalities=["waveforms"], uploaded=False)
    counts = JournalDispatcher.count_files_by_version(db, ["waveforms"], active=True, uploaded=False)
    assert counts == [(v, len(f)) for v, f in sorted(by_ver.items())]
    for v, files in by_ver.items():
        assert set(local_ops.iter_file_paths(db, ["waveforms"], 2, version=v, active=True, uploaded=False)) == files

    listing = local_ops.JournalFileListing(db, ["waveforms"], 2, active=True, uploaded=False)
    assert dict(listing.items()) == active
    assert list(listing.keys()) == list(dict(listing.items()).keys())


def test_write_files_list_mode_streams_lazy_listing(site, tmp_path):
    from chorus_upload import script_generators
    src, db = site
    run_update(src, db, "v1", "full")
    listing = local_ops.JournalFileListing(db, ["waveforms"], 2, active=True, uploaded=False)
    out = tmp_path / "files.txt"
    script_generators._write_files({"waveforms": ({"path": str(src)}, listing)}, "dt", filename=str(out),
                                   out_type="list", dest_config={"path": "az://acct/container"})
    expected = sorted((src / p).as_posix() for p in listing.keys())
    assert sorted(out.read_text().splitlines()) == expected


# ---------------- verify (streamed, local destination -> thread path)

def test_verify_files_streams_and_reports_missing_and_mismatched(site, tmp_path):
    from chorus_upload import upload_ops
    import shutil
    src, db = site
    run_update(src, db, "v1", "full")
    patterns = local_ops._compile_patterns(["waveforms"], {})
    dest = tmp_path / "dest"
    for page in local_ops.iter_upload_pages(db, ["waveforms"], 100, modality_configs={}):
        for fn, info in page:
            target = dest / "v1" / info["central_path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src / fn, target)
    rel_missing = "P001/Waveforms/b.dat"
    rel_bad = "P001/Waveforms/c.dat"
    central = {fn: info["central_path"] for page in local_ops.iter_upload_pages(db, ["waveforms"], 100)
               for fn, info in page}
    (dest / "v1" / central[rel_missing]).unlink()
    (dest / "v1" / central[rel_bad]).write_bytes(b"different size")

    n_matched, mismatched, missing, failed = upload_ops.verify_files(
        FileSystemHelper(str(dest)), db, version="v1", modalities=["waveforms"],
        page_size=1, compiled_patterns=patterns, modality_configs={})
    assert (n_matched, mismatched, missing, failed) == (3, {rel_bad}, {rel_missing}, set())


def test_thread_verify_failed_check_does_not_stop_run(monkeypatch):
    from chorus_upload import upload_ops
    import types

    def fake_info(dated, item, **kw):
        fid, fn, size, md5, mod = item
        if fn == "bad":
            raise TimeoutError("network")
        return ({'size': size}, md5, fid, fn, size, md5)
    monkeypatch.setattr(upload_ops, "_get_file_info", fake_info)
    items = [(i, f"f{i}", 1, "m", "waveforms") for i in range(20)] + [(99, "bad", 1, "m", "waveforms")]
    pages = [items[i:i + 4] for i in range(0, len(items), 4)]
    n, mm, mi, _, failed = upload_ops._thread_verify(
        types.SimpleNamespace(root=types.SimpleNamespace(name="v1")), iter(pages), 3)
    assert (n, mm, mi, failed) == (20, [], [], ["bad"])


def test_download_spill_streams_by_size_and_cleans_up(tmp_path):
    from chorus_upload import upload_ops
    spill = upload_ops._DownloadSpill(str(tmp_path / "spill.tmp"), buffer_rows=7)
    items = [(i, f"f{i}", (i % 3) * 100, None, "waveforms") for i in range(50)]
    for it in items:
        spill.append(it)
    assert len(spill) == 50
    assert spill.count(100, 200) == len([i for i in items if 100 <= i[2] < 200])
    got = [r for page in spill.iter_pages(100, float('inf'), 4) for r in page]
    assert all(len(p) <= 4 for p in spill.iter_pages(0, float('inf'), 4))
    assert got == [i for i in items if i[2] >= 100]
    spill.close()
    assert not (tmp_path / "spill.tmp").exists()


# ---------------- file-list lookups (streamed, memory bounded by the list)

def test_mark_files_as_deleted_streams(site):
    src, db = site
    run_update(src, db, "v1", "append")
    local_ops.mark_files_as_deleted(["P001/Waveforms/a.dat", "P002/Waveforms/d.dat", "not/in/journal"], db)
    rows = journal_rows(db)
    assert rows["P001/Waveforms/a.dat"] == [(False, "v1")]
    assert rows["P002/Waveforms/d.dat"] == [(False, "v1")]
    assert rows["P001/Waveforms/b.dat"] == [(True, "v1")]


def test_mark_as_uploaded_streams_and_checks_destination(site, tmp_path):
    from chorus_upload import upload_ops
    import shutil
    src, db = site
    run_update(src, db, "v1", "full")
    patterns = local_ops._compile_patterns(["waveforms"], {})
    dest = tmp_path / "dest"
    central = {fn: info["central_path"] for page in local_ops.iter_upload_pages(db, ["waveforms"], 100)
               for fn, info in page}
    for fn in ["P001/Waveforms/a.dat", "P001/Waveforms/b.dat"]:   # c.dat is listed but not at dest
        (dest / "v1" / central[fn]).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src / fn, dest / "v1" / central[fn])

    upload_ops.mark_as_uploaded(FileSystemHelper(str(dest)), db, "v1",
                                ["P001/Waveforms/a.dat", "P001/Waveforms/b.dat", "P001/Waveforms/c.dat"],
                                compiled_patterns=patterns, modality_configs={})
    with sqlite3.connect(db) as c:
        uploaded = {r[0] for r in c.execute(
            "SELECT srcpaths.SRC_PATH || '/' || FILENAME FROM journal_v2 LEFT JOIN srcpaths"
            " ON srcpaths.id = journal_v2.SRC_PATH_ID WHERE UPLOAD_DT_ID IS NOT NULL")}
    assert uploaded == {"P001/Waveforms/a.dat", "P001/Waveforms/b.dat"}


def test_path_lookup_uses_lazy_index_and_matches_scan(tmp_path):
    from chorus_upload.journaldb_ops import JournalDispatcher, JournalTableV2
    import random
    db = str(tmp_path / "journal.db")
    JournalTableV2.create_journal_table(db)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO modalities (MODALITY) VALUES ('waveforms')")
        c.execute("INSERT INTO versions (VERSION) VALUES ('v1')")
        c.executemany("INSERT INTO srcpaths (SRC_PATH) VALUES (?)", [(f"P{d:03d}/Waveforms",) for d in range(50)])
        c.executemany(
            "INSERT INTO journal_v2 (SRC_PATH_ID, FILENAME, MODALITY_ID, SRC_MODTIME_us, SIZE, MD5,"
            " TIME_VALID_us, TIME_INVALID_us, UPLOAD_DT_ID, VERSION_ID) VALUES (?,?,1,0,1,'m',0,?,NULL,1)",
            [(1 + i % 50, f"f{i}.dat", 1 if i % 7 == 0 else None) for i in range(5000)])

    def has_index():
        with sqlite3.connect(db) as c:
            return c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (JournalTableV2.path_index,)).fetchone()
    assert has_index() is None          # not created with the table

    all_rows = [r for b in JournalDispatcher.iter_files_batches(db, None, 1000, active=True) for r in b]
    wanted = set(random.Random(0).sample([r[1] for r in all_rows], 300)) | {"P000/Waveforms/none.dat"}
    got = JournalDispatcher.get_files_by_paths(db, wanted, active=True)
    assert has_index() is not None      # created on first lookup
    assert got == [r for r in all_rows if r[1] in wanted]

    with sqlite3.connect(db) as c:
        c.execute("CREATE TEMP TABLE _want_paths (W_PATH TEXT, W_NAME TEXT)")
        plan = " ".join(r[-1] for r in c.execute(
            "EXPLAIN QUERY PLAN " + JournalTableV2._paths_lookup_stmt(active=True)))
    assert JournalTableV2.path_index in plan and "SCAN journal_v2" not in plan, plan
