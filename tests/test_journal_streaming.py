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
            last = (fn, info)
    rel_missing = "P001/Waveforms/b.dat"
    rel_bad = "P001/Waveforms/c.dat"
    central = {fn: info["central_path"] for page in local_ops.iter_upload_pages(db, ["waveforms"], 100)
               for fn, info in page}
    (dest / "v1" / central[rel_missing]).unlink()
    (dest / "v1" / central[rel_bad]).write_bytes(b"different size")

    n_matched, mismatched, missing = upload_ops.verify_files(
        FileSystemHelper(str(dest)), db, version="v1", modalities=["waveforms"],
        page_size=1, compiled_patterns=patterns, modality_configs={})
    assert (n_matched, mismatched, missing) == (3, {rel_bad}, {rel_missing})
