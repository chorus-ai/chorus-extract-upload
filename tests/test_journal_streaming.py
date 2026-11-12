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


# ---------------- file-list lookups

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


# ---------------- several active rows for one path are reconciled by journal update

def _duplicate_active_row(db, filename, version):
    """Insert a second active copy of a row (as an inconsistent journal would have)."""
    with sqlite3.connect(db) as c:
        c.execute("INSERT OR IGNORE INTO versions (VERSION) VALUES (?)", (version,))
        vid = c.execute("SELECT id FROM versions WHERE VERSION = ?", (version,)).fetchone()[0]
        c.execute("INSERT INTO journal_v2 (PERSON_ID, SRC_PATH_ID, FILENAME, MODALITY_ID, SRC_MODTIME_us, SIZE, MD5,"
                  " TIME_VALID_us, TIME_INVALID_us, UPLOAD_DT_ID, VERSION_ID)"
                  " SELECT PERSON_ID, SRC_PATH_ID, FILENAME, MODALITY_ID, SRC_MODTIME_us, SIZE, MD5,"
                  " TIME_VALID_us, NULL, NULL, ? FROM journal_v2 WHERE FILENAME = ? AND TIME_INVALID_us IS NULL",
                  (vid, filename))


@pytest.mark.parametrize("mode", ["full", "append"])
def test_update_journal_reconciles_duplicate_active_rows(site, mode):
    src, db = site
    run_update(src, db, "v1", mode)
    _duplicate_active_row(db, "a.dat", "v0")       # unchanged file, two active rows
    _duplicate_active_row(db, "c.dat", "v0")       # file that also changed on disk
    write(src, "P001/Waveforms/c.dat", b"changed!", mtime=1_700_000_500)
    run_update(src, db, "v2", mode)

    rows = journal_rows(db)
    # a.dat: the newest row is kept as is; the older duplicate is outdated; the file is not dropped.
    assert rows["P001/Waveforms/a.dat"] == [(False, "v1"), (True, "v0")]
    # c.dat: duplicates collapse, then the normal "updated" handling applies to the newest.
    assert rows["P001/Waveforms/c.dat"] == [(False, "v1"), (False, "v0"), (True, "v2")]
    assert all(sum(active for active, _ in v) == 1 for v in rows.values())

    before = journal_rows(db)
    run_update(src, db, "v3", mode)               # consistent now: nothing changes
    assert journal_rows(db) == before


# ---------------- directory walk for local sources (journal update)

WALK_PATTERNS = ["{patient_id:w}/Waveforms/{filepath}", "OMOP/{filepath}", "Metadata/{filepath}",
                 "{version:w}/{patient_id:w}/Images/{filepath}", "{patient_id}/Waveforms/*.dat", None]


@pytest.fixture
def tree(tmp_path):
    """Mixed case, hidden names, nesting, links to a file / a directory, a broken link."""
    src = tmp_path / "src"
    for rel in ["P001/Waveforms/a.dat", "P001/waveforms/b.DAT", "P001/WAVEFORMS/sub/deep/c.dat",
                "P002/Waveforms/.hidden", "P002/Waveforms/.hid/d.dat", "P002/Other/e.dat",
                "omop/person.csv", "OMOP/sub/visit.csv", "Metadata/m.json", "top.txt",
                "v1/P003/Images/i.dcm", "v1/P003/images/s/j.dcm"]:
        write(src, rel, rel.encode(), mtime=1_700_000_000)
    (src / "P002/Waveforms/emptydir").mkdir()
    outside = tmp_path / "outside"
    write(outside, "x/linked.dat", b"linked")
    os.symlink(outside / "x/linked.dat", src / "P001/Waveforms/filelink.dat")
    os.symlink(outside / "x", src / "P001/Waveforms/dirlink")       # below "**": not followed
    os.symlink(outside, src / "P009")                                # at "*": followed
    os.symlink(src / "nowhere", src / "P001/Waveforms/broken.dat")
    return src


@pytest.mark.parametrize("pattern", WALK_PATTERNS)
def test_walk_finds_the_same_files_as_glob(tree, pattern):
    fs = FileSystemHelper(str(tree))
    globbed = sorted(p for page in fs.get_files_iter(pattern=pattern, page_size=3) for p in page)
    pages = list(fs.walk_files_iter(pattern=pattern, page_size=3))
    walked = sorted(p for page in pages for (p, _, _) in page)
    assert walked == globbed and len(walked) == len(set(walked))
    assert all(len(page) <= 3 for page in pages)
    for page in pages:                                   # same size / modify time as get_metadata
        for p, size, mtime in page:
            meta = FileSystemHelper.get_metadata(root=fs.root, path=p)
            assert (size, mtime) == (meta['size'], meta['modify_time_us'])


def test_walk_declines_patterns_it_does_not_reproduce(tree):
    fs = FileSystemHelper(str(tree))
    for pattern in ["**/x/**/*.dat", "../{filepath}", "a**/b"]:
        assert fs.walk_files_iter(pattern=pattern) is None


def test_unchanged_files_skip_the_pool(site, monkeypatch):
    src, db = site
    run_update(src, db, "v1", "full")
    checked = []
    real = local_ops._update_journal_one_file
    monkeypatch.setattr(local_ops, "_update_journal_one_file",
                        lambda root, relpath, *a, **kw: checked.append(relpath) or real(root, relpath, *a, **kw))
    run_update(src, db, "v2", "full")
    assert checked == [] and journal_rows(db) == {p: [(True, "v1")] for p in journal_rows(db)}

    write(src, "P001/Waveforms/c.dat", b"changed!", mtime=1_700_000_500)
    write(src, "P002/Waveforms/e.dat", b"new")
    (src / "P001/Waveforms/b.dat").unlink()
    run_update(src, db, "v3", "full")
    assert sorted(checked) == ["P001/Waveforms/c.dat", "P002/Waveforms/e.dat"]
    rows = journal_rows(db)
    assert rows["P001/Waveforms/c.dat"] == [(False, "v1"), (True, "v3")]
    assert rows["P002/Waveforms/e.dat"] == [(True, "v3")]
    assert rows["P001/Waveforms/b.dat"] == [(False, "v1")]
    assert rows["P001/Waveforms/a.dat"] == [(True, "v1")]


def test_walk_and_glob_sources_give_the_same_journal(site, tmp_path, monkeypatch):
    src, db = site
    db_glob = str(tmp_path / "glob.db")

    def scenario(dbname):
        run_update(src, dbname, "v1", "full")
        write(src, "P001/Waveforms/c.dat", b"changed!", mtime=1_700_000_500)
        (src / "P001/Waveforms/b.dat").unlink()
        run_update(src, dbname, "v2", "full")
        write(src, "P001/Waveforms/c.dat", b"P001/Waveforms/c.dat", mtime=1_700_000_000)   # restore
        write(src, "P001/Waveforms/b.dat", b"P001/Waveforms/b.dat", mtime=1_700_000_000)
        return journal_rows(dbname)

    walked = scenario(db)
    monkeypatch.setattr(FileSystemHelper, "walk_files_iter", lambda self, **kw: None)   # as a cloud source
    assert scenario(db_glob) == walked
