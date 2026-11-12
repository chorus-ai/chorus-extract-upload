"""verify_files decision table: local (L / ~L / X) x remote (R / ~R / Y) -> journal action.

Real v2 journal and local files; destination is a local dir (thread path) or a fake Azure
client (async path, phase 2).
"""
import os
import shutil
import sqlite3
import types
from pathlib import Path

import pytest

from chorus_upload import local_ops, upload_ops
from chorus_upload.journaldb_ops import JournalDispatcher
from chorus_upload.storage_helper import FileSystemHelper

MOD = "waveforms"
FILES = ["P001/Waveforms/a.dat", "P001/Waveforms/b.dat", "P001/Waveforms/c.dat",
         "P002/Waveforms/d.dat", "P002/Waveforms/f.dat", "P002/Waveforms/g.dat"]


def write(root, rel, data, mtime=1_700_000_000):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    os.utime(p, (mtime, mtime))


def upload_state(db):
    """{path: upload_dt or None} for active rows."""
    with sqlite3.connect(db) as c:
        return dict(c.execute(
            "SELECT srcpaths.SRC_PATH || '/' || FILENAME, uploads.UPLOAD_DT FROM journal_v2"
            " LEFT JOIN srcpaths ON srcpaths.id = journal_v2.SRC_PATH_ID"
            " LEFT JOIN uploads ON uploads.id = journal_v2.UPLOAD_DT_ID"
            " WHERE TIME_INVALID_us IS NULL").fetchall())


@pytest.fixture
def world(tmp_path):
    """Journal v1 of FILES, all copied to dest/v1; all but g.dat recorded as uploaded."""
    src, dest, db = tmp_path / "src", tmp_path / "dest", str(tmp_path / "journal.db")
    for i, rel in enumerate(FILES):
        write(src, rel, f"content-{i:04d}".encode())
    local_ops.update_journal(FileSystemHelper(str(src)), [MOD], db, journaling_mode="full",
                             version="v1", page_size=2)
    patterns = local_ops._compile_patterns([MOD], {})
    central = {}
    for page in local_ops.iter_upload_pages(db, [MOD], 100):
        for fn, info in page:
            central[fn] = info["central_path"]
            target = dest / "v1" / info["central_path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src / fn, target)
            os.utime(target, (1_700_000_100, 1_700_000_100))
            if fn != "P002/Waveforms/g.dat":
                JournalDispatcher.mark_as_uploaded_with_duration(db, [("20240101000000", None, None, info["file_id"])])
    return types.SimpleNamespace(src=src, dest=dest, db=db, central=central, patterns=patterns)


def damage(w):
    (w.dest / "v1" / w.central["P001/Waveforms/b.dat"]).unlink()                      # Y
    write(w.dest / "v1", w.central["P001/Waveforms/c.dat"], b"CONTENT-0002")           # ~R, same size
    write(w.src, "P002/Waveforms/d.dat", b"content-0003", mtime=1_700_009_999)        # ~L (mtime)
    (w.src / "P002/Waveforms/f.dat").unlink()                                          # X


def run_verify(w, **kw):
    return upload_ops.verify_files(FileSystemHelper(str(w.dest)), w.db, modalities=[MOD],
                                   src_paths={MOD: FileSystemHelper(str(w.src))},
                                   page_size=1, compiled_patterns=w.patterns, modality_configs={}, **kw)


EXPECTED = {'matched': 1, 'matched_set_upload_time': 1, 'remote_missing_cleared': 1,
            'remote_corrupt_cleared': 1, 'local_changed': 1, 'local_missing': 1}


def test_dry_run_reports_without_changing_journal(world):
    damage(world)
    before = upload_state(world.db)
    assert run_verify(world, version="v1") == EXPECTED
    assert upload_state(world.db) == before


def test_update_upload_status_applies_decision_table(world):
    damage(world)
    assert run_verify(world, version="v1", update_upload_status=True) == EXPECTED
    after = upload_state(world.db)
    assert after["P001/Waveforms/a.dat"] == "20240101000000"     # L,R: kept
    assert after["P002/Waveforms/g.dat"] == "20240101000000"     # L,R, was null: version's upload time
    assert after["P001/Waveforms/b.dat"] is None                 # L,Y: cleared
    assert after["P001/Waveforms/c.dat"] is None                 # L,~R: cleared
    assert after["P002/Waveforms/d.dat"] == "20240101000000"     # ~L: untouched
    assert after["P002/Waveforms/f.dat"] == "20240101000000"     # X: untouched
    # cleared files are picked up by the next upload.
    pending = {fn for page in local_ops.iter_upload_pages(world.db, [MOD], 100) for fn, _ in page}
    assert pending == {"P001/Waveforms/b.dat", "P001/Waveforms/c.dat"}
    # second run converges: nothing left to set or clear.
    again = run_verify(world, version="v1", update_upload_status=True)
    assert again.get('matched_set_upload_time', 0) == 0
    assert again.get('remote_missing_cleared', 0) == 0 and again.get('remote_corrupt_cleared', 0) == 0
    assert again['remote_missing_not_uploaded'] == 1 and again['remote_corrupt_not_uploaded'] == 1


def test_version_scope_vs_all_versions(world):
    write(world.src, "P003/Waveforms/h.dat", b"h")
    local_ops.update_journal(FileSystemHelper(str(world.src)), [MOD], world.db,
                             journaling_mode="full", version="v2", page_size=2)
    v1 = run_verify(world, version="v1")
    v2 = run_verify(world, version="v2")
    allv = run_verify(world, all_versions=True)
    assert sum(v1.values()) == len(FILES) and sum(v2.values()) == 1
    assert allv == {k: v1.get(k, 0) + v2.get(k, 0) for k in set(v1) | set(v2)}
    assert v2 == {'remote_missing_not_uploaded': 1}               # h.dat never uploaded
    with pytest.raises(ValueError):
        run_verify(world, version="v1", all_versions=True)


# ---------------- Azure async path (fake blob client): phase 2 and failure containment

def test_async_path_phase2_and_failures(world, monkeypatch):
    damage(world)
    with sqlite3.connect(world.db) as c:
        md5_of = dict(c.execute("SELECT srcpaths.SRC_PATH || '/' || FILENAME, MD5 FROM journal_v2"
                                " LEFT JOIN srcpaths ON srcpaths.id = journal_v2.SRC_PATH_ID"
                                " WHERE TIME_INVALID_us IS NULL").fetchall())
    by_central = {world.central[fn]: fn for fn in world.central}
    downloads = []

    class FakeAz:
        service_client = types.SimpleNamespace(url="u", credential=None, api_version=None)

    class FakeAsyncClient:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class FakeFS:
        get_metadata = staticmethod(FileSystemHelper.get_metadata)   # real local checks

        def __init__(self, root, **kw):
            self.root = root

        async def async_get_metadata(self, path, compute_md5=False, local_md5=None, **kw):
            assert local_md5 is None               # journal md5 must never be stamped on the blob
            fn = by_central[path]
            size = len(f"content-0000".encode())
            if fn == "P001/Waveforms/b.dat":
                return None                         # Y
            if fn == "P002/Waveforms/g.dat":
                raise TimeoutError("network")       # failed
            if fn == "P001/Waveforms/a.dat":        # no md5 until downloaded (phase 2)
                if compute_md5:
                    downloads.append(fn)
                    return {'size': size, 'md5': md5_of[fn], 'modify_time_us': 1_700_000_100_000_000}
                return {'size': size, 'md5': None, 'modify_time_us': 1_700_000_100_000_000}
            if fn == "P001/Waveforms/c.dat":
                return {'size': size, 'md5': "0" * 32, 'modify_time_us': 1_700_000_100_000_000}   # ~R
            return {'size': size, 'md5': md5_of[fn], 'modify_time_us': 1_700_000_100_000_000}

    monkeypatch.setattr(upload_ops, "AzureBlobClient", FakeAz)
    monkeypatch.setattr(upload_ops, "AsyncBlobServiceClient", FakeAsyncClient)
    monkeypatch.setattr(upload_ops, "AioHttpTransport", lambda **kw: None)
    monkeypatch.setattr(upload_ops, "aiohttp", types.SimpleNamespace(
        TCPConnector=lambda **kw: None, ClientSession=lambda **kw: None))
    monkeypatch.setattr(upload_ops, "FileSystemHelper", FakeFS)

    dest = types.SimpleNamespace(client=FakeAz(), root=Path("dest"), internal_host=None)
    counts = upload_ops.verify_files(dest, world.db, version="v1", modalities=[MOD],
                                     src_paths={MOD: FileSystemHelper(str(world.src))},
                                     update_upload_status=True, page_size=1,
                                     compiled_patterns=world.patterns, modality_configs={})
    assert counts == {'matched': 1, 'remote_missing_cleared': 1, 'remote_corrupt_cleared': 1,
                      'local_changed': 1, 'local_missing': 1, 'failed': 1}
    assert downloads == ["P001/Waveforms/a.dat"]          # only the md5-less blob was downloaded
    after = upload_state(world.db)
    assert after["P002/Waveforms/g.dat"] is None           # failed check: left as is (was null)
    assert after["P001/Waveforms/b.dat"] is None and after["P001/Waveforms/c.dat"] is None
    assert not [p for p in os.listdir(os.path.dirname(world.db)) if p.endswith(".tmp")]


def test_md5_equal_accepts_double_b64_artifact():
    import base64
    md5 = "0123456789abcdef0123456789abcdef"
    doubled = base64.b64encode(bytes.fromhex(md5)).hex()
    assert upload_ops._md5_equal(md5, md5) and upload_ops._md5_equal(doubled, md5)
    assert not upload_ops._md5_equal("f" * 32, md5)
    assert upload_ops._remote_outcome({'size': 1, 'md5': "abc-2"}, 1, md5) == 'no_md5'   # S3 multipart etag


def test_null_upload_time_falls_back_to_remote_mtime_when_version_never_uploaded(world):
    with sqlite3.connect(world.db) as c:
        fids = c.execute("SELECT FILE_ID FROM journal_v2").fetchall()
    JournalDispatcher.mark_as_not_uploaded(world.db, fids)
    assert JournalDispatcher.latest_upload_by_version(world.db) == {}
    counts = run_verify(world, version="v1", update_upload_status=True)
    assert counts == {'matched_set_upload_time': len(FILES)}
    assert set(upload_state(world.db).values()) == {"20231114221500"}   # remote mtime, UTC


# ---------------- sync get_metadata reports a missing file as a dict of Nones

def test_upload_and_verify_reports_missing_destination(tmp_path):
    (tmp_path / "a.dat").write_bytes(b"x")

    class SilentSrc:   # "copy" succeeds but writes nothing
        root = tmp_path
        def copy_file_to(self, **kw):
            pass

    dest = FileSystemHelper(str(tmp_path / "dest"))
    info = {'central_path': 'c/a.dat', 'size': 1, 'md5': 'm', 'file_id': 1}
    res = upload_ops._upload_and_verify(SilentSrc(), "a.dat", info, dest)
    assert res[2] == upload_ops.sync_state.MISSING_DEST


def test_mark_as_uploaded_logs_missing_not_mismatched(world, caplog):
    import logging
    (world.dest / "v1" / world.central["P002/Waveforms/g.dat"]).unlink()
    caplog.set_level(logging.ERROR, logger="chorus_upload.upload_ops")
    upload_ops.mark_as_uploaded(FileSystemHelper(str(world.dest)), world.db, "v1", ["P002/Waveforms/g.dat"],
                                compiled_patterns=world.patterns, modality_configs={})
    msgs = [r.getMessage() for r in caplog.records]
    assert any("missing file P002/Waveforms/g.dat" in m for m in msgs)
    assert not any("mismatched" in m for m in msgs)
