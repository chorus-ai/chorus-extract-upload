"""Regression tests: remote delete, local destinations, thread config, S3 paths, Azure auth,
copy errors, mark-as-uploaded, journal list --modalities and smaller fixes.  No network."""
import os
import sqlite3
import types

import pytest

from chorus_upload import config_helper, local_ops, remote_file_ops, storage_helper, upload_ops
from chorus_upload.storage_helper import FileSystemHelper

MOD = "waveforms"


def write(root, rel, data=b"x", mtime=1_700_000_000):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    os.utime(p, (mtime, mtime))


def upload_state(db):
    with sqlite3.connect(db) as c:
        return dict(c.execute(
            "SELECT srcpaths.SRC_PATH || '/' || FILENAME, uploads.UPLOAD_DT FROM journal_v2"
            " LEFT JOIN srcpaths ON srcpaths.id = journal_v2.SRC_PATH_ID"
            " LEFT JOIN uploads ON uploads.id = journal_v2.UPLOAD_DT_ID WHERE TIME_INVALID_us IS NULL").fetchall())


@pytest.fixture
def site(tmp_path):
    src, db = tmp_path / "src", str(tmp_path / "journal.db")
    for i, rel in enumerate(["P001/Waveforms/a.dat", "P001/Waveforms/b.dat", "P002/Waveforms/sub/c.dat"]):
        write(src, rel, f"content-{i}".encode())
    local_ops.update_journal(FileSystemHelper(str(src)), [MOD], db, journaling_mode="full",
                             version="v1", page_size=2)
    return types.SimpleNamespace(src=src, db=db, tmp=tmp_path)


# ---------------- remote delete

@pytest.fixture
def central(tmp_path):
    root = tmp_path / "central"
    for rel in ["v1/a.dat", "v1/b.txt", "v1/sub/c.dat", "v2/d.dat", "myjournal.db", "myjournal.db.locked", "journal.db"]:
        write(root, rel)
    config = {"central_path": {"path": str(root)}, "configuration": {},
              "journal": {"source": {"path": "az://container/myjournal.db"}}}
    return root, config


def test_collect_files_to_delete_patterns_and_journal_guard(central):
    root, config = central
    fs = FileSystemHelper(str(root))
    protected = remote_file_ops._protected_names(config)
    collect = lambda pats, recursive=True: remote_file_ops._collect_files_to_delete(fs, pats, recursive, 2, protected)
    assert collect(["v1/*.dat"]) == {"v1/a.dat"}                                  # wildcard (was never matched)
    assert collect(["v1/"]) == {"v1/a.dat", "v1/b.txt", "v1/sub/c.dat"}           # directory with slash
    assert collect(["v1"]) == {"v1/a.dat", "v1/b.txt", "v1/sub/c.dat"}            # directory without slash
    assert collect(["v2/d.dat", "nope.dat"]) == {"v2/d.dat"}                      # exact file; missing skipped
    assert collect(["*"], recursive=False) == set()                               # only journals at top level
    assert collect(["myjournal.db", "journal.db", "myjournal.db.locked"]) == set()


@pytest.mark.parametrize("answer, deleted", [("", False), ("n", False), ("maybe", False), ("y", True), ("YES", True)])
def test_remote_delete_needs_explicit_yes(central, monkeypatch, answer, deleted):
    root, config = central
    monkeypatch.setattr("builtins.input", lambda: answer)
    remote_file_ops._delete_remote_files(types.SimpleNamespace(files=["v2/d.dat"], recursive=False), config, None)
    assert (root / "v2/d.dat").exists() == (not deleted)


def test_remote_delete_aborts_on_end_of_input(central, monkeypatch):
    root, config = central
    def eof():
        raise EOFError
    monkeypatch.setattr("builtins.input", eof)
    remote_file_ops._delete_remote_files(types.SimpleNamespace(files=["v2/d.dat"], recursive=False), config, None)
    assert (root / "v2/d.dat").exists()


# ---------------- local destination, thread count

def test_upload_to_local_destination_end_to_end(site):
    """Real copy + verify through the thread path; used to fail in _clone_client(None)."""
    assert storage_helper._clone_client(None, pool_size=4) is None
    dest = site.tmp / "dest"
    upload_ops.upload_files(FileSystemHelper(str(site.src)), FileSystemHelper(str(dest)), [MOD], site.db, page_size=2)
    assert all(v is not None for v in upload_state(site.db).values())
    copied = sorted(p.read_bytes() for p in dest.rglob("*.dat"))
    assert copied == sorted(p.read_bytes() for p in site.src.rglob("*.dat"))


def test_configured_thread_count_is_honored(site, monkeypatch):
    assert local_ops.worker_threads(None) == min(32, (os.cpu_count() or 1) + 4)
    assert local_ops.worker_threads(3) == 3
    seen = {}

    def fake_parallel(src, dest, page_source, group_stats, db, dt, update_args, step, perf, nthreads,
                      verbose=False, max_files=None):
        seen['nthreads'] = nthreads
        return update_args, perf, set()
    monkeypatch.setattr(upload_ops, "_parallel_upload", fake_parallel)
    upload_ops.upload_files(FileSystemHelper(str(site.src)), FileSystemHelper(str(site.tmp / "d")), [MOD], site.db,
                            num_threads=3)
    assert seen['nthreads'] == 3


# ---------------- S3 / GCS

@pytest.fixture
def s3(monkeypatch):
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(k, raising=False)
    # used to raise TypeError: the client was built with keyword names S3Client does not have.
    client, host = storage_helper._make_client(
        {"path": "s3://bucket/root", "auth": {"aws_access_key_id": "test-id", "aws_secret_access_key": "test-secret"}})
    assert host is None and client.sess.get_credentials().access_key == "test-id"
    clone = storage_helper._clone_client(client)
    assert clone is not client and clone.sess.get_credentials().access_key == "test-id"
    return client


def test_s3_path_helper_and_upload(s3, tmp_path, monkeypatch):
    from cloudpathlib import S3Path
    fs = FileSystemHelper("s3://bucket/root", client=s3)            # used to raise AttributeError (.container)
    assert fs.is_cloud and fs.container == "bucket"

    uploads = []
    monkeypatch.setattr(S3Path, "mkdir", lambda self, **kw: None)
    monkeypatch.setattr(S3Path, "upload_from", lambda self, src, **kw: uploads.append((str(src), str(self))))
    write(tmp_path, "a/b.dat")
    FileSystemHelper(str(tmp_path)).copy_file_to(relpath=("a/b.dat", "c/d.dat"), dest_path=fs)
    assert uploads == [(str(tmp_path / "a/b.dat"), "s3://bucket/root/c/d.dat")]


def test_s3_multipart_etag_md5(s3, monkeypatch):
    from cloudpathlib import S3Path
    monkeypatch.setattr(S3Path, "exists", lambda self: True)
    monkeypatch.setattr(S3Path, "stat", lambda self: os.stat_result((0, 0, 0, 0, 0, 0, 5, 0, 1_700_000_000, 0)))
    monkeypatch.setattr(S3Path, "etag", property(lambda self: '"abcdef-3"'))
    root = FileSystemHelper("s3://bucket/root", client=s3).root
    get = lambda **kw: FileSystemHelper.get_metadata(root=root, path="x.dat", with_metadata=True, with_md5=True, **kw)
    assert get(local_md5="m" * 32)["md5"] == "m" * 32          # multipart etag: supplied md5 is used
    assert get()["md5"] == "abcdef-3" and get()["size"] == 5   # none supplied: etag returned as is
    assert upload_ops._remote_outcome(get(), 5, "m" * 32) == "no_md5"


def test_gs_path_reports_missing_library():
    pytest.importorskip("cloudpathlib")
    try:
        import google.cloud.storage  # noqa: F401
        pytest.skip("google-cloud-storage installed; client creation needs credentials")
    except ImportError:
        pass
    with pytest.raises(ValueError, match="gs"):
        storage_helper._make_client({"path": "gs://bucket/root"})
    with pytest.raises(ValueError, match="Unknown cloud storage provider"):
        storage_helper._make_client({"path": "ftp://host/root"})


# ---------------- Azure auth

KEY = "a2V5a2V5a2V5"   # dummy base64, not a real key


def az_client(auth):
    return storage_helper._make_client({"path": "az://container/root", "auth": auth})


def test_azure_connection_string_and_key_auth(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    conn = f"DefaultEndpointsProtocol=https;AccountName=acct1;AccountKey={KEY};EndpointSuffix=core.windows.net"
    for auth in ({"azure_storage_connection_string": conn},
                 {"azure_connection_string": conn},                       # name used in the config template
                 {"azure_account_name": "acct1", "azure_account_key": KEY},
                 {"auth_mode": "key", "azure_account_name": "acct1", "azure_account_key": KEY}):
        client, host = az_client(auth)
        svc = client.service_client
        assert svc.account_name == "acct1" and svc.credential.account_key == KEY
        assert host == "acct1.blob.core.windows.net"

    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", conn.replace("acct1", "envacct"))
    client, host = az_client({})                                          # nothing in config: environment
    assert client.service_client.account_name == "envacct" and host == "envacct.blob.core.windows.net"


def test_azure_login_and_sas_modes_unchanged(monkeypatch):
    from azure.identity import DefaultAzureCredential
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    client, host = az_client({"auth_mode": "login", "azure_account_name": "acct2", "azure_account_key": KEY})
    assert isinstance(client.service_client.credential, DefaultAzureCredential)   # explicit login wins over a key
    assert host == "acct2.blob.core.windows.net"
    client, _ = az_client({"auth_mode": "sas", "azure_account_name": "acct3", "azure_sas_token": "sv=1&sig=abc"})
    assert client.service_client.account_name == "acct3"
    with pytest.raises(ValueError):
        az_client({"auth_mode": "bogus", "azure_account_name": "acct"})
    with pytest.raises(ValueError):
        az_client({"auth_mode": "connection_string"})                    # no string anywhere
    assert storage_helper._redact_url("https://a.blob.core.windows.net/c/b?sv=1&sig=secret") == \
        "https://a.blob.core.windows.net/c/b"


# ---------------- copy errors are raised, not logged and ignored

def test_local_copy_failure_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        FileSystemHelper(str(tmp_path)).copy_file_to(relpath="missing.dat", dest_path=FileSystemHelper(str(tmp_path / "d")))


# ---------------- mark_as_uploaded: upload time is not a journal version

def test_mark_as_uploaded_with_upload_time_different_from_journal_version(site):
    import shutil
    patterns = local_ops._compile_patterns([MOD], {})
    dest = site.tmp / "dest"
    for page in local_ops.iter_upload_pages(site.db, [MOD], 100):
        for fn, info in page:
            target = dest / "v1" / info["central_path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(site.src / fn, target)
    upload_ops.mark_as_uploaded(FileSystemHelper(str(dest)), site.db, "20250607080910",
                                ["P001/Waveforms/a.dat", "P001/Waveforms/b.dat"],
                                compiled_patterns=patterns, modality_configs={})
    state = upload_state(site.db)
    assert state["P001/Waveforms/a.dat"] == state["P001/Waveforms/b.dat"] == "20250607080910"
    assert state["P002/Waveforms/sub/c.dat"] is None


# ---------------- journal list --modalities, multi-modality update, small fixes

def test_update_journal_two_modalities_in_one_call_and_list_by_modality(tmp_path, capsys):
    src, db = tmp_path / "src", str(tmp_path / "journal.db")
    write(src, "P001/Waveforms/a.dat")
    write(src, "P001/Waveforms/b.dat")
    write(src, "P001/Images/i.dcm")
    fs = FileSystemHelper(str(src))
    for version in ("v1", "v2"):          # create, then update: both loops used to `del perf` per modality
        local_ops.update_journal(fs, ["waveforms", "images"], db, journaling_mode="full", version=version, page_size=2)

    everything = local_ops.list_versions(db, None)
    assert int(everything["upload"].sum()) == 3
    only = local_ops.list_versions(db, None, modalities=["waveforms"])
    assert int(only["upload"].sum()) == 2 and not [c for c in only.columns if c.startswith("images")]
    assert "images_upload" in everything.columns


def test_adaptive_threads_locks_after_slowdown():
    tuner = local_ops._AdaptiveThreads(initial=4, max_threads=32)
    tuner.update(100, 1.0)      # baseline
    tuner.update(200, 1.0)      # faster -> double
    assert tuner.nthreads == 8 and not tuner.locked
    tuner.update(100, 1.0)      # slower -> halve and lock
    assert tuner.nthreads == 4 and tuner.locked
    tuner.update(1000, 1.0)     # locked: no more changes
    assert tuner.nthreads == 4


def test_modalities_are_stripped():
    mods = config_helper.get_modalities({"configuration": {"supported_modalities": "OMOP, Waveforms ,Images"}})
    assert mods == ["OMOP", "Waveforms", "Images", "Metadata"]


def test_async_client_follows_connection_verify(monkeypatch):
    import asyncio
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    seen = []

    class FakeAsyncClient:
        def __init__(self, **kw):
            seen.append(("client", kw["connection_verify"], kw["account_url"]))
    monkeypatch.setattr(upload_ops, "AsyncBlobServiceClient", FakeAsyncClient)
    monkeypatch.setattr(upload_ops, "AioHttpTransport",
                        lambda **kw: seen.append(("transport", kw["connection_verify"])))
    monkeypatch.setattr(upload_ops, "aiohttp", types.SimpleNamespace(
        TCPConnector=lambda **kw: None, ClientSession=lambda **kw: None))

    async def build(auth):
        client, _ = az_client(auth)
        clone = storage_helper._clone_client(client, pool_size=2)
        assert clone._connection_verify == client._connection_verify
        upload_ops._async_blob_service_client(clone, 4)

    base = {"azure_account_name": "acct1", "azure_account_key": KEY}
    asyncio.run(build(base))                                   # default: off, like the sync client
    asyncio.run(build({**base, "connection_verify": True}))    # opt-in: on for both
    assert [s[1] for s in seen] == [False, False, True, True]
    assert seen[1][2].startswith("https://acct1.blob.core.windows.net")
