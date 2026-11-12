"""SQLite layer: quoting of embedded values, all-or-nothing writes; command history."""
import os
import sqlite3

import pytest

from chorus_upload import history_ops, local_ops
from chorus_upload.journaldb_ops import JournalDispatcher, SQLiteDB
from chorus_upload.storage_helper import FileSystemHelper


def rows(db, sql):
    with sqlite3.connect(db) as c:
        return c.execute(sql).fetchall()


def test_apostrophe_in_lookup_values(tmp_path):
    db = str(tmp_path / "j.db")
    SQLiteDB.create_lookup_table(db, "srcpaths", "SRC_PATH")
    ids = SQLiteDB.insert_lookup_table(db, "srcpaths", "SRC_PATH", {"P001/O'Brien/Waveforms", "plain"})
    assert set(ids) == {"P001/O'Brien/Waveforms", "plain"}
    assert SQLiteDB.table_exists(db, "srcpaths") and not SQLiteDB.table_exists(db, "x'y")


def test_journal_update_and_paging_with_apostrophe_paths(tmp_path):
    src, db = tmp_path / "src", str(tmp_path / "journal.db")
    rel = "P001/Waveforms/O'Brien/it's.dat"
    (src / rel).parent.mkdir(parents=True)
    (src / rel).write_bytes(b"x")
    for version in ("v'1", "v'2"):       # second run exercises the update path and its lookups
        local_ops.update_journal(FileSystemHelper(str(src)), ["waveforms"], db,
                                 journaling_mode="full", version=version, page_size=2)
    page = JournalDispatcher.get_files_page(db, ["waveforms"], version="v'1", active=True)
    assert [r[1] for r in page] == [rel]
    assert JournalDispatcher.get_files_by_paths(db, [rel], active=True)[0][1] == rel


def test_insert_is_all_or_nothing_and_raises(tmp_path):
    db = str(tmp_path / "j.db")
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t (a INTEGER NOT NULL, b TEXT)")
    assert SQLiteDB.insert(db, "t", ["a", "b"], []) == 0                 # int, not a tuple
    assert SQLiteDB.insert(db, "t", ["a", "b"], [(1, "x"), (2, "y")]) == 2
    with pytest.raises(sqlite3.IntegrityError):
        SQLiteDB.insert(db, "t", ["a", "b"], [(3, "ok"), (None, "bad"), (5, "never")])
    assert rows(db, "SELECT a FROM t ORDER BY a") == [(1,), (2,)]        # (3, ok) was rolled back


def test_update_is_all_or_nothing_and_raises(tmp_path):
    db = str(tmp_path / "j.db")
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT NOT NULL)")
        c.executemany("INSERT INTO t VALUES (?, ?)", [(1, "x"), (2, "y")])
    assert SQLiteDB.update(db, "t", ["b = ?"], [("z", 1)], "a = ?") == 1
    with pytest.raises(sqlite3.IntegrityError):
        SQLiteDB.update(db, "t", ["b = ?"], [("w", 1), (None, 2)], "a = ?")
    assert rows(db, "SELECT b FROM t ORDER BY a") == [("z",), ("y",)]    # 'w' was rolled back


def test_query_does_not_modify_callers_columns(tmp_path):
    db = str(tmp_path / "j.db")
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t (a INTEGER, b TEXT)")
    cols = ["COUNT(a)"]
    SQLiteDB.query(db, "t", cols, groupby=["b"])
    assert cols == ["COUNT(a)"]


def test_history_command_and_quotes_in_arguments(tmp_path):
    # `history` has no sub-command; used to raise UnboundLocalError before running.
    assert history_ops._recreate_params({"command": "history", "verbose": True, "config": "c.toml"}) == \
        ("--verbose --config c.toml", "history", "")
    common, cmd, params = history_ops._recreate_params(
        {"command": "file", "file_command": "mark_as_deleted", "file": "P001/O'Brien/a.dat", "verbose": False})
    assert (cmd, params) == ("file mark_as_deleted", "--file P001/O'Brien/a.dat")

    db = str(tmp_path / "j.db")
    cid = history_ops.save_command_history(common, cmd, params, None, "az://c/it's", db)
    history_ops.update_command_completion(cid, 1.5, db)
    stored = rows(db, "SELECT COMMAND, PARAMS, SRC_PATHS, DEST_PATH, DURATION FROM command_history_v2")
    assert stored == [("file mark_as_deleted", "--file P001/O'Brien/a.dat", "None", "az://c/it's", 1.5)]
