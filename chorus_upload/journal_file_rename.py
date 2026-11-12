#!/usr/bin/env python3
import sqlite3
from contextlib import closing
from pathlib import Path
import csv
import argparse
import logging
import sys

from chorus_upload.journaldb_ops import SQLiteDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s:%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def backup(database_name):
    """Create a backup of journal_v2 table before any modifications."""
    log.info(f"Creating backup of {database_name}")

    if SQLiteDB.table_exists(database_name, "journal_v2_backup"):
        log.error("Backup table 'journal_v2_backup' already exists. Run 'remove-backup' first or rename it manually.")
        return False

    try:
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute("CREATE TABLE journal_v2_backup AS SELECT * FROM journal_v2")
            conn.commit()

        # Count rows backed up
        count_result = SQLiteDB.query(database_name, "journal_v2_backup", ["COUNT(*)"])
        row_count = count_result[0][0] if count_result else 0
        log.info(f"Successfully backed up {row_count} rows to 'journal_v2_backup'")
        return True
    except Exception as e:
        log.error(f"Failed to create backup: {e}")
        return False


def read_csv(csv_path):
    """Read rename CSV and return list of (old_path, new_path) tuples, normalized."""
    rows = []
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                old_path = row['old_path'].removeprefix("./")
                new_path = row['new_path'].removeprefix("./")
                rows.append((old_path, new_path))
        log.info(f"Read {len(rows)} rows from {csv_path}")
        return rows
    except Exception as e:
        log.error(f"Failed to read CSV {csv_path}: {e}")
        return None


def resolve_modality_id(database_name, modality_name):
    """Resolve modality name to ID. Returns ID or None if not found."""
    result = SQLiteDB.query(database_name, "modalities", ["id"],
                           where_clause=f"MODALITY = {SQLiteDB.quote(modality_name)}",
                           max_return=1)
    if result:
        # fetchone() returns a single tuple, not a list
        return result[0]

    # List available modalities
    available = SQLiteDB.get_unique_values(database_name, "modalities", "MODALITY")
    log.error(f"Modality '{modality_name}' not found in database.")
    if available:
        log.error(f"Available modalities: {', '.join([m[0] for m in available])}")
    return None


def query_single_result(database_name, table_name, columns, where_clause):
    """Query and return the first result as a tuple (for max_return=1). Returns None if no result."""
    result = SQLiteDB.query(database_name, table_name, columns, where_clause=where_clause, max_return=1)
    return result  # Already a tuple or None from fetchone()


def scan_srcpath_changes(rows):
    """
    Scan CSV rows and classify into same-dir and different-dir changes.
    Returns (same_dir_rows, diff_dir_rows).
    """
    same_dir = []
    diff_dir = []

    for old_path, new_path in rows:
        old_parent = str(Path(old_path).parent)
        new_parent = str(Path(new_path).parent)

        if old_parent == new_parent:
            same_dir.append((old_path, new_path))
        else:
            diff_dir.append((old_path, new_path))

    return same_dir, diff_dir


def prompt_srcpath_changes(diff_dir_rows):
    """
    Prompt user about srcpath changes. Returns True to allow, False to deny.
    """
    if not diff_dir_rows:
        return True

    print(f"\n{'='*60}")
    print(f"Found {len(diff_dir_rows)} row(s) with directory changes:")
    for old_path, new_path in diff_dir_rows:
        print(f"  {old_path} → {new_path}")
    print(f"{'='*60}")

    response = input("Create new srcpath entries and update these rows? [y/N] ").strip().lower()
    return response == 'y'


def apply(database_name, csv_path, modality_name, srcpath_change_policy):
    """Apply file renames to the journal_v2 table."""
    log.info(f"Applying renames from {csv_path} to {database_name} for modality '{modality_name}'")

    # Check backup exists
    if not SQLiteDB.table_exists(database_name, "journal_v2_backup"):
        log.error("Backup table 'journal_v2_backup' does not exist. Run 'backup' first.")
        return False

    # Resolve modality
    modality_id = resolve_modality_id(database_name, modality_name)
    if modality_id is None:
        return False

    # Parse CSV
    rows = read_csv(csv_path)
    if rows is None:
        return False

    # Classify rows
    same_dir_rows, diff_dir_rows = scan_srcpath_changes(rows)

    # Decide on srcpath changes
    allow_srcpath_changes = False
    excluded_rows = []

    if diff_dir_rows:
        if srcpath_change_policy == "ask":
            allow_srcpath_changes = prompt_srcpath_changes(diff_dir_rows)
        elif srcpath_change_policy == "allow":
            allow_srcpath_changes = True
            log.info(f"Allowing {len(diff_dir_rows)} srcpath changes (policy: allow)")
        elif srcpath_change_policy == "deny":
            allow_srcpath_changes = False
            log.info(f"Excluding {len(diff_dir_rows)} srcpath changes (policy: deny)")

        if not allow_srcpath_changes:
            excluded_rows = diff_dir_rows
            log.info(f"Excluding {len(diff_dir_rows)} rows with directory changes from this run")

    # Process same-dir rows (filename-only renames)
    update_params_filename_only = []
    not_found_rows = []
    matched_rows = 0

    for old_path, new_path in same_dir_rows:
        old_parent = Path(old_path).parent.as_posix()
        old_filename = Path(old_path).name
        new_filename = Path(new_path).name

        # Look up SRC_PATH_ID for old parent dir
        src_path_result = query_single_result(database_name, "srcpaths", ["id"],
                                             f"SRC_PATH = {SQLiteDB.quote(old_parent)}")
        if not src_path_result:
            log.warning(f"Old path not found: {old_path} (directory '{old_parent}' not in srcpaths)")
            not_found_rows.append((old_path, "old path not found"))
            continue

        src_path_id = src_path_result[0]

        # Find matching FILE_IDs for this path and modality
        file_ids = SQLiteDB.query(database_name, "journal_v2", ["FILE_ID"],
                                 where_clause=f"SRC_PATH_ID = {src_path_id} AND FILENAME = {SQLiteDB.quote(old_filename)} AND MODALITY_ID = {modality_id}")

        if not file_ids:
            log.warning(f"No matching rows found for {old_path} (modality: {modality_name})")
            not_found_rows.append((old_path, "no matching rows for this modality"))
            continue

        matched_rows += len(file_ids)
        for (file_id,) in file_ids:
            update_params_filename_only.append((new_filename, file_id))

    # Process allowed diff-dir rows (with srcpath changes)
    update_params_with_srcpath = []
    new_srcpaths = set()

    for old_path, new_path in diff_dir_rows:
        if not allow_srcpath_changes:
            continue

        old_parent = Path(old_path).parent.as_posix()
        old_filename = Path(old_path).name
        new_parent = Path(new_path).parent.as_posix()
        new_filename = Path(new_path).name

        # Collect new srcpath for get-or-create
        new_srcpaths.add(new_parent)

        # Look up SRC_PATH_ID for old parent dir
        src_path_result = query_single_result(database_name, "srcpaths", ["id"],
                                             f"SRC_PATH = {SQLiteDB.quote(old_parent)}")
        if not src_path_result:
            log.warning(f"Old path not found: {old_path} (directory '{old_parent}' not in srcpaths)")
            not_found_rows.append((old_path, "old path not found"))
            continue

        src_path_id = src_path_result[0]

        # Find matching FILE_IDs for this path and modality
        file_ids = SQLiteDB.query(database_name, "journal_v2", ["FILE_ID"],
                                 where_clause=f"SRC_PATH_ID = {src_path_id} AND FILENAME = {SQLiteDB.quote(old_filename)} AND MODALITY_ID = {modality_id}")

        if not file_ids:
            log.warning(f"No matching rows found for {old_path} (modality: {modality_name})")
            not_found_rows.append((old_path, "no matching rows for this modality"))
            continue

        matched_rows += len(file_ids)

    # Get or create new srcpaths
    new_srcpath_ids = {}
    if new_srcpaths:
        log.info(f"Get-or-creating {len(new_srcpaths)} new srcpath entries")
        new_srcpath_ids = SQLiteDB.insert_lookup_table(database_name, "srcpaths", "SRC_PATH", new_srcpaths)

    # Build update params for diff-dir rows now that we have new srcpath IDs
    for old_path, new_path in diff_dir_rows:
        if not allow_srcpath_changes:
            continue

        old_parent = Path(old_path).parent.as_posix()
        old_filename = Path(old_path).name
        new_parent = Path(new_path).parent.as_posix()
        new_filename = Path(new_path).name

        # Look up SRC_PATH_ID for old parent dir
        src_path_result = query_single_result(database_name, "srcpaths", ["id"],
                                             f"SRC_PATH = {SQLiteDB.quote(old_parent)}")
        if not src_path_result:
            continue

        src_path_id = src_path_result[0]
        new_src_path_id = new_srcpath_ids.get(new_parent)

        # Find matching FILE_IDs for this path and modality
        file_ids = SQLiteDB.query(database_name, "journal_v2", ["FILE_ID"],
                                 where_clause=f"SRC_PATH_ID = {src_path_id} AND FILENAME = {SQLiteDB.quote(old_filename)} AND MODALITY_ID = {modality_id}")

        if file_ids:
            for (file_id,) in file_ids:
                update_params_with_srcpath.append((new_filename, new_src_path_id, file_id))

    # Execute updates
    rows_updated = 0

    # Filename-only updates
    if update_params_filename_only:
        log.info(f"Updating {len(update_params_filename_only)} rows (filename-only)")
        count = SQLiteDB.update(database_name, "journal_v2",
                               sets=["FILENAME=?"],
                               params=update_params_filename_only,
                               where_clause="FILE_ID=?")
        rows_updated += count

    # Srcpath + filename updates
    if update_params_with_srcpath:
        log.info(f"Updating {len(update_params_with_srcpath)} rows (filename + srcpath)")
        count = SQLiteDB.update(database_name, "journal_v2",
                               sets=["FILENAME=?", "SRC_PATH_ID=?"],
                               params=update_params_with_srcpath,
                               where_clause="FILE_ID=?")
        rows_updated += count

    # Print summary
    print(f"\n{'='*60}")
    print(f"Apply Summary:")
    print(f"  CSV rows processed: {len(rows)}")
    print(f"  Rows matched in DB: {matched_rows}")
    print(f"  Rows updated: {rows_updated}")
    print(f"  Rows not found: {len(not_found_rows)}")
    print(f"  Rows excluded (srcpath policy): {len(excluded_rows)}")
    if not_found_rows:
        print(f"\n  Not found rows:")
        for old_path, reason in not_found_rows:
            print(f"    {old_path} ({reason})")
    if excluded_rows:
        print(f"\n  Excluded rows (srcpath changes):")
        for old_path, new_path in excluded_rows:
            print(f"    {old_path} → {new_path}")
    print(f"{'='*60}\n")

    return True


def verify(database_name, csv_path, modality_name):
    """Verify that renames were applied correctly."""
    log.info(f"Verifying renames in {database_name} for modality '{modality_name}'")

    # Check backup exists
    if not SQLiteDB.table_exists(database_name, "journal_v2_backup"):
        log.error("Backup table 'journal_v2_backup' does not exist. Run 'backup' first.")
        return False

    # Resolve modality
    modality_id = resolve_modality_id(database_name, modality_name)
    if modality_id is None:
        return False

    # Parse CSV
    rows = read_csv(csv_path)
    if rows is None:
        return False

    # Classify rows to understand expected behavior
    same_dir_rows, diff_dir_rows = scan_srcpath_changes(rows)

    passed = 0
    failed = 0
    not_renamed = []

    # Verify each row
    for old_path, new_path in rows:
        old_parent = Path(old_path).parent.as_posix()
        old_filename = Path(old_path).name
        new_parent = Path(new_path).parent.as_posix()
        new_filename = Path(new_path).name

        # Look up SRC_PATH_ID for old parent dir in backup
        src_path_result = query_single_result(database_name, "srcpaths", ["id"],
                                             f"SRC_PATH = {SQLiteDB.quote(old_parent)}")
        if not src_path_result:
            log.warning(f"Old path not found: {old_path}")
            not_renamed.append((old_path, "old path not present in journal for this modality"))
            continue

        old_src_path_id = src_path_result[0]

        # Find matching FILE_IDs in backup table
        backup_file_ids = SQLiteDB.query(database_name, "journal_v2_backup", ["FILE_ID"],
                                        where_clause=f"SRC_PATH_ID = {old_src_path_id} AND FILENAME = {SQLiteDB.quote(old_filename)} AND MODALITY_ID = {modality_id}")

        if not backup_file_ids:
            log.warning(f"No matching rows found in backup for {old_path}")
            not_renamed.append((old_path, "no matching rows found in backup for this modality"))
            continue

        # For each found FILE_ID, verify the current state
        for (file_id,) in backup_file_ids:
            # Fetch current row
            current_result = query_single_result(database_name, "journal_v2",
                                                ["FILE_ID", "SRC_PATH_ID", "FILENAME", "PERSON_ID", "MODALITY_ID",
                                                 "SRC_MODTIME_us", "SIZE", "MD5", "TIME_VALID_us", "TIME_INVALID_us",
                                                 "UPLOAD_DT_ID", "VERSION_ID"],
                                                f"FILE_ID = {file_id}")

            if not current_result:
                log.error(f"FILE_ID {file_id} missing from current table")
                failed += 1
                not_renamed.append((old_path, "file ID missing from current table"))
                continue

            current_row = current_result
            cur_filename = current_row[2]
            cur_src_path_id = current_row[1]

            # Check if filename was updated
            if cur_filename == old_filename and cur_src_path_id == old_src_path_id:
                # Row still has old values — not renamed
                not_renamed.append((old_path, "no changes applied"))
                continue

            # Check if filename matches expected new filename
            if cur_filename != new_filename:
                log.error(f"FILE_ID {file_id}: filename mismatch. Expected '{new_filename}', got '{cur_filename}'")
                failed += 1
                not_renamed.append((old_path, f"filename mismatch (expected '{new_filename}', got '{cur_filename}')"))
                continue

            # If a srcpath change was expected, verify that too
            if old_parent != new_parent:
                # This row should have a new SRC_PATH_ID
                new_src_path_result = query_single_result(database_name, "srcpaths", ["id"],
                                                         f"SRC_PATH = {SQLiteDB.quote(new_parent)}")
                if not new_src_path_result:
                    log.error(f"FILE_ID {file_id}: new srcpath '{new_parent}' not found in database")
                    failed += 1
                    not_renamed.append((old_path, f"new srcpath not found in database"))
                    continue

                expected_src_path_id = new_src_path_result[0]
                if cur_src_path_id != expected_src_path_id:
                    log.error(f"FILE_ID {file_id}: srcpath mismatch. Expected ID {expected_src_path_id}, got {cur_src_path_id}")
                    failed += 1
                    not_renamed.append((old_path, f"srcpath mismatch"))
                    continue
            else:
                # Same directory expected — srcpath should not change
                if cur_src_path_id != old_src_path_id:
                    log.error(f"FILE_ID {file_id}: srcpath unexpectedly changed")
                    failed += 1
                    not_renamed.append((old_path, f"srcpath unexpectedly changed"))
                    continue

            # Verify all other columns are unchanged
            # Fetch backup row for comparison
            backup_result = query_single_result(database_name, "journal_v2_backup",
                                               ["FILE_ID", "SRC_PATH_ID", "FILENAME", "PERSON_ID", "MODALITY_ID",
                                                "SRC_MODTIME_us", "SIZE", "MD5", "TIME_VALID_us", "TIME_INVALID_us",
                                                "UPLOAD_DT_ID", "VERSION_ID"],
                                               f"FILE_ID = {file_id}")

            if not backup_result:
                log.error(f"FILE_ID {file_id} missing from backup table")
                failed += 1
                not_renamed.append((old_path, "file ID missing from backup"))
                continue

            backup_row = backup_result

            # Compare non-changed columns
            columns_to_check = [3, 4, 5, 6, 7, 8, 9, 10, 11]  # PERSON_ID through VERSION_ID
            column_names = ["PERSON_ID", "MODALITY_ID", "SRC_MODTIME_us", "SIZE", "MD5", "TIME_VALID_us", "TIME_INVALID_us", "UPLOAD_DT_ID", "VERSION_ID"]

            mismatch = False
            for idx, col_name in zip(columns_to_check, column_names):
                if current_row[idx] != backup_row[idx]:
                    log.error(f"FILE_ID {file_id}: {col_name} changed unexpectedly (backup={backup_row[idx]}, current={current_row[idx]})")
                    mismatch = True

            if mismatch:
                failed += 1
                not_renamed.append((old_path, "unexpected changes in other columns"))
                continue

            # All checks passed
            passed += 1

    # Print summary
    print(f"\n{'='*60}")
    print(f"Verify Summary:")
    print(f"  CSV rows checked: {len(rows)}")
    print(f"  Rows verified passed: {passed}")
    print(f"  Rows with issues: {failed}")
    print(f"  Files not renamed: {len(not_renamed)}")

    if not_renamed:
        print(f"\n  Not renamed (with reasons):")
        for old_path, reason in not_renamed:
            print(f"    {old_path} ({reason})")

    print(f"{'='*60}\n")

    # Exit with error if there are any actual failures (not counting policy-excluded rows)
    if failed > 0:
        return False

    return True


def remove_backup(database_name):
    """Remove the backup table."""
    log.info(f"Removing backup table from {database_name}")

    try:
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute("DROP TABLE IF EXISTS journal_v2_backup")
            conn.commit()

        log.info("Successfully removed 'journal_v2_backup'")
        return True
    except Exception as e:
        log.error(f"Failed to remove backup: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Rename files in journal_v2 database")
    parser.add_argument("-v", "--verbose", help="verbose output", action="store_true")

    subparsers = parser.add_subparsers(dest="command", help="subcommand")

    # backup subcommand
    backup_parser = subparsers.add_parser("backup", help="Create backup of journal_v2 table")
    backup_parser.add_argument("--db", required=True, help="journal database file")

    # apply subcommand
    apply_parser = subparsers.add_parser("apply", help="Apply file renames from CSV")
    apply_parser.add_argument("--db", required=True, help="journal database file")
    apply_parser.add_argument("--csv", required=True, help="CSV file with old_path,new_path columns")
    apply_parser.add_argument("--modality", required=True, help="modality to operate on (e.g., Images, Waveforms)")
    apply_parser.add_argument("--srcpath-change-policy", choices=["ask", "allow", "deny"], default="ask",
                             help="how to handle rows with directory changes (default: ask)")

    # verify subcommand
    verify_parser = subparsers.add_parser("verify", help="Verify that renames were applied correctly")
    verify_parser.add_argument("--db", required=True, help="journal database file")
    verify_parser.add_argument("--csv", required=True, help="CSV file with old_path,new_path columns")
    verify_parser.add_argument("--modality", required=True, help="modality to verify (e.g., Images, Waveforms)")

    # remove-backup subcommand
    remove_parser = subparsers.add_parser("remove-backup", help="Remove the backup table")
    remove_parser.add_argument("--db", required=True, help="journal database file")

    args = parser.parse_args()

    # Set verbose logging if requested
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Dispatch to subcommand
    if args.command == "backup":
        if not backup(args.db):
            sys.exit(1)
    elif args.command == "apply":
        if not apply(args.db, args.csv, args.modality, args.srcpath_change_policy):
            sys.exit(1)
    elif args.command == "verify":
        if not verify(args.db, args.csv, args.modality):
            sys.exit(1)
    elif args.command == "remove-backup":
        if not remove_backup(args.db):
            sys.exit(1)


if __name__ == "__main__":
    main()
