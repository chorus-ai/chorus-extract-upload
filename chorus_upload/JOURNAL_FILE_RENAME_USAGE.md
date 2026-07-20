# journal_file_rename.py — Usage Guide

## Overview

`journal_file_rename.py` is a utility script for bulk-renaming files in the `journal_v2` table of `journal.v2.db`, driven by a CSV mapping of old→new file paths. It ensures safety by:

1. Creating a backup table before any changes
2. Applying renames only to the specified modality (Images, Waveforms, etc.)
3. Supporting interactive prompts for directory changes (srcpath updates)
4. Verifying that all renames were applied correctly
5. Allowing cleanup of the backup table once verified

## Quick Start

```bash
# 1. Create a backup
python3 journal_file_rename.py backup --db journal.v2.db

# 2. Apply renames (filename-only; srcpath changes require explicit approval)
python3 journal_file_rename.py apply --db journal.v2.db --csv renames.csv --modality Waveforms

# 3. Verify the changes
python3 journal_file_rename.py verify --db journal.v2.db --csv renames.csv --modality Waveforms

# 4. Remove the backup (only if verify passed)
python3 journal_file_rename.py remove-backup --db journal.v2.db
```

## Subcommands

### `backup --db DATABASE`

Creates a snapshot of the `journal_v2` table before any modifications.

- **Fails if**: Backup table `journal_v2_backup` already exists (user must remove it first).
- **Logs**: Number of rows backed up.

### `apply --db DATABASE --csv CSV_FILE --modality MODALITY [--srcpath-change-policy POLICY]`

Applies file renames from a CSV to the journal database.

**Required Arguments:**
- `--db`: Path to `journal.v2.db`
- `--csv`: CSV file with `old_path,new_path` columns (see format below)
- `--modality`: Modality to filter on (e.g., `Images`, `Waveforms`). Only rows with this modality will be processed. Use `python3 journal_file_rename.py apply --db journal.v2.db --csv dummy.csv --modality Unknown` to list available modalities.

**Optional Arguments:**
- `--srcpath-change-policy {ask,allow,deny}` (default: `ask`)
  - `ask`: Prompt user before creating new directory entries for files that change parent directories.
  - `allow`: Automatically create new directory entries without prompting.
  - `deny`: Skip any files that would move to a different parent directory.

**Output Summary:**
- Rows processed: Total CSV rows read
- Rows matched: Files found in the database for the given modality
- Rows updated: Actual number of rows modified
- Rows not found: CSV rows with no matching database entries (old path not in DB for this modality)
- Rows excluded: CSV rows skipped due to srcpath policy

### `verify --db DATABASE --csv CSV_FILE --modality MODALITY`

Verifies that renames were applied correctly by comparing backup and current rows.

**Output Summary:**
- Rows checked: Total CSV rows verified
- Rows verified passed: Renames applied correctly
- Rows with issues: Verification failures (exits non-zero)
- Files not renamed: List of rows that were not renamed (with reasons):
  - "old path not present in journal" — file not found in original database
  - "no changes applied" — file still has original name/path (update failed or was excluded by policy)
  - "unexpected changes in other columns" — other columns changed unexpectedly

**Exit Code:**
- 0: All checked rows either passed verification or were excluded by policy (expected behavior)
- 1: One or more actual errors/mismatches found

### `remove-backup --db DATABASE`

Drops the backup table `journal_v2_backup` after verification is complete.

- Does **not** automatically run verification first — ensure `verify` passed before running this.
- Safe to run multiple times (uses `DROP TABLE IF EXISTS`).

## CSV Format

CSV must have a header row with `old_path,new_path` columns (matches the convention of `waveform_file_rename.csv`).

**Example:**
```csv
old_path,new_path
./data/waveforms/sub1/file_old.hdf5,./data/waveforms/sub1/file_new.hdf5
./data/waveforms/sub2/file_old.hdf5,../new_data/waveforms/sub2/file_new.hdf5
```

**Path Format:**
- Paths with leading `./` are automatically normalized (the dot-slash is stripped).
- Paths with `../` are treated as directory changes and subject to the `--srcpath-change-policy`.
- Paths are split into parent directory (stored in `srcpaths` lookup table) and filename (stored in `journal_v2.FILENAME`).

**Behavior:**
- **Filename-only renames** (same parent directory): Applied automatically, updating only `FILENAME` column.
- **Directory changes** (different parent): Subject to `--srcpath-change-policy`:
  - If `allow` or user approves at prompt: Updates both `SRC_PATH_ID` and `FILENAME`, creating new `srcpaths` entries as needed.
  - If `deny` or user rejects at prompt: Skipped (reported as "excluded by policy" in verify output).

## Modality

The script operates on a single modality per run. This is necessary because the same filename may legitimately exist in multiple modalities (e.g., `image.jpg` in both the Images and Thumbnails modality).

To list available modalities, run:
```bash
python3 journal_file_rename.py apply --db journal.v2.db --csv dummy.csv --modality NonExistent
```

The error message will list available modalities.

## Matching Behavior

For each CSV row, the script:

1. Splits `old_path` into parent directory and filename using `pathlib.Path`.
2. Looks up the parent directory's `SRC_PATH_ID` from the `srcpaths` table.
3. Queries `journal_v2` for all rows matching:
   - `SRC_PATH_ID` (parent directory ID)
   - `FILENAME` (basename)
   - `MODALITY_ID` (specified modality)
4. **All matching rows** are renamed (not just active ones).

This means if the same file was re-added in multiple versions or upload cycles, all copies get the new name.

## Verification Example

```bash
$ python3 journal_file_rename.py verify --db journal.v2.db --csv renames.csv --modality Waveforms

============================================================
Verify Summary:
  CSV rows checked: 3
  Rows verified passed: 2
  Rows with issues: 0
  Files not renamed: 1

  Not renamed (with reasons):
    ./old_data/file_missing.hdf5 (no matching rows found in backup for this modality)
============================================================
```

Exit code 0: Passed (the one unmatched file is expected, not an error).

## Common Workflows

### Rename files within the same directory
```bash
python3 journal_file_rename.py backup --db journal.v2.db
python3 journal_file_rename.py apply --db journal.v2.db --csv renames.csv --modality Waveforms --srcpath-change-policy deny
python3 journal_file_rename.py verify --db journal.v2.db --csv renames.csv --modality Waveforms
python3 journal_file_rename.py remove-backup --db journal.v2.db
```

### Move files to a new directory (non-interactive)
```bash
python3 journal_file_rename.py backup --db journal.v2.db
python3 journal_file_rename.py apply --db journal.v2.db --csv renames.csv --modality Waveforms --srcpath-change-policy allow
python3 journal_file_rename.py verify --db journal.v2.db --csv renames.csv --modality Waveforms
python3 journal_file_rename.py remove-backup --db journal.v2.db
```

### Interactive review of directory changes
```bash
python3 journal_file_rename.py backup --db journal.v2.db
python3 journal_file_rename.py apply --db journal.v2.db --csv renames.csv --modality Waveforms
# Script pauses and prompts for each directory change
python3 journal_file_rename.py verify --db journal.v2.db --csv renames.csv --modality Waveforms
python3 journal_file_rename.py remove-backup --db journal.v2.db
```

## Error Handling

### Backup table already exists
```
ERROR: Backup table 'journal_v2_backup' already exists. Run 'remove-backup' first or rename it manually.
```
**Solution:** Run `remove-backup` or rename `journal_v2_backup` manually, then retry `backup`.

### Unknown modality
```
ERROR: Modality 'Waveforms2' not found in database.
Available modalities: Images, Waveforms
```
**Solution:** Use one of the listed modalities.

### Backup table doesn't exist when running apply/verify
```
ERROR: Backup table 'journal_v2_backup' does not exist. Run 'backup' first.
```
**Solution:** Run `backup` before `apply` or `verify`.

## Notes

- **All matching rows renamed:** If a file was re-added in different versions, all versions get the same new name. This is intentional.
- **Verification is thorough:** The `verify` step re-queries the database independently and compares backup vs. current rows. It's the authoritative check, not just a count of affected rows.
- **No automatic cleanup:** The backup table is not removed automatically. Run `remove-backup` explicitly once `verify` passes.
- **Logging:** Use `-v` flag on any subcommand for verbose debug output.
