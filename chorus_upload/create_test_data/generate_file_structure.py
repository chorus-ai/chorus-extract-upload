import os
from datetime import datetime
import random
import shutil
import numpy as np
import glob

MB = 1024 * 1024

# ---------------------------------------------------------------------------
# Size category tables
# ---------------------------------------------------------------------------
# Each entry: (label, target_bytes, poisson_lambda)
#
# File size = target_bytes * uniform(0.75, 0.99) — "slightly less than" the
# named power-of-2 MB boundary.  count = max(1, Poisson(lambda)) per category
# guarantees at least one file while Poisson shapes the distribution.
#
# Three tables are used to avoid multiplying large files across sections:
#
#   SIZE_CATEGORIES        — full range, per-patient files only.
#                            Lambdas are tuned so the overall file set
#                            exercises the upload/verify concurrency groups:
#
#     Upload group    concurrency  target files  lambda/sub-cat
#     small  (<1MB)      128          ~64          64
#     medium (1–8MB)      64          ~11 each     11  (<2MB, <4MB, <8MB)
#     large  (8–64MB)     32          ~6 each       6  (<16MB, <32MB, <64MB)
#     xlarge (64–128MB)   16          ~4             4  (<128MB)
#     xxlarge(128–256MB)   8          ~2             2  (<256MB)
#     huge   (≥256MB)      4          ~1             1  (<512MB)
#
#     Expected per-patient total ≈ 2.1 GB (first snapshot)
#
#   GLOBAL_DIR_CATEGORIES  — capped at <32MB, for global OMOP and Metadata
#                            directories.  Large-file coverage is already
#                            provided by SIZE_CATEGORIES; duplicating huge
#                            files here would inflate the dataset needlessly.
#                            Expected per dir ≈ 60 MB.
#
#   UPDATE_NEW_DIR_CATEGORIES — capped at <16MB, for new subdirectories
#                            added in the second snapshot.  This branch can
#                            fire multiple times (once per patient/modality),
#                            so a tight cap keeps the update snapshot lean.
#                            Expected per trigger ≈ 30 MB.

# File counts set to (upload-group concurrency + 2) so every concurrency slot
# is occupied plus a small queue.  Sub-categories within each upload group share
# the total evenly (rounded up so the total exceeds the concurrency limit).
#
# Upload group   concurrency  target total  lambda/sub-cat  sub-cats
# small  (<1MB)      256          258           258             1
# medium (<8MB)      128          130            44             3  (<2MB,<4MB,<8MB)
# large  (<32MB)      64           66            33             2  (<16MB,<32MB)
# xlarge (<256MB)      8           10+           22,5,5         3  (<64MB,<128MB,<256MB)
# huge   (≥256MB)      4            6             6             1  (<512MB)
#
# Expected total for 2 patients, Waveforms only ≈ 7.1 GB
SIZE_CATEGORIES_CONCURRENCY = [
    ('<1MB',   1 * MB,  258),  # small:  ~258 files × 0.87 MB  ≈   224 MB
    ('<2MB',   2 * MB,   44),  # medium: ~44  files × 1.75 MB  ≈    77 MB \
    ('<4MB',   4 * MB,   44),  #         ~44  files × 3.5  MB  ≈   154 MB  } ~132 total
    ('<8MB',   8 * MB,   44),  #         ~44  files × 7    MB  ≈   308 MB /
    ('<16MB',  16 * MB,  33),  # large:  ~33  files × 14   MB  ≈   462 MB \ ~66 total
    ('<32MB',  32 * MB,  33),  #         ~33  files × 28   MB  ≈   924 MB /
    ('<64MB',  64 * MB,  22),  # xlarge: ~22  files × 56   MB  ≈  1232 MB \
    ('<128MB', 128 * MB,  5),  #         ~5   files × 112  MB  ≈   560 MB  } ~32 total
    ('<256MB', 256 * MB,  5),  #         ~5   files × 224  MB  ≈  1120 MB /
    ('<512MB', 512 * MB,  6),  # huge:   ~6   files × 448  MB  ≈  2688 MB
]                              #                              total ≈  7749 MB ≈ 7.6 GB

SIZE_CATEGORIES = [
    ('<1MB',   1 * MB,   64),   # ~64 files × 0.87 MB  ≈   56 MB
    ('<2MB',   2 * MB,   11),   # ~11 files × 1.75 MB  ≈   19 MB
    ('<4MB',   4 * MB,   11),   # ~11 files × 3.5  MB  ≈   38 MB
    ('<8MB',   8 * MB,   11),   # ~11 files × 7    MB  ≈   77 MB
    ('<16MB',  16 * MB,   6),   #  ~6 files × 14   MB  ≈   84 MB
    ('<32MB',  32 * MB,   6),   #  ~6 files × 28   MB  ≈  168 MB
    ('<64MB',  64 * MB,   6),   #  ~6 files × 56   MB  ≈  336 MB
    ('<128MB', 128 * MB,  4),   #  ~4 files × 112  MB  ≈  448 MB
    ('<256MB', 256 * MB,  2),   #  ~2 files × 224  MB  ≈  448 MB
    ('<512MB', 512 * MB,  1),   #  ~1 file  × 448  MB  ≈  448 MB
]                               #                    total ≈ 2122 MB

GLOBAL_DIR_CATEGORIES = [
    ('<1MB',  1 * MB,  3),
    ('<2MB',  2 * MB,  2),
    ('<4MB',  4 * MB,  2),
    ('<8MB',  8 * MB,  1),
    ('<16MB', 16 * MB, 1),
    ('<32MB', 32 * MB, 1),
]                               # expected ≈ 60 MB per directory

UPDATE_NEW_DIR_CATEGORIES = [
    ('<1MB',  1 * MB,  3),
    ('<2MB',  2 * MB,  2),
    ('<4MB',  4 * MB,  2),
    ('<8MB',  8 * MB,  1),
    ('<16MB', 16 * MB, 1),
]                               # expected ≈ 30 MB per new subdir

MODALITIES = ["Waveforms", "Images", "OMOP"]


def _random_file_size(target_bytes):
    """Return a byte count in [0.75 × target, 0.99 × target]."""
    return int(target_bytes * random.uniform(0.75, 0.99))


def _poisson_count(lam):
    """Draw a file count from Poisson(lam), minimum 1."""
    return max(1, int(np.random.poisson(lam)))


def _write_random_file(filepath, size_bytes, chunk_size=4 * MB):
    """Write size_bytes of random data in chunks to avoid large allocations."""
    with open(filepath, 'wb') as fout:
        remaining = size_bytes
        while remaining > 0:
            chunk = min(chunk_size, remaining)
            fout.write(os.urandom(chunk))
            remaining -= chunk


def _make_subdir(basedir, modality, pid):
    """Create and return the leaf directory for a patient + modality."""
    if modality == "Waveforms":
        subdir = str(random.randint(10000, 20000))
    else:
        subdir = os.path.join(str(random.randint(10000, 20000)),
                              str(random.randint(10000, 20000)))
    path = os.path.join(basedir, str(pid), modality, subdir)
    os.makedirs(path, exist_ok=True)
    return path


def _generate_files(destdir, categories, personids, unix0_2023, unix0_2024,
                    label_prefix="", modalities=None):
    """Generate files for a set of size categories into destdir.

    Files are randomly assigned to (patient, modality) subdirectories when
    personids is non-empty; otherwise written directly into destdir with
    sequential numeric names.  modalities restricts which modalities are used
    (defaults to all modalities in MODALITIES).
    """
    if modalities is None:
        modalities = MODALITIES
    total = 0
    filenum = 0
    for label, target_bytes, lam in categories:
        count = _poisson_count(lam)
        for _ in range(count):
            size = _random_file_size(target_bytes)
            if personids:
                pid      = random.choice(personids)
                modality = random.choice(modalities)
                leafdir  = _make_subdir(destdir, modality, pid)
                interval_st  = random.randint(unix0_2023, unix0_2024)
                interval_end = random.randint(interval_st, unix0_2024)
                interval_dur = interval_end - interval_st
                datestr  = datetime.fromtimestamp(interval_st).strftime("%Y%m%d_%H%M%S")
                ext      = ".dat" if modality == "Waveforms" else ".dcm"
                filename = f"{pid}_{datestr}_{interval_dur}{ext}"
                filepath = os.path.join(leafdir, filename)
            else:
                ext      = os.path.splitext(os.listdir(destdir)[0])[1] if os.listdir(destdir) else ".csv"
                filename = f"{filenum}{ext}"
                filepath = os.path.join(destdir, filename)

            tag = f"  {label_prefix}{label:7s}  {size / MB:7.2f} MB  {filepath}"
            print(tag)
            _write_random_file(filepath, size)
            total += size
            filenum += 1
    return total


def generateFirstFiles():
    rootfolder = "TestData"
    os.makedirs(rootfolder, exist_ok=True)

    initpushdir = os.path.join(rootfolder, "SiteFolder_FirstSnapshot")
    os.makedirs(initpushdir, exist_ok=True)

    random.seed(0)
    np.random.seed(0)

    unix0_2023 = 1672549200
    unix0_2024 = 1704085200

    numpatients = 2
    personids = [random.randint(1000000, 2000000) for _ in range(numpatients)]
    print(f"Patient IDs: {personids}")

    total_bytes = 0

    # Waveforms only, file counts = upload-group concurrency + 2, distributed
    # randomly across both patients.
    print("\n--- Waveforms per-patient files ---")
    total_bytes += _generate_files(initpushdir, SIZE_CATEGORIES_CONCURRENCY,
                                   personids, unix0_2023, unix0_2024,
                                   modalities=["Waveforms"])

    print(f"\nFirst snapshot total: {total_bytes / MB:.1f} MB")


def generateUpdate():
    random.seed(1)
    np.random.seed(1)

    rootfolder  = "TestData"
    initpushdir = os.path.join(rootfolder, "SiteFolder_FirstSnapshot")
    secpushdir  = os.path.join(rootfolder, "SiteFolder_SecondSnapshot")
    os.makedirs(secpushdir, exist_ok=True)

    unix0_2023 = 1672549200
    unix0_2024 = 1704085200

    total_bytes = 0

    personids = [int(i) for i in os.listdir(initpushdir)
                 if i not in ("OMOP", "Metadata") and
                 os.path.isdir(os.path.join(initpushdir, i))]

    # Per-patient mutations: copy / replace / modify existing files.
    # Replacement sizes are drawn from GLOBAL_DIR_CATEGORIES (small/medium)
    # to avoid re-inflating the dataset with more large files.
    for pid in personids:
        for modality in MODALITIES:
            src_modality_dir = os.path.join(initpushdir, str(pid), modality)
            if not os.path.exists(src_modality_dir):
                continue

            dst_modality_dir = os.path.join(secpushdir, str(pid), modality)
            os.makedirs(dst_modality_dir, exist_ok=True)

            files   = glob.glob(os.path.join(src_modality_dir, "**", "*"), recursive=True)
            filenum = 0
            for origpath in files:
                if os.path.isdir(origpath):
                    continue

                curfile  = os.path.relpath(origpath, src_modality_dir)
                destfile = os.path.join(dst_modality_dir, curfile)
                os.makedirs(os.path.dirname(destfile), exist_ok=True)

                _, target_bytes, _ = random.choice(GLOBAL_DIR_CATEGORIES)
                new_size = _random_file_size(target_bytes)

                option = random.randint(0, 3)
                if option == 0:
                    shutil.copy(origpath, destfile)
                    total_bytes += os.path.getsize(origpath)
                elif option == 1:
                    # Replace with a new file (different name)
                    interval_st  = random.randint(unix0_2023, unix0_2024)
                    interval_end = random.randint(interval_st, unix0_2024)
                    interval_dur = interval_end - interval_st
                    datestr  = datetime.fromtimestamp(interval_st).strftime("%Y%m%d_%H%M%S")
                    ext      = ".dat" if modality == "Waveforms" else ".dcm"
                    newname  = f"{pid}_{datestr}_{interval_dur}{ext}"
                    newpath  = os.path.join(os.path.dirname(destfile), newname)
                    _write_random_file(newpath, new_size)
                    total_bytes += new_size
                elif option == 2:
                    # Modify contents in-place (same name, new data)
                    _write_random_file(destfile, new_size)
                    total_bytes += new_size
                else:
                    shutil.copy(origpath, destfile)
                    total_bytes += os.path.getsize(origpath)

                filenum += 1

            # 1-in-4 chance: add a new subdirectory with small/medium files.
            # Capped at UPDATE_NEW_DIR_CATEGORIES (<16MB) because this branch
            # can fire multiple times; using the full SIZE_CATEGORIES here
            # would re-introduce large files for every trigger.
            if random.randint(0, 3) == 1:
                subdir = (str(random.randint(10000, 20000))
                          if modality == "Waveforms"
                          else os.path.join(str(random.randint(10000, 20000)),
                                            str(random.randint(10000, 20000))))
                newdir = os.path.join(dst_modality_dir, subdir)
                os.makedirs(newdir, exist_ok=True)

                for label, target_bytes, lam in UPDATE_NEW_DIR_CATEGORIES:
                    count = _poisson_count(lam)
                    for _ in range(count):
                        interval_st  = random.randint(unix0_2023, unix0_2024)
                        interval_end = random.randint(interval_st, unix0_2024)
                        interval_dur = interval_end - interval_st
                        datestr  = datetime.fromtimestamp(interval_st).strftime("%Y%m%d_%H%M%S")
                        size     = _random_file_size(target_bytes)
                        ext      = ".dat" if modality == "Waveforms" else ".dcm"
                        filename = f"{pid}_{datestr}_{interval_dur}{ext}"
                        filepath = os.path.join(newdir, filename)
                        print(f"  new {label:7s}  {size / MB:7.2f} MB  {filepath}")
                        _write_random_file(filepath, size)
                        total_bytes += size
                        filenum += 1

    # Global OMOP update: replace all files, small/medium only.
    omop_dir = os.path.join(secpushdir, "OMOP")
    os.makedirs(omop_dir, exist_ok=True)
    print("Updating OMOP")
    filenum = 0
    for label, target_bytes, lam in GLOBAL_DIR_CATEGORIES:
        count = _poisson_count(lam)
        for _ in range(count):
            size     = _random_file_size(target_bytes)
            filepath = os.path.join(omop_dir, f"{filenum}.csv")
            print(f"  OMOP {label:7s}  {size / MB:7.2f} MB  {filepath}")
            _write_random_file(filepath, size)
            total_bytes += size
            filenum += 1

    print(f"\nSecond snapshot total: {total_bytes / MB:.1f} MB")
