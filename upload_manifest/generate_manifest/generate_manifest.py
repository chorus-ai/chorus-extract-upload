import os
import sqlite3
import os
import glob
import hashlib
from datetime import datetime, timezone
import pandas as pd
import time

#registry will only hold target files
# upload will need to handle source and target(transformed) files.
# source to target transform may be m to n.


# TODO: DONE - Remove registry
# TODO: DONE - Change schema of files to include modtime, filesize
# TODO: DONE - Change matching to look at all three (name, time, size) 
# TODO: DONE - calculate MD5 for all non-matching files
# TODO: DONE - Set "ready-to-sync" field for all non-matches
# TODO: DONE - Update to use paths instead of filenames
# TODO: Datetime format - integer format. nanosec
# TODO: Add waveform/image option, put all tables in same db file
# TODO: OMOP option with slightly different gen/update logic
# TODO: compare first with relativepath, moddate, and filesize. 
#   mod date could be changed if there is copy.  if all same, assume same.  if any is not same, compute md5 and check.  if md5 same, assume same and update the new date.


# return file metadata from relative path and base_dir
def get_file_metadata(base_dir: str | os.PathLike, rel_path: str | os.PathLike, ) -> dict:
    """
    Get the metadata of a file.

    :param file_path: The path to the file.
    :type file_path: str | os.PathLike
    :return: A dictionary containing the metadata of the file.
    :rtype: dict
    """
    # Initialize the dictionary to store the metadata
    metadata = {}

    fn = os.path.join(base_dir, rel_path)
    
    # get the relative path
    metadata["path"] = os.path.relpath(rel_path)

    statinfo = os.stat(fn)
    metadata["size"] = statinfo.st_size
    metadata["create_time"] = pd.to_datetime(statinfo.st_ctime, unit='s')
    metadata["modify_time"] = pd.to_datetime(statinfo.st_mtime, unit='s')

    return metadata

def get_file_md5(base_dir: str | os.PahtLike, file_path: str | os.PathLike) -> str:
    """
    Get the MD5 hash of a file.

    :param file_path: The path to the file.
    :type file_path: str | os.PathLike
    :return: The MD5 hash of the file.
    :rtype: str
    """
    fn = os.path.join(base_dir, rel_path)
    
    # Initialize the MD5 hash object
    md5 = hashlib.md5()

    # Open the file in binary mode
    with open(fn, "rb") as f:
        # Read the file in chunks
        for chunk in iter(lambda: f.read(4096), b""):
            # Update the MD5 hash with the chunk
            md5.update(chunk)

    # Get the hexadecimal representation of the MD5 hash
    md5_hash = md5.hexdigest()

    return md5_hash


# get the list of files in relative path, one data type at a time.  return relative file paths
def get_files_rel(root_path: str | os.PathLike, datatype : str = None) -> list[str | os.PathLike]:
    
    if (datatype == "Waveforms") or (datatype == "Images"):
        # first get the data type subdirs.
        path_wildcard = os.path.join(root_path, "*", datatype)
        data_paths = glob.glob(path_wildcard)
    elif datatype == "OMOP":
        # get OMOP files.
        data_paths = [os.path.join(root_path, datatype)]
    else: 
        print("ERROR: unkown data type requested.", datatype)
        return None

    files = []
    # now get via os.walk - this will walk through entire subdirectory.
    for dp in data_paths:
        for root, subdirs, files in os.walk(dp):
            for f in files:
                fp = os.path.abspath(os.path.join(root, f))
                files.append(fp)
                
    root = os.path.abspath(root_path)
                
    return [os.path.relpath(f, root) for f in files]
    

# current assumptions/working decisions for initial run:
# TONY:  working with subdirectories of form:  pid/type/files
# 1 file per record - can be extended to 1 dir per record or using a file-record mapping
# no upload dir specified on local site's side
# entity relationships: many sessions per person, many records per session, many files per record
# TONY removed this assumption: files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension

# [ ]: need support for cloud files.


# def files_are_same(file1: str | os.PathLike, file2: str | os.PathLike) -> bool:
#     meta1 = get_file_metadata(file1)
#     meta2 = get_file_metadata(file2)

#     if (meta1["path"] == meta2["path"]) and (meta1["name"] == meta2["name"]) and (meta1["size"] == meta2["size"]) and (meta1["last_modified"] == meta2["last_modified"]):
#         return True
        
#     md5_1 = get_file_md5(file1)
#     md5_2 = get_file_md5(file2)
    
#     if (md5_1 == md5_2):
#         return True
    
#     return False


# def file_unchanged(old_meta: dict, old_md5: str, file: str | os.PathLike) -> bool:
#     new_meta = get_file_metadata(file)
    
#     if (old_meta["path"] == new_meta["path"]) and (old_meta["name"] == new_meta["name"]) and (old_meta["size"] == new_meta["size"]) and (old_meta["last_modified"] == new_meta["last_modified"]):
#         return True
    
#     new_md5 = get_file_md5(file)
    
#     if (old_md5 == new_md5):
#         return True
    
#     return False


# current assumptions/working decisions for initial run:
# 1 file per record - can be extended to 1 dir per record or using a file-record mapping
# no upload dir specified on local site's side
# entity relationships: many sessions per person, many records per session, many files per record
# files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension
def gen_manifest(folderpath, databasename="journal.db"):
    # print("Starting Manifest")
    if os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print("Initial manifest, but a journal with that name already exists. Please delete and re-run.")
        exit()
    con = sqlite3.connect(databasename)

    cur = con.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS manifest(\
                FILE_ID INTEGER NOT NULL PRIMARY KEY, \
                PERSON_ID INTEGER, \
                FILEPATH TEXT, \
                MODTIME TEXT, \
                SIZE INTEGER, \
                MD5 TEXT, \
                TIME_VALID TEXT, \
                TIME_INVALID TEXT, \
                SYNC_READY INTEGER)")
    
    
    # curfolder = os.getcwd()
    # os.chdir(folderpath)
    # globpath = os.path.join("*", "Waveforms","*")
    # # print(globpath)
    # paths = glob.glob(globpath)
    # print(paths[0])

    paths1 = get_files_rel(folderpath, "Waveforms")
    paths2 = get_files_rel(folderpath, "Images")
    paths3 = get_files_rel(folderpath, "OMOP")
    paths = [*paths1, *paths2, *paths3]

    epoch_now = time.time()

    for i_path, relpath in enumerate(paths):
        # print(path)
        meta = get_file_metadata(folderpath, relpath)
        # first item is personid.
        personid = relpath.split(os.path.sep)[0] if ("Waveforms" in relpath) or ("Images" in relpath) else None
        filesize = meta['size']
        moddatetime = meta['modify_time']
        mymd5 = get_file_md5(folderpath, relpath)
        
        myargs = (i_path, personid, relpath, moddatetime, filesize, mymd5, epoch_now, None, 1)
        cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])

    con.commit() 
    con.close()


# record is a mess with a file-wise traversal. Files need to be grouped by unique record ID (visit_occurence?)
def update_manifest(folderpath, databasename="journal.db"):
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"No manifest exists for filename {databasename}")
        return
    con = sqlite3.connect(databasename)

    cur = con.cursor()

    paths1 = get_files_rel(folderpath, "Waveforms")
    paths2 = get_files_rel(folderpath, "Images")
    paths3 = get_files_rel(folderpath, "OMOP")
    paths = [*paths1, *paths2, *paths3]

    # print(paths[0])

    dt_nosymbols = datetime.now().strftime("%Y%m%d_%H%M%S")
    epoch_now = time.time()

    filesbackup = "manifest_" + dt_nosymbols
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM manifest")

    i_file = cur.execute("SELECT MAX(file_id) from manifest").fetchone()[0] + 1
    # print(i_file)

    # get all active files, these are active-pending-delete until the file is found in filesystem
    activefiletuple = cur.execute("Select file_id from manifest where TIME_INVALID IS NULL").fetchall()
    
    activefileset = {i[0] for i in activefiletuple}

    cur.execute("UPDATE manifest SET SYNC_READY=?", (0,))

    for i_path, path in enumerate(paths):
        # print(path)
        meta = get_file_metadata(folderpath, relpath)
        # first item is personid.
        personid = relpath.split(os.path.sep)[0] if ("Waveforms" in relpath) or ("Images" in relpath) else None
        filesize = meta['size']
        moddatetime = meta['modify_time']
        # mymd5 = get_file_md5(folderpath, relpath)

        # query files by filename, modtime, and size
        filecheckrow = cur.execute("Select file_id, md5 from manifest where filepath=? and modtime=? and size=? and TIME_INVALID IS NULL",(path, moddatetime, filesize))
        results = filecheckrow.fetchall()
        # There should only be 1 active file according to the path in a well-formed 
        if len(results) > 1:
            print("Multiple active files with that path - can't compare to old files")
            print("Exiting update...")
            return
        elif len(results) == 1:
            results = results[0]
            oldfileid = results[0]
            # oldmd5 = results[1]
            
            # check if old and new are the same
            if 
            
            
            # if exists and md5 is different, invalidate existing file_id
            # then add new file like above, using new id, increment i_file
            # keep record the same
            # if oldmd5 != mymd5:
            #     cur.execute("UPDATE manifest SET TIME_INVALID=? WHERE file_id=?", (dt_string, oldfileid,))
            #     myargs = (i_file, personid, filename, moddt_string, filesize, mymd5, dt_string, None, 1)
            #     cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            #     i_file += 1
            #     activefileset.remove(oldfileid)
            # # if exists and md5 is same, remove from active-pending-delete list
            # else:
            #     activefileset.remove(oldfileid)
            activefileset.remove(oldfileid)
            
        else:
            # if DNE, add new file
            mymd5 = hashlib.md5(open(path, 'rb').read()).hexdigest()
            myargs = (i_file, personid, path, moddatetime, filesize, mymd5, epoch_now, None, 1)
            cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            i_file += 1

    # for all files that were active but aren't there anymore
    # invalidate in manifest
    # for file_id,record_id in zip(activefilemap.keys(),activefilemap.values()):
    myargs = map(lambda v: (epoch_now, v), activefileset)
    cur.executemany("UPDATE manifest SET TIME_INVALID=? WHERE file_id=?", myargs)


    con.commit() 
    con.close()

