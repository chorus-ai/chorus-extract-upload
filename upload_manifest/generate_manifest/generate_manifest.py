import os
import sqlite3
import os
import glob
import hashlib
import pandas as pd
import time
import math


# TODO: DONE - Remove registry
# TODO: DONE - Change schema of files to include modtime, filesize
# TODO: DONE - Change matching to look at all three (name, time, size) 
# TODO: DONE - calculate MD5 for all non-matching files
# TODO: DONE - Set "ready-to-sync" field for all non-matches
# TODO: DONE - Update to use paths instead of filenames
# TODO: DONE - Datetime format - integer format. nanosec
# TODO: DONE - Add waveform/image option, put all tables in same db file
# TODO: DONE - OMOP option with slightly different gen/update logic
# TODO: DONE - compare first with relativepath, moddate, and filesize. 
#   mod date could be changed if there is copy.  if all same, assume same.  if any is not same, compute md5 and check.  if md5 same, assume same and update the new date.


# current assumptions/working decisions for initial run:
# TONY:  working with subdirectories of form:  pid/type/files
# 1 file per record - can be extended to 1 dir per record or using a file-record mapping
# no upload dir specified on local site's side
# entity relationships: many sessions per person, many records per session, many files per record
# TONY removed this assumption: files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension

# [ ]: need support for cloud files.
# [ ]: commandline argument parsing.

# TODO: DONE - Datetime format
# TODO: Add waveform/image option, put all tables in same db file
# TODO: OMOP option with slightly different gen/update logic
# TODO: Remove sync flag setting to 0 - to be done by uploading
# TODO: Change comparison for mtimes that are different, but filesize, path, hash are the same
# --------- Copy ops can change mtimes without changing contents or relative name
# --------- If other 3 match, invalidate old row, add new row with new mtime and syncflag equal to old syncflag

# TODO: cloud files
# TODO: integrate with AZCLI
# TODO: verification of manifest
# TODO: command processor
# TODO: update manifest with cloud files.

# process:
#   1. local manifest is created
#   2. local manifest is used to generate update list
#   3. local manifest is used to send data to cloud
#   4. local manifest is verified with cloud files



# simple assumption for upload between updates
# we always compare to the last manifest.
# all remote stores are dated, so override with partial uploads.



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
    metadata["create_time"] = statinfo.st_ctime_ns
    metadata["modify_time"] = statinfo.st_mtime_ns

    return metadata

def get_file_md5(base_dir: str | os.PathLike, file_path: str | os.PathLike) -> str:
    """
    Get the MD5 hash of a file.

    :param file_path: The path to the file.
    :type file_path: str | os.PathLike
    :return: The MD5 hash of the file.
    :rtype: str
    """
    fn = os.path.join(base_dir, file_path)
    
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
def get_files_relpath(root_path: str | os.PathLike, datatype : str = None) -> list[str | os.PathLike]:
    
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


    all_files = []
    # now get via os.walk - this will walk through entire subdirectory.
    for dp in data_paths:
        for root, subdirs, files in os.walk(dp, topdown=True, followlinks=True):
            for f in files:
                fp = os.path.abspath(os.path.join(root, f))
                all_files.append(fp)
    # print("all files ", all_files)
                
    root = os.path.abspath(root_path)
                
    return [os.path.relpath(f, root) for f in all_files]
    


# current assumptions/working decisions for initial run:
# 1 file per record - can be extended to 1 dir per record or using a file-record mapping
# no upload dir specified on local site's side
# entity relationships: many sessions per person, many records per session, many files per record
# files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension
def gen_manifest(folderpath, databasename="journal.db"):
    
    
    print("Creating Manifest...", end='', flush=True)
    
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
                MODTIME_ns INTEGER, \
                SIZE INTEGER, \
                MD5 TEXT, \
                TIME_VALID_ns INTEGER, \
                TIME_INVALID_ns INTEGER, \
                SYNC_READY INTEGER)")
    
    # curfolder = os.getcwd()
    # os.chdir(folderpath)
    # globpath = os.path.join("*", "Waveforms","*")
    # # print(globpath)
    # paths = glob.glob(globpath)
    # print(paths[0])
    paths1 = get_files_relpath(folderpath, "Waveforms")
    paths2 = get_files_relpath(folderpath, "Images")
    paths3 = get_files_relpath(folderpath, "OMOP")
    paths = [*paths1, *paths2, *paths3]

    curtimestamp = time.time_ns()

    all_args = []
    for i_path, relpath in enumerate(paths):
        # print(i_path, relpath)
        meta = get_file_metadata(folderpath, relpath)
        # first item is personid.
        personid = relpath.split(os.path.sep)[0] if ("Waveforms" in relpath) or ("Images" in relpath) else None
        filesize = meta['size']
        modtimestamp = meta['modify_time']
        mymd5 = get_file_md5(folderpath, relpath)
        
        myargs = (i_path, personid, relpath, modtimestamp, filesize, mymd5, curtimestamp, None, 1)

        all_args.append(myargs)

    if len(all_args) > 0:
        cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)
        
    con.commit() 
    con.close()
    print("Done", flush = True)


# record is a mess with a file-wise traversal. Files need to be grouped by unique record ID (visit_occurence?)
def update_manifest(folderpath, databasename="journal.db"):

    print("Updating Manifest...", databasename, flush=True)

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"No manifest exists for filename {databasename}")
        return
    con = sqlite3.connect(databasename)

    cur = con.cursor()

    paths1 = get_files_relpath(folderpath, "Waveforms")
    paths2 = get_files_relpath(folderpath, "Images")
    paths3 = get_files_relpath(folderpath, "OMOP")
    paths = [*paths1, *paths2, *paths3]

    # print(paths[0])

    # do not back up - this allows changes to accumulate.
    # backup_timestamp = math.floor(time.time())
    # filesbackup = "manifest_" + str(backup_timestamp)
    # cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    # cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM manifest")


    curtimestamp = time.time_ns()

    i_file = cur.execute("SELECT MAX(file_id) from manifest").fetchone()[0]
    if i_file is None:
        print("WARNING:  updating an empty database")
        i_file = 1
    else: 
        i_file += 1
    # print(i_file)

    # get all active files, these are active-pending-delete until the file is found in filesystem
    activefiletuples = cur.execute("Select filepath, file_id, size, modtime_ns, md5, sync_ready from manifest where TIME_INVALID_ns IS NULL").fetchall()
    
    # initialize by putting all files in files_to_inactivate.  remove from this list if we see the files on disk
    files_to_inactivate = {}
    for i in activefiletuples:
        fpath = i[0]
        if fpath not in files_to_inactivate.keys():
            files_to_inactivate[fpath] = [ (i[1], i[2], i[3], i[4], i[5]) ]
        else:
            files_to_inactivate[fpath].append((i[1], i[2], i[3], i[4], i[5]))

    # cur.execute("UPDATE manifest SET SYNC_READY=0")

    all_insert_args = []
    for i_path, relpath in enumerate(paths):
        # print(path)
        
        # get information about the current file
        meta = get_file_metadata(folderpath, relpath)
        # first item is personid.
        personid = relpath.split(os.path.sep)[0] if ("Waveforms" in relpath) or ("Images" in relpath) else None
        filesize = meta['size']
        modtimestamp = meta['modify_time']
        # 

        # get information about the old file
        # query files by filename, modtime, and size
        # this should be merged with the initial call to get active fileset since we in theory will be checking most files.
        # filecheckrow = cur.execute("Select file_id, size, modtime_ns, md5, sync_ready from manifest where filepath=? and TIME_INVALID_ns IS NULL", [(relpath)])

        # results = filecheckrow.fetchall()
        if relpath in files_to_inactivate.keys():
            results = files_to_inactivate[relpath]
            # There should only be 1 active file according to the path in a well-formed 
            if len(results) > 1:
                print("Multiple active files with that path - can't compare to old files")
                print("Exiting update...")
                return
            elif len(results) == 1:
                (oldfileid, oldsize, oldmtime, oldmd5, oldsync) = results[0]
                
                # if old size and new size, old mtime and new mtime are same (name is already matched)
                # then same file, skip rest.
                if (oldmtime == modtimestamp):
                    if (oldsize == filesize):
                        # files are very likely the same because of size and modtime match
                        del files_to_inactivate[relpath]  # do not mark file as inactive.
                        print("SAME ", relpath )
                        continue
                    else:
                        #time stamp same but file size is different?
                        print("ERROR: File size is different but modtime is the same.", relpath)
                        continue
                    
                else:
                    # timestamp changed. we need to compute md5 to check, or to update.
                    mymd5 = get_file_md5(folderpath, relpath)

                    # files are extremely likely the same just moved.
                    # set the old entry as invalid and add a new one that's essentially a copy except for modtime..
                    # choosing not to change SYNC_READY
                    # cur.execute("UPDATE manifest SET TIME_INVALID_ns=? WHERE file_id=?", (curtimestamp, oldfileid))
                    if (oldsize == filesize) and (mymd5 == oldmd5):
                        # essentially copy the old and update the modtime.
                        myargs = (i_file, personid, relpath, modtimestamp, oldsize, oldmd5, curtimestamp, None, oldsync)
                        print("MOVED ", relpath)
                        # files_to_inactivate.remove(oldfileid)  # we should mark old as inactive 
                    else:
                        # files are different.  set the old file as invalid and add a new file.
                        myargs = (i_file, personid, relpath, modtimestamp, filesize, mymd5, curtimestamp, None, 1)
                        print("UPDATE ", relpath)
                        # modified file. old one should be removed.
                    
                    i_file += 1 # this is a uid.  so has to increment.
                        
                    all_insert_args.append(myargs)
                    # cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)            
            else:
                print("ERROR: File found but no metadata.", relpath)
                continue
        else:
            # if DNE, add new file
            mymd5 = get_file_md5(folderpath, relpath)

            myargs = (i_file, personid, relpath, modtimestamp, filesize, mymd5, curtimestamp, None, 1)
            all_insert_args.append(myargs)
            # cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            print("ADD  ", relpath)
            i_file += 1

    if len(all_insert_args) > 0:
        cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", all_insert_args)

    # for all files that were active but aren't there anymore
    # invalidate in manifest
    # for file_id,record_id in zip(activefilemap.keys(),activefilemap.values()):
    # myargs = map(lambda v: (curtimestamp, v), files_to_inactivate)
    
    myargs = []
    for relpath, vals in files_to_inactivate.items():
        if (relpath in paths):
            print("OLD  ", relpath)
        else:
            print("DELETED  ", relpath)
        
        for v in vals:
            myargs.append((curtimestamp, v[0]))
    print("SQLITE update arguments ", myargs)
    if (len(myargs) > 0):
        cur.executemany("UPDATE manifest SET TIME_INVALID_ns=? WHERE file_id=?",myargs)

    con.commit() 
    con.close()
    print("Done", flush = True)



# all files to upload are marked as sync_ready. 
def select_files_to_upload(databasename="journal.db", outfilename = "files.txt"):

    print("Extracting Active Files...", end='', flush=True)

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"No manifest exists for filename {databasename}")
        return
    con = sqlite3.connect(databasename)

    cur = con.cursor()

    # identify 3 subsets:   deleted, updated, and added.
    # deleted:  sync_ready file with invalid time stamp.  No additional valid time stamp
    # added:  sync_ready file with null invalid time stemp and no prior entry with non-null invalid time stamp
    # modified:  sync_ready file with null invalid time, and a prior entry with non-null invalid time stamp
    # files_to_remove = cur.execute("Select filepath, size, md5, time_invalid_ns from manifest where sync_ready == 1").fetchall()
    # to_remove = [ ]
    
    files_to_update = cur.execute("Select file_id, filepath, size, md5, time_invalid_ns from manifest where sync_ready == 1").fetchall()
    
    # print(files_to_update)
    # only need to add, not delete.  rely on manifest to mark deletion.
    
    
    # print("to inactivate ", files_to_inactivate)
    # myargs = set()
    active_files = set()
    inactive_files = set()
    for (file_id, fn, size, md5, invalidtime) in files_to_update:
        if invalidtime is not None:
            inactive_files.add(fn)
        else:
            active_files.add(fn)

    files = set()
    for (file_id, fn, size, md5, invalidtime) in files_to_update:
        if fn in active_files:
            if fn in inactive_files:
                print("UPDATE ", fn)
            else:
                print("ADD ", fn)
            files.add(fn)
        else:
            if fn in inactive_files:
                print("DEL ", fn)
            else:
                print("IMPOSSIBLE: file not in active or inactive list ", fn)
        
        # myargs.add((0, file_id))
    # convert to list then sort
    file_list = list(files)
    # sort the file-list
    file_list.sort()
    
    # now update the manifest's sync flag.
    # if len(myargs) > 0:
    #     print("updating sync flag")
    #     cur.executemany("UPDATE manifest SET SYNC_READY=? WHERE FILE_ID=?",myargs)
    
    # do we need to delete files in central at all?  manifest will be updated.
    # we only need to add new files
    
    con.close()

    with open(outfilename, 'w') as f:
        f.write("\n".join(file_list))
        
    print("Done", flush = True)
    

def list_manifests(databasename="journal.db"):
    if not os.path.exists(databasename):
        print(f"No manifest exists for filename {databasename}")
        return
    
    con = sqlite3.connect(databasename)
    cur = con.cursor()
    
    # List tables in the SQLite database
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    
    if tables:
        print("Tables in the database:")
        for table in tables:
            print(table[0])
    else:
        print("No tables found in the database.")
    
    con.close()



def delete_manifest(manifest_name, databasename="journal.db"):
    if manifest_name == "manifest":
        print("ERROR: you are trying to delete 'manifest'.  this is not allowed")
        return
    
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"No manifest exists for filename {databasename}")
        return

    con = sqlite3.connect(databasename)
    cur = con.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {manifest_name}")

    print("Dropped database ", manifest_name)

    con.close()
    
    
# this should only be called when an upload is initiated.
def backup_manifest(databasename="journal.db"):
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"No manifest exists for filename {databasename}")
        return

    con = sqlite3.connect(databasename)
    cur = con.cursor()

    backup_timestamp = round(time.time())

    filesbackup = "manifest_" + str(backup_timestamp)
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM manifest")

    print("backed up database manifest to ", filesbackup)

    con.close()


    
# this should only be called when an upload is initiated.
def restore_manifest(manifestname, databasename="journal.db"):
    if manifestname == "manifest":
        print("ERROR: trying to restrore 'manifest' to 'manifest'")
        return
    
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"No manifest exists for filename {databasename}")
        return

    con = sqlite3.connect(databasename)
    cur = con.cursor()

    backup_timestamp = round(time.time())

    # first do a backup
    filesbackup = "manifest_" + str(backup_timestamp)
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM manifest")

    # then restore
    cur.execute(f"DROP TABLE IF EXISTS manifest")
    cur.execute(f"CREATE TABLE manifest AS SELECT * FROM {manifestname}")

    print("restored database manifest from ", manifestname, ".  original manifest table backed up to ", filesbackup)

    con.close()
