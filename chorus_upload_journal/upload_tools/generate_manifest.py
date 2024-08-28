import os
import sqlite3
import pandas as pd
import time
import math
from storage_pathlib import FileSystemHelper

from typing import Optional, Union
from time import strftime, gmtime

from cloudpathlib import AnyPath
from pathlib import Path

import re
from enum import Enum
import concurrent.futures



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
# TONY:  working with subdirectories of form:  pid/type/dir.../files
# 1 file per record - can be extended to 1 dir per record or using a file-record mapping
# no upload dir specified on local site's side
# entity relationships: many sessions per person, many records per session, many files per record
# TONY removed this assumption: files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension


# TODO: DONE - Datetime format
# TODO: DONE - Add waveform/image option, put all tables in same db file
# TODO: DONE - OMOP option with slightly different gen/update logic
# TODO: DONE - Remove sync flag setting to 0 - to be done during uploading
# TODO: DONE - Change comparison for mtimes that are different, but filesize, path, hash are the same
# --------- Copy ops can change mtimes without changing contents or relative name
# --------- If other 3 match, invalidate old row, add new row with new mtime and syncflag equal to old syncflag

# TODO: DONE - generate manifest from cloud storage
# TODO: DONE - update logic on how modify time is used?  not compare source to destination by this, but compare source to source
# TODO: DONE - changed sync_ready flag to upload_dtstr to track upload time.. 
# TODO: NO - integrate with AZCLI.  Using cloudpathlib instead
# TODO: DONE - verification of manifest
# TODO: DONE - command processor
# TODO: DONE - update manifest with cloud files.

# TODO: DONE save command history
# TODO: DONE upload only allow unuploaded files.
# TODO: DONE upload allows amending the LAST upload.
# TODO: DONE upload and verify each file at a time
# TODO: DONE support windows paths


# process:
#   1. local manifest is created
#   2. local manifest is used to generate update list
#   3. local manifest is used to send data to cloud, local manifest is sent to cloud as well
#   4. local manifest is verified with cloud files


# okay to have multiple updates between uploads
# we always compare to the last manifest.
# all remote stores are dated, so override with partial uploads.

DEFAULT_MODALITIES = ['Waveforms', 'Images', 'OMOP']

def save_command_history(common_params:str, cmd:str, parameters:str, 
                         src_paths:str, dest_path:str,
                         databasename="journal.db"):
    """
    Saves the command history to a SQLite database.

    Parameters:
    - cmd (str): The command that was executed.
    - parameters (str): The parameters passed to the command.
    - databasename (str): The name of the SQLite database file. Default is "journal.db".
    """
    
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    curtimestamp = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
    # curtimestamp = int(math.floor(time.time() * 1e6))

    cur.execute("CREATE TABLE IF NOT EXISTS command_history (\
                        COMMAND_ID INTEGER PRIMARY KEY AUTOINCREMENT, \
                        DATETIME TEXT, \
                        COMMON_PARAMS TEXT, \
                        COMMAND TEXT, \
                        PARAMS TEXT, \
                        SRC_PATHS TEXT, \
                        DEST_PATH TEXT, \
                        Duration TEXT)")  # datetime string of the upload.
 
    cur.execute("INSERT INTO command_history ( \
        DATETIME, COMMON_PARAMS, COMMAND, PARAMS, SRC_PATHS, DEST_PATH \
        ) VALUES(?, ?, ?, ?, ?, ?)",
        (curtimestamp, common_params, cmd, parameters, src_paths, dest_path))
    con.commit()
    
    # get the inserted record id
    command_id = cur.lastrowid
    
    con.close()
    
    return command_id


def update_command_completion(command_id:int, duration:float, databasename="journal.db"):
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    cur.execute("UPDATE command_history SET DURATION=? WHERE COMMAND_ID=?", 
                (duration, command_id))
    con.commit()
    
    con.close()    
    
def show_command_history(databasename="journal.db"):
    """
    Retrieve and display the command history from the specified SQLite database.

    Parameters:
    - databasename (str): The name of the SQLite database file. Default is "journal.db".

    Returns:
    None
    """
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    # get all the commands
    commands = cur.execute("SELECT * FROM command_history").fetchall()
    con.close()
    
    for (id, timestamp, common_params, cmd, params, src_paths, dest_path, duration) in commands:
        # convert microsecond timestamp to datetime
        # dt = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(int(timestamp/1e6)))
        print(f"  {id}\t{timestamp}:\t{common_params} {cmd} {params}.  {src_paths}->{dest_path}.  elapse {duration}s")



def update_manifest(root : FileSystemHelper, modalities: list[str] = DEFAULT_MODALITIES, databasename="journal.db", verbose: bool = False):
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    # check if manifest table exists in sqlite
    table_exists = cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='manifest'").fetchone()
    con.close()

    # check if manifest table exists
    if (table_exists is not None) and (table_exists[0] == "manifest"):
        return _update_manifest(root, modalities, databasename, verbose= verbose)
    else:
        return _gen_manifest(root, modalities, databasename, verbose = verbose)
        
# compile a regex for extracting person id from waveform and iamge paths
# the personid is the first part of the path, followed by the modality, then the rest of the path
PERSONID_REGEX = re.compile(r"([^/:]+)/(Waveforms|Images)/.*")

def _gen_manifest_one_file(root : FileSystemHelper, relpath:str, modality:str, curtimestamp:int):
    # print(i_path, relpath)
    start = time.time()
    meta = FileSystemHelper.get_metadata(root = root.root, path = relpath, with_metadata=True, with_md5=True)
    # first item is personid.
    md5_time = time.time() - start
    
    # extract personid using regex
    matched = PERSONID_REGEX.match(relpath)
    personid = matched.group(1) if matched else None
    # p = AnyPath(relpath)
    # personid = p.parts[0] if ("Waveforms" in p.parts) or ("Images" in p.parts) else None
    
    filesize = meta['size']
    modtimestamp = meta['modify_time_us']
    
    mymd5 = meta['md5']
                
    myargs = (personid, relpath, modality, modtimestamp, filesize, mymd5, curtimestamp, "ADDED", md5_time)

    return myargs



def _gen_manifest(root : FileSystemHelper, modalities: list[str] = DEFAULT_MODALITIES, 
                  databasename="journal.db", verbose: bool = False):
    """
    Generate a manifest for the given root directory and subdirectories.

    Args:
        root (FileSystemHelper): The root directory to generate the manifest for.
        modalities (list[str], optional): The subdirectories to include in the manifest. Defaults to ['Waveforms', 'Images', 'OMOP'].
        databasename (str, optional): The name of the database to store the manifest. Defaults to "journal.db".
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """
    print("INFO: Creating Manifest...", flush=True)
    
        
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS manifest(\
                FILE_ID INTEGER PRIMARY KEY AUTOINCREMENT, \
                PERSON_ID INTEGER, \
                FILEPATH TEXT, \
                MODALITY TEXT, \
                SRC_MODTIME_us INTEGER, \
                SIZE INTEGER, \
                MD5 TEXT, \
                TIME_VALID_us INTEGER, \
                TIME_INVALID_us INTEGER, \
                UPLOAD_DTSTR TEXT, \
                STATE TEXT, \
                MD5_DURATION REAL, \
                UPLOAD_DURATION REAL, \
                VERIFY_DURATION REAL)")  # datetime string of the upload.
    con.commit()
    con.close()
    
    paths = []
    curtimestamp = int(math.floor(time.time() * 1e6))
    all_args = []
    
    modalities = modalities if modalities else DEFAULT_MODALITIES
    
    for modality in modalities:
        paths = root.get_files(modality)  # list of relative paths in posix format and in string form.
        # paths += paths1
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for relpath in paths:
                future = executor.submit(_gen_manifest_one_file, root, relpath, modality, curtimestamp)
                futures.append(future)
            
            for future in concurrent.futures.as_completed(futures):
                myargs = future.result()
                rlpath = myargs[1]
                status = myargs[7]
                if verbose and status == "ADDED":
                    print("INFO: ADDED ", rlpath)
                all_args.append(myargs)


            if len(all_args) > 0:
                con = sqlite3.connect(databasename, check_same_thread=False)
                cur = con.cursor()

                try:
                    cur.executemany("INSERT INTO manifest ( \
                        PERSON_ID, \
                        FILEPATH, \
                        MODALITY, \
                        SRC_MODTIME_us, \
                        SIZE, \
                        MD5, \
                        TIME_VALID_us, \
                        STATE, \
                        MD5_DURATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)
                except sqlite3.IntegrityError as e:
                    print("ERROR: create ", e)
                    print(all_args)
                        
                con.commit() 
                con.close()

def _update_manifest_one_file(root: FileSystemHelper, relpath:str, modality:str, curtimestamp:int, modality_files_to_inactivate:dict):
    # get information about the current file
    meta = FileSystemHelper.get_metadata(root = root.root, path = relpath, with_metadata = True, with_md5 = False)
    # root.get_file_metadata(relpath)
    
    # first item is personid.
    matched = PERSONID_REGEX.match(relpath)
    personid = matched.group(1) if matched else None
    # p = AnyPath(relpath)
    # personid = p.parts[0] if ("Waveforms" in p.parts) or ("Images" in p.parts) else None
    
    filesize = meta['size']
    modtimestamp = meta['modify_time_us']
    # 

    # get information about the old file
    # query files by filename, modtime, and size
    # this should be merged with the initial call to get active fileset since we in theory will be checking most files.
    # filecheckrow = cur.execute("Select file_id, size, src_modtime_us, md5, upload_dtstr from manifest where filepath=? and TIME_INVALID_us IS NULL", [(relpath)])

    # results = filecheckrow.fetchall()
    if relpath in modality_files_to_inactivate.keys():
        results = modality_files_to_inactivate[relpath]
        # There should only be 1 active file according to the path in a well-formed 
        if (len(results) > 1):
            # print("ERROR: Multiple active files with that path - manifest is not consistent")
            return (personid, relpath, modality, modtimestamp, None, None, curtimestamp, None, "ERROR1", None)
        if (len(results) == 0):
            # print("ERROR: File found but no metadata.", relpath)
            return (personid, relpath, modality, modtimestamp, None, None, curtimestamp, None, "ERROR2", None)
        
        if len(results) == 1:
            (oldfileid, oldsize, oldmtime, oldmd5, oldsync) = results[0]
            
            # if old size and new size, old mtime and new mtime are same (name is already matched)
            # then same file, skip rest.
            if (oldmtime == modtimestamp):
                if (oldsize == filesize):
                    # files are very likely the same because of size and modtime match
                    keep_file = relpath
                    # del modality_files_to_inactivate[relpath]  # do not mark file as inactive.
                    # print("SAME ", relpath )
                    return (personid, relpath, modality, modtimestamp, oldsize, oldmd5, curtimestamp, oldsync, "KEEP", None)
                else:
                    #time stamp same but file size is different?
                    # print("ERROR: File size is different but modtime is the same.", relpath)
                    return (personid, relpath, modality, modtimestamp, oldsize, oldmd5, curtimestamp, oldsync, "ERROR3", None)
                
            else:
                # timestamp changed. we need to compute md5 to check, or to update.
                start = time.time()
                mymd5 = FileSystemHelper.get_metadata(root = root.root, path = relpath, with_metadata = False, with_md5 = True)['md5']
                md5_time = time.time() - start

                keep_file = None
                # files are extremely likely the same just moved.
                # set the old entry as invalid and add a new one that's essentially a copy except for modtime..
                # choosing not to change SYNC_TIME
                # cur.execute("UPDATE manifest SET TIME_INVALID_us=? WHERE file_id=?", (curtimestamp, oldfileid))
                if (oldsize == filesize) and (mymd5 == oldmd5):
                    # essentially copy the old and update the modtime.
                    myargs = (personid, relpath, modality, modtimestamp, oldsize, oldmd5, curtimestamp, oldsync, "MOVED", md5_time)

                    # files_to_inactivate.remove(oldfileid)  # we should mark old as inactive 
                else:
                    # files are different.  set the old file as invalid and add a new file.
                    myargs = (personid, relpath, modality, modtimestamp, filesize, mymd5, curtimestamp, None, "UPDATED", md5_time)
                    # if verbose:
                    #     print("INFO: UPDATED ", relpath)
                    # modified file. old one should be removed.
    else:
        # if DNE, add new file
        start = time.time()
        mymd5 = FileSystemHelper.get_metadata(root = root.root, path = relpath, with_metadata = False, with_md5 = True)['md5']
        md5_time = time.time() - start

        myargs = (personid, relpath, modality, modtimestamp, filesize, mymd5, curtimestamp, None, "ADDED", md5_time)

            
    return myargs


# record is a mess with a file-wise traversal. Files need to be grouped by unique record ID (visit_occurence?)
def _update_manifest(root: FileSystemHelper, modalities: list[str] = DEFAULT_MODALITIES,
                     databasename="journal.db", verbose: bool = False):
    """
    Update the manifest database with the files found in the specified root directory.

    Args:
        root (FileSystemHelper): The root directory to search for files.
        modalities (list[str], optional): The subdirectories to search for files. Defaults to ['Waveforms', 'Images', 'OMOP'].
        databasename (str, optional): The name of the manifest database. Defaults to "journal.db".
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """
    print("INFO: Updating Manifest...", databasename, flush=True)

    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    # check if manifest table exists.
    curtimestamp = int(math.floor(time.time() * 1e6))

    i_file = cur.execute("SELECT MAX(file_id) from manifest").fetchone()[0]
    if i_file is None:
        print("WARNING:  updating an empty database")
        i_file = 1
    else: 
        i_file += 1
    con.close()    

    modalities = modalities if modalities else DEFAULT_MODALITIES
    

    paths = []
    all_insert_args = []
    all_del_args = []
    files_to_inactivate = {}
    for modality in modalities:
        paths = root.get_files(modality)  # relative paths, posix format, string form.
        
        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()
        # get all active files, these are active-pending-delete until the file is found in filesystem
        activefiletuples = cur.execute(f"Select filepath, file_id, size, src_modtime_us, md5, upload_dtstr from manifest where TIME_INVALID_us IS NULL and MODALITY = '{modality}'").fetchall()
        con.close()
        
        # initialize by putting all files in files_to_inactivate.  remove from this list if we see the files on disk
        modality_files_to_inactivate = {}
        for i in activefiletuples:
            fpath = i[0]
            if fpath not in modality_files_to_inactivate.keys():
                modality_files_to_inactivate[fpath] = [ (i[1], i[2], i[3], i[4], i[5]) ]
            else:
                modality_files_to_inactivate[fpath].append((i[1], i[2], i[3], i[4], i[5]))

        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for relpath in paths:
                future = executor.submit(_update_manifest_one_file, root, relpath, modality, curtimestamp, modality_files_to_inactivate)
                futures.append(future)
            
            for future in concurrent.futures.as_completed(futures):
                myargs = future.result()
                status = myargs[8]
                rlpath = myargs[1]
                if status == "ERROR1":
                    print("ERROR: Multiple active files with that path - manifest is not consistent", relpath)
                elif status == "ERROR2":
                    print("ERROR: File found but no metadata.", rlpath)
                elif status == "ERROR3":
                    print("ERROR: File size is different but modtime is the same.", rlpath)
                elif status == "KEEP":
                    del modality_files_to_inactivate[myargs[1]]
                else:
                    if verbose:
                        if status == "MOVED":
                            print("INFO: COPIED/MOVED ", rlpath)
                        elif status == "UPDATED":
                            print("INFO: UPDATED ", rlpath)
                        elif status == "ADDED":
                            print("INFO: ADDED ", rlpath)
                    all_insert_args.append(myargs)
        
        # update the modality_files_to_inactivate with the new files to inactivate.
        files_to_inactivate.update(modality_files_to_inactivate)

        # for all files that were active but aren't there anymore
        # invalidate in manifest
        
        for relpath, vals in modality_files_to_inactivate.items():
            # check if there are non alphanumeric characters in the path
            # if so, print out the path
            if not (relpath.isalnum() or relpath.isascii()):
                print("INFO: Path contains non-alphanumeric characters:", relpath)
            if verbose:
                if (relpath in paths):
                    print("INFO: OUTDATED  ", relpath)
                else:
                    print("INFO: DELETED  ", relpath)
            state = "OUTDATED" if (relpath in paths) else "DELETED"
            for v in vals:
                all_del_args.append((curtimestamp, state, v[0]))
        # print("SQLITE update arguments ", all_del_args)

    if (len(all_insert_args) > 0) or (len(all_del_args) > 0):
        # back up only on upload
        # backup_manifest(databasename)
        
        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()

        try:
            if len(all_insert_args) > 0:
                cur.executemany("INSERT INTO manifest (\
                    PERSON_ID, \
                    FILEPATH, \
                    MODALITY, \
                    SRC_MODTIME_us, \
                    SIZE, \
                    MD5, \
                    TIME_VALID_us, \
                    UPLOAD_DTSTR, \
                    STATE, \
                    MD5_DURATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", all_insert_args)
        except sqlite3.IntegrityError as e:
            print("ERROR: update ", e)
            print(all_insert_args)
            
        if (len(all_del_args) > 0):
            cur.executemany("UPDATE manifest SET TIME_INVALID_us=?, STATE=? WHERE file_id=?", all_del_args)
            
        con.commit() 
        con.close()
    else:
        print("INFO: Nothing to change in manifest.")
        


# all files to upload are marked with upload_dtstr = NULL. 
def list_files(databasename="journal.db", version: Optional[str] = None, 
                         modalities: list[str] = None,
                         verbose:bool = False):
    """
    Selects the files to upload from the manifest database.  Print to stdout if outfile is not specified, and return list.

    Args:
        databasename (str, optional): The name of the manifest database. Defaults to "journal.db".
        version (str, optional): The version of the upload, which is the datetime of an upload.  If None (default) the un-uploaded files are returned
        outfilename (Optional[str], optional): The name of the output file to write the selected files. Defaults to None.
        verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
        List[str]: The list of files to upload.
    """
    # Rest of the code...
        
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='manifest'").fetchone():
        print(f"ERROR: table manifest does not exist.")
        con.close()
        return None

    # identify 3 subsets:   deleted, updated, and added.
    # deleted:  file with invalid time stamp.  No additional valid time stamp
    # added:  file with null invalid time stemp and no prior entry with non-null invalid time stamp
    # modified:  file with null invalid time, and a prior entry with non-null invalid time stamp
    # files_to_remove = cur.execute("Select filepath, size, md5, time_invalid_us from manifest where upload_dtstr IS NULL").fetchall()
    # to_remove = [ ]
    
    version_where = f"= '{version}'" if version is not None else "IS NULL"
    if (modalities is None) or (len(modalities) == 0):    
        # select where upload_dtstr is NULL or time_invalid_us is NULL
        files_to_update = cur.execute(f"Select filepath, time_invalid_us from manifest where upload_dtstr {version_where}").fetchall()
    else:
        files_to_update = []
        for modality in modalities:
            files_to_update += cur.execute(f"Select filepath, time_invalid_us from manifest where upload_dtstr {version_where} AND MODALITY = '{modality}'").fetchall()
    # print(files_to_update)
    # only need to add, not delete.  rely on manifest to mark deletion.
    con.close()
    
    print("INFO: extracted files to upload for ", ("current" if version is None else version), " upload version and modalities =", 
          (",".join(modalities) if modalities is not None else "all"))
    
    # this is pulling back only files with changes during since the last upload.  an old file may be inactivated but is in a previous upload
    # so we only see the "new" part, which would look like an add.
    # any deletion would be updating entries in a previous upload batch, so would not see deletion
    # only update we would see is if a file is added, then updated between 2 uploads.   Not thta add/delete/add would show as moved or updated.
    
    # deleted or overwritten files would still be in old version of the cloud.  so we don't need to delete.
    
    # first get the set of active and inactive files.  these are hashtables for quick lookups.
    active_files = set()
    inactive_files = set()
    for (fn, invalidtime) in files_to_update:
        if invalidtime is None:
            active_files.add(fn)  # current files or added files
        else:
            inactive_files.add(fn) # old files or deleted files
    
    updates = active_files.intersection(inactive_files)
    adds = active_files - updates
    deletes = inactive_files - updates
    if verbose:
        for f in adds:
            print("INFO: ADD ", f)
        for f in updates:
            print("INFO: UPDATE ", f)
        for f in deletes:
            print("INFO: DELETE ", f)
            
    file_list = list(active_files)
    # sort the file-list
    # file_list.sort()
    
    if (file_list is None) or (len(file_list) == 0):
        print("INFO: No active files found")
    
    # do we need to delete files in central at all? NO.  manifest will be updated.
    # we only need to add new files
                
    return file_list

# def list_uploads(databasename="journal.db"):
#     """
#     Retrieve a list of unique upload dates from the manifest database.

#     Args:
#         databasename (str): The name of the manifest database file. Defaults to "journal.db".

#     Returns:
#         list: A list of unique upload dates from the manifest database.

#     Raises:
#         None

#     """
    
#     if not os.path.exists(databasename):
#         print(f"ERROR: No manifest exists for filename {databasename}")
#         return None
    
#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()
    
#     # List unique values in upload_dtstr column in manifest table
#     uploads = [u[0] for u in cur.execute("SELECT DISTINCT upload_dtstr FROM manifest;").fetchall()]
#     con.close()
    
#     if (uploads is not None) and (len(uploads) > 0):
#         print("INFO: upload records in the database:")
#         for table in uploads:
#             print("INFO:  ", table)
#     else:
#         print("INFO: No uploads found in the database.")
    
#     return uploads

# def list_manifests(databasename="journal.db"):
#     """
#     Lists the tables in the SQLite database specified by the given database name.

#     Args:
#         databasename (str): The name of the SQLite database file. Defaults to "journal.db".

#     Returns:
#         list: A list of tables in the database.

#     Raises:
#         None

#     """
    
#     if not os.path.exists(databasename):
#         print(f"ERROR: No manifest exists for filename {databasename}")
#         return None
    
#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()
    
#     # List tables in the SQLite database
#     tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    
#     if tables:
#         print("INFO: Tables in the database:")
#         for table in tables:
#             print("INFO:  ", table[0])
#     else:
#         print("INFO: No tables found in the database.")
    
#     con.close()

#     return tables


# def delete_manifest(databasename="journal.db", suffix:str = None):
#     if (suffix is None) or (suffix == "") :
#         print("ERROR: you must specify a suffix for the 'manifest' table.")
#         return None
    
#     if not os.path.exists(databasename):
#         # os.remove(pushdir_name + ".db")
#         print(f"ERROR: No manifest exists for filename {databasename}")
#         return None

#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()

#     # check if the target table exists
#     manifestname = "manifest_" + suffix

#     cur.execute(f"DROP TABLE IF EXISTS {manifestname}")

#     print("NOTE: Dropped table ", manifestname)

#     con.close()
    
#     return manifestname

    
# this should only be called when an upload is initiated.
def backup_manifest(databasename="journal.db", suffix:str = None):
    """
    Backs up the database manifest table to a new table with a given suffix.

    Args:
        databasename (str): The name of the database file. Defaults to "journal.db".
        suffix (str, optional): The suffix to be added to the backup table name. Defaults to None.

    Returns:
        str: The name of the backup table.

    Raises:
        None

    """
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return None

    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='manifest'").fetchone():
        print(f"ERROR: table manifest does not exist to backup.")
        con.close()
        return None

    suff = suffix if suffix is not None else strftime("%Y%m%d%H%M%S", gmtime())

    filesbackup = "manifest_" + suff
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM manifest")

    print("INFO: backed up database manifest to ", filesbackup)

    con.close()

    return filesbackup

    
# this should only be called when an upload is initiated.
# def restore_manifest(databasename="journal.db", suffix:str = None):
#     """
#     Restores the database manifest from a backup table.

#     Args:
#         databasename (str, optional): The name of the database file. Defaults to "journal.db".
#         suffix (str, optional): The suffix of the backup table. If not specified, an error message is printed.

#     Returns:
#         str: The name of the restored backup table.

#     Raises:
#         None

#     """
#     if suffix is None:
#         print("ERROR: previous version not specified.  please supply a suffix.")
#         return
    
#     if not os.path.exists(databasename):
#         # os.remove(pushdir_name + ".db")
#         print(f"ERROR: No manifest exists for filename {databasename}")
#         return

#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()

#     # check if the target table exists
#     manifestname = "manifest_" + suffix
#     if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{manifestname}'").fetchone():
#         print(f"ERROR:  backup table {manifestname} does not exist.")
#         con.close()
#         return

#     # do not backup, similar to git checkout - current changes are lost if not committed.
#     # backup_name = backup_manifest(databasename)
    
#     # then restore
#     cur.execute(f"DROP TABLE IF EXISTS manifest")
#     cur.execute(f"CREATE TABLE manifest AS SELECT * FROM {manifestname}")

#     print("INFO: restored database manifest from ", manifestname)

#     con.close()
#     return manifestname


class sync_state(Enum):
    UNKNOWN = 0
    MATCHED = 1
    MISMATCHED = 2
    MISSING_SRC = 3
    MISSING_DEST = 4
    DELETED = 5
    MISSING_IN_DB = 6
    MULTIPLE_ACTIVES_IN_DB = 7

def _upload_and_verify(src_path : FileSystemHelper, 
                       fn : str,
                       dated_dest_path : FileSystemHelper,
                       databasename="journal.db"):
    state = sync_state.UNKNOWN
    # fid_list = []
    file_id = None
    del_list = []
    
    # ======= copy.  parallelizable
    start = time.time()
    srcfile = src_path.root.joinpath(fn)
    if srcfile.exists():
        src_path.copy_file_to(fn, dated_dest_path) 
    else:
        # print("ERROR:  file not found ", srcfile)
        state = sync_state.MISSING_SRC
    copy_time = time.time() - start
    verify_time = None
    
    # ======= read metadata from db.  May be parallelizable        
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    # note that this is not the same list as the selected files since that one excludes deleted  
    result = cur.execute(f"Select file_id, size, md5 from manifest where time_invalid_us is NULL AND upload_dtstr is NULL AND filepath = '{fn}'").fetchall()
    con.close()

    verified = False
    if (state == sync_state.UNKNOWN):
        # ======= verify.  parallelizable.
        # 4 cases:  
        # case 1 missing in db. this should not happen.;
        if (result is None) or (len(result) == 0):
            state = sync_state.MISSING_IN_DB
            # print("ERROR: uploaded file is not matched in manifest.  should not happen.")
        elif (len(result) > 1):
            state = sync_state.MULTIPLE_ACTIVES_IN_DB
            # print("ERROR: multiple entries in manifest for ", fn)
    
    # src metadata
    # print(result)
    if (state == sync_state.UNKNOWN):

        file_id, size, md5 = result[0]

        # get the dest file info
        start = time.time()
        dest_meta = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = fn, with_metadata = True, with_md5 = True)
        # case 2 missing in cloud.  this is the missing case;
        verify_time = time.time() - start
        if dest_meta is None:
            state = sync_state.MISSING_DEST
            # print("ERROR:  missing file at destination", fn)
    
    if (state == sync_state.UNKNOWN):
        
        if (size == dest_meta['size']):
            
            # start = time.time()
            # dest_md5 = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = fn, with_metadata = False, with_md5 = True)['md5']
            # verify_time = time.time() - start
            dest_md5 = dest_meta['md5']
            
            if (md5 == dest_md5):
                # case 3: matched
                state = sync_state.MATCHED
                verified = True
                # fid_list.append(file_id)  # verified.  this may not be parallelizable.
            else:
                # case 4: mismatched.
                state = sync_state.MISMATCHED
                # entries are not updated because of mismatch.
                # print("ERROR:  mismatched upload file ", fn, " upload failed? fileid ", file_id, ": cloud size ", dest_meta['size'], " manifest size ", size, "; cloud md5 ", dest_md5, " manifest md5 ", md5)            
        else:
            # case 4: mismatched.
            state = sync_state.MISMATCHED
            # entries are not updated because of mismatch.
            # print("ERROR:  mismatched upload file ", fn, " upload failed? fileid ", file_id, ": cloud size ", dest_meta['size'], " manifest size ", size)
            verify_time = 0

    # if verified upload, then remove any old file
    # ======= read metadata from db.  May be parallelizable
    if verified:        
        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()
        # note that this is not the same list as the selected files since that one excludes deleted  
        result = cur.execute(f"Select file_id from manifest where time_invalid_us is NOT NULL AND upload_dtstr is NULL AND filepath = '{fn}'").fetchall()
        con.close()

        if (result is not None) and (len(result) > 0):
            for (file_id,) in result:
                # state = sync_state.DELETED
                del_list.append(file_id)  # this may not be paralleizable.

    return (fn, state, file_id, del_list, copy_time, verify_time)




def upload_files(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                 databasename="journal.db",
                 modalities: list[str] = DEFAULT_MODALITIES, amend:bool = False, verbose: bool = False):
    """
    Uploads files from the source path to the destination path and updates the manifest.
    only allows uploaded new files and not reupload of previous upload.
    note that manifest is updated only when verified. 

    Args:
        src_path (FileSystemHelper): The source path from where the files will be uploaded.
        dest_path (FileSystemHelper): The destination path where the files will be copied to.
        databasename (str, optional): The name of the manifest database file. Defaults to "journal.db".
        upload_datetime_str (str, optional): The upload datetime string. Defaults to None.

    Returns:
        str: The upload version string.

    Raises:
        FileNotFoundError: If the manifest database file does not exist.
        AssertionError: If some uploaded files are not in the manifest or if there are mismatched files.

    """

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return

    # if amend then get the latest version from db
    if amend:
        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()
    
        upload_ver_str = cur.execute("SELECT MAX(upload_dtstr) FROM manifest").fetchone()[0]
        if upload_ver_str is None:
            raise ValueError("ERROR: no previous upload found to amend.")
        con.close()
    else:
        # get the current datetime
        upload_ver_str = strftime("%Y%m%d%H%M%S", gmtime())
        
    # create a dated root directory to receive the files
    dated_dest_path = FileSystemHelper(dest_path.root.joinpath(upload_ver_str))

    #---------- upload files.    
    # first get the list of files for the requested version
    files_to_upload = list_files(databasename, version=None, modalities=modalities, verbose = False)
    if (files_to_upload is None) or (len(files_to_upload) == 0):
        print("INFO: no files copied.  Done")
        return None
    
    # copy file, verify and update manifest
    update_args = []
    del_args = []
    missing_dest = set()
    missing_src = set()
    multiple_actives = set()
    matched = set()
    mismatched = set()
    replaced = set()
    deleted = set()
    missing_in_db = set()

    # NEW    
    step = 100
    nthreads = max(1, os.cpu_count() - 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers = nthreads) as executor:
        futures = []
        for fn in files_to_upload:
            if verbose:
                print("INFO:  copying ", fn, " from ", str(src_path.root), " to ", str(dated_dest_path.root))
            future = executor.submit(_upload_and_verify, src_path, fn, dated_dest_path, databasename)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            (fn2, state, fid, del_list, copy_time, verify_time) = future.result()
            if state == sync_state.MISSING_IN_DB:
                print("ERROR: uploaded file is not matched in manifest.  should not happen.", fn2)
                missing_in_db.add(fn2)
            elif state == sync_state.MISSING_DEST:
                missing_dest.add(fn2)
                print("ERROR:  missing file at destination", fn2)
            elif state == sync_state.MISSING_SRC:
                print("ERROR:  file not found ", fn2)
                missing_src.add(fn2)
            elif state == sync_state.MULTIPLE_ACTIVES_IN_DB:
                print("ERROR: multiple entries in manifest for ", fn2)
                multiple_actives.add(fn2)
            elif state == sync_state.MATCHED:
                # merge the updates for matched.
                matched.add(fn2)
                update_args.append((upload_ver_str, copy_time, verify_time, fid))
                del_args += [(upload_ver_str, fid) for fid in del_list]
                if len(del_list) > 0:
                    replaced.add(fn2)
            elif state == sync_state.MISMATCHED:
                mismatched.add(fn2)
                print("ERROR:  mismatched upload file ", fn2, " upload failed? fileid ", fid)            

            # elif state == sync_state.DELETED:
            #     deleted.add(fn2)
            
            # update the manifest - likely not parallelizable.
            if len(update_args) >= step:
                con = sqlite3.connect(databasename, check_same_thread=False)
                cur = con.cursor()
                try:        
                    cur.executemany("UPDATE manifest SET upload_dtstr=?, upload_duration=?, verify_duration=? WHERE file_id=?", update_args)
                    cur.executemany("UPDATE manifest SET upload_dtstr=? WHERE file_id=?", del_args)
                except sqlite3.InterfaceError as e:
                    print("ERROR: update ", e)
                    print(update_args)
                    
                con.commit() 
                con.close()
                update_args = []
                del_args = []
            # 
    
    #### --------------OLD begin
    # # then copy the files over
    # count = src_path.copy_files_to(files_to_upload, dated_dest_path, verbose=verbose, )
    
    # print("INFO: copied ", count, " files from ", src_path.root, " to ", dated_dest_path.root)

    # #----------- verify upload and update manifest
    
    # # get list of files that have been uploaded and needs to be updated.
    # # update the manifest with what's uploaded, verifying the size and md5 also.
    # con = sqlite3.connect(databasename, check_same_thread=False)
    # cur = con.cursor()
    # # note that this is not the same list as the selected files since that one excludes deleted  
    # entries_in_version = cur.execute("Select file_id, filepath, modality, size, md5 from manifest where time_invalid_us is NULL AND upload_dtstr is NULL").fetchall()
    # con.close()

    # # check for boundary cases
    # uploaded_files = set(files_to_upload)
    # files_to_update = set([f[1] for f in entries_in_version])
    # if len(uploaded_files - files_to_update) > 0:
    #     missing_in_db = uploaded_files - files_to_update
    #     print("ERROR: some uploaded files are not in manifest.  ", uploaded_files - files_to_update)
        
    # # other cases are handled below.
    
    modality_set = set(modalities) if modalities is not None else set(DEFAULT_MODALITIES)
    # for (file_id, fn, modality, size, md5) in entries_in_version:
    #     if modality not in modality_set:
    #         # exclude the files that do not have requested modality
    #         continue
                
    #     if fn in uploaded_files:
    #         dest_meta = dated_dest_path.get_file_metadata(fn)
    #         if dest_meta is None:
    #             missing.add(fn)
    #             print("ERROR:  missing file at destination", fn)
    #             continue

    #         dest_md5 = dated_dest_path.get_file_md5(fn)
            
    #         if (size == dest_meta['size']) and (md5 == dest_md5):
    #             matched.add(fn)
    #             update_args.append((upload_ver_str, file_id))  # verified
    #         else:      
    #             mismatched.add(fn)
    #             # entries are not updated because of mismatch.
    #             print("ERROR:  mismatched file ", fn, " upload failed? fileid ", file_id, ": cloud size ", dest_meta['size'], " manifest size ", size, "; cloud md5 ", dest_md5, " manifest md5 ", md5)
    #     else:
    #         print("ERROR:  unuploaded active file in upload set ", fn, " fileid ", file_id, " : size ", size, " md5 ", md5)
    #### --------------OLD end
    
    if len(update_args) > 0:
        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()
        try:        
            cur.executemany("UPDATE manifest SET upload_dtstr=?, upload_duration=?, verify_duration=? WHERE file_id=?", update_args)
            cur.executemany("UPDATE manifest SET upload_dtstr=? WHERE file_id=?", del_args)
        except sqlite3.InterfaceError as e:
            print("ERROR: update ", e)
            print(update_args)
            
        con.commit() 
        con.close()
        update_args = []
        del_args = []

    # handle all deleted (only undeleted files that are not "uploaded" are the ones that are were added and deleted between uploads.).
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    # note that this is not the same list as the selected files since that one excludes deleted  
    entries_in_version = cur.execute("Select file_id, filepath, modality from manifest where time_invalid_us is not NULL AND upload_dtstr is NULL").fetchall()
    con.close()

    modality_set = set(modalities) if modalities is not None else set(DEFAULT_MODALITIES)
    for (file_id, fn, modality) in entries_in_version:
        if modality not in modality_set:
            # exclude the files that do not have requested modality
            continue
        
        deleted.add(fn)
        del_args.append((upload_ver_str, file_id))  # deleted
        
    if len(del_args) > 0:
        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()
        try:        
            cur.executemany("UPDATE manifest SET upload_dtstr=? WHERE file_id=?", del_args)
        except sqlite3.InterfaceError as e:
            print("ERROR: update ", e)
            print(update_args)
        con.commit() 
        con.close()

    # create a versioned backup - AFTER updating the manifest.
    backup_manifest(databasename, suffix=upload_ver_str)
    
    # finally, upload the manifest.
    manifest_path = FileSystemHelper(os.path.dirname(os.path.abspath(databasename)))  # local fs, no client.
    
    # Question - do we put manifest in the dated target subdirectory?  YES for now
    # Question - do we leave the manifest update time as NULL?  
    # Question - do we copy the manifest into the source directory? YES for now (in case of overwrite)
    manifest_fn = os.path.basename(databasename)
    manifest_path.copy_files_to([manifest_fn], dated_dest_path)
    # manifest_path.copy_files_to([(manifest_fn, "_".join([manifest_fn, upload_ver_str]))], src_path)

    return upload_ver_str

def _get_file_info(dated_dest_path: FileSystemHelper, file_info: tuple):
    
    fid, fn, size, md5 = file_info
    
    dest_meta = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = fn, with_metadata = True, with_md5 = True)
    dest_md5 = dest_meta['md5'] if dest_meta is not None else None

    return (dest_meta, dest_md5, fid, fn, size, md5)

# verify a set of files that have been uploaded.  version being None means using the last upload
def verify_files(dest_path: FileSystemHelper, databasename:str="journal.db", 
                 version: Optional[str] = None, 
                 modalities: Optional[list] = None, verbose: bool = False):
    """
    Verify the integrity of uploaded files by comparing their metadata and MD5 checksums.

    Args:
        dest_path (FileSystemHelper): The destination path where the files are stored.
        databasename (str, optional): The name of the database file. Defaults to "journal.db".
        upload_datetime_str (str, optional): The upload datetime string. If not provided, the latest upload datetime will be used.

    Returns:
        set: A set of filenames that have mismatched metadata or MD5 checksums.
    """
    
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return

    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    # get the current datetime
    dtstr = cur.execute("SELECT MAX(upload_dtstr) FROM manifest").fetchone()[0] if version is None else version
    
    if dtstr is None:
        print("INFO: no previous upload found.")
        con.close()
        return
    
    
    # get the list of files, md5, size for upload_dtstr matching dtstr
    if modalities is None:
        files_to_verify = cur.execute(f"Select file_id, filepath, size, md5 from manifest where upload_dtstr = '{dtstr}' AND time_invalid_us is NULL").fetchall()
    else:
        files_to_verify = []
        for modality in modalities:
            files_to_verify += cur.execute(f"Select file_id, filepath, size, md5 from manifest where upload_dtstr = '{dtstr}' AND time_invalid_us is NULL AND MODALITY = '{modality}'").fetchall()
        
    con.close()
    
    #---------- verify files.    
    # create a dated root directory to receive the files
    dated_dest_path = FileSystemHelper(dest_path.root.joinpath(dtstr))
    
    # now compare the metadata and md5
    missing = set()
    matched = set()
    mismatched = set()
    threads = max(1, os.cpu_count() - 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers = threads) as executor:
        futures = []
        for file_info in files_to_verify:
            future = executor.submit(_get_file_info, dated_dest_path, file_info )
            futures.append(future)
            
        for future in concurrent.futures.as_completed(futures):
            (dest_meta, dest_md5, fid, fn, size, md5) = future.result()
            if dest_meta is None:
                missing.add(fn)
                print("ERROR:  missing file ", fn)
                continue

            if (size == dest_meta['size']) and (md5 == dest_md5):
                if verbose:
                    print("INFO: Verified upload", dtstr, " ", fn, " ", fid)
                matched.add(fn)
            else:
                print("ERROR:  mismatched file ", fid, " ", fn, " for upload ", dtstr, ": cloud size ", dest_meta['size'], " manifest size ", size, "; cloud md5 ", dest_md5, " manifest md5 ", md5)
                mismatched.add(fn)
                
    print("INFO:  verified ", len(matched), " files, ", len(mismatched), " mismatched, ", len(missing), " missing.")
            
    return matched, mismatched, missing