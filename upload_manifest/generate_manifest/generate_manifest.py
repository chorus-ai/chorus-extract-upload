import os
import sqlite3
import pandas as pd
import time
import math
from storage_pathlib import FileSystemHelper

from typing import Optional, Union
from time import strftime, gmtime


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

# process:
#   1. local manifest is created
#   2. local manifest is used to generate update list
#   3. local manifest is used to send data to cloud
#   4. local manifest is verified with cloud files



# simple assumption for upload between updates
# we always compare to the last manifest.
# all remote stores are dated, so override with partial uploads.



# current assumptions/working decisions for initial run:
# 1 file per record - can be extended to 1 dir per record or using a file-record mapping
# no upload dir specified on local site's side
# entity relationships: many sessions per person, many records per session, many files per record
# files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension
def gen_manifest(root : FileSystemHelper, subdirs: list[str] = ['Waveforms', 'Images', 'OMOP'], databasename="journal.db", verbose: bool = False):
    
    
    print("INFO: Creating Manifest...", flush=True)
    
    if os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print("ERROR: Initial manifest, but a journal with that name already exists. Please delete and re-run.")
        exit()
        
    con = sqlite3.connect(databasename)
    cur = con.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS manifest(\
                FILE_ID INTEGER NOT NULL PRIMARY KEY, \
                PERSON_ID INTEGER, \
                FILEPATH TEXT, \
                SRC_MODTIME_us INTEGER, \
                SIZE INTEGER, \
                MD5 TEXT, \
                TIME_VALID_us INTEGER, \
                TIME_INVALID_us INTEGER, \
                UPLOAD_DTSTR TEXT)")  # datetime string of the upload.
    
    paths = []
    for subdir in subdirs:
        paths1 = root.get_files(subdir)
        paths += paths1
        

    curtimestamp = int(math.floor(time.time() * 1e6))

    all_args = []
    for i_path, relpath in enumerate(paths):
        # print(i_path, relpath)
        meta = root.get_file_metadata(relpath)
        # first item is personid.
        personid = relpath.split(os.path.sep)[0] if ("Waveforms" in relpath) or ("Images" in relpath) else None
        filesize = meta['size']
        modtimestamp = meta['modify_time_us']
        mymd5 = root.get_file_md5(relpath)
        
        myargs = (i_path, personid, relpath, modtimestamp, filesize, mymd5, curtimestamp, None, None)

        if verbose:
            print("INFO: ADDED ", relpath)

        all_args.append(myargs)

    if len(all_args) > 0:
        cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)
        
    con.commit() 
    con.close()


# record is a mess with a file-wise traversal. Files need to be grouped by unique record ID (visit_occurence?)
def update_manifest(root: FileSystemHelper, subdirs: list[str] = ['Waveforms', 'Images', 'OMOP'], databasename="journal.db", verbose: bool = False):

    print("INFO: Updating Manifest...", databasename, flush=True)

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return
    
    con = sqlite3.connect(databasename)
    cur = con.cursor()
    
    # check if manifest table exists.
    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='manifest'").fetchone():
        print(f"ERROR:  table manifest does not exist.")
        con.close()
        return

    paths = []
    for subdir in subdirs:
        paths1 = root.get_files(subdir)
        paths += paths1

    # print(paths[0])

    curtimestamp = int(math.floor(time.time() * 1e6))

    i_file = cur.execute("SELECT MAX(file_id) from manifest").fetchone()[0]
    if i_file is None:
        print("WARNING:  updating an empty database")
        i_file = 1
    else: 
        i_file += 1
    # print(i_file)

    # get all active files, these are active-pending-delete until the file is found in filesystem
    activefiletuples = cur.execute("Select filepath, file_id, size, src_modtime_us, md5, upload_dtstr from manifest where TIME_INVALID_us IS NULL").fetchall()
    con.close()
    
    # initialize by putting all files in files_to_inactivate.  remove from this list if we see the files on disk
    files_to_inactivate = {}
    for i in activefiletuples:
        fpath = i[0]
        if fpath not in files_to_inactivate.keys():
            files_to_inactivate[fpath] = [ (i[1], i[2], i[3], i[4], i[5]) ]
        else:
            files_to_inactivate[fpath].append((i[1], i[2], i[3], i[4], i[5]))

    all_insert_args = []
    for i_path, relpath in enumerate(paths):
        # print(path)
        
        # get information about the current file
        meta = root.get_file_metadata(relpath)
        # first item is personid.
        personid = relpath.split(os.path.sep)[0] if ("Waveforms" in relpath) or ("Images" in relpath) else None
        filesize = meta['size']
        modtimestamp = meta['modify_time_us']
        # 

        # get information about the old file
        # query files by filename, modtime, and size
        # this should be merged with the initial call to get active fileset since we in theory will be checking most files.
        # filecheckrow = cur.execute("Select file_id, size, src_modtime_us, md5, upload_dtstr from manifest where filepath=? and TIME_INVALID_us IS NULL", [(relpath)])

        # results = filecheckrow.fetchall()
        if relpath in files_to_inactivate.keys():
            results = files_to_inactivate[relpath]
            # There should only be 1 active file according to the path in a well-formed 
            if len(results) > 1:
                print("ERROR: Multiple active files with that path - can't compare to old files")
                return
            elif len(results) == 1:
                (oldfileid, oldsize, oldmtime, oldmd5, oldsync) = results[0]
                
                # if old size and new size, old mtime and new mtime are same (name is already matched)
                # then same file, skip rest.
                if (oldmtime == modtimestamp):
                    if (oldsize == filesize):
                        # files are very likely the same because of size and modtime match
                        del files_to_inactivate[relpath]  # do not mark file as inactive.
                        # print("SAME ", relpath )
                        continue
                    else:
                        #time stamp same but file size is different?
                        print("ERROR: File size is different but modtime is the same.", relpath)
                        continue
                    
                else:
                    # timestamp changed. we need to compute md5 to check, or to update.
                    mymd5 = root.get_file_md5(relpath)

                    # files are extremely likely the same just moved.
                    # set the old entry as invalid and add a new one that's essentially a copy except for modtime..
                    # choosing not to change SYNC_TIME
                    # cur.execute("UPDATE manifest SET TIME_INVALID_us=? WHERE file_id=?", (curtimestamp, oldfileid))
                    if (oldsize == filesize) and (mymd5 == oldmd5):
                        # essentially copy the old and update the modtime.
                        myargs = (i_file, personid, relpath, modtimestamp, oldsize, oldmd5, curtimestamp, None, oldsync)
                        if verbose:
                            print("INFO: COPIED/MOVED ", relpath)
                        # files_to_inactivate.remove(oldfileid)  # we should mark old as inactive 
                    else:
                        # files are different.  set the old file as invalid and add a new file.
                        myargs = (i_file, personid, relpath, modtimestamp, filesize, mymd5, curtimestamp, None, None)
                        if verbose:
                            print("INFO: UPDATED ", relpath)
                        # modified file. old one should be removed.
                    
                    i_file += 1 # this is a uid.  so has to increment.
                        
                    all_insert_args.append(myargs)
                    # cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)            
            else:
                print("ERROR: File found but no metadata.", relpath)
                continue
        else:
            # if DNE, add new file
            mymd5 = root.get_file_md5(relpath)

            myargs = (i_file, personid, relpath, modtimestamp, filesize, mymd5, curtimestamp, None, None)
            all_insert_args.append(myargs)
            # cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            if verbose:
                print("INFO: ADDED  ", relpath)
            i_file += 1


    # for all files that were active but aren't there anymore
    # invalidate in manifest
    # for file_id,record_id in zip(activefilemap.keys(),activefilemap.values()):
    # myargs = map(lambda v: (curtimestamp, v), files_to_inactivate)
    
    myargs = []
    for relpath, vals in files_to_inactivate.items():
        if verbose:
            if (relpath in paths):
                print("INFO: OUTDATED  ", relpath)
            else:
                print("INFO: DELETED  ", relpath)
        
        for v in vals:
            myargs.append((curtimestamp, v[0]))
    # print("SQLITE update arguments ", myargs)

    if (len(all_insert_args) > 0) or (len(myargs) > 0):
        # back up only on upload
        # backup_manifest(databasename)
        
        con = sqlite3.connect(databasename)
        cur = con.cursor()

        if len(all_insert_args) > 0:
            cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", all_insert_args)
        if (len(myargs) > 0):
            cur.executemany("UPDATE manifest SET TIME_INVALID_us=? WHERE file_id=?",myargs)

        con.commit() 
        con.close()
    else:
        print("INFO: Nothing to change in manifest.")
        



# all files to upload are marked with upload_dtstr = NULL. 
def select_files_to_upload(databasename="journal.db", outfilename : Optional[str] = None, verbose:bool = False):

    print("INFO: Extracting Active Files to upload.  Deleted files are not reported.", flush=True)

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return
    
    con = sqlite3.connect(databasename)
    cur = con.cursor()

    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='manifest'").fetchone():
        print(f"ERROR: table manifest does not exist.")
        con.close()
        return None

    # identify 3 subsets:   deleted, updated, and added.
    # deleted:  upload_dtstr=NULL file with invalid time stamp.  No additional valid time stamp
    # added:  upload_dtstr=NULL file with null invalid time stemp and no prior entry with non-null invalid time stamp
    # modified:  upload_dtstr=NULL file with null invalid time, and a prior entry with non-null invalid time stamp
    # files_to_remove = cur.execute("Select filepath, size, md5, time_invalid_us from manifest where upload_dtstr IS NULL").fetchall()
    # to_remove = [ ]
    
    # select where upload_dtstr is NULL or time_invalid_us is NULL
    files_to_update = cur.execute("Select filepath, time_invalid_us from manifest where upload_dtstr IS NULL").fetchall()
    # print(files_to_update)
    # only need to add, not delete.  rely on manifest to mark deletion.
    con.close()
    
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
    
    if outfilename is not None:
        with open(outfilename, 'w') as f:
            f.write("\n".join(file_list))
    # else:
    #     print("Files to upload: ")
    #     print("\n".join(file_list))
                
    return file_list

def list_uploads(databasename="journal.db"):
    if not os.path.exists(databasename):
        print(f"ERROR: No manifest exists for filename {databasename}")
        return None
    
    con = sqlite3.connect(databasename)
    cur = con.cursor()
    
    # List unique values in upload_dtstr column in manifest table
    uploads = [u[0] for u in cur.execute("SELECT DISTINCT upload_dtstr FROM manifest;").fetchall()]
    con.close()
    
    if (uploads is not None) and (len(uploads) > 0):
        print("INFO: upload records in the database:")
        for table in uploads:
            print("INFO:  ", table)
    else:
        print("INFO: No uploads found in the database.")
    
    return uploads

def list_manifests(databasename="journal.db"):
    if not os.path.exists(databasename):
        print(f"ERROR: No manifest exists for filename {databasename}")
        return None
    
    con = sqlite3.connect(databasename)
    cur = con.cursor()
    
    # List tables in the SQLite database
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    
    if tables:
        print("INFO: Tables in the database:")
        for table in tables:
            print("INFO:  ", table[0])
    else:
        print("INFO: No tables found in the database.")
    
    con.close()

    return tables


# def delete_manifest(databasename="journal.db", suffix:str = None):
#     if (suffix is None) or (suffix == "") :
#         print("ERROR: you must specify a suffix for the 'manifest' table.")
#         return None
    
#     if not os.path.exists(databasename):
#         # os.remove(pushdir_name + ".db")
#         print(f"ERROR: No manifest exists for filename {databasename}")
#         return None

#     con = sqlite3.connect(databasename)
#     cur = con.cursor()

#     # check if the target table exists
#     manifestname = "manifest_" + suffix

#     cur.execute(f"DROP TABLE IF EXISTS {manifestname}")

#     print("NOTE: Dropped table ", manifestname)

#     con.close()
    
#     return manifestname

    
# this should only be called when an upload is initiated.
def backup_manifest(databasename="journal.db", suffix:str = None):
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return None

    con = sqlite3.connect(databasename)
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
def restore_manifest(databasename="journal.db", suffix:str = None):
    if suffix is None:
        print("ERROR: previous version not specified.  please supply a suffix.")
        return
    
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return

    con = sqlite3.connect(databasename)
    cur = con.cursor()

    # check if the target table exists
    manifestname = "manifest_" + suffix
    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{manifestname}'").fetchone():
        print(f"ERROR:  backup table {manifestname} does not exist.")
        con.close()
        return

    # do not backup, similar to git checkout - current changes are lost if not committed.
    # backup_name = backup_manifest(databasename)
    
    # then restor
    cur.execute(f"DROP TABLE IF EXISTS manifest")
    cur.execute(f"CREATE TABLE manifest AS SELECT * FROM {manifestname}")

    print("INFO: restored database manifest from ", manifestname)

    con.close()
    return manifestname



def upload_files(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                 databasename="journal.db", upload_datetime_str: Optional[str] = None):

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return

    # get the current datetime
    upload_ver_str = strftime("%Y%m%d%H%M%S", gmtime()) if upload_datetime_str is None else upload_datetime_str

    #---------- upload files.    
    # first get the list of files
    files_to_upload = select_files_to_upload(databasename)

    # create a dated root directory to receive the files
    dated_dest_path = FileSystemHelper(dest_path.root.joinpath(upload_ver_str))
    # then copy the files over
    src_path.copy_files_to(files_to_upload, dated_dest_path)
    
    #----------- verify upload and update manifest
    # update the manifest with what's uploaded, verifying the size and md5 also.
    con = sqlite3.connect(databasename)
    cur = con.cursor()
    # note that this is not the same list as the selected files since that one excludes deleted    
    entries_to_update = cur.execute("Select file_id, filepath, size, md5, time_invalid_us from manifest where upload_dtstr IS NULL").fetchall()
    uploaded_files = set(files_to_upload)
    files_to_update = set([f[1] for f in entries_to_update])
    if len(uploaded_files - files_to_update) > 0:
        print("ERROR: some uploaded files are not in manifest.  ", uploaded_files - files_to_update)
    # other cases are handled below.
    
    args_verified = []
    args_deleted = []
    for (file_id, fn, size, md5, invalidtime) in entries_to_update:
        if fn in uploaded_files:
            dest_meta = dated_dest_path.get_file_metadata(fn)
            dest_md5 = dated_dest_path.get_file_md5(fn)
            
            if (size == dest_meta['size']) and (md5 == dest_md5):
                args_verified.append((upload_ver_str, file_id))
            else:
                print("ERROR:  mismatched file ", fn, " fileid ", file_id, ": cloud size ", dest_meta['size'], " manifest size ", size, "; cloud md5 ", dest_md5, " manifest md5 ", md5)
        else:  # should be the deleted files
            if (invalidtime is not None):
                args_deleted.append((upload_ver_str, file_id))
            else:
                print("ERROR:  missing file ", fn, " fileid ", file_id, " : size ", size, " md5 ", md5)
            
    cur.executemany("UPDATE manifest SET upload_dtstr=? WHERE file_id=?", args_verified)
    cur.executemany("UPDATE manifest SET upload_dtstr=? WHERE file_id=?", args_verified)
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
    manifest_path.copy_files_to([(manifest_fn, "_".join([manifest_fn, upload_ver_str]))], src_path)

    return upload_ver_str


def verify_files(dest_path: FileSystemHelper, databasename:str="journal.db", upload_datetime_str: Optional[str] = None):
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No manifest exists for filename {databasename}")
        return

    con = sqlite3.connect(databasename)
    cur = con.cursor()

    # get the current datetime
    dtstr = cur.execute("SELECT MAX(upload_dtstr) FROM manifest").fetchone()[0] if upload_datetime_str is None else upload_datetime_str
    
    if dtstr is None:
        print("INFO: no previous upload found.")
        return
    
    
    # get the list of files, md5, size for upload_dtstr matching dtstr
    files_to_verify = cur.execute("Select filepath, size, md5 from manifest where upload_dtstr=?", [dtstr]).fetchall()
    con.close()
    
    #---------- verify files.    
    # create a dated root directory to receive the files
    dated_dest_path = FileSystemHelper(dest_path.root.joinpath(dtstr))
    
    # now compare the metadata and md5
    mismatched = set()
    for (fn, size, md5) in files_to_verify:
        # what if the file is missing?
        
        dest_meta = dated_dest_path.get_file_metadata(fn)
        if dest_meta is None:
            print("ERROR:  missing file ", fn)
            mismatched.add(fn)
            continue
        
        dest_md5 = dated_dest_path.get_file_md5(fn)
        
        if (size == dest_meta['size']) and (md5 == dest_md5):
            print("INFO: Verified upload", dtstr, " ", fn)
        else:
            print("ERROR:  mismatched file ", fn, " for upload ", dtstr, ": cloud size ", dest_meta['size'], " manifest size ", size, "; cloud md5 ", dest_md5, " manifest md5 ", md5)
            mismatched.add(fn)
            
    return mismatched