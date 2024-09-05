import os
import sqlite3
import pandas as pd
import time
import math
from chorus_upload_journal.upload_tools.storage_helper import FileSystemHelper

from typing import Optional, Union
from time import strftime, gmtime

from cloudpathlib import AnyPath
from pathlib import Path

import re
from enum import Enum
import concurrent.futures
from chorus_upload_journal.upload_tools.defaults import DEFAULT_MODALITIES
from chorus_upload_journal.upload_tools.local_ops import list_files, backup_manifest
import chorus_upload_journal.upload_tools.config_helper as config_helper
import chorus_upload_journal.upload_tools.storage_helper as storage_helper


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
# TODO: DONE handle large files in azure that do not compute MD5 (recommendation is to set MD5 on cloud file using locally computed MD5 - not an integrity assurance)
# TODO: explore other ways of computing MD5 on upload.
# TODO: explore integration of azcli or az copy.

# process:
#   1. local manifest is created
#   2. local manifest is used to generate update list
#   3. local manifest is used to send data to cloud, local manifest is sent to cloud as well
#   4. local manifest is verified with cloud files

# okay to have multiple updates between uploads
# we always compare to the last manifest.
# all remote stores are dated, so override with partial uploads.



# check out a journal to work on
# local name is what is in the config file
# cloud file will be renamed to .locked_{timestamp}
# which will be returned as a key for unlocking later.
def checkout_journal(config):
    #============= get the manifest file name and path obj  (download from cloud to local)
    manifest_path_str = config_helper.get_journal_config(config)["path"]  # full file name
    manifest_path = FileSystemHelper(manifest_path_str, client = storage_helper._make_client(config_helper.get_journal_config(config)))
    
    # if not cloud, then we are done.
    if not manifest_path.is_cloud:
        md5 = FileSystemHelper.get_metadata(manifest_path.root, with_metadata = False, with_md5 = True)['md5']
        return (manifest_path, None, manifest_path, md5)
    
    # since it's a cloud path, we want to lock as soon as possible.
    manifest_file = manifest_path.root
    manifest_fn = manifest_file.name
    # see if any lock files exists.
    manifest_dir = manifest_file.parent
    lock_file = manifest_dir.joinpath(manifest_fn + ".locked_" + str(int(time.time())))
    
    # so we try to lock by renaming. if it fails, then we handle if possible.
    downloadable = False
    already_locked = False
    try:
        manifest_file.rename(lock_file)
        downloadable = True
        # locks = list(manifest_dir.glob(manifest_fn + ".locked_*"))
        # print(locks)
    except:
        # if journal file is not found, it's either that we don't have a file, or it's locked.
        locked_files = list(manifest_dir.glob(manifest_fn + ".locked_*"))

        if (len(locked_files) == 0):
            # not locked, so journal does not exist.  create a lock.  this is to indicate to others that there is a lock
            lock_file.touch()
        else:
            already_locked = True
            
    if already_locked:      
        raise ValueError("Manifest is locked. Please try again later.")
    # except FileExistsError as e:
       
    local_path = FileSystemHelper(manifest_fn)
    lock_path = FileSystemHelper(lock_file, client = manifest_path.client)
    
    if downloadable:  # only if there something to download.
        # download the file
        remote_md5 = FileSystemHelper.get_metadata(lock_path.root, with_metadata = False, with_md5 = True)['md5']
        
        if (local_path.root.exists()):
            # compare md5
            local_md5 = FileSystemHelper.get_metadata(local_path.root, with_metadata = False, with_md5 = True)['md5']
            if (local_md5 != remote_md5):
                # remove localfile and then download.
                local_path.root.unlink()
                lock_path.copy_file_to(relpath = None, dest_path = local_path.root)
                local_md5 = FileSystemHelper.get_metadata(local_path.root, with_metadata = False, with_md5 = True)['md5']
            # if md5 matches, don't need to copy.
        else:
            # local file does not exist, need to copy 
            lock_path.copy_file_to(relpath = None, dest_path = local_path.root)
            local_md5 = FileSystemHelper.get_metadata(local_path.root, with_metadata = False, with_md5 = True)['md5']
    else:
        # nothing to download, using local file 
        remote_md5 = ""
        local_md5 = ""

    if remote_md5 != local_md5:
        raise ValueError(f"Manifest file is not downloaded correctly - MD5 remote {remote_md5}, local {local_md5}.  Please check the file.")

    # return manifest_file (final in cloud), lock_file (locked in cloud), and local_file (local)
    return (manifest_path, lock_path, local_path, remote_md5)


def checkin_journal(manifest_path, lock_path, local_path, remote_md5):
    # check journal file in.
    
    if not manifest_path.is_cloud:
        # not cloud, so nothing to do.
        return

    # first copy local to cloud as lockfile. (with overwrite)
    # since each lock is dated, unlikely to have conflicts.
    if lock_path.root.exists():
        local_path.copy_file_to(relpath = None, dest_path = lock_path)
    else:
        raise ValueError("Locked file lost, unable to unlock.")
    
    # then rename lock back to manifest.
    unlock_failed = False
    try:
        lock_path.root.rename(manifest_path.root)
    except:
        unlock_failed = True
        
    if unlock_failed:
        raise ValueError("unable to unlock - lockfile lost, or manifest already unlocked.")

# force unlocking.
def unlock_journal(args, config, manifest_fn):
    manifest_path_str = config_helper.get_journal_config(config)["path"]  # full file name
    manifest_path = FileSystemHelper(manifest_path_str, client = storage_helper._make_client(config_helper.get_journal_config(config)))
    
    manifest_file = manifest_path.root
    manifest_fn = manifest_file.name
    # see if any lock files exists.
    manifest_dir = manifest_file.parent
    
    locked_files = list(manifest_dir.glob(manifest_fn + ".locked_*"))
    if len(locked_files) == 0:
        print("INFO: Manifest ", str(manifest_file), " is not locked.")
        return
            
    if not manifest_file.exists():
        if (len(locked_files) > 1):
            # force unlock so use the most recent.
            # multiple locked files
            fns = [str(f.name) for f in locked_files].sort()
            last = fns[-1]
            manifest_dir.joinpath(last).rename(manifest_file)
            
        elif (len(locked_files) == 1):
            # no need to sort.
            locked_files[0].rename(manifest_file)
    
    locked_files = list(manifest_dir.glob(manifest_fn + ".locked_*"))
    # delete all lock files
    for lock_file in locked_files:
        lock_file.unlink()
    
    print("INFO: force unlocked ", str(manifest_file))
    
    

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

        # get the dest file info.  Don't rely on cloud md5
        start = time.time()
        dest_meta = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = fn, with_metadata = True, with_md5 = False)
        # case 2 missing in cloud.  this is the missing case;
        verify_time = time.time() - start
        if dest_meta is None:
            state = sync_state.MISSING_DEST
            # print("ERROR:  missing file at destination", fn)
    
    if (state == sync_state.UNKNOWN):
        
        if (size == dest_meta['size']):
            
            # MD5 could be missing, set by uploader, or computed differently by cloud provider.  Can't rely on it.
            # start = time.time()
            # dest_md5 = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = fn, with_metadata = False, with_md5 = True)['md5']
            # verify_time = time.time() - start
            # dest_md5 = dest_meta['md5']
            
            # if (md5 == dest_md5):
            # case 3: matched
            state = sync_state.MATCHED
            verified = True
            # fid_list.append(file_id)  # verified.  this may not be parallelizable.
            # else:
            #     # case 4: mismatched.
            #     state = sync_state.MISMATCHED
            # entries are not updated because of mismatch.
            # print("ERROR:  mismatched upload file ", fn, " upload failed? fileid ", file_id, ": cloud size ", dest_meta['size'], " manifest size ", size, "; cloud md5 ", dest_md5, " manifest md5 ", md5)            
        else:
            # case 4: mismatched.
            state = sync_state.MISMATCHED
            # entries are not updated because of mismatch.
            # print("ERROR:  mismatched upload file ", fn, " upload failed? fileid ", file_id, ": cloud size ", dest_meta['size'], " manifest size ", size)
            # verify_time = 0

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
    with concurrent.futures.ThreadPoolExecutor() as executor:
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
    manifest_path.copy_file_to(manifest_fn, dated_dest_path)
    # manifest_path.copy_files_to([(manifest_fn, "_".join([manifest_fn, upload_ver_str]))], src_path)

    return upload_ver_str

def _get_file_info(dated_dest_path: FileSystemHelper, file_info: tuple):
    
    fid, fn, size, md5 = file_info
    
    # this would actually compute the md5
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
    large_matched = set()
    mismatched = set()
    threads = max(1, os.cpu_count() - 2)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for file_info in files_to_verify:
            if verbose:
                print("INFO: verifying ", file_info[1], " for upload ", dtstr)
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
            # elif (dest_md5 is None) and (dest_meta['size'] == size):
            #     print("INFO: large file, matched by size only ", fn)
            #     large_matched.add(fn)
            else:
                print("ERROR:  mismatched file ", fid, " ", fn, " for upload ", dtstr, ": cloud size ", dest_meta['size'], " manifest size ", size, "; cloud md5 ", dest_md5, " manifest md5 ", md5)
                mismatched.add(fn)
                
    print("INFO:  verified ", len(matched), " files (", len(large_matched), "large files w/o md5)", len(mismatched), " mismatched, ", len(missing), " missing.")
            
    return matched, mismatched, missing