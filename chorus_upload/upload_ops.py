import os
import time
from chorus_upload.storage_helper import FileSystemHelper

from typing import Optional
from time import strftime, gmtime

from enum import Enum
import concurrent.futures
import threading
from chorus_upload.local_ops import list_files_with_info, convert_local_to_central_path
from itertools import islice
# from chorus_upload.local_ops import backup_journal
import chorus_upload.config_helper as config_helper
import chorus_upload.storage_helper as storage_helper
import chorus_upload.perf_counter as perf_counter

from chorus_upload.journaldb_ops import JournalDispatcher

import azure

import logging
log = logging.getLogger(__name__)

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

# TODO: DONE - generate journal from cloud storage
# TODO: DONE - update logic on how modify time is used?  not compare source to destination by this, but compare source to source
# TODO: DONE - changed sync_ready flag to upload_dtstr to track upload time.. 
# TODO: NO - integrate with AZCLI.  Using cloudpathlib instead
# TODO: DONE - verification of journal
# TODO: DONE - command processor
# TODO: DONE - update journal with cloud files.

# TODO: DONE save command history
# TODO: DONE upload only allow unuploaded files.
# TODO: DONE upload allows amending the LAST upload.
# TODO: DONE upload and verify each file at a time
# TODO: DONE support windows paths
# TODO: DONE handle large files in azure that do not compute MD5 (recommendation is to set MD5 on cloud file using locally computed MD5 - not an integrity assurance)
# TODO: explore other ways of computing MD5 on upload.
# TODO: explore integration of azcli or az copy.
# TODO: add a version string separate from upload_dtstr

# process:
#   1. local journal is created
#   2. local journal is used to generate update list
#   3. local journal is used to send data to cloud, local journal is sent to cloud as well
#   4. local journal is verified with cloud files

# okay to have multiple updates between uploads
# we always compare to the last journal.
# all remote stores are dated, so override with partial uploads.

LOCK_SUFFIX = ".locked"

# output journal_path, lock_path (must be cloud), and local_path (must be local)
def get_journal_paths(config, local_fn_override: str = None):
      
    #============= get the journal file name and path obj  (download from cloud to local)
    journal_config = config_helper.get_journal_config(config)
    
    journal_path_str = config_helper.get_journal_path(config) # full file name
    local_path_str = local_fn_override if local_fn_override is not None else config_helper.get_journal_local_path(config)
    
    journal_config = config_helper.get_journal_site_config(config, 'source')
    client, internal_host =storage_helper._make_client(journal_config)
    journal_path = FileSystemHelper(journal_path_str, client = client, internal_host = internal_host)
    local_path = FileSystemHelper(local_path_str)
    
    if (local_path.is_cloud):
        raise ValueError("local path is cloud storage.  Please provide a local path.")
    
    # if not cloud, then no lock.
    if not journal_path.is_cloud:
        return (journal_path, None, local_path)
    
    # since it's a cloud path, we want to lock as soon as possible.
    journal_file = journal_path.root
    journal_fn = journal_file.name
    # see if any lock files exists.
    journal_dir = journal_file.parent
    
    # make the lock file and lock.
    lock_file = journal_dir.joinpath(journal_fn + LOCK_SUFFIX)
    lock_path = FileSystemHelper(lock_file, client = journal_path.client, internal_host = journal_path.internal_host)
    
    if (not lock_path.is_cloud):
        raise ValueError("lock path is not cloud storage.  Please provide a cloud path for journal path.")
    
    return (journal_path, lock_path, local_path)


# check out a journal to work on
# config has the journal path, and can extract or generate local path
# local_fn_override is a kwargs that may be supplied if there is one provided.
# cloud file will be renamed to .locked
# which will be returned as a key for unlocking later.
# return (journal_path, lock_path, local_path)
def checkout_journal(journal_path, lock_path, local_path, transport_method: str="builtin"):
    
    # enforced restriction:  lock_path is cloud or None.  local_path is local.
    
    if (not journal_path.is_cloud):
        log.info(f"journal is a local file: {str(journal_path.root)}. no locking needed.")
        # local, ignore lock, and just copy if needed.
        # md5 = FileSystemHelper.get_metadata(journal_path.root, with_metadata = False, with_md5 = True)['md5']
        if journal_path.root.absolute() != local_path.root.absolute():
            journal_path.copy_file_to(relpath = None, dest_path = local_path.root)
        return (journal_path, None, local_path)
    
    # lock_path is already checked to be cloud, or none.
    lock_file = lock_path.root
    # check if already locked.
    if lock_file.exists():
        raise ValueError("journal is locked. Please try again later.")

    local_file = local_path.root
    if (local_file.exists()):
        local_file.unlink()
        
    journal_file = journal_path.root
    # so we try to lock by renaming. if it fails, then we handle if possible.
    # Can't use rename to ensure that only one process can lock the file.  linux and cloud - silent replacement.  Windows - exception.
    downloadable = journal_file.exists()
    if downloadable: 
        # can't use rename because internally it does not use the right path.
        journal_path.copy_file_to(relpath = None, dest_path = lock_file)
        # copy from journal path to local.
        journal_path.copy_file_to(relpath = None, dest_path = local_file)
        # complete the rename
        journal_path.root.unlink()
        # journal_file.rename(lock_file)
        # lock_path.copy_file_to(relpath = None, dest_path = local_file)
    else:
        # no lock and no journal file.  okay to create.
        log.debug(f"creating a new journal lock file at {str(lock_file)} and local file at {str(local_file)}")
        lock_file.touch()
        local_file.touch()
    
    # if downloadable:  # only if there something to download.
        # # download the file
        # remote_md5 = FileSystemHelper.get_metadata(lock_path.root, with_metadata = False, with_md5 = True)['md5']
        # if (local_path.root.exists()):
        #     # compare md5
        #     local_md5 = FileSystemHelper.get_metadata(local_path.root, with_metadata = False, with_md5 = True)['md5']
        #     if (local_md5 != remote_md5):
        #         # remove localfile and then download.
        #         local_path.root.unlink()
        #         lock_path.copy_file_to(relpath = None, dest_path = local_path.root)
        #         local_md5 = FileSystemHelper.get_metadata(local_path.root, with_metadata = False, with_md5 = True)['md5']
        #     # if md5 matches, don't need to copy.
        # else:
        #     # local file does not exist, need to copy 
        #     lock_path.copy_file_to(relpath = None, dest_path = local_path.root)
        #     local_md5 = FileSystemHelper.get_metadata(local_path.root, with_metadata = False, with_md5 = True)['md5']
       
    # else:
    #     # nothing to download, using local file 
    #     remote_md5 = None
    #     local_md5 = None

    # if remote_md5 != local_md5:
    #     raise ValueError(f"journal file is not downloaded correctly - MD5 remote {remote_md5}, local {local_md5}.  Please check the file.")

    log.info(f"checked out journal from cloud storage as local file {str(local_file)}")

    # return journal_file (final in cloud), lock_file (locked in cloud), and local_file (local)
    return (journal_path, lock_path, local_path)


def checkin_journal(journal_path, lock_path, local_path, transport_method: str="builtin"):
    # check journal file in.
    # lock_path is cloud or None.  local_path is local.
    
    if not journal_path.is_cloud:
        log.info(f"journal is a local file: {str(journal_path.root)}. no unlocking needed.")
        # not cloud, if local path is not the same as journal path, copy back
        if journal_path.root.absolute() != local_path.root.absolute():
            local_path.copy_file_to(relpath = None, dest_path = journal_path.root)
        return

    if (lock_path is None):
        log.info(f"no lock file specified.  prob local journal.")
        return
    
    lock_file = lock_path.root

    # first copy local to cloud as lockfile. (with overwrite)
    # since each lock is dated, unlikely to have conflicts.
    if not lock_file.exists():
        log.info(f"no lock file, unable to checkin.")
        return
    
    # then rename lock back to journal.
    try:
        local_path.copy_file_to(relpath = None, dest_path = journal_path)
        lock_file.unlink()
        # lock_file.rename(journal_path.root)
    except:
        raise ValueError("Checkin failed, unable to move lock file as journal file.  journal may exist and is already unlocked.")


# force unlocking.
def unlock_journal(journal_path, lock_path, transport_method: str="builtin"):
    # lock file is none or cloud
    
    log.debug(f"unlocking journal {journal_path} with lock {lock_path if lock_path is not None else 'None'}")
    
    if (lock_path is None):
        log.info(f"no lock file specified.  prob local journal.")
        return
    
    if (not journal_path.is_cloud):
        raise ValueError("journal is not cloud storage.  cannot unlock.")
    
    journal_file = journal_path.root    
    locked_file = lock_path.root
    
    has_journal = journal_file.exists()
    has_lock = locked_file.exists()
    if has_journal:
        if (has_lock):
            log.info(f"journal and lock files both present.  Keeping journal and removing lock")
            locked_file.unlink()
        else:
            log.info(f"journal {str(journal_file)} is not locked.")
    else:
        if (has_lock):
            log.debug(f"journal file {str(journal_file)} missing, but lock file {str(locked_file)} present.  restoring journal from lock.")
            lock_path.copy_file_to(relpath = None, dest_path = journal_file)
            locked_file.unlink()
            # locked_file.rename(journal_file)
            log.info(f"force unlocked {str(journal_file)}")
        else:
            log.info("no journal or lock file.  okay to create")
    

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
                       fn : str, info: dict,
                       dated_dest_path : FileSystemHelper,
                       all_deleted : dict, **kwargs):
    state = sync_state.UNKNOWN
    
    # ======= copy.  parallelizable
    start = time.time()
    srcfile = src_path.root.joinpath(fn)
    threads_per_file = kwargs.get("nthreads", 1)
        
    destfn = info['central_path']
    
    lock = kwargs.get("lock", None)
        
    if srcfile.exists():
        try:
            src_path.copy_file_to(relpath=(fn, destfn), dest_path=dated_dest_path, **{'nthreads': threads_per_file, 'lock': lock})
        except azure.core.exceptions.ServiceResponseError as e:
            try:
                log.warning(f"copy failed {fn}, retrying")
                src_path.copy_file_to(relpath=(fn, destfn), dest_path=dated_dest_path, **{'nthreads': threads_per_file, 'lock': lock})
            except azure.core.exceptions.ServiceResponseError as e:                
                log.error(f"copy failed {fn}, due to {str(e)}. Please rerun 'file upload' when connectivity improves.")
                state = sync_state.MISSING_DEST
    else:
        # log.error(f"file not found {srcfile}")
        state = sync_state.MISSING_SRC
    copy_time = time.time() - start
    verify_time = None
    
    # ======= read metadata from db.  May be parallelizable  
    verified = False
    
    file_id = info['file_id']
    size = info['size']
    md5 = info['md5']

    if (state == sync_state.UNKNOWN):

        # get the dest file info.  Don't rely on cloud md5
        start = time.time()
        dest_meta = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = destfn, with_metadata = True, with_md5 = True, local_md5 = md5, lock=lock)
        # case 2 missing in cloud.  this is the missing case;
        verify_time = time.time() - start
        if dest_meta is None:
            state = sync_state.MISSING_DEST
            #log.error(f"missing file at destination {destfn}")
    
    if (state == sync_state.UNKNOWN):
        
        if (size == dest_meta['size']) and (md5 == dest_meta['md5']):
            
            # MD5 could be missing, set by uploader, or computed differently by cloud provider.  Can't rely on it.
            # start = time.time()
            # dest_md5 = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = destfn, with_metadata = False, with_md5 = True)['md5']
            # verify_time = time.time() - start
            # dest_md5 = dest_meta['md5']
            
            # if (md5 == dest_md5):
            # case 3: matched
            state = sync_state.MATCHED
            verified = True
            # fid_list.append(file_id)  # verified.  this may not be parallelizable.
            # else:
            # case 4: mismatched.
            # state = sync_state.MISMATCHED
            # entries are not updated because of mismatch.
            # log.error(f"mismatched upload file {fn} upload failed? fileid {file_id}: cloud size {dest_meta['size']} journal size {size}; cloud md5 {dest_md5} journal md5 {md5}")            
        else:
            # case 4: mismatched.
            state = sync_state.MISMATCHED
            # entries are not updated because of mismatch.
            # log.error(f"mismatched upload file {fn} upload failed? fileid {file_id}: cloud size {dest_meta['size']} journal size {size}")
            # verify_time = 0

    del_list = []
    # if verified upload, then remove any old file
    # ======= read metadata from db.  May be parallelizable
    if verified:
        del_list = list(all_deleted.get(fn, set()))

    return (fn, info, state, del_list, copy_time, verify_time)


def _parallel_upload(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                     files_to_upload, files_to_mark_deleted, 
                     databasename, upload_dt_str, update_args, del_args, step,
                     missing_dest, missing_src, matched, mismatched, replaced,
                     perf, nuploads, threads_per_file, verbose = False):
    dated_dest_paths = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=nuploads) as executor:
        lock = threading.Lock()
        
        futures = []
        for fn, info in files_to_upload.items():
            dated_dest_path = FileSystemHelper(dest_path.root.joinpath(info['version']), client = dest_path.client, internal_host = dest_path.internal_host)
            future = executor.submit(_upload_and_verify, src_path, fn, info, dated_dest_path, files_to_mark_deleted, **{'nthreads': threads_per_file, "lock": lock})
            futures.append(future)
    
        for future in concurrent.futures.as_completed(futures):
            (fn2, info, state, del_list, copy_time, verify_time) = future.result()
            
            dated_dest_path = FileSystemHelper(dest_path.root.joinpath(info['version']), client = dest_path.client, internal_host = dest_path.internal_host)
            dated_dest_paths.add(dated_dest_path)
            perf.add_file(info['size'])
            
            if state == sync_state.MISSING_DEST:
                missing_dest.append(fn2)
                log.error(f"missing file at destination {fn2}")
            elif state == sync_state.MISSING_SRC:
                log.error(f"file not found {fn2}")
                missing_src.append(fn2)
            elif state == sync_state.MATCHED:
                # merge the updates for matched.
                matched.append(fn2)
                update_args.append((upload_dt_str, copy_time, verify_time, info['file_id']))
                if len(del_list) > 0:
                    del_args += [(upload_dt_str, fid) for fid in del_list]
                    replaced.append(fn2)
                if verbose:
                    log.info(f"copied {fn2} from {str(src_path.root)} to {str(dated_dest_path.root)}")
                else:
                    print(".", end="", flush=True)
            elif state == sync_state.MISMATCHED:
                mismatched.append(fn2)
                log.error(f"mismatched upload file {fn2} upload failed? fileid {info['file_id']}")            
            
            # update the journal - likely not parallelizable.
            if len(update_args) >= step:
                if verbose:
                    log.info(f"UPLOAD updating journal {len(update_args)}")
                # handle additions and updates
                JournalDispatcher.mark_as_uploaded_with_duration(databasename, update_args)
                update_args = []
            
                # backup intermediate file into the dated dest path.
                # journal_path.copy_file_to(relpath=journal_fn, dest_path=dated_dest_path)
                
                perf.report()
                
    return (update_args, del_args, perf, missing_dest, missing_src, matched, mismatched, replaced, dated_dest_paths)

# new version, pull list once.
def upload_files_parallel(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                 modalities: list[str], databasename: str,
                 max_num_files : int = None,
                 **kwargs):
    """
    Uploads files from the source path to the destination path and updates the journal.
    only allows uploaded new files and not reupload of previous upload.
    note that journal is updated only when verified. 

    Args:
        src_path (FileSystemHelper): The source path from where the files will be uploaded.
        dest_path (FileSystemHelper): The destination path where the files will be copied to.
        databasename (str, optional): The name of the journal database file. Defaults to "journal.db".
        upload_datetime_str (str, optional): The upload datetime string. Defaults to None.

    Returns:
        str: The upload version string.

    Raises:
        FileNotFoundError: If the journal database file does not exist.
        AssertionError: If some uploaded files are not in the journal or if there are mismatched files.

    """
    log.info(f'UPLOAD_NEW uploading {max_num_files} files')
    
    verbose = kwargs.get("verbose", False)
    modality_configs = kwargs.get("modality_configs", {})

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        log.error(f"ERROR: No journal exists for filename {databasename}")
        return None, 0
    
    # ======== upload_dt_str should be constructed from the version string in the journal.
    upload_dt_str = strftime("%Y%m%d%H%M%S", gmtime())   # use string for upload ver_str for readability


    #---------- upload files.    
    # first get the list of files for the requested version
    # files_to_upload is a filename to info mapping
    # previously, it was a version to filename mapping.
    _, files_to_upload, files_to_mark_deleted = list_files_with_info(databasename, version=None, modalities=modalities, 
                                                                    verbose = False, modality_configs = modality_configs)
    if (files_to_upload is None) or (len(files_to_upload) == 0):
        log.info("no files to upload.  Done")
        return None, max_num_files

    if max_num_files is not None:
        upload_count = min(max_num_files, len(files_to_upload))
        remaining = max_num_files - upload_count
    else:
        upload_count = len(files_to_upload)
        remaining = None
                
    log.info(f"UPLOAD: files found {len(files_to_upload)} to upload {upload_count}")    
    if upload_count > 0:
        # keep the first remaining items in the dictionary
        files_to_upload = dict(islice(files_to_upload.items(), upload_count))            

    # log.info(f"UPLOAD: limited files to upload {len(files_to_upload)}")

    perf = perf_counter.PerformanceCounter(total_file_count = len(files_to_upload))
    
    # copy file, verify and update journal
    update_args = []
    del_args = []
    missing_dest = []
    missing_src = []
    matched = []
    mismatched = []
    replaced = []
    deleted = []
    dated_dest_paths = {}

    # NEW    
    step = kwargs.get("page_size", 1000)
    
    # finally, upload the journal.
    journal_path = FileSystemHelper(os.path.dirname(os.path.abspath(databasename)))  # local fs, no client.
    # Question - do we put journal in the dated target subdirectory?  YES for now
    # Question - do we leave the journal update time as NULL?  
    # Question - do we copy the journal into the source directory? YES for now (in case of overwrite)
    journal_fn = os.path.basename(databasename)

    # for fn, info in files_to_upload.items():  # only the active files.
    #     # create a dated root directory to receive the files
    #     dated_dest_path = FileSystemHelper(dest_path.root.joinpath(info['version']), client = dest_path.client, internal_host = dest_path.internal_host)
    #     (fn2, info, state, del_list, copy_time, verify_time) = _upload_and_verify( src_path, fn, info, dated_dest_path, files_to_mark_deleted)

    # split set to small and large files by AZURE block size.
    n_cores = kwargs.get("n_cores", 32)
    nthreads = min(n_cores, min(32, (os.cpu_count() or 1) + 4))
    
    for threads_per_file in range(1, (nthreads // 2 + 1)):
        files = { fn: info for fn, info in files_to_upload.items() 
                 if (info['size'] > storage_helper.AZURE_MAX_BLOCK_SIZE * (threads_per_file-1)) and
                    (info['size'] <= storage_helper.AZURE_MAX_BLOCK_SIZE * threads_per_file) }

        nuploads = nthreads // threads_per_file        
        if len(files) > 0:
            log.info(f"UPLOAD {len(files)} files sizes {storage_helper.AZURE_MAX_BLOCK_SIZE * (threads_per_file-1)} to {storage_helper.AZURE_MAX_BLOCK_SIZE * threads_per_file}, {nuploads} uploads")
        
            (update_args, del_args, perf, missing_dest, missing_src, matched, mismatched, replaced, dated_paths) = \
                _parallel_upload(src_path, dest_path,
                                files, files_to_mark_deleted,
                                databasename, upload_dt_str, update_args, del_args, step,
                                missing_dest, missing_src, matched, mismatched, replaced,
                                perf, nuploads, threads_per_file, verbose)
            for dp in dated_paths:
                dated_dest_paths[str(dp.root)] = dp
    
    files = { fn: info for fn, info in files_to_upload.items() 
                if (info['size'] > storage_helper.AZURE_MAX_BLOCK_SIZE * (nthreads // 2)) }
    log.info(f"UPLOAD {len(files)} files > {storage_helper.AZURE_MAX_BLOCK_SIZE * (nthreads // 2)}, 2 uploads")
    if len(files) > 0:
        (update_args, del_args, perf, missing_dest, missing_src, matched, mismatched, replaced, dated_paths) = \
            _parallel_upload(src_path, dest_path,
                            files, files_to_mark_deleted,
                            databasename, upload_dt_str, update_args, del_args, step,
                            missing_dest, missing_src, matched, mismatched, replaced,
                            perf, 2, nthreads, verbose)
        for dp in dated_paths:
            dated_dest_paths[str(dp.root)] = dp
    
    # report the remaiing.
    if len(update_args) > 0:
        log.info(f"UPLOAD updating journal update last batch {len(update_args)}")
        # log.debug(update_args)
        # handle additions and updates
        JournalDispatcher.mark_as_uploaded_with_duration(databasename, update_args)
        update_args = []
        
        # don't need to delete the outdated files - mark all deleted as uploaded. 
        perf.report()

    # # other cases are handled below.

    missing_dest = set(missing_dest)
    missing_src = set(missing_src)
    matched = set(matched)
    mismatched = set(mismatched)
    replaced = set(replaced)

    # handle all deleted (only undeleted files that are not "uploaded" are the ones that are were added and deleted between uploads.).
    del_args = []
    for fn, fids in files_to_mark_deleted:
        deleted.append(fn)
        for fid in fids:
            del_args.append((fid,))  # deleted
    deleted = set(deleted)
    
    # delete every thing in mark_deleted.
    if len(del_args) > 0:
        log.info(f"Marking as deleted: {len(del_args)}")
        JournalDispatcher.mark_as_uploaded(databasename, 
                                      version = upload_dt_str,
                                      upload_args = del_args)
        del_args = []
    
        perf.report()
    
    # create a versioned backup - AFTER updating the journal.
    # do not do backup the table - this will create really big files.
    # backup_journal(databasename, suffix=upload_dt_str)
    # just copy the journal file to the dated dest path as a backup, and locally sa well
    for dated_dest_path in dated_dest_paths.values():
        if verbose:
            log.info(f"UPLOAD: backing up journal to {str(dated_dest_path.root)}")
        journal_path.copy_file_to(relpath=journal_fn, dest_path=dated_dest_path)
        
    # dest_fn = src_path.root.joinpath("_".join([journal_fn, upload_dt_str]))
    # journal_path.copy_file_to(journal_fn, dest_fn)
    perf.report()
    
    del perf
    return upload_dt_str, remaining  # if max_num_files is None, remaining is None, meaning no limit.

def _get_file_info(dated_dest_path: FileSystemHelper, file_info: tuple, **kwargs):
    
    fid, fn, size, md5, modality = file_info
    compiled_patterns = kwargs.get("compiled_patterns", {})
    modality_configs = kwargs.get("modality_configs", {})
    central_fn = convert_local_to_central_path(fn, in_compiled_pattern = compiled_patterns.get(modality, None), 
                                            modality = modality,
                                            omop_per_patient = modality_configs.get(modality, {}).get("omop_per_patient", False))
    
    # this would actually compute the md5
    dest_meta = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = central_fn, with_metadata = True, with_md5 = True,  **kwargs)
    dest_md5 = dest_meta['md5'] if (dest_meta is not None) else None

    return (dest_meta, dest_md5, fid, fn, size, md5)

def _get_file_info2(dest_path: FileSystemHelper, file_info: tuple,  **kwargs):
    
    fid, fn, size, md5, version, modality = file_info
    
    compiled_patterns = kwargs.get("compiled_patterns", {})
    modality_configs = kwargs.get("modality_configs", {})
    central_fn = convert_local_to_central_path(fn, in_compiled_pattern = compiled_patterns.get(modality, None), 
                                            modality = modality,
                                            omop_per_patient = modality_configs.get(modality, {}).get("omop_per_patient", False))
    
    dest_root = dest_path.root.joinpath(version)
    
    # this would actually compute the md5
    dest_meta = FileSystemHelper.get_metadata(root = dest_root, path = central_fn, with_metadata = True, with_md5 = True, local_md5=md5,  **kwargs)
    dest_md5 = dest_meta['md5'] if (dest_meta is not None) else None

    return (dest_meta, dest_md5, dest_root, fid, fn, size, md5)


# verify a set of files that have been uploaded.  version being None means using the last upload
def verify_files(dest_path: FileSystemHelper, databasename:str="journal.db", 
                 version: Optional[str] = None,
                 modalities: Optional[list] = None,
                 **kwargs):
    """
    Verify the integrity of uploaded files by comparing their metadata and MD5 checksums.

    Args:
        dest_path (FileSystemHelper): The destination path where the files are stored.
        databasename (str, optional): The name of the database file. Defaults to "journal.db".
        version (str, optional): The version datetime string. If not provided, the latest version will be used.

    Returns:
        set: A set of filenames that have mismatched metadata or MD5 checksums.
    """
    verbose = kwargs.get("verbose", False)
    
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        log.error(f"No journal exists for filename {databasename}")
        return
    
    dtstr = version if version is not None else JournalDispatcher.get_latest_version(databasename)
    
    files_to_verify = JournalDispatcher.get_files_with_meta(databasename, 
                                                    version = dtstr, 
                                                    modalities = modalities, 
                                                    **{'active': True})   

    if len(files_to_verify) == 0:
        log.info("no files to verify.")
        return
        
    #---------- verify files.    
    # create a dated root directory to receive the files
    dated_dest_path = FileSystemHelper(dest_path.root.joinpath(dtstr), client = dest_path.client, internal_host = dest_path.internal_host)
    
    # now compare the metadata and md5
    missing = []
    matched = []
    large_matched = []
    mismatched = []

    # procssPoolExecutor seems to work better with remote files.
    n_cores = kwargs.get("n_cores", 32)
    nthreads = min(n_cores, min(32, (os.cpu_count() or 1) + 4))
    log.info(f"Using {nthreads} threads")
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        lock = threading.Lock()
        futures = []
        for (fid, filepath, modtime, size, md5, modality, invalidtime, vresion, uploadtime) in files_to_verify:
            file_info = (fid, filepath, size, md5, modality)
            future = executor.submit(_get_file_info, dated_dest_path, file_info, **{**kwargs, "lock": lock})
            futures.append(future)
            
        for future in concurrent.futures.as_completed(futures):
            (dest_meta, dest_md5, fid, fn, size, md5) = future.result()

            if (dest_meta is None) or (dest_meta['size'] is None):
                missing.append(fn)
                log.error(f"missing file {fn}")
                continue

            if (size == dest_meta['size']) and (md5 == dest_md5):
                if verbose:
                    log.info(f"Verified upload {dtstr} {fn} {fid}")
                else:
                    print(".", end="", flush=True)
                matched.append(fn)
            # elif (dest_md5 is None) and (dest_meta['size'] == size):
            #     log.info(f"large file, matched by size only {fn}")
            #     large_matched.append(fn)
            else:
                log.error(f"mismatched file {fid} {fn} for upload {dtstr}: cloud size {dest_meta['size']} journal size {size}; cloud md5 {dest_md5} journal md5 {md5}")
                mismatched.append(fn)
            
    missing = set(missing)
    matched = set(matched)
    large_matched = set(large_matched)
    mismatched = set(mismatched)
    
    log.info(f"verified {len(matched)} files ({len(large_matched)} large files w/o md5) {len(mismatched)} mismatched, {len(missing)} missing.")
            
    return matched, mismatched, missing


# verify a set of files that have been uploaded.  version being None means using the last upload
def mark_as_uploaded(dest_path: FileSystemHelper, databasename:str="journal.db", 
                 version: Optional[str] = None,
                 files: list[str] = None, 
                 **kwargs):
    
    # version is what will be set.
    verbose = kwargs.get("verbose", False)
    
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        log.error(f"No journal exists for filename {databasename}")
        return

    # get the list of files, md5, size for unuploaded, active files
    db_files = JournalDispatcher.get_files_with_meta(databasename, 
                                                version = version,
                                                modalities = None,
                                                **{'active': True, 'uploaded': False, 'files': files})

    # make a hashtable
    db_files = {f[1]: f for f in db_files}
    
    matched = []
    
    log.info(f"marking {len(files)} files as uploaded.")
    for f in files:
   
        if f not in db_files:
            log.warning(f"file {f} not found in journal or was previously uploaded.")
            continue
        
        (_fid, _fn, _mtime, _size, _md5, _mod, _invalidtime, _ver, _uploadtime) = db_files[f]
        _info = (_fid, _fn, _size, _md5, _ver, _mod)
        
        (dest_meta, dest_md5, dest_root, fid, fn, size, md5) = _get_file_info2(dest_path, _info, **kwargs)
        
        if dest_meta is None:
            log.error(f"missing file {fn} in destination {dest_root}")
            continue

        if (size == dest_meta['size']) and (md5 == dest_md5):
            if verbose:
                log.info(f"marking as uploaded {version} {fn} {fid}")
            else:
                print(".", end="", flush=True)
            matched.append((fid,))
        else:
            log.error(f"mismatched file {fid} {fn} for upload {version}: cloud size {dest_meta['size']} journal size {size}; cloud md5 {dest_md5} journal md5 {md5}")

    if len(matched) > 0:
        JournalDispatcher.mark_as_uploaded(database_name = databasename, 
                        version = version,
                        upload_args = matched)
    
    log.info(f"marked {len(matched)} files as uploaded.")