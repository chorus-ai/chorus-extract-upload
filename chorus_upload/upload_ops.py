import os
import time
from chorus_upload.storage_helper import FileSystemHelper

from typing import Optional
from time import strftime, gmtime

from enum import Enum
import concurrent.futures
import threading
import sqlite3
from contextlib import contextmanager
from itertools import islice

from chorus_upload.local_ops import convert_local_to_central_path, iter_upload_pages, iter_pending_deleted_ids
# from chorus_upload.local_ops import backup_journal
import chorus_upload.config_helper as config_helper
import chorus_upload.storage_helper as storage_helper
import chorus_upload.perf_counter as perf_counter
from chorus_upload.remote_file_ops import DEFAULT_REMOTE_OP_PAGE_SIZE
from chorus_upload.journaldb_ops import JournalDispatcher

from azure.core.exceptions import ServiceResponseError

import asyncio
import aiofiles.os
import aiohttp
from cloudpathlib import AzureBlobClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.core.pipeline.transport import AioHttpTransport

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
        log.debug(f"journal is a local file: {str(journal_path.root)}. no locking needed.")
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
        log.debug(f"journal is a local file: {str(journal_path.root)}. no unlocking needed.")
        # not cloud, if local path is not the same as journal path, copy back
        if journal_path.root.absolute() != local_path.root.absolute():
            local_path.copy_file_to(relpath = None, dest_path = journal_path.root)
        return

    if (lock_path is None):
        log.debug(f"no lock file specified.  prob local journal.")
        return
    
    lock_file = lock_path.root

    # first copy local to cloud as lockfile. (with overwrite)
    # since each lock is dated, unlikely to have conflicts.
    if not lock_file.exists():
        log.error(f"no lock file, unable to checkin.")
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
    
    log.info(f"unlocking journal {journal_path} with lock {lock_path if lock_path is not None else 'None'}")
    
    if (lock_path is None):
        log.debug(f"no lock file specified.  prob local journal.")
        return
    
    if (not journal_path.is_cloud):
        raise ValueError("journal is not cloud storage.  cannot unlock.")
    
    journal_file = journal_path.root    
    locked_file = lock_path.root
    
    has_journal = journal_file.exists()
    has_lock = locked_file.exists()
    if has_journal:
        if (has_lock):
            log.warning(f"journal and lock files both present.  Keeping journal and removing lock")
            locked_file.unlink()
        else:
            log.debug(f"journal {str(journal_file)} is not locked.")
    else:
        if (has_lock):
            lock_path.copy_file_to(relpath = None, dest_path = journal_file)
            locked_file.unlink()
            # locked_file.rename(journal_file)
            log.warning(f"journal file {str(journal_file)} missing, but lock file {str(locked_file)} present.  force unlocking.")
        else:
            log.debug("no journal or lock file.  okay to create")
    

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
                    #    all_deleted : dict, 
                       **kwargs):
    state = sync_state.UNKNOWN
    
    # ======= copy.  parallelizable
    start = time.time()
    srcfile = src_path.root.joinpath(fn)
    threads_per_file = kwargs.get("nthreads", 1)
        
    destfn = info['central_path']
    
    lock = kwargs.get("lock", None)
        
    if srcfile.exists():
        # any failure is confined to this file (reported as MISSING_DEST) so it cannot abort the group.
        try:
            try:
                src_path.copy_file_to(relpath=(fn, destfn), dest_path=dated_dest_path, **{'nthreads': threads_per_file, 'lock': lock})
            except ServiceResponseError as e:
                log.warning(f"copy failed {fn}, retrying")
                src_path.copy_file_to(relpath=(fn, destfn), dest_path=dated_dest_path, **{'nthreads': threads_per_file, 'lock': lock})
        except Exception as e:
            log.error(f"copy failed {fn}, due to {type(e).__name__}: {str(e)}. Please rerun 'file upload' when connectivity improves.")
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
        try:
            dest_meta = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = destfn, with_metadata = True, with_md5 = True, local_md5 = md5, lock=lock)
        except Exception as e:
            log.error(f"verify failed {fn}, due to {type(e).__name__}: {str(e)}")
            dest_meta = None
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

    # note needed
    # del_list = []
    # # if verified upload, then remove any old file
    # # ======= read metadata from db.  May be parallelizable
    # if verified:
    #     del_list = list(all_deleted.get(fn, set()))

    return (fn, info, state, dated_dest_path,
            # del_list, 
            copy_time, verify_time)


async def _produce_pages(pages, queue: asyncio.Queue, n_workers: int, db_lock: asyncio.Lock = None):
    """Feed items from an iterator of pages into a bounded queue, then one None per worker.

    Pages are pulled in a worker thread (they may run SQLite queries), under db_lock if given.
    The next page is pulled right after the previous one is queued, so the queue keeps the
    workers fed during the read; memory is bounded by queue size + one page.
    """
    page_iter = iter(pages)
    while True:
        if db_lock is not None:
            async with db_lock:
                page = await asyncio.to_thread(next, page_iter, None)
        else:
            page = await asyncio.to_thread(next, page_iter, None)
        if page is None:
            break
        for item in page:
            await queue.put(item)
    for _ in range(n_workers):
        await queue.put(None)   # one stop sentinel per worker


def _bounded_submit(executor, pages, submit, window: int, on_done):
    """Submit submit(item) for every item in an iterator of pages, keeping at most `window`
    futures in flight; on_done(future) is called on the calling thread as each completes."""
    in_flight = set()
    for page in pages:
        for item in page:
            in_flight.add(submit(item))
            if len(in_flight) >= window:
                done, in_flight = concurrent.futures.wait(
                    in_flight, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    on_done(future)
    for future in concurrent.futures.as_completed(in_flight):
        on_done(future)


def _record_upload_result(result, src_path : FileSystemHelper, upload_dt_str, update_args,
                          perf, group_perf, dated_dest_paths, verbose):
    """Bookkeeping for one finished upload; appends MATCHED files to update_args."""
    (fn2, info, state, dated_dest_path, copy_time, verify_time) = result

    dated_dest_paths.add(dated_dest_path)
    perf.add_file(info['size'])
    if group_perf is not None:
        group_perf.add_file(info['size'])

    if state == sync_state.MISSING_DEST:
        log.error(f"missing file at destination {fn2}")
    elif state == sync_state.MISSING_SRC:
        log.error(f"file not found {fn2}")
    elif state == sync_state.MATCHED:
        update_args.append((upload_dt_str, copy_time, verify_time, info['file_id']))
        if verbose:
            log.info(f"copied {fn2} from {str(src_path.root)} to {str(dated_dest_path.root)}")
        else:
            print(".", end="", flush=True)
    elif state == sync_state.MISMATCHED:
        log.error(f"mismatched upload file {fn2} upload failed? fileid {info['file_id']}")


def _thread_upload(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                   pages,
                   databasename, upload_dt_str, update_args,
                   step,
                   perf, nuploads, threads_per_file, verbose = False, *, group_perf=None):
    """Upload files from an iterator of pages (lists of (path, info)) with a thread pool.

    At most 2 * nuploads uploads are submitted at a time, so memory is bounded by the
    page size while every worker always has a next file ready.  Pages are pulled (DB
    reads) on the calling thread, which also does the journal flushes, so SQLite access
    is serialized.
    """
    dated_dest_paths = set()

    # Each worker thread gets its own cloned Azure client via thread-local storage.
    # The main thread's client must NOT be shared across workers.
    thread_local = threading.local()
    orig_client = dest_path.client
    dest_root = dest_path.root
    int_host = dest_path.internal_host
    lock = threading.Lock()

    def upload_task(fn, info):
        if not hasattr(thread_local, 'client'):
            thread_local.client = storage_helper._clone_client(orig_client, pool_size=threads_per_file)
        dated_dest_path = FileSystemHelper(dest_root.joinpath(info['version']), client=thread_local.client, internal_host=int_host)
        return _upload_and_verify(src_path, fn, info, dated_dest_path,
                                  nthreads=threads_per_file, lock=lock)

    def on_done(future):
        _record_upload_result(future.result(), src_path, upload_dt_str, update_args,
                              perf, group_perf, dated_dest_paths, verbose)
        if len(update_args) >= step:
            if verbose:
                log.debug(f"UPLOAD updating journal {len(update_args)}")
            JournalDispatcher.mark_as_uploaded_with_duration(databasename, update_args)
            update_args.clear()
            perf.report()

    with concurrent.futures.ThreadPoolExecutor(max_workers=nuploads) as executor:
        _bounded_submit(executor, pages, lambda item: executor.submit(upload_task, *item),
                        2 * nuploads, on_done)

    return (update_args,
            perf,
            dated_dest_paths)


async def _async_upload_and_verify(src_path : FileSystemHelper,
                       fn : str, info: dict,
                       dated_dest_path : FileSystemHelper,
                       **kwargs):
    state = sync_state.UNKNOWN
    destfn = info['central_path']
    size = info['size']
    md5 = info['md5']

    # ======= copy
    start = time.time()
    srcfile = src_path.root.joinpath(fn)
    nthreads = kwargs.get('nthreads', 0)
    if await aiofiles.os.path.exists(srcfile):
        # any failure is confined to this file (reported as MISSING_DEST) so it cannot abort the group.
        try:
            try:
                await src_path.async_copy_file_to(relpath=(fn, destfn), dest_path=dated_dest_path,
                                                   nthreads=nthreads)
            except ServiceResponseError as e:
                log.warning(f"async copy failed {fn}, due to {str(e)}.  retrying")
                await src_path.async_copy_file_to(relpath=(fn, destfn), dest_path=dated_dest_path,
                                                   nthreads=nthreads)
        except Exception as e:
            log.error(f"async copy failed {fn}, due to {type(e).__name__}: {str(e)}. Please rerun 'file upload' when connectivity improves.")
            state = sync_state.MISSING_DEST
    else:
        state = sync_state.MISSING_SRC
    copy_time = time.time() - start
    verify_time = None

    # ======= verify
    if state == sync_state.UNKNOWN:
        start = time.time()
        try:
            dest_meta = await dated_dest_path.async_get_metadata(
                path=destfn, with_metadata=True, with_md5=True, local_md5=md5)
        except Exception as e:
            log.error(f"async verify failed {fn}, due to {type(e).__name__}: {str(e)}")
            dest_meta = None
        verify_time = time.time() - start
        if dest_meta is None:
            state = sync_state.MISSING_DEST

    if state == sync_state.UNKNOWN:
        if (size == dest_meta['size']) and (md5 == dest_meta['md5']):
            state = sync_state.MATCHED
        else:
            state = sync_state.MISMATCHED

    return (fn, info, state, dated_dest_path, copy_time, verify_time)


def _async_upload(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                  pages,
                  databasename, upload_dt_str, update_args,
                  step,
                  perf, concurrency, verbose = False, *, connections_per_file: int = 1,
                  async_pool_max: int = 256, group_perf=None, queue_size: int = None):
    """Upload files from an iterator of pages (lists of (path, info)) with `concurrency`
    async workers.

    A producer pulls pages (SQLite reads, in a worker thread) into a bounded queue of
    queue_size items (>= 2 * concurrency); a fixed pool of workers drains it.  The next
    page is fetched as soon as the previous page is queued, so the queue keeps every
    worker fed during the DB read, and memory is bounded by ~2 pages instead of one
    task per file.  Page reads and journal flushes share one lock: SQLite has a single
    writer and a long page scan would otherwise make a concurrent flush fail with
    'database is locked'.
    """

    if src_path.is_cloud:
        raise ValueError("Async upload is only supported for local source path.")
    if not isinstance(dest_path.client, AzureBlobClient):
        raise ValueError("Async upload is only supported for Azure Blob Storage destination.")

    dated_dest_paths = set()
    queue_size = max(queue_size or 0, 2 * concurrency)

    async def process_uploads():
        svc = dest_path.client.service_client
        pool_size = min(concurrency * connections_per_file, async_pool_max)
        connector = aiohttp.TCPConnector(limit=pool_size)
        transport = AioHttpTransport(session=aiohttp.ClientSession(connector=connector))
        async with AsyncBlobServiceClient(account_url=svc.url,
                                          credential=svc.credential,
                                          api_version=svc.api_version,
                                          transport=transport) as async_client:
            queue = asyncio.Queue(maxsize=queue_size)
            db_lock = asyncio.Lock()

            async def worker():
                while (item := await queue.get()) is not None:
                    fn, info = item
                    dated_dest_path = FileSystemHelper(
                        dest_path.root.joinpath(info['version']),
                        client=dest_path.client,
                        internal_host=dest_path.internal_host,
                        async_azclient=async_client,
                    )
                    result = await _async_upload_and_verify(src_path, fn, info, dated_dest_path,
                                                            nthreads=connections_per_file)
                    _record_upload_result(result, src_path, upload_dt_str, update_args,
                                          perf, group_perf, dated_dest_paths, verbose)

                    if len(update_args) >= step:
                        # swap out the batch before awaiting so other workers keep appending.
                        batch = update_args[:]
                        update_args.clear()
                        if verbose:
                            log.info(f"UPLOAD updating journal {len(batch)}")
                        async with db_lock:
                            await asyncio.to_thread(JournalDispatcher.mark_as_uploaded_with_duration,
                                                    databasename, batch)
                        perf.report()

            await asyncio.gather(_produce_pages(pages, queue, concurrency, db_lock),
                                 *(worker() for _ in range(concurrency)))

    asyncio.run(process_uploads())

    return (
        update_args,
        perf,
        dated_dest_paths,
    )


# Upload size groups and concurrency settings.
#
# AZURE_MAX_BLOCK_SIZE = 4 MB (set in storage_helper), so any file > 4 MB
# is uploaded as multiple 4 MB blocks.  conn/file = max_concurrency passed
# to upload_blob(), controlling parallel block uploads per file.
#
# Async groups share one aiohttp pool; pool_size = min(concurrency × conn/file, 256).
# Thread groups: each thread gets its own cloned BlobServiceClient with
# pool_size = conn/file (hardcoded, not derived from nthreads, so large-file
# block parallelism is consistent regardless of CPU count).
#
# group    size        type    concurrency  conn/file  aiohttp pool / thread pool
# small   <  1 MB     async       256    ×     1     =  256  (single PUT, no blocks)
# medium  <  8 MB     async       128    ×     2     =  256  (1–2 blocks of 4 MB)
# large   < 32 MB     async        64    ×     4     =  256  (2–8 blocks of 4 MB)
# xlarge  < 256 MB    thread         8   ×     4     =    4/thread (8–64 blocks)
# huge    >= 256 MB   thread         4   ×     8     =    8/thread (64+ blocks)
#
# Thread groups use LPT (largest-first) scheduling: each thread has an isolated
# connection pool so classical scheduling theory applies — large files start early
# and overlap; small files fill the straggler tail.
# Async groups use natural (unsorted) ordering: all coroutines share one aiohttp
# pool, so LPT spikes connection demand at the start and hurts throughput.
# Mixed-size ordering naturally balances pool demand against semaphore churn.
# Files are streamed from the journal in pages, so these orderings apply per page.
#
# (concur_type, min_size, max_size, concurrency, connections_per_file)
UPLOAD_SIZE_GROUPS = {
    'small':   ('async',  0,            2**20,        256, 1),
    'medium':  ('async',  2**20,         2**23,        128, 2),
    'large':   ('async',  2**23,         2**25,         64, 4),
    'xlarge':  ('thread', 2**25,         2**28,          8, 4),
    'huge':    ('thread', 2**28,  float('inf'),          4, 8),
}

UPLOAD_ASYNC_POOL_MAX = 256  # cap aiohttp pool size across all async groups


def _interleave_by_size(items):
    """Alternate largest and smallest (two-pointer walk after one sort).

    Keeps a consistent mix of fast-completing (small) and slow-completing (large)
    files in flight, which prevents both the pool-contention spike of LPT and the
    straggler tail of SJF.
    """
    sorted_items = sorted(items, key=lambda kv: kv[1]['size'], reverse=True)
    interleaved = []
    lo, hi = 0, len(sorted_items) - 1
    while lo <= hi:
        interleaved.append(sorted_items[lo])
        if lo != hi:
            interleaved.append(sorted_items[hi])
        lo += 1
        hi -= 1
    return interleaved


def _ordered_pages(pages, max_files, order):
    """Cap pages to max_files (in journal FILE_ID order), then order each page.

    order: 'lpt' (largest first), 'interleave', or None (as listed).
    """
    n = 0
    for page in pages:
        if max_files is not None:
            if n >= max_files:
                return
            page = page[:max_files - n]
        n += len(page)
        if order == 'lpt':
            page = sorted(page, key=lambda kv: kv[1]['size'], reverse=True)
        elif order == 'interleave':
            page = _interleave_by_size(page)
        yield page


def _parallel_upload(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                     page_source, group_stats,
                     databasename, upload_dt_str, update_args,
                     step,
                     perf, nthreads:int, verbose = False, max_files: int = None):
    """Dispatch file uploads to _async_upload or _thread_upload based on destination cloud type
    and file size group.

    page_source(min_size, max_size, page_rows) returns an iterator of pages (lists of
    (path, info)) of pending files in [min_size, max_size), in journal FILE_ID order.
    group_stats maps group name -> (pending count, pending bytes).
    max_files caps the number of files across all groups (None = no cap).

    Each group streams pages of step × concurrency × conn/file rows: a multiple of the
    journal flush step and of the in-flight upload count, so a page always holds whole
    flush batches and enough files to keep every upload slot busy while the next page
    is read.

    Design rationale
    ----------------
    Azure Blob supports true async I/O via the azure-storage-blob aio SDK, so small-to-large
    files benefit from high-concurrency async upload.  Files >= 32 MB are routed
    to a thread pool instead because per-file block-level parallelism is more important than
    task-level concurrency at that size.  For non-Azure destinations (S3, local) the async
    path is unavailable; all groups fall back to _thread_upload automatically.
    """
    use_async = isinstance(dest_path.client, AzureBlobClient)
    dated_paths = set()
    remaining = max_files

    for group_name, (concur_type, min_size, max_size, concurrency, connections_per_file) in UPLOAD_SIZE_GROUPS.items():
        group_pending, group_bytes = group_stats.get(group_name, (0, 0))
        group_count = group_pending if remaining is None else min(group_pending, remaining)
        if group_count == 0:
            continue

        if concur_type == 'thread':
            # LPT is safe for thread groups because each thread has an isolated connection pool.
            order = 'lpt'
        elif group_name in ('medium', 'large'):
            # intra-group size spread (8×) is large enough to matter; small (<1 MB) is not.
            order = 'interleave'
        else:
            order = None

        group_size_str = (f"{group_bytes / (1024 * 1024):.1f} MB" if group_count == group_pending
                          else f"capped from {group_pending}")
        group_perf = perf_counter.PerformanceCounter(total_file_count=group_count)

        if concur_type == 'async' and use_async:
            page_rows = step * concurrency * connections_per_file
            pages = _ordered_pages(page_source(min_size, max_size, page_rows), group_count, order)
            log.info(f"UPLOAD {group_count} files ({group_size_str}) in {group_name} group, async, "
                     f"{concurrency} concurrent × {connections_per_file} conn/file "
                     f"(pool_size={min(concurrency * connections_per_file, UPLOAD_ASYNC_POOL_MAX)}, page={page_rows})")
            (update_args,
                perf,
                _dated_paths) = \
                _async_upload(src_path, dest_path,
                                pages,
                                databasename, upload_dt_str, update_args,
                                step,
                                perf, concurrency, verbose,
                                connections_per_file=connections_per_file,
                                async_pool_max=UPLOAD_ASYNC_POOL_MAX,
                                group_perf=group_perf,
                                queue_size=page_rows)
            dated_paths.update(_dated_paths)
        elif concur_type == 'thread' or (concur_type == 'async' and not use_async):
            nuploads = min(concurrency, nthreads)
            page_rows = step * nuploads * connections_per_file
            pages = _ordered_pages(page_source(min_size, max_size, page_rows), group_count, order)
            mode = "thread (async unavailable)" if concur_type == 'async' else "thread"
            log.info(f"UPLOAD {group_count} files ({group_size_str}) in {group_name} group, {mode}, "
                     f"{nuploads} threads × {connections_per_file} conn/file (page={page_rows})")
            (update_args,
                perf,
                _dated_paths) = \
                _thread_upload(src_path, dest_path,
                                pages,
                                databasename, upload_dt_str, update_args,
                                step,
                                perf, nuploads, connections_per_file, verbose,
                                group_perf=group_perf)
            dated_paths.update(_dated_paths)
        else:
            raise ValueError(f"Unknown concurrency type {concur_type} for group {group_name}")

        if remaining is not None:
            remaining -= group_count

        log.info(f"UPLOAD {group_name} group complete:")
        group_perf.report()

    return (update_args,
            perf,
            dated_paths)


def upload_files(src_path : FileSystemHelper, dest_path : FileSystemHelper,
                 modalities: list[str], databasename: str,
                 max_num_files : int = None,
                 **kwargs):
    """Upload unuploaded journal files to the destination and record results.

    Retrieves the pending file list from the journal, then delegates to
    _parallel_upload which dispatches each size group to _async_upload (Azure)
    or _thread_upload (all destinations) based on destination cloud type.
    The journal is updated incrementally as files are verified, and a copy is
    backed up to each dated destination directory after upload completes.

    Args:
        src_path: Local source root.
        dest_path: Remote (or local) destination root.
        modalities: List of modality names to upload.
        databasename: Path to the journal SQLite database.
        max_num_files: Cap on number of files to upload in this call; None means unlimited.

    Returns:
        (upload_dt_str, remaining): Version string for this upload batch and
        the number of files left to upload (None if no cap was applied).
    """
    if max_num_files is not None:
        log.info(f'UPLOAD uploading {max_num_files} files')
    
    verbose = kwargs.get("verbose", False)
    modality_configs = kwargs.get("modality_configs", {})

    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        log.error(f"ERROR: No journal exists for filename {databasename}")
        return None, 0
    
    # ======== upload_dt_str should be constructed from the version string in the journal.
    upload_dt_str = strftime("%Y%m%d%H%M%S", gmtime())   # use string for upload ver_str for readability


    #---------- upload files.
    # Pending files are streamed from the journal in pages per size group (see _parallel_upload),
    # never materialized as a whole list.  Only per-group counts are fetched up front, in one scan.
    if not JournalDispatcher.table_exists(database_name = databasename):
        log.error(f"table journal does not exist in {databasename}.")
        return None, max_num_files
    group_stats = dict(zip(UPLOAD_SIZE_GROUPS.keys(),
                           JournalDispatcher.count_pending_by_size(
                               databasename, modalities,
                               [(g[1], g[2]) for g in UPLOAD_SIZE_GROUPS.values()])))
    total_pending = sum(count for count, _ in group_stats.values())
    if total_pending == 0:
        log.info("no files to upload.  Done")
        return None, max_num_files
    else:
        log.info(f"uploading {total_pending} files to {str(dest_path.root)}")

    if max_num_files is not None:
        upload_count = min(max_num_files, total_pending)
        remaining = max_num_files - upload_count
    else:
        upload_count = total_pending
        remaining = None

    log.info(f"UPLOAD: files found {total_pending}, to upload {upload_count}")

    def page_source(min_size, max_size, page_rows):
        return iter_upload_pages(databasename, modalities, page_rows,
                                 min_size = min_size, max_size = max_size,
                                 modality_configs = modality_configs)

    perf = perf_counter.PerformanceCounter(total_file_count = upload_count)
    
    # copy file, verify and update journal
    update_args = []
    # del_args = []  # not needed.
    
    # for debugging
    # missing_dest = []
    # missing_src = []
    # matched = []
    # mismatched = []
    # replaced = []
    # deleted = []   # only for debug
    dated_dest_paths = {}

    # NEW    
    step = kwargs.get("page_size", DEFAULT_REMOTE_OP_PAGE_SIZE)
    
    # finally, upload the journal.
    journal_path = FileSystemHelper(os.path.dirname(os.path.abspath(databasename)))  # local fs, no client.
    # Question - do we put journal in the dated target subdirectory?  YES for now
    # Question - do we leave the journal update time as NULL?  
    # Question - do we copy the journal into the source directory? YES for now (in case of overwrite)
    journal_fn = os.path.basename(databasename)

    n_cores = kwargs.get("n_cores", 32)
    nthreads = min(n_cores, min(32, (os.cpu_count() or 1) + 4))

    log.info(f"UPLOAD {upload_count} files")
    if upload_count > 0:
        (update_args,
         perf,
         dated_paths) = \
            _parallel_upload(src_path, dest_path,
                            page_source, group_stats,
                            databasename, upload_dt_str, update_args,
                            step,
                            perf, nthreads, verbose,
                            max_files = upload_count)
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
    # these are used for debugging
    # missing_dest = set(missing_dest)
    # missing_src = set(missing_src)
    # matched = set(matched)
    # mismatched = set(mismatched)
    # replaced = set(replaced)

    # handle all deleted (only undeleted files that are not "uploaded" are the ones that are were added and deleted between uploads.).
    # for ones that are mark deleted, we mark them as uploaded as well.  Streamed in pages;
    # marked rows drop out of the query, and the FILE_ID keyset keeps paging stable.
    del_page_rows = step * max(c * k for (_, _, _, c, k) in UPLOAD_SIZE_GROUPS.values())
    n_deleted = 0
    for fids in iter_pending_deleted_ids(databasename, modalities, del_page_rows):
        JournalDispatcher.mark_as_uploaded(databasename,
                                      version = upload_dt_str,
                                      upload_args = [(fid,) for fid in fids])
        n_deleted += len(fids)
    if n_deleted > 0:
        log.info(f"Marked as deleted and uploaded: {n_deleted}")
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
    
    dest_meta = FileSystemHelper.get_metadata(root = dated_dest_path.root, path = central_fn, with_metadata = True, with_md5 = True, local_md5 = md5, **kwargs)
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


def _async_verify(dated_dest_path: FileSystemHelper,
                  pages,
                  concurrency: int, *,
                  compute_md5: bool = False,
                  queue_size: int = None,
                  needs_download=None,
                  **kwargs) -> tuple:
    """Async verification for Azure destinations.

    needs_download: optional sink with .append() (e.g. a disk-backed _DownloadSpill);
      defaults to a new list.  A file whose check raises is reported in `failed`, not
      missing, and does not stop the run.

    pages: iterable of lists of (fid, fn, size, md5, modality).  Streamed through a bounded
      queue (>= 2 * concurrency) to `concurrency` workers, so memory is bounded by ~2 pages.

    Phase 1 (compute_md5=False): fetches blob properties only — fast, no downloads.
      Files where both remote and journal md5 are absent are returned in needs_download.
    Phase 2 (compute_md5=True): streams and hashes blobs with no md5 anywhere.
      needs_download will be empty on return.

    Returns (n_matched, mismatched, missing, needs_download, failed); matched files are only counted.
    """
    if not isinstance(dated_dest_path.client, AzureBlobClient):
        raise ValueError("_async_verify only supports Azure Blob Storage destinations.")

    n_matched = 0
    mismatched = []
    missing = []
    failed = []
    if needs_download is None:
        needs_download = []

    verbose = kwargs.get("verbose", False)
    compiled_patterns = kwargs.get("compiled_patterns", {})
    modality_configs = kwargs.get("modality_configs", {})

    async def process():
        svc = dated_dest_path.client.service_client
        connector = aiohttp.TCPConnector(limit=min(concurrency, 256))
        transport = AioHttpTransport(session=aiohttp.ClientSession(connector=connector))
        async with AsyncBlobServiceClient(account_url=svc.url,
                                          credential=svc.credential,
                                          api_version=svc.api_version,
                                          transport=transport) as async_client:
            async_dest = FileSystemHelper(
                dated_dest_path.root,
                client=dated_dest_path.client,
                internal_host=dated_dest_path.internal_host,
                async_azclient=async_client,
            )
            queue = asyncio.Queue(maxsize=max(queue_size or 0, 2 * concurrency))

            async def worker():
                nonlocal n_matched
                while (item := await queue.get()) is not None:
                    fid, fn, size, md5, modality = item
                    try:
                        central_fn = convert_local_to_central_path(
                            fn,
                            in_compiled_pattern=compiled_patterns.get(modality, None),
                            modality=modality,
                            omop_per_patient=modality_configs.get(modality, {}).get("omop_per_patient", False))
                        dest_meta = await async_dest.async_get_metadata(
                            path=central_fn, with_metadata=True, with_md5=True,
                            local_md5=md5, compute_md5=compute_md5)
                    except Exception as e:
                        log.error(f"verify failed {fid} {fn}, due to {type(e).__name__}: {str(e)}")
                        failed.append(fn)
                        continue
                    if _classify_verify(dest_meta, fid, fn, size, md5, modality,
                                        mismatched, missing, needs_download, verbose):
                        n_matched += 1

            await asyncio.gather(_produce_pages(pages, queue, concurrency),
                                 *(worker() for _ in range(concurrency)))

    asyncio.run(process())
    return n_matched, mismatched, missing, needs_download, failed


def _classify_verify(dest_meta, fid, fn, size, md5, modality,
                     mismatched, missing, needs_download, verbose) -> bool:
    """Classify one async verify result into the given lists; returns True if matched."""
    if dest_meta is None or dest_meta['size'] is None:
        missing.append(fn)
        log.error(f"missing file {fn}")
    elif size != dest_meta['size']:
        log.error(f"mismatched file {fid} {fn}: remote size {dest_meta['size']} journal size {size}")
        mismatched.append(fn)
    elif dest_meta['md5'] is None and md5 is None:
        # both md5s absent: size matches but content unverifiable without download
        needs_download.append((fid, fn, size, md5, modality))
    elif md5 is not None and dest_meta['md5'] is not None and md5 != dest_meta['md5']:
        log.error(f"mismatched file {fid} {fn}: remote md5 {dest_meta['md5']} journal md5 {md5}")
        mismatched.append(fn)
    else:
        # size matches; md5 either matches or one side is None (size-only match)
        if verbose:
            log.debug(f"verified {fn} {fid}")
        else:
            print(".", end="", flush=True)
        return True
    return False


def _thread_verify(dated_dest_path: FileSystemHelper,
                   pages,
                   nthreads: int,
                   **kwargs) -> tuple:
    """Thread-pool verification. Used for huge files that require blob download+hash.

    pages: iterable of lists of (fid, fn, size, md5, modality); at most 2 * nthreads
    checks are in flight.  A check that raises is reported in `failed` and does not stop
    the run.  Returns (n_matched, mismatched, missing, [], failed).
    """
    verbose = kwargs.get("verbose", False)
    n_matched = 0
    mismatched = []
    missing = []
    failed = []

    dtstr = dated_dest_path.root.name
    lock = threading.Lock()

    def check(item):
        try:
            return _get_file_info(dated_dest_path, item, **{**kwargs, "lock": lock}), None
        except Exception as e:
            return None, (item, e)

    def on_done(future):
        nonlocal n_matched
        result, error = future.result()
        if error is not None:
            (item, e) = error
            log.error(f"verify failed {item[0]} {item[1]}, due to {type(e).__name__}: {str(e)}")
            failed.append(item[1])
            return
        dest_meta, dest_md5, fid, fn, size, md5 = result

        if dest_meta is None or dest_meta['size'] is None:
            missing.append(fn)
            log.error(f"missing file {fn}")
        elif size != dest_meta['size']:
            log.error(f"mismatched file {fid} {fn}: remote size {dest_meta['size']} journal size {size}")
            mismatched.append(fn)
        elif md5 is not None and dest_md5 is not None and md5 != dest_md5:
            log.error(f"mismatched file {fid} {fn} for upload {dtstr}: remote md5 {dest_md5} journal md5 {md5}")
            mismatched.append(fn)
        else:
            if verbose:
                log.debug(f"verified {fn} {fid}")
            else:
                print(".", end="", flush=True)
            n_matched += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        _bounded_submit(executor, pages, lambda item: executor.submit(check, item),
                        2 * nthreads, on_done)

    return n_matched, mismatched, missing, [], failed  # no needs_download from thread path


def _parallel_verify(dated_dest_path: FileSystemHelper,
                     source,
                     nthreads: int, *,
                     compute_md5: bool = True,
                     **kwargs) -> tuple:
    """Dispatch verification to _async_verify or _thread_verify by cloud type and file size.

    Design rationale
    ----------------
    This function is the size-group dispatcher for verification — the verify-side
    analogue of _parallel_upload.  It is intentionally not responsible for the
    two-phase orchestration (metadata-only phase 1 vs. download+hash phase 2);
    that logic belongs in verify_files, which calls this function only for the
    phase 2 download+hash pass.

    Phase 1 (metadata-only, flat concurrency=128) does not benefit from size
    grouping because it issues only a get_blob_properties call per file.
    Routing it through this function would add unnecessary complexity.  Phase 1
    is therefore dispatched directly from verify_files.

    This function handles phase 2: files whose md5 is absent from both the remote
    blob and the journal, requiring a full content download+hash.  Files are
    grouped by size to balance concurrency against per-connection memory pressure:

    # group    size        type    concurrency   notes
    # small   <   1MB     async       32         stream+hash
    # medium  <   8MB     async       16         stream+hash
    # large   <  64MB     async        8         stream+hash
    # xlarge  < 128MB     async        4         stream+hash
    # xxlarge < 256MB     thread       4         sync stream+hash
    # huge    >= 256MB    thread       2         sync stream+hash

    For non-Azure destinations the async path is unavailable; all groups fall
    back to _thread_verify.

    Args:
        files_to_verify: iterable of (fid, fn, size, md5, modality)
        compute_md5: passed through to _async_verify; True means stream+hash.

    Args:
        source: has count(min_size, max_size) and iter_pages(min_size, max_size, batch)
          yielding lists of (fid, fn, size, md5, modality) — e.g. a _DownloadSpill.  Each
          group is streamed in pages of page_size × concurrency rows.

    Returns:
        (n_matched, mismatched, missing, failed) — a count and plain lists.
    """
    verbose = kwargs.get("verbose", False)

    if not isinstance(dated_dest_path.client, AzureBlobClient):
        raise ValueError("_parallel_verify only supports Azure Blob Storage destinations.")

    use_async = True  # already guaranteed Azure by the guard above

    download_groups = {
        'small':   ('async',  0,       2**20,  32),
        'medium':  ('async',  2**20,   2**23,  16),
        'large':   ('async',  2**23,   2**26,   8),
        'xlarge':  ('async',  2**26,   2**27,   4),
        'xxlarge': ('thread', 2**27,   2**28,   4),
        'huge':    ('thread', 2**28,   float('inf'), 2),
    }

    step = kwargs.get("page_size", DEFAULT_REMOTE_OP_PAGE_SIZE)
    matched = 0
    mismatched = []
    missing = []
    failed = []

    for group_name, (concur_type, min_size, max_size, concurrency) in download_groups.items():
        n_group = source.count(min_size, max_size)
        if n_group == 0:
            continue

        if concur_type == 'async' and use_async:
            page_rows = step * concurrency
            log.info(f"VERIFY {n_group} files in {group_name} group, async")
            m, mm, mi, _, fa = _async_verify(dated_dest_path, source.iter_pages(min_size, max_size, page_rows),
                                             concurrency, compute_md5=compute_md5,
                                             queue_size=page_rows, **kwargs)
        else:
            nuploads = min(concurrency, nthreads)
            log.info(f"VERIFY {n_group} files in {group_name} group, thread")
            m, mm, mi, _, fa = _thread_verify(dated_dest_path,
                                              source.iter_pages(min_size, max_size, step * nuploads),
                                              nuploads, **kwargs)
        matched += m
        mismatched += mm
        missing += mi
        failed += fa

    return matched, mismatched, missing, failed


class _DownloadSpill:
    """Disk-backed list of (fid, fn, size, md5, modality) awaiting phase-2 download+hash.

    append() buffers up to buffer_rows in memory, then writes to a scratch SQLite file
    (indexed by size) next to the journal; the file is removed on close.  Phase 2 streams
    it per size group, so memory stays bounded however many files lack an md5.
    """

    def __init__(self, path: str, buffer_rows: int):
        self.path = path
        self.buffer_rows = buffer_rows
        self._buf = []
        self._count = 0
        self._remove()
        with self._connect() as c:
            c.execute("CREATE TABLE pending (FILE_ID INTEGER PRIMARY KEY, PATH TEXT, SIZE INTEGER,"
                      " MD5 TEXT, MODALITY TEXT)")
            c.execute("CREATE INDEX pending_size ON pending (SIZE)")

    def _connect(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.execute("PRAGMA synchronous=OFF")   # scratch data: durability not needed
        return closing_commit(conn)

    def _remove(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def append(self, item):
        self._buf.append(item)
        self._count += 1
        if len(self._buf) >= self.buffer_rows:
            self.flush()

    def flush(self):
        if self._buf:
            with self._connect() as c:
                c.executemany("INSERT OR IGNORE INTO pending VALUES (?,?,?,?,?)", self._buf)
            self._buf.clear()

    def __len__(self):
        return self._count

    def count(self, min_size, max_size) -> int:
        self.flush()
        hi = "" if max_size == float('inf') else " AND SIZE < ?"
        args = (min_size,) if max_size == float('inf') else (min_size, max_size)
        with self._connect() as c:
            return c.execute(f"SELECT COUNT(*) FROM pending WHERE SIZE >= ?{hi}", args).fetchone()[0]

    def iter_pages(self, min_size, max_size, batch: int):
        """Yield lists of (fid, fn, size, md5, modality) in [min_size, max_size), FILE_ID order."""
        self.flush()
        hi = "" if max_size == float('inf') else " AND SIZE < ?"
        last = None
        while True:
            after = "" if last is None else " AND FILE_ID > ?"
            args = [min_size] + ([] if max_size == float('inf') else [max_size]) + ([] if last is None else [last])
            with self._connect() as c:
                rows = c.execute(f"SELECT FILE_ID, PATH, SIZE, MD5, MODALITY FROM pending WHERE SIZE >= ?{hi}{after}"
                                 " ORDER BY FILE_ID LIMIT ?", args + [batch]).fetchall()
            if not rows:
                return
            last = rows[-1][0]
            yield rows

    def close(self):
        self._buf.clear()
        self._remove()


@contextmanager
def closing_commit(conn):
    """Yield conn, commit on success, always close."""
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def verify_files(dest_path: FileSystemHelper, databasename: str = "journal.db",
                 version: Optional[str] = None,
                 modalities: Optional[list] = None,
                 **kwargs):
    """Verify the integrity of uploaded files against the journal.

    Two-phase verification strategy
    --------------------------------
    For Azure Blob destinations, verification is split into two phases to avoid
    downloading file content unless strictly necessary:

    Phase 1 — metadata only, all files, high concurrency (async, concurrency=128):
        Calls _async_verify with compute_md5=False.  For each file:
        - If the remote blob is missing → missing.
        - If sizes differ → mismatched.
        - If either the remote blob md5 or the journal md5 is present → compared
          directly; matched or mismatched.  (If only one side has an md5,
          _async_verify writes the known md5 back to the blob so future verifications
          are cheaper.)
        - If both md5s are absent → deferred to phase 2 (needs_download list).
        Phase 1 uses a single flat concurrency because get_blob_properties is
        cheap and uniform regardless of file size — no size grouping needed.

    Phase 2 — content download + hash, deferred files only (Azure only):
        Calls _parallel_verify, which groups files by size and dispatches each
        group to _async_verify(compute_md5=True) for small/medium/large/xlarge
        files, or to _thread_verify for very large files (>128 MB).  Phase 2
        only runs when needs_download is non-empty, which is rare once blobs
        have md5 attributes set.

    For non-Azure destinations (S3, local):
        _thread_verify is called directly (single phase).  The sync get_metadata
        path always provides an md5 (S3 etag, local computed hash), so needs_download
        is never populated and phase 2 is unreachable.

    Args:
        dest_path: Destination root (cloud or local).
        databasename: Path to the journal SQLite database.
        version: Upload version string to verify; defaults to the latest version.
        modalities: List of modality names to check; None means all.

    Active files of the version are streamed from the journal in batches of
    page_size × concurrency rows; only mismatched/missing paths (and phase-2 candidates)
    are kept in memory.

    Phase-2 candidates are spilled to a scratch SQLite file next to the journal.  A file
    whose check raises (e.g. a network error) is reported in `failed` and does not stop
    the run; rerun verify for those.

    Returns:
        (n_matched, mismatched, missing, failed) — matched count; the others are sets of paths.
    """
    verbose = kwargs.get("verbose", False)

    if not os.path.exists(databasename):
        log.error(f"No journal exists for filename {databasename}")
        return

    dtstr = version if version is not None else JournalDispatcher.get_latest_version(databasename)

    filters = {'version': dtstr, 'active': True}
    n_files = sum(n for _, n in JournalDispatcher.count_files_by_version(databasename, modalities, **filters))

    if n_files == 0:
        log.info("no files to verify.")
        return

    dated_dest_path = FileSystemHelper(dest_path.root.joinpath(dtstr),
                                       client=dest_path.client,
                                       internal_host=dest_path.internal_host)

    n_cores = kwargs.get("n_cores", 32)
    nthreads = min(n_cores, min(32, (os.cpu_count() or 1) + 4))

    step = kwargs.get("page_size", DEFAULT_REMOTE_OP_PAGE_SIZE)

    def file_info_pages(batch):
        # read-only pass: one streaming cursor over the journal.
        for rows in JournalDispatcher.iter_files_batches(databasename, modalities, batch, **filters):
            yield [(fid, filepath, size, md5, modality)
                   for (fid, filepath, _, size, md5, modality, _, _, _) in rows]

    if isinstance(dest_path.client, AzureBlobClient):
        # Phase 1: metadata-only, all files.  Flat concurrency=128 is intentional —
        # get_blob_properties cost is uniform regardless of file size, so size
        # grouping adds no benefit here.
        concurrency = 128
        page_rows = step * concurrency
        log.info(f"VERIFY phase 1: fetching metadata for {n_files} files (async, concurrency={concurrency}, page={page_rows})")
        spill = _DownloadSpill(databasename + ".verify_download.tmp", buffer_rows=page_rows)
        try:
            matched, mismatched, missing, _, failed = _async_verify(
                dated_dest_path, file_info_pages(page_rows), concurrency=concurrency,
                queue_size=page_rows, needs_download=spill, **kwargs)

            if len(spill) > 0:
                # Phase 2: content download + hash only for files with no md5 anywhere.
                # _parallel_verify groups by size and dispatches to async or thread.
                log.info(f"VERIFY phase 2: downloading and hashing {len(spill)} files with no md5")
                m, mm, mi, fa = _parallel_verify(
                    dated_dest_path, spill, nthreads,
                    compute_md5=True, **kwargs)
                matched += m
                mismatched += mm
                missing += mi
                failed += fa
        finally:
            spill.close()
    else:
        # Non-Azure: single-phase thread verify.  The sync get_metadata path
        # always returns an md5 (S3 etag or locally computed hash), so there
        # is no deferred phase 2.
        log.info(f"VERIFY: {n_files} files using {nthreads} threads")
        matched, mismatched, missing, _, failed = _thread_verify(
            dated_dest_path, file_info_pages(step * nthreads), nthreads, **kwargs)

    mismatched = set(mismatched)
    missing = set(missing)
    failed = set(failed)

    log.info(f"verified {matched} files, {len(mismatched)} mismatched, {len(missing)} missing, "
             f"{len(failed)} failed to check{' (rerun verify)' if failed else ''}.")

    return matched, mismatched, missing, failed


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

    # stream the unuploaded, active rows once and keep only those named in `files`,
    # so memory is bounded by the file list, not the journal.
    wanted = set(files)
    db_files = {}
    batch = kwargs.get("page_size", DEFAULT_REMOTE_OP_PAGE_SIZE)
    for rows in JournalDispatcher.iter_files_batches(databasename, None, batch, version = version,
                                                     active = True, uploaded = False):
        for row in rows:
            if row[1] in wanted:
                db_files[row[1]] = row
    
    matched = []
    
    log.info(f"marking {len(files)} files as uploaded.")
    for f in files:
   
        if f not in db_files:
            log.warning(f"file {f} not found in journal or was previously uploaded.")
            continue
        
        (_fid, _fn, _, _size, _md5, _mod, _, _ver, _) = db_files[f]
        _info = (_fid, _fn, _size, _md5, _ver, _mod)
        
        (dest_meta, dest_md5, dest_root, fid, fn, size, md5) = _get_file_info2(dest_path, _info, **kwargs)
        
        if dest_meta is None:
            log.error(f"missing file {fn} in destination {dest_root}")
            continue

        if (size == dest_meta['size']) and (md5 == dest_md5):
            if verbose:
                log.debug(f"marking as uploaded {version} {fn} {fid}")
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