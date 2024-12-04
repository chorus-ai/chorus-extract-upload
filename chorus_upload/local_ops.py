import re
import time
import math
import os
from typing import Optional, List
from chorus_upload.storage_helper import FileSystemHelper
from pathlib import Path
import concurrent.futures
import chorus_upload.perf_counter as perf_counter
from chorus_upload.journaldb_ops import JournalDB

# Futures ThreadPoolExecutor may not work since python has a global interpreter lock (GIL) that prevents thread based parallelism, at least until 3.13 (optional)
# alternative: use asyncio?

def update_journal(root : FileSystemHelper, modalities: list[str],
                   databasename: str,
                   journaling_mode = "append",
                   version: str = None,
                   amend: bool = False,
                   **kwargs):
    
    
    table_exists = JournalDB.table_exists(database_name = databasename,
                                          table_name = "journal")
    
    # check if journal table exists
    if table_exists:
        return _update_journal(root, modalities, databasename = databasename, 
                               journaling_mode = journaling_mode, 
                               version = version, amend = amend, kwargs = kwargs)
    else:  # no amend possible since the file did not exist.
        return _gen_journal(root, modalities, databasename, version = version, kwargs = kwargs)
        
# compile a regex for extracting person id from waveform and iamge paths
# the personid is the first part of the path, followed by the modality, then the rest of the path
PERSONID_REGEX = re.compile(r"([^/:]+)/(Waveforms|Images)/.*", re.IGNORECASE)

def _gen_journal_one_file(root : FileSystemHelper, relpath:str, modality:str, curtimestamp:int, version:str):
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
    
    mymd5 = meta['md5']  # if azure file, could be None.
                
    myargs = (personid, relpath, modality, modtimestamp, filesize, mymd5, curtimestamp, None, "ADDED", md5_time, version)

    return myargs



def _gen_journal(root : FileSystemHelper, modalities: list[str], 
                  databasename: str, 
                  version:str = None, 
                  **kwargs):
    """
    Generate a journal for the given root directory and subdirectories.

    Args:
        root (FileSystemHelper): The root directory to generate the journal for.
        modalities (list[str], optional): The subdirectories to include in the journal. Defaults to ['Waveforms', 'Images', 'OMOP'].
        databasename (str, optional): The name of the database to store the journal. Defaults to "journal.db".
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """
    print("INFO: Creating journal. NOTE: this may take a while for md5 computation.", flush=True)
    
    verbose = kwargs.get("verbose", False)

    # TODO: set the version to either the supplied version name, or with current time.
    new_version = version if version is not None else time.strftime("%Y%m%d%H%M%S")
        
    perf = perf_counter.PerformanceCounter()
        
    JournalDB.create_journal_table(databasename)
    
    paths = []
    curtimestamp = int(math.floor(time.time() * 1e6))
    all_args = []
    
    page_size = kwargs.get("page_size", 1000)
    n_cores = kwargs.get("n_cores", 32)
    nthreads = min(n_cores, min(32, (os.cpu_count() or 1) + 4))
    print("INFO: Using ", nthreads, " threads")

    for modality in modalities:
        start = time.time()
        # pattern = f"{modality}/**/*" if modality == "OMOP" else f"*/{modality}/**/*"
        if (modality.lower() == "waveforms"):
            pattern = "*/[wW][aA][vV][eE][fF][oO][rR][mM][sS]/**/*"
        elif (modality.lower() == "images"):
            pattern = "*/[iI][mM][aA][gG][eE][sS]/**/*"
        elif (modality.lower() == "omop"):
            pattern = "[oO][mM][oO][pP]/**/*"
        else:
            pattern = f"*/{modality}/**/*"
            
        # paths = root.get_files(pattern)  # list of relative paths in posix format and in string form.
        # # paths += paths1
        # print("Get File List took ", time.time() - start)
        
        total_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:

            for paths in root.get_files_iter(pattern, page_size = page_size):
                # list of relative paths in posix format and in string form.
                futures = []

                for relpath in paths:
                    if verbose:
                        print("INFO: scanning ", relpath)
                    future = executor.submit(_gen_journal_one_file, root, relpath, modality, curtimestamp, new_version)
                    futures.append(future)
                
                for future in concurrent.futures.as_completed(futures):
                    myargs = future.result()
                    perf.add_file(myargs[4])
                    rlpath = myargs[1]
                    status = myargs[7]
                    if verbose and status == "ADDED":
                        print("INFO: ADDED ", rlpath)
                    else:
                        print(".", end="", flush=True)
                        
                    all_args.append(myargs)

                insert_count, last_row = JournalDB.insert_journal_entries(databasename, all_args)
                
                total_count += len(all_args)
                print(f"INFO: inserted {insert_count} of {len(all_args)}.  added {total_count} files to journal.db of {last_row} rows" )

                all_args = []
                
                perf.report()
        
        del perf
        print("INFO: Journal Update took ", time.time() - start, "s")
        

def _update_journal_one_file(root: FileSystemHelper, relpath:str, modality:str, curtimestamp:int, modality_files_to_inactivate:dict, version:str):
    # get information about the current file
    meta = FileSystemHelper.get_metadata(root = root.root, path = relpath, with_metadata = True, with_md5 = False)
    # root.get_file_metadata(relpath)
    
    # TODO need to handle version vs old version.
    
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
    # filecheckrow = cur.execute("Select file_id, size, src_modtime_us, md5, upload_dtstr from journal where filepath=? and TIME_INVALID_us IS NULL", [(relpath)])

    # results = filecheckrow.fetchall()
    if relpath in modality_files_to_inactivate.keys():
        results = modality_files_to_inactivate[relpath]
        # There should only be 1 active file according to the path in a well-formed 
        if (len(results) > 1):
            # print("ERROR: Multiple active files with that path - journal is not consistent")
            return (personid, relpath, modality, modtimestamp, None, None, curtimestamp, None, "ERROR1", None, None)
        if (len(results) == 0):
            # print("ERROR: File found but no metadata.", relpath)
            return (personid, relpath, modality, modtimestamp, None, None, curtimestamp, None, "ERROR2", None, None)
        
        if len(results) == 1:
            (oldfileid, oldsize, oldmtime, oldmd5, oldsync, oldversion) = results[0]
            
            # if old size and new size, old mtime and new mtime are same (name is already matched)
            # then same file, skip rest.
            if (oldmtime == modtimestamp):
                if (oldsize == filesize):
                    # files are very likely the same because of size and modtime match
                    keep_file = relpath
                    # del modality_files_to_inactivate[relpath]  # do not mark file as inactive.
                    # print("SAME ", relpath )
                    return (personid, relpath, modality, modtimestamp, oldsize, oldmd5, curtimestamp, oldsync, "KEEP", None, oldversion)
                else:
                    #time stamp same but file size is different?
                    # print("ERROR: File size is different but modtime is the same.", relpath)
                    return (personid, relpath, modality, modtimestamp, oldsize, oldmd5, curtimestamp, oldsync, "ERROR3", None, oldversion)
                
            else:
                # timestamp changed. we need to compute md5 to check, or to update.
                start = time.time()
                mymd5 = FileSystemHelper.get_metadata(root = root.root, path = relpath, with_metadata = False, with_md5 = True)['md5']
                md5_time = time.time() - start

                keep_file = None
                # files are extremely likely the same just moved.
                # set the old entry as invalid and add a new one that's essentially a copy except for modtime..
                # choosing not to change SYNC_TIME
                # cur.execute("UPDATE journal SET TIME_INVALID_us=? WHERE file_id=?", (curtimestamp, oldfileid))
                if (oldsize == filesize) and (mymd5 == oldmd5):
                    # essentially copy the old and update the modtime.
                    myargs = (personid, relpath, modality, modtimestamp, oldsize, oldmd5, curtimestamp, oldsync, "MOVED", md5_time, oldversion)

                    # files_to_inactivate.remove(oldfileid)  # we should mark old as inactive 
                else:
                    # files are different.  set the old file as invalid and add a new file.
                    myargs = (personid, relpath, modality, modtimestamp, filesize, mymd5, curtimestamp, None, "UPDATED", md5_time, version)
                    # if verbose:
                    #     print("INFO: UPDATED ", relpath)
                    # modified file. old one should be removed.
    else:
        # if DNE, add new file
        start = time.time()
        mymd5 = FileSystemHelper.get_metadata(root = root.root, path = relpath, with_metadata = False, with_md5 = True)['md5']
        md5_time = time.time() - start

        myargs = (personid, relpath, modality, modtimestamp, filesize, mymd5, curtimestamp, None, "ADDED", md5_time, version)

            
    return myargs


# record is a mess with a file-wise traversal. Files need to be grouped by unique record ID (visit_occurence?)
def _update_journal(root: FileSystemHelper, modalities: list[str],
                    databasename : str,
                    journaling_mode = "append",
                     version: str = None,
                     amend:bool = False, 
                     **kwargs):
    """
    Update the journal database with the files found in the specified root directory.

    Args:
        root (FileSystemHelper): The root directory to search for files.
        modalities (list[str], optional): The subdirectories to search for files. Defaults to ['Waveforms', 'Images', 'OMOP'].
        databasename (str, optional): The name of the journal database. Defaults to "journal.db".
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """
    
    verbose = kwargs.get("verbose", False)

    print("INFO: Updating journal...", databasename, flush=True)

    # TODO:  
    # if amend, get the last version, and add files to that version.
    # if not amend, create a new version - either with supplied version name, or with current time.
    version = version if version is not None else time.strftime("%Y%m%d%H%M%S")
    if amend: # get the last version
        version = JournalDB.get_max_value(databasename, table_name="journal", column_name="VERSION")
    
    perf = perf_counter.PerformanceCounter()
    
    # check if journal table exists.
    curtimestamp = int(math.floor(time.time() * 1e6))
    
    page_size = kwargs.get("page_size", 1000)
    
    n_cores = kwargs.get("n_cores", 32)
    nthreads = min(n_cores, min(32, (os.cpu_count() or 1) + 4))
    print("INFO: Using ", nthreads, " threads")

    paths = []
    all_insert_args = []
    all_del_args = []
    total_count = 0
    for modality in modalities:
        start = time.time()
        
        if (modality.lower() == "waveforms"):
            pattern = "*/[wW][aA][vV][eE][fF][oO][rR][mM][sS]/**/*"
        elif (modality.lower() == "images"):
            pattern = "*/[iI][mM][aA][gG][eE][sS]/**/*"
        elif (modality.lower() == "omop"):
            pattern = "[oO][mM][oO][pP]/**/*"
        else:
            pattern = f"*/{modality}/**/*"
        
        # do one modality at a time for now - logic is tested.  doing multiple modalities may accidentally delete?
        activefiletuples = JournalDB.query(databasename, table_name = "journal",
                                           columns = ["FILEPATH", "FILE_ID", "SIZE", "SRC_MODTIME_us", "MD5", "UPLOAD_DTSTR", "VERSION"],
                                           where_clause = f"TIME_INVALID_us IS NULL AND MODALITY = '{modality}'",
                                           count = None)

        # initialize by putting all files in files_to_inactivate.  remove from this list if we see the files on disk
        modality_files_to_inactivate = {}
        for i in activefiletuples:
            fpath = i[0]
            if fpath not in modality_files_to_inactivate.keys():
                modality_files_to_inactivate[fpath] = [ (i[1], i[2], i[3], i[4], i[5], i[6]) ]
            else:
                modality_files_to_inactivate[fpath].append((i[1], i[2], i[3], i[4], i[5], i[6]))

        with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
            
            for paths in root.get_files_iter(pattern, page_size = page_size ):  # list of relative paths in posix format and in string form.
                futures = []

                for relpath in paths:
                    if verbose:
                        print("INFO: scanning ", relpath)
                    future = executor.submit(_update_journal_one_file, root, relpath, modality, curtimestamp, modality_files_to_inactivate, version)
                    futures.append(future)
                
                for future in concurrent.futures.as_completed(futures):
                    myargs = future.result()
                    perf.add_file(myargs[4])
                    
                    status = myargs[8]
                    rlpath = myargs[1]
                    if status == "ERROR1":
                        print("ERROR: Multiple active files with that path - journal is not consistent", relpath)
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
            
                    # back up only on upload
                    # backup_journal(databasename)
                    
                # save to database
                update_count, last_row = JournalDB.insert_journal_entries(databasename, all_insert_args)
                
                total_count += len(all_insert_args)
                print(f"INFO: inserted {update_count} of {len(all_insert_args)}.  added {total_count} files to journal.db of {last_row} rows" )

                all_insert_args = []
                
                perf.report()

            # for all files that were active but aren't there anymore
            # invalidate in journal
            
            for relpath, vals in modality_files_to_inactivate.items():
                # check if there are non alphanumeric characters in the path
                # if so, print out the path
                if not (relpath.isalnum() or relpath.isascii()):
                    print("INFO: Path contains non-alphanumeric characters:", relpath)

                for v in vals:
                    if (relpath in paths):
                        all_del_args.append(("OUTDATED", v[0]))
                        if verbose:
                            print("INFO: OUTDATED  ", relpath)
                    elif (journaling_mode == "full"):
                        all_del_args.append(("DELETED", v[0]))
                        if verbose:
                            print("INFO: DELETED  ", relpath)
            # print("SQLITE update arguments ", all_del_args)
            if (total_count == 0) and len(all_del_args) == 0:
                print("INFO: Nothing to change in journal for modality ", modality)
            elif (len(all_del_args) > 0):
                # back up only on upload
                # backup_journal(databasename)
                
                deleted = JournalDB.inactivate_journal_entries(databasename, 
                                                       "journal", 
                                                       curtimestamp, 
                                                       all_del_args)
                
                to_delete = len(all_del_args)
                print(f"INFO: deleted/outdated {deleted} of {to_delete} from journal." )
            
        del perf
        print("INFO: Journal Update Elapsed time ", time.time() - start, "s")


# explicitly mark files as deleted.  This is best used when the journaling mode is "append"        
def mark_files_as_deleted(files_to_remove:List[str], 
                          databasename: str, 
                          version: str = None, 
                          verbose:bool = False):
    
    if version is not None:
        where_clause = f"TIME_INVALID_us IS NULL AND version = '{version}'"
    else:
        where_clause = "TIME_INVALID_us IS NULL"
        
    
    # find all files that are active
    activefiletuples = JournalDB.query(databasename, table_name = "journal",
                                        columns = ["FILEPATH", "FILE_ID"],
                                        where_clause = where_clause,
                                        count = None)
    
    # convert filepath to dictionary with list of file_id for quick look up
    active_files = {}
    for (fpath, file_id) in activefiletuples:
        if fpath in active_files.keys():
            active_files[fpath].append(file_id)
        else:
            active_files[fpath] = [file_id]
    
    # get the list of file ids.
    to_inactivate = set()
    for f in files_to_remove:
        if f in active_files.keys():
            to_inactivate = set.union(to_inactivate, set(active_files[f]))
            
    if verbose:
        print("INFO: Inactivating (marking as deleted) ", len(all_del_args), " files")
        for f in to_inactivate:
            print("INFO: DELETED ", f)
        
    if to_inactivate is None or len(to_inactivate) == 0:
        print("No files to delete")
        return

    all_del_args = [("DELTED", f) for f in to_inactivate]
    # all_del_args = list(to_inactivate)
    
    # get the current timestamp
    curtimestamp = int(math.floor(time.time() * 1e6))
    
    # update the database
    deleted = JournalDB.inactivate_journal_entries(databasename,
                                             "journal",
                                             curtimestamp,
                                             all_del_args)
    
    to_delete = len(all_del_args)
    print(f"INFO: mark as deleted {deleted} of {to_delete} from journal." )

    


# all files to upload are marked with upload_dtstr = NULL. 
# return list of files organized by version
def list_files(databasename: str, version: Optional[str] = None, 
                         modalities: list[str] = None,
                         verbose:bool = False):
    """
    Selects the files to upload from the journal database.  Print to stdout if outfile is not specified, and return list.

    Args:
        databasename (str, optional): The name of the journal database. Defaults to "journal.db".
        version (str, optional): The version of the upload, which is the datetime of an upload.  If None (default) the un-uploaded files are returned
        outfilename (Optional[str], optional): The name of the output file to write the selected files. Defaults to None.
        verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
        dict: The dictionary of {version: list[filenames]}.
    """
    # when listing files, looking for version match if specified, and upload_dtstr is NULL.
        
    table_exists = JournalDB.table_exists(database_name = databasename,
                                        table_name = "journal")
    
    if not table_exists:
        print(f"ERROR: table journal does not exist.")
        return None

    # identify 3 subsets:   deleted, updated, and added.
    # deleted:  file with invalid time stamp.  No additional valid time stamp
    # added:  file with null invalid time stemp and no prior entry with non-null invalid time stamp
    # modified:  file with null invalid time, and a prior entry with non-null invalid time stamp
    # files_to_remove = cur.execute("Select filepath, size, md5, time_invalid_us from journal where upload_dtstr IS NULL").fetchall()
    # to_remove = [ ]
    
    if version is None:
        where_clause = "upload_dtstr IS NULL"
    else:
        where_clause = f"upload_dtstr IS NULL AND version = '{version}'"
    
    if (modalities is not None) and (len(modalities) > 0):
        where_clause += " AND ("
        where_2 = [f"MODALITY = '{modality}'" for modality in modalities]
        where_clause += " OR ".join(where_2)
        where_clause += ")"
        
            
    # select where upload_dtstr is NULL or time_invalid_us is NULL
    files_to_update = JournalDB.query(database_name = databasename, 
                                        table_name = "journal",
                                        columns = ["FILEPATH", "TIME_INVALID_us", "VERSION"],
                                        where_clause = where_clause,
                                        count = None)
        
    print("INFO: extracted files to upload for ", ("current" if version is None else version), " upload version and modalities =", 
          (",".join(modalities) if modalities is not None else "all"))
    
    # this is pulling back only files with changes during since the last upload.  an old file may be inactivated but is in a previous upload
    # so we only see the "new" part, which would look like an add.
    # any deletion would be updating entries in a previous upload batch, so would not see deletion
    # only update we would see is if a file is added, then updated between 2 uploads.   Not thta add/delete/add would show as moved or updated.
    
    # deleted or overwritten files would still be in old version of the cloud.  so we don't need to delete.
    
    # first get the set of active and inactive files.  these are hashtables for quick lookups.
    # should not have same active file in multiple versions -  older one would be invalidated so should go to inactive files.
    active_files = {}
    inactive_files = {}
    for (fn, invalidtime, version) in files_to_update:
        if invalidtime is None:
            # current files or added files
            if (version in active_files.keys()):
                active_files[version].append(fn)
            else:
                active_files[version] = [fn]
        else:
             # old files or deleted files
            if (version in inactive_files.keys()):
                inactive_files[version].append(fn)
            else:
                inactive_files[version] = [fn]
                
    for version in active_files.keys():
        active_files[version] = set(active_files[version])
    for version in inactive_files.keys():
        inactive_files[version] = set(inactive_files[version])
    
    if verbose:
        all_active_files = set()
        all_inactive_files = set()
        for k in active_files.keys():
            all_active_files = set.union(all_active_files, active_files[k])
        for k in inactive_files.keys():
            all_inactive_files = set.union(all_inactive_files, inactive_files[k])
        updates = all_active_files.intersection(all_inactive_files)
        adds = all_active_files - updates
        deletes = all_inactive_files - updates
        for f in adds:
            print("INFO: ADD ", f)
        for f in updates:
            print("INFO: UPDATE ", f)
        for f in deletes:
            print("INFO: DELETE ", f)
            
    # sort the file-list
    # file_list.sort()
    
    if len(active_files) == 0:
        print("INFO: No active files found")
    
    # do we need to delete files in central at all? NO.  journal will be updated.
    # we only need to add new files
                
    return active_files


# all files to upload are marked with upload_dtstr = NULL. 
# return list of files organized by version
def list_files_with_info(databasename: str, version: Optional[str] = None, 
                         modalities: list[str] = None,
                         verbose:bool = False):
    """
    Selects the files to upload from the journal database.  Print to stdout if outfile is not specified, and return list.

    Args:
        databasename (str, optional): The name of the journal database. Defaults to "journal.db".
        version (str, optional): The version of the upload, which is the datetime of an upload.  If None (default) the un-uploaded files are returned
        outfilename (Optional[str], optional): The name of the output file to write the selected files. Defaults to None.
        verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
        dict: The dictionary of {version: list[filenames]}.
    """
    # when listing files, looking for version match if specified, and upload_dtstr is NULL.
        
    table_exists = JournalDB.table_exists(database_name = databasename,
                                        table_name = "journal")
    
    if not table_exists:
        print(f"ERROR: table journal does not exist.")
        return None

    # identify 3 subsets:   deleted, updated, and added.
    # deleted:  file with invalid time stamp.  No additional valid time stamp
    # added:  file with null invalid time stemp and no prior entry with non-null invalid time stamp
    # modified:  file with null invalid time, and a prior entry with non-null invalid time stamp
    # files_to_remove = cur.execute("Select filepath, size, md5, time_invalid_us from journal where upload_dtstr IS NULL").fetchall()
    # to_remove = [ ]
    
    if version is None:
        where_clause = "upload_dtstr IS NULL"
    else:
        where_clause = f"upload_dtstr IS NULL AND version = '{version}'"
    
    if (modalities is not None) and (len(modalities) > 0):
        where_clause += " AND ("
        where_2 = [f"MODALITY = '{modality}'" for modality in modalities]
        where_clause += " OR ".join(where_2)
        where_clause += ")"
        
            
    # select where upload_dtstr is NULL or time_invalid_us is NULL
    files_to_update = JournalDB.query(database_name = databasename, 
                                        table_name = "journal",
                                        columns = ["FILEPATH", "FILE_ID", "SIZE", "MD5", "TIME_INVALID_us", "VERSION"],
                                        where_clause = where_clause,
                                        count = None)
        
    print("INFO: extracted files to upload for ", ("current" if version is None else version), " upload version and modalities =", 
          (",".join(modalities) if modalities is not None else "all"))
    
    # this is pulling back only files with changes during since the last upload.  an old file may be inactivated but is in a previous upload
    # so we only see the "new" part, which would look like an add.
    # any deletion would be updating entries in a previous upload batch, so would not see deletion
    # only update we would see is if a file is added, then updated between 2 uploads.   Not thta add/delete/add would show as moved or updated.
    
    # deleted or overwritten files would still be in old version of the cloud.  so we don't need to delete.
    
    # first get the set of active and inactive files.  these are hashtables for quick lookups.
    # should not have same active file in multiple versions -  older one would be invalidated so should go to inactive files.
    active_files_by_version = {}
    active_files = {}  # file path should be unique.
    inactive_files = {}  # file path to file id mapping is not unique
    for (fn, fid, size, md5, invalidtime, version) in files_to_update:
        if invalidtime is None:
            # updated files or added files.  should be in the most recent version.
            if (fn in active_files.keys()):
                print(f"ERROR: Inconsistent Journal.  Multiple active files with path {fn}", flush=True)
            else:
                active_files[fn] = {'file_id': fid, 'size': size, 'md5': md5, 'version': version}
                
            if (version in active_files_by_version.keys()):
                active_files_by_version[version].append(fn)
            else:
                active_files_by_version[version] = [fn]
        else:
            # old files or deleted files
            if (fid in inactive_files.keys()):
                inactive_files[fn].append(fid)
            else:
                inactive_files[fn] = [fid]
    
    for version in active_files_by_version.keys():
        active_files_by_version[version] = set(active_files_by_version[version])
    for fn in inactive_files.keys():
        inactive_files[fn] = set(inactive_files[fn])    
    
    # figure out the deleted ones 
    if verbose:
        all_active_files = set(active_files.keys())
        all_inactive_files = set(inactive_files.keys())
        updates = all_active_files.intersection(all_inactive_files)
        adds = all_active_files - updates
        deletes = all_inactive_files - updates
        for f in adds:
            print("INFO: ADD ", f)
        for f in updates:
            print("INFO: UPDATE ", f)
        for f in deletes:
            print("INFO: DELETE ", f)
            
    # sort the file-list
    # file_list.sort()
    
    if len(active_files) == 0:
        print("INFO: No active files found")
    
    # do we need to delete files in central at all? NO.  journal will be updated.
    # we only need to add new files
                
    return active_files_by_version, active_files, inactive_files

def list_versions(databasename: str):
    """
    Retrieve a list of unique upload dates from the journal database.

    Args:
        databasename (str): The name of the journal database file. Defaults to "journal.db".

    Returns:
        list: A list of unique upload dates from the journal database.

    Raises:
        None

    """
    
    if not Path(databasename).exists():
        print(f"ERROR: No journal exists for filename {databasename}")
        return None
    
    
    # List unique values in upload_dtstr column in journal table
    res = JournalDB.get_unique_values(databasename, "journal", "VERSION", count = None)
    versions = [u[0] for u in res]
    
    if (versions is not None) and (len(versions) > 0):
        print("INFO: upload records in the database:")
        for table in versions:
            print("INFO:  ", table)
    else:
        print("INFO: No uploads found in the database.")
    
    return versions


# def list_journals(databasename: str):
#     """
#     Lists the tables in the SQLite database specified by the given database name.

#     Args:
#         databasename (str): The name of the SQLite database file. Defaults to "journal.db".

#     Returns:
#         list: A list of tables in the database.

#     Raises:
#         None

#     """
    
#     if not Path(databasename).exists():
#         print(f"ERROR: No journal exists for filename {databasename}")
#         return None
    
#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()
    
#     # List tables in the SQLite database
#     tables = [c[0] for c in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
    
#     if tables:
#         print("INFO: Journals in the database:")
#         for table in tables:
#             if table.startswith("journal"):
#                 print("INFO:  ", table)
#     else:
#         print("INFO: No tables found in the database.")
    
#     con.close()

#     return tables


# def delete_journal(databasename: str, suffix:str = None):
#     if (suffix is None) or (suffix == "") :
#         print("ERROR: you must specify a suffix for the 'journal' table.")
#         return None
    
#     if not Path(databasename).exists():
#         # os.remove(pushdir_name + ".db")
#         print(f"ERROR: No journal exists for filename {databasename}")
#         return None

#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()

#     # check if the target table exists
#     journalname = "journal_" + suffix

#     cur.execute(f"DROP TABLE IF EXISTS {journalname}")

#     print("NOTE: Dropped table ", journalname)

#     con.close()
    
#     return journalname


    
# # this should only be called when an upload is initiated.
# def backup_journal(databasename: str, suffix:str = None):
#     """
#     Backs up the database journal table to a new table with a given suffix.

#     Args:
#         databasename (str): The name of the database file. Defaults to "journal.db".
#         suffix (str, optional): The suffix to be added to the backup table name. Defaults to None.

#     Returns:
#         str: The name of the backup table.

#     Raises:
#         None

#     """
#     if not Path(databasename).exists():
#         # os.remove(pushdir_name + ".db")
#         print(f"ERROR: No journal exists for filename {databasename}")
#         return None

#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()
    
#     if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='journal'").fetchone():
#         print(f"ERROR: table journal does not exist to backup.")
#         con.close()
#         return None

#     suff = suffix if suffix is not None else strftime("%Y%m%d%H%M%S", gmtime())

#     filesbackup = "journal_" + suff
#     cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
#     cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM journal")

#     print("INFO: backed up database journal to ", filesbackup)

#     con.close()

#     return filesbackup

    
# this should only be called when an upload is initiated.
# def restore_journal(databasename: str, suffix:str = None):
#     """
#     Restores the database journal from a backup table.

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
    
#     if not Path(databasename).exists():
#         # os.remove(pushdir_name + ".db")
#         print(f"ERROR: No journal exists for filename {databasename}")
#         return

#     con = sqlite3.connect(databasename, check_same_thread=False)
#     cur = con.cursor()

#     # check if the target table exists
#     journalname = "journal_" + suffix
#     if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{journalname}'").fetchone():
#         print(f"ERROR:  backup table {journalname} does not exist.")
#         con.close()
#         return

#     # do not backup, similar to git checkout - current changes are lost if not committed.
#     # backup_name = backup_journal(databasename)
    
#     # then restore
#     cur.execute(f"DROP TABLE IF EXISTS journal")
#     cur.execute(f"CREATE TABLE journal AS SELECT * FROM {journalname}")

#     print("INFO: restored database journal from ", journalname)

#     con.close()
#     return journalname

