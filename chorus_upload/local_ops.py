import sqlite3
import re
import time
import math
import os
from typing import Optional, List
from chorus_upload.storage_helper import FileSystemHelper
from pathlib import Path
from chorus_upload.defaults import DEFAULT_MODALITIES
import concurrent.futures


def update_journal(root : FileSystemHelper, modalities: list[str] = DEFAULT_MODALITIES,
                   databasename="journal.db", 
                   version: str = None,
                   amend: bool = False,
                   verbose: bool = False):
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    # check if journal table exists in sqlite
    table_exists = cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='journal'").fetchone()
    con.close()

    # check if journal table exists
    if (table_exists is not None) and (table_exists[0] == "journal"):
        return _update_journal(root, modalities, databasename, version = version, amend = amend, verbose= verbose)
    else:  # no amend possible since the file did not exist.
        return _gen_journal(root, modalities, databasename, version = version, verbose = verbose)
        
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
                
    myargs = (personid, relpath, modality, modtimestamp, filesize, mymd5, curtimestamp, "ADDED", md5_time, version)

    return myargs



def _gen_journal(root : FileSystemHelper, modalities: list[str] = DEFAULT_MODALITIES, 
                  databasename="journal.db", 
                  version:str = None, 
                  verbose: bool = False):
    """
    Generate a journal for the given root directory and subdirectories.

    Args:
        root (FileSystemHelper): The root directory to generate the journal for.
        modalities (list[str], optional): The subdirectories to include in the journal. Defaults to ['Waveforms', 'Images', 'OMOP'].
        databasename (str, optional): The name of the database to store the journal. Defaults to "journal.db".
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """
    print("INFO: Creating journal. NOTE: this may take a while for md5 computation.", flush=True)
    
    # TODO: set the version to either the supplied version name, or with current time.
    new_version = version if version is not None else time.strftime("%Y%m%d%H%M%S")
        
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS journal(\
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
                VERSION TEXT, \
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
            
        
        nthreads = max(1, os.cpu_count() - 2)
        page_size = 1000
        total_count = 0
        with concurrent.futures.ThreadPoolExecutor() as executor:

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
                    rlpath = myargs[1]
                    status = myargs[7]
                    if verbose and status == "ADDED":
                        print("INFO: ADDED ", rlpath)
                    else:
                        print(".", end="", flush=True)
                        
                    all_args.append(myargs)

                con = sqlite3.connect(databasename, check_same_thread=False)
                cur = con.cursor()

                try:
                    cur.executemany("INSERT INTO journal ( \
                        PERSON_ID, \
                        FILEPATH, \
                        MODALITY, \
                        SRC_MODTIME_us, \
                        SIZE, \
                        MD5, \
                        TIME_VALID_us, \
                        STATE, \
                        MD5_DURATION, \
                        VERSION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)
                except sqlite3.IntegrityError as e:
                    print("ERROR: create ", e)
                    print(all_args)
                        
                con.commit() 
                con.close()
                total_count += len(all_args)
                print("INFO: Added ", total_count, " files to journal.db" )
                all_args = []
        
        print("INFO: Journal Update took ", time.time() - start, "s")
        
        # paths = root.get_files(pattern)  # list of relative paths in posix format and in string form.
        # # paths += paths1
        # print("Get File List took ", time.time() - start)


        # nthreads = max(1, os.cpu_count() - 2)
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     futures = []
        #     for relpath in paths:
        #         if verbose:
        #             print("INFO: scanning ", relpath)
        #         future = executor.submit(_gen_journal_one_file, root, relpath, modality, curtimestamp)
        #         futures.append(future)
            
        #     for future in concurrent.futures.as_completed(futures):
        #         myargs = future.result()
        #         rlpath = myargs[1]
        #         status = myargs[7]
        #         if verbose and status == "ADDED":
        #             print("INFO: ADDED ", rlpath)
        #         else:
        #             print(".", end="", flush=True)
                    
        #         all_args.append(myargs)

        #         if len(all_args) >= 1000:
        #             con = sqlite3.connect(databasename, check_same_thread=False)
        #             cur = con.cursor()

        #             try:
        #                 cur.executemany("INSERT INTO journal ( \
        #                     PERSON_ID, \
        #                     FILEPATH, \
        #                     MODALITY, \
        #                     SRC_MODTIME_us, \
        #                     SIZE, \
        #                     MD5, \
        #                     TIME_VALID_us, \
        #                     STATE, \
        #                     MD5_DURATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)
        #             except sqlite3.IntegrityError as e:
        #                 print("ERROR: create ", e)
        #                 print(all_args)
                            
        #             con.commit() 
        #             con.close()
        #             print("INFO: Added ", len(all_args), " files to journal.db" )
        #             all_args = []


        # start = time.time()
        # if len(all_args) > 0:
        #     con = sqlite3.connect(databasename, check_same_thread=False)
        #     cur = con.cursor()

        #     try:
        #         cur.executemany("INSERT INTO journal ( \
        #             PERSON_ID, \
        #             FILEPATH, \
        #             MODALITY, \
        #             SRC_MODTIME_us, \
        #             SIZE, \
        #             MD5, \
        #             TIME_VALID_us, \
        #             STATE, \
        #             MD5_DURATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", all_args)
        #     except sqlite3.IntegrityError as e:
        #         print("ERROR: create ", e)
        #         print(all_args)
                    
        #     con.commit() 
        #     con.close()
        # print("SQLITE insert took ", time.time() - start)

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
def _update_journal(root: FileSystemHelper, modalities: list[str] = DEFAULT_MODALITIES,
                     databasename="journal.db", 
                     version: str = None,
                     amend:bool = False, 
                     verbose: bool = False):
    """
    Update the journal database with the files found in the specified root directory.

    Args:
        root (FileSystemHelper): The root directory to search for files.
        modalities (list[str], optional): The subdirectories to search for files. Defaults to ['Waveforms', 'Images', 'OMOP'].
        databasename (str, optional): The name of the journal database. Defaults to "journal.db".
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """
    print("INFO: Updating journal...", databasename, flush=True)

    # TODO:  
    # if amend, get the last version, and add files to that version.
    # if not amend, create a new version - either with supplied version name, or with current time.
    if amend: # get the last version
        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()
        last_version = cur.execute("SELECT MAX(VERSION) from journal").fetchone()[0]
        con.close()
        version = last_version if last_version is not None else version
    else:
        version = version if version is not None else time.strftime("%Y%m%d%H%M%S")
    
    
    # check if journal table exists.
    curtimestamp = int(math.floor(time.time() * 1e6))

    modalities = modalities if modalities else DEFAULT_MODALITIES
    
    page_size = 1000
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
            

        con = sqlite3.connect(databasename, check_same_thread=False)
        cur = con.cursor()
        # get all active files, these are active-pending-delete until the file is found in filesystem
        activefiletuples = cur.execute(f"Select filepath, file_id, size, src_modtime_us, md5, upload_dtstr, version from journal where TIME_INVALID_us IS NULL and MODALITY = '{modality}'").fetchall()
        con.close()

        # initialize by putting all files in files_to_inactivate.  remove from this list if we see the files on disk
        modality_files_to_inactivate = {}
        for i in activefiletuples:
            fpath = i[0]
            if fpath not in modality_files_to_inactivate.keys():
                modality_files_to_inactivate[fpath] = [ (i[1], i[2], i[3], i[4], i[5], i[6]) ]
            else:
                modality_files_to_inactivate[fpath].append((i[1], i[2], i[3], i[4], i[5], i[6]))

        nthreads = max(1, os.cpu_count() - 2)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            
            for paths in root.get_files_iter(pattern, page_size = page_size ):  # list of relative paths in posix format and in string form.
                futures = []

                for relpath in paths:
                    if verbose:
                        print("INFO: scanning ", relpath)
                    future = executor.submit(_update_journal_one_file, root, relpath, modality, curtimestamp, modality_files_to_inactivate, version)
                    futures.append(future)
                
                for future in concurrent.futures.as_completed(futures):
                    myargs = future.result()
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
                con = sqlite3.connect(databasename, check_same_thread=False)
                cur = con.cursor()

                try:
                    cur.executemany("INSERT INTO journal (\
                        PERSON_ID, \
                        FILEPATH, \
                        MODALITY, \
                        SRC_MODTIME_us, \
                        SIZE, \
                        MD5, \
                        TIME_VALID_us, \
                        UPLOAD_DTSTR, \
                        STATE, \
                        MD5_DURATION, \
                        VERSION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", all_insert_args)
                except sqlite3.IntegrityError as e:
                    print("ERROR: update ", e)
                    print(all_insert_args)
                    
                con.commit() 
                con.close()
                total_count += len(all_insert_args)
                print("INFO: Added ", total_count, " files to journal.db" )
                all_insert_args = []

            # for all files that were active but aren't there anymore
            # invalidate in journal
            
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

            if (len(all_del_args) > 0):
                # back up only on upload
                # backup_journal(databasename)
                
                con = sqlite3.connect(databasename, check_same_thread=False)
                cur = con.cursor()

                try:
                    if (len(all_del_args) > 0):
                        cur.executemany("UPDATE journal SET TIME_INVALID_us=?, STATE=? WHERE file_id=?", all_del_args)
                        
                        print("Inactivated ", len(all_del_args), " from Journal")
                except sqlite3.IntegrityError as e:
                    print("ERROR: delete ", e)
                    print(all_del_args)
                        
                con.commit() 
                con.close()
            else:
                print("INFO: Nothing to change in journal for modality ", modality)
            
        print("INFO: Journal Update Elapsed time ", time.time() - start, "s")


        #     paths = root.get_files(pattern)  # list of relative paths in posix format and in string form.
            
        #     for relpath in paths:
        #         if verbose:
        #             print("INFO: scanning ", relpath)
        #         future = executor.submit(_update_journal_one_file, root, relpath, modality, curtimestamp, modality_files_to_inactivate)
        #         futures.append(future)
            
        #     for future in concurrent.futures.as_completed(futures):
        #         myargs = future.result()
        #         status = myargs[8]
        #         rlpath = myargs[1]
        #         if status == "ERROR1":
        #             print("ERROR: Multiple active files with that path - journal is not consistent", relpath)
        #         elif status == "ERROR2":
        #             print("ERROR: File found but no metadata.", rlpath)
        #         elif status == "ERROR3":
        #             print("ERROR: File size is different but modtime is the same.", rlpath)
        #         elif status == "KEEP":
        #             del modality_files_to_inactivate[myargs[1]]
        #         else:
        #             if verbose:
        #                 if status == "MOVED":
        #                     print("INFO: COPIED/MOVED ", rlpath)
        #                 elif status == "UPDATED":
        #                     print("INFO: UPDATED ", rlpath)
        #                 elif status == "ADDED":
        #                     print("INFO: ADDED ", rlpath)
        #             all_insert_args.append(myargs)
        

        #         if (len(all_insert_args) > 1000):
        #             # back up only on upload
        #             # backup_journal(databasename)
                    
        #             con = sqlite3.connect(databasename, check_same_thread=False)
        #             cur = con.cursor()

        #             try:
        #                 cur.executemany("INSERT INTO journal (\
        #                     PERSON_ID, \
        #                     FILEPATH, \
        #                     MODALITY, \
        #                     SRC_MODTIME_us, \
        #                     SIZE, \
        #                     MD5, \
        #                     TIME_VALID_us, \
        #                     UPLOAD_DTSTR, \
        #                     STATE, \
        #                     MD5_DURATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", all_insert_args)
        #             except sqlite3.IntegrityError as e:
        #                 print("ERROR: update ", e)
        #                 print(all_insert_args)
                        
        #             con.commit() 
        #             con.close()
        #             print("INFO: Added ", len(all_insert_args), " files to journal.db" )
        #             all_insert_args = []

        # # for all files that were active but aren't there anymore
        # # invalidate in journal
        
        # for relpath, vals in modality_files_to_inactivate.items():
        #     # check if there are non alphanumeric characters in the path
        #     # if so, print out the path
        #     if not (relpath.isalnum() or relpath.isascii()):
        #         print("INFO: Path contains non-alphanumeric characters:", relpath)
        #     if verbose:
        #         if (relpath in paths):
        #             print("INFO: OUTDATED  ", relpath)
        #         else:
        #             print("INFO: DELETED  ", relpath)
        #     state = "OUTDATED" if (relpath in paths) else "DELETED"
        #     for v in vals:
        #         all_del_args.append((curtimestamp, state, v[0]))
        # # print("SQLITE update arguments ", all_del_args)

        # if (len(all_insert_args) > 0) or (len(all_del_args) > 0):
        #     # back up only on upload
        #     # backup_journal(databasename)
            
        #     con = sqlite3.connect(databasename, check_same_thread=False)
        #     cur = con.cursor()

        #     try:
        #         if len(all_insert_args) > 0:
        #             cur.executemany("INSERT INTO journal (\
        #                 PERSON_ID, \
        #                 FILEPATH, \
        #                 MODALITY, \
        #                 SRC_MODTIME_us, \
        #                 SIZE, \
        #                 MD5, \
        #                 TIME_VALID_us, \
        #                 UPLOAD_DTSTR, \
        #                 STATE, \
        #                 MD5_DURATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", all_insert_args)
                    
        #     except sqlite3.IntegrityError as e:
        #         print("ERROR: update ", e)
        #         print(all_insert_args)

        #     try:
        #         if (len(all_del_args) > 0):
        #             cur.executemany("UPDATE journal SET TIME_INVALID_us=?, STATE=? WHERE file_id=?", all_del_args)
        #     except sqlite3.IntegrityError as e:
        #         print("ERROR: delete ", e)
        #         print(all_del_args)
                    
        #     con.commit() 
        #     con.close()
        # else:
        #     print("INFO: Nothing to change in journal for modality ", modality)
        


# all files to upload are marked with upload_dtstr = NULL. 
# return list of files organized by version
def list_files(databasename="journal.db", version: Optional[str] = None, 
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
        
        
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='journal'").fetchone():
        print(f"ERROR: table journal does not exist.")
        con.close()
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
    
    if (modalities is None) or (len(modalities) == 0):    
        # select where upload_dtstr is NULL or time_invalid_us is NULL
        files_to_update = cur.execute(f"Select filepath, time_invalid_us, version from journal where {where_clause}").fetchall()
    else:
        files_to_update = []
        for modality in modalities:
            files_to_update += cur.execute(f"Select filepath, time_invalid_us, version from journal where {where_clause} AND MODALITY = '{modality}'").fetchall()
    # print(files_to_update)
    # only need to add, not delete.  rely on journal to mark deletion.
    con.close()
    
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
                active_files[version].add(fn)
            else:
                active_files[version] = set([fn])
        else:
             # old files or deleted files
            if (version in inactive_files.keys()):
                inactive_files[version].add(fn)
            else:
                inactive_files[version] = set([fn])
    
    all_active_files = set()
    all_inactive_files = set()
    for k in active_files.keys():
        all_active_files = set.union(all_active_files, active_files[k])
    for k in inactive_files.keys():
        all_inactive_files = set.union(all_inactive_files, inactive_files[k])
    updates = all_active_files.intersection(all_inactive_files)
    adds = all_active_files - updates
    deletes = all_inactive_files - updates
    if verbose:
        for f in adds:
            print("INFO: ADD ", f)
        for f in updates:
            print("INFO: UPDATE ", f)
        for f in deletes:
            print("INFO: DELETE ", f)
            
    # sort the file-list
    # file_list.sort()
    
    if len(all_active_files) == 0:
        print("INFO: No active files found")
    
    # do we need to delete files in central at all? NO.  journal will be updated.
    # we only need to add new files
                
    return active_files

def list_versions(databasename="journal.db"):
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
    
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    # List unique values in upload_dtstr column in journal table
    versions = [u[0] for u in cur.execute("SELECT DISTINCT version FROM journal;").fetchall()]
    con.close()
    
    if (versions is not None) and (len(versions) > 0):
        print("INFO: upload records in the database:")
        for table in versions:
            print("INFO:  ", table)
    else:
        print("INFO: No uploads found in the database.")
    
    return versions


def list_journals(databasename="journal.db"):
    """
    Lists the tables in the SQLite database specified by the given database name.

    Args:
        databasename (str): The name of the SQLite database file. Defaults to "journal.db".

    Returns:
        list: A list of tables in the database.

    Raises:
        None

    """
    
    if not Path(databasename).exists():
        print(f"ERROR: No journal exists for filename {databasename}")
        return None
    
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    # List tables in the SQLite database
    tables = [c[0] for c in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
    
    if tables:
        print("INFO: Journals in the database:")
        for table in tables:
            if table.startswith("journal"):
                print("INFO:  ", table)
    else:
        print("INFO: No tables found in the database.")
    
    con.close()

    return tables


# def delete_journal(databasename="journal.db", suffix:str = None):
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


    
# this should only be called when an upload is initiated.
def backup_journal(databasename="journal.db", suffix:str = None):
    """
    Backs up the database journal table to a new table with a given suffix.

    Args:
        databasename (str): The name of the database file. Defaults to "journal.db".
        suffix (str, optional): The suffix to be added to the backup table name. Defaults to None.

    Returns:
        str: The name of the backup table.

    Raises:
        None

    """
    if not Path(databasename).exists():
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No journal exists for filename {databasename}")
        return None

    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='journal'").fetchone():
        print(f"ERROR: table journal does not exist to backup.")
        con.close()
        return None

    suff = suffix if suffix is not None else strftime("%Y%m%d%H%M%S", gmtime())

    filesbackup = "journal_" + suff
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM journal")

    print("INFO: backed up database journal to ", filesbackup)

    con.close()

    return filesbackup

    
# this should only be called when an upload is initiated.
# def restore_journal(databasename="journal.db", suffix:str = None):
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

