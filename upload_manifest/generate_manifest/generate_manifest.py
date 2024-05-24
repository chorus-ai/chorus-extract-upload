import os
import sqlite3
import os
import glob
import hashlib
from datetime import datetime, timezone

#registry will only hold target files
# upload will need to handle source and target(transformed) files.
# source to taregt transofmr may be m to n.

# do we need date/time?  


def get_file_metadata(file_path: str | os.PathLike) -> dict:
    """
    Get the metadata of a file.

    :param file_path: The path to the file.
    :type file_path: str | os.PathLike
    :return: A dictionary containing the metadata of the file.
    :rtype: dict
    """
    # Initialize the dictionary to store the metadata
    metadata = {}

    fn = os.path.abspath(file_path)
    
    # get the file path
    metadata["path"] = os.path.dirname(fn)
    
    # Get the file name
    metadata["name"] = os.path.basename(fn)

    # Get the file size
    metadata["size"] = os.path.getsize(fn)

    # Get the last modified time of the file
    metadata["last_modified"] = os.path.getmtime(fn)

    return metadata

def get_file_md5(file_path: str | os.PathLike) -> str:
    """
    Get the MD5 hash of a file.

    :param file_path: The path to the file.
    :type file_path: str | os.PathLike
    :return: The MD5 hash of the file.
    :rtype: str
    """
    # Initialize the MD5 hash object
    md5 = hashlib.md5()

    # Open the file in binary mode
    with open(file_path, "rb") as f:
        # Read the file in chunks
        for chunk in iter(lambda: f.read(4096), b""):
            # Update the MD5 hash with the chunk
            md5.update(chunk)

    # Get the hexadecimal representation of the MD5 hash
    md5_hash = md5.hexdigest()

    return md5_hash

def files_are_same(file1: str | os.PathLike, file2: str | os.PathLike) -> bool:
    meta1 = get_file_metadata(file1)
    meta2 = get_file_metadata(file2)

    if (meta1["path"] == meta2["path"]) and (meta1["name"] == meta2["name"]) and (meta1["size"] == meta2["size"]) and (meta1["last_modified"] == meta2["last_modified"]):
        return True
        
    md5_1 = get_file_md5(file1)
    md5_2 = get_file_md5(file2)
    
    if (md5_1 == md5_2):
        return True
    
    return False


def file_unchanged(old_meta: dict, old_md5: str, file: str | os.PathLike) -> bool:
    new_meta = get_file_metadata(file)
    
    if (old_meta["path"] == new_meta["path"]) and (old_meta["name"] == new_meta["name"]) and (old_meta["size"] == new_meta["size"]) and (old_meta["last_modified"] == new_meta["last_modified"]):
        return True
    
    new_md5 = get_file_md5(file)
    
    if (old_md5 == new_md5):
        return True
    
    return False


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

    cur.execute("CREATE TABLE IF NOT EXISTS registry(\
                RECORD_ID INTEGER NOT NULL PRIMARY KEY, \
                PERSON_ID INTEGER, \
                SESSION_ID INTEGER, \
                DATE TEXT, \
                TIME TEXT, \
                UPLOADDIR TEXT, \
                ISACTIVE INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS source_files(\
                FILE_ID INTEGER NOT NULL PRIMARY KEY, \
                RECORD_ID INTEGER, \
                PERSON_ID INTEGER, \
                SRCFILE TEXT, \
                MD5 TEXT, \
                TIME_VALID TEXT, \
                TIME_INVALID TEXT)")
        
    globpath = os.path.join(folderpath, "*", "Waveforms","*")
    # print(globpath)
    paths = glob.glob(globpath)
    # print(paths[0])

    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d_%H:%M:%S")

    for i_path, path in enumerate(paths):
        # print(path)
        pathstem = os.path.split(path)[0]
        filename = os.path.split(path)[1]
        personid = os.path.split(os.path.split(pathstem)[0])[1]
        mydate = filename.split("_")[1]
        mytime = filename.split("_")[2]
        mymd5 = hashlib.md5(open(path, 'rb').read()).hexdigest()
        myargs = (i_path, personid,i_path, mydate,mytime, "",1)
        print(myargs)
        # print([type(item) for item in myargs])        
        cur.executemany("INSERT INTO registry VALUES(?, ?, ?, ?, ?, ?, ?)", [myargs])
        myargs = (i_path,i_path, personid,filename, mymd5,dt_string, None)
        cur.executemany("INSERT INTO source_files VALUES(?, ?, ?, ?, ?, ?, ?)", [myargs])

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

    globpath = os.path.join(folderpath, "*", "Waveforms","*")
    # print(globpath)
    paths = glob.glob(globpath)
    # print(paths[0])

    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d_%H:%M:%S")
    dt_nosymbols = now.strftime("%Y%m%d_%H%M%S")
    registrybackup = "registry_" + dt_nosymbols
    cur.execute(f"DROP TABLE IF EXISTS {registrybackup}")
    cur.execute(f"CREATE TABLE {registrybackup} AS SELECT * FROM registry")

    filesbackup = "source_files_" + dt_nosymbols
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM source_files")

    i_file = cur.execute("SELECT MAX(file_id) from source_files").fetchone()[0] + 1
    print(i_file)
    i_record = cur.execute("SELECT MAX(record_id) from registry").fetchone()[0] + 1
    print(i_record)

    # get all active files, these are active-pending-delete until the file is found in filesystem
    activefiletuple = cur.execute("Select file_id, record_id from source_files where TIME_INVALID IS NULL").fetchall()
    
    activefilemap = {i[0]:i[1] for i in activefiletuple}
    for i_path, path in enumerate(paths):
        print(path)
        pathstem = os.path.split(path)[0]
        filename = os.path.split(path)[1]
        personid = os.path.split(os.path.split(pathstem)[0])[1]
        mydate = filename.split("_")[1]
        mytime = filename.split("_")[2]
        mymd5 = hashlib.md5(open(path, 'rb').read()).hexdigest()

        # query files for filename
        filecheckrow = cur.execute("Select file_id, md5, record_id from source_files where srcfile=? and TIME_INVALID IS NULL",(filename,))
        results = filecheckrow.fetchall()
        if len(results) > 1:
            print("Multiple active files with that filename - can't compare to old files")
            print("Exiting update...")
            return
        elif len(results) == 1:
            results = results[0]
            oldfileid = results[0]
            oldmd5 = results[1]
            oldrecord = results[2]

            # if exists and md5 is different, invalidate existing file_id
            # then add new file like above, using new id, increment i_file
            # keep record the same
            if oldmd5 != mymd5:
                cur.execute("UPDATE source_files SET TIME_INVALID=? WHERE file_id=?", (dt_string, oldfileid,))
                myargs = (i_file,oldrecord, personid,filename, mymd5,dt_string, None)
                cur.executemany("INSERT INTO source_files VALUES(?, ?, ?, ?, ?, ?, ?)", [myargs])
                i_file += 1
                del activefilemap[oldfileid]
            # if exists and md5 is same, remove from active-pending-delete list
            else:
                del activefilemap[oldfileid]
        # if DNE, add new file
        # add new record
        else:
            myargs = (i_record, personid,i_file, mydate,mytime, "",1)
            # print(myargs)
            # print([type(item) for item in myargs])        
            cur.executemany("INSERT INTO registry VALUES(?, ?, ?, ?, ?, ?, ?)", [myargs])
            myargs = (i_file,i_record, personid,filename, mymd5,dt_string, None)
            cur.executemany("INSERT INTO source_files VALUES(?, ?, ?, ?, ?, ?, ?)", [myargs])
            i_file += 1
            i_record += 1

    # for all files that were active but aren't there anymore
    # invalidate in source_files, and invalidate in registry
    # for file_id,record_id in zip(activefilemap.keys(),activefilemap.values()):
    cur.executemany("UPDATE registry SET ISACTIVE=0 WHERE record_id=?",tuple([(i,) for i in activefilemap.values()]))
    myargs = map(lambda v: (dt_string,v), activefilemap.keys())
    cur.executemany("UPDATE source_files SET TIME_INVALID=? WHERE file_id=?",myargs)


    con.commit() 
    con.close()
    
    
def get_local_paths(folderpath) -> list[str | os.PathLike]:
    # patient_id/type/files
    waveform_path = os.path.join(folderpath, "*", "Waveforms","*")
    image_path = os.path.join(folderpath, "*", "Images","*")
    # print(globpath)
    paths1 = glob.glob(waveform_path)
    paths2 = glob.glob(image_path)
    # # omop/files - always uploaded. so probably don't need to be included.
    # omop_path = os.path.join(folderpath, "OMOP", "*")
    # paths3 = glob.glob(omop_path)
    return [*paths1, *paths2]

# current assumptions/working decisions for initial run:
# TONY:  working with subdirectories of form:  pid/type/files
# 1 file per record - can be extended to 1 dir per record or using a file-record mapping
# no upload dir specified on local site's side
# entity relationships: many sessions per person, many records per session, many files per record
# TONY removed this assumption: files have a format of PERSONID_STARTDATE_STARTTIME_LENGTH.extension

# is Session_ID used to map to an manifest generation/update session?
# [x] NOT NEEDED- always have to compute md5 to update the database, or to check if file is same (even if size and date are same). 
# [x]: keep a synced flag.  this will allow multiple manifest updates to be bundled easily.  synced flag can be turned off after syncing.
# [x]: is the file naming convention appropriate?  there is no provision for multiple files in a directory with same startdate/time/length?
# [ ]: need support for cloud files.



def gen_manifest2(folderpath, databasename="journal.db"):
    # print("Starting Manifest")
    if os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print("Initial manifest, but a journal with that name already exists. Please delete and re-run.")
        exit()
    con = sqlite3.connect(databasename)

    cur = con.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS registry(\
                RECORD_ID INTEGER NOT NULL PRIMARY KEY, \
                PERSON_ID INTEGER, \
                SESSION_ID INTEGER, \
                DATE TEXT, \
                TIME TEXT, \
                SIZE INTEGER, \
                UPLOADDIR TEXT, \
                ISACTIVE INTEGER, \
                SYNCED INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS source_files(\
                FILE_ID INTEGER NOT NULL PRIMARY KEY, \
                RECORD_ID INTEGER, \
                PERSON_ID INTEGER, \
                SRCFILE TEXT, \
                MD5 TEXT, \
                TIME_VALID TEXT, \
                TIME_INVALID TEXT, \
                SYNCED INTEGER)")
        
    paths = get_local_paths(folderpath)
    # print(paths[0])

    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d_%H:%M:%S")

    for i_path, path in enumerate(paths):
        # print(path)
        
        meta = get_file_metadata(path)
        filename = meta['name']
        dir = meta['path']
        personid = os.path.basename(os.path.dirname(dir))
        mydt = datetime.fromtimestamp(meta['last_modified'], tz=timezone.utc)
        mysize = meta['size']
        mydate = mydt.strftime("%Y-%m-%d")
        mytime = mydt.strftime("%H:%M:%S")
        mymd5 = get_file_md5(path)     
        
        myargs = (i_path, personid, i_path, mydate, mytime, mysize, "", 1, 0)
        print(myargs)
        # print([type(item) for item in myargs])        
        cur.executemany("INSERT INTO registry VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
        myargs = (i_path, i_path, personid, filename, mymd5, dt_string, None, 0)
        cur.executemany("INSERT INTO source_files VALUES(?, ?, ?, ?, ?, ?, ?, ?)", [myargs])

    con.commit() 
    con.close()



# record is a mess with a file-wise traversal. Files need to be grouped by unique record ID (visit_occurence?)
def update_manifest2(folderpath, databasename="journal.db"):
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"No manifest exists for filename {databasename}")
        return
    con = sqlite3.connect(databasename)

    cur = con.cursor()

    paths = get_local_paths(folderpath)
    # print(paths[0])

    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d_%H:%M:%S")
    dt_nosymbols = now.strftime("%Y%m%d_%H%M%S")
    registrybackup = "registry_" + dt_nosymbols
    cur.execute(f"DROP TABLE IF EXISTS {registrybackup}")
    cur.execute(f"CREATE TABLE {registrybackup} AS SELECT * FROM registry")

    filesbackup = "source_files_" + dt_nosymbols
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM source_files")

    i_file = cur.execute("SELECT MAX(file_id) from source_files").fetchone()[0] + 1
    print(i_file)
    i_record = cur.execute("SELECT MAX(record_id) from registry").fetchone()[0] + 1
    print(i_record)

    # get all active files, these are active-pending-delete until the file is found in filesystem
    activefiletuple = cur.execute("SELECT file_id, record_id from source_files where TIME_INVALID IS NULL").fetchall()
    
    activefilemap = {i[0]:i[1] for i in activefiletuple}
    # process local files
    for i_path, path in enumerate(paths):
        print(path)
        
        meta = get_file_metadata(path)
        filename = meta['name']
        dir = meta['path']
        personid = os.path.basename(os.path.dirname(dir))
        mydt = datetime.fromtimestamp(meta['last_modified'], tz=timezone.utc)
        mysize = meta['size']
        mydate = mydt.strftime("%Y-%m-%d")
        mytime = mydt.strftime("%H:%M:%S")
        mymd5 = get_file_md5(path)     

        # query files for filename
        # really need date and time...
        filecheckrow = cur.execute("Select file_id, md5, record_id from source_files where srcfile=? and TIME_INVALID IS NULL and SYNCED=0",(filename,))
        results = filecheckrow.fetchall()
        if len(results) > 1:
            print("Multiple active files with that filename - can't compare to old files")
            print("Exiting update...")
            return
        elif len(results) == 1:
            results = results[0]
            oldfileid = results[0]
            oldmd5 = results[1]
            oldrecord = results[2]
            
            # if exists and md5 is different, invalidate existing file_id
            # then add new file like above, using new id, increment i_file
            # keep record the same
            if (oldmd5 != mymd5):
                cur.execute("UPDATE source_files SET TIME_INVALID=? WHERE file_id=?", (dt_string, oldfileid,))
                myargs = (i_file, oldrecord, personid, filename, mymd5, dt_string, None, 0)
                cur.executemany("INSERT INTO source_files VALUES(?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
                i_file += 1
            
            del activefilemap[oldfileid]
                
            
        # if DNE, add new file
        # add new record
        else:
            myargs = (i_record, personid,i_file, mydate, mytime, mysize, "", 1, 0)
            # print(myargs)
            # print([type(item) for item in myargs])        
            cur.executemany("INSERT INTO registry VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            myargs = (i_file, i_record, personid, filename, mymd5, dt_string, None, 0)
            cur.executemany("INSERT INTO source_files VALUES(?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            i_file += 1
            i_record += 1

    # for all files that were active but aren't there anymore
    # invalidate in source_files, and invalidate in registry
    # for file_id,record_id in zip(activefilemap.keys(),activefilemap.values()):
    cur.executemany("UPDATE registry SET ISACTIVE=0 WHERE record_id=?",tuple([(i,) for i in activefilemap.values()]))
    myargs = map(lambda v: (dt_string,v), activefilemap.keys())
    cur.executemany("UPDATE source_files SET TIME_INVALID=? WHERE file_id=?",myargs)


    con.commit() 
    con.close()