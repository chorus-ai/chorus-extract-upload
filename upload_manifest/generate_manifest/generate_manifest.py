import os
import sqlite3
import os
import glob
import hashlib
from datetime import datetime

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