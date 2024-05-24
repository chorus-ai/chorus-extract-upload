import os
import sqlite3
import os
import glob
import hashlib
from datetime import datetime
import pandas as pd


# TODO: DONE - Remove registry
# TODO: DONE - Change schema of files to include modtime, filesize
# TODO: Change matching to look at all three (name, time, size) 
# TODO: calculate MD5 for all non-matching files
# TODO: DONE Set "ready-to-sync" field for all non-matches
# TODO: Update to use paths instead of filenames
# TODO: Datetime format

# QUESTION: Datetimes local or unix?

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

    cur.execute("CREATE TABLE IF NOT EXISTS manifest(\
                FILE_ID INTEGER NOT NULL PRIMARY KEY, \
                PERSON_ID INTEGER, \
                FILENAME TEXT, \
                MODTIME TEXT, \
                SIZE INTEGER, \
                MD5 TEXT, \
                TIME_VALID TEXT, \
                TIME_INVALID TEXT, \
                SYNC_READY INTEGER)")
        
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
        statinfo = os.stat(path)
        filesize = statinfo.st_size
        modtimestamp = statinfo.st_mtime
        moddatetime = pd.to_datetime(modtimestamp, unit='s')
        moddt_string = moddatetime.strftime("%Y%m%d_%H%M%S")
        mymd5 = hashlib.md5(open(path, 'rb').read()).hexdigest()
        # myargs = (i_path, personid,i_path, mydate,mytime, "",1)
        # print(myargs)
        # print([type(item) for item in myargs])        
        myargs = (i_path, personid,filename,moddt_string,filesize, mymd5,dt_string, None,1)
        cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])

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

    filesbackup = "manifest_" + dt_nosymbols
    cur.execute(f"DROP TABLE IF EXISTS {filesbackup}")
    cur.execute(f"CREATE TABLE {filesbackup} AS SELECT * FROM manifest")

    i_file = cur.execute("SELECT MAX(file_id) from manifest").fetchone()[0] + 1
    # print(i_file)

    # get all active files, these are active-pending-delete until the file is found in filesystem
    activefiletuple = cur.execute("Select file_id from manifest where TIME_INVALID IS NULL").fetchall()
    
    activefileset = {i[0] for i in activefiletuple}

    cur.execute("UPDATE manifest SET SYNC_READY=?", (0,))

    for i_path, path in enumerate(paths):
        # print(path)
        pathstem = os.path.split(path)[0]
        filename = os.path.split(path)[1]
        personid = os.path.split(os.path.split(pathstem)[0])[1]
        statinfo = os.stat(path)
        filesize = statinfo.st_size
        modtimestamp = statinfo.st_mtime
        moddatetime = pd.to_datetime(modtimestamp, unit='s')
        moddt_string = moddatetime.strftime("%Y%m%d_%H%M%S")
        

        # query files for filename
        filecheckrow = cur.execute("Select file_id, md5 from manifest where filename=? and modtime=? and size=? and TIME_INVALID IS NULL",(filename,moddt_string,filesize))
        results = filecheckrow.fetchall()
        if len(results) > 1:
            print("Multiple active files with that filename - can't compare to old files")
            print("Exiting update...")
            return
        elif len(results) == 1:
            results = results[0]
            oldfileid = results[0]
            # oldmd5 = results[1]
            

            # if exists and md5 is different, invalidate existing file_id
            # then add new file like above, using new id, increment i_file
            # keep record the same
            # if oldmd5 != mymd5:
            #     cur.execute("UPDATE manifest SET TIME_INVALID=? WHERE file_id=?", (dt_string, oldfileid,))
            #     myargs = (i_file, personid,filename,moddt_string,filesize, mymd5,dt_string, None,1)
            #     cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            #     i_file += 1
            #     activefileset.remove(oldfileid)
            # # if exists and md5 is same, remove from active-pending-delete list
            # else:
            #     activefileset.remove(oldfileid)
            activefileset.remove(oldfileid)
        # if DNE, add new file
        else:
            mymd5 = hashlib.md5(open(path, 'rb').read()).hexdigest()
            myargs = (i_file, personid,filename,moddt_string,filesize, mymd5,dt_string, None,1)
            cur.executemany("INSERT INTO manifest VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [myargs])
            i_file += 1

    # for all files that were active but aren't there anymore
    # invalidate in manifest
    # for file_id,record_id in zip(activefilemap.keys(),activefilemap.values()):
    myargs = map(lambda v: (dt_string,v), activefileset)
    cur.executemany("UPDATE manifest SET TIME_INVALID=? WHERE file_id=?",myargs)


    con.commit() 
    con.close()