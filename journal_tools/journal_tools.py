import sqlite3
import os

def list_uploads(databasename="journal.db"):
    """
    Retrieve a list of unique upload dates from the journal database.

    Args:
        databasename (str): The name of the journal database file. Defaults to "journal.db".

    Returns:
        list: A list of unique upload dates from the journal database.

    Raises:
        None

    """
    
    if not os.path.exists(databasename):
        print(f"ERROR: No journal exists for filename {databasename}")
        return None
    
    con = sqlite3.connect(databasename)
    cur = con.cursor()
    
    # List unique values in version column in journal table
    uploads = [u[0] for u in cur.execute("SELECT DISTINCT version FROM journal;").fetchall()]
    con.close()
    
    if (uploads is not None) and (len(uploads) > 0):
        print("INFO: upload records in the database:")
        for table in uploads:
            print("INFO:  ", table)
    else:
        print("INFO: No uploads found in the database.")
    
    return uploads

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
    
    if not os.path.exists(databasename):
        print(f"ERROR: No journal exists for filename {databasename}")
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

def restore_journal(databasename="journal.db", suffix:str = None):
    """
    Restores the database journal from a backup table.

    Args:
        databasename (str, optional): The name of the database file. Defaults to "journal.db".
        suffix (str, optional): The suffix of the backup table. If not specified, an error message is printed.

    Returns:
        str: The name of the restored backup table.

    Raises:
        None

    """
    if suffix is None:
        print("ERROR: previous version not specified.  please supply a suffix.")
        return
    
    if not os.path.exists(databasename):
        # os.remove(pushdir_name + ".db")
        print(f"ERROR: No journal exists for filename {databasename}")
        return

    con = sqlite3.connect(databasename)
    cur = con.cursor()

    # check if the target table exists
    journalname = "journal_" + suffix
    if not cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{journalname}'").fetchone():
        print(f"ERROR:  backup table {journalname} does not exist.")
        con.close()
        return

    # do not backup, similar to git checkout - current changes are lost if not committed.
    # backup_name = backup_journal(databasename)
    
    # then restore
    cur.execute(f"DROP TABLE IF EXISTS journal")
    cur.execute(f"CREATE TABLE journal AS SELECT * FROM {journalname}")

    print("INFO: restored database journal from ", journalname)

    con.close()
    return journalname