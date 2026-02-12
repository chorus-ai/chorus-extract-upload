import sqlite3
from contextlib import closing
from enum import Enum
from pathlib import Path
import shutil
from chorus_upload.storage_helper import FileSystemHelper

import logging
log = logging.getLogger(__name__)



#TODO: incrementally process the parameters.
#TODO: check length of where clause.

class SQLiteDB:
    # set a class variable for verbosity
    chunk_size = 1000
    max_where_clause = 200000
    
    @classmethod
    def _make_select_stmt(cls, 
                         table_or_subquery:str, 
                         columns:list, 
                         where_clause:str = None):
        fields = ', '.join(columns) if (columns is not None) and (len(columns) > 0) else "*"
        where_str = "" if (where_clause is None) or (where_clause == "") else f" WHERE {where_clause}"
        return f"SELECT {fields} FROM {table_or_subquery}{where_str}"
    
    
    # # columns is a list of columns to retrieve from
    # # where_clause is a string in case  we have to deal with AND/OR, etc.
    # @classmethod
    # def query(cls, 
    #             database_name: str, 
    #             table_name:str, 
    #             columns : list, 
    #             where_clause: str = None,
    #             max_return:int = None):
        
    #     select_str = cls._make_select_stmt(table_name, columns, where_clause)

    #     if where_clause is not None and (len(where_clause) > cls.max_where_clause):
    #         log.warning(f"QUERY where clause is long")
        
    #     with sqlite3.connect(database_name, check_same_thread=False) as conn:
    #         with closing(conn.cursor()) as cur:
    #             res = cur.execute(select_str)
    #             if (max_return is None) or (max_return == 0):
    #                 # get all
    #                 vals = res.fetchall()
    #             elif max_return == 1:
    #                 vals = res.fetchone()
    #             else:
    #                 vals = res.fetchmany(max_return)
    #     return vals        

    # columns and groupby should use the same naming convention - both should be list of strings.
    @classmethod
    def query(cls,
                      database_name: str,
                      table_name: str,
                      columns : list, 
                      where_clause: str = None,
                      groupby : list = [],
                      max_return:int = None):
        
        cols = set(columns).union(set(groupby))
        
        select_str = cls._make_select_stmt(table_name, cols, where_clause)

        if where_clause is not None and (len(where_clause) > cls.max_where_clause):
            log.warning(f"QUERY where clause is long")
        
        if (groupby is not None) and (len(groupby) > 0):
            select_str += f" GROUP BY {','.join(groupby)}"
        
        print(select_str)
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                res = cur.execute(select_str)
                if (max_return is None) or (max_return == 0):
                    # get all
                    vals = res.fetchall()
                elif max_return == 1:
                    vals = res.fetchone()
                else:
                    vals = res.fetchmany(max_return)
        return vals    
        

    @classmethod
    def _make_select_stmt_with_left_join(cls, 
                         table_or_subquery:str, 
                         columns:list, 
                         join_criteria: list = None,
                         where_clause:str = None):
        fields = []
        for (col, out) in columns:
            if out is None:
                fields.append(col)
            else:
                fields.append(f"{col} AS {out}")
        fields = ', '.join(fields)

        join_clause = ""
        for join in join_criteria:
            join_clause += f" LEFT JOIN {join[0]} ON {join[1]} = {join[2]}"

        where_str = "" if (where_clause is None) or (where_clause == "") else f" WHERE {where_clause}"
        return f"SELECT {fields} FROM {table_or_subquery} {join_clause}{where_str}"
    
    @classmethod
    def query_with_left_join(cls,
                        database_name: str,
                        table_name: str,
                        columns: list,
                        join_criteria: list = None,
                        where_clause: str = None,
                        max_return: int = None):
        """
        Query the database with a JOIN clause.
        Parameters:
        - database_name (str): The name of the database file to connect to.
        - table_name (str): The name of the table to query.
        - columns (list): The columns to retrieve from the table, in the form of (table.column, output name).
        - join_criteria (list of tuples, optional): The JOIN criteria - tuples of parent table, childtable foreign key, parent table  key.
        - where_clause (str, optional): The WHERE clause to use in the query.  operates on joined data
        - max_return (int, optional): The number of rows to retrieve from the query.
        Returns:
        - list: A list of tuples containing the query results.
        """
        
        select_stmt = cls._make_select_stmt_with_left_join(table_name, columns, join_criteria, where_clause)
        if where_clause is not None and (len(where_clause) > cls.max_where_clause):
            log.warning(f"QUERY with LEFT JOIN where clause is long")

        print(select_stmt)

        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                res = cur.execute(select_stmt)
                
                if (max_return is None) or (max_return == 0):
                    # get all
                    vals = res.fetchall()
                elif max_return == 1:
                    vals = res.fetchone()
                else:
                    vals = res.fetchmany(max_return)
        return vals

    # column_types is a list of tuples of form (column_name, column_type, column property)
    # index_on is a list of column names
    @classmethod
    def create_table(cls, database_name: str,
                     table_name : str, 
                     column_types: list[tuple],
                     foreign_keys: list = None,
                     index_on: list = None):
        if (column_types is None) or (len(column_types) == 0):
            return
        
        columns = ', '.join([f"{k} {v} {prop}" for (k, v, prop) in column_types])
        foreigns = ', '.join([f"FOREIGN KEY ({lid}) REFERENCES {tab}({fid}) ON UPDATE RESTRICT ON DELETE RESTRICT" for (lid, tab, fid) in foreign_keys]) if foreign_keys is not None else ""
        indexes = ', '.join(index_on) if index_on is not None else ""
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                if foreign_keys is not None:
                    #log.debug(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}, {foreigns})")
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns}, {foreigns})")
                else:
                    #log.debug(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
                if index_on is not None:
                    cur.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_idx ON {table_name} ({indexes})")
            conn.commit()
        
    # create a lookup table.  
    @classmethod
    def create_lookup_table(cls, database_name: str, table_name: str, column_name: str):
        SQLiteDB.create_table(
            database_name = database_name,
            table_name = table_name, 
            column_types = [
                ("id", "INTEGER", "PRIMARY KEY AUTOINCREMENT"), 
                (f"{column_name}", "TEXT", "UNIQUE"), # matches the cloud directory. required
                ],
            foreign_keys=None,
            index_on = [column_name, ])

    # insert and retrieve lookup table value,
    @classmethod
    def insert_lookup_table(cls, database_name: str, table_name: str, column_name: str, lookup: set):
        # if lookup is empty, return the whole lookup table.
        
        if lookup is not None and len(lookup) > 0:
            lookup_list = list(lookup)
            for i in range(0, len(lookup_list), cls.chunk_size):
                batch = lookup_list[i:min(i + cls.chunk_size, len(lookup_list))]
                insert_args = [(val,) for val in batch]
                SQLiteDB.insert(database_name=database_name,
                        table_name=table_name,
                        columns=[column_name,],
                        params=insert_args,
                        unique=True)
        # query to fill the dictionary
        if lookup is not None and (len(lookup) > 0):
            value_ids = []
            for i in range(0, len(lookup), cls.chunk_size):
                batch = lookup_list[i:min(i + cls.chunk_size, len(lookup_list))]
                where_clause = column_name + " in (" + ",".join([f"'{val}'" for val in batch]) + ")"
                value_ids.extend(SQLiteDB.query(database_name = database_name,
                                                table_name = table_name,
                                                columns = ['id', column_name],
                                                where_clause = where_clause))
        else:
            value_ids = SQLiteDB.query(database_name = database_name,
                                        table_name = table_name,
                                        columns = ['id', column_name],
                                        where_clause = None)
        return {val: val_id for (val_id, val) in value_ids}


    # columns is a list of columns names
    # params is a list of tuples, each containing the same number of elements as columns
    # order matters.
    @classmethod
    def insert(cls, 
                       database_name: str, 
                       table_name: str,
                       columns: list, 
                       params: list, unique:bool = False) -> int:
        if (params is None) or (len(params) == 0):
            return 0, None
        
        fields = ', '.join(columns)
        placeholders = ', '.join('?' * len(columns))
        inserted = 0
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    if unique is not None and unique:
                        #log.debug(f"INSERT OR IGNORE INTO {table_name} ({fields}) VALUES ({placeholders})")
                        cur.executemany(f"INSERT OR IGNORE INTO {table_name} ({fields}) VALUES ({placeholders})", params)
                    else:
                        #log.debug(f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders})")
                        cur.executemany(f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders})", params)
                    inserted = cur.rowcount
                except sqlite3.IntegrityError as e:
                    log.error(f"insert {e}")
                    log.debug(params)
                except sqlite3.InterfaceError as e:
                    log.error(f"insert {e}")
                    log.debug(params)
                except sqlite3.ProgrammingError as e:
                    log.error(f"insert {e}")
                    log.debug(params)

            conn.commit()
        return inserted

    # columns is a list of columns names
    # params is a list (not tuple, or list of tuples), each containing the same number of elements as columns
    # order matters
    @classmethod
    def insert1(cls, 
                       database_name: str, 
                       table_name: str,
                       columns: list, 
                       params: tuple, unique:bool = False) -> int:
        if (columns is None) or (len(columns) == 0):
            return 0, None
        if (params is None) or (len(params) == 0):
            return 0, None
        
        fields = ', '.join(columns)
        values = ', '.join(params)
        inserted = 0
        lastrowid = None
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    if unique:
                        #log.debug(f"INSERT OR IGNORE INTO {table_name} ({fields}) VALUES ({values})")
                        cur.execute(f"INSERT OR IGNORE INTO {table_name} ({fields}) VALUES ({values})")
                    else:
                        #log.debug(f"INSERT INTO {table_name} ({fields}) VALUES ({values})")
                        cur.execute(f"INSERT INTO {table_name} ({fields}) VALUES ({values})")
                    inserted = cur.rowcount
                    lastrowid = cur.lastrowid
                except sqlite3.IntegrityError as e:
                    log.error(f"insert {e}")
                    log.debug(params)
                except sqlite3.InterfaceError as e:
                    log.error(f"insert {e}")
                    log.debug(params)
                except sqlite3.ProgrammingError as e:
                    log.error(f"insert {e}")
                    log.debug(params)

            conn.commit()
        return inserted, lastrowid

    # sets is a list of string predicates "column_name = value", where value may be '?'
    # where_clause is a string in case  we have to deal with AND/OR, etc.
    # params is a list of tuples corresponding to the '?' in set and where
    # order matters, should match to sets and where_clause
    @classmethod
    def update(cls, database_name: str,
               table_name: str, 
               sets : list, 
               params: list,
               where_clause: str = ""):
        if (sets is None) or (len(sets) == 0):
            return 0
        if (params is None) or (len(params) == 0):
            return 0
        
        log.info(f"SQLiteDB updating {table_name} rows {len(params)}")
        
        set_str = ', '.join(sets)
        where_str = "" if (where_clause is None) or (where_clause == "") else f" WHERE {where_clause}"
        if (len(where_str) > cls.max_where_clause):
            log.warning(f"UPDATE where clause is long")
        
        count = 0
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    #log.debug(f"UPDATE {table_name} SET {set_str} {where_str}")
                    cur.executemany(f"UPDATE {table_name} SET {set_str} {where_str}", params)
                    count = cur.rowcount
                except sqlite3.IntegrityError as e:
                    log.error(f"update {e}")
                    log.debug(params)
                except sqlite3.InterfaceError as e:
                    log.error(f"update {e}")
                    log.debug(params)
                except sqlite3.ProgrammingError as e:
                    log.error(f"update {e}")
                    log.debug(params)
                
            conn.commit()
        return count
    
    @classmethod
    def update1(cls, database_name: str,
               table_name: str, 
               sets : list, 
               where_clause: str = ""):
        if (sets is None) or (len(sets) == 0):
            return 0
        
        set_str = ', '.join(sets)
        where_str = "" if (where_clause is None) or (where_clause == "") else f" WHERE {where_clause}"

        count = 0
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    #log.debug(f"UPDATE {table_name} SET {set_str} {where_str}")
                    cur.execute(f"UPDATE {table_name} SET {set_str} {where_str}")
                    count = cur.rowcount
                except sqlite3.IntegrityError as e:
                    log.error(f"update {e}")
                    log.debug(sets)
                except sqlite3.InterfaceError as e:
                    log.error(f"update {e}")
                    log.debug(sets)
                except sqlite3.ProgrammingError as e:
                    log.error(f"update {e}")
                    log.debug(sets)
                
            conn.commit()
        return count
    


    @classmethod
    def get_max_value(cls, database_name: str, table_name: str, column_name: str):
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                val = cur.execute(f"SELECT MAX({column_name}) FROM {table_name}").fetchone()[0]
        return val
    @classmethod
    def get_unique_values(cls, 
                          database_name: str, 
                          table_name: str, 
                          column_name: str,
                          max_return: int = None):
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                res = cur.execute(f"SELECT DISTINCT {column_name} FROM {table_name}")
                if (max_return is None) or (max_return == 0):
                    # get all
                    vals = res.fetchall()
                elif max_return == 1:
                    vals = res.fetchone()
                else:
                    vals = res.fetchmany(max_return)
        return vals


    @classmethod
    def table_exists(cls, database_name: str, table_name: str ) -> bool:
        table_exists = cls.query(database_name = database_name,
                                 table_name = "sqlite_master",
                                 columns = ["name"],
                                 where_clause = f"type='table' AND name='{table_name}'",
                                 max_return = 1)
            
        return (table_exists is not None) and (table_exists[0] == table_name)
        

# create dispatch class.
class CommandHistoryDispatcher:
    @classmethod
    def _get_version(cls, database_name: str):
        # if journal table exists, return the version number
        if SQLiteDB.table_exists(database_name, "journal_v2"):
            return 2
        elif SQLiteDB.table_exists(database_name, "journal"):
            return 1

        # else check the command_history table version
        if SQLiteDB.table_exists(database_name, "command_history_v2"):
            return 2
        elif SQLiteDB.table_exists(database_name, "command_history"):
            return 1
        
        # else return latest version.
        return 2  # neither exists, so set as 2.
    
    @classmethod
    def create_command_history_table(cls, database_name: str):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            CommandHistoryTableV1.create_command_history_table(database_name)
        elif dbver == 2:
            CommandHistoryTableV2.create_command_history_table(database_name)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def insert_command_history_entry(cls, database_name: str, params: tuple) -> int:
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return CommandHistoryTableV1.insert_command_history_entry(database_name, params)
        elif dbver == 2:
            return CommandHistoryTableV2.insert_command_history_entry(database_name, params)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def get_command_history(cls, database_name: str):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return CommandHistoryTableV1.get_command_history(database_name)
        elif dbver == 2:
            return CommandHistoryTableV2.get_command_history(database_name)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def update_command_completion(cls, database_name: str, command_id:int, duration:float):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return CommandHistoryTableV1.update_command_completion(database_name, command_id, duration)
        elif dbver == 2:
            return CommandHistoryTableV2.update_command_completion(database_name, command_id, duration)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")


# create dispatch class
class JournalDispatcher:
    
    @classmethod
    def table_exists(cls, database_name:str):
        return (SQLiteDB.table_exists(database_name, "journal") or
                SQLiteDB.table_exists(database_name, "journal_v2"))
    
    @classmethod
    def _get_version(cls, database_name: str):
        # if journal table exists, return the version number
        if SQLiteDB.table_exists(database_name, "journal_v2"):
            return 2
        elif SQLiteDB.table_exists(database_name, "journal"):
            return 1

        # else check the command_history table version
        if SQLiteDB.table_exists(database_name, "command_history_v2"):
            return 2
        elif SQLiteDB.table_exists(database_name, "command_history"):
            return 1
        
        # else return latest version.
        return 2  # neither exists, so set as 2.
    
    @classmethod
    def create_journal_table(cls, database_name: str):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            JournalTableV1.create_journal_table(database_name)
        elif dbver == 2:
            JournalTableV2.create_journal_table(database_name)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def insert_journal_entries(cls, database_name: str, params: list) -> int:
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.insert_journal_entries(database_name, params)
        elif dbver == 2:
            return JournalTableV2.insert_journal_entries(database_name, params)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def inactivate_journal_entries(cls, database_name: str, invalidate_time: int, file_states: list):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.inactivate_journal_entries(database_name, invalidate_time, file_states)
        elif dbver == 2:
            return JournalTableV2.inactivate_journal_entries(database_name, invalidate_time, file_states)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def mark_as_uploaded_with_duration(cls, database_name: str, update_args: list):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.mark_as_uploaded_with_duration(database_name, update_args)
        elif dbver == 2:
            return JournalTableV2.mark_as_uploaded_with_duration(database_name, update_args)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def mark_as_uploaded(cls, database_name: str, version: str, upload_args: list):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.mark_as_uploaded(database_name, version, upload_args)
        elif dbver == 2:
            return JournalTableV2.mark_as_uploaded(database_name, version, upload_args)
        
    @classmethod
    def get_latest_version(cls, database_name: str):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.get_latest_version(database_name)
        elif dbver == 2:
            return JournalTableV2.get_latest_version(database_name)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod   
    def get_files_with_meta(cls, database_name: str, version: str, modalities: list, **kwargs):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.get_files_with_meta(database_name, version, modalities, **kwargs)
        elif dbver == 2:
            return JournalTableV2.get_files_with_meta(database_name, version, modalities, **kwargs)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
    
    @classmethod
    def get_files(cls, database_name: str, version: str, modalities: list, **kwargs):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.get_files(database_name, version, modalities, **kwargs)
        elif dbver == 2:
            return JournalTableV2.get_files(database_name, version, modalities, **kwargs)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")
        
    @classmethod
    def get_stats(cls, 
                    database_name: str, 
                    version: str, 
                    **kwargs):
        
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.get_stats(database_name, version, **kwargs)
        elif dbver == 2:
            return JournalTableV2.get_stats(database_name, version, **kwargs)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")

    @classmethod
    def get_versions(cls, database_name: str):
        dbver = cls._get_version(database_name)
        if dbver == 1:
            return JournalTableV1.get_versions(database_name)
        elif dbver == 2:
            return JournalTableV2.get_versions(database_name)
        else:
            raise ValueError(f"Unsupported Journal version {dbver}")

class CommandHistoryTableV1:
    table_name = "command_history"
    
    @classmethod
    def create_command_history_table(cls, database_name: str):
        SQLiteDB.create_table(
            database_name = database_name,
            table_name = cls.table_name, 
            column_types = [
                ("COMMAND_ID", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
                ("DATETIME", "TEXT", ""), 
                ("COMMON_PARAMS", "TEXT", ""), 
                ("COMMAND", "TEXT", ""), 
                ("PARAMS", "TEXT", ""), 
                ("SRC_PATHS", "TEXT", ""), 
                ("DEST_PATH", "TEXT", ""), 
                ("Duration", "TEXT", "")
                ],
            index_on = None)

        
    # params is a list of tuples
    @classmethod
    def insert_command_history_entry(cls, database_name: str,
                                    params: tuple) -> int:
        params2 = tuple([param if param is not None else '' for param in params])
        return SQLiteDB.insert1(database_name = database_name,
                            table_name = cls.table_name,
                            columns = [
                                 "DATETIME", 
                                 "COMMON_PARAMS", 
                                 "COMMAND", 
                                 "PARAMS", 
                                 "SRC_PATHS", 
                                 "DEST_PATH"
                                 ],
                            params = params2)
        
    @classmethod
    def get_command_history(cls, database_name: str):
        return SQLiteDB.query(database_name = database_name,
                         table_name = cls.table_name,
                         columns = None,
                         where_clause = None,
                         max_return = None)

    @classmethod
    def update_command_completion(cls, 
                                  database_name: str, 
                                  command_id:int, 
                                  duration:float):
        """
        Update the duration of a command in the command history.
        Parameters:
        - command_id (int): The ID of the command to update.
        - duration (float): The new duration of the command.
        - databasename (str): The name of the database file to connect to. Default is "journal.db".
        """
        return SQLiteDB.update1(database_name = database_name, 
                     table_name = cls.table_name,
                     sets = [f"DURATION = {duration}"],
                     where_clause = f"COMMAND_ID = {command_id}"
                    )

class JournalTableV1:
    table_name = "journal"
    
    @classmethod
    def create_journal_table(cls, database_name: str):
        SQLiteDB.create_table(
            database_name = database_name,
            table_name = cls.table_name, 
            column_types = [
                ("FILE_ID", "INTEGER", "PRIMARY KEY AUTOINCREMENT"), # required
                ("PERSON_ID", "INTEGER", ""), # never uesd
                ("FILEPATH", "TEXT", ""), # required
                ("MODALITY", "TEXT", ""),  # used for selecting files, not used as returned value
                ("SRC_MODTIME_us", "INTEGER", ""), # used for file comparison
                ("SIZE", "INTEGER", ""),  # used for file comparison and for threading
                ("MD5", "TEXT", ""),  # used for file comparison
                ("TIME_VALID_us", "INTEGER", ""),  # used to determine when a file became valid.  not used in current code.
                ("TIME_INVALID_us", "INTEGER", ""),  # used to determine when a file became invalid.  as query filter only
                ("UPLOAD_DTSTR", "TEXT", ""),  # upload timestamp, used as query filter (a flag), and as a record.
                ("VERSION", "TEXT", ""), # matches the cloud directory. required
                ("STATE", "TEXT", ""),  # set but not used.  possible value ADDED, UPDATED, DELETED, OUTDATED, MOVED
                ("MD5_DURATION", "REAL", ""), # only for performance measurement
                ("UPLOAD_DURATION", "REAL", ""), # only for performance measurement
                ("VERIFY_DURATION", "REAL", "")  # only for performance measurement
                ],
            index_on = None)
        
    # list is a list of tuples
    @classmethod
    def insert_journal_entries(cls, database_name: str, params: list) -> int:
        return SQLiteDB.insert(database_name = database_name, 
                           table_name = cls.table_name,
                           columns = [
                               "PERSON_ID", 
                               "FILEPATH", 
                               "MODALITY", 
                               "SRC_MODTIME_us", 
                               "SIZE", 
                               "MD5", 
                               "TIME_VALID_us", 
                               "UPLOAD_DTSTR",
                               "STATE", 
                               "MD5_DURATION", 
                               "VERSION", 
                               ],
                            params = params)

    # file_states is a list of tuples of form (state, file_id)
    # state can be "DELETED", "OUTDATED"
    @classmethod
    def inactivate_journal_entries(cls, database_name: str, 
                          invalidate_time: int,
                          file_states: list):
        return SQLiteDB.update(database_name = database_name,
                            table_name = cls.table_name,
                            sets = [f"TIME_INVALID_us = {invalidate_time}", 
                                    "STATE = ?"],
                            params = file_states,
                            where_clause = "FILE_ID = ?")
        
    @classmethod
    def mark_as_uploaded_with_duration(cls, database_name: str, 
                            update_args: list):
        return SQLiteDB.update(database_name = database_name, 
                                table_name = cls.table_name,
                                sets = ["upload_dtstr=?", 
                                        "upload_duration=?",
                                        "verify_duration=?"],
                                params = update_args,
                                where_clause="file_id=?"
                                )
    
    @classmethod
    def mark_as_uploaded(cls, database_name: str, 
                        version: str,
                        upload_args: list):
        return SQLiteDB.update(database_name = database_name,
                        table_name = cls.table_name,
                        sets = [f"upload_dtstr='{version}'"],
                        params = upload_args,
                        where_clause="file_id=?")
    
    @classmethod
    def get_latest_version(cls, database_name: str):
        return SQLiteDB.get_max_value(database_name = database_name,
                                      table_name = cls.table_name,
                                      column_name = "VERSION")
        
    
    @classmethod
    def _make_where_clause(cls, version: str, modalities: list, **kwargs):
        """
        Constructs a SQL WHERE clause based on the provided parameters.
        Args:
            version (str): The version string to filter by.
            modalities (list): A list of modality strings to filter by.
            **kwargs: Additional keyword arguments for filtering:
                - active (bool, optional): If True, filters for active records (TIME_INVALID_us IS NULL).
                                           If False, filters for inactive records (TIME_INVALID_us IS NOT NULL).
                - uploaded (bool, optional): If True, filters for uploaded records (UPLOAD_DTSTR IS NOT NULL).
                                             If False, filters for non-uploaded records (UPLOAD_DTSTR IS NULL).
        Returns:
            str: A SQL WHERE clause string based on the provided filters.
                 Returns None if no filters are provided.
        """
        
        active = kwargs.get("active", None)
        uploaded = kwargs.get("uploaded", None)        
        
        where_clause = ""
        if active is not None:
            where_clause += "TIME_INVALID_us IS NULL" if active else "TIME_INVALID_us IS NOT NULL"
            
        if uploaded is not None:
            if len(where_clause) > 0:
                where_clause += " AND "
            
            where_clause += "UPLOAD_DTSTR IS NOT NULL" if uploaded else "UPLOAD_DTSTR IS NULL"
            

        if version is not None:
            if len(where_clause) > 0:
                where_clause += " AND "
            where_clause += f"VERSION = '{version}'"
        

        if (modalities is not None) and (len(modalities) > 0):
            if len(where_clause) > 0:
                where_clause += " AND "
            modestr = [f"'{m}'" for m in modalities]
            where_clause += "MODALITY in (" + ",".join(modestr) + ")" 
            
        return where_clause if len(where_clause) > 0 else None

    
    @classmethod
    def get_files_with_meta(cls, database_name: str, 
                       version: str, modalities: list, 
                       **kwargs):
        
        where_clause = cls._make_where_clause(version, modalities, **kwargs)
        max_return = kwargs.get("count", None)
            
        return SQLiteDB.query(database_name = database_name,
                        table_name = cls.table_name,
                        columns = ["FILE_ID", "FILEPATH", "SRC_MODTIME_us", "SIZE", "MD5", "MODALITY", "TIME_INVALID_us", "VERSION", "UPLOAD_DTSTR"],
                        where_clause = where_clause,
                        max_return = max_return)


    @classmethod
    def get_files(cls, database_name: str, 
                       version: str, modalities: list, 
                       **kwargs):
        
        where_clause = cls._make_where_clause(version, modalities, **kwargs)
        max_return = kwargs.get("count", None)
            
        return SQLiteDB.query(database_name = database_name,
                        table_name = cls.table_name,
                        columns = ["FILE_ID", "FILEPATH"],
                        where_clause = where_clause,
                        max_return = max_return)
        
    @classmethod
    def get_stats(cls, 
                    database_name: str, 
                    version: str, 
                    **kwargs):
        
        
        groupby_list = ['VERSION', 'MODALITY']
        group_list = ','.join(groupby_list)
        
        cols = group_list
        cols.append("COUNT(FILE_ID) AS n_rows")
        cols.append("COUNT(DISTINCT PERSON_ID) AS n_persons")
        cols.append("COUNT(DISTINCT FILEPATH) AS n_files")
        cols.append("SUM(TIME_INVALID_us IS NULL) AS n_active")
        cols.append("SUM(TIME_INVALID_us IS NULL AND UPLOAD_DTSTR IS NULL) as n_upload")
                            
        where_clause = cls._make_where_clause_for_join(version, None, **kwargs)

        # query = f"SELECT versions.VERSION as version, modalities.MODALITY as modality, g.row_count as row_count FROM {subquery}"
        # query += f" LEFT JOIN versions ON versions.id = g.VERSION_ID"
        # query += f" LEFT JOIN modalities ON modalities.id = g.MODALITY_ID"
        # query += f" WHERE {cls._make_where_clause_for_join(version, modalities, **kwargs)}"
            
        results = SQLiteDB.query(database_name = database_name,
                        table_name = cls.table_name,
                        columns = cols,
                        where_clause = where_clause,
                        groupby = groupby_list,
                        max_return = None)
        # return results
        return [(version, modality, n_rows, n_persons, n_files, n_active, n_upload) for (version, modality, n_rows, n_persons, n_files, n_active, n_upload) in results]
    
        
    @classmethod
    def get_versions(cls, database_name: str):
        return SQLiteDB.get_unique_values(database_name = database_name,
                                      table_name = cls.table_name,
                                      column_name = "VERSION")


class CommandHistoryTableV2:
    table_name = "command_history_v2"
    
    @classmethod
    def create_command_history_table(cls, database_name: str):
        SQLiteDB.create_table(
            database_name = database_name,
            table_name = cls.table_name, 
            column_types = [
                ("COMMAND_ID", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
                ("DATETIME", "TEXT", "NOT NULL"), 
                ("COMMON_PARAMS", "TEXT", "NOT NULL"), 
                ("COMMAND", "TEXT", "NOT NULL"), 
                ("PARAMS", "TEXT", ""), 
                ("SRC_PATHS", "TEXT", ""), 
                ("DEST_PATH", "TEXT", ""), 
                ("Duration", "REAL", "")
                ],
            index_on = None)

        
    # params is a list of tuples
    @classmethod
    def insert_command_history_entry(cls, database_name: str,
                                    params: tuple) -> int:
        params2 = tuple([param if param is not None else '' for param in params])
        return SQLiteDB.insert1(database_name = database_name,
                            table_name = cls.table_name,
                            columns = [
                                 "DATETIME", 
                                 "COMMON_PARAMS", 
                                 "COMMAND", 
                                 "PARAMS", 
                                 "SRC_PATHS", 
                                 "DEST_PATH"
                                 ],
                            params = params2)
        
    @classmethod
    def get_command_history(cls, database_name: str):
        return SQLiteDB.query(database_name = database_name,
                         table_name = cls.table_name,
                         columns = None,
                         where_clause = None,
                         max_return = None)

    @classmethod
    def update_command_completion(cls, 
                                  database_name: str, 
                                  command_id:int, 
                                  duration:float):
        """
        Update the duration of a command in the command history.
        Parameters:
        - command_id (int): The ID of the command to update.
        - duration (float): The new duration of the command.
        - databasename (str): The name of the database file to connect to. Default is "journal.db".
        """
        return SQLiteDB.update1(database_name = database_name, 
                     table_name = cls.table_name,
                     sets = [f"DURATION = {duration}"],
                     where_clause = f"COMMAND_ID = {command_id}"
                    )

class JournalTableV2:
    
    profiling = False  # set to True to enable performance measurement.
    table_name = "journal_v2"
    
    @classmethod
    def create_journal_table(cls, database_name: str):

        # create a table for preformance measurement.
        SQLiteDB.create_lookup_table(
            database_name = database_name,
            table_name = "srcpaths", 
            column_name = "SRC_PATH")

        # create a table for preformance measurement.
        SQLiteDB.create_lookup_table(
            database_name = database_name,
            table_name = "modalities", 
            column_name = "MODALITY")
        
        # create a table for preformance measurement.
        SQLiteDB.create_lookup_table(
            database_name = database_name,
            table_name = "versions",
            column_name = "VERSION")
        
        # create a table for preformance measurement.
        SQLiteDB.create_lookup_table(
            database_name = database_name,
            table_name = "uploads", 
            column_name = "UPLOAD_DT")

        SQLiteDB.create_table(
            database_name = database_name,
            table_name = cls.table_name, 
            column_types = [
                ("FILE_ID", "INTEGER", "PRIMARY KEY AUTOINCREMENT"), # required
                ("PERSON_ID", "INTEGER", ""), # never uesd -  maybe central files
                ("SRC_PATH_ID", "INTEGER", "NOT NULL"), # required - leave as is for now.
                ("FILENAME", "TEXT", "NOT NULL"), # required - leave as is for now.
                ("MODALITY_ID", "INTEGER", ""),  # used for selecting files, not used as returned value
                ("SRC_MODTIME_us", "INTEGER", "NOT NULL"), # used for file comparison
                ("SIZE", "INTEGER", "NOT NULL"),  # used for file comparison and for threading
                ("MD5", "TEXT", "NOT NULL"),  # used for file comparison
                ("TIME_VALID_us", "INTEGER", "NOT NULL"),  # used to determine when a file became valid.  not used in current code.
                ("TIME_INVALID_us", "INTEGER", ""),  # used to determine when a file became invalid.  as query filter only
                ("UPLOAD_DT_ID", "INTEGER", ""),  # upload timestamp, used as query filter (a flag), and as a record.
                ("VERSION_ID", "INTEGER", "NOT NULL"), # matches the cloud directory. required
                # ("STATE", "TEXT", ""),  # set but not used.  possible value ADDED, UPDATED, DELETED, OUTDATED, MOVED
                ],
            foreign_keys = [ ("SRC_PATH_ID", "srcpaths", "id"),
                             ("MODALITY_ID", "modalities", "id"),
                             ("UPLOAD_DT_ID", "uploads", "id"),
                             ("VERSION_ID", "versions", "id")],
            index_on = None) # index on SRC_PATH_ID
                        
        if cls.profiling:
            profile_db = database_name.replace(".db", "_profile.db")
            # create a table for preformance measurement.
            SQLiteDB.create_table(
                database_name = profile_db,
                table_name = "performance", 
                column_types = [
                    ("FILE_ID", "INTEGER", "PRIMARY KEY"), # required
                    ("STATE", "TEXT", "NOT NULL"), # required
                    ("MD5_DURATION", "REAL", "NOT NULL"), # only for performance measurement
                    ("UPLOAD_DURATION", "REAL", ""), # only for performance measurement
                    ("VERIFY_DURATION", "REAL", "")  # only for performance measurement
                    ],
                foreign_keys = None,
                index_on = None)

       
    # list is a list of tuples
    @classmethod
    def insert_journal_entries(cls, database_name: str, params: list) -> int:
        # insert or retrieve the modalities, uploads, and versions first.
        modalities = set()
        uploads = set()
        versions = set()
        parent_paths = set()
        for (pid, fpath, mod, mtime, size, md5, valid_time, upload, state, md5_dur, ver) in params:
            # get file parent path and filename using pathlib
            parent_paths.add(Path(fpath).parent.as_posix())
            modalities.add(mod)
            if upload is not None:
                uploads.add(upload)
            versions.add(ver)

        # insert modalities if it does not already exist and retrieve the ids for all entries in the dictionary
        modalities = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                    table_name = "modalities",
                                                    column_name = "MODALITY",
                                                    lookup = modalities)
        # insert uploads if it does not already exist and retrieve the ids for all entries in the dictionary
        if len(uploads) > 0:
            uploads = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                    table_name = "uploads",
                                                        column_name = "UPLOAD_DT",
                                                        lookup = uploads)
        else:
            uploads = None
            
        # insert versions if it does not already exist and retrieve the ids for all entries in the dictionary
        versions = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                    table_name = "versions",
                                                     column_name = "VERSION",
                                                     lookup = versions)
        
        # insert parent paths if it does not already exist and retrieve the ids for all entries in the dictionary
        parent_paths = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                        table_name = "srcpaths",
                                                        column_name = "SRC_PATH",
                                                        lookup = parent_paths)
        
        # formulate the args for the new journal entries.
        new_params = []
        filenames = set()
        for (pid, fpath, mod, mtime, size, md5, valid_time, upload, state, md5_dur, ver) in params:
            path = Path(fpath)
            parent_id = parent_paths[path.parent.as_posix()]
            mod_id = modalities[mod]
            upload_id = uploads[upload] if upload is not None else None
            ver_id = versions[ver]
            new_params.append( (pid, parent_id, path.name, mod_id, mtime, size, md5, valid_time, upload_id, ver_id))
            
            filenames.add(fpath)
            
        res = SQLiteDB.insert(database_name = database_name, 
                           table_name = cls.table_name,
                           columns = [
                               "PERSON_ID",
                               "SRC_PATH_ID", 
                               "FILENAME", 
                               "MODALITY_ID", 
                               "SRC_MODTIME_us", 
                               "SIZE",
                               "MD5", 
                               "TIME_VALID_us", 
                               "UPLOAD_DT_ID",
                               "VERSION_ID", 
                               ],
                            params = new_params)

        if cls.profiling:
            profile_db = database_name.replace(".db", "_profile.db")

            # query for fid of what was just inserted.
            fid_generator = cls.get_files(database_name = database_name,
                                        version = None, modalities = None,
                                        **{"active": True, 'uploaded': False})
            # since active, unuploaded, fpath should be unique in the results.
            fids = {fpath: fid for (fid, fpath) in fid_generator}

            # Prepare performance parameters
            perf_params = []
            for (pid, fpath, mod, mtime, size, md5, valid_time, upload, state, md5_dur, ver) in params:
                perf_params.append((fids[fpath], state, md5_dur))
                
            # insert the performance parameters.
            SQLiteDB.insert(database_name = profile_db, 
                            table_name = "performance",
                            columns = [
                                    "FILE_ID",
                                    "STATE",
                                    "MD5_DURATION",
                                ],
                                params = perf_params)

        return res

    
    # file_states is a list of tuples of form (state, file_id)
    # state can be "DELETED", "OUTDATED"
    @classmethod
    def inactivate_journal_entries(cls, database_name: str, 
                          invalidate_time: int,
                          file_states: list):
        revised_args = [(file_id,) for (state, file_id) in file_states]
        
        if cls.profiling:
            profile_db = database_name.replace(".db", "_profile.db")
            SQLiteDB.update(database_name = profile_db,
                            table_name = "performance",
                            sets = ["STATE=?"],
                            params = file_states,
                            where_clause="FILE_ID=?")
        
        return SQLiteDB.update(database_name = database_name,
                            table_name = cls.table_name,
                            sets = [f"TIME_INVALID_us = {invalidate_time}"],
                            params = revised_args,
                            where_clause = "FILE_ID = ?")
        
    @classmethod
    def mark_as_uploaded_with_duration(cls, database_name: str, 
                            update_args: list):
        # update the uploads table first
        uploads = set()
        perf_params = []
        for (upload_dt, upload_dur, verify_dur, fid) in update_args:
            uploads.add(upload_dt)
            perf_params.append((upload_dur, verify_dur, fid))
            
        # insert upload timestamp and get he corresponding ids
        uploads = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                table_name = "uploads",
                                                column_name = "UPLOAD_DT",
                                                lookup = uploads)
        if cls.profiling:
            profile_db = database_name.replace(".db", "_profile.db")

            # insert performance parameters.
            SQLiteDB.update(database_name = profile_db,
                            table_name = "performance",
                            sets = ["UPLOAD_DURATION=?", "VERIFY_DURATION=?"],
                            params = perf_params,
                            where_clause="FILE_ID=?")
        
        # update the journal table
        journal_args = [(uploads[upload_dt], fid) for (upload_dt, _, _, fid) in update_args]
        
        return SQLiteDB.update(database_name = database_name, 
                                table_name = cls.table_name,
                                sets = ["UPLOAD_DT_ID=?"],
                                params = journal_args,
                                where_clause="file_id=?"
                                )
    
    @classmethod
    def mark_as_uploaded(cls, database_name: str, 
                        version: str,
                        upload_args: list):
        upload_id = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                    table_name = "uploads",
                                                    column_name = "UPLOAD_DT",
                                                    lookup = set([version]))
        upload_id = upload_id[version]
        
        return SQLiteDB.update(database_name = database_name,
                        table_name = cls.table_name,
                        sets = [f"UPLOAD_DT_ID='{upload_id}'"],
                        params = upload_args,
                        where_clause="file_id=?")
    
    @classmethod
    def get_latest_version(cls, database_name: str):
        return SQLiteDB.get_max_value(database_name = database_name,
                                      table_name = "versions",
                                      column_name = "VERSION")
        
    @classmethod
    def _make_where_clause_for_join(cls, 
                                    version: str, 
                                    modalities: list, 
                                    **kwargs):
        """
        Constructs a SQL WHERE clause based on the provided parameters.
        Args:
            version (str): The version string to filter by.
            modalities (list): A list of modality strings to filter by.
            **kwargs: Additional keyword arguments for filtering:
                - active (bool, optional): If True, filters for active records (TIME_INVALID_us IS NULL).
                                           If False, filters for inactive records (TIME_INVALID_us IS NOT NULL).
                - uploaded (bool, optional): If True, filters for uploaded records (UPLOAD_DTSTR IS NOT NULL).
                                             If False, filters for non-uploaded records (UPLOAD_DTSTR IS NULL).
        Returns:
            str: A SQL WHERE clause string based on the provided filters.
                 Returns None if no filters are provided.
        """
        
        active = kwargs.get("active", None)
        uploaded = kwargs.get("uploaded", None)        
        
        where_clause = ""
        if active is not None:
            where_clause += "TIME_INVALID_us IS NULL" if active else "TIME_INVALID_us IS NOT NULL"
            
        if uploaded is not None:
            if len(where_clause) > 0:
                where_clause += " AND "
            where_clause += "UPLOAD_DT_ID IS NOT NULL" if uploaded else "UPLOAD_DT_ID IS NULL"
            
        # requires version to be defined via "versions.VERSION as version"
        if version is not None:
            if len(where_clause) > 0:
                where_clause += " AND "
            where_clause += f"version = '{version}'"
        
        # requires modality to be defined via "modalities.MODALITY as modality"
        if (modalities is not None) and (len(modalities) > 0):
            if len(where_clause) > 0:
                where_clause += " AND "
            modestr = [f"'{m}'" for m in modalities]
            where_clause += "modality in (" + ",".join(modestr) + ")" 
            
        return where_clause if len(where_clause) > 0 else None

    @classmethod
    def get_files_with_meta(cls, database_name: str, 
                       version: str, modalities: list, 
                       **kwargs):
        
        max_return = kwargs.get("count", None)
        columns = [
            ("FILE_ID", None),
            ("srcpaths.SRC_PATH", "src_path"),
            ("FILENAME", None),
            ("SRC_MODTIME_us", None),
            ("SIZE", None),
            ("MD5", None),
            ("modalities.MODALITY", "modality"),
            ("TIME_INVALID_us", None),
            ("versions.VERSION", "version"),
            ("uploads.UPLOAD_DT", "upload_dtstr"),
        ]
        join_criteria = [
            ("srcpaths", "srcpaths.id", f"{cls.table_name}.SRC_PATH_ID"),
            ("modalities", "modalities.id", f"{cls.table_name}.MODALITY_ID"),
            ("versions", "versions.id", f"{cls.table_name}.VERSION_ID"),
            ("uploads", "uploads.id", f"{cls.table_name}.UPLOAD_DT_ID"),
        ]
        where_clause = cls._make_where_clause_for_join(version, modalities, **kwargs)
            
        result = SQLiteDB.query_with_left_join(database_name = database_name,
                        table_name = cls.table_name,
                        columns = columns,
                        join_criteria=join_criteria,
                        where_clause = where_clause,
                        max_return = max_return)

        # create a generator to yield the results
        return [(fid, str(Path(srcpath) / fn), mtime, size, md5, mod, invalidtime, ver, uploaddt) for (fid, srcpath, fn, mtime, size, md5, mod, invalidtime, ver, uploaddt) in result]

    @classmethod
    def get_files(cls, database_name: str, 
                       version: str, modalities: list, 
                       **kwargs):
        
        max_return = kwargs.get("count", None)
        columns = [
            ("FILE_ID", None),
            ("srcpaths.SRC_PATH", "src_path"),
            ("FILENAME", None),
        ]
        join_criteria = [
            ("srcpaths", "srcpaths.id", f"{cls.table_name}.SRC_PATH_ID"),
        ]

        if version is not None:
            columns.append( ("versions.VERSION", "version"))
            join_criteria.append( ("versions", "versions.id", f"{cls.table_name}.VERSION_ID") )
        if (modalities is not None) and (len(modalities) > 0): 
            columns.append( ("modalities.MODALITY", "modality"))
            join_criteria.append( ("modalities", "modalities.id", f"{cls.table_name}.MODALITY_ID") )   
        
        where_clause = cls._make_where_clause_for_join(version, modalities, **kwargs)
            
        result = SQLiteDB.query_with_left_join(database_name = database_name,
                        table_name = cls.table_name,
                        columns = columns,
                        join_criteria=join_criteria,
                        where_clause = where_clause,
                        max_return = max_return)

        # create a generator to yield the results
        return [(fid, str(Path(srcpath) / fn)) for (fid, srcpath, fn) in result]
    
    @classmethod
    def get_stats(cls, 
                    database_name: str, 
                    version: str, 
                    **kwargs):
        
        groupby_list = ['VERSION_ID', 'MODALITY_ID']
        group_list = ', '.join(groupby_list)
        
        subquery = f"(SELECT {group_list}, "
        cols = [
            "COUNT(FILE_ID) AS n_rows",
            "COUNT(DISTINCT(PERSON_ID)) AS n_persons",
            "COUNT(DISTINCT CAST(SRC_PATH_ID AS TEXT) || '|' || FILENAME) AS n_files",
            "SUM(TIME_INVALID_us IS NULL) AS n_active",
            "SUM(TIME_INVALID_us IS NULL AND UPLOAD_DT_ID IS NULL) AS n_upload",
        ]
        subquery += ", ".join(cols)
        subquery += f" FROM {cls.table_name}"
        subquery += f" GROUP BY {group_list}) AS g"

        columns = [
            ("versions.VERSION", "version"),
            ("modalities.MODALITY", "modality")
        ]        
        columns.extend([
            ("g.n_rows", "n_rows"),
            ("g.n_persons", "n_persons"),
            ("g.n_files", "n_files"),
            ("g.n_active", "n_active"),
            ("g.n_upload", "n_upload"),
        ])
        join_criteria = [
            ("versions", "versions.id", "g.VERSION_ID"),
            ("modalities", "modalities.id", "g.MODALITY_ID"),
        ]
        where_clause = cls._make_where_clause_for_join(version, None, **kwargs)

        results = SQLiteDB.query_with_left_join(database_name = database_name,
                        table_name = subquery,
                        columns = columns,
                        join_criteria=join_criteria,
                        where_clause = where_clause,
                        max_return = None)
        return [(version, modality, n_rows, n_persons, n_files, n_active, n_upload) for (version, modality, n_rows, n_persons, n_files, n_active, n_upload) in results]
        
        # return results
    
    
    @classmethod
    def get_versions(cls, database_name: str):
        return SQLiteDB.query(database_name = database_name,
                         table_name = "versions",
                         columns = ["VERSION"],
                         where_clause = None,
                         max_return = None)


def _copy_journal_v1_to_v2(database_name: str, params: list) -> int:
    # insert or retrieve the modalities, uploads, and versions first.
    modalities = set()
    uploads = set()
    versions = set()
    parent_paths = set()
    for (fid, pid, fpath, mod, mtime, size, md5, valid, invalid, upload, ver, state, md5_dur, upload_dur, verify_dur ) in params:
        # get file parent path and filename using pathlib
        parent_paths.add(Path(fpath).parent.as_posix())
        modalities.add(mod)
        if upload is not None:
            uploads.add(upload)
        versions.add(ver)

    # insert modalities if it does not already exist and retrieve the ids for all entries in the dictionary
    modalities = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                table_name = "modalities",
                                                column_name = "MODALITY",
                                                lookup = modalities)
    # insert uploads if it does not already exist and retrieve the ids for all entries in the dictionary
    if len(uploads) > 0:
        uploads = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                table_name = "uploads",
                                                    column_name = "UPLOAD_DT",
                                                    lookup = uploads)
    else:
        uploads = None
        
    # insert versions if it does not already exist and retrieve the ids for all entries in the dictionary
    versions = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                table_name = "versions",
                                                    column_name = "VERSION",
                                                    lookup = versions)
    
    # insert parent paths if it does not already exist and retrieve the ids for all entries in the dictionary
    parent_paths = SQLiteDB.insert_lookup_table(database_name = database_name,
                                                    table_name = "srcpaths",
                                                    column_name = "SRC_PATH",
                                                    lookup = parent_paths)
    
    # formulate the args for the new journal entries.
    new_params = []
    filenames = set()
    for (fid, pid, fpath, mod, mtime, size, md5, valid, invalid, upload, ver, state, md5_dur, upload_dur, verify_dur ) in params:
        path = Path(fpath)
        parent_id = parent_paths[path.parent.as_posix()]
        mod_id = modalities[mod]
        upload_id = uploads[upload] if upload is not None else None
        ver_id = versions[ver]
        new_params.append( (pid, parent_id, path.name, mod_id, mtime, size, md5, valid, invalid, upload_id, ver_id))
        
        filenames.add(fpath)
        
    res = SQLiteDB.insert(database_name = database_name, 
                        table_name = JournalTableV2.table_name,
                        columns = [
                            "PERSON_ID",
                            "SRC_PATH_ID", 
                            "FILENAME", 
                            "MODALITY_ID", 
                            "SRC_MODTIME_us", 
                            "SIZE",
                            "MD5", 
                            "TIME_VALID_us",
                            "TIME_INVALID_us",
                            "UPLOAD_DT_ID",
                            "VERSION_ID", 
                            ],
                        params = new_params)

    if (JournalTableV2.profiling):
        profile_db = database_name.replace(".db", "_profile.db")
        # create a table for preformance measurement.
        SQLiteDB.create_table(
            database_name = profile_db,
            table_name = "performance", 
            column_types = [
                ("FILE_ID", "INTEGER", "PRIMARY KEY"), # required
                ("STATE", "TEXT", "NOT NULL"), # required
                ("MD5_DURATION", "REAL", "NOT NULL"), # only for performance measurement
                ("UPLOAD_DURATION", "REAL", ""), # only for performance measurement
                ("VERIFY_DURATION", "REAL", "")  # only for performance measurement
                ],
            foreign_keys = None,
            index_on = None)

        
        # query for fid of what was just inserted.
        fid_generator = JournalTableV2.get_files(database_name = database_name,
                                        version = None, modalities = None)
        # since active, unuploaded, fpath should be unique in the results.
        fids = {fpath: fid for (fid, fpath) in fid_generator}

        # Prepare performance parameters
        perf_params = []
        for (fid, pid, fpath, mod, mtime, size, md5, valid, invalid, upload, ver, state, md5_dur, upload_dur, verify_dur ) in params:
            perf_params.append((fids[fpath], state, md5_dur, upload_dur, verify_dur))
            
        # insert the performance parameters.
        SQLiteDB.insert(database_name = profile_db, 
                            table_name = "performance",
                            columns = [
                                "FILE_ID",
                                "STATE",
                                "MD5_DURATION",
                                "UPLOAD_DURATION",
                                "VERIFY_DURATION",
                                ],
                            params = perf_params)

    return res


def _upgrade_journal(local_path, lock_path):
    """
    Upgrades the journal database to the latest version.
    Parameters:
    - local_path (str): The path to the local journal database file.
    - lock_path (str): The path to the lock file for the journal database.
    """
    
    local_fn = str(local_path.root) if isinstance(local_path, FileSystemHelper) else local_path
    
    # first check local_path
    old_cmd_hist_class = None
    new_cmd_hist_class = None
    old_journal_class = None
    new_journal_class = None
    old_ver = None
    if SQLiteDB.table_exists(local_fn, JournalTableV1.table_name):
        old_cmd_hist_class = CommandHistoryTableV1
        new_cmd_hist_class = CommandHistoryTableV2
        old_journal_class = JournalTableV1
        new_journal_class = JournalTableV2
        old_ver = "V1"
    elif SQLiteDB.table_exists(local_fn, JournalTableV2.table_name):
        log.info(f"Journal database is already at the latest version (v2).")
        return None
    else:
        log.error(f"No journal table, or unsupported version to upgrade.")
        return None
    
    log.info(f"Found journal database version {old_ver} at {local_fn}")

    # create a locak backup first.    
    orig_db_fn = local_fn.replace(".db", f"_{old_ver}.db")
    shutil.move(local_fn, orig_db_fn)
    
    # TESTING
    # shutil.copy(local_fn, orig_db_fn)
    # local_fn = local_fn.replace(".db", "_v2.db")        

    # ------------ do the backup.
    # copy history table
    history = SQLiteDB.query(database_name = orig_db_fn,
                            table_name = old_cmd_hist_class.table_name,
                            columns = None,
                            where_clause = None,
                            max_return = None)

    new_cmd_hist_class.create_command_history_table(local_fn)
    for (_, dt, common, command, param, src, dest, time) in history:
        new_arg = (f"'{dt}'", f"'{common}'", f"'{command}'", f"'{param}'", f"'{src}'", f"'{dest}'")
        (_, cmd_id) = new_cmd_hist_class.insert_command_history_entry(local_fn, new_arg)
        if (time is not None):
            new_cmd_hist_class.update_command_completion(local_fn, cmd_id, time)


    # create the other tables and copy the journal
    new_journal_class.create_journal_table(local_fn)

    journals = SQLiteDB.query(database_name = orig_db_fn,
                            table_name = old_journal_class.table_name,
                            columns = None,
                            where_clause = None,
                            max_return = None)
    
    if old_ver == "V1":
        count = _copy_journal_v1_to_v2(local_fn, journals)
    
    # copy the orig_db_path as a backup.
    if lock_path is not None:
        backup_path = str(lock_path.root).replace(".db", f"_{old_ver}.db")
        shutil.copy(lock_path, backup_path)
        # orig_db_fn.copy_file_to(dest_path = backup_path)
    else: # already copied. 
        backup_path = orig_db_fn
    log.info(f"Backed up old journal database {local_fn} version {old_ver} as {backup_path}")

    # print some stats
    log.info(f"Copied {len(history)} entries from {old_ver} cmd history database as {SQLiteDB.get_row_count(local_fn, new_cmd_hist_class.table_name)} new entries.")
    log.info(f"Copied {len(journals)} entries from {old_ver} journal database as {SQLiteDB.get_row_count(local_fn, new_journal_class.table_name)} new entries.")
    

    
    
