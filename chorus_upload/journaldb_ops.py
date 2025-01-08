import sqlite3
from contextlib import closing



class SQLiteDB:
    # set a class variable for verbosity
    
    # columns is a list of columns to retrieve from
    # where_clause is a string in case  we have to deal with AND/OR, etc.
    @classmethod
    def query(cls, 
                database_name: str, 
                table_name:str, 
                columns : list, 
                where_clause: str = None,
                count:int = None):
        if (columns is None) or (len(columns) == 0):
            fields = "*"
        else:
            fields = ', '.join(columns)
            
        where_str = "" if (where_clause is None) or (where_clause == "") else f" WHERE {where_clause}"
            
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                # print(f"SELECT {fields} FROM {table_name} {where_str}")
                res = cur.execute(f"SELECT {fields} FROM {table_name} {where_str}")
                if (count is None) or (count == 0):
                    # get all
                    return res.fetchall()
                elif count == 1:
                    return res.fetchone()
                else:
                    return res.fetchmany(count)

    # column_types is a list of tuples of form (column_name, column_type, column property)
    # index_on is a list of column names
    @classmethod
    def create_table(cls, database_name: str,
                     table_name : str, 
                     column_types: list[tuple],
                     index_on: list = None):
        if (column_types is None) or (len(column_types) == 0):
            return
        
        columns = ', '.join([f"{k} {v} {prop}" for (k, v, prop) in column_types])
        indexes = ', '.join(index_on) if index_on is not None else ""
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
                if index_on is not None:
                    cur.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_idx ON {table_name} ({indexes})")
            conn.commit()
        

    # columns is a list of columns names
    # params is a list of tuples, each containing the same number of elements as columns
    # order matters.
    @classmethod
    def insert(cls, 
                       database_name: str, 
                       table_name: str,
                       columns: list, 
                       params: list) -> int:
        if (params is None) or (len(params) == 0):
            return 0, None
        
        fields = ', '.join(columns)
        placeholders = ', '.join('?' * len(columns))
        inserted = 0
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    cur.executemany(f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders})", params)
                    inserted = cur.rowcount
                except sqlite3.IntegrityError as e:
                    print("ERROR: insert ", e)
                    print(params)
                except sqlite3.InterfaceError as e:
                    print("ERROR: insert ", e)
                    print(params)
                except sqlite3.ProgrammingError as e:
                    print("ERROR: insert ", e)
                    print(params)

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
                       params: tuple) -> int:
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
                    cur.execute(f"INSERT INTO {table_name} ({fields}) VALUES ({values})")
                    inserted = cur.rowcount
                    lastrowid = cur.lastrowid
                except sqlite3.IntegrityError as e:
                    print("ERROR: insert ", e)
                    print(params)
                except sqlite3.InterfaceError as e:
                    print("ERROR: insert ", e)
                    print(params)
                except sqlite3.ProgrammingError as e:
                    print("ERROR: insert ", e)
                    print(params)

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
        
        print("INFO: SQLiteDB updating ", table_name, " rows ", len(params))
        
        set_str = ', '.join(sets)
        where_str = "" if (where_clause is None) or (where_clause == "") else f" WHERE {where_clause}"
        count = 0
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    cur.executemany(f"UPDATE {table_name} SET {set_str} {where_str}", params)
                    count = cur.rowcount
                except sqlite3.IntegrityError as e:
                    print("ERROR: update ", e)
                    print(params)
                except sqlite3.InterfaceError as e:
                    print("ERROR: update ", e)
                    print(params)
                except sqlite3.ProgrammingError as e:
                    print("ERROR: update ", e)
                    print(params)
                
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
                    print(f"UPDATE {table_name} SET {set_str} {where_str}", flush = True)
                    cur.execute(f"UPDATE {table_name} SET {set_str} {where_str}")
                    count = cur.rowcount
                except sqlite3.IntegrityError as e:
                    print("ERROR: update ", e)
                    print(sets)
                except sqlite3.InterfaceError as e:
                    print("ERROR: update ", e)
                    print(sets)
                except sqlite3.ProgrammingError as e:
                    print("ERROR: update ", e)
                    print(sets)
                
            conn.commit()
        return count
    


    @classmethod
    def get_max_value(cls, database_name: str, table_name: str, column_name: str):
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                return cur.execute(f"SELECT MAX({column_name}) FROM {table_name}").fetchone()[0]

    @classmethod
    def get_unique_values(cls, 
                          database_name: str, 
                          table_name: str, 
                          column_name: str,
                          count: int = None):
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                res = cur.execute(f"SELECT DISTINCT {column_name} FROM {table_name}")
                if (count is None) or (count == 0):
                    # get all
                    return res.fetchall()
                elif count == 1:
                    return res.fetchone()
                else:
                    return res.fetchmany(count)

        
    @classmethod
    def table_exists(cls, database_name: str, table_name: str = "journal") -> bool:
        table_exists = cls.query(database_name = database_name,
                                 table_name = "sqlite_master",
                                 columns = ["name"],
                                 where_clause = f"type='table' AND name='{table_name}'",
                                 count = 1)
            
        return (table_exists is not None) and (table_exists[0] == table_name)
        

class CommandHistoryTable:
    @classmethod
    def create_command_history_table(cls, database_name: str):
        SQLiteDB.create_table(
            database_name = database_name,
            table_name = "command_history", 
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
                            table_name = "command_history",
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
                         table_name = "command_history",
                         columns = None,
                         where_clause = None,
                         count = None)

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
                     table_name = "command_history",
                     sets = [f"DURATION = {duration}"],
                     where_clause = f"COMMAND_ID = {command_id}"
                    )

class JournalTable:
    @classmethod
    def create_journal_table(cls, database_name: str):
        SQLiteDB.create_table(
            database_name = database_name,
            table_name = "journal", 
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
                           table_name = "journal",
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
                            table_name = "journal",
                            sets = [f"TIME_INVALID_us = {invalidate_time}", 
                                    "STATE = ?"],
                            params = file_states,
                            where_clause = "FILE_ID = ?")
        
    @classmethod
    def mark_as_uploaded_with_duration(cls, database_name: str, 
                            update_args: list):
        return SQLiteDB.update(database_name = database_name, 
                                table_name = "journal",
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
                        table_name = "journal",
                        sets = [f"upload_dtstr='{version}'"],
                        params = upload_args,
                        where_clause="file_id=?")
    
    @classmethod
    def get_latest_version(cls, database_name: str):
        return SQLiteDB.get_max_value(database_name = database_name,
                                      table_name = "journal",
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
            where_clause += f"version = '{version}'"
        

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
        count = kwargs.get("count", None)
            
        return SQLiteDB.query(database_name = database_name,
                        table_name = "journal",
                        columns = ["FILE_ID", "FILEPATH", "SRC_MODTIME_us", "SIZE", "MD5", "MODALITY", "TIME_INVALID_us", "VERSION", "UPLOAD_DTSTR"],
                        where_clause = where_clause,
                        count = count)


    @classmethod
    def get_files(cls, database_name: str, 
                       version: str, modalities: list, 
                       **kwargs):
        
        where_clause = cls._make_where_clause(version, modalities, **kwargs)
        count = kwargs.get("count", None)
            
        return SQLiteDB.query(database_name = database_name,
                        table_name = "journal",
                        columns = ["FILE_ID", "FILEPATH"],
                        where_clause = where_clause,
                        count = count)

    
    # @classmethod
    # def get_unuploaded_files(cls, database_name: str, version: str, modalities: list, count: int = None):
    #     where_clause = "upload_dtstr IS NULL"
        
    #     if version is not None:
    #         where_clause += f" AND version = '{version}'"
        
    #     if (modalities is not None) and (len(modalities) > 0):
    #         modestr = [f"'{m}'" for m in modalities]
    #         where_clause += " AND MODALITY in (" + ",".join(modestr) + ")" 
            
    #     return SQLiteDB.query(database_name = database_name,
    #                     table_name = "journal",
    #                     columns = ["FILEPATH", "FILE_ID", "SIZE", "MD5", "TIME_INVALID_us", "VERSION", "MODALITY"],
    #                     where_clause = where_clause,
    #                     count = count)
    
    # @classmethod
    # def get_active_unuploaded_files(cls, database_name: str, version: str, modalities:list, files:list, count: int = None):
    #     where_clause = "upload_dtstr is NULL AND time_invalid_us is NULL"
        
    #     if version is not None:
    #         where_clause += f" AND version = '{version}'"
            
    #     if (modalities is not None) and (len(modalities) > 0):
    #         modestr = [f"'{m}'" for m in modalities]
    #         where_clause += " AND MODALITY in (" + ",".join(modestr) + ")" 

    #     if len(files) < 1000:
    #         where_clause += " and filepath in (" + ",".join(files) + ")"

    #     return SQLiteDB.query(database_name = database_name,
    #                     table_name = "journal",
    #                     columns = ["file_id", "filepath", "size", "md5", "version", "modality"],
    #                     where_clause = where_clause,
    #                     count = count)
            
    # @classmethod
    # def get_active_files_info(cls, database_name: str, version: str, 
    #                             modalities: list, count: int = None):
        
    #     where_clause = "TIME_INVALID_us IS NULL"
        
    #     if version is not None:
    #         where_clause += f" AND version = '{version}'"         
                    
    #     if (modalities is not None) and (len(modalities) > 0):
    #         modestr = [f"'{m}'" for m in modalities]
    #         where_clause += " AND MODALITY in (" + ",".join(modestr) + ")" 
            
    #     return SQLiteDB.query(database_name = database_name,
    #                     table_name = "journal",
    #                     columns = ["file_id", "filepath", "size", "md5", "modality"],
    #                     where_clause = where_clause,
    #                     count = count)
        
    # @classmethod
    # def get_active_files_info2(cls, database_name: str, version: str, 
    #                             modalities: list, count: int = None):
        
    #     where_clause = "TIME_INVALID_us IS NULL"
        
    #     if version is not None:
    #         where_clause += f" AND version = '{version}'"         
                    
    #     if (modalities is not None) and (len(modalities) > 0):
    #         modestr = [f"'{m}'" for m in modalities]
    #         where_clause += " AND MODALITY in (" + ",".join(modestr) + ")" 

    #     return SQLiteDB.query(database_name, table_name = "journal",
    #                         columns = ["FILEPATH", "FILE_ID", "SIZE", "SRC_MODTIME_us", "MD5", "UPLOAD_DTSTR", "VERSION"],
    #                         where_clause = where_clause,
    #                         count = None)
        
    # @classmethod
    # def get_active_files(cls, database_name: str, version: str, 
    #                      modalities: list, count: int = None):
    #     where_clause = "TIME_INVALID_us IS NULL"
            
    #     if version is not None:
    #         where_clause += f" AND version = '{version}'"
        
    #     if (modalities is not None) and (len(modalities) > 0):
    #         modestr = [f"'{m}'" for m in modalities]
    #         where_clause += " AND MODALITY in (" + ",".join(modestr) + ")" 

    #     return SQLiteDB.query(database_name = database_name,
    #                     table_name = "journal",
    #                     columns = ["FILE_ID", "FILEPATH"],
    #                     where_clause = where_clause,
    #                     count = count)

class JournalTableNew:
    ...
    
