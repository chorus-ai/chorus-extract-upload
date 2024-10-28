import sqlite3
from contextlib import closing



class JournalDB:
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
        lastrowid = None
        with sqlite3.connect(database_name, check_same_thread=False) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    cur.executemany(f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders})", params)
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
                    cur.execute(f"UPDATE {table_name} SET {set_str} {where_str}")
                    count = cur.rowcount
                except sqlite3.IntegrityError as e:
                    print("ERROR: update ", e)
                    print(sets)
                except sqlite3.InterfaceError as e:
                    print("ERROR: update ", e)
                    print(params)
                except sqlite3.ProgrammingError as e:
                    print("ERROR: update ", e)
                    print(params)
                
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
        

            
    @classmethod
    def create_journal_table(cls, database_name: str):
        cls.create_table(
            database_name = database_name,
            table_name = "journal", 
            column_types = [
                ("FILE_ID", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
                ("PERSON_ID", "INTEGER", ""), 
                ("FILEPATH", "TEXT", ""), 
                ("MODALITY", "TEXT", ""), 
                ("SRC_MODTIME_us", "INTEGER", ""), 
                ("SIZE", "INTEGER", ""), 
                ("MD5", "TEXT", ""), 
                ("TIME_VALID_us", "INTEGER", ""), 
                ("TIME_INVALID_us", "INTEGER", ""), 
                ("UPLOAD_DTSTR", "TEXT", ""), 
                ("VERSION", "TEXT", ""), 
                ("STATE", "TEXT", ""), 
                ("MD5_DURATION", "REAL", ""), 
                ("UPLOAD_DURATION", "REAL", ""), 
                ("VERIFY_DURATION", "REAL", "")
                ],
            index_on = None)
        # [
        #         "FILEPATH", 
        #         "MODALITY", 
        #         "VERSION", 
        #         "UPLOAD_DTSTR", 
        #         "TIME_INVALID_us"])
        
    # list is a list of tuples
    @classmethod
    def insert_journal_entries(cls, database_name: str, params: list) -> int:
        return cls.insert(database_name = database_name, 
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
        
    @classmethod
    def create_command_history_table(cls, database_name: str):
        cls.create_table(
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
        return cls.insert(database_name = database_name,
                            table_name = "command_history",
                            columns = [
                                 "DATETIME", 
                                 "COMMON_PARAMS", 
                                 "COMMAND", 
                                 "PARAMS", 
                                 "SRC_PATHS", 
                                 "DEST_PATH"
                                 ],
                            params = [params])


    # file_states is a list of tuples of form (state, file_id)
    # state can be "DELETED", "OUTDATED"
    @classmethod
    def inactivate_journal_entries(cls, database_name: str, 
                          table_name: str, 
                          invalidate_time: int,
                          file_states: list):
        return cls.update(database_name = database_name,
                            table_name = table_name,
                            sets = [f"TIME_INVALID_us = {invalidate_time}", 
                                    "STATE = ?"],
                            params = file_states,
                            where_clause = "FILE_ID = ?")
        
        
