import sqlite3
import time
import chorus_upload_journal.upload_tools.config_helper as config_helper
from chorus_upload_journal.upload_tools.defaults import DEFAULT_MODALITIES
import json

def _strip_account_info(args):
    filtered_args = {}
    for arg in vars(args):
        # if ("aws_" in arg) or ("azure_" in arg) or ("google_" in arg):
        #     continue
        if (arg == "func"):
            continue
        filtered_args[arg] = getattr(args, arg)
        
    return filtered_args

def _recreate_params(filtered_args: dict):
    params = ""
    common_params = ""
    command = ""
    # catch verbose and config first
    for arg, val in filtered_args.items():
        if (arg in ["verbose", "config"]) and (val is not None):
            arg_str = arg.replace("_", "-")
            if (type(val) == bool):
                if val == True:
                    common_params += f" --{arg_str}"
            else:
                common_params += f" --{arg_str} {val}"
    
    if ("command" in filtered_args.keys()) and (filtered_args["command"] is not None):
        command = filtered_args["command"]
    
    # cat the rest
    for arg, val in filtered_args.items():
        if (arg not in ["command", "verbose", "config"]) and (val is not None):
            arg_str = arg.replace("_", "-")
            if type(val) == bool:
                if val == True:
                    params += f" --{arg_str}"
            else:
                params += f" --{arg_str} {val}"
    
    params = params.strip()
    common_params = common_params.strip()
    print(common_params, command, params) 
    return (common_params, command, params)

def _get_paths_for_history(args, config):
    command = args.command
    
    if (command in ["update", "verify", "upload"]):
        dest_path = config_helper.get_central_config(config)["path"]
    else:
        dest_path = None
    
    src_paths_json = None    
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        
    if (command in ["update", "upload", "select"]):
        src_paths = { mod: mod_config["path"] for mod, mod_config in mod_configs.items() }
        # convert dict to json string
        if len(src_paths) > 0:
            src_paths_json = json.dumps(src_paths)

    return (src_paths_json, dest_path)

def save_command_history(common_params:str, cmd:str, parameters:str, 
                         src_paths:str, dest_path:str,
                         databasename="journal.db"):
    """
    Saves the command history to a SQLite database.

    Parameters:
    - cmd (str): The command that was executed.
    - parameters (str): The parameters passed to the command.
    - databasename (str): The name of the SQLite database file. Default is "journal.db".
    """
    
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()
    
    curtimestamp = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
    # curtimestamp = int(math.floor(time.time() * 1e6))

    cur.execute("CREATE TABLE IF NOT EXISTS command_history (\
                        COMMAND_ID INTEGER PRIMARY KEY AUTOINCREMENT, \
                        DATETIME TEXT, \
                        COMMON_PARAMS TEXT, \
                        COMMAND TEXT, \
                        PARAMS TEXT, \
                        SRC_PATHS TEXT, \
                        DEST_PATH TEXT, \
                        Duration TEXT)")  # datetime string of the upload.
 
    cur.execute("INSERT INTO command_history ( \
        DATETIME, COMMON_PARAMS, COMMAND, PARAMS, SRC_PATHS, DEST_PATH \
        ) VALUES(?, ?, ?, ?, ?, ?)",
        (curtimestamp, common_params, cmd, parameters, src_paths, dest_path))
    con.commit()
    
    # get the inserted record id
    command_id = cur.lastrowid
    
    con.close()
    
    return command_id


def update_command_completion(command_id:int, duration:float, databasename="journal.db"):
    """
    Update the duration of a command in the command history.
    Parameters:
    - command_id (int): The ID of the command to update.
    - duration (float): The new duration of the command.
    - databasename (str): The name of the database file to connect to. Default is "journal.db".
    """
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    cur.execute("UPDATE command_history SET DURATION=? WHERE COMMAND_ID=?", 
                (duration, command_id))
    con.commit()
    
    con.close()    
    
    
def show_command_history(databasename="journal.db"):
    """
    Retrieve and display the command history from the specified SQLite database.

    Parameters:
    - databasename (str): The name of the SQLite database file. Default is "journal.db".

    Returns:
    None
    """
    con = sqlite3.connect(databasename, check_same_thread=False)
    cur = con.cursor()

    # get all the commands
    commands = cur.execute("SELECT * FROM command_history").fetchall()
    con.close()
    
    for (id, timestamp, common_params, cmd, params, src_paths, dest_path, duration) in commands:
        # convert microsecond timestamp to datetime
        # dt = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(int(timestamp/1e6)))
        print(f"  {id}\t{timestamp}:\t{common_params} {cmd} {params}.  {src_paths}->{dest_path}.  elapse {duration}s")

