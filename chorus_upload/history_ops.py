import time
import chorus_upload.config_helper as config_helper
import json
from chorus_upload.journaldb_ops import JournalDB

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
        
    exclude_params = ["command", "verbose", "config"]
    if (command in ["journal", "file"]):    
        full_command = command + " " + filtered_args[command + "_command"] # concate command and subcommand
        exclude_params.append(command + "_command")  # fixed bug where command was previously concate of command and subcommand.
            
    # print("DEBUG: exclude commands", exclude_params, "filtered args", filtered_args)
            
    # cat the rest
    for arg, val in filtered_args.items():
        if (arg not in exclude_params) and (val is not None):
            arg_str = arg.replace("_", "-")
            if type(val) == bool:
                if val == True:
                    params += f" --{arg_str}"
            else:
                params += f" --{arg_str} {val}"
    
    params = params.strip()
    common_params = common_params.strip()
    print(common_params, full_command, params) 
    return (common_params, full_command, params)

def _get_paths_for_history(args, config):
    command = args.command
    # if (command in ["journal", "file"]):    
    #     command = command + " " + args[command + "_command"]
    
    if ((command == "file") and 
        (args.file_command in ["upload", "verify", "mark_as_uploaded_central", "mark_as_uploaded_local"])):
        dest_path = config_helper.get_central_config(config)["path"]
    else:
        dest_path = None
    
    src_paths_json = None
    default_modalities = config_helper.get_modalities(config)  
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        
    if (((command == "file") and 
         (args.file_command in ["upload"])) or 
        ((command == "journal") and 
         (args.journal_command in ["update"]))):
        src_paths = { mod: mod_config["path"] for mod, mod_config in mod_configs.items() }
        # convert dict to json string
        if len(src_paths) > 0:
            src_paths_json = json.dumps(src_paths)

    return (src_paths_json, dest_path)

def save_command_history(common_params:str, cmd:str, parameters:str, 
                         src_paths:str, dest_path:str,
                         databasename:str):
    """
    Saves the command history to a SQLite database.

    Parameters:
    - cmd (str): The command that was executed.
    - parameters (str): The parameters passed to the command.
    - databasename (str): The name of the SQLite database file. Default is "journal.db".
    """
    
    curtimestamp = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
    # curtimestamp = int(math.floor(time.time() * 1e6))

    JournalDB.create_command_history_table(databasename)
 
    _, command_id = JournalDB.insert_command_history_entry(databasename,
                                             (curtimestamp, common_params, cmd, parameters, src_paths, dest_path))
    
    return command_id


def update_command_completion(command_id:int, duration:float, databasename: str):
    """
    Update the duration of a command in the command history.
    Parameters:
    - command_id (int): The ID of the command to update.
    - duration (float): The new duration of the command.
    - databasename (str): The name of the database file to connect to. Default is "journal.db".
    """
    
    return JournalDB.update1(database_name = databasename, 
                     table_name = "command_history",
                     sets = [f"DURATION = {duration}"],
                     where_clause = "COMMAND_ID = command_id"
                    )
        
    
def show_command_history(databasename: str):
    """
    Retrieve and display the command history from the specified SQLite database.

    Parameters:
    - databasename (str): The name of the SQLite database file. Default is "journal.db".

    Returns:
    None
    """
    
    commands = JournalDB.query(database_name = databasename,
                               table_name = "command_history",
                               columns = None,
                               where_clause = None,
                               count = None)
    
    for (id, timestamp, common_params, cmd, params, src_paths, dest_path, duration) in commands:
        # convert microsecond timestamp to datetime
        # dt = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(int(timestamp/1e6)))
        print(f"  {id}\t{timestamp}:\t{common_params} {cmd} {params}.  {src_paths}->{dest_path}.  elapsed {duration}s")

