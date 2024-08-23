import os
import shutil
import time
import argparse
 
from journal_tools import restore_manifest, list_uploads, list_manifests
# from generate_manifest import save_command_history, show_command_history

from pathlib import Path
from chorus_upload_journal.upload_tools import config_helper

# create command processor that support subcommands
# https://docs.python.org/3/library/argparse.html#sub-commands

IS_SITE = False
IS_CENTRAL = True

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
                params += f" --{arg_str}"
            else:
                params += f" --{arg_str} {val}"
    
    params = params.strip()
    common_params = common_params.strip()
    print(common_params, command, params) 
    return (common_params, command, params)

# def _getattr(args, argname):
#     out = getattr(args, argname)
#     return out.strip("'").strip("\"") if out else None

# def _show_history(args, config, manifest_fn):    
#     print("Command History: please note account information were not saved.")
#     show_command_history(manifest_fn)

def _print_usage(args, config, manifest_fn):
    # TODO: DONE update this
    print("The following are example usage for each of the commands:")
    print("  Please use 'auth_help' to see help information about authentication methods")
    print("  Please use each subcommand's help to get a detailed list of parameters")
    print("For each command, the following parameters are common and can be specified before the command name:")
    print("  -v or --verbose:  verbose output")
    print("  -c or --config:   config file (defaults to config.toml in the script directory) with storage path locations")
    print("working with manifests:")
    print("  list:      python generate_manifest list-versions")
    print("  revert:    python generate_manifest revert-version --version 20210901120000")
    
# print authentication help info
# def _print_config_usage(args, config, manifest_fn):
#     print("The script requires a configuration file (config.toml file in the script generate_manifest directory) to be populated.")
#     print("This file can be generated by filling in the config_template.toml file.")
#     print("")
#     print("The config.toml file contains 3 sections, each contains one or more paths, and each path can be local or cloud storage.")
#     print("[journal]:  contains the path to the journal file.  This file will typically be in the azure container.")
#     print("[central_path]:  contains the path to the default central storage location.  This is where the files will be uploaded to.")
#     print("[site_path]:  contains the path to the default DGS storage location.  This is where the files will be uploaded from.")
#     print("Site_path may contain subsections for each modality, for example [site_path.Waveforms].  This allows different modality files to be stored in different locations")
#     print("")    
#     print("For each section or subsection, if the path is a cloud path, e.g. s3:// or az://, then the following parameters are required for authentication.")
#     print("Authentication is only needed for update, upload, and verify commands Local file system is assumed to not require authentication.")
#     print("")
#     print("  AWS:   in order of precedence")
#     print("         aws-session-token  (temporary credential)")
#     print("         aws-access-key-id, aws-secre-access-key.  (may be set as env-vars AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
#     print("         aws-profile.  see aws/credentials files in home directory")
#     print("         if none specified, profile `default` is used.")
#     print("  Azure: in order of precedence")
#     print("         account-url with embedded sas token")
#     print("         azure-account-name, azure-sas-token.  constructs an account-url")
#     print("         azure-storage_connection_string.")
#     print("         azure-account-name, azure-account-key. constructs a connection string")
#     print("         connection string can be set as a environment variable: AZURE_STORAGE_CONNECTION_STRING")
#     # print("  Google: (UNTESTED) requires google-application-credentials")
#     # print("         application credentials may be specified on the commandline, or as an environment variable (see subcommand help)")

    
# helper to list known update manifests
def _list_manifests(args, config, manifest_fn):
    print("Uploads known in current manifest: ")
    list_uploads(manifest_fn)
    print("Backed up Manifests: ")
    list_manifests(manifest_fn)
        
# helper to revert to a previous manifest
def _revert_manifest(args, config, manifest_fn):
    revert_time = args.version
    print("Revert to: ", revert_time)
    restore_manifest(manifest_fn, revert_time)
    

if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Generate manifest for a site folder")
    parser.add_argument("-v", "--verbose", help="verbose output", action="store_true")
    parser.add_argument("-c", "--config", help="config file (defaults to config.toml) with storage path locations", required=False)
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    subparsers.required = True
    
    parser_help = subparsers.add_parser("usage", help = "show help information")
    parser_help.set_defaults(func = _print_usage)
    
    #------ authentication help
    # parser_auth = subparsers.add_parser("config_help", help = "show help information about the configuration file")
    # parser_auth.set_defaults(func = _print_config_usage)
    
    # create the parser for the "list" command
    parser_list = subparsers.add_parser("list-versions", help = "list the versions in a manifest database")
    parser_list.set_defaults(func = _list_manifests)
    
    # DANGEROUS
    parser_revert = subparsers.add_parser("revert-version", help = "revert to a previous manifest version.")
    parser_revert.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)
    parser_revert.set_defaults(func = _revert_manifest)

    # # DANGEROUS
    # parser_delete = subparsers.add_parser("delete", help = "delete version in a manifest database")
    # parser_delete.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)
        
    # parse the arguments
    args = parser.parse_args()
    
    # parse the config file
    # get the current script path
    if args.config is not None:
        config_fn = args.config
    else:
        scriptdir = Path(__file__).absolute().parent
        config_fn = str(scriptdir.joinpath("config.toml"))
    if (not Path(config_fn).exists()):
        raise ValueError("Config file does not exist: ", config_fn, " please create one from the config_template.toml file.")
        
    print("Using config file: ", config_fn)
    config = config_helper.load_config(config_fn)
    
    # get the manifest file  (download from cloud to local)
    manifest_fn = config_helper.get_journal_config(config)["path"]
    
    # # === save command history
    # args_dict = _strip_account_info(args)
    # if ("config" not in args_dict.keys()) or (args_dict["config"] is None):
    #     args_dict["config"] = config_fn
    # save_command_history(*(_recreate_params(args_dict)), manifest_fn)
    
    # call the subcommand function.
    args.func(args, config, manifest_fn)
    
    