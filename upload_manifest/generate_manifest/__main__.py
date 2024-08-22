import os
import shutil
import time
import argparse

from generate_manifest import update_manifest 
from generate_manifest import restore_manifest, list_uploads, list_manifests
from generate_manifest import upload_files, verify_files, list_files
from generate_manifest import save_command_history, show_command_history
from generate_manifest import DEFAULT_MODALITIES
from storage_pathlib import FileSystemHelper
from pathlib import Path
import config_helper
import json

# TODO: DONE need to support folder for different file types being located at different places.
#       each location can be checked for specific set of file types.
# TODO: DONE group authentication parameters

# TODO: DONE support Azure SAS token (via account url) and AWS session token
# TODO: DONE capture command history.
# TODO: NOT AT THIS TIME: function to generate bash scripts (azcopy and azcli) for upload instead of using built in.  This would require a verfication and mark_uploaded function.
# TODO: DONE check the sas token to account url conversion.
# TODO: DONE update and upload one or more modality only

# TODO: DONE config file to reduce command line parameters
# TODO: DONE manifest track source path and dest container
# TODO: pull and push journal files from central.

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

# def _getattr(args, argname):
#     out = getattr(args, argname)
#     return out.strip("'").strip("\"") if out else None

def _show_history(args, config, manifest_fn):    
    print("Command History: please note account information were not saved.")
    show_command_history(manifest_fn)

def _print_usage(args, config, manifest_fn):
    # TODO: DONE update this
    print("The following are example usage for each of the commands:")
    print("  Please use 'auth_help' to see help information about authentication methods")
    print("  Please use each subcommand's help to get a detailed list of parameters")
    print("For each command, the following parameters are common and can be specified before the command name:")
    print("  -v or --verbose:  verbose output")
    print("  -c or --config:   config file (defaults to config.toml in the script directory) with storage path locations")
    print("generating manifest")
    print("  update:    python generate_manifest --config ./config.toml update")
    print("             python generate_manifest update --modalities Waveforms,Images,OMOP")
    print("generating upload file list")
    print("  select:    python generate_manifest select")
    print("             python generate_manifest select --version 20210901120000 -f filelist.txt")
    print("uploading to and verifying with central")
    print("  upload:    python generate_manifest upload ")
    print("             python generate_manifest upload --modalities Images")
    print("  verify:    python generate_manifest verify --modalities Imgaes --version 20210901120000")
    print("working with manifests:")
    print("  list:      python generate_manifest list-versions")
    print("  revert:    python generate_manifest revert-version --version 20210901120000")
    
# print authentication help info
def _print_config_usage(args, config, manifest_fn):
    print("The script requires a configuration file (config.toml file in the script generate_manifest directory) to be populated.")
    print("This file can be generated by filling in the config.toml.template file.")
    print("")
    print("The config.toml file contains 3 sections, each contains one or more paths, and each path can be local or cloud storage.")
    print("[journal]:  contains the path to the journal file.  This file will typically be in the azure container.")
    print("[central_path]:  contains the path to the default central storage location.  This is where the files will be uploaded to.")
    print("[site_path]:  contains the path to the default DGS storage location.  This is where the files will be uploaded from.")
    print("Site_path may contain subsections for each modality, for example [site_path.Waveforms].  This allows different modality files to be stored in different locations")
    print("")    
    print("For each section or subsection, if the path is a cloud path, e.g. s3:// or az://, then the following parameters are required for authentication.")
    print("Authentication is only needed for update, upload, and verify commands Local file system is assumed to not require authentication.")
    print("")
    print("  AWS:   in order of precedence")
    print("         aws-session-token  (temporary credential)")
    print("         aws-access-key-id, aws-secre-access-key.  (may be set as env-vars AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
    print("         aws-profile.  see aws/credentials files in home directory")
    print("         if none specified, profile `default` is used.")
    print("  Azure: in order of precedence")
    print("         account-url with embedded sas token")
    print("         azure-account-name, azure-sas-token.  constructs an account-url")
    print("         azure-storage_connection_string.")
    print("         azure-account-name, azure-account-key. constructs a connection string")
    print("         connection string can be set as a environment variable: AZURE_STORAGE_CONNECTION_STRING")
    # print("  Google: (UNTESTED) requires google-application-credentials")
    # print("         application credentials may be specified on the commandline, or as an environment variable (see subcommand help)")

# internal helper to create the cloud client
def __make_aws_client(auth_params: dict):
    # Azure format for account_url
    aws_session_token = config_helper.get_auth_param(auth_params, "aws_session_token")
    aws_profile = config_helper.get_auth_param(auth_params, "aws_profile")
    aws_access_key_id = config_helper.get_auth_param(auth_params, "aws_access_key_id")    
    aws_secret_access_key = config_helper.get_auth_param(auth_params, "aws_secret_access_key")
    
    from cloudpathlib import S3Client
    if aws_session_token:  # preferred.
        # session token specified, then use it
        return S3Client(aws_session_token = aws_session_token)
    elif aws_access_key_id and aws_secret_access_key:
        # access key and secret key specified, then use it
        return S3Client(access_key_id = aws_access_key_id, 
                        secret_access_key = aws_secret_access_key)
    elif aws_profile:
        # profile specified, then use it
        return S3Client(profile_name = aws_profile)
    
    aws_session_token = aws_session_token if aws_session_token else os.environ.get("AWS_SESSION_TOKEN")
    aws_profile = aws_profile if aws_profile else "default"
    aws_access_key_id = aws_access_key_id if aws_access_key_id else os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = aws_secret_access_key if aws_secret_access_key else os.environ.get("AWS_SECRET_ACCESS_KEY")
    
    if aws_session_token:  # preferred.
        # session token specified, then use it
        return S3Client(aws_session_token = aws_session_token)
    elif aws_access_key_id and aws_secret_access_key:
        # access key and secret key specified, then use it
        return S3Client(access_key_id = aws_access_key_id, 
                        secret_access_key = aws_secret_access_key)
    elif aws_profile:
        # profile specified, then use it
        return S3Client(profile_name = aws_profile)
        
    # default profile
    return S3Client()

# internal helper to create the cloud client
def __make_az_client(auth_params: dict):
    # Azure format for account_url
        
    # parse out all the relevant argument
    azure_account_url = config_helper.get_auth_param(auth_params, "azure_account_url")
    azure_sas_token = config_helper.get_auth_param(auth_params, "azure_sas_token")
    azure_account_name = config_helper.get_auth_param(auth_params, "azure_account_name")
    if azure_account_url is None:
        azure_account_url = f"https://{azure_account_name}.blob.core.windows.net/?{azure_sas_token}" if azure_account_name and azure_sas_token else None

    # format for account url for client.  container is not discarded unlike stated.  so must use format like below.
    # example: 'https://{account_url}.blob.core.windows.net/?{sas_token}
    # https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    # format for path:  az://{container}/...
    # note if container is specified in account_url, we will get duplicates.
    
    azure_storage_connection_string = config_helper.get_auth_param(auth_params, "azure_storage_connection_string")
    azure_account_key = config_helper.get_auth_param(auth_params, "azure_account_key")
    if azure_storage_connection_string is None:
        azure_storage_connection_string = f"DefaultEndpointsProtocol=https;AccountName={azure_account_name};AccountKey={azure_account_key};EndpointSuffix=core.windows.net" if azure_account_name and azure_account_key else None 
    
    from cloudpathlib import AzureBlobClient
    if azure_account_url:
        return AzureBlobClient(account_url=azure_account_url)
    elif azure_storage_connection_string:
        # connection string specified, then use it
        return AzureBlobClient(connection_string = azure_storage_connection_string)
    
    azure_storage_connection_string = azure_storage_connection_string if azure_storage_connection_string else os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    
    if azure_account_url:
        return AzureBlobClient(account_url=azure_account_url)
    elif azure_storage_connection_string:
        # connection string specified, then use it
        return AzureBlobClient(connection_string = azure_storage_connection_string)
    else:
        raise ValueError("No viable Azure account info available to open connection")

# # internal helper to create the cloud client
# def __make_gs_client(auth_params: dict):
#     from cloudpathlib import GoogleCloudClient
#     if google_application_credentials:
#         # application credentials specified, then use it
#         return GoogleCloudClient(application_credentials = google_application_credentials)
#     elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
#         # application credentials specified in environment variables, then use it
#         return GoogleCloudClient(application_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
#     else:
#         raise ValueError("No viable Google application credentials to open connection")

# internal helper to create the cloud client
def _make_client(cloud_params:dict):
    path_str = cloud_params["path"]
    
    if (path_str.startswith("s3://")):
        return __make_aws_client(cloud_params)
    elif (path_str.startswith("az://")):
        return __make_az_client(cloud_params)
    # elif (path_str.startswith("gs://")):
    #     return __make_gs_client(args, for_central = for_central)
    elif ("://" in path_str):
        raise ValueError("Unknown cloud storage provider.  Supports s3, az")
    else:
        return None
    
        
# helper to call update manifest
def _update_manifest(args, config, manifest_fn):
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        

    # for each modality, create the file system help and process
    for mod, mod_config in mod_configs.items():        
        # create the file system helper    
        datafs = FileSystemHelper(mod_config["path"], client = _make_client(mod_config))

        print("Update Manifest ", manifest_fn, " for ", mod)
        update_manifest(datafs, modalities = [mod], databasename = manifest_fn, verbose = args.verbose)
    
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
    
def _write_files(file_list, dt, outfilename, outtype : str = "list"):
    if args.output_file is None:
        return
    fn = outfilename + "_" + dt
    with open(fn, 'w') as f:
        
#         if (outtype == "azcli"):
#             f.write("#!/bin/sh\n\n")
#             f.write("script to upload files to azure for upload date " + dt + "\n")

#             f.write("# set environment variables\n")
#             f.write("AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1\n")
            
#             f.write("# login to azure\n")
#             f.write("az login\n")
            
#             f.write("# upload files for \n")
#             f.write("CHORUS_URL = {PLEASE FILL IN}")
            
#             for f in file_list:
#                 f.write("az storage blob upload --account-name {account} --sas-token {sas_token} --container-name {container} --name {fn} --file {root}{fn}\n")            
            
#             f.write("# update manifest with upload date-time " + dt + "\n")
#             account_url = something
#             az_path = something
#             f.write("python generate_manifest verify --central-path " + central_url + " --central-azure-account-url " + account_url + " --manifest ./journal.db --set-time " + dt + "\n")
#         elif (outtype == "azcopy"):
#             ...
#         else:
        f.write("\n".join(file_list))
            
# helper to display/save files to upload
def _select_files(args, config, manifest_fn):
    
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    file_list = list_files(manifest_fn, version = args.version, modalities = mods, verbose=args.verbose)
    dt = args.version if args.version else "un-uploaded"
    _write_files(file_list, dt, args.output_file)
    

# helper to upload files
def _upload_files(args, config, manifest_fn):

    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        

    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    # if (args.site_path is None):
    #     raise ValueError("site path is required")
    
    # if (args.central_path is None):
    #     raise ValueError("central path is required")
    
    for mod, mod_config in mod_configs.items():
        sitefs = FileSystemHelper(mod_config["path"], client = _make_client(mod_config))
        upload_files(sitefs, centralfs, manifest_fn, modalities = [mod], amend = args.amend, verbose = args.verbose)
    
    
# helper to report file verification
def _verify_files(args, config, manifest_fn):
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        

    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    verify_files(centralfs, manifest_fn, version = args.version, modalities = mods, verbose = args.verbose)
    

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
    parser_auth = subparsers.add_parser("config_help", help = "show help information about the configuration file")
    parser_auth.set_defaults(func = _print_config_usage)
    
    #------ create the parser for the "update" command
    parser_update = subparsers.add_parser("update", help = "create or update the current manifest")
    parser_update.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_update.set_defaults(func = _update_manifest)
    
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

    # create the parser for the "select" command
    parser_select = subparsers.add_parser("select", help = "list files to upload")
    parser_select.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to all un-uploaded files.", required=False)
    parser_select.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to all.  case sensitive.", required=False)
    parser_select.add_argument("--output-file", help="output file", required=False)
    # parser_select.add_argument("--output-type", help="the output file type: [list | azcli | azcopy].  azcli and azcopy are executable scripts.", required=False)
    parser_select.set_defaults(func = _select_files)
    
    parser_upload = subparsers.add_parser("upload", help = "upload files to server")
    parser_upload.add_argument("--version", help="datetime of an upload (use list to get date times). defaults to all un-uploaded files", required=False)
    parser_upload.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_upload.add_argument("--amend", help="amend the last upload", action="store_true")
    # optional list of files. if not present, use current manifest
    # when upload, mark the manifest version as uploaded.
    parser_upload.set_defaults(func = _upload_files)
    
    parser_verify = subparsers.add_parser("verify", help = "verify manifest with file system")
    parser_verify.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to most recent uploaded files.", required=False)
    parser_verify.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_verify.set_defaults(func = _verify_files)
        
    parser_history = subparsers.add_parser("history", help = "show command history")
    parser_history.set_defaults(func = _show_history)
        
    # parse the arguments
    args = parser.parse_args()
    
    #============= locate and parse the config file
    # get the current script path
    if args.config is not None:
        config_fn = args.config
    else:
        scriptdir = Path(__file__).absolute().parent
        config_fn = str(scriptdir.joinpath("config.toml"))
    if (not Path(config_fn).exists()):
        raise ValueError("Config file does not exist: ", config_fn, ". please create one from the config.toml.template file.")
        
    print("Using config file: ", config_fn)
    config = config_helper.load_config(config_fn)
    
    
    #============= get the manifest file  (download from cloud to local)
    manifest_fn = config_helper.get_journal_config(config)["path"]
    # if (manifest_fn.startswith("az://")):
    #     remote_fn = FileSystemHelper(manifest_fn, client = _make_client(config_helper.get_journal_config(config)))
    #     local_manifest = manifest_fs.download_file(manifest_fn, manifest_fn)
    # elif (manifest_fn.startswith("s3://")):
    #     ...
        
    #     # push up a lock file.
    
    # === save command history
    args_dict = _strip_account_info(args)
    if ("config" not in args_dict.keys()) or (args_dict["config"] is None):
        args_dict["config"] = config_fn
    save_command_history(*(_recreate_params(args_dict)), *(_get_paths_for_history(args, config)),
                         manifest_fn, )
    
    # call the subcommand function.
    args.func(args, config, manifest_fn)
    
    
    # upload the manifest and unlock.
    
    