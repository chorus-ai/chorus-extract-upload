import os
import shutil
import time
import argparse

from generate_manifest import update_manifest 
from generate_manifest import restore_manifest, list_uploads, list_manifests
from generate_manifest import upload_files, verify_files, list_files
from generate_manifest import save_command_history, show_command_history
from storage_pathlib import FileSystemHelper

# TODO: DONE need to support folder for different file types being located at different places.
#       each location can be checked for specific set of file types.
# TODO: DONE group authentication parameters

# TODO: DONE support Azure SAS token (via account url) and AWS session token
# TODO: DONE capture command history.
# TODO: NOT AT THIS TIME: function to generate bash scripts (azcopy and azcli) for upload instead of using built in.  This would require a verfication and mark_uploaded function.
# TODO: DONE check the sas token to account url conversion.
# TODO: DONE update and upload one or more modality only
# TODO: hide some login options.

# create command processor that support subcommands
# https://docs.python.org/3/library/argparse.html#sub-commands

IS_SITE = False
IS_CENTRAL = True

def _strip_account_info(args):
    filtered_args = {}
    for arg in vars(args):
        if ("aws_" in arg) or ("azure_" in arg) or ("google_" in arg):
            continue
        filtered_args[arg] = getattr(args, arg)
        
    return filtered_args

def _recreate_params(filtered_args: dict):
    params = ""
    command = ""
    for arg, val in filtered_args.items():
        if arg == "func":
            continue
        if arg == "command":
            command = val
            continue
        if  val is not None:
            arg_str = arg.replace("_", "-")
            params += f" --{arg_str} {val}"
    params = params.strip()
    print(command, params) 
    return (command, params)

def _show_history(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    print("Command History: please note account information were not saved.")
    show_command_history(manifest_fn)

def _print_usage(args):
    # TODO: update
    print("The following are example usage for each of the commands:")
    print("  Please use 'auth_help' to see help information about authentication methods")
    print("  Please use each subcommand's help to get a detailed list of parameters")
    print("generating manifest")
    print("  update:    python generate_manifest --manifest ./journal.db update --site-path /mnt/x/project/chorus/data")
    print("             python generate_manifest --manifest ./journal.db update --site-path s3://chorus --site-aws-profile default")
    print("             python generate_manifest --manifest ./journal.db update --site-path s3://chorus --site-aws-access-key-id=xxxxxxx; --site-aws-secret-access-key=yyyyyyyyyy")
    print("             python generate_manifest --manifest ./journal.db update --site-path az://chorus --site-azure-account-name=yyyyyyyyyy --site-azure-sas-token XXXXXXX")
    print("             AZURE_CONNECTION_STRING=\"DefaultEndpointsProtocol=https;AccountName=xxxxxx;AccountKey=yyyyyyyyyyyy;EndpointSuffix=core.windows.net\"; python generate_manifest --manifest ./journal.db update --site-path az://chorus")
    print("generating upload file list")
    print("  select:    python generate_manifest --manifest ./journal.db select")
    print("             python generate_manifest --manifest ./journal.db select --version 20210901120000 -f filelist.txt")
    print("uploading to and verifying with central")
    print("  upload:    python generate_manifest --manifest ./journal.db upload --site-path /mnt/x/project/chorus/data --central-path az://chorus/site")
    print("             python generate_manifest --manifest ./journal.db upload --site-path s3://chorus --site-aws-profile default --central-path az://chorus/site --central-account-name abcd --central-account-key xxxxxxxxxxx")
    print("             AZURE_CONNECTION_STRING=\"DefaultEndpointsProtocol=https;AccountName=xxxxxx;AccountKey=yyyyyyyyyyyy;EndpointSuffix=core.windows.net\"; python generate_manifest verify --central-path az://chorus/site --version 20210901120000")
    print("  verify:    python generate_manifest --manifest ./journal.db verify --central-path az://chorus/site --central-account-name abcd --central-account-key xxxxxxxxxxx --version 20210901120000")
    print("working with manifests:")
    print("  list:      python generate_manifest --manifest ./journal.db list")
    print("  revert:    python generate_manifest --manifest ./journal.db revert --version 20210901120000")
    
# print authentication help info
def _print_auth_usage(args):
    print("The following commands require authentication WHEN using cloud storage:")
    print("  update: requires site directory (onprem or cloud) and site manifest file (onprem or cloud)")
    print("  upload: requires site directory/manifest (onprem or cloud) and central directory (onprem or cloud)")
    print("  verify: requires updated site manifest file (onprem or cloud) and central directory (onprem or cloud)")
    print("Local file system is assumed to not require authentication.")
    print("Cloud storage requires authentication FOR EACH DIRECTORY/URL and parameters depends on cloud service provider")
    print("  AWS:   requires in ordre of precedence")
    print("         aws-session-token  (temporary credential)")
    print("         aws-access-key-id, aws-secre-access-key.  (may be set as env-var)")
    print("         aws-profile.  see aws/credentials files in home directory")
    print("         if none specified, profile `default` is used.")
    print("  Azure: requires in order of precendence")
    print("         account-url.   with sas token")
    print("         azure-account-name, azure-sas-token.  constructs an account-url")
    print("         azure-storage_connection_string.")
    print("         azure-account-name, azure-account-key. constructs a connection string")
    print("         connection string can be set as a environment variable (see subcommand help)")
    # print("  Google: (UNTESTED) requires google-application-credentials")
    # print("         application credentials may be specified on the commandline, or as an environment variable (see subcommand help)")

def _add_data_dir_args(parser, for_central: bool):
    prefix = "central" if for_central else "site"
    parser.add_argument(f"--{prefix}-path", help=f"onprem/cloud {prefix} data folder. Examples: az://container/, aws://container/, or /mnt/x/project/chorus/data", required=True)
    
def _add_manifest_args(parser):
    parser.add_argument("-m", "--manifest", help="onprem site manifest file (defaults to ./journal.db). this should be a local file, and will be uploaded during 'upload'", required=True)

# helper for command line argument set up
def _add_cloud_credential_args(parser, for_central: bool):
    prefix = "central" if for_central else "site"
    
    aws_group = parser.add_argument_group(title = f"{prefix} aws authentication parameters")    
    aws_group.add_argument(f"--{prefix}-aws-access-key-id", help="AWS access key. Can also specify as environment variable AWS_ACCESS_KEY_ID", required=False)
    aws_group.add_argument(f"--{prefix}-aws-secret-access-key", help="AWS access secret key. Can also specify as environment variable AWS_SECRET_ACCESS_KEY", required=False)
    aws_group.add_argument(f"--{prefix}-aws-session-token", help="AWS session token. Can also specify as environment variable AWS_SESSION_TOKEN", required=False)
    aws_group.add_argument(f"--{prefix}-aws-profile", help="AWS access secret key.  if no authentication parameters are specified, default profile is used.", required=False)
    
    azure_group = parser.add_argument_group(title = f"{prefix} azure authentication parameters")
    azure_group.add_argument(f"--{prefix}-azure-account-url", help="Azure account url. should contain SAS token", required=False)
    azure_group.add_argument(f"--{prefix}-azure-sas-token", help="Azure SAS token.", required=False)
    azure_group.add_argument(f"--{prefix}-azure-account-name", help="Azure account name.", required=False)
    azure_group.add_argument(f"--{prefix}-azure-account-key", help="Azure account key.", required=False)
    azure_group.add_argument(f"--{prefix}-azure-storage_connection_string", help="Azure account connection string. . can also specify as environment variable AZURE_STORAGE_CONNECTION_STRING", required=False)
    
    # google_group = parser.add_argument_group(title = f"{prefix} google authentication parameters")
    # google_group.add_argument(f"--{prefix}-google-application-credentials", help="Google application credentials. can also specify as environment variable GOOGLE_APPLICATION_CREDENTIALS", required=False)

# internal helper to create the cloud client
def __make_aws_client(args, for_central: bool):
    # Azure format for account_url
    prefix = "central" if for_central else "site"

    aws_session_token = getattr(args, f"{prefix}_aws_session_token")
    aws_profile = getattr(args, f"{prefix}_aws_profile")    
    aws_access_key_id = getattr(args, f"{prefix}_aws_access_key_id")
    aws_secret_access_key = getattr(args, f"{prefix}_aws_secret_access_key")
    
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
    aws_acesss_key_id = aws_access_key_id if aws_access_key_id else os.environ.get("AWS_ACCESS_KEY_ID")
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
def __make_az_client(args, for_central: bool):
    # Azure format for account_url
    prefix = "central" if for_central else "site"
    
    # parse out all the relevant argument
    azure_account_url = getattr(args, f"{prefix}_azure_account_url")
    azure_sas_token = getattr(args, f"{prefix}_azure_sas_token")
    azure_account_name = getattr(args, f"{prefix}_azure_account_name")
    if azure_account_url is None:
        azure_account_url = f"https://{azure_account_name}.blob.core.windows.net/?{azure_sas_token}" if azure_account_name and azure_sas_token else None

    # format for account url for client.  container is not discarded unlike stated.  so must use format like below.
    # example: 'https://{account_url}.blob.core.windows.net/?{sas_token}
    # https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    # format for path:  az://{container}/...
    # note if container is specified in account_url, we will get duplicates.
    
    azure_storage_connection_string = getattr(args, f"{prefix}_azure_storage_connection_string")
    azure_account_key = getattr(args, f"{prefix}_azure_account_key")
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
# def __make_gs_client(google_application_credentials:str):
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
def _make_client(args, for_central: bool):
    path_str = args.central_path if for_central else args.site_path
    
    if (path_str.startswith("s3://")):
        return __make_aws_client(args, for_central = for_central)
    elif (path_str.startswith("az://")):
        return __make_az_client(args, for_central = for_central)
    # elif (path_str.startswith("gs://")):
    #     return __make_gs_client(args, for_central = for_central)
    elif ("://" in path_str):
        raise ValueError("Unknown cloud storage provider.  Supports s3, az")
    else:
        return None
        
# helper to call update manifest
def _update_manifest(args):
    
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")

    datafs = FileSystemHelper(args.site_path, client = _make_client(args, IS_SITE))

    print("Update Manifest: ", manifest_fn)
    mods = args.modalities.split(',') if args.modalities else None
    update_manifest(datafs, modalities = mods, databasename = manifest_fn, verbose = args.verbose)
    
# helper to list known update manifests
def _list_manifests(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    print("Backed up Manifests: ")
    list_manifests(manifest_fn)
    print("Uploads known in current manifest: ")
    list_uploads(manifest_fn)
    
# helper to revert to a previous manifest
def _revert_manifest(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
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
def _select_files(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    
    mods = args.modalities.split(',') if args.modalities else None
    file_list = list_files(manifest_fn, version = args.version, modalities = mods, verbose=args.verbose)
    dt = args.version if args.version else "unuploaded"
    _write_files(file_list, dt, args.output_file)
    

# helper to upload files
def _upload_files(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")

    if (args.site_path is None):
        raise ValueError("site path is required")
    
    if (args.central_path is None):
        raise ValueError("central path is required")
    
    sitefs = FileSystemHelper(args.site_path, client = _make_client(args, IS_SITE))
    
    centralfs = FileSystemHelper(args.central_path, client = _make_client(args, IS_CENTRAL))

    mods = args.modalities.split(',') if args.modalities else None
    upload_files(sitefs, centralfs, manifest_fn, modalities = mods, amend = args.amend, verbose = args.verbose)
    
# helper to report file verification
def _verify_files(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    central_fs = FileSystemHelper(args.central_path, client = _make_client(args, IS_CENTRAL))
    mods = args.modalities.split(',') if args.modalities else None
    verify_files(central_fs, manifest_fn, version = args.version, modalities = mods, verbose = args.verbose)
    

if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Generate manifest for a site folder")
    parser.add_argument("-v", "--verbose", help="verbose output", action="store_true")
    parser.add_argument("-m", "--manifest", help="onprem site manifest file (defaults to ./journal.db). this should be a local file, and will be uploaded during 'upload'", required=True)
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    subparsers.required = True
    
    parser_help = subparsers.add_parser("usage", help = "show help information")
    parser_help.set_defaults(func = _print_usage)
    
    #------ authentication help
    parser_auth = subparsers.add_parser("auth_help", help = "show help information about authentication methods")
    parser_auth.set_defaults(func = _print_auth_usage)
    
    #------ create the parser for the "update" command
    parser_update = subparsers.add_parser("update", help = "create or update the current manifest")
    # _add_manifest_args(parser_update)
    _add_data_dir_args(parser_update, IS_SITE)
    _add_cloud_credential_args(parser_update, IS_SITE)
    parser_update.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_update.set_defaults(func = _update_manifest)
    
    # create the parser for the "list" command
    parser_list = subparsers.add_parser("list", help = "list the versions in a manifest database")
    # _add_manifest_args(parser_list)
    parser_list.set_defaults(func = _list_manifests)
    
    # DANGEROUS
    parser_revert = subparsers.add_parser("revert", help = "revert to a previous manifest version.")
    # _add_manifest_args(parser_revert)
    parser_revert.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)
    parser_revert.set_defaults(func = _revert_manifest)

    # # DANGEROUS
    # parser_delete = subparsers.add_parser("delete", help = "delete version in a manifest database")
    # _add_manifest_args(parser_delete)
    # parser_delete.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)

    # create the parser for the "select" command
    parser_select = subparsers.add_parser("select", help = "list files to upload")
    # _add_manifest_args(parser_select)
    parser_select.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to all un-uploaded files.", required=False)
    parser_select.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to all.  case sensitive.", required=False)
    parser_select.add_argument("--output-file", help="output file", required=False)
    # parser_select.add_argument("--output-type", help="the output file type: [list | azcli | azcopy].  azcli and azcopy are executable scripts.", required=False)
    parser_select.set_defaults(func = _select_files)
    
    parser_upload = subparsers.add_parser("upload", help = "upload files to server")
    # _add_manifest_args(parser_upload)
    parser_upload.add_argument("--version", help="datetime of an upload (use list to get date times). defaults to all un-uploaded files", required=False)
    parser_upload.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_upload.add_argument("--amend", help="amend the last upload", action="store_true")
    # optional list of files. if not present, use current manifest
    # when upload, mark the manifest version as uploaded.
    _add_data_dir_args(parser_upload, IS_SITE)
    _add_data_dir_args(parser_upload, IS_CENTRAL)
    _add_cloud_credential_args(parser_upload, IS_SITE)
    _add_cloud_credential_args(parser_upload, IS_CENTRAL)
    parser_upload.set_defaults(func = _upload_files)
    
    parser_verify = subparsers.add_parser("verify", help = "verify manifest with file system")
    # _add_manifest_args(parser_verify)
    parser_verify.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to most recent uploaded files.", required=False)
    parser_verify.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    _add_data_dir_args(parser_verify, IS_CENTRAL)
    _add_cloud_credential_args(parser_verify, IS_CENTRAL)
    parser_verify.set_defaults(func = _verify_files)
        
    parser_history = subparsers.add_parser("history", help = "show command history")
    # _add_manifest_args(parser_history)
    parser_history.set_defaults(func = _show_history)
        
    args = parser.parse_args()
    
    manifest_fn = args.manifest if "manifest" in vars(args) else os.path.join(".", "journal.db")
    save_command_history(*(_recreate_params(_strip_account_info(args))), manifest_fn)
    
    args.func(args)
    
    