import os
import shutil
import time
import argparse

from generate_manifest import gen_manifest, update_manifest, select_files_to_upload, restore_manifest, list_uploads, list_manifests, upload_files, verify_files
from storage_pathlib import FileSystemHelper

# TODO: DONE need to support folder for different file types being located at different places.
#       each location can be checked for specific set of file types.
# TODO: DONE group authentication parameters


# create command processor that support subcommands
# https://docs.python.org/3/library/argparse.html#sub-commands

IS_SITE = False
IS_CENTRAL = True


def __make_aws_client(aws_profile:str, aws_access_key_id:str, aws_secret_access_key:str, aws_default_region:str):
    from cloudpathlib import S3Client
    if aws_profile:
        # profile specified, then use it
        return S3Client(profile_name = aws_profile)
    elif aws_access_key_id and aws_secret_access_key and aws_default_region:
        # access key and secret key specified, then use it
        return S3Client(access_key_id = aws_access_key_id, 
                        secret_access_key = aws_secret_access_key, 
                        default_region = aws_default_region)
    elif os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY") and os.environ.get("AWS_DEFAULT_REGION"):
        # access key and secret key specified in environment variables, then use it
        return S3Client(access_key_id = os.environ.get("AWS_ACCESS_KEY_ID"), 
                        secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY"), 
                        default_region = os.environ.get("AWS_DEFAULT_REGION"))
    else:
        # default profile
        return S3Client()
    
def __make_az_client(azure_account_name:str, azure_account_key:str, azure_storage_connection_string:str):
    from cloudpathlib import AzureBlobClient
    if azure_storage_connection_string:
        # connection string specified, then use it
        return AzureBlobClient(connection_string = azure_storage_connection_string)
    elif azure_account_name and azure_account_key:
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={azure_account_name};AccountKey={azure_account_key};EndpointSuffix=core.windows.net"

        # account name and key specified, then use it
        return AzureBlobClient(connection_string = connection_string)
    elif os.environ.get("AZURE_STORAGE_CONNECTION_STRING"):
        # connection string specified in environment variables, then use it
        return AzureBlobClient(connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))
    elif os.environ.get("AZURE_ACCOUNT_NAME") and os.environ.get("AZURE_ACCOUNT_KEY"):
        azure_account_name = os.environ.get("AZURE_ACCOUNT_NAME")
        azure_account_key = os.environ.get("AZURE_ACCOUNT_KEY")
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={azure_account_name};AccountKey={azure_account_key};EndpointSuffix=core.windows.net"
        # account name and key specified in environment variables, then use it
        return AzureBlobClient(connection_string=connection_string)
    else:
        raise ValueError("No viable Azure account name and key or connection string to open connection")
    
def __make_gs_client(google_application_credentials:str):
    from cloudpathlib import GoogleCloudClient
    if google_application_credentials:
        # application credentials specified, then use it
        return GoogleCloudClient(application_credentials = google_application_credentials)
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        # application credentials specified in environment variables, then use it
        return GoogleCloudClient(application_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    else:
        raise ValueError("No viable Google application credentials to open connection")
    
def _make_client(args, for_central: bool):
    if for_central:
        path_str = args.central_path
        if (path_str.startswith("s3://")):
            return __make_aws_client(args.central_aws_profile, args.central_aws_access_key_id, args.central_aws_secret_access_key, args.central_aws_default_region)
        elif (path_str.startswith("az://")):
            return __make_az_client(args.central_azure_account_name, args.central_azure_account_key, args.central_azure_storage_connection_string)
        elif (path_str.startswith("gs://")):
            return __make_gs_client(args.central_google_application_credentials)
        elif ("://" in path_str):
            raise ValueError("Unknown cloud storage provider.  Supprots s3, az, and gs")
        else:
            return None
    else:
        path_str = args.site_path
        if (path_str.startswith("s3://")):
            return __make_aws_client(args.site_aws_profile, args.site_aws_access_key_id, args.site_aws_secret_access_key, args.site_aws_default_region)
        elif (path_str.startswith("az://")):
            return __make_az_client(args.site_azure_account_name, args.site_azure_account_key, args.site_azure_storage_connection_string)
        elif (path_str.startswith("gs://")):
            return __make_gs_client(args.site_google_application_credentials)
        elif ("://" in path_str):
            raise ValueError("Unknown cloud storage provider.  Supprots s3, az, and gs")
        else:
            return None
        

def _update_manifest(args):
    if (args.site_path is None):
        raise ValueError("site path is required")
    
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")

    datafs = FileSystemHelper(args.site_path, client = _make_client(args, IS_SITE))

    if os.path.exists(manifest_fn): 
        print("Update Manifest: ", manifest_fn)    
        update_manifest(datafs, databasename = manifest_fn, subdirs = ['Waveforms', 'Images', 'OMOP'])
    else:
        print("Create Manifest: ", manifest_fn)    
        gen_manifest(datafs, databasename = manifest_fn, subdirs = ['Waveforms', 'Images', 'OMOP'])
    

def _list_manifests(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    print("Backed up Manifests: ")
    list_manifests(manifest_fn)
    print("Uploads known in current manifest: ")
    list_uploads(manifest_fn)
    
def _revert_manifest(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    revert_time = args.time
    print("Revert to: ", revert_time)
    restore_manifest(manifest_fn, revert_time)
    
def _select_files(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    select_files_to_upload(manifest_fn, args.list_file, verbose=True)
    

def _print_auth_usage(args):
    print("The following commands require authentication WHEN using cloud storage:")
    print("  update: requires site directory (onprem or cloud) and site manifest file (onprem or cloud)")
    print("  upload: requires site directory/manifest (onprem or cloud) and central directory (onprem or cloud)")
    print("  upload: requires updated site manifest file (onprem or cloud) and central directory (onprem or cloud)")
    print("Local file system is assumed to not require authentication.")
    print("Cloud storage requires authentication FOR EACH DIRECTORY/URL and parameters depends on cloud service provider")
    print("  AWS:   requires --access-key-id, --access-secret-key, --default-region")
    print("         these may be specified on the commandline, as environment variables (see subcommand help)")
    print("         these may also be stored in aws/credentials file, in which case specify the profile name with --aws-profile")
    print("         order of parsing is commandline parameters, environment variable, default profile")
    print("         if no authentication parameters are specified, default profile is used.")
    print("  Azure: requires the pair (--azure-account-name, --azure-account-key), or --azure-storage_connection_string")
    print("         connection_string may be specified on the commandline, or as environment variables (see subcommand help)")
    print("         connection_string has preference over azure-account-name and azure-account-key, then environment variables")
    print("  Google: (UNTESTED) requires --google-application-credentials")
    print("         application credentials may be specified on the commandline, or as an environment variable (see subcommand help)")
    

def _upload_files(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")

    if (args.site_path is None):
        raise ValueError("site path is required")
    sitefs = FileSystemHelper(args.site_path, client = _make_client(args, IS_SITE))
    
    if (args.central_path is None):
        raise ValueError("central path is required")
    centralfs = FileSystemHelper(args.central_path, client = _make_client(args, IS_CENTRAL))

    upload_files(sitefs, centralfs, manifest_fn)
    
def _verify_files(args):
    manifest_fn = args.manifest if args.manifest else os.path.join(".", "journal.db")
    central_fs = FileSystemHelper(args.central_path, client = _make_client(args, IS_CENTRAL))
    upload_datetime = args.upload_datetime
    verify_files(central_fs, manifest_fn, upload_datetime)
    ...

def _add_cloud_credential_args(parser, for_central: bool):
    prefix = "central" if for_central else "site"
    
    aws_group = parser.add_argument_group(title = f"{prefix} aws authentication parameters")    
    aws_group.add_argument(f"--{prefix}-aws-access-key-id", help="AWS access key. Can also specify as environment variable AWS_ACCESS_KEY_ID", required=False)
    aws_group.add_argument(f"--{prefix}-aws-secret-access-key", help="AWS access secret key. Can also specify as environment variable AWS_SECRET_ACCESS_KEY", required=False)
    aws_group.add_argument(f"--{prefix}-aws-default-region", help="AWS access secret key. Can also specify as environment variable AWS_DEFAULT_REGION", required=False)
    aws_group.add_argument(f"--{prefix}-aws-profile", help="AWS access secret key.  if no authentication parameters are specified, default profile is used.", required=False)
    
    azure_group = parser.add_argument_group(title = f"{prefix} azure authentication parameters")
    azure_group.add_argument(f"--{prefix}-azure-account-name", help="Azure account name. can also specify as environment variable AZURE_ACCOUNT_NAME", required=False)
    azure_group.add_argument(f"--{prefix}-azure-account-key", help="Azure account key. can also specify as environment variable AZURE_ACCOUNT_KEY", required=False)
    azure_group.add_argument(f"--{prefix}-azure-storage_connection_string", help="Azure account connection string. . can also specify as environment variable AZURE_STORAGE_CONNECTION_STRING", required=False)
    
    google_group = parser.add_argument_group(title = f"{prefix} google authentication parameters")
    google_group.add_argument(f"--{prefix}-google-application-credentials", help="Google application credentials. can also specify as environment variable GOOGLE_APPLICATION_CREDENTIALS", required=False)

def _add_data_dir_args(parser, for_central: bool):
    prefix = "central" if for_central else "site"
    parser.add_argument(f"--{prefix}-path", help=f"onprem/cloud {prefix} data folder. Examples: az://container/, aws://container/, or /mnt/x/project/chorus/data", required=True)
    
def _add_manifest_args(parser):
    parser.add_argument("-m", "--manifest", help="onprem site manifest file (defaults to ./journal.db). this should be a local file, and will be uploaded during 'upload'", required=True)


if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Generate manifest for a site folder")
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    subparsers.required = True
    
    #------ authentication help
    parser_auth = subparsers.add_parser("auth_help", help = "show help information about authentication methods")
    parser_auth.set_defaults(func = _print_auth_usage)
    
    #------ create the parser for the "update" command
    parser_update = subparsers.add_parser("update", help = "create or update the current manifest")
    _add_manifest_args(parser_update)
    _add_data_dir_args(parser_update, IS_SITE)
    _add_cloud_credential_args(parser_update, IS_SITE)
    parser_update.add_argument("-s", "--subdirs", help="list of subdirectories to include in the manifest. defaults to Waveforms,Images,OMOP.  case sensitive.", nargs='+', required=False)
    parser_update.set_defaults(func = _update_manifest)
    
    # create the parser for the "list" command
    parser_list = subparsers.add_parser("list", help = "list the versions in a manifest database")
    _add_manifest_args(parser_list)
    parser_list.set_defaults(func = _list_manifests)
    
    # DANGEROUS
    parser_revert = subparsers.add_parser("revert", help = "revert to a previous manifest version.")
    _add_manifest_args(parser_revert)
    parser_revert.add_argument("--time", help="datetime of an upload (use list to get date times).", required=False)
    parser_revert.set_defaults(func = _revert_manifest)

    # # DANGEROUS
    # parser_delete = subparsers.add_parser("delete", help = "delete version in a manifest database")
    # _add_manifest_args(parser_delete)
    # parser_delete.add_argument("--time", help="datetime of an upload (use list to get date times).", required=False)

    # create the parser for the "select" command
    parser_select = subparsers.add_parser("select", help = "create list of files to upload")
    _add_manifest_args(parser_select)
    parser_select.add_argument("-f", "--list-file", help="list of files to upload", required=False)
    parser_select.set_defaults(func = _select_files)
    
    parser_upload = subparsers.add_parser("upload", help = "upload files to server")
    _add_manifest_args(parser_upload)
    # optional list of files. if not present, use current manifest
    # when upload, mark the manifest version as uploaded.
    _add_data_dir_args(parser_upload, IS_SITE)
    _add_data_dir_args(parser_upload, IS_CENTRAL)
    _add_cloud_credential_args(parser_upload, IS_SITE)
    _add_cloud_credential_args(parser_upload, IS_CENTRAL)
    parser_upload.set_defaults(func = _upload_files)
    
    parser_verify = subparsers.add_parser("verify", help = "verify manifest with file system")
    _add_manifest_args(parser_verify)
    parser_verify.add_argument("--time", help="datetime of an upload (use list to get date times).", required=False)
    _add_data_dir_args(parser_verify, IS_CENTRAL)
    _add_cloud_credential_args(parser_verify, IS_CENTRAL)
    parser_verify.set_defaults(func = _verify_files)
    
    
    # minimally, need the top level directory
    # storage_url = os.environ["AZURE_STORAGE_BLOB_URL"]
    
    args = parser.parse_args()
    # print(args)

    args.func(args)
    # folder = args.data_dir
    # if args.manifest:
    #     manifest_fn = args.manifest
    # else:
    #     manifest_fn = os.path.join(folder, "journal.db")
    
    # if os.path.exists(manifest_fn):
    #     update_manifest(folder, databasename = manifest_fn)
    # else:
    #     gen_manifest(folder, databasename = manifest_fn)
    
    ## if (os.path.exists("journal.db")):
    ##     os.remove("journal.db")
    ## gen_manifest("TestData/SiteFolder_FirstSnapshot")
    ## time.sleep(2)
    ## update_manifest("TestData/SiteFolder_SecondSnapshot")
    
    