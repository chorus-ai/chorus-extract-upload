import os
import shutil
import time
import argparse

from generate_manifest import gen_manifest, update_manifest, select_files_to_upload, list_manifests


# create command processor that support subcommands
# https://docs.python.org/3/library/argparse.html#sub-commands

def _update_manifest(args):
    if (args.local_dir is None) or (os.path.exists(args.local_dir) == False):
        print("Local directory ", args.local_dir, " does not exist.  no action")
        return

    if args.manifest:
        manifest_fn = args.manifest
    else:
        manifest_fn = os.path.join(args.local_dir, "journal.db")


    if os.path.exists(manifest_fn): 
        print("Update Manifest: ", manifest_fn)    
        update_manifest(args.local_dir, databasename = manifest_fn)
    else:
        print("Create Manifest: ", manifest_fn)    
        gen_manifest(args.local_dir, databasename = manifest_fn)

def _list_manifests(args):
    list_manifests(args.manifest)

def _select_files(args):
    select_files_to_upload(args.manifest, args.file_list)

def _upload_files(args):
    ...
    
def _verify_files(args):
    ...

if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Generate manifest for a site folder")
    parser.add_argument("-m", "--manifest", help="manifest file (defaults to site_data_fold/journal.db)", required=False)
    
    # create subparsers
    subparsers = parser.add_subparsers(help="sub-command help")
    
    # create the parser for the "generate" command
    parser_update = subparsers.add_parser("update", help = "create or update the current manifest")
    # TODO: need to support folder for different file types being located at different places.
    parser_update.add_argument("-l", "--local-dir", help="Verify with local/cloud data folder")
    parser_update.set_defaults(func = _update_manifest)
    
    parser_list = subparsers.add_parser("list", help = "list the versions in a manifest database")
    parser_list.set_defaults(func = _list_manifests)
    # DANGEROUS
    # parser_revert = subparsers.add_parser("revert", help = "revert to a previous manifest version.")
    # parser_revert.add_argument("-t", "--time", help="timed manifest backup to revert to")
    
    # DANGEROUS
    # parser_list = subparsers.add_parser("delete", help = "delete version in a manifest database")
    # parser_revert.add_argument("-t", "--time", help="timed manifest backup to revert to")

    parser_select = subparsers.add_parser("select", help = "create list of files to upload")
    parser_select.add_argument("-f", "--file-list", help="list of files to upload")
    parser_select.set_defaults(func = _select_files)
    
    parser_upload = subparsers.add_parser("upload", help = "upload files to server")
    # optional list of files. if not present, use current manifest
    # when upload, mark the manifest version as uploaded.
    parser_upload.add_argument("-f", "--file-list", help="list of files to upload")
    parser_upload.set_defaults(func = _upload_files)
    
    parser_verify = subparsers.add_parser("verify", help = "verify manifest with file system")
    parser_verify.add_argument("-l", "--local-dir", help="Verify with local/cloud data folder")
    parser_verify.add_argument("-c", "--central-dir", help="Verify with central folder")
    parser_verify.set_defaults(func = _verify_files)
    
    # minimally, need the top level directory
    
    args = parser.parse_args()
    print(args)
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
    
    