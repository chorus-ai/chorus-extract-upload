import os
import shutil
import time
import argparse

from chorus_upload_journal.upload_tools.local_ops import update_manifest, list_files
# from chorus_upload_journal.upload_tools.generate_manifest import restore_manifest, list_uploads, list_manifests
# from chorus_upload_journal.upload_tools.upload_ops_builtin import upload_files, verify_files, list_files
from chorus_upload_journal.upload_tools import history_ops 
from chorus_upload_journal.upload_tools.storage_helper import FileSystemHelper, _make_client
from pathlib import Path
from chorus_upload_journal.upload_tools import config_helper
import json
from chorus_upload_journal.upload_tools.defaults import DEFAULT_MODALITIES
from chorus_upload_journal.upload_tools import upload_ops


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
# TODO: DONE pull and push journal files from central.
# TODO: DONE measure time
# TODO: DONE parallelize update
# TODO: TESTED test large data and fix timeout if any.
# TODO: use azcli or azcopy

# create command processor that support subcommands
# https://docs.python.org/3/library/argparse.html#sub-commands

IS_SITE = False
IS_CENTRAL = True


# def _getattr(args, argname):
#     out = getattr(args, argname)
#     return out.strip("'").strip("\"") if out else None

def _show_history(args, config, manifest_fn):    
    print("Command History: please note account information were not saved.")
    history_ops.show_command_history(manifest_fn)

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
    # print("working with manifests:")
    # print("  list:      python generate_manifest list-versions")
    # print("  revert:    python generate_manifest revert-version --version 20210901120000")
    
        
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
# def _list_manifests(args, config, manifest_fn):
#     print("Uploads known in current manifest: ")
#     list_uploads(manifest_fn)
#     print("Backed up Manifests: ")
#     list_manifests(manifest_fn)
        
# helper to revert to a previous manifest
# def _revert_manifest(args, config, manifest_fn):
#     revert_time = args.version
#     print("Revert to: ", revert_time)
#     restore_manifest(manifest_fn, revert_time)
    
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
    first = True  # first upload is based on user specification whether its amend or not.
    # subsequent modalities in this call always amend.
    for mod, mod_config in mod_configs.items():
        sitefs = FileSystemHelper(mod_config["path"], client = _make_client(mod_config))
        upload_ops.upload_files(sitefs, centralfs, manifest_fn, modalities = [mod], amend = (args.amend if first else True), verbose = args.verbose)
        first = False
    
# helper to report file verification
def _verify_files(args, config, manifest_fn):
    print("NOTE: This will download files to verify md5.  It will take a while and can take significant disk space.")
    
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        

    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    upload_ops.verify_files(centralfs, manifest_fn, version = args.version, modalities = mods, verbose = args.verbose)
    

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
    parser_auth.set_defaults(func = config_helper._print_config_usage)
    
    #------ create the parser for the "update" command
    parser_update = subparsers.add_parser("update", help = "create or update the current manifest")
    parser_update.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_update.set_defaults(func = _update_manifest)
    
    # create the parser for the "list" command
    # parser_list = subparsers.add_parser("list-versions", help = "list the versions in a manifest database")
    # parser_list.set_defaults(func = _list_manifests)
    
    # DANGEROUS
    # parser_revert = subparsers.add_parser("revert-version", help = "revert to a previous manifest version.")
    # parser_revert.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)
    # parser_revert.set_defaults(func = _revert_manifest)

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
    
    parser_verify = subparsers.add_parser("verify", help = "verify manifest with file system.  WARNING files will be downloaded for md5 computation")
    parser_verify.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to most recent uploaded files.", required=False)
    parser_verify.add_argument("--modalities", help="list of modalities to include in the manifest update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_verify.set_defaults(func = _verify_files)
        
    parser_unlock = subparsers.add_parser("unlock", help = "unlock a cloud manifest file")
    parser_unlock.set_defaults(func = upload_ops.unlock_journal)
        
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
    upload_method = config_helper.get_upload_methods(config)
    
    if (args.command not in ["config_help", "usage", "unlock"]):
        manifest_path, locked_path, local_path, manifest_md5 = upload_ops.checkout_journal(config)
        
        #     # push up a lock file.
        manifest_fn = str(local_path.root)
        # === save command history
        args_dict = history_ops._strip_account_info(args)
        if ("config" not in args_dict.keys()) or (args_dict["config"] is None):
            args_dict["config"] = config_fn
        command_id = history_ops.save_command_history(*(history_ops._recreate_params(args_dict)), *(history_ops._get_paths_for_history(args, config)), manifest_fn)

        # call the subcommand function.
        start = time.time()
        args.func(args, config, manifest_fn)
        end = time.time()
        elapsed = end - start
        print(f"Command Completed in {elapsed:.2f} seconds.")
        history_ops.update_command_completion(command_id, elapsed, manifest_fn)
        
        # push journal up.
        upload_ops.checkin_journal(manifest_path, locked_path, local_path, manifest_md5)
        
    else:
        # if just printing usage or help, don't need to checkout the journal.
        # if unlock, do it in the cloud only.
        manifest_fn = config_helper.get_journal_config(config)["path"]
    
        # call the subcommand function.
        args.func(args, config, manifest_fn)
    
