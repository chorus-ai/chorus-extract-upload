import os
import shutil
import time
import argparse

from chorus_upload_journal.upload_tools.local_ops import update_journal, list_files
# from chorus_upload_journal.upload_tools.generate_journal import restore_journal, list_uploads, list_journals
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
# TODO: DONE journal track source path and dest container
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

def _show_history(args, config, journal_fn):    
    print("Command History: please note account information were not saved.")
    history_ops.show_command_history(journal_fn)

def _print_usage(args, config, journal_fn):
    # TODO: DONE update this
    print("The following are example usage for each of the commands:")
    print("  Please use 'auth_help' to see help information about authentication methods")
    print("  Please use each subcommand's help to get a detailed list of parameters")
    print("For each command, the following parameters are common and can be specified before the command name:")
    print("  -v or --verbose:  verbose output")
    print("  -c or --config:   config file (defaults to config.toml in the script directory) with storage path locations")
    print("generating journal")
    print("  update-journal:    python chorus_upload_journal/upload_tools --config ./config.toml update")
    print("             python chorus_upload_journal/upload_tools update --modalities Waveforms,Images,OMOP")
    print("generating upload file list")
    print("  list-files:    python chorus_upload_journal/upload_tools select")
    print("             python chorus_upload_journal/upload_tools select --version 20210901120000 -f filelist.txt")
    print("uploading to and verifying with central")
    print("  upload-files:    python chorus_upload_journal/upload_tools upload ")
    print("             python chorus_upload_journal/upload_tools upload --modalities Images")
    print("  verify-files:    python chorus_upload_journal/upload_tools verify --modalities Images --version 20210901120000")
    # print("working with journals:")
    # print("  list:      python chorus_upload_journal/upload_tools list-versions")
    # print("  revert:    python chorus_upload_journal/upload_tools revert-version --version 20210901120000")
    
        
# helper to call update journal
def _update_journal(args, config, journal_fn):
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        

    # for each modality, create the file system help and process
    for mod, mod_config in mod_configs.items():        
        # create the file system helper    
        datafs = FileSystemHelper(mod_config["path"], client = _make_client(mod_config))

        print("Update journal ", journal_fn, " for ", mod)
        update_journal(datafs, modalities = [mod], databasename = journal_fn, verbose = args.verbose)
    
# helper to list known update journals
# def _list_journals(args, config, journal_fn):
#     print("Uploads known in current journal: ")
#     list_uploads(journal_fn)
#     print("Backed up journals: ")
#     list_journals(journal_fn)
        
# helper to revert to a previous journal
# def _revert_journal(args, config, journal_fn):
#     revert_time = args.version
#     print("Revert to: ", revert_time)
#     restore_journal(journal_fn, revert_time)
    
# only works with azure dest for now.
def _write_files(file_list: list, dt: str, filename : str, **kwargs):
    out_type = kwargs.get('out_type', '').lower()
    dest_config = kwargs.get('dest_config', {})
    account_name = dest_config.get('azure_account_name', '')
    sas_token = dest_config.get('azure_sas_token', '')
    container = dest_config.get('azure_container', '')
    config_fn = kwargs.get('config_fn', '')
        
    if account_name is None or account_name == "":
        print("ERROR: destination is not an azure path")
        raise ValueError("Destination is not an azure path")
        
    p = Path(filename)
    fn = "_".join([p.stem, dt]) + p.suffix
    print("list written to ", fn)
    with open(fn, 'w') as f:
        # if windows, use double backslash
        if (out_type == "azcli"):
            if dt != "NEW":
                print("ERROR: cannot generate azcli script for existing file upload version", dt)
                return
            
            if (os.name == 'nt'):
                # BATCH FILE FORMAT
                f.write("@echo off\r\n")
                f.write("setlocal\r\n")
                f.write("set dt=%date:~10,4%%date:~4,2%%date:~7,2%%time:~0,2%%time:~3,2%%time:~6,2%\r\n")
                f.write("\r\n")
                f.write("REM set environment variables\r\n")
                f.write("set AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1\r\n")
                f.write("\r\n")
                f.write("REM login to azure\r\n")
                f.write(f"set account={account_name}\r\n")
                f.write(f"set sas_token=\"{sas_token}\"\r\n")
                f.write(f"set container={container}\r\n")
                f.write("\r\n")
                f.write("echo PLEASE add your virtual environment activation string if any.\r\n")
                f.write("\r\n")
                f.write("REM upload files\r\n")
                f.write("if exist azcli.log del azcli.log\r\n")
                f.write("type nul > azcli.log\r\n")
                
                for root, fn in file_list:
                    local_file = "\\".join([root, fn])
                    local_file = local_file.replace("/", "\\")
                    f.write(f"az storage blob upload --account-name %account% --sas-token %sas_token% --container-name %container% --name %dt%/" + fn + " --file " + local_file + "\r\n")
                    f.write("echo " + fn + " >> azcli.log\r\n")
                                           
                f.write("python chorus_upload_journal/upload_tools -c " + config_fn + " mark-as-uploaded --file-list azcli.log --version ${dt}\r\n")            

            else:
                f.write("#!/bin/bash\n")
                f.write("# script to upload files to azure for upload date " + dt + "\n")
                f.write("dt=`date '+%Y%m%d%H%M%S'`\n")
                f.write("\n")
                f.write("# set environment variables\n")
                f.write("export AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1\n")
                f.write("\n")
                
                f.write("# login to azure\n")
                f.write(f"account=\"{account_name}\"\n")
                f.write(f"sas_token=\"{sas_token}\"\n")
                f.write(f"container=\"{container}\"\n")
                f.write("\n")
                
                f.write("echo PLEASE add your virtual environment activation string if any.\n")
                f.write("\n")
                f.write("# upload files\n")
                f.write("if [ -e 'azcli.log' ]; then\n")
                f.write("    rm azcli.log\n")
                f.write("fi\n")
                f.write("touch azcli.log\n")
            
                for root, fn in file_list:
                    local_file = "/".join([root, fn])
                    f.write("az storage blob upload --account-name ${account} --sas-token \"${sas_token}\" --container-name ${container} --name ${dt}/" + fn + " --file " + local_file + "\n")     
                    f.write("echo " + fn + " >> azcli.log\n")
                                           
                f.write("python chorus_upload_journal/upload_tools -c " + config_fn + " mark-as-uploaded --file-list azcli.log --version ${dt}\n")
                
        elif (out_type == "azcopy"):
            print("ERROR: azcopy not implemented yet")
        else:
            filenames = ["/".join([root, fn]) for (root, fn) in file_list]
            if (os.name == 'nt'):
                f.write("\n\r".join([fn.replace("/", "\\") for fn in filenames]))
            else:
                f.write("\n".join(filenames))
                
            
# helper to display/save files to upload
def _select_files(args, config, journal_fn):
    
    # get the local path and central path and credentials
    
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    # do one modality at a time
    dt = args.version if args.version else "NEW"

    if (args.output_file is None) or (args.output_file == ""):
        for mod in mods:
            mod_files = list_files(journal_fn, version = args.version, modalities = [mod], verbose=args.verbose)
            mod_path = config_helper.get_site_config(config, mod)['path']

            print("files for version: ", dt, " root at ", mod_path )
            print("\n".join(mod_files))    
    else:
        central = config_helper.get_central_config(config)
        file_list = []
        for mod in mods:
            mod_files = list_files(journal_fn, version = args.version, modalities = [mod], verbose=args.verbose)
            mod_path = config_helper.get_site_config(config, mod)['path']

            file_list += [(mod_path, f) for f in mod_files]
    
        _write_files(file_list, dt, filename = args.output_file, out_type = args.output_type, dest_config = central, config_fn = args.config)
    
    
def _mark_as_uploaded(args, config, journal_fn):
    version = args.version
    file = args.file
    filelist = args.file_list
    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))    
    
    if (filelist is not None):
        with open(filelist, 'r') as f:
            files = [fn.strip() for fn in f.readlines()]
            upload_ops.mark_as_uploaded(centralfs, journal_fn, version, files, verbose = args.verbose)
    else:
        upload_ops.mark_as_uploaded(centralfs, journal_fn, version, [file], verbose = args.verbose)

# helper to upload files
def _upload_files(args, config, journal_fn):

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
        upload_ops.upload_files(sitefs, centralfs, journal_fn, modalities = [mod], amend = (args.amend if first else True), verbose = args.verbose)
        first = False
    
# helper to report file verification
def _verify_files(args, config, journal_fn):
    print("NOTE: This will download files to verify md5.  It will take a while and can take significant disk space.")
    
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else DEFAULT_MODALITIES
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        

    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    upload_ops.verify_files(centralfs, journal_fn, version = args.version, modalities = mods, verbose = args.verbose)
    

if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Generate journal for a site folder")
    parser.add_argument("-v", "--verbose", help="verbose output", action="store_true")
    parser.add_argument("-c", "--config", help="config file (defaults to config.toml) with storage path locations", required=False)
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    subparsers.required = True
    
    parser_help = subparsers.add_parser("usage", help = "show help information")
    parser_help.set_defaults(func = _print_usage)
    
    #------ authentication help
    parser_auth = subparsers.add_parser("config-help", help = "show help information about the configuration file")
    parser_auth.set_defaults(func = config_helper._print_config_usage)
    
    #------ create the parser for the "update" command
    parser_update = subparsers.add_parser("update-journal", help = "create or update the current journal")
    parser_update.add_argument("--modalities", help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_update.set_defaults(func = _update_journal)
    
    # create the parser for the "list" command
    # parser_list = subparsers.add_parser("list-versions", help = "list the versions in a journal database")
    # parser_list.set_defaults(func = _list_journals)
    
    # DANGEROUS
    # parser_revert = subparsers.add_parser("revert-version", help = "revert to a previous journal version.")
    # parser_revert.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)
    # parser_revert.set_defaults(func = _revert_journal)

    # # DANGEROUS
    # parser_delete = subparsers.add_parser("delete", help = "delete version in a journal database")
    # parser_delete.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)

    # create the parser for the "select" command
    parser_select = subparsers.add_parser("list-files", help = "list files to upload")
    parser_select.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to all un-uploaded files.", required=False)
    parser_select.add_argument("--modalities", help="list of modalities to include in the journal update. defaults to all.  case sensitive.", required=False)
    parser_select.add_argument("--output-file", help="output file", required=False)
    parser_select.add_argument("--output-type", help="the output file type: [list | azcli | azcopy].  azcli and azcopy are executable scripts.", required=False)
    parser_select.set_defaults(func = _select_files)
    
    parser_upload = subparsers.add_parser("upload-files", help = "upload files to server")
    parser_upload.add_argument("--version", help="datetime of an upload (use list to get date times). defaults to all un-uploaded files", required=False)
    parser_upload.add_argument("--modalities", help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_upload.add_argument("--amend", help="amend the last upload", action="store_true")
    # optional list of files. if not present, use current journal
    # when upload, mark the journal version as uploaded.
    parser_upload.set_defaults(func = _upload_files)

    parser_mark = subparsers.add_parser("mark-as-uploaded", help = "verify a file and mark as uploaded with time stamp.  WARNING files will be downloaded for md5 computation")
    parser_mark.add_argument("--version", help="datetime of an upload (use list to get date times).", required=True)
    parser_mark.add_argument("--file", help="file name of the file to check.  version/file is the path in the central azure environment", required=False)
    parser_mark.add_argument("--file-list", help="file list to check.  version/file is the path in the central azure environment", required=False)
    parser_mark.set_defaults(func = _mark_as_uploaded)
    
    parser_verify = subparsers.add_parser("verify-files", help = "verify journal with file system.  WARNING files will be downloaded for md5 computation")
    parser_verify.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to most recent uploaded files.", required=False)
    parser_verify.add_argument("--modalities", help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", required=False)
    parser_verify.set_defaults(func = _verify_files)
        
    parser_unlock = subparsers.add_parser("unlock-journal", help = "unlock a cloud journal file")
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
    
    if (args.command not in ["config-help", "usage", "unlock-journal"]):
        journal_path, locked_path, local_path, journal_md5 = upload_ops.checkout_journal(config)
        
        #     # push up a lock file.
        journal_fn = str(local_path.root)
        # === save command history
        args_dict = history_ops._strip_account_info(args)
        if ("config" not in args_dict.keys()) or (args_dict["config"] is None):
            args_dict["config"] = config_fn
        command_id = history_ops.save_command_history(*(history_ops._recreate_params(args_dict)), *(history_ops._get_paths_for_history(args, config)), journal_fn)

        # call the subcommand function.
        start = time.time()
        args.func(args, config, journal_fn)
        end = time.time()
        elapsed = end - start
        print(f"Command Completed in {elapsed:.2f} seconds.")
        history_ops.update_command_completion(command_id, elapsed, journal_fn)
        
        # push journal up.
        upload_ops.checkin_journal(journal_path, locked_path, local_path, journal_md5)
        
    else:
        # if just printing usage or help, don't need to checkout the journal.
        # if unlock, do it in the cloud only.
        journal_fn = config_helper.get_journal_config(config)["path"]
    
        # call the subcommand function.
        args.func(args, config, journal_fn)
    
