import os
import shutil
import time
import argparse

from chorus_upload.local_ops import update_journal, list_files
# from chorus_upload.generate_journal import restore_journal, list_uploads, list_journals
# from chorus_upload.upload_ops_builtin import upload_files, verify_files, list_files
from chorus_upload import history_ops 
from chorus_upload.storage_helper import FileSystemHelper, _make_client
from pathlib import Path
from cloudpathlib import AnyPath
from chorus_upload import config_helper
import json
from chorus_upload import upload_ops
from chorus_upload import local_ops
import chorus_upload.storage_helper as storage_helper
import re



# TODO: DONE need to support folder for different file types being located at different places.
#       each location can be checked for specific set of file types.
# TODO: DONE group authentication parameters

# TODO: DONE support Azure SAS token (via account url) and AWS session token
# TODO: DONE capture command history.
# TODO: DONE: function to generate bash scripts (azcopy and azcli) for upload instead of using built in.
#       This would require a verfication and mark_uploaded function.
# TODO: DONE check the sas token to account url conversion.
# TODO: DONE update and upload one or more modality only

# TODO: DONE config file to reduce command line parameters
# TODO: DONE journal track source path and dest container
# TODO: DONE pull and push journal files from central.
# TODO: DONE measure time
# TODO: DONE parallelize update
# TODO: TESTED test large data and fix timeout if any.
# TODO: TABLED.  use azcli or azcopy internally
# TODO: DONE rename subcommands.
# TODO: DONE add upload count limit
# TODO: DONE add resume flags for upload - avoids re-checkout journal.   Instead of this flag, use "journal checkin --local-journal" first, followed by command to resume.
#           This is to address a interrupted "file upload", where journal has been checked out but not checked in
#           the central site then has a lock file that is old, and local journal contains updates.
# TODO: DONE use local MD5 instead of computing MD5 by downloading.  with HTTPs, each rest call should be computing MD5 for the block on transit.
# TODO: add support for snapshot vs append modes for journal update
# TODO: add support for marking files as deleted.

# TODO: handle the case where the journal has to be uploaded by azcli because of security review.
#       journal update, list
#       file mark_as_deleted, upload, marked_as_uploaded_central, verify
#    workaround - if azcli is only transport, specify the journal file as local. last line in azcopy uploads journal directory.

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
    print("  journal update:    python chorus_upload --config ./config.toml journal update")
    print("             python chorus_upload journal update --modalities Waveforms,Images,OMOP")
    print("generating upload file list")
    print("  file list:    python chorus_upload file list")
    print("             python chorus_upload file list --version 20210901120000 -f filelist.txt")
    print("uploading to and verifying with central")
    print("  file upload:    python chorus_upload file upload ")
    print("             python chorus_upload file upload --modalities Images")
    print("  file verify:    python chorus_upload file verify --modalities Images --version 20210901120000")
    print("working with journals:")
    print("  list versions:      python chorus_upload journal list")
    print("  checkout:      python chorus_upload journal checkout --local-journal journal.db")
    print("  checkin:      python chorus_upload journal checkin --local-journal journal.db")
    # print("  revert:    python chorus_upload revert-version --version 20210901120000")
    
        
# helper to call update journal
def _update_journal(args, config, journal_fn):
    version = args.version
    amend = args.amend
    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }
    journaling_mode = config_helper.get_journaling_mode(config)

    # for each modality, create the file system help and process
    first = True
    for mod, mod_config in mod_configs.items():        
        # create the file system helper    
        datafs = FileSystemHelper(mod_config["path"], client = _make_client(mod_config))

        print("Update journal ", journal_fn, " for ", mod)
        update_journal(datafs, modalities = [mod], journaling_mode = journaling_mode,
                       databasename = journal_fn, 
                       version = version, amend = (args.amend if first else True), 
                       verbose = args.verbose)
        first = False
            
# helper to revert to a previous journal
# def _revert_journal(args, config, journal_fn):
#     revert_time = args.version
#     print("Revert to: ", revert_time)
#     restore_journal(journal_fn, revert_time)
    
WINDOWS_STRINGS = {
    "eol": "\n",  # supposed to use "\n" reguardless of os.
    "var_start": "%",
    "var_end": "%",
    "comment": "@REM",
    "set_var": "set ",
    "set_var_quoted": "set \"",
    "set_var_quoted_start": "",
    "set_var_quoted_end": "\"",
    "command_processor": "cmd /c ",
    "export_var": "set"
}
LINUX_STRINGS = {
    "eol": "\n",
    "var_start": "${",
    "var_end": "}",
    "comment": "#",
    "set_var": "",
    "set_var_quoted": "",
    "set_var_quoted_start": "\"",
    "set_var_quoted_end": "\"",
    "command_processor": "",
    "export_var": "export"
}
    
# only works with azure dest for now.
# file_list is a dictionary of structure {modality: (config, {version: [files]})}
def _write_files(file_list: dict, upload_datetime: str, filename : str, **kwargs):
    out_type = kwargs.get('out_type', '').lower()
    dest_config = kwargs.get('dest_config', {})
    journal_transport = kwargs.get('journal_transport', 'builtin')
    pattern = re.compile(r"az://[^/]+/(.+)")
    cloud_journal = kwargs.get('cloud_journal', "journal.db")
    matches = pattern.match(cloud_journal)
    if matches:
        cloud_journal_path = matches.groups()[0]
    else:
        # if not storing journal in azure, then don't use azcli for journal transport.
        journal_transport = "builtin"
        cloud_journal_path = "journal.db"
    
    local_journal = cloud_journal.split("/")[-1]
    
    max_upload_count = int(args.max_num_files) if ("max_num_files" in vars(args)) and (args.max_num_files is not None) else None 
    if (not dest_config['path'].startswith("az://")):
        print("WARNING: Destination is not an azure path.  cannot generate azcli script, but can genearete a list of source files")
        out_type = "list"
    
    account_name = dest_config.get('azure_account_name', '')
        
    if account_name is None or account_name == "":
        print("ERROR: destination is not an azure path")
        raise ValueError("Destination is not an azure path")
 
    sas_token = dest_config.get('azure_sas_token', '')
    container = dest_config.get('azure_container', '')
    
    # track configuration file
    config_fn = kwargs.get('config_fn', '')
    
    # check source paths.
    source_paths = set(config['path'] for _, (config, _) in file_list.items())
    source_is_cloud = any([(p.startswith("az://")) or (p.startswith("s3://")) or (p.startswith("gs://")) for p in source_paths])
    if (out_type == "azcli") and source_is_cloud:
        print("WARNING: source path is in the cloud, cannot generate azcli script.  using 'azcopy' instead")
        out_type = "azcopy"
        
    # get output file name.
    p = Path(filename)
    # fn = "_".join([p.stem, upload_datetime]) + p.suffix
    fn = filename
    print("list writing to ", fn)

    # is_windows = os.name == 'nt'
    is_windows = True
    
    eol = WINDOWS_STRINGS["eol"] if is_windows else LINUX_STRINGS["eol"]
    comment = WINDOWS_STRINGS["comment"] if is_windows else LINUX_STRINGS["comment"]
    var_start = WINDOWS_STRINGS["var_start"] if is_windows else LINUX_STRINGS["var_start"]
    var_end = WINDOWS_STRINGS["var_end"] if is_windows else LINUX_STRINGS["var_end"]
    set_var = WINDOWS_STRINGS["set_var"] if is_windows else LINUX_STRINGS["set_var"]
    set_var_quoted = WINDOWS_STRINGS["set_var_quoted"] if is_windows else LINUX_STRINGS["set_var_quoted"]
    set_var_quoted_start = WINDOWS_STRINGS["set_var_quoted_start"] if is_windows else LINUX_STRINGS["set_var_quoted_start"]
    set_var_quoted_end = WINDOWS_STRINGS["set_var_quoted_end"] if is_windows else LINUX_STRINGS["set_var_quoted_end"]
    export_var = WINDOWS_STRINGS["export_var"] if is_windows else LINUX_STRINGS["export_var"]
    cmd_proc = WINDOWS_STRINGS["command_processor"] if is_windows else LINUX_STRINGS["command_processor"]
    testme = ""
    testme_end = ""
    
    with open(fn, 'w') as f:
        # if windows, use double backslash
        
        if (out_type != "azcli") and (out_type != "azcopy"):
            for mod, (config, flists) in file_list.items():
                for version, flist in flists.items():
                    root = config["path"]            
                    filenames = ["/".join([root, fn]) for fn in flist]
                    if is_windows:
                        fns = [fn.replace("/", "\\") for fn in filenames]
                    else:
                        fns = filenames
                        
                    f.write(eol.join(fns))
                    f.write(eol)
        
        else:
            
            # this first part of the script is same for both azcli and azcopy.
            if is_windows:
                # BATCH FILE FORMAT
                f.write("@echo off" + eol)
                f.write("setlocal" + eol)
            else:
                f.write("#!/bin/bash" + eol)
                
            f.write(comment + " script to upload files to azure for upload date " + upload_datetime + eol)
            f.write(eol)

            f.write(comment + " set environment variables" + eol)
            f.write(set_var + "dt=" + upload_datetime + "" + eol)
            f.write(export_var + " AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1" + eol)
            f.write(eol)
            f.write(comment + " Destination azure credentials" + eol)
            f.write(set_var + "account=" + account_name + "" + eol)
            if is_windows:
                f.write(set_var_quoted + "sas_token=" + set_var_quoted_start + sas_token.replace("%", "%%") + set_var_quoted_end + eol)
            else:                
                f.write(set_var_quoted + "sas_token=" + set_var_quoted_start + sas_token + set_var_quoted_end + eol)
            f.write(set_var + "container=" + container + "" + eol)
            f.write(eol)
            
            f.write("echo PLEASE add your virtual environment activation string if any." + eol)
            f.write(eol)
                        
            # check to see if cloud_journal is a cloud path
            if not cloud_journal.startswith("az://"):
                local_journal = cloud_journal
                if is_windows:
                    f.write(set_var + "\"local_journal=" + local_journal + "\"" + eol)
                else:
                    f.write(set_var + "local_journal=\"" + local_journal + "\"" + eol)
            else:
                            
                f.write(comment + " Checkout the journal" + eol)
                if is_windows:
                    f.write(set_var + "\"local_journal=" + local_journal + "\"" + eol)
                    f.write(set_var + "\"cloud_journal=" + cloud_journal_path + "\"" + eol)
                else:
                    f.write(set_var + "local_journal=\"" + local_journal + "\"" + eol)
                    f.write(set_var + "cloud_journal=\"" + cloud_journal_path + "\"" + eol)
                    
                if (journal_transport == "builtin"):
                    f.write(testme + "python chorus_upload -c " + config_fn + 
                            " journal checkout --local-journal \"" + var_start+"local_journal"+var_end + "\"" + testme_end + eol)                
                else:
                    # use azcli to perform checkout.
                    # check if lock file exists.
                    # if yes, end.
                    # if no, check if journal exists, 
                    #    if yes, move it to locked, and copy it to local
                    #    if no, create a locked file.  use local journal.

                    if is_windows:
                        f.write("for /F \"tokens=*\" %%a in ('az storage blob exists --account-name " + var_start+"account"+var_end +
                                " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " +
                                var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" + " --output tsv') do " + 
                                set_var + "exists=%%a" + eol)
                        f.write("if \"%exists%\" == \"True\" (" + eol)
                    else:
                        f.write(set_var + "exists=`az storage blob exists --account-name " + var_start+"account"+var_end +
                                " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " +
                                var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" + " --output tsv`" + eol)
                        f.write("if [[ \"\$exists\" == \"True\" ]]; then" + eol)
                        
                    f.write("    echo Journal is locked.  Cannot continue." + eol)
                    if is_windows:
                        f.write("    exit /b 1" + eol)
                        f.write(")" + eol)
                    else:
                        f.write("    exit 1" + eol)
                        f.write("fi" + eol)
                    
                    if is_windows:
                        f.write("for /F \"tokens=*\" %%a in ('az storage blob exists --account-name " + var_start+"account"+var_end +
                            " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " +
                            var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + "\" --output tsv') do " + 
                            set_var + "exists=%%a" + eol)
                        f.write("if \"%exists%\" == \"False\" (" + eol)
                        f.write("    type nul > " + var_start+"local_journal"+var_end + eol)
                    else:
                        f.write(set_var + "exists=`az storage blob exists --account-name " + var_start+"account"+var_end +
                            " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " +
                            var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + "\" --output tsv`" + eol)
                        f.write("if [[ \"\$exists\" == \"False\" ]]; then" + eol)
                        # create a new lock file
                        f.write("    touch " + var_start+"local_journal"+var_end + eol)
                    f.write("    " + cmd_proc + "az storage blob upload --account-name " + var_start+"account"+var_end +
                            " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " +
                            var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" +
                            " --file \"" + var_start+"local_journal"+var_end + "\" --only-show-errors --output none" + eol)
                    if is_windows:
                        f.write(") else (" + eol)
                    else:
                        f.write("else" + eol)
                    # move the journal to locked
                    f.write(testme + "    " + cmd_proc + "az storage blob copy start" + 
                            " --source-blob \"" + var_start+"cloud_journal"+var_end + "\"" +
                            " --source-container " + var_start+"container"+var_end + 
                            " --source-account-name " + var_start+"account"+var_end +
                            " --source-sas \"" + var_start+"sas_token"+var_end + "\"" + 
                            " --destination-blob \"" + var_start+"cloud_journal"+var_end + ".locked\"" + 
                            " --destination-container " + var_start+"container"+var_end + 
                            " --account-name " + var_start+"account"+var_end +
                            " --sas-token \"" + var_start+"sas_token"+var_end + "\"" +  testme_end + eol)
                    f.write(testme + "    " + cmd_proc + "az storage blob download --account-name " + var_start+"account"+var_end +
                            " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " +
                            var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + "\"" +
                            " --file \"" + var_start+"local_journal"+var_end + testme_end + "\" --overwrite --only-show-errors --output none" + eol)
                    if is_windows:
                        f.write(")" + eol)
                    else:
                        f.write("fi" + eol)
                    
            f.write(eol)
            
            f.write(comment + " upload files" + eol)
            f.write(eol)
            
            # then we iterate over the files, and decide based on out_type.
            upload_count = 0
            for mod, (config, flists) in file_list.items():
                root = config['path']
                
                f.write(eol)
                f.write(comment + " Files upload for " + mod + " at root " + root + eol)
                
                if is_windows:
                    f.write("if exist files_" + var_start+"dt"+var_end + ".txt del files_" + var_start+"dt"+var_end + ".txt" + eol)
                    f.write("type nul > files_" + var_start+"dt"+var_end + ".txt" + eol)
                else:
                    f.write("if [ -e files_" + var_start+"dt"+var_end + ".txt ]; then" + eol)
                    f.write("    rm files_" + var_start+"dt"+var_end + ".txt" + eol)
                    f.write("fi" + eol)
                    f.write("touch files_" + var_start+"dt"+var_end + ".txt" + eol)
                
                if (root.startswith("az://")):
                    mod_local_account = config.get('azure_account_name', '')
                    mod_local_container = config.get('azure_container', '')
                    mod_local_sas_token = config.get('azure_sas_token', '')
                    
                    f.write(set_var + "mod_local_account=" + mod_local_account + "" + eol)
                    f.write(set_var_quoted + "mod_local_sas_token=" + set_var_quoted_start + mod_local_sas_token + set_var_quoted_end + eol)
                    f.write(set_var + "mod_local_container=" + mod_local_container + "" + eol)
                    f.write(eol)
                            
                elif (root.startswith("s3://")):
                    mod_local_container = config.get('s3_bucket', '')
                    mod_local_ACCESS_KEY = config.get('s3_access_key', '')
                    mod_local_SECRET_KEY = config.get('s3_secret_key', '')

                    f.write(export_var + " AWS_ACCESS_KEY_ID=\"" + mod_local_ACCESS_KEY + "\"" + eol)
                    f.write(export_var + " AWS_SECRET_ACCESS_KEY=\"" + mod_local_SECRET_KEY + "\"" + eol)
                    f.write(eol)

                for version, flist in flists.items():
                    f.write(eol)
                    f.write(set_var + "ver=" + version + "" + eol)                    
                    
                    count = 0
                    step = 100
                    for fn in flist:
                        if is_windows:
                            local_file = "\\".join([root, fn])
                            local_file = local_file.replace("/", "\\")
                        else:
                            local_file = "/".join([root, fn])
                            
                        
                        if (out_type == "azcli"):
                            f.write(testme + "" + cmd_proc + "az storage blob upload --account-name " + var_start+"account"+var_end + 
                                    " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " + 
                                    var_start+"container"+var_end + " --name \"" + var_start+"ver"+var_end + "/" + 
                                    fn + "\" --file \"" + local_file + "\" --overwrite --only-show-errors --output none" + testme_end + eol)     
                        elif (out_type == "azcopy"):
                            if (root.startswith("az://")):
                                f.write(testme + "" + cmd_proc + "azcopy copy \"https://" + var_start+"mod_local_account"+var_end + 
                                        ".blob.core.windows.net/" + var_start+"mod_local_container"+var_end + "/" + 
                                        fn + "?" + var_start+"mod_local_sas_token"+var_end + "\" \"https://" + 
                                        var_start+"account"+var_end + ".blob.core.windows.net/" + 
                                        var_start+"container"+var_end + "/" + var_start+"ver"+var_end + 
                                        "/" + fn + "?" + var_start+"sas_token"+var_end + "\"" + testme_end + eol )
                            elif (root.startswith("s3://")):
                                f.write(testme + "" + cmd_proc + "azcopy copy \"https://s3.amazonaws.com/" + 
                                        var_start+"mod_local_container"+var_end + "/" + fn + "\" \"https://" + 
                                        var_start+"account"+var_end + ".blob.core.windows.net/" + 
                                        var_start+"container"+var_end + "/" + var_start+"ver"+var_end + 
                                        "/" + fn + "?" + var_start+"sas_token"+var_end + "\"" + testme_end + eol )
                            # elif (root.startswith("gs://")):
                            #     ...
                            else:
                                f.write(testme + "" + cmd_proc + "azcopy copy \"" + local_file + "\" \"https://" + 
                                        var_start+"account"+var_end + ".blob.core.windows.net/" + 
                                        var_start+"container"+var_end + "/" + var_start+"ver"+var_end + 
                                        "/" + fn + "?" + var_start+"sas_token"+var_end + "\"" + testme_end + eol )
                                
                        f.write("echo " + fn + " >> files_" + var_start+"dt"+var_end + ".txt" + eol)
                        f.write(eol)
                        
                        count += 1
                        upload_count += 1
                        
                        if (count >= step):
                            # last part is again the same.
                            f.write(eol)
                            f.write(testme + "python chorus_upload -c " + config_fn + 
                                    " file mark_as_uploaded_local --local-journal \"" + var_start+"local_journal"+var_end + "\"" +
                                    " --file-list files_" + var_start+"dt"+var_end + ".txt --upload-datetime " + 
                                    var_start+"dt"+var_end + testme_end + eol)

                            f.write(eol)
                            if is_windows:
                                f.write("if exist files_" + var_start+"dt"+var_end + ".txt del files_" + 
                                        var_start+"dt"+var_end + ".txt" + eol)
                                f.write("type nul > files_" + var_start+"dt"+var_end + ".txt" + eol)
                            else:
                                f.write("if [ -e files_" + var_start+"dt"+var_end + ".txt ]; then" + eol)
                                f.write("    rm files_" + var_start+"dt"+var_end + ".txt" + eol)
                                f.write("fi" + eol)
                                f.write("touch files_" + var_start+"dt"+var_end + ".txt" + eol)
                            f.write(eol)
                            count = 0
                        if (max_upload_count is not None) and (max_upload_count > 0) and (upload_count > max_upload_count):
                            break
                              
                    if count > 0:
                        # last part is again the same.
                        f.write(eol)
                        f.write(testme + "python chorus_upload -c " + config_fn + 
                                " file mark_as_uploaded_local --local-journal \"" + var_start+"local_journal"+var_end + "\"" +
                                " --file-list files_" + var_start+"dt"+var_end + ".txt --upload-datetime " + 
                                var_start+"dt"+var_end + testme_end + eol)
                        f.write(eol)
                        
                        if is_windows:
                            f.write("if exist files_" + var_start+"dt"+var_end + ".txt del files_" + 
                                    var_start+"dt"+var_end + ".txt" + eol)
                            f.write("type nul > files_" + var_start+"dt"+var_end + ".txt" + eol)
                        else:
                            f.write("if [ -e files_" + var_start+"dt"+var_end + ".txt ]; then" + eol)
                            f.write("    rm files_" + var_start+"dt"+var_end + ".txt" + eol)
                            f.write("fi" + eol)
                            f.write("touch files_" + var_start+"dt"+var_end + ".txt" + eol)

                    if (max_upload_count is not None) and (max_upload_count > 0) and (upload_count > max_upload_count):
                        break
                            
            f.write(comment + " TEST checkin the journal" + eol)
            f.write(testme + "" + cmd_proc + "az storage blob list --account-name " + var_start+"account"+var_end + 
                    " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " + 
                    var_start+"container"+var_end + " --prefix " + var_start+"ver"+var_end +
                    "/ --output table" + testme_end + eol)     
            f.write(eol)
            # backup to uploaded directory.
            f.write(testme + "" + cmd_proc + "az storage blob upload --account-name " + var_start+"account"+var_end + 
                        " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " + 
                        var_start+"container"+var_end + " --name \"" + var_start+"ver"+var_end + "/journal.db\"" + 
                        " --file \"" + var_start+"local_journal"+var_end + "\" --overwrite --only-show-errors --output none" + testme_end + eol) 
            
            if cloud_journal.startswith("az://"):
                if (journal_transport == "builtin"):
                    f.write(testme + "python chorus_upload -c " + config_fn + 
                            " journal checkin --local-journal \"" + var_start+"local_journal"+var_end + "\"" + testme_end + eol)
                else:
                    f.write(comment + " COPY journal file to upload directory" + eol)
                    f.write(testme + "" + cmd_proc + "az storage blob upload --account-name " + var_start+"account"+var_end + 
                                            " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " + 
                                            var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + 
                                            "\" --file \"" + var_start+"local_journal"+var_end + "\" --overwrite --only-show-errors --output none" + testme_end + eol)
                    f.write(testme + "" + cmd_proc + "az storage blob delete --account-name " + var_start+"account"+var_end + 
                                            " --sas-token \"" + var_start+"sas_token"+var_end + "\" --container-name " + 
                                            var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" + 
                                            " --only-show-errors --output none" + testme_end + eol)
                    f.write(eol)
                
            
# helper to display/save files to upload
def _select_files(args, config, journal_fn):
    
    # get the local path and central path and credentials
    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    # do one modality at a time
    upload_datetime = args.version if args.version else time.strftime("%Y%m%d%H%M%S")

    if (args.output_file is None) or (args.output_file == ""):
        for mod in mods:
            mod_files = list_files(journal_fn, version = args.version, modalities = [mod], verbose=args.verbose)
            mod_config = config_helper.get_site_config(config, mod)

            for (version, files) in mod_files.items():
                print("files for version: ", version, " root at ", mod_config["path"], " for ", mod)
                print("\n".join(files))
    else:
        central = config_helper.get_central_config(config)
        file_list = {}
        for mod in mods:
            mod_files = list_files(journal_fn, version = args.version, modalities = [mod], verbose=args.verbose)
            mod_config = config_helper.get_site_config(config, mod)

            file_list[mod] = (mod_config, mod_files)
    
        _write_files(file_list, upload_datetime, filename = args.output_file, 
                     out_type = args.output_type, dest_config = central, 
                     config_fn = args.config, max_num_files = args.max_num_files,
                     journal_transport = config_helper.get_journal_config(config).get('upload_method', 'builtin'),
                     cloud_journal = config_helper.get_journal_config(config).get('path', 'journal.db'))
    
    
def _mark_as_uploaded(args, config, journal_fn):
    upload_datetime = args.upload_datetime
    file = args.file
    filelist = args.file_list
    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    journal_f = args.local_journal if args.local_journal else journal_fn
    
    if (filelist is not None):
        with open(filelist, 'r') as f:
            files = [fn.strip() for fn in f.readlines()]
            upload_ops.mark_as_uploaded(centralfs, journal_f, upload_datetime, files, verbose = args.verbose)
    else:
        upload_ops.mark_as_uploaded(centralfs, journal_f, upload_datetime, [file], verbose = args.verbose)
        
        
def _mark_as_deleted(args, config, journal_fn):
    version = args.version
    file = args.file
    filelist = args.file_list
    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    journal_f = args.local_journal if args.local_journal else journal_fn
    
    if (filelist is not None):
        with open(filelist, 'r') as f:
            files = [fn.strip() for fn in f.readlines()]
            local_ops.mark_files_as_deleted(files, databasename = journal_f, version = version, verbose = args.verbose)
    else:
        local_ops.mark_files_as_deleted([file], databasename = journal_f, version = version, verbose = args.verbose)
        

def _checkout_journal(args, config, journal_fn):
    local_fn = args.local_journal if args.local_journal else "journal.db"
    journal_upload_method = config.get('journal_upload_method', 'builtin')
    

    # if local_fn exists, make a backup.
    if Path(local_fn).exists():
        backup_fn = local_fn + ".bak_" + time.strftime("%Y%m%d%H%M%S")
        print("INFO: Using local journal ", local_fn, " backup at ", backup_fn)
        shutil.copy(local_fn, backup_fn)
    
    if journal_fn.startswith("az://") or journal_fn.startswith("s3://") or journal_fn.startswith("gs://"):
        # central journal
        # now download the journal file
        journal_path, locked_path, local_path, journal_md5 = upload_ops.checkout_journal(config, local_fn)
        if not Path(local_fn).exists():
            if journal_md5 is not None:
                raise ValueError("ERROR: journal file not properly checked out: ", local_fn)
            # else - original journal does not exist.
        else:
            shutil.copy(local_fn, local_fn + ".bak")  # used for md5.
        print("INFO: checked out ", journal_path, " to ", local_path, " with md5 ", journal_md5, " locked at ", locked_path)
                
    else:
        # if local_fn and journal_fn are same, do nothing.
        if local_fn != journal_fn:
            if Path(journal_fn).exists():            
                # make a copy
                shutil.copy(journal_fn, local_fn)
            print("INFO: copied ", journal_fn, " to ", local_fn)
        else:
            # else if local_fn and journal_fn are same, do nothing.
            print("INFO: journal file is already local.")    
        


def _checkin_journal(args, config, journal_fn):
    local_fn = args.local_journal if args.local_journal else "journal.db"
    journal_upload_method = config.get('journal_upload_method', 'builtin')

    
    if journal_fn.startswith("az://") or journal_fn.startswith("s3://") or journal_fn.startswith("gs://"):
        # if cloud journal
        client = storage_helper._make_client(config_helper.get_journal_config(config))
        
        # compute the lock path and md5
        orig_md5 = FileSystemHelper.get_metadata(path=Path(local_fn + ".bak"), with_md5=True)['md5']
        # generate the lock file name
        journal_path = FileSystemHelper(journal_fn, client = client)
                
        local_file = FileSystemHelper(local_fn)
        # check in to central.
        upload_ops.checkin_journal(journal_path, local_file, orig_md5)
 
        print("INFO: checked in ", local_fn, " to ", journal_fn, " with original md5 ", orig_md5)
    
    else:
        # local journal
        if journal_fn != local_fn:
            # back up journal_fn
            backup_fn = journal_fn + ".bak_" + time.strftime("%Y%m%d%H%M%S")
            if Path(journal_fn).exists():            
                shutil.copy(journal_fn, backup_fn)

            # and copy local_fn back/
            if Path(local_fn).exist():
                shutil.copy(local_fn, journal_fn)
            else:
                print("ERROR:  no local file ", local_file, "to copy to ", journal_fn)            
        else:
            # else if local_fn and journal_fn are same, do nothing.
            print("INFO: journal file is already local.")    
    

    
    

# helper to upload files
def _upload_files(args, config, journal_fn):
    max_upload_count = int(args.max_num_files) if ("max_num_files" in vars(args)) and (args.max_num_files is not None) else None

    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }

    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    # if (args.site_path is None):
    #     raise ValueError("site path is required")
    
    # if (args.central_path is None):
    #     raise ValueError("central path is required")
    # subsequent modalities in this call always amend.
    
    if max_upload_count is not None:
        remaining = max_upload_count
        for mod, mod_config in mod_configs.items():
            sitefs = FileSystemHelper(mod_config["path"], client = _make_client(mod_config))
            _, remaining = upload_ops.upload_files(sitefs, centralfs, modalities = [mod], databasename = journal_fn, verbose = args.verbose, max_num_files = remaining)
            if remaining <= 0:
                break        
    else:
        for mod, mod_config in mod_configs.items():
            sitefs = FileSystemHelper(mod_config["path"], client = _make_client(mod_config))
            upload_ops.upload_files(sitefs, centralfs, modalities = [mod], databasename = journal_fn, verbose = args.verbose)
    
# helper to report file verification
def _verify_files(args, config, journal_fn):
    print("NOTE: This will download files to verify md5.  It will take a while and can take significant disk space.")
    
    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }        

    central_config = config_helper.get_central_config(config)
    centralfs = FileSystemHelper(central_config["path"], client = _make_client(central_config))
    
    upload_ops.verify_files(centralfs, journal_fn, version = args.version, modalities = mods, verbose = args.verbose)
    
def _list_versions(args, config, journal_fn):
    print("INFO: Uploads known in current journal: ")
    local_ops.list_versions(journal_fn)
    print("INFO: Backed up journals: ")
    local_ops.list_journals(journal_fn)


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
    
    #------- history
    parser_history = subparsers.add_parser("history", help = "show command history")
    parser_history.set_defaults(func = _show_history)
    
    #------ create subparsers
    parser_journal = subparsers.add_parser("journal", help = "journal operations (update, list, checkout, checkin, unlock)")
    journal_subparsers = parser_journal.add_subparsers(help="sub-command help", dest="journal_command")
    
    parser_file = subparsers.add_parser("file", help = "file operations (list, upload, mark_as_uploaded_central, mark_as_uploaded_local, verify)")
    file_subparsers = parser_file.add_subparsers(help="sub-command help", dest="file_command")
    
    
    #------ create the parser for the "update" command
    parser_update = journal_subparsers.add_parser("update", help = "create or update the current journal")
    parser_update.add_argument("--modalities", 
                               help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", 
                               required=False)
    parser_update.add_argument("--version", 
                               help="version string for the upcoming upload.  If not specified, the current datetime in YYYYMMDDHHMMSS format is used.", 
                               required=False)
    parser_update.add_argument("--amend", 
                               help="amend the last journal update.  If this flag is set but the version is not provided, then the last version is used.", 
                               action="store_true")
    parser_update.set_defaults(func = _update_journal)
    
    # create the parser for the "list" command
    parser_list = journal_subparsers.add_parser("list", help = "list the versions in a journal database")
    parser_list.set_defaults(func = _list_versions)
    
    parser_checkout = journal_subparsers.add_parser("checkout", help = "checkout a cloud journal file and create a local copy named journal.db")
    parser_checkout.add_argument("--local-journal", help="local filename for the journal file", required=False)
    parser_checkout.set_defaults(func = _checkout_journal)
    
    parser_checkin = journal_subparsers.add_parser("checkin", help = "check in a cloud journal file from local copy")
    parser_checkin.add_argument("--local-journal", help="local filename for the journal file", required=False)
    parser_checkin.set_defaults(func = _checkin_journal)
    
    parser_unlock = journal_subparsers.add_parser("unlock", help = "unlock a cloud journal file")
    parser_unlock.set_defaults(func = upload_ops.unlock_journal)
        

    
    # DANGEROUS
    # parser_revert = subparsers.add_parser("revert-version", help = "revert to a previous journal version.")
    # parser_revert.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)
    # parser_revert.set_defaults(func = _revert_journal)

    # # DANGEROUS
    # parser_delete = subparsers.add_parser("delete", help = "delete version in a journal database")
    # parser_delete.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)

    # ----------- file stuff.

    # create the parser for the "select" command
    parser_select = file_subparsers.add_parser("list", help = "list files to upload")
    parser_select.add_argument("--version", help="datetime of an upload (use list to get date times).  defaults to all un-uploaded files.", required=False)
    parser_select.add_argument("--modalities", help="list of modalities to include in the journal update. defaults to all.  case sensitive.", required=False)
    parser_select.add_argument("--output-file", help="output file", required=False)
    parser_select.add_argument("--output-type", help="the output file type: [list | azcli | azcopy].  azcli and azcopy are executable scripts.", required=False)
    parser_select.add_argument("--max-num-files", help="maximum number of files to list.", required=False)
    parser_select.set_defaults(func = _select_files)
    
    
    parser_delete = file_subparsers.add_parser("mark_as_deleted", 
                                        help = "mark a file or a list of files as deleted. ")
    parser_delete.add_argument("--version", help="a version to work on (use 'journal list' to get date times).  defaults to all versions.", required=False)
    parser_delete.add_argument("--file", help="file name of the file to mark as deleted. uses relative path", required=False)
    parser_delete.add_argument("--file-list", help="file list to mark as deleted.  uses relative path", required=False)
    parser_delete.add_argument("--local-journal", help="local journal file name as alternative to the remote journal", required=False)
    parser_delete.set_defaults(func = _mark_as_deleted)

    
    parser_upload = file_subparsers.add_parser("upload", help = "upload files to server")
    parser_upload.add_argument("--version", help="datetime of an upload (use list to get date times). defaults to all un-uploaded files", required=False)
    parser_upload.add_argument("--modalities", 
                               help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", 
                               required=False)
    parser_upload.add_argument("--max-num-files", help="maximum number of files to list.", required=False)

    # optional list of files. if not present, use current journal
    # when upload, mark the journal version as uploaded.
    parser_upload.set_defaults(func = _upload_files)

    parser_mark = file_subparsers.add_parser("mark_as_uploaded_central", 
                                        help = "verify a file and mark as uploaded with time stamp.")
    parser_mark.add_argument("--upload-datetime", help="datetime of an upload (use list to get date times).", required=True)
    parser_mark.add_argument("--file", help="file name of the file to check.  version/file is the path in the central azure environment", required=False)
    parser_mark.add_argument("--file-list", help="file list to check.  version/file is the path in the central azure environment", required=False)
    parser_mark.set_defaults(func = _mark_as_uploaded)

    parser_mark = file_subparsers.add_parser("mark_as_uploaded_local", 
                                        help = "verify a file and mark as uploaded with time stamp.")
    parser_mark.add_argument("--upload-datetime", help="datetime of an upload (use list to get date times).", required=True)
    parser_mark.add_argument("--file", help="file name of the file to check.  version/file is the path in the central azure environment", required=False)
    parser_mark.add_argument("--file-list", help="file list to check.  version/file is the path in the central azure environment", required=False)
    parser_mark.add_argument("--local-journal", help="local journal file name as alternative to the remote journal", required=False)
    parser_mark.set_defaults(func = _mark_as_uploaded)

    
    parser_verify = file_subparsers.add_parser("verify", 
                                          help = "verify a submssion version in cloud. Note Upload-files also verifies. WARNING files will be downloaded for md5 computation")
    parser_verify.add_argument("--version", 
                               help="datetime of a submission version.  defaults to most recent version.", 
                               required=False)
    parser_verify.add_argument("--modalities", 
                               help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP'.  case sensitive.", 
                               required=False)
    parser_verify.set_defaults(func = _verify_files)
        
        
    # parse the arguments
    args = parser.parse_args()
    
    #============= locate and parse the config file
    # get the current script path
    if args.config is not None:
        config_fn = args.config
    else:
        scriptdir = Path(__file__).absolute().parent
        config_fn = str(scriptdir.joinpath("config.toml"))
    
    if (args.command in ["config-help", "usage"]):
        # call the subcommand function.
        args.func(args, None, None)
        
    else:
        if (not Path(config_fn).exists()):
            raise ValueError("Config file does not exist: ", 
                            config_fn, 
                            ". please create one from the config.toml.template file,",
                            " or specify the config file with the -c option.")
        
        print("INFO: Using config file: ", config_fn)
        config = config_helper.load_config(config_fn)
        upload_method = config_helper.get_upload_methods(config)

        if ((args.command in ["journal"]) and (args.journal_command in ["unlock", "checkout", "checkin"])):
            # if unlock, do it in the cloud only. for checkout and checkin, the argfunc handles the checkin and checkout.
            # TODO: merge this later.
            skip_history = True
            skip_checkout = True
            skip_checkin = True
            # journal_path, locked_path, local_path, journal_md5 = upload_ops.checkout_journal(config, simulate = skip_checkout)
            # journal_fn = str(journal_path.root)
            journal_fn = config_helper.get_journal_config(config)["path"]
        elif ((args.command in ["file"]) and (args.file_command in ["mark_as_uploaded_local"])):
            # purely local operation on the local file.
            skip_history = False
            skip_checkout = True
            skip_checkin = True
            journal_fn = args.local_journal if args.local_journal else "journal.db"
        # elif ((args.command in ["file"]) and (args.file_command in ["upload"]) and ("resume" in vars(args)) and (args.resume)):
        #     # resume an upload means no checkout is needed
        #     skip_history = False
        #     skip_checkout = True
        #     skip_checkin = False
        #     journal_path, locked_path, local_path, journal_md5 = upload_ops.checkout_journal(config, simulate = skip_checkout)
        #     journal_fn = str(local_path.root)
        else:
            # normal path - checkout, compute, checkin.
            skip_history = False
            skip_checkout = False
            skip_checkin = False
            journal_path, locked_path, local_path, journal_md5 = upload_ops.checkout_journal(config)
            journal_fn = str(local_path.root)
            
        # === save command history
        if not skip_history:
            args_dict = history_ops._strip_account_info(args)
            if ("config" not in args_dict.keys()) or (args_dict["config"] is None):
                args_dict["config"] = config_fn
            command_id = history_ops.save_command_history(*(history_ops._recreate_params(args_dict)),
                                                        *(history_ops._get_paths_for_history(args, config)), journal_fn)

        # call the subcommand function.
        start = time.time()
        args.func(args, config, journal_fn)
        end = time.time()
        elapsed = end - start
        print(f"Command Completed in {elapsed:.2f} seconds.")

        # save history runtime
        if not skip_history:
            history_ops.update_command_completion(command_id, elapsed, journal_fn)
            
        # and check in.
        if not skip_checkin:
            # push journal up.
            upload_ops.checkin_journal(journal_path, local_path, journal_md5)
            
    
