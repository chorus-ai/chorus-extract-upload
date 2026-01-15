import time
import argparse

from chorus_upload.local_ops import update_journal, list_files_with_info, _get_modality_pattern
# from chorus_upload.generate_journal import restore_journal, list_uploads, list_journals
# from chorus_upload.upload_ops_builtin import upload_files, verify_files, list_files
from chorus_upload import history_ops 
from chorus_upload.storage_helper import FileSystemHelper, _make_client
from pathlib import Path
from chorus_upload import config_helper
from chorus_upload import upload_ops
from chorus_upload import local_ops
import chorus_upload.storage_helper as storage_helper

from chorus_upload.journaldb_ops import JournalDispatcher
import chorus_upload.journaldb_ops as journaldb_ops

from remote_file_ops import _list_remote_files, _upload_remote_files, _download_remote_files, _delete_remote_files

import parse

from script_generators import _write_files

import logging
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s:%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


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
# TODO: DONE add support for full vs append modes for journal update
# TODO: DONE add support for marking files as deleted.

# TODO: DONE handle the case where the journal has to be uploaded by azcli because of security review.
#       journal update, list
#       file mark_as_deleted, upload, marked_as_uploaded_central, verify
#    workaround - if azcli is only transport, specify the journal file as local. last line in azcopy uploads journal directory.
# TODO: DONE performance improvement - review database query call patterns.  minimize number of calls.
# TODO: DONE abstract sqlite calls.
# TODO: DONE parallelize upload support?
# TODO: DONE additional parameters in config.toml for page_size and num_threads
# TODO: DONE add param in config.toml for local journal path
# TODO: journaling_mode "append" would have one directory per submission.  how should these be managed?
# TODO: DONE support different path patterns per modality path
# TODO: DONE fix cloud md5.  azure blob md5 is in content-md5 header - automatically base64 encoded.   Need to fix all cloud files where md5 are doubly base64 encoded.
# TODO: DONE updated with variable number of threads for upload to minimize connection timeout
# TODO: asyncio?
# TODO: DONE (v2) minimize db size.
# TODO: process uploaded data in central
# TODO: DONE add support to upload files in the submission directory: SUBMISSION/WAVEFORM_SUBMISSION.md
# TODO: refactor script file generation to script_generators.py
# TODO: support different journal transport methods.
# TODO: support different upload methods.
# TODO: DONE change to require explicit checkout and checkin - other functions should work from journal.db.local file.
# TODO: azcli needs account_name and should fail if that is not there...
# TODO: DONE azure-azure copy requires {account_name}.blob.core.windows.net in the URL.
# TODO: use logger instead of print.


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
    print("             python chorus_upload journal update --modalities Waveforms,Images,Metadata")
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
    journal_version = args.version
    amend = args.amend
    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }
    journaling_mode = config_helper.get_journaling_mode(config)

    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)

    # for each modality, create the file system help and process
    first = True
    for mod, mod_config in mod_configs.items():        
        # create the file system helper    
        client, internal_host =storage_helper._make_client(mod_config)
        datafs = FileSystemHelper(config_helper.get_path_str(mod_config), client = client, internal_host = internal_host)

        log.info(f"Update journal {journal_fn} for {mod}")
        update_journal(datafs, modalities = [mod], 
                       databasename = journal_fn, 
                       journaling_mode = journaling_mode,
                       version = journal_version, amend = (amend if first else True), 
                       verbose = args.verbose, num_threads = nthreads, page_size = page_size,
                       modality_configs = {mod: mod_config})
        first = False
            
# helper to revert to a previous journal
# def _revert_journal(args, config, journal_fn):
#     revert_time = args.version
#     log.info("Revert to: {revert_time}")
#     restore_journal(journal_fn, revert_time)
    

            
# helper to display/save files to upload
def _select_files(args, config, journal_fn):
    
    # get the local path and central path and credentials
    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    modality_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }
    # do one modality at a time
    ver = args.version if ('version' in vars(args)) and (args.version is not None) else time.strftime("%Y%m%d%H%M%S")
    page_size = config_helper.get_config(config).get('page_size', 1000)

    if (args.output_file is None) or (args.output_file == ""):
        for mod in mods:
            mod_files, _, _ = list_files_with_info(journal_fn, version = args.version, modalities = [mod],
                                                   verbose=args.verbose, modality_configs = modality_configs)
            mod_config = config_helper.get_site_config(config, mod)

            if mod_files is None:
                log.info(f"No files found for modality {mod}")
                continue

            for (version, files) in mod_files.items():
                print("files for version: ", version, " root at ", config_helper.get_path_str(mod_config), " for ", mod)
                print("\n".join(files))
    else:
        # generate a script for uploading files.
        central = config_helper.get_central_config(config)
        
        file_list = {}
        for mod in mods:
            _, active_files, _ = list_files_with_info(journal_fn, version = args.version, modalities = [mod], 
                                                   verbose=args.verbose, modality_configs = modality_configs)
            mod_config = config_helper.get_site_config(config, mod)

            file_list[mod] = (mod_config, active_files)
            
        out_type = args.output_type if ('output_type' in vars(args)) and (args.output_type is not None) else \
            config_helper.get_config(config).get('upload_method', 'list')
        journal_transport = args.output_type if ('output_type' in vars(args)) and (args.output_type is not None) else \
            config_helper.get_journal_config(config).get('upload_method', 'list')
    
        _write_files(file_list, ver, 
                     filename = args.output_file, 
                     **{'out_type': out_type, 
                        'dest_config': central, 
                        'config_fn': args.config, 
                        'max_num_files': int(args.max_num_files) if ("max_num_files" in vars(args)) and (args.max_num_files is not None) else None,
                        'journal_transport': journal_transport,
                        'cloud_journal': config_helper.get_journal_path(config),
                        'local_journal': journal_fn,
                        'page_size': page_size})
    
    
# file or filelist should be local relative paths.
# journal_fn is always local.
def _mark_as_uploaded(args, config, journal_fn):
    upload_datetime = args.upload_datetime if 'upload_datetime' in vars(args) else time.strftime("%Y%m%d%H%M%S")
    file = args.file if 'file' in vars(args) else None
    filelist = args.file_list if 'file_list' in vars(args) else None
    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    journal_f = args.local_journal if 'local_journal' in vars(args) else journal_fn
    
    mods = config_helper.get_modalities(config)
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }    
    compiled_patterns = { mod: parse.compile(_get_modality_pattern(mod, mod_configs))  for mod in mods }

    if (filelist is not None):
        with open(filelist, 'r') as f:
            files = [fn.strip() for fn in f.readlines()]
            upload_ops.mark_as_uploaded(centralfs, journal_f, upload_datetime, files, **{'verbose' : args.verbose, 'modality_configs': mod_configs, 'compiled_patterns': compiled_patterns})
    else:
        upload_ops.mark_as_uploaded(centralfs, journal_f, upload_datetime, [file], **{'verbose' : args.verbose, 'modality_configs': mod_configs, 'compiled_patterns': compiled_patterns})
        
        
# file or filelist should be local relative paths.
def _mark_as_deleted(args, config, journal_fn):
    version = args.version if 'version' in vars(args) else None
    file = args.file if 'file' in vars(args) else None
    filelist = args.file_list if 'file_list' in vars(args) else None
    
    if (filelist is not None):
        with open(filelist, 'r') as f:
            files = [fn.strip() for fn in f.readlines()]
            local_ops.mark_files_as_deleted(files, databasename = journal_fn, version = version, verbose = args.verbose)
    elif (file is not None):
        local_ops.mark_files_as_deleted([file], databasename = journal_fn, version = version, verbose = args.verbose)
    else:
        log.error("must provide either --file or --file-list")
    
    

# helper to upload files
def _upload_files(args, config, journal_fn):
    max_upload_count = int(args.max_num_files) if ("max_num_files" in vars(args)) and (args.max_num_files is not None) else None

    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }

    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    remaining = max_upload_count
    for mod, mod_config in mod_configs.items():
        client, internal_host =storage_helper._make_client(mod_config)
        sitefs = FileSystemHelper(config_helper.get_path_str(mod_config), client = client, internal_host = internal_host)
        _, remaining = upload_ops.upload_files_parallel(sitefs, centralfs, modalities = [mod], databasename = journal_fn, max_num_files = remaining, 
                                                    verbose = args.verbose, num_threads = nthreads, page_size = page_size, modality_configs = mod_configs)
        if (remaining is not None) and (remaining <= 0):
            break        
    
# helper to report file verification
def _verify_files(args, config, journal_fn):
    log.info("This will NOT download files to verify md5.  It relies on file size and previously saved md5.")
    
    default_modalities = config_helper.get_modalities(config)
    mods = args.modalities.split(',') if ("modalities" in vars(args)) and (args.modalities is not None) else default_modalities
    # get the config path for each modality.  if not matched, use default.
    mod_configs = { mod: config_helper.get_site_config(config, mod) for mod in mods }  
    compiled_patterns = { mod: parse.compile(_get_modality_pattern(mod, mod_configs)) for mod in mods }      

    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    upload_ops.verify_files(centralfs, journal_fn, version = args.version, modalities = mods,
                            **{'num_threads': nthreads, 'page_size': page_size, 'verbose' : args.verbose, 'modality_configs': mod_configs, 'compiled_patterns': compiled_patterns})
    
    
def _list_versions(args, config, journal_fn):
    log.info("Uploads known in current journal: ")
    local_ops.list_versions(journal_fn)
    # log.info("Backed up journals: ")
    # local_ops.list_journals(journal_fn)


if __name__ == "__main__":
    
    # parse command line arguments
    parser = argparse.ArgumentParser(description="Generate journal for a site folder")
    parser.add_argument("-v", "--verbose", help="verbose output", action="store_true")
    parser.add_argument("-c", "--config", help="config file (defaults to config.toml) with storage path locations", required=False)
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    subparsers.required = True
    
    #------ help
    parser_help = subparsers.add_parser("usage", help = "show help information")
    parser_help.set_defaults(func = _print_usage)
    
    #------ config file help
    parser_auth = subparsers.add_parser("config-help", help = "show help information about the configuration file")
    parser_auth.set_defaults(func = config_helper._print_config_usage)
    
    #------- history subparser
    parser_history = subparsers.add_parser("history", help = "show command history")
    parser_history.set_defaults(func = _show_history)
    
    #------ create subparsers
    #------ create the parser for the "journal" command
    parser_journal = subparsers.add_parser("journal", help = "journal operations (update, list, checkout, checkin, unlock)")
    journal_subparsers = parser_journal.add_subparsers(help="sub-command help", dest="journal_command")
    
    
    #------ create the parser for the "update" command
    parser_update = journal_subparsers.add_parser("update", help = "create or update the current journal")
    parser_update.add_argument("--modalities", 
                               help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP,Metadata'.  case sensitive.", 
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
    parser_checkout.add_argument("--local-journal", help="local filename for the journal file, overrides config file", required=False)
    # parser_checkout.set_defaults(func = _checkout_journal)
    
    parser_checkin = journal_subparsers.add_parser("checkin", help = "check in a cloud journal file from local copy")
    parser_checkin.add_argument("--local-journal", help="local filename for the journal file, overrides config file", required=False)
    # parser_checkin.set_defaults(func = _checkin_journal)
    
    parser_unlock = journal_subparsers.add_parser("unlock", help = "unlock a cloud journal file")
    # parser_unlock.set_defaults(func = upload_ops.unlock_journal)
        
    parser_upgrade = journal_subparsers.add_parser("upgrade", help = "upgrade a journal database to the NEXT version")
    
    # DANGEROUS
    # parser_revert = subparsers.add_parser("revert-version", help = "revert to a previous journal version.")
    # parser_revert.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)
    # parser_revert.set_defaults(func = _revert_journal)

    # # DANGEROUS
    # parser_delete = subparsers.add_parser("delete", help = "delete version in a journal database")
    # parser_delete.add_argument("--version", help="datetime of an upload (use list to get date times).", required=False)

    # ----------- file stuff.
    #------ create the parser for the "file" command
    parser_file = subparsers.add_parser("file", help = "file operations (list, upload, mark_as_uploaded_central, mark_as_uploaded_local, verify)")
    file_subparsers = parser_file.add_subparsers(help="sub-command help", dest="file_command")
    

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
                               help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP,Metadata'.  case sensitive.", 
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
                               help="list of modalities to include in the journal update. defaults to 'Waveforms,Images,OMOP,Metadata'.  case sensitive.", 
                               required=False)
    parser_verify.set_defaults(func = _verify_files)
        
    #------- remote file management    
    #------ create the parser for the "file" command
    parser_remote_file = subparsers.add_parser("remote", help = "remote file operations (list, upload, download, delete)")
    remote_file_subparsers = parser_remote_file.add_subparsers(help="sub-command help", dest="remote_command")

    # create the parser for the "list" command
    parser_remote_list = remote_file_subparsers.add_parser("list", help = "list files to upload")
    parser_remote_list.add_argument("file", nargs="?", help="file name, directory, or pattern to match files in Azure storage", default=None)
    parser_remote_list.add_argument("--recursive", "-r", help="recursively list files in subdirectories", action="store_true", required=False)
    parser_remote_list.set_defaults(func = _list_remote_files)
    
    # create the parse for "upload" command
    parser_remote_upload = remote_file_subparsers.add_parser("upload", help = "upload file to central")
    parser_remote_upload.add_argument("--overwrite", help="overwrite the file if it exists in Azure storage", action="store_true", required=False)
    parser_remote_upload.add_argument("local", nargs="+", help="source filename pattern(s) or directory(ies) (trailing /) on local filesystem")
    parser_remote_upload.add_argument("remote", help="destination directory in Azure storage with trailing /")
    parser_remote_upload.add_argument("--recursive", "-r", help="recursively include files in subdirectories", action="store_true", required=False)
    parser_remote_upload.set_defaults(func = _upload_remote_files)

    # create the parser for "download" command
    parser_remote_download = remote_file_subparsers.add_parser("download", help = "download files from central")
    parser_remote_download.add_argument("--overwrite", help="overwrite the file if it exists on local filesystem", action="store_true", required=False)
    parser_remote_download.add_argument("remote", nargs="+", help="source filename pattern, or directory (trailing /) on remote filesystem")
    parser_remote_download.add_argument("local", help="destination file or directory in local storage (trailing /)")
    parser_remote_download.add_argument("--recursive", "-r", help="recursively include files in subdirectories", action="store_true", required=False)
    parser_remote_download.set_defaults(func = _download_remote_files)

    # create the parser for "delete" command
    parser_remote_delete = remote_file_subparsers.add_parser("delete", help = "delete files from central")
    parser_remote_delete.add_argument("files", nargs="+", help="file name, directory, or pattern to match files in Azure storage", default=None)
    parser_remote_delete.add_argument("--recursive", "-r", help="recursively delete files in subdirectories", action="store_true", required=False)
    parser_remote_delete.set_defaults(func = _delete_remote_files)
    
        
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
        
        log.info(f"Using config file: {config_fn}")
        config = config_helper.load_config(config_fn)
        
        # get the configuration for profiling
        JournalDispatcher.profiling = config["configuration"].get("profiling", False)
        
        # set a default client for central storage
        central_config = config_helper.get_central_config(config)
        central_client, internal_host =storage_helper._make_client(central_config)
        central_client.set_as_default_client()
        
        local_journal_fn_override = args.local_journal if ('local_journal' in vars(args)) and (args.local_journal is not None) else None
        journal_path, lock_path, local_path = upload_ops.get_journal_paths(config, 
                                                                           local_fn_override = local_journal_fn_override)
        
        if ((args.command in ["journal"]) and (args.journal_command in ["unlock"])):

            upload_ops.unlock_journal(journal_path, lock_path)

        elif ((args.command in ["journal"]) and (args.journal_command in ["checkout"])):
            # for checkout and checkin, the argfunc handles the checkin and checkout.
            upload_ops.checkout_journal(journal_path, lock_path, local_path)

        elif ((args.command in ["journal"]) and (args.journal_command in ["checkin"])):
            # for checkout and checkin, the argfunc handles the checkin and checkout.
            upload_ops.checkin_journal(journal_path, lock_path, local_path)
        
        elif ((args.command in ["journal"]) and (args.journal_command in ["upgrade"])):                            
            
            local_journal_fn = str(local_path.root)

            if lock_path.is_cloud and lock_path.root.exists():
                raise ValueError(f"ERROR: cannot upgrade journal because it is locked at {lock_path.root}.  Please unlock first.")

            # checkout journal            
            upload_ops.checkout_journal(journal_path, lock_path, local_path)
            
            # === no need to save command history
            # call the subcommand function.
            start = time.time()
            journaldb_ops._upgrade_journal(local_path, lock_path)
            end = time.time()
            elapsed = end - start
            log.info(f"Command Completed in {elapsed:.2f} seconds.")
    
            # push journal up.
            upload_ops.checkin_journal(journal_path, lock_path, local_path)
        elif (args.command in ["remote"]):
            local_journal_fn = str(local_path.root)
            
            # call the subcommand function.
            start = time.time()
            args.func(args, config, local_journal_fn)
            end = time.time()
            elapsed = end - start
            log.info(f"Command Completed in {elapsed:.2f} seconds.")
            
        else:
            # if ((args.command in ["file"]) and (args.file_command in ["mark_as_uploaded_local"])):
            #     # purely local operation on the local file.
            #     skip_checkout = True
            #     skip_checkin = True

            # else:
            #     # normal path - checkout, compute, checkin.
            #     skip_checkout = False
            #     skip_checkin = False
                            
            local_journal_fn = str(local_path.root)
            
            if lock_path.is_cloud and not lock_path.root.exists():
                log.error(f"journal is not checked out.  Please checkout first.")
                # # checkout the journal
                # upload_ops.checkout_journal(journal_path, lock_path, local_path)
                            
            # === save command history
            args_dict = history_ops._strip_account_info(args)
            if ("config" not in args_dict.keys()) or (args_dict["config"] is None):
                args_dict["config"] = config_fn
            command_id = history_ops.save_command_history(*(history_ops._recreate_params(args_dict)),
                                                        *(history_ops._get_paths_for_history(args, config)), local_journal_fn)

            # call the subcommand function.
            start = time.time()
            args.func(args, config, local_journal_fn)
            end = time.time()
            elapsed = end - start
            log.info(f"Command Completed in {elapsed:.2f} seconds.")

            # save history runtime
            history_ops.update_command_completion(command_id, elapsed, local_journal_fn)
                
            # and check in.
            # if not skip_checkin:
            #     # push journal up.
            #     upload_ops.checkin_journal(journal_path, lock_path, local_path)
            if lock_path.is_cloud:
                log.info(f"operation completed.  please check in journal")
    
