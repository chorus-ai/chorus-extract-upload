from chorus_upload.storage_helper import FileSystemHelper
from chorus_upload import config_helper
import chorus_upload.storage_helper as storage_helper
import os
import concurrent.futures
import threading

import logging
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s:%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def _list_remote_files(args, config, journal_fn):
    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    recursive = args.recursive
    wildcard = "**/*" if recursive else "*"
    
    # get the file name/pattern
    pattern = args.file if args.file else wildcard
    # patterm = f"{pattern}{wildcard}" if (pattern.endswith("/") or pattern.endswith("\\")) else pattern
        
    print(f"Files matching pattern: {pattern}")
    for paths in centralfs.get_files_iter(pattern, page_size=page_size, include_dirs=True):
        for path in paths:
            print(f"{path}")
    
    
def __copy_parallel(src_fs, src_dest_tuples, nthreads):
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        lock = threading.Lock()
        futures = {}
        for src, dest in src_dest_tuples:
            future = executor.submit(src_fs.copy_file_to, relpath=src, dest_path=dest)
            futures[future] = (src, dest)
        
        for future in concurrent.futures.as_completed(futures.keys()):
            src, dest = futures[future]
            try:
                future.result()
                print(f"Copied {src} to {dest}")
            except Exception as e:
                print(f"Error copying file: {e}")

    
def _upload_remote_files(args, config, journal_fn):
    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    if args.remote is None or len(args.remote) == 0:
        print("Please specify at the remote file/directory.")
        return
    
    # first get the remote path
    remote = args.remote
    remote_path = centralfs._make_path(remote)
    remotefs = FileSystemHelper(remote_path, client = client, internal_host = internal_host)
    
    
    if args.local is None or len(args.local) == 0:
        print("Please specify at least one local file/directory to upload.")
        return
    
    
    wildcard = "**/*" if args.recursive else "*"
    
    locals = args.local
    
    # if remote_path is a file, then local file has to be a single file as well.
    if remote_path.is_file():
        if len(args.local) > 1:
            print(f"Remote path {remote} is a file, but multiple local files were specified.")
            return
        
        localfs = FileSystemHelper(locals[0], client = None, internal_host = None)
        if not localfs.root.is_file():
            print(f"Local path {locals[0]} is not a file, can't copy to file remote path {remote}.")
            return
        
        print(f"Uploading {localfs.root} to {remote_path}")
        localfs.copy_file_to(relpath=None, dest_path=remote_path)
        return

    if not remote_path.is_dir():
        print(f"Remote path {remote} is not a directory, cannot copy multiple files to it")
        return
    
    # remote should be a directory now, files could be a list of files, patterns, or directories.
    
    # iterate over each local file in the list and copy to remote directory.
    for local in locals:
        # determine if we are working with absolute or relative paths.
        if local[0] in ["/", "\\", "~"] or local[1:].startswith(":\\"):
            # absolute path
            localfs = FileSystemHelper('/', client = None, internal_host = None)
        else:
            # relative path
            localfs = FileSystemHelper(os.getcwd(), client = None, internal_host = None)
        
        file_list = [] # relative filename, target path

        # check if local is a file, directory, or pattern
        local_pattern = f"{local}{wildcard}" if (local.endswith("/") or local.endswith("\\")) else local
        # glob to get the matching files .
        for paths in localfs.get_files_iter(pattern=local_pattern, page_size=page_size):
            for path in paths:
                remote_path = remotefs._make_path(path)
                file_list.append((path, remote_path))
        
        __copy_parallel(localfs, file_list, nthreads)       
        


def _download_remote_files(args, config, journal_fn):

    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    if args.local is None or len(args.local) == 0:
        print("Please specify at local file/directory.")
        return

    if args.remote is None or len(args.remote) == 0:
        print("Please specify at least one remote file/directory to download.")
        return
        
    recursive = args.recursive
    wildcard = "**/*" if recursive else "*"
    
    # first get the local path
    local = args.local
    if local[0] in ["/", "\\", "~"] or local[1:].startswith(":\\"):
        # absolute path
        localfs = FileSystemHelper('/', client = None, internal_host = None)
    else:
        # relative path
        localfs = FileSystemHelper(os.getcwd(), client = None, internal_host = None)
                
    local_path = localfs._make_path(local)
    
    remotes = args.remote
    
    # if local_path is a file, then remote file has to be a single file as well.
    if local_path.is_file():
        if len(args.remote) > 1:
            print(f"Local path {local} is a file, but multiple remote files were specified.")
            return
        
        remotefs = FileSystemHelper(remotes[0], client = None, internal_host = None)
        if not remotefs.root.is_file():
            print(f"remote path {remotes[0]} is not a file, can't copy to file local path {local}.")
            return
        
        print(f"Downloading {remotefs.root} to {local_path}")
        remotefs.copy_file_to(relpath=None, dest_path=local_path)
        return

    if not local_path.is_dir():
        print(f"Local path {local} is not a directory, cannot copy multiple files to it")
        return
    
    # local should be a directory now, remote files could be a list of files, patterns, or directories.
    
    # iterate over each local file in the list and copy to remote directory.
    for remote in remotes:
        # determine if we are working with absolute or relative paths.
        
        file_list = [] # relative filename, target path
        
        # check if local is a file, directory, or pattern
        remote_pattern = f"{remote}{wildcard}" if (remote.endswith("/") or remote.endswith("\\")) else remote
        # glob to get the matching files .
        for paths in centralfs.get_files_iter(pattern=remote_pattern, page_size=page_size):
            for path in paths:
                local_path = localfs._make_path(path)
                file_list.append((path, local_path))
                
        __copy_parallel(centralfs, file_list, nthreads)
    
    
def _delete_remote_files(args):
    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    if args.files is None or len(args.files) == 0:
        log.error("Please specify at least one file/directory to delete.")
        return
    
    recursive = args.recursive
    wildcard = "**/*" if recursive else "*"
    
    
    patterns = args.files
    # get the file name/pattern
    print(f"Files to be deleted matching pattern(s): {patterns}")
    for pattern in patterns:
        pat = f"{pattern}{wildcard}" if (pattern.endswith("/") or pattern.endswith("\\")) else pattern
        for paths in centralfs.get_files_iter(pat, page_size=page_size):
            for path in paths:
                if path.name.endswith("journal.db") or path.name.endswith("journal.db.locked"):
                    print(f"Skipping journal file {path}.")
                    continue
                print(f"{path}")

            
    print(f"Are you sure you want to delete these files? (y/n)")
    confirm = input().lower()
    if confirm == 'n':
        return

    for pattern in patterns:
        pat = f"{pattern}{wildcard}" if (pattern.endswith("/") or pattern.endswith("\\")) else pattern
        for paths in centralfs.get_files_iter(pat, page_size=page_size):
            for path in paths:
                if path.name.endswith("journal.db") or path.name.endswith("journal.db.locked"):
                    print(f"Skipping journal file {path}.")
                    continue
                print(f"Deleting {path}")
                centralfs.delete_file(path)
                