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
    
    
def __copy_parallel(src_fs, dest_fs, src_list, nthreads):
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        lock = threading.Lock()
        futures = {}
        for src in src_list:
            future = executor.submit(src_fs.copy_file_to, relpath=src, dest_path=dest_fs.root)
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
    
    if args.local is None or len(args.local) == 0:
        print("Please specify at local file/directory.")
        return

    if args.remote is None or len(args.remote) == 0:
        print("Please specify at least one remote file/directory to download.")
        return
        
    recursive = args.recursive
    wildcard = "**/*" if recursive else "*"
    
    # first get the remote path
    remote2 = args.remote2
    if remote2[0] in ["/", "\\", "~"] or remote2[1:].startswith(":\\"):
        # absolute path
        remote2fs = FileSystemHelper('/', client = None, internal_host = None)
    else:
        # relative path
        remote2fs = FileSystemHelper(os.getcwd(), client = None, internal_host = None)
                
    remote2_path = remote2fs.root.joinpath(remote2)
    print(f"remote2 path: {remote2_path}")
    
    remotes = args.remote
    
    # get the list of remote files to download.
    file_list = set() # relative filename
    # iterate over each remote2 file in the list and copy to remote directory.
    for remote in remotes:
        # check if remote is a file, directory, or pattern
        remote_pattern = f"{remote}{wildcard}" if (remote.endswith("/") or remote.endswith("\\")) else remote
        # glob to get the matching files .
        for paths in centralfs.get_files_iter(pattern=remote_pattern, page_size=page_size):
            file_list.update(paths)
                
        
    if len(file_list) == 1:
        print(f"Only one file to download: {file_list}")
        src = file_list.pop()
        if remote2_path.is_dir():
            # copy file to dir.
            src_path = centralfs.root.joinpath(src)
            srcfs = FileSystemHelper(src_path, client = centralfs.client, internal_host = None)
            srcfs.copy_file_to(relpath=None, dest_path=remote2_path)
        else:  # file, or doesn't exist.   copy file to file - filenames may differ, hence (src, remote2)
            centralfs.copy_file_to(relpath=(src, remote2), dest_path=remote2fs.root)   # if relpath is specified, dest_path is treated as a directory.
            print(f"Copied {centralfs.root}/{src} to {dest_path}/{remote2}")
    else:  # > 0
        print(f"Multiple files to download: {file_list}")
        if remote2_path.is_file():
            print(f"remote2 path {remote2} is a file, but multiple remote files were specified.")
            return
        remote2_path.mkdir(parents=True, exist_ok=True)
        print(f"Downloading files to dir {remote2_path}")
                        
        __copy_parallel(centralfs, remote2fs, file_list, nthreads)


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
                
    local_path = localfs.root.joinpath(local)
    print(f"Local path: {local_path}")
    
    remotes = args.remote
    
    # get the list of remote files to download.
    file_list = set() # relative filename
    # iterate over each local file in the list and copy to remote directory.
    for remote in remotes:
        # check if remote is a file, directory, or pattern
        remote_pattern = f"{remote}{wildcard}" if (remote.endswith("/") or remote.endswith("\\")) else remote
        # glob to get the matching files .
        for paths in centralfs.get_files_iter(pattern=remote_pattern, page_size=page_size):
            file_list.update(paths)
                
        
    if len(file_list) == 1:
        print(f"Only one file to download: {file_list}")
        src = file_list.pop()
        if local_path.is_dir():
            # copy file to dir.
            src_path = centralfs.root.joinpath(src)
            srcfs = FileSystemHelper(src_path, client = centralfs.client, internal_host = None)
            srcfs.copy_file_to(relpath=None, dest_path=local_path)
        else:  # file, or doesn't exist.   copy file to file - filenames may differ, hence (src, local)
            centralfs.copy_file_to(relpath=(src, local), dest_path=localfs.root)   # if relpath is specified, dest_path is treated as a directory.
            print(f"Copied {centralfs.root}/{src} to {localfs.root}/{local}")
    else:  # > 0
        print(f"Multiple files to download: {file_list}")
        if local_path.is_file():
            print(f"Local path {local} is a file, but multiple remote files were specified.")
            return
        local_path.mkdir(parents=True, exist_ok=True)
        print(f"Downloading files to dir {local_path}")
                        
        __copy_parallel(centralfs, localfs, file_list, nthreads)
    
    
def _delete_remote_files(args, config, journal_fn):
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
                print(f"Found file {path} for deletion.")
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
                p = centralfs.root.joinpath(path)
                print(f"Deleting from {centralfs} {p}")
                p.unlink()
                