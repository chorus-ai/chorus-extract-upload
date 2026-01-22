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
    pattern = f"{pattern}{wildcard}" if (pattern.endswith("/") or pattern.endswith("\\")) else pattern
        
    print(f"Files matching pattern: {pattern}")
    for paths in centralfs.get_files_iter(pattern, page_size=page_size, include_dirs=True):
        for path in paths:
            print(f"{path}")
    
    
# copy list of files from src_fs to dest_path, which should be directory.
def __copy_parallel(src_fs, src_list, dest_path, nthreads):
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        lock = threading.Lock()
        futures = {}
        for src in src_list:
            # copy, from src_fs to dest_path, preserving relpath src.
            future = executor.submit(src_fs.copy_file_to, relpath=src, dest_path=dest_path)
            futures[future] = src
        
        for future in concurrent.futures.as_completed(futures.keys()):
            src = futures[future]
            dest = dest_path.joinpath(src)
            try:
                future.result()
                print(f"Copied {src_fs.root.joinpath(src)} to {dest}")
            except Exception as e:
                print(f"Error copying file: {e}")

def __copy_files(srcfs, src_patterns, destfs, dest, nthreads, page_size, recursive):
    wildcard = "**/*" if recursive else "*"
    print(f"Files to be copied matching pattern(s): {src_patterns}")
    
    dest_path = destfs.root.joinpath(dest)  # target directory or file
    print(f"Destination path: {dest_path}")
    
    # get the list of remote files to download.    
    file_list = set()
    for pattern in src_patterns:
        pat = f"{pattern}{wildcard}" if (pattern.endswith("/") or pattern.endswith("\\")) else pattern
        for paths in srcfs.get_files_iter(pat, page_size=page_size):
            file_list.update(paths)
                
    if len(file_list) == 1:
        print(f"Only one file to copy: {file_list}")
        src = file_list.pop()
        if dest_path.is_dir():
            # copy file to dir, so need to specify file name as relpath, and dest_path should be the directory.
            srcfs.copy_file_to(relpath=src, dest_path=dest_path)
        else:  # file, or doesn't exist.   copy file to file - filenames may differ, hence (src, local)
            srcfs.copy_file_to(relpath=(src, dest), dest_path=destfs.root)   # if relpath is specified, dest_path is treated as a directory.
            print(f"Copied {srcfs.root}/{src} to {dest_path}")
    else:  # > 0
        print(f"Multiple files to copy: {file_list}")
        if dest_path.is_file():
            print(f"Local path {dest} is a file, but multiple remote files were specified.")
            return
        dest_path.mkdir(parents=True, exist_ok=True)
        print(f"Copying files to dir {dest_path}")
                        
        __copy_parallel(srcfs, file_list, dest_path, nthreads)


    
def _upload_remote_files(args, config, journal_fn):
    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    if args.remote is None or len(args.remote) == 0:
        print("Please specify the remote file/directory to upload to.")
        return
    
    if args.local is None or len(args.local) == 0:
        print("Please specify at least one local file/directory to upload from.")
        return
        
    recursive = args.recursive
    
    # first get the remote path (destination)
    remote = args.remote
    remote_path = centralfs.root.joinpath(remote)
    print(f"dest path: {remote_path}")
    
    _locals = args.local
    # convert all to relative to root.
    print(f"local path: {_locals}")
    
    # has to be relative to current path

    localfs = FileSystemHelper(os.getcwd(), client=None, internal_host=None)
    
    if any([l.startswith("~") or "./" in l or ".\\" in l or ":/" in l or ":\\" in l for l in _locals]):
        print("Error: local paths must be relative to the current working directory and cannot have ./ .")
        return
    
    locals = [l[1:] if (l.startswith("/") or l.startswith("\\")) else l for l in _locals]
    __copy_files(localfs, locals, centralfs, remote, nthreads, page_size, recursive)



def _download_remote_files(args, config, journal_fn):

    central_config = config_helper.get_central_config(config)
    client, internal_host =storage_helper._make_client(central_config)
    centralfs = FileSystemHelper(config_helper.get_path_str(central_config), client = client, internal_host = internal_host)
    
    nthreads = config_helper.get_config(config).get('nthreads', 1)
    page_size = config_helper.get_config(config).get('page_size', 1000)
    
    if args.local is None or len(args.local) == 0:
        print("Please specify the local file/directory to download to.")
        return

    if args.remote is None or len(args.remote) == 0:
        print("Please specify at least one remote file/directory to download from.")
        return
        
    recursive = args.recursive
    
    # first get the local path (dest)
    local = args.local
    if local[0] in ["/", "\\"]:
        # absolute path
        localfs = FileSystemHelper('/', client = None, internal_host = None)
        local = local[1:]
    elif local[0] == "~":
        # home dir
        home_dir = os.path.expanduser("~")
        localfs = FileSystemHelper(home_dir, client = None, internal_host = None)
        local = local[2:] if local[1] in ["/", "\\"] else local[1:]
    elif local[1:3] == ":\\" or local[1:3] == ":/":
        # windows absolute path
        localfs = FileSystemHelper(local[0:3], client = None, internal_host = None)
        local = local[3:]
    else:
        # relative path
        localfs = FileSystemHelper(os.getcwd(), client = None, internal_host = None)
                
    remotes = args.remote
    
    # only handles relative remote paths - cannot be a az:// or c:\ path
    if any([r.startswith("~") or "./" in r or ".\\" in r or ":/" in r or ":\\" in r for r in remotes]):
        print("Error: remote paths cannot start with ~")
        return
    # strip leading / or \ if any.
    remotes = [r[1:] if (r.startswith("/") or r.startswith("\\")) else r for r in remotes]
    __copy_files(centralfs, remotes, localfs, local, nthreads, page_size, recursive)
    
    
    
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
    count = 0
    for pattern in patterns:
        pat = f"{pattern}{wildcard}" if (pattern.endswith("/") or pattern.endswith("\\")) else pattern
        for paths in centralfs.get_files_iter(pat, page_size=page_size):
            for path in paths:
                if path.name.endswith("journal.db") or path.name.endswith("journal.db.locked"):
                    print(f"Skipping journal file {path}.")
                    continue
                print(f"{path}")
                count += 1

    if count == 0:
        print("No files found to delete.")
        return  
    
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
                