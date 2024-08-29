
# choices:  cloudpathlib, pyfilesystem (more supported)

# pyfilesystem (pip install fs)
# has many backends:  https://docs.pyfilesystem.org/en/latest/backends/index.html
# https://docs.pyfilesystem.org/en/latest/implementers.html
# including S3, Azure, Google Cloud, etc.
# support:  azureblob store:  fs-azureblob  (no support for retrieving md5)
#          s3 store:  fs-s3fs  (md5 is in etag from info? )
#          local:  fs-osfs (should be built in)  (md5 has to be locally computed)
# and also other types.
# Same interface for all, and has url parser to help choose the correct backend.
# 
# cloudpathlib (pip install cloudpathlib)
# has backends for s3, azure, google cloud.   Extension of pathlib
# package:  cloudpathlib
# support   azureblob, s3, google cloud
#       azureblob:  cloudpathlib.azureblob.   (has support for retrieving md5 (in content-MD5 as base64 encoded))
#       s3:  cloudpathlib.s3.   (has etag ==? md5)
#       local:  pathlib.   (md5 has to be locally computed)
# note that pathlib provides the same interface for posix and windows paths, but it does not seem to abstract the details as much.

# focus on using pyfilesystem for now.
#   known protocols:
#       osfs:// or no protocol string:  local file system
#       ftp://, ftps:// ftp
#       tar:// tar
#       zip://  zip
#       s3://  s3
#       dropbox://
#       webdav://
#       ssh://
#       gs://  google cloud
#       azblob://, azblobv2://  azure blob
#           format "[protocol]://[account_name]:[account_key]@[container]"

# this version uses pathlib and cloudpathlib
# md5 for AWS is in eTag (maybe, if it's not multipart?)
#    for azure is in md5 (maybe, if it's uploaded via portal)
# seems a little easier to use but slightly more complex to set up.
# md5 appears working.

# TODO:  check file existence and metadata before copying.

from cloudpathlib import AnyPath, CloudPath, S3Path, AzureBlobPath, GSPath
from cloudpathlib.client import Client
from pathlib import Path, WindowsPath, PureWindowsPath, PurePosixPath, PosixPath
import hashlib
import math
from typing import Optional, Union
import sys
if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import time
import os

import shutil

# to ensure consistent interface, we use default profile (S3) or require environment variables to be set
# azure:  url:  az://{container}/
#     credential:  {AZURE_STORAGE_CONNECTION_STRING} or {AZURE_ACCOUNT_NAME}+{AZURE_ACCOUNT_KEY} or {AZURE_ACCOUNT_NAME}+{AZURE_SAS_TOKEN}
# aws:    url:  s3://{container}/
#     credential:  default profile, or {AWS_ACCESS_KEY_ID} {AWS_SECRET_ACCESS_KEY} {AWS_DEFAULT_REGION}
# gcp:    url:  gs://{bucket}/
#     credential:  GOOGLE_APPLICATION_CREDENTIALS

# alternatively, a cloud service specific client with the appropriate credential can be constructed
#   outside of this class and passed in, to initialize AnyPath (or cloudpath?)
#   in this case, cloud service specific credentials need to be set on the ciient.

# alternatively, a Path or CloudPath can be passed in.


class FileSystemHelper:
    
    # Azure client ways to sign in:
    # account_url + credential, or account_url with embedded SAS token
    # connection_string, explicit or in environment variable.  With embedded SAS token or credential
    # existing blob_service_client
    
    
    # S3 client ways to sign in - create client using 
    #  access and secret keys (provided, or via env-var)
    #  profile name in credential file
    #  session token
    #  existing session object
    @classmethod
    def _make_path(cls, path: Union[str, CloudPath, Path], client : Optional[Client] = None):
        """
        Create a Path object.

        Args:
            path (Union[str, CloudPath, Path]): The path to be converted.
            client (Optional[Client]): An optional client object for accessing the storage location. Defaults to None.

        Returns:
            a path object depending on the path type.
        """
        
        if isinstance(path, CloudPath) or isinstance(path, PureWindowsPath):
            # cloud path, does not need to do anything more
            # windows path or subclass WindowsPath. don't need to do anything more.
            return path
                
        if isinstance(path, PurePosixPath):
            # check to see if this is really a pure posix path (or subclass PosixPath)
            pstr = str(path)
            # note this is not perfect, example:  a posix file with name that include ":\" or "\"
            if (("\\" in pstr) or (":\\" in pstr)) and ("/" not in pstr):
                # absolute path but has ":" and "\" in the first part, so it's a windows path
                raise ValueError("path is a windows path, but passed in as a posix path " + str(path))
                # if isinstance(path.PosixPath):
                #     return WindowsPath(pstr)
                # else:
                #     return PureWindowsPath(pstr)
            else:
                return path
        
        if isinstance(path, str):
            if (("\\" in path) or (":\\" in path)) and ("/" not in path):
                # windows path
                if os.name == 'nt':
                    return WindowsPath(path)
                else:
                    return PureWindowsPath(path)
            else:
                return AnyPath(path, client = client)  # supports either pathlib.Path or cloudpathlib.CloudPath
    
    # class method to convert a path to a POSIX-compliant string
    @classmethod
    def _as_posix(cls, path: Union[str, Path, CloudPath], client : Optional[Client] = None) -> str:
        """
        Convert the path to a POSIX-compliant string.

        Args:
            path (Union[str, Path, CloudPath]): The path to be converted.

        Returns:
            str: The POSIX-compliant string representation of the path.
        """

        # if string is provided, it may be a url, posix, or windows path.  so convert to a Path or CloudPath object first.
        p = FileSystemHelper._make_path(path, client)
        
        if isinstance(p, PureWindowsPath):
            return p.as_posix()
        else:
            # CloudPath does not provide as_posix() method, but should already be using "/" as separator.
            return str(p)

    
    def __init__(self, path: Union[str, CloudPath, Path], client : Optional[Client] = None):
        """
        Initialize the StoragePath object.

        Args:
            path (Union[str, CloudPath, Path]): The path to the storage location. It can be a string URL, a CloudPath object, or a Path object.
            client (Optional[Client]): An optional client object for accessing the storage location. Defaults to None.

        """
        
        # check if data is in kwargs
        self.client = client
        self.root = FileSystemHelper._make_path(path, client = client)  # supports either pathlib.Path or cloudpathlib.CloudPath
        # If no client, then environment variable, or default profile (s3) is used.            
        self.is_cloud = isinstance(self.root, CloudPath)
        # self.is_dir = self.root.is_dir()  what is the path is mean to be a directory but does not exist yet?  can't tell if it's a directory.
                       
    @classmethod
    def get_metadata(cls, root: Union[CloudPath, Path] = None, 
                     path: Union[CloudPath, Path] = None,
                     with_metadata: bool = True,
                     with_md5: bool = False) -> dict:
        """
        Get the metadata of a file or directory.

        Args:
            path (Union[str, CloudPath, Path]): The path to the file or directory.
            client (Optional[Client]): An optional client object for accessing the storage location. Defaults to None.

        Returns:
            dict: A dictionary containing the metadata of the file or directory.
        """
        if path is None and root is None:
            return {}
        
        if path is None:
            curr = root
            rel = root
        elif root is None:
            curr = path
            rel = path
        else:
            curr = root.joinpath(path)
            rel = path
        
        metadata = {}
        if (with_metadata):
            info = curr.stat()
            metadata["path"] = FileSystemHelper._as_posix(rel)
            if ("\\" in metadata["path"]):
                print("WARNING:  path contains backslashes: ", metadata["path"])
            metadata["size"] = info.st_size

            # in cloudpathlib, only size and mtime are populated, mtime is the time of last upload, in second granularity, so not super useful
            
            metadata["create_time_us"] = int(math.floor(info.st_ctime * 1e6)) if info.st_ctime else None
            metadata["modify_time_us"] = int(math.floor(info.st_mtime * 1e6)) if info.st_mtime else None
            # metadata["create_time_ns"] = math.floor(info.st_ctime_ns / 1000) if info.st_ctime_ns else None
            # metadata["modify_time_ns"] = math.floor(info.st_mtime_ns / 1000) if info.st_mtime_ns else None

        if (with_md5):
            md5_str = None
            
            # if isinstance(curr, S3Path):
            #     # Get the MD5 hash from the ETag. multipart upload has ETAG that is not md5.  if object is encrypted with KMS also not md5.
            #     # https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
            #     # can set content-md5 but we are already storing in db.
            #     md5_str = curr.etag.strip('"')
            # elif isinstance(curr, AzureBlobPath):
            #     # md5 is not calculated for large data files (>100MB)
            #     # https://learn.microsoft.com/en-us/answers/questions/282572/md5-hash-calculation-for-large-files
            #     # can set content-md5 but we are already storing in db.
            #     if (curr.md5 is not None):
            #         md5_str = curr.md5.hex()
            #     else:
            #         md5_str = None  # not available
            # elif isinstance(curr, GSPath):
            #     # likely not reliable, like S3 or AzureBlob
            #     md5_str = curr.etag.strip('"')
                
            # elif isinstance(curr, Path):                
                # Calculate the MD5 hash locally
                
            # calculate for any path.  involves downloading, effectively.    
            # Initialize the MD5 hash object
            if (curr.exists()):
                md5 = hashlib.md5()

                # Open the file in binary mode
                with curr.open("rb") as f:
                    # Read the file in chunks
                    for chunk in iter(lambda: f.read(1024*1024), b""):
                        # Update the MD5 hash with the chunk
                        md5.update(chunk)

                # Get the hexadecimal representation of the MD5 hash
                md5_str = md5.hexdigest()
                
            metadata["md5"] = md5_str
        return metadata
    
                        
    # def get_file_metadata(self, path: Union[str, Path, CloudPath] = None) -> dict:
    #     """
    #     Retrieves the metadata of a file.

    #     Args:
    #         path (Union[str, Path, CloudPath]): The path of the file.

    #     Returns:
    #         dict: A dictionary containing the file metadata.
    #             - "path" (str): The relative path of the file.
    #             - "size" (int): The size of the file in bytes.
    #             - "create_time_us" (int): The creation time of the file in microseconds.
    #             - "modify_time_us" (int): The modification time of the file in microseconds.
    #     """
        
    #     # Initialize the dictionary to store the metadata
    #     metadata = {}
    #     if path is None:
    #         test = self.root
    #     else:
    #         test = FileSystemHelper._make_path(path, client = self.client)   # could be relative or absolute
    #     curr = test if test.is_relative_to(self.root) else self.root.joinpath(test)  # if test is not relative to root, then root is not a prefix and assume test is a subdir.
        
    #     if not curr.exists():
    #         return None
    #     info = curr.stat()
        
    #     metadata["path"] = FileSystemHelper._as_posix(curr.relative_to(self.root), client = self.client)
    #     if ("\\" in metadata["path"]):
    #         print("WARNING:  path contains backslashes: ", metadata["path"])
    #     metadata["size"] = info.st_size

    #     # in cloudpathlib, only size and mtime are populated, mtime is the time of last upload, in second granularity, so not super useful
        
    #     metadata["create_time_us"] = int(math.floor(info.st_ctime * 1e6)) if info.st_ctime else None
    #     metadata["modify_time_us"] = int(math.floor(info.st_mtime * 1e6)) if info.st_mtime else None
    #     # metadata["create_time_ns"] = math.floor(info.st_ctime_ns / 1000) if info.st_ctime_ns else None
    #     # metadata["modify_time_ns"] = math.floor(info.st_mtime_ns / 1000) if info.st_mtime_ns else None


    #     return metadata
    
    # def _calc_file_md5(self, path: Union[Path, CloudPath] = None) -> str:
    #     """
    #     Calculate the MD5 hash of a file.

    #     Args:
    #         path (Union[Path, CloudPath]): The path to the file, as Path or CloudPath, incorporating.

    #     Returns:
    #         str: The hexadecimal representation of the MD5 hash.
    #     """
    #     if path is None:
    #         test = self.root
    #     else:
    #         test = FileSystemHelper._make_path(path, client = self.client)   # could be relative or absolute
    #     curr = test if test.is_relative_to(self.root) else self.root.joinpath(test)  # if test is not relative to root, then root is not a prefix and assume test is a subdir.

    #     if not curr.exists():
    #         print("ERROR:  file does not exist: ", curr)
    #         return None
        
    #     # Initialize the MD5 hash object
    #     md5 = hashlib.md5()

    #     # Open the file in binary mode
    #     with curr.open("rb") as f:
    #         # Read the file in chunks
    #         for chunk in iter(lambda: f.read(1024*1024), b""):
    #             # Update the MD5 hash with the chunk
    #             md5.update(chunk)

    #     # Get the hexadecimal representation of the MD5 hash
    #     return md5.hexdigest()

    
    # # limited testing seems to show that the md5 aws etag and azure content-md5 are okay.
    # def get_file_md5(self, path: Union[str, Path, CloudPath] = None, verify:bool = False  ) -> str:
    #     """
    #     Get the MD5 hash of a file.

    #     Args:
    #         path (Union[str, Path, CloudPath]): The path of the file, could be relative to the root.
    #         verify (bool, optional): Whether to verify the MD5 hash. Defaults to False.

    #     Returns:
    #         str: The MD5 hash of the file.
    #     """
    #     if path is None:
    #         test = self.root
    #     else:
    #         test = FileSystemHelper._make_path(path, client = self.client)   # could be relative or absolute
    #     curr = test if test.is_relative_to(self.root) else self.root.joinpath(test)  # if test is not relative to root, then root is not a prefix and assume test is a subdir.
        
    #     md5_str = None
        
    #     if isinstance(self.root, S3Path):
    #         # Get the MD5 hash from the ETag
    #         md5_str = curr.etag.strip('"')
    #     elif isinstance(self.root, AzureBlobPath):          
    #         md5_str = curr.md5.hex()
    #     elif isinstance(self.root, GSPath):
    #         md5_str = curr.etag.strip('"')

    #     if md5_str is None:
    #         if (self.is_cloud): 
    #             print("INFO: calculating md5 for ", curr)
    #         # Calculate the MD5 hash locally
    #         md5_str = self._calc_file_md5(curr)
            
    #     else:
    #         if (self.is_cloud) and verify: 
    #             md5_calced = self._calc_file_md5(curr)
    #             print("INFO: INTERNAL VERIFICIATION:  md5 for ", curr, ": cloud ", md5_str, ", calc", md5_calced)
            
    #     return md5_str
    
    
    def get_files(self, pattern : str = None ) -> list[str]:
        """
        Returns a list of file paths within the specified subpath.

        Args:
            pattern (str, optional): The subpath within the root directory to search for files. Defaults to None.  can prefix with "*/" or "**/" for more levels of recursion.

        Returns:
            list[str]: A list of file paths relative to the root directory.
        """

        paths = []
        # all pathlib and cloudpathlib paths can use "/"
        
        pattern = pattern if pattern is not None else "**/*"
        
        # not using rglob as it does too much matching.
        for f in self.root.glob(pattern):  # this may be inconsistently implemented in different clouds.
            if not f.is_file():
                continue
            
            paths.append(FileSystemHelper._as_posix(f.relative_to(self.root), client = self.client))
            
        # return relative path
        return paths
    

    # copy 1 file from one location to another.
    def copy_file_to(self, relpath: Union[str, tuple], dest_path: Union[Self, Path, CloudPath], verbose: bool = False):
        """
        Copy a file from the current storage path to the destination path.

        Args:
            paths (Union[str, tuple]): a relative path to the root dir, or None if the root is a file.
            dest_path (Union[Self, Path, CloudPath]): The destination path, either a directory or a path

        Returns:
            None

        Raises:
            None
        """
        # if relpath is None, treat the both src and dest as directories.
        
        # if this is a directory, relpath is required.
        # if this is a file, relpath is ignored.
        dest = FileSystemHelper._make_path(dest_path) if isinstance(dest_path, Path) or isinstance(dest_path, CloudPath) else dest_path.root
        dest_is_cloud = isinstance(dest, CloudPath)
        src_is_cloud = self.is_cloud
                
        if relpath is None:
            src_file = self.root
            dest_file = dest
        else:
            # relpath is specified, so src and dest are directories            
            src_file = self.root.joinpath(relpath[0] if isinstance(relpath, tuple) else relpath)
            dest_file = dest.joinpath(relpath[1] if isinstance(relpath, tuple) else relpath)

        dest_file.parent.mkdir(parents=True, exist_ok=True)

        if src_is_cloud:
            if dest_is_cloud:
                src_file.copy(dest_file, force_overwrite_to_cloud=True)
            else:
                src_file.download_to(dest_file)
        else:
            if dest_is_cloud:
                dest_file.upload_from(src_file, force_overwrite_to_cloud=True)
            else:
                try:
                    shutil.copy2(str(src_file), str(dest_file))
                except shutil.SameFileError:
                    pass
                except PermissionError:
                    print("ERROR: permission issue copying file ", src_file, " to ", dest_file)
                except Exception as e:
                    print("ERROR: issue copying file ", src_file, " to ", dest_file, " exception ", e)
        # start = time.time()
        # meta =  FileSystemHelper.get_metadata(path = dest_file, with_metadata = True, with_md5 = True)
        # md5 = meta['md5']
        # print("INFO:  info time: ", time.time() - start, " size =", meta['size'], " md5 = ", md5)   
        
    


    # copy multiple files from one location to another.
    def copy_files_to(self, relpaths: list, dest_path: Union[Self, Path, CloudPath], verbose: bool = False) -> int:
        """
        Copy files from the current storage path to the destination path.

        Args:
            paths (list): A list of file paths, relative to the src.root, to be copied.
            dest_path (Union[Self, Path, CloudPath]): The destination path where the files will be copied to.

        Returns:
            list of (name, file size, and md5) of the target

        Raises:
            None
        """
        if len(relpaths) == 0:
            return 0
        
        files = relpaths if isinstance(relpaths[0], tuple) else [(f, f) for f in relpaths]
        
        src = self.root
        src_is_cloud = self.is_cloud
        dest = FileSystemHelper._make_path(dest_path) if isinstance(dest_path, Path) or isinstance(dest_path, CloudPath) else dest_path.root
        dest_is_cloud = isinstance(dest, CloudPath)
        count = 0
        if src_is_cloud:
            if dest_is_cloud:
                for sf, df in files:
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
                    # if verbose:
                    #     print("INFO:  copying ", str(src_file), " to ", str(dest_file))
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    src_file.copy(dest_file, force_overwrite_to_cloud=True)
                    count += 1
            else:
                for sf, df in files:
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
                    # if verbose:
                    #     print("INFO:  copying ", str(src_file), " to ", str(dest_file))
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    src_file.download_to(dest_file)
                    count += 1

        else:
            if dest_is_cloud:
                for sf, df in files:
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
                    # if verbose:
                    #     print("INFO:  copying ", str(src_file), " to ", str(dest_file))
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    dest_file.upload_from(src_file, force_overwrite_to_cloud=True)
                    count += 1
            else:
                for sf, df in files:
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
                    # if verbose:
                    #     print("INFO:  copying ", str(src_file), " to ", str(dest_file))
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.copy2(str(src_file), str(dest_file))
                        count += 1
                    except shutil.SameFileError:
                        pass
                    except PermissionError:
                        print("ERROR: permission issue copying file ", src_file, " to ", dest_file)
                    except Exception as e:
                        print("ERROR: issue copying file ", src_file, " to ", dest_file, " exception ", e)
                        
        return count