
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
from pathlib import Path
import hashlib
import math
from typing import Optional, Union
import sys
if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

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
    def __init__(self, path: Union[str, CloudPath, Path], client : Optional[Client] = None):
        """
        Initialize the StoragePath object.

        Args:
            path (Union[str, CloudPath, Path]): The path to the storage location. It can be a string URL, a CloudPath object, or a Path object.
            client (Optional[Client]): An optional client object for accessing the storage location. Defaults to None.

        """
        
        # check if data is in kwargs
            
        if (isinstance(path, str)):
            # input is url string. possibly a client is passed in.  If no client, then environment variable, or default profile (s3) is used.
            self.root = AnyPath(path, client = client)  # supports either pathlib.Path or cloudpathlib.CloudPath
        elif (isinstance(path, Path)) or (isinstance(path, CloudPath)):
            # existing Path object has been passed in.
            self.root = path
            
        self.is_cloud = isinstance(self.root, CloudPath)
        
    def get_file_metadata(self, path: Union[str, Path, CloudPath]) -> dict:
        """
        Retrieves the metadata of a file.

        Args:
            path (Union[str, Path, CloudPath]): The path of the file.

        Returns:
            dict: A dictionary containing the file metadata.
                - "path" (str): The relative path of the file.
                - "size" (int): The size of the file in bytes.
                - "create_time_us" (int): The creation time of the file in microseconds.
                - "modify_time_us" (int): The modification time of the file in microseconds.
        """
        
        # Initialize the dictionary to store the metadata
        metadata = {}
        
        curr = self.root.joinpath(path) if isinstance(path, str) else path
        if not curr.exists():
            return None
        info = curr.stat()
        
        metadata["path"] = str(curr.relative_to(self.root))
        metadata["size"] = info.st_size

        # in cloudpathlib, only size and mtime are populated, mtime is the time of last upload, in second granularity, so not super useful
        
        metadata["create_time_us"] = int(math.floor(info.st_ctime * 1e6)) if info.st_ctime else None
        metadata["modify_time_us"] = int(math.floor(info.st_mtime * 1e6)) if info.st_mtime else None
        # metadata["create_time_ns"] = math.floor(info.st_ctime_ns / 1000) if info.st_ctime_ns else None
        # metadata["modify_time_ns"] = math.floor(info.st_mtime_ns / 1000) if info.st_mtime_ns else None

        # metadata["md5"] = self.get_file_md5(curr)

        return metadata
    
    def calc_file_md5(self, path: Union[str, Path, CloudPath]) -> str:
        """
        Calculate the MD5 hash of a file.

        Args:
            path (Union[str, Path, CloudPath]): The path to the file.

        Returns:
            str: The hexadecimal representation of the MD5 hash.
        """
        curr = self.root.joinpath(path) if isinstance(path, str) else path
        if not curr.exists():
            return None
        
        # Initialize the MD5 hash object
        md5 = hashlib.md5()

        # Open the file in binary mode
        with curr.open("rb") as f:
            # Read the file in chunks
            for chunk in iter(lambda: f.read(1024*1024), b""):
                # Update the MD5 hash with the chunk
                md5.update(chunk)

        # Get the hexadecimal representation of the MD5 hash
        return md5.hexdigest()

    
    # limited testing seems to show that the md5 aws etag and azure content-md5 are okay.
    def get_file_md5(self, path: Union[str, Path, CloudPath], verify:bool = False  ) -> str:
        """
        Get the MD5 hash of a file.

        Args:
            path (Union[str, Path, CloudPath]): The path of the file.
            verify (bool, optional): Whether to verify the MD5 hash. Defaults to False.

        Returns:
            str: The MD5 hash of the file.
        """
        
        curr = self.root.joinpath(path) if isinstance(path, str) else path
        
        md5_str = None
        
        if isinstance(self.root, S3Path):
            # Get the MD5 hash from the ETag
            md5_str = curr.etag.strip('"')
        elif isinstance(self.root, AzureBlobPath):          
            md5_str = curr.md5.hex()
        elif isinstance(self.root, GSPath):
            md5_str = curr.etag.strip('"')

        if md5_str is None:
            if (self.is_cloud): 
                print("INFO: calculating md5 for ", curr)
            # Calculate the MD5 hash locally
            md5_str = self.calc_file_md5(curr)
            
        else:
            if (self.is_cloud) and verify: 
                md5_calced = self.calc_file_md5(curr)
                print("INFO: INTERNAL VERIFICIATION:  md5 for ", curr, ": cloud ", md5_str, ", calc", md5_calced)
            
        return md5_str
    
    
    def get_files(self, subpath : str = None) -> list[str]:
        """
        Returns a list of file paths within the specified subpath.

        Args:
            subpath (str, optional): The subpath within the root directory to search for files. Defaults to None.

        Returns:
            list[str]: A list of file paths relative to the root directory.
        """

        paths = []
        for f in self.root.rglob(subpath + "/**/*"):  # this may be inconsistently implemented in different connectors.
            if not f.is_file():
                continue
            
            paths.append(str(f.relative_to(self.root)))
            
        # return relative path
        return paths
    

    # copy multiple files from one location to another.
    def copy_files_to(self, paths: list, dest_path: Union[Self, Path, CloudPath], verbose: bool = False):
        """
        Copy files from the current storage path to the destination path.

        Args:
            paths (list): A list of file paths to be copied.
            dest_path (Union[Self, Path, CloudPath]): The destination path where the files will be copied to.

        Returns:
            None

        Raises:
            None
        """
        if len(paths) == 0:
            return
        
        files = paths if isinstance(paths[0], tuple) else [(f, f) for f in paths]
        
        src = self.root
        src_is_cloud = self.is_cloud
        dest = dest_path if isinstance(dest_path, Path) or isinstance(dest_path, CloudPath) else dest_path.root
        dest_is_cloud = isinstance(dest, CloudPath)
        count = 0
        if src_is_cloud:
            if dest_is_cloud:
                for sf, df in files:
                    if verbose:
                        print("INFO:  copying ", src, "/", sf, " to ", dest, "/", df)
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    src_file.copy(dest_file, force_overwrite_to_cloud=True)
                    count += 1
            else:
                for sf, df in files:
                    if verbose:
                        print("INFO:  copying ", src, "/", sf, " to ", dest, "/", df)
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    src_file.download_to(dest_file)
                    count += 1

        else:
            if dest_is_cloud:
                for sf, df in files:
                    if verbose:
                        print("INFO:  copying ", src, "/", sf, " to ", dest, "/", df)
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    dest_file.upload_from(src_file, force_overwrite_to_cloud=True)
                    count += 1
            else:
                for sf, df in files:
                    if verbose:
                        print("INFO:  copying ", src, "/", sf, " to ", dest, "/", df)
                    src_file = src.joinpath(sf)
                    dest_file = dest.joinpath(df)
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