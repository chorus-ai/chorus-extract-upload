
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
# TODO:  admin method to delete files remotely.
# TODO:  az cli method to upload, rename, and download files.

from cloudpathlib import AnyPath, CloudPath, S3Path, AzureBlobPath, GSPath
from cloudpathlib.client import Client
from cloudpathlib.enums import FileCacheMode
from pathlib import Path, WindowsPath, PureWindowsPath, PurePosixPath
# from pathlib import PosixPath
import hashlib
import math
from typing import Optional, Union
import sys
if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import os

import shutil
from chorus_upload import config_helper
import base64

import asyncio
import aiofiles
from azure.core.exceptions import ResourceNotFoundError

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from cloudpathlib import S3Client
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from cloudpathlib import AzureBlobClient
import requests
from requests.adapters import HTTPAdapter
from azure.core.pipeline.transport import RequestsTransport, AioHttpTransport
# from cloudpathlib import GoogleCloudClient
from urllib.parse import urlparse, urlunparse
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient



import logging
log = logging.getLogger(__name__)


AZURE_MAX_BLOCK_SIZE = 4 * 1024 * 1024

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


# # superclass patched cloudpathlib AzureBlobClient to avoid issue with 
# class PatchedAzureBlobClient(AzureBlobClient):
#     def __init__(self, blob_service_client, **kwargs):
#         super().__init__(blob_service_client=blob_service_client, **kwargs)
#         # bypass parent constructor entirely
#         self._client = blob_service_client

#     # override any method that tries to rebuild the client
#     def _get_service_client(self):
#         return self._client

#     def _get_container_client(self, container_name):
#         return self._client.get_container_client(container_name)


# internal helper to create the cloud client
def __make_aws_client(auth_params: dict) -> S3Client:
    # Azure format for account_url
    aws_session_token = config_helper.get_auth_param(auth_params, "aws_session_token")
    aws_profile = config_helper.get_auth_param(auth_params, "aws_profile")
    aws_access_key_id = config_helper.get_auth_param(auth_params, "aws_access_key_id")    
    aws_secret_access_key = config_helper.get_auth_param(auth_params, "aws_secret_access_key")
    
    if aws_session_token:  # preferred.
        # session token specified, then use it
        return S3Client(aws_session_token = aws_session_token, file_cache_mode = FileCacheMode.cloudpath_object)
    elif aws_access_key_id and aws_secret_access_key:
        # access key and secret key specified, then use it
        return S3Client(access_key_id = aws_access_key_id, 
                        secret_access_key = aws_secret_access_key, file_cache_mode = FileCacheMode.cloudpath_object)
    elif aws_profile:
        # profile specified, then use it
        return S3Client(profile_name = aws_profile, file_cache_mode = FileCacheMode.cloudpath_object)
    
    aws_session_token = aws_session_token if aws_session_token else os.environ.get("AWS_SESSION_TOKEN")
    aws_profile = aws_profile if aws_profile else "default"
    aws_access_key_id = aws_access_key_id if aws_access_key_id else os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = aws_secret_access_key if aws_secret_access_key else os.environ.get("AWS_SECRET_ACCESS_KEY")
    
    if aws_session_token:  # preferred.
        # session token specified, then use it
        return S3Client(aws_session_token = aws_session_token, file_cache_mode = FileCacheMode.cloudpath_object)
    elif aws_access_key_id and aws_secret_access_key:
        # access key and secret key specified, then use it
        return S3Client(access_key_id = aws_access_key_id, 
                        secret_access_key = aws_secret_access_key, file_cache_mode = FileCacheMode.cloudpath_object)
    elif aws_profile:
        # profile specified, then use it
        return S3Client(profile_name = aws_profile, file_cache_mode = FileCacheMode.cloudpath_object)
        
    # default profile
    return S3Client(file_cache_mode = FileCacheMode.cloudpath_object)

# internal helper to create the cloud client
def __make_az_client(auth_params: dict) -> AzureBlobClient:
    # Azure format for account_url

    # parse out all the relevant argument
    azure_auth_mode = config_helper.get_auth_param(auth_params, "auth_mode", "login")

    azure_account_url = config_helper.get_auth_param(auth_params, "azure_account_url")
    azure_account_name = config_helper.get_auth_param(auth_params, "azure_account_name", default=None)

    if azure_account_url is None :
        if azure_account_name is None:
            raise ValueError("Azure account name must be specified if account url is not specified")
        else:
            # default account url for account not specified.
            azure_account_url = f"https://{azure_account_name}.blob.core.windows.net"

    if azure_auth_mode == "login":
        # force the commandline login
        # Use DefaultAzureCredential to pick up your Entra login session
        credential = DefaultAzureCredential()
    elif azure_auth_mode == "sas":
        azure_sas_token = config_helper.get_auth_param(auth_params, "azure_sas_token")
        azure_account_url = f"{azure_account_url}/?{azure_sas_token}" if azure_sas_token is not None else azure_account_url
        credential = None
    else:
        raise ValueError("Unknown Azure auth_mode.  use 'login' or 'sas' " + str(azure_auth_mode))


    # format for account url for client.  container is not discarded unlike stated.  so must use format like below.
    # example: 'https://{account_url}.blob.core.windows.net/?{sas_token}
    # https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    # format for path:  az://{container}/...
    # note if container is specified in account_url, we will get duplicates.

    azure_storage_connection_string = config_helper.get_auth_param(auth_params, "azure_storage_connection_string", default=None)
    azure_account_key = config_helper.get_auth_param(auth_params, "azure_account_key", default=None)
    if azure_storage_connection_string is None and azure_account_name and azure_account_key:
        azure_storage_connection_string = f"DefaultEndpointsProtocol=https;AccountName={azure_account_name};AccountKey={azure_account_key};EndpointSuffix=core.windows.net"
    else:
        azure_storage_connection_string = None

    azure_storage_connection_string_env = os.environ.get("AZURE_STORAGE_CONNECTION_STRING") if "AZURE_STORAGE_CONNECTION_STRING" in os.environ.keys() else None

    # to avoid connection timeout: https://stackoverflow.com/questions/65092741/solve-timeout-errors-on-file-uploads-with-new-azure-storage-blob-package

    # ms copilot code is not accurate. do not use _connection_verify, or set _connection_ssl_context (or create a ssl context.)
    # it's also not needed to directly create a ConnectionConfiguration object.
    # the kwargs contain keywords that are extracted as needed by ConnectionConfiguration and other objects.
    # so just put their keywords as name value pairs.  specifically, just set on BlobServiceClient the following:
    # connection_verify = False, connection_cert = None

    # Shared BlobServiceClient kwargs for every construction path below.
    _bsc_kwargs = dict(
        connection_verify=False,
        connection_cert=None,
        max_single_get_size=AZURE_MAX_BLOCK_SIZE,
        max_single_put_size=AZURE_MAX_BLOCK_SIZE,
    )

    def _wrap(factory_fn: callable) -> AzureBlobClient:
        """Build an AzureBlobClient and attach a clone factory.

        _clone_client uses _clone_factory to create a fresh BlobServiceClient
        (with its own connection pool) for each upload thread, without needing
        to introspect cloudpathlib or Azure SDK private attributes.
        The credential object is shared across clones intentionally — one
        token-refresh loop is correct; only the HTTP connection pool is isolated.
        """
        az_client = AzureBlobClient(
            blob_service_client=factory_fn(),
            file_cache_mode=FileCacheMode.cloudpath_object)
        az_client._clone_factory = factory_fn
        return az_client

    def _make_transport(pool_size):
        """Return a RequestsTransport with a custom pool size, or None to use the default."""
        if pool_size is None:
            return None
        session = requests.Session()
        session.mount('https://', HTTPAdapter(pool_maxsize=pool_size))
        return RequestsTransport(session=session)

    if azure_account_url:
        if azure_auth_mode == "login":
            def _factory(_url=azure_account_url, _cred=credential, pool_size=None):
                extra = {'transport': _make_transport(pool_size)} if pool_size is not None else {}
                return BlobServiceClient(account_url=_url, credential=_cred, **_bsc_kwargs, **extra)
            return _wrap(_factory)
        elif azure_auth_mode == "sas":
            def _factory(_url=azure_account_url, pool_size=None):
                extra = {'transport': _make_transport(pool_size)} if pool_size is not None else {}
                return BlobServiceClient(account_url=_url, **_bsc_kwargs, **extra)
            return _wrap(_factory)
    elif azure_storage_connection_string_env:
        def _factory(_conn=azure_storage_connection_string_env, pool_size=None):
            extra = {'transport': _make_transport(pool_size)} if pool_size is not None else {}
            return BlobServiceClient.from_connection_string(conn_str=_conn, **_bsc_kwargs, **extra)
        return _wrap(_factory)
    elif azure_storage_connection_string:
        def _factory(_conn=azure_storage_connection_string, pool_size=None):
            extra = {'transport': _make_transport(pool_size)} if pool_size is not None else {}
            return BlobServiceClient.from_connection_string(conn_str=_conn, **_bsc_kwargs, **extra)
        return _wrap(_factory)
    else:
        raise ValueError("No viable Azure account info available to open connection")
        
def __get_azure_internal_host(cloud_config:dict) -> Optional[str]:
    azure_account_name = config_helper.get_auth_param(cloud_config.get("auth", {}), "azure_account_name", default=None)
    if azure_account_name:
        return f"{azure_account_name}.blob.core.windows.net"
    else:
        raise ValueError("Azure account name must be specified in the configuration toml file in each azure auth section.")

# internal helper to create the cloud client
# def __make_gs_client(auth_params: dict):
#     # if google_application_credentials:
#     #     # application credentials specified, then use it
#     #     return GoogleCloudClient(application_credentials = google_application_credentials)
#     # elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
#     if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
#         # application credentials specified in environment variables, then use it
#         return GoogleCloudClient(application_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
#     else:
#         raise ValueError("No viable Google application credentials to open connection")

# internal helper to create the cloud client
def _make_client(cloud_config:dict):
    
    path_str = cloud_config.get("path", None)
    cloud_params = cloud_config.get("auth", {})
    
    if (path_str.startswith("s3://")):
        return __make_aws_client(cloud_params), None
    elif (path_str.startswith("az://")):
        return __make_az_client(cloud_params), __get_azure_internal_host(cloud_config)
    # elif (path_str.startswith("gs://")):
    #     return __make_gs_client(cloud_params), None
    elif ("://" in path_str):
        raise ValueError("Unknown cloud storage provider.  Supports s3, az, gs")
    else:
        return None, None
    
    
def _clone_client(client, pool_size: int = None):
    if isinstance(client, S3Client):
        return __clone_aws_client(client)
    elif isinstance(client, AzureBlobClient):
        return __clone_az_client(client, pool_size=pool_size)
    # elif isinstance(client, GoogleCloudClient):
    #     return __clone_gs_client(client)
    else:
        raise ValueError("Unknown client type.  Supports S3Client, AzureBlobClient, GoogleCloudClient")

def __clone_aws_client(orig: S3Client) -> S3Client:
    # Extract the constructor parameters CloudPathLib stores
    params = {
        "aws_session_token": orig.aws_session_token,
        "profile_name": orig.profile_name,
        "access_key_id": orig.access_key_id,
        "secret_access_key": orig.secret_access_key,
        "file_cache_mode": orig.file_cache_mode,
    }

    # Remove None values so we pass only the active auth method
    params = {k: v for k, v in params.items() if v is not None}

    # Recreate the client
    return S3Client(**params)

def __clone_az_client(orig: AzureBlobClient, pool_size: int = None) -> AzureBlobClient:
    if hasattr(orig, '_clone_factory'):
        # Create a fresh BlobServiceClient (own connection pool) using the
        # same factory that built the original.  The credential object is
        # intentionally shared — one token-refresh loop is correct.
        # pool_size sets the requests HTTPAdapter pool_maxsize for this clone.
        az_client = AzureBlobClient(
            blob_service_client=orig._clone_factory(pool_size=pool_size),
            file_cache_mode=FileCacheMode.cloudpath_object)
        az_client._clone_factory = orig._clone_factory
        return az_client
    # Fallback for clients not built through _make_client (e.g. in tests):
    # share the existing BlobServiceClient — it is thread-safe.
    return AzureBlobClient(
        blob_service_client=orig.service_client,
        file_cache_mode=FileCacheMode.cloudpath_object)

# def __clone_gs_client(orig: GoogleCloudClient) -> GoogleCloudClient:
#     # Extract the constructor parameters CloudPathLib stores
#     params = {
#         "application_credentials": orig.application_credentials,
#         "file_cache_mode": orig.file_cache_mode,
#     }

#     # Remove None values so we pass only the active auth method
#     params = {k: v for k, v in params.items() if v is not None}

#     # Recreate the client
#     return GoogleCloudClient(**params) 


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
                # TODO: incorporate client url into the path.
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

    
    def __init__(self, path: Union[str, CloudPath, Path], 
                 client : Optional[Client] = None,
                 **kwargs):
        """
        Initialize the StoragePath object.

        Args:
            path (Union[str, CloudPath, Path]): The path to the storage location. It can be a string URL, a CloudPath object, or a Path object.
            client (Optional[Client]): An optional client object for accessing the storage location. Defaults to None.

        """
        
        # check if data is in kwargs
        self.client = client
        # async_azclient must be created inside an active event loop (e.g. via
        # `async with AsyncBlobServiceClient(...)`) and passed in explicitly.
        # Never create it here: __init__ runs outside any event loop and the
        # resulting client would be bound to a closed loop on the next asyncio.run().
        self.async_azclient = kwargs.get('async_azclient')
        self.root = FileSystemHelper._make_path(path, client = client)  # supports either pathlib.Path or cloudpathlib.CloudPath
        if isinstance(self.root, CloudPath):
            self.container = self.root.container
        else:
            self.container = None
        # If no client, then environment variable, or default profile (s3) is used.            
        self.is_cloud = isinstance(self.root, CloudPath)
        # self.is_dir = self.root.is_dir()  what is the path is mean to be a directory but does not exist yet?  can't tell if it's a directory.
        self.internal_host = kwargs.get("internal_host", None)
                
    def __str__(self):
        return f"<client {self.client}, path {self.root}>"

    def __repr__(self):
        return f"<FileSystemHelper client={self.client} root={self.root!r} is_cloud={self.is_cloud}>"
                       
    @classmethod
    def _calc_file_md5(cls, curr: Union[CloudPath, Path] ):
        """
        Calculate the MD5 hash of a file.

        Args:
            path (Union[Path, CloudPath]): The path to the file, as Path or CloudPath, incorporating.

        Returns:
            str: The hexadecimal representation of the MD5 hash.
        """
        md5 = None
        
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
            # md5_str = md5.hexdigest()
            
        return md5

    async def async_get_metadata(self, path: Union[str, CloudPath, Path] = None,
                                 with_metadata: bool = True,
                                 with_md5: bool = False,
                                 local_md5: str = None,
                                 **kwargs) -> dict:
        """Async version of get_metadata. Supports local paths and Azure Blob Storage.
        Returns None if the file/blob does not exist."""
        lock = kwargs.get("lock", None)

        curr = self.root.joinpath(path) if path is not None else self.root
        rel  = path if path is not None else self.root

        metadata = {'md5': None, 'size': None, 'create_time_us': None, 'modify_time_us': None, 'path': None}

        if isinstance(curr, AzureBlobPath):
            blob_client = self.async_azclient.get_blob_client(
                container=curr.container, blob=curr.blob)
            try:
                props = await blob_client.get_blob_properties()
            except ResourceNotFoundError:
                return None

            if with_metadata:
                metadata["path"] = FileSystemHelper._as_posix(rel)
                if "\\" in metadata["path"]:
                    log.warning(f"path contains backslashes: {metadata['path']}")
                metadata["size"] = props.size
                metadata["modify_time_us"] = int(math.floor(props.last_modified.timestamp() * 1e6)) if props.last_modified else None
                metadata["create_time_us"] = int(math.floor(props.creation_time.timestamp() * 1e6)) if props.creation_time else None

            if with_md5:
                set_md5 = False
                cloud_md5_bytes = props.content_settings.content_md5  # bytes or None

                if cloud_md5_bytes is None:
                    if local_md5 is None:
                        # blob has no md5 and none supplied — leave as None
                        md5_str = None
                    else:
                        md5_str = local_md5
                        if lock is not None:
                            with lock:
                                log.info(f"Missing md5 for {curr}. setting with supplied MD5. hex= {md5_str}")
                        else:
                            log.info(f"Missing md5 for {curr}. setting with supplied MD5. hex= {md5_str}")
                        set_md5 = True
                else:
                    md5_str = cloud_md5_bytes.hex()
                    if local_md5 is not None and md5_str != local_md5:
                        if base64.b64decode(cloud_md5_bytes).hex() == local_md5:
                            if lock is not None:
                                with lock:
                                    log.warning(f"cloud md5 is doubly b64encoded. fixing {curr} with md5 {md5_str}, local {local_md5}. b64decoded cloud {base64.b64decode(cloud_md5_bytes).hex()}")
                            else:
                                log.warning(f"cloud md5 is doubly b64encoded. fixing {curr} with md5 {md5_str}, local {local_md5}. b64decoded cloud {base64.b64decode(cloud_md5_bytes).hex()}")
                            md5_str = local_md5
                            set_md5 = True
                        else:
                            if lock is not None:
                                with lock:
                                    log.info(f"cloud and supplied local md5 are different for {curr} cloud {md5_str}, local {local_md5}")
                            else:
                                log.info(f"cloud and supplied local md5 are different for {curr} cloud {md5_str}, local {local_md5}")

                if set_md5:
                    content_settings = props.content_settings
                    content_settings.content_md5 = bytes.fromhex(md5_str)
                    await blob_client.set_http_headers(content_settings=content_settings)

                metadata["md5"] = md5_str

        elif isinstance(curr, Path):
            if not await aiofiles.os.path.exists(curr):
                return None

            if with_metadata:
                stat_result = await aiofiles.os.stat(curr)
                metadata["path"] = FileSystemHelper._as_posix(rel)
                if "\\" in metadata["path"]:
                    log.warning(f"path contains backslashes: {metadata['path']}")
                metadata["size"] = stat_result.st_size
                metadata["modify_time_us"] = int(math.floor(stat_result.st_mtime * 1e6)) if stat_result.st_mtime else None
                metadata["create_time_us"] = int(math.floor(stat_result.st_ctime * 1e6)) if stat_result.st_ctime else None

            if with_md5:
                md5 = hashlib.md5()
                async with aiofiles.open(curr, "rb") as f:
                    while chunk := await f.read(1024 * 1024):
                        md5.update(chunk)
                metadata["md5"] = md5.hexdigest()

        else:
            raise NotImplementedError(f"async_get_metadata does not support {type(curr)}")

        return metadata

    @classmethod
    def get_metadata(cls, root: Union[CloudPath, Path] = None, 
                     path: Union[CloudPath, Path] = None,
                     with_metadata: bool = True,
                     with_md5: bool = False,
                     local_md5: str = None,
                     **kwargs) -> dict:
        """
        Get the metadata of a file or directory.

        Args:
            root (Union[CloudPath, Path]): The root directory of the file or directory.
            path (Union[CloudPath, Path]): The path to the file or directory.
            client (Optional[Client]): An optional client object for accessing the storage location. Defaults to None.
            local_md5 (str) : if a local md5 string is supplied, will use if if the r

        Returns:
            dict: A dictionary containing the metadata of the file or directory.
        """
        lock = kwargs.get("lock", None)
        
        metadata = {'md5': None, 'size': None, 'create_time_us': None, 'modify_time_us': None, 'path': None}
        if path is None and root is None:
            return metadata
        
        if path is None:
            curr = root
            rel = root
        elif root is None:
            curr = path
            rel = path
        else:
            curr = root.joinpath(path)
            rel = path
        
        if not curr.exists():
            return metadata
        
        if (with_metadata):
            info = curr.stat()
            metadata["path"] = FileSystemHelper._as_posix(rel)
            if ("\\" in metadata["path"]):
                log.warning(f"path contains backslashes: {metadata['path']}")
            metadata["size"] = info.st_size

            # in cloudpathlib, only size and mtime are populated, mtime is the time of last upload, in second granularity, so not super useful
            
            metadata["create_time_us"] = int(math.floor(info.st_ctime * 1e6)) if info.st_ctime else None
            metadata["modify_time_us"] = int(math.floor(info.st_mtime * 1e6)) if info.st_mtime else None
            # metadata["create_time_ns"] = math.floor(info.st_ctime_ns / 1000) if info.st_ctime_ns else None
            # metadata["modify_time_ns"] = math.floor(info.st_mtime_ns / 1000) if info.st_mtime_ns else None

        if (with_md5):
            set_md5 = False  
            if isinstance(curr, S3Path):
                # Get the MD5 hash from the ETag. multipart upload has ETAG that is not md5.  if object is encrypted with KMS also not md5.
                # https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
                # can set content-md5 but we are already storing in db.
                md5_str = curr.etag.strip('"')
            elif isinstance(curr, AzureBlobPath):
                # md5 is not calculated for large data files (>10MB)
                # https://learn.microsoft.com/en-us/answers/questions/282572/md5-hash-calculation-for-large-files
                # can set content-md5 but we are already storing in db.
                
                # cases:  no md5, if has local_md5, use it.  if not, compute it.
                #     has md5, but no local_md5 supplied.  keep md5.
                #     has md5, and local_md5 supplied, but they are the same.  keep md5
                #     has md5, and local_md5 supplied, but they are different.  if b64decode(md5) == local_md5, set md5 to local_md5.  else keep md5 and ignore local_md5.
                if (curr.md5 is None):                        
                    if local_md5 is None:
                        md5 = FileSystemHelper._calc_file_md5(curr)
                        md5_str = md5.hexdigest() if md5 else None
                        if lock is not None:
                            with lock:
                                log.debug(f"Missing md5 for {curr}. setting with computed MD5. hex= {md5_str}")
                        else:
                            log.debug(f"Missing md5 for {curr}. setting with computed MD5. hex= {md5_str}")

                    else:
                        md5_str = local_md5
                        if lock is not None:
                            with lock:
                                log.debug(f"Missing md5 for {curr}. setting with supplied MD5. hex= {md5_str}")
                        else:
                            log.debug(f"Missing md5 for {curr}. setting with supplied MD5. hex= {md5_str}")
                        
                    set_md5 = True
                        
                else:
                    md5_str = curr.md5.hex()
                    # has cloud md5.
                    if local_md5 is not None:
                        if (md5_str != local_md5):
                            # cloud and local md5s are different
                            
                            if (base64.b64decode(curr.md5).hex() == local_md5):
                                if lock is not None:
                                    with lock:
                                        log.warning(f"cloud md5 is doubly b64encoded. fixing {curr} with md5 {md5_str}, local {local_md5}. b64decoded cloud {base64.b64decode(curr.md5).hex()}")
                                else:
                                    log.warning(f"cloud md5 is doubly b64encoded. fixing {curr} with md5 {md5_str}, local {local_md5}. b64decoded cloud {base64.b64decode(curr.md5).hex()}")
                                md5_str = local_md5
                                set_md5 = True
                            else:
                                if lock is not None:
                                    with lock:
                                        log.warning(f"cloud and supplied local md5 are different for {curr} cloud {md5_str}, local {local_md5}")
                                else:
                                    log.warning(f"cloud and supplied local md5 are different for {curr} cloud {md5_str}, local {local_md5}")
                        # else md5 matches.  no action needed.
                    
                    # else no local_md5, no action needed.
                
                if set_md5:
                    blob_serv_client = curr.client.service_client
                    blob_client = blob_serv_client.get_blob_client(container = curr.container, blob = curr.blob)
                    content_settings = blob_client.get_blob_properties().content_settings
                    content_settings.content_md5 = bytes.fromhex(md5_str)
                    blob_client.set_http_headers(content_settings = content_settings)

            elif isinstance(curr, GSPath):
                # likely not reliable, like S3 or AzureBlob
                md5_str = curr.etag.strip('"')
            else:
                md5 = FileSystemHelper._calc_file_md5(curr)
                md5_str = md5.hexdigest() if md5 else None                

            metadata["md5"] = md5_str
        return metadata
    
    def convert_pattern(self, pattern: str, recursive: bool) -> str:
        """
        Convert a pattern from the "parse" package format to glob format.

        Args:
            pattern (str): The pattern to be converted.

        Returns:
            str: The converted pattern in glob format.
        """
        wildcard = "**/*" if recursive else "*"
        glob_pattern = pattern if pattern is not None else wildcard
        glob_pattern = glob_pattern.replace("{filepath}", wildcard)
        glob_pattern = glob_pattern.replace("{patient_id:d}", "*").replace("{patient_id:w}", "*").replace("{patient_id}", "*")
        glob_pattern = glob_pattern.replace("{version:d}", "*").replace("{version:w}", "*").replace("{version}", "*")
        return glob_pattern
    
    
    # not used.
    def get_files(self, pattern : str = None , include_dirs: bool = False, recursive:bool = True) -> list[str]:
        """
        Returns a list of file paths within the specified subpath.

        Args:
            pattern (str, optional): The subpath within the root directory to search for files. Defaults to None.  should be of the form "{patient_id:w}/Images/{filepath}", specified in config.toml file.

        Returns:
            list[str]: A list of file paths relative to the root directory.
        """

        paths = []
        # all pathlib and cloudpathlib paths can use "/"
        
        pattern = self.convert_pattern(pattern, recursive)
        # convert pattern to be case insensitive
        log.debug(f"getting list of files for {pattern}")
        # not using rglob as it does too much matching.
        # case_sensitive is a pathlib 3.12+ feature.
        count = 0
        for f in self.root.glob(pattern, case_sensitive = False ):  # this may be inconsistently implemented in different clouds.
            if f.is_file():
                paths.append(FileSystemHelper._as_posix(f.relative_to(self.root), client = self.client))
            elif f.is_dir() and include_dirs:
                paths.append(FileSystemHelper._as_posix(f.relative_to(self.root), client = self.client) + "/")  # add trailing slash to indicate directory
            else:
                continue
            
            count += 1
            if (count % 1000) == 0:
                log.info(f"Found {count} files in {pattern}")
                
        if (count > 0):
            log.info(f"Found {count} files in {pattern}")
            
        log.info("completed retrieving files.")
        # return relative path
        return paths
    

    def get_files_iter(self, pattern: str = None, include_dirs : bool = False, recursive:bool = True, page_size: int = 1000):
        """
        Yields file paths within the specified subpath.

        Args:
            pattern (str, optional): The subpath within the root directory to search for files. Defaults to None. Can prefix with "*/" or "**/" for more levels of recursion.

        Yields:
            str: A file path relative to the root directory.
        """

        # cloudpathlib infers directories, but present them as "files".  it is a prefix match up to a / delimiter or end of filename
        # this means for foo/bar/test.txt,
        #   foo/bar, matches, return as file
        #   foo/bar/ matches, return as file
        #   foo/ba does not match
        #   foo/bar/test.txt matches, return as file
        #   foo/bar/test.txt/ matches, return as file
        # to properly check, we probably need to check EVERY file returned for existence (strip /, exist->file, not exist-> directory)
        # or alternatively, post process the array.  problem is we are retrieving by batches.
        # another alternative is to process by tracing the directories.
        # check if pattern points to a directory

        glob_pattern = self.convert_pattern(pattern, recursive)
        log.info(f"getting list of files for {pattern} converted to {glob_pattern}")
        paths = []
        count = 0

        # log.warning(f"globbing: {self.root.cloud_prefix}{self.root.container}/{glob_pattern} with client {self.client}")
        
        for f in self.root.glob(glob_pattern, case_sensitive = False):  # this may be inconsistently implemented in different clouds.
            # check if file exists
            
            if f.is_file():
                # log.warning(f"f {f}")
                paths.append(FileSystemHelper._as_posix(f.relative_to(self.root), client=self.client))
            elif f.is_dir() and include_dirs:
                # log.warning(f"d {f}")
                paths.append(FileSystemHelper._as_posix(f.relative_to(self.root), client=self.client) + "/")  # add trailing slash to indicate directory
            else:
            #     log.warning(f"? {f}")
                continue
            
            count += 1
            if count >= page_size:
                log.info(f"Found {count} files in {pattern}")
                yield paths
                paths = []
                count = 0
        
        if (count > 0):
            yield paths
        log.info("completed retrieving files.")
        


    # copy 1 file from one location to another.
    def copy_file_to(self, relpath: Union[str, tuple], dest_path: Union[Self, Path, CloudPath], verbose: bool = False, **kwargs):
        """
        Copy a file from the current storage path to the destination path.

        Args:
            paths (Union[str, tuple]): a relative path to the root dir, or None if the root is a file.
            dest_path (Union[Self, Path, CloudPath]): The destination path, either a directory or a file path

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
                
        # not used.  let azure sdk automatically tune the number of threads per upload.  
        # we set the number of concurrent uploads, and tune the timeout.
        nthreads = kwargs.get("nthreads", 0) # 0 means use the default number of threads
        timeout = kwargs.get("timeout", 120)
                
        lock = kwargs.get("lock", None)
                
        if relpath is None: 
            # relpath is not specified, treat both src and dest as file paths.
            src_file = self.root
            # dest is a file.
            dest_file = dest
        else:
            # relpath is specified, so src and dest are directories
            # if relpath is a tuple, then we have src and dest.
            src_file = self.root.joinpath(relpath[0] if isinstance(relpath, tuple) else relpath)
            dest_file = dest.joinpath(relpath[1] if isinstance(relpath, tuple) else relpath)

        dest_file.parent.mkdir(parents=True, exist_ok=True)

        if src_is_cloud:
            if dest_is_cloud:
                log.debug(f"copying from cloud {src_file} to cloud {dest_file}")
                src_uri = src_file.as_uri()
                dest_uri = dest_file.as_uri()
                
                if src_uri.startswith("az://") and dest_uri.startswith("az://") and \
                    (src_file.client.service_client == dest_file.client.service_client) :
                    # src and dest are both azure blobs, and they share the same client.  use server side copy
                    dest_service = dest_file.client.service_client
                    dest_container = dest_service.get_container_client(dest_file.container)
                    dest_blob_client = dest_container.get_blob_client(dest_file.blob)
                    
                    src_service = src_file.client.service_client
                    src_container = src_service.get_container_client(src_file.container)
                    src_blob_client = src_container.get_blob_client(src_file.blob)
                    src_url = src_blob_client.url
                    parsed_url = urlparse(src_url)
                    internal_url = urlunparse(parsed_url._replace(netloc=self.internal_host))
                    log.debug(f"copying within azure from {src_url} via {internal_url} to {dest_blob_client.url}")
                    # now copy from internal url
                    dest_blob_client.start_copy_from_url(internal_url)
                else:               
                    src_file.copy(dest_file, force_overwrite_to_cloud=True)
                
                
            else:
                log.debug(f"downloading from cloud {src_file} to local {dest_file}")
                src_file.download_to(dest_file)
        else:
            if dest_is_cloud:
                log.debug(f"uploading from local {src_file} to cloud {dest_file}")
                # dest_file.upload_from(src_file, force_overwrite_to_cloud=True)
                dest_relpath = str(dest_file)[len("az://" + dest_file.container + "/"):] 
                if verbose:
                    log.debug(f"upload to {dest_relpath} with {str(nthreads) if nthreads > 0 else 'default number of'} threads")

                blob_serv_client = dest_file.client.service_client
                container_client = blob_serv_client.get_container_client(dest_file.container)
                with open(src_file, "rb") as data:
                    try:
                        if nthreads < 1:
                            container_client.upload_blob(name=dest_relpath, data=data, connection_timeout=timeout, overwrite=True)
                        else:
                            container_client.upload_blob(name=dest_relpath, data=data, connection_timeout=timeout, overwrite=True, max_concurrency=nthreads)

                    except TypeError as e:
                        log.error(f"uploading file {src_file} to {dest_relpath} with timeout {timeout} container {dest_file.container} exception {e}")
                        
            else:
                log.debug(f"copying from local {src_file} to local {dest_file}")
                try:
                    shutil.copy2(str(src_file), str(dest_file))
                except shutil.SameFileError:
                    pass
                except PermissionError as e:
                    log.error(f"permission issue copying file {src_file} to {dest_file} exception {e}")
                except Exception as e:
                    log.error(f"issue copying file {src_file} to {dest_file} exception {e}")
        # start = time.time()
        # meta =  FileSystemHelper.get_metadata(path = dest_file, with_metadata = True, with_md5 = True)
        # md5 = meta['md5']
        # log.debug(f"info time: {time.time() - start} size = {meta['size']} md5 = {md5}")


    async def async_copy_file_to(self, relpath: Union[str, tuple], dest_path: 'FileSystemHelper', **kwargs):
        """Async version of copy_file_to. Supports local source to local or Azure destination.

        Args:
            relpath: relative path string, or (src_rel, dest_rel) tuple.
            dest_path: FileSystemHelper for the destination.
        """
        if self.is_cloud:
            raise ValueError("async_copy_file_to only supports a local source path.")

        nthreads = kwargs.get("nthreads", 0)
        timeout  = kwargs.get("timeout", 120)

        src_rel, dest_rel = relpath if isinstance(relpath, tuple) else (relpath, relpath)
        src_file  = self.root / src_rel
        dest_file = dest_path.root / dest_rel

        if isinstance(dest_file, AzureBlobPath):
            if dest_path.async_azclient is None:
                raise ValueError("async_copy_file_to: Azure destination requires async_azclient on dest_path.")
            blob_name = dest_file.blob
            container = dest_file.container
            async_container_client = dest_path.async_azclient.get_container_client(container)
            upload_kwargs = {"overwrite": True, "connection_timeout": timeout}
            if nthreads > 0:
                upload_kwargs["max_concurrency"] = nthreads
            async with aiofiles.open(src_file, "rb") as f:
                await async_container_client.upload_blob(name=blob_name, data=f, **upload_kwargs)

        elif isinstance(dest_file, Path):
            await aiofiles.os.makedirs(dest_file.parent, exist_ok=True)
            async with aiofiles.open(src_file, "rb") as src:
                async with aiofiles.open(dest_file, "wb") as dst:
                    while chunk := await src.read(1024 * 1024):
                        await dst.write(chunk)

        else:
            raise NotImplementedError(f"async_copy_file_to does not support destination type {type(dest_file)}")
