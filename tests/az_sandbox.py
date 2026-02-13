#%% import

from cloudpathlib import AnyPath, CloudPath, S3Path, AzureBlobPath, GSPath
from cloudpathlib.client import Client
from cloudpathlib.enums import FileCacheMode
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from cloudpathlib import AzureBlobClient
from chorus_upload.storage_helper import FileSystemHelper
import os

#%%
# create client
cred = DefaultAzureCredential()
bsc = BlobServiceClient(account_url="https://upload.chorus4ai.org", credential=cred, connection_verify=False, connection_cert = None, max_single_get_size=4*1024*1024, max_single_put_size = 4*1024*1024)
abc= AzureBlobClient(blob_service_client = bsc, file_cache_mode = FileCacheMode.cloudpath_object)


#%% check if we can get account information
# account_info = bsc.get_account_information()  # 
# print(f"Account kind: {account_info['account_kind']}")

#%% check azureBlobClient glob operation.
cp = AzureBlobPath("az://emory-temp/20251217115855/journal_test.db", client=abc)
print(f"does it exist {cp.exists()}")
print(f"is dir?  is file? {cp.is_dir()} {cp.is_file()}")

#%% try the file system client - THIS IS NEEDED FOR cloudpathlib upgrade to 0.20+, which has performance improvements and allows for python 3.13+
dlc = DataLakeServiceClient(account_url="https://upload.chorus4ai.org",
                            credential=cred, 
                            # connection_verify=False, 
                            # connection_cert = None
                            )
for fs in dlc.list_file_systems():
    print(f"fs - {fs.name}")
fsc = dlc.get_file_system_client("emory-temp")
# this succeeds
print(f"fsc url = {fsc.url}. exists? {fsc.exists()}")
print("Top-level objects:")
# this fails with "The specified container does not exist".
# according to coPilot, this is a proxy-rewrite failure.  supposedly the "exists" is a false positive - 
#     https HEAD request is accidentally mapped to a url with response 200.
# the URL needs to contain account name, path needs to match ADSL gen 2 REST structure, 
#     and proxy rewrite must convert to a proper DFS endpoint.
for path in fsc.get_paths(recursive=False):
    if path.is_directory:
        print("DIR :", path.name)
    else:
        print("FILE:", path.name)
        
        
#%%
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
cred = DefaultAzureCredential()
proxy_url = 'https://upload.chorus4ai.org'
proxies = {
    'https': proxy_url,
}
dlc = DataLakeServiceClient(
    account_url="https://mghb2ailanding.dfs.core.windows.net/", 
    credential=cred, 
    proxies=proxies,
)
# this still does not work.  Tried with direct proxy settings in the client code,
# also tried environment variable settings (HTTPS_PROXY, NO_PROXY).  no luck
# problem is with URL rewriting - LIKELY upload.chorus4ai.org is not an actual proxy.
for fs in dlc.list_file_systems():
    print(f"fs - {fs.name}")
fsc = dlc.get_file_system_client(file_system="emory-temp")
print(f"fsc url = {fsc.url}. exists? {fsc.exists()}")
print("Top-level objects:")
for path in fsc.get_paths(recursive=False):
    if path.is_directory:
        print("DIR :", path.name)
    else:
        print("FILE:", path.name)

# paths = fsc.get_paths(path="20251217115855", recursive=True)
# for path in paths:
#     print(f" - {path.name}, is_directory: {path.is_directory}, size: {path.content_length}")



#%% now try to glob  (this is not working for CloudPathLib 0.20+ using HNS in datalake v2 and switch to filesystem client causes 'container not found'.  this may be a config issue.)
cp = AnyPath("az://emory-temp", client=abc)
print(f"cp name {cp.name}")
for f in cp.glob(pattern="20251217115855/*", case_sensitive=False):
    print(f" - {f}")


#%% any path test - okay.
ap = AnyPath(".", client=abc)
for f in ap.glob("*", case_sensitive=False):
    print(f" - {f}")

#%% connect to container
cc = bsc.get_container_client("emory-temp")
for blob in cc.list_blobs():
    print(f" - {blob.name}")
    
    

#%% test getting a list of files
fsh = FileSystemHelper("az://emory-temp", client = abc)
for paths in fsh.get_files(pattern="*"):
    for path in paths:
        print(f" - {path}")


#%% try using the FileSystemHelper
fsh = FileSystemHelper("az://emory-temp/journal_test.db", abc)
fsh.root.touch()
for blob in cc.list_blobs():
    print(f" - {blob.name}")

#%%  try deleting
bc = cc.get_blob_client("journal_test.db")
bc.delete_blob()
for blob in cc.list_blobs():
    print(f" - {blob.name}")
    
#%%    
# copy from locked to unlocked.
lbc = cc.get_blob_client("journal_test.db.locked")
bc.start_copy_from_url(lbc.url)
# this shoudl fail because upload.chorus4ai.org does not allow public read access.
for blob in cc.list_blobs():
    print(f" - {blob.name}")

#%% download
with open("test.db", 'wb') as file:
    download_stream = lbc.download_blob()
    file.write(download_stream.readall())
    print(f"downloaded")

#%% try copying from original url
bc.start_copy_from_url("https://mghb2ailanding.blob.core.windows.net/emory-temp/journal_test.db.locked")
for blob in cc.list_blobs():
    print(f" - {blob.name}")
bc.delete_blob()
    
#%%
# convert from public to windows.net url
from urllib.parse import urlparse, urlunparse
lbc_url = lbc.url
parsed_url = urlparse(lbc_url)
new_netloc = "mghb2ailanding.blob.core.windows.net"
new_url = urlunparse(parsed_url._replace(netloc=new_netloc))
print(new_url)
bc.start_copy_from_url(new_url)
for blob in cc.list_blobs():
    print(f" - {blob.name}")

#%%
from azure.storage.filedatalake import DataLakeServiceClient
import os

service = DataLakeServiceClient.from_connection_string(
    "YOUR_CONNECTION_STRING"
)

fs = service.get_file_system_client("my-container")

print("Top-level objects:")
for path in fs.get_paths(recursive=False):
    if path.is_directory:
        print("DIR :", path.name)
    else:
        print("FILE:", path.name)
