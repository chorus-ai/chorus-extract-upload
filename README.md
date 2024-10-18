# chorus-extract-upload
Scripts and tools for organizing uploads to the CHoRUS central data repository

## Site-Specific Upload Environments

| SITE | Upload Environment |
| ---- | ------------------ |
| PITT | AZURE - Linux      |
| UVA  | LOCAL - Windows    | 
| EMRY | AWS/LOCAL - Linux        |
| TUFT | AZURE - Linux      |
| SEA  | GCP - Linux        |
| MGH  | AZURE - Linux      |
| NATI | LOCAL - HPC/Linux  |
| COLU | AZURE - Linux      |
| DUKE | AZURE              |
| MIT  | AZURE - Linux      | 
| FLOR | AZURE              |
| UCLA | LOCAL - Unknown    |
| UCSF | LOCAL - Linux      |
| UNM  | N/A                |
| MAYO | LOCAL - Windows    |



## Installation

The Upload Tool is a Python package.  The package can be installed using pip.  The package requires Python 3.7 or later.  A virtual environment (venv or conda) is strongly recommended.

1. create and configure a conda environment
```
conda create --name chorus python=3.10.14
conda activate chorus

pip install flit
```

or alternatively with python virtual envionment
```
python -m venv {venv_directory}
source {venv_directory}/bin/activate

pip install flit
```

2. get the software
```
git clone https://github.com/chorus-ai/chorus-extract-upload
cd chorus-extract-upload
```

3. install the software and dependencies
```
flit install
```

NOTE: for developers, you can instead run 
```
flit install --symlink
```
which allows changes in the code directory to be immediately reflected in the python environment.

4. Configure /etc/hosts
You need to modify the `/etc/hosts` file on the system from which you will be running the upload tool.

You will need root access to edit this file.  Add the following to the file:
```
xxx.xxx.xxx.xxx             choruspilotstorage.blob.core.windows.net
```

If this is not configured, you may see error in AZ CLI like so:

```
The request may be blocked by network rules of storage account. Please check network rule set using 'az storage account show -n accountname --query networkRuleSet'.
If you want to change the default action to apply when no rule matches, please use 'az storage account update'.
```

And with the built-in azure python library:
```
HttpResponseError: Operation returned an invalid status 'This request is not authorized to perform this operation.'
ErrorCode:AuthorizationFailure
```


On windows, the `/etc/hosts` file is instead `C:\Windows\system32\drivers\etc\hosts`.   Administrator privilege is needed to edit this file.

5. AZ CLI installation
You can configure the tool to use AZ CLI to upload files to the CHoRUS central cloud, or alternatively use the built in azure library for upload.   If you will be using AZ CLI, please install AZ CLI according to Microsoft instructions:

[Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
[Install Azcopy](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10?tabs=dnf)

Also, please make sure that the following environment variable is set:

Windows
```
set AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1
```

Linux
```
export AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1
```


## Configuration File:
A `config.toml` file needs to be customized for each DGS.

```
[configuration]
# upload method can be one of "azcli" or "builtin"
upload_method = "builtin"

[journal]
path = "az://container/journal.db"   # specify the journal file name. defaults to cloud storage
azure_account_name = "account_name"
azure_sas_token = "sastoken"

[central_path]
# specify the central (target) container/path, to which files are uploaded.  
# This is also the default location for the journal file
path = "az://container/"
azure_account_name = "account_name"
azure_sas_token = "sastoken"

[site_path]
  [site_path.default]
  # specify the default site (source) path
  path = "/mnt/data/site"

  # each can have its own access credentials
  [site_path.OMOP]
  # optional:  specific root paths for omop data
  path = "/mnt/data/site"

  [site_path.Images]
  # optional:  specific root paths for images
  path = "s3://container/path"
  aws_access_key_id = "access_key_id"
  aws_secret_access_key = "secret_access"

  [site_path.Waveforms]
  path = "/mnt/another_datadir/site"
```


## Usage

The Upload Tool is named `chorus_upload` contained in the subdirectory `chorus_upload` in `chorus-extract-upload`.  The Upload Tool can be run as

```
python chorus_upload [params] <subcommand> [subcommand params]
```

The `-h` parameter will display help information for the tool or each subcommand.  Suppported commands include `update`, `upload`, `usage`, and `verify`

Different `config.toml` files can be specified by using the `-c` parameter



### Setting Azure credential

From the Azure Portal, navigate to `Storage Account` / `Containers`, and select your DGS container.  Please make note of the account name (should be `choruspilotstorage`) and the container name (should be a short name for your institution).  In the left menu, select `Settings` / `Shared access tokens`.  Create a new SAS token with `Read`, `Add`, `Create`, `Write`, and `List` enabled, and optionally `Delete` if you intend to use the same sas token for deletion later.  We do not need `Immutable Storage`.  Copy the SAS token string and save it in a secure location.  The SAS token will be used by the Upload Tool. 

If you are transferring files from a cloud account to CHoRUS, please refer to you institution's documentation to retrieve credentials for other storage clouds.  For a list of supported authentication mechanisms for each tested cloud providers, please see the `config.toml.template` file.

### Create or Update Manifest
To create or an update journal, the required parameters are a journal name, a `site-path`, and optionally the cloud credential if `site-path` is a cloud storage path.  Optionally, the type of data (`OMOP`, `Images`, `Waveforms`) to use to update journal may be specified.   Multiple journal updates may be performed before a data submission.

```
python chorus_upload journal update
```


### Upload files

File upload follows the same pattern as journal update. 

From local file system
```
python chorus_upload file upload
```
