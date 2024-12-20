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
| MGH  | LOCAL - Linux      |
| NATI | LOCAL - HPC/Linux  |
| COLU | AZURE - Linux      |
| DUKE | AZURE              |
| MIT  | GCP - Linux      | 
| FLOR | AZURE              |
| UCLA | LOCAL - Unknown    |
| UCSF | LOCAL - Linux      |
| UNM  | N/A                |
| MAYO | LOCAL - Windows    |

### Installation

The `chorus-upload` tool is a Python package.  The package can be installed using pip.  The package requires Python 3.7 or later.  A virtual environment (venv or conda) is strongly recommended.  Either Python 3.10 or 3.12 works but the next version will use 3.12.

1. Create and configure a conda environment: 
```
conda create --name chorus python=3.10.14

conda activate chorus

pip install flit
```

or alternatively with python virtual environment
```
python -m venv {venv_directory}
source {venv_directory}/bin/activate

pip install flit
```

2. Get the software:
```
git clone https://github.com/chorus-ai/chorus-extract-upload
cd chorus-extract-upload
```

3. Install the software and dependencies:
```
cd chorus-extract-upload
flit install
```

NOTE: for developers, you can instead run 
```
flit install --symlink
```
which allows changes in the code directory to be immediately reflected in the python environment.

4. Configure /etc/hosts:
You need to modify the `/etc/hosts` file on the system from which you will be running the upload tool.

You will need root access to edit this file.  Add the following to the file:
```
172.203.106.139             choruspilotstorage.blob.core.windows.net
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


On windows, the `/etc/hosts` file equivalent `C:\Windows\system32\drivers\etc\hosts`.   Administrator privilege is needed to edit this file.

5. AZ CLI installation (only when using `chorus-upload` generated azcli scripts):
You can configure the tool to use AZ CLI to upload files to the CHoRUS central cloud, or alternatively use the built in azure library for upload.   If you will be using AZ CLI, please install AZ CLI according to Microsoft instructions:

[Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)


6. Setting environment variables:
Also, please make sure that the following environment variable is set.  It is recommended that they are set in Linux .profiles file or as Windows user environment variable.

Windows
```
set AZURE_CLI_DISABLE_CONNECTION_VERIFICATION 1
```

Linux
```
export AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1
```



### Getting Azure credential

For users with CHoRUS cloud storage access via the Azure Portal, please generate a sas token for your container.  If you do not have access, please reach out to a member of the CHoRUS central cloud team.

From the Azure Portal, navigate to `Storage Account` / `Containers`, and select your DGS container.  Please make note of the account name (should be `choruspilotstorage`) and the container name (should be a short name for your institution).  In the left menu, select `Settings` / `Shared access tokens`.  Create a new SAS token with `Read`, `Add`, `Create`, `Write`, `Move`, `Delete`, and `List` enabled (or just select all the options).  The expiration of the SAS token is recommended to be 1 month from the creation date.  Copy the SAS token string and save it in a secure location.  The SAS token will be used by the `chorus-upload` tool. 

If you are transferring files from a cloud account to CHoRUS, please refer to your institution's documentation to retrieve credentials for other storage clouds.  For a list of supported authentication mechanisms for each tested cloud providers, please see the `config.toml.template` file.


### Configuration:
A `config.toml` file must be customized for each DGS.  A template is available as `chorus_upload/config.toml.template` in the `chorus-extract-upload` source tree.

```
[configuration]
# upload method can be one of "azcli" or "builtin"
upload_method = "builtin"

# journaling mode can be either "full" or "append". 
# in full mode, the source data is assumed to be a full repo and journal is taking a snapshot.  Previous version file that are missing in the current file system are considered as deleted
# in append mode, the source data is assumed to be new or updated files only.  Previous version file that are missing in the current file system are NOT considered as deleted.  To delete a file, "file delete" has to be called.
journaling_mode = "full"

[journal]
path = "az://{DGS_CONTAINER}/journal.db"   # specify the journal file name. defaults to cloud storage
azure_account_name = "choruspilotstorage"
azure_sas_token = "{sastoken}"

# local path for downloaded journal file.  default is "journal.db"
local_path = "journal.db"

[central_path]
# specify the central (target) container/path, to which files are uploaded.  
# This is also the default location for the journal file
path = "az://{DGS_CONTAINER}/"
azure_account_name = "choruspilotstorage"
azure_sas_token = "{sastoken}"

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


### Usage

First activate the python virtual environment:
```
conda activate chorus
```
or
```
source {venv_directory}/bin/activate
```

The `chorus-upload` tool can be run from its source directory as

```
cd chorus-extract-upload
python chorus_upload [params] <command> <subcommand> [subcommand params]
```

The `-h` parameter will display help information for the tool or each subcommand.

Different `config.toml` files can be specified by using the `-c` parameter

The general process is 1. create/update journal, and 2. upload files


### Create or Update Manifest
To create or an update journal, the following command can be run.  
```
python chorus_upload -c config.toml journal update
```

By default, the current date and time is used as the submission version.  A specific version can be specified optionally
```
python chorus_upload -c config.toml journal update --version 20241130080000
```

The last version can be amended by using the --amend flag.   Multiple journal updates may be performed before a data submission.
```
python chorus_upload -c config.toml journal update --amend
```

The types of data (`OMOP`, `Images`, `Waveforms`) can be specified to restrict update to one or more data types.
```
python chorus_upload -c config.toml journal update --modalities OMOP,Images
```

### Upload files

Files can be uploaded using either the integrated, multithreading file upload logic, or via a generated az-cli script.  Only files that have been added or modified since the last submission will be added.  

#### Option 1: using integrated Azure Python SDK API  (RECOMMENDED)

From local file system
```
python chorus_upload -c config.toml file upload
```

Optionally, the type of data (`OMOP`, `Images`, `Waveforms`) may be specified to restrict upload to one or more data types.   For additional parameters, please see output of 
```
python chorus_upload -c config.toml file upload -h
```

If the file upload is interrupted, and you need to restart, first locate the local journal file (`[journal] local_path` in the `config.toml` file), then call
```
python chorus_upload -c config.toml journal checkin --local-journal {journal_filename}
```

then rerun the upload command.  The script will upload all the missed files plus up to 100 previously uploaded files.


#### Option 2: using generated az-cli script
A Linux bash script or a Windows batch file can be generated that includes all files that are submission candidates.  Executing this batch/shell script will invoke `az storage blob upload` to upload the files one by one.  Please note that this is currently single threaded.

```
python chorus_upload -c config.toml file list --output-file {output_file_name} --output-type azcli
```

Data types can be optionally specified via the `--modalities` flag.  For additional parameters, please see output of 
```
python chorus_upload -c config.toml file list -h
```


If the file upload is interrupted, and you need to restart, first locate the local journal file (`[journal] local_path` in the `config.toml` file), then call
```
python chorus_upload -c config.toml journal checkin --local-journal {journal_filename}
```

then rerun the `file list` command and execute the generated batch/shell script.  The `file list` command will generate a new batch/shell script with all the missed files plus up to 100 previously uploaded files.


> **Optional**
> ### Verify file uploads
> This is not required as data upload also verifies the data upload.  Command below would verify the last version uploaded.
> ```
> python chorus_upload -c config.toml file verify
> ```


