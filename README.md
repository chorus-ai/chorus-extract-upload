# chorus-extract-upload
Scripts and tools for organizing uploads to the CHoRUS central data repository

## IMPORTANT UPDATE 5/2/2025
Please note that due to recent CHoRUS cloud network configuration changes, previous `config.toml` files need to be updated to reflect the new container name, the new storage account name.

SAS tokens can be retrieved using a new process using site-specific 1Password URLs.  Please see "Getting Azure credential" section below.

For new users, please follow the instructions below.

For existing users, the changes are in the following sections of the `config.toml` file.

```
[journal]
path : add "-temp" suffix to container name:  e.g. "az://emory/journal.db" to "az://emory-temp/journal.db"
azure_account_name : "choruspilotstorage" -> "mghb2ailanding"
azure_sas_token : retrieve using the 1password process.

[central_path]
path : add "-temp" suffix to container name:  e.g. "az://emory" to "az://emory-temp"
Azure_container : add "-temp" suffix to container name:  e.g. "emory" to "emory-temp"
azure_account_name : "choruspilotstorage" -> "mghb2ailanding"
azure_sas_token : retrieve using the 1password process.
```

Also, previous change in `/etc/hosts` is no longer needed.  Please see step 4 below for instruction of undoing that change.


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

The `chorus-upload` tool is a Python package.  The package can be installed using pip.  The package requires Python 3.7 or later.  A virtual environment (venv or conda) is strongly recommended.  To install conda, please follow the [Conda Installation Instructions](https://docs.anaconda.com/miniconda/install/#quick-command-line-install)

> **NOTE** Python 3.12 is now required.

1. Create and configure a conda environment: 
```
conda create --name chorus python=3.12

conda activate chorus

pip install flit
```

or alternatively with python virtual environment
```
python -m venv {venv_directory}
source {venv_directory}/bin/activate

pip install flit
```
> **Note**: on Windows, the command to activate python virtual environment is `{venv_directory}\Scripts\activate.bat`


2. Get the software:
```
git clone https://github.com/chorus-ai/chorus-extract-upload
cd chorus-extract-upload
```

> **Alternative** If you do not have git on the system, you can download the source code as a zip file and decompress it into the `chorus-extrat-upload` directory.

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
The `/etc/hosts` modification that were previously required is no longer required for the `chorus-upload` tool. If you have previously added the following entry to your `/etc/hosts` file:

```
172.203.106.139             choruspilotstorage.blob.core.windows.net
```

please remove it to avoid potential conflicts or issues. 

On Linux or macOS, you can edit the `/etc/hosts` file using a text editor with root privileges:

```
sudo nano /etc/hosts
```

Locate the line containing `172.203.106.139 choruspilotstorage.blob.core.windows.net` and delete it. Save the file and exit the editor.

On Windows, the equivalent file is located at:

```
C:\Windows\System32\drivers\etc\hosts
```

Open the file in a text editor with Administrator privileges, locate the same line, and remove it. Save the file and close the editor.

After making these changes, no further `/etc/hosts` configuration is necessary for the tool to function correctly.


> **Optional**
> 5. AZ CLI installation (only when using `chorus-upload` generated azcli scripts):
> You can configure the tool to use AZ CLI to upload files to the CHoRUS central cloud, or alternatively use the built in azure library for upload.   If you will be using AZ CLI, please install AZ CLI according to Microsoft instructions:
>
> [Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)


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

**Using 1Password for SAS Token Retrieval**

For institutions using 1Password to manage Azure credentials, follow these steps to retrieve your SAS token:

1. Open your institution-specific 1Password URL in a web browser. The URL should be provided by your CHoRUS administrator.
2. Log in to 1Password using your credentials.
3. Search for the entry labeled with your institution's name and "Azure SAS Token."
4. Copy the SAS token value from the entry.
5. Paste the SAS token into the `azure_sas_token` field in your `config.toml` file.

Ensure the SAS token is stored securely and not shared publicly. If you encounter any issues, contact your CHoRUS administrator for assistance.

~~For users with CHoRUS cloud storage access via the Azure Portal, please generate a sas token for your container.  If you do not have access, please reach out to a member of the CHoRUS central cloud team.~~

~~From the Azure Portal, navigate to `Storage Account` / `Containers`, and select your DGS container.  Please make note of the account name (should be `mghb2ailanding`) and the container name (should be a short name for your institution).  In the left menu, select `Settings` / `Shared access tokens`.  Create a new SAS token with `Read`, `Add`, `Create`, `Write`, `Move`, `Delete`, and `List` enabled (or just select all the options).  The expiration of the SAS token is recommended to be 1 month from the creation date.  Copy the SAS token string and save it in a secure location.  The SAS token will be used by the `chorus-upload` tool.~~

If you are transferring files from a cloud account to CHoRUS, please refer to your institution's documentation to retrieve credentials for other storage clouds.  For a list of supported authentication mechanisms for each tested cloud providers, please see the `config.toml.template` file.


### Configuration:
A `config.toml` file must be customized for each DGS.  A template is available as `chorus_upload/config.toml.template` in the `chorus-extract-upload` source tree.  Please see the template file for more details about each variable.   Below is an example illustrating different site_paths for different data types:  OMOP and Metadata are in versioned site directory, waveform data is in an on-prem directory, and image data are in S3 bucket.

```
# REQUIRED field are annotated as such

[configuration]
supported_modalities = "OMOP,Images,Waveforms,Metadata"

# REQUIRED
# journaling mode can be either "full" or "append". 
# in full mode, the source data is assumed to be a full repo and journal is taking a snapshot.  Previous version file that are missing in the current file system are considered as deleted
# in append mode, the source data is assumed to be new or updated files only.  Previous version file that are missing in the current file system are NOT considered as deleted.  To delete a file, "file delete" has to be called.
journaling_mode = "full"

# flag to indicate whether the central path should have modality as top level or patient a top level.  Default to patient_first = true 
patient_first = true

[journal]
# REQUIRED.  location of the journal file.  defaults to chorus cloud storage.
path = "az://emory-temp/journal.db"

# REQUIRED if journal is in azure cloud.
azure_account_name = "mghb2ailanding"

# REQUIRED if journal is in azure cloud.
azure_sas_token = "sastoken"

# local path for downloaded journal file.  default is "journal.db"
local_path = "journal.db"

[central_path]
# specify the central (target) container/path, to which files are uploaded.  
# This is also the default location for the journal file
# REQUIRED
path = "az://emory-temp/"

# REQUIRED
azure_container = "emory-temp"
# REQUIRED azure account credentials are specified as one or more of the following.
azure_account_name = "mghb2ailanding"
# REQUIRED azure account credentials are specified as one or more of the following.
azure_sas_token = "sastoken"

[site_path]

# NOTE: LOCAL WINDOWS PATH should use either forward slash "/" or double backslash "\\"
# each can have its own access credentials if in cloud.

  [site_path.default]
  # REQUIRED specify the default site (source) path.
  path = "/mnt/data/site"

  # OPTIONAL if same as default path.  
  [site_path.OMOP]
  # REQUIRED if section present:  specific root paths for omop data
  path = "/mnt/data/site"

  # if true, OMOP is a subdirectory of patient directory:  "{patient_id:w}/OMOP/*.csv"
  # if false, OMOP is a sibling directory of patients:  "OMOP/*.csv"
  # default is false
  # omop_per_patient = false

  # indicates if the relative path is in a dated directory. default is false
  versioned = true

  # pattern for omop files.  defaults to "OMOP/{filepath}".  
  # parsible variables are "version", "patient_id", and "filepath"
  # use ":d" for digit only, or ":w" for digit, letter, and underscore. e.g. {patient_id:w}
  # modify to match the file organization under the root path.
  pattern = "{version:w}/OMOP/{filepath}"


  # OPTIONAL if same as default path.
  [site_path.Images]
  # REQUIRED if section present:  specific root paths for images
  path = "s3://container/path"
  s3_container = "container"
  aws_access_key_id = "access_key_id"
  aws_secret_access_key = "secret_access"
  
  # indicates if the relative path is in a dated directory.  default is false
  versioned = false

  # pattern for Image files.  defaults to "{patient_id:w}/Images/{filepath}".  
  # parsible variables are "version", "patient_id", and "filepath"
  # use ":d" for digit only, or ":w" for digit, letter, and underscore. e.g. {patient_id:w}
  # modify to match the file organization under the root path.
  pattern = "{patient_id:w}/Images/{filepath}"

  # OPTIONAL if same as default path.  
  [site_path.Waveforms]
  # REQUIRED if section present.  specifies root path of waveforms
  path = "/mnt/another_datadir/site"

  # indicates if the relative path is in a dated directory.  default is false
  versioned = false

  # pattern for Waveform files.  defaults to "{patient_id:w}/Waveforms/{filepath}".  
  # parsible variables are "version", "patient_id", and "filepath"
  # use ":d" for digit only, or ":w" for digit, letter, and underscore. e.g. {patient_id:w}
  # modify to match the file organization under the root path.
  pattern = "{patient_id:w}/Waveforms/{filepath}"

  # OPTIONAL if same as default path.  
  [site_path.Metadata]
  # REQUIRED if section present.  specifies root path of Metadata
  path = "/mnt/another_datadir/site"

  # indicates if the relative path is in a dated directory.  default is false
  versioned = false

  # pattern for Submission Metadata files.  defaults to "Metadata/{filepath}".  
  # parsible variables are "version", "patient_id", and "filepath"
  # use ":d" for digit only, or ":w" for digit, letter, and underscore. e.g. {patient_id:w}
  # modify to match the file organization under the root path.
  pattern = "{version:w}/Metadata/{filepath}"

```


### General Usage

First activate the python virtual environment:
```
conda activate chorus
```
or
```
source {venv_directory}/bin/activate
```

The `chorus-upload` tool can be run from its source directory FOLLOWING THE GENERAL PATTERN as

```
cd chorus-extract-upload
python chorus_upload [params] <command> <subcommand> [subcommand params]
```

The `-h` parameter will display help information for the tool or each subcommand.

Different `config.toml` files can be specified by using the `-c` parameter


### Upload Process

THERE ARE 2 STEPS:

### 1. Create or Update Jounral
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

### 2. Upload files

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


