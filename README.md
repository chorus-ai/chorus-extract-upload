# chorus-extract-upload
Scripts and tools for organizing uploads to the CHoRUS central data repository

## IMPORTANT UPDATE 1/22/2026
Please note that due to CHoRUS cloud network configuration changes, web-based Active Directory login is now required instead of SAS tokens.  The `config.toml` file has been revised to allow this and related changes and therefore must be updated.  Please see the `Configuration` section to create or update the `config.toml` file.   AZ CLI will also be required for log in.


## Site-Specific Upload Environments

| SITE | Upload Environment |
| ---- | ------------------ |
| PITT | AZURE - Linux      |
| UVA  | LOCAL - Windows    | 
| EMRY | LOCAL - Linux        |
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

The `chorus-upload` tool is a Python package.  The package has been developed and tested using Python 3.12, and is currently only compatible with 3.12.  A virtual environment (venv, conda, or mamba) is strongly recommended.  To install miniconda, please follow the [Conda Installation Instructions](https://docs.anaconda.com/miniconda/install/#quick-command-line-install).  To install micromamba, please follow the [Mamba Installation Instructions](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html)

> **NOTE** Python 3.12 is required.  Other versions of python are not compatible.

1. Create and configure a conda environment: 
```
conda create -n chorus python=3.12

conda activate chorus

pip install flit
```

or use micromamba:
```
micromamba create -n chorus python=3.12

micromamba activate chorus

pip install flit
```

or alternatively with python virtual environment if your system has python 3.12 
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

> **REQUIRED**
> 5. AZ CLI installation (required for Active Directory log in):
> Please install AZ CLI according to Microsoft instructions:
>
> [Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)



### Configuration:

A `config.toml` file must be customized for each DGS.  A template is available as `chorus_upload/template.config.toml` in the `chorus-extract-upload` source tree.  Please see the template file for more details about each variable.   Below is an example illustrating different site_paths for different data types:  OMOP and Metadata are in versioned site directory, waveform data is in an on-prem directory, and image data are in S3 bucket.

If you are transferring files from a cloud account to CHoRUS, please refer to your institution's documentation to retrieve credentials for other storage clouds.  For a list of supported authentication mechanisms for each tested cloud providers, please see the `template.config.toml` file.

To edit the config.toml file, make a copy of `template.config.toml`, for example save as `config.toml`.  There are two REQUIRED edits, marked as `{CONTAINER}`, and `{LOCALPATH}`.  Replace all `{CONTAINER}` strings with site specific azure container name, for example `emory-temp`; note that `{` and `}` are also replaced.  The `{DATAPATH}` string should be replaced with the local file system path for the directory that contains `OMOP`, `Metadata`, and patient data directories.

If the OMOP, Waveforms, Images, and Metadata directories are not placed in the same root level folder, the `template.config.toml` file allows additional customization of the path for each modality.  Please see the `template.config.toml` file for details.

Example minimal `config.toml` file is show below

```

[configuration]
# common options.

supported_modalities = "OMOP,Images,Waveforms,Metadata"
page_size = 1000
num_threads = 12

[journal]
# REQUIRED  journaling mode can be either "full" or "append". 
journaling_mode = "append"

  [journal.local]
  path = "journal.v2.db"

  [journal.source]
  path = "az://emory-temp/journal.v2.db"

    [journal.source.auth]
    auth_mode = "login" 
    azure_account_url = "https://upload.chorus4ai.org"
    azure_account_name = "mghb2ailanding"

[central_path]
# REQUIRED specify the central (target) container/path, to which files are uploaded.  
path = "az://emory-temp/"

# REQUIRED - container to store this in.
azure_container = "emory-temp"

  [central_path.auth]
  # CONDITIONAL if file is in cloud
  # REQUIRED authentication mode.  either "login" or "sas_token"
  auth_mode = "login" 
  azure_account_url = "https://upload.chorus4ai.org"
  azure_account_name = "mghb2ailanding"

[site_path]
# NOTE: LOCAL WINDOWS PATH should use either forward slash "/" or double backslash "\\"
# each can have its own access credentials if in cloud.  These would not be inherited from default.

  [site_path.default]
  # REQUIRED specify the default site (source) path.
  path = "/mnt/data/site"

  # OPTIONAL if same as default path.  
  [site_path.OMOP]
  # OPTIONAL if section present:  specific root paths for omop data
  # path = "/mnt/data/site"

  # OPTIONAL if same as default path.
  [site_path.Images]

  # OPTIONAL pattern for Image files.  defaults to "{patient_id:w}/Images/{filepath}".  
  # parsible variables are "version", "patient_id", and "filepath"
  # use ":d" for digit only, or ":w" for digit, letter, and underscore. e.g. {patient_id:w}
  # modify to match the file organization under the root path.
  pattern = "{patient_id:w}/Images/{filepath}"

  # OPTIONAL if same as default path.  
  [site_path.Waveforms]
  pattern = "{patient_id:w}/Waveforms/{filepath}"

  [site_path.Metadata]
  pattern = "Metadata/{filepath}"

```


### Login

First log in using the AZCLI tool, and 
```
az login --use-device-code
```

this will present an url and a code.  Visit the url and follow the prompt.  When instructed, return the the commandline.

```
To sign in, use a web browser to open the page https://microsoft.com/devicelogin and enter the code ELLBL72ZY to authenticate.
```

Which then would present a set of subscriptions.  Choose the one named `mgh-chorus-nonprod`.

```
Retrieving tenants and subscriptions for the selection...
The following tenants don't contain accessible subscriptions. Use `az login --allow-no-subscriptions` to have tenant level access.
e004fb9c-b0a4-424f-bcd0-xxxxxxxxxxxx 'Emory'
93846777-7290-4ff3-a02d-xxxxxxxxxxxx 'MDIC'

[Tenant and subscription selection]

No     Subscription name    Subscription ID                       Tenant
-----  -------------------  ------------------------------------  ---------------------------------
[1] *  mgh-chorus-nonprod   2e7bd5a0-047d-44c9-a1eb-7f9f19622fd3  Mass General Brigham Incorporated

The default is marked with an *; the default tenant is 'Mass General Brigham Incorporated' and subscription is 'mgh-chorus-nonprod' (2e7bd5a0-047d-44c9-a1eb-7f9f19622fd3).
```

The credential will be saved locally in the user's .azcli directory.  Once logged in, the chorus_upload tool will be able to access the appropriate container.


### Usage

First activate the python virtual environment:
```
conda activate chorus
```
or
```
micromamba activate chorus
```
or
```
source {venv_directory}/bin/activate
```

The `chorus-upload` tool can be run from its source directory FOLLOWING THE GENERAL PATTERN as

```
cd chorus-extract-upload
python chorus_upload -c config.toml journal checkout 
python chorus_upload -c config.toml <command> <subcommand> [subcommand params]
python chorus_upload -c config.toml <command> <subcommand> [subcommand params]
...
python chorus_upload -c config.toml journal checkin 
```
Not that we checkout the journal file, perform multiple local operations, and then checkin the journal file, which uploads the updated journal to the cloud container. 

The `-h` parameter will display help information for the tool or each subcommand.

Different `config.toml` files can be specified by using the `-c` parameter


### Upload Process

THERE ARE 4 STEPS:

### 1. check out the journal
```
python chorus_upload -c config.toml journal checkout 
```

### 2. Create or Update Jounral
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

### 3. Upload files

> **Optional**
> ### Review files to be uploaded
> This is not required but is a good practice.  Command below would list all files to be uploaded.  The `--modalities` parameter can be added to restrict to a type of files..
> ```
> python chorus_upload -c config.toml file list
> ```


Files can be uploaded using the integrated, multithreading file upload logic.  Only files that have been added or modified since the last submission will be uploaded.   

#### Using integrated Azure Python SDK API  (RECOMMENDED)

From local file system
```
python chorus_upload -c config.toml file upload
```

Optionally, the type of data (`OMOP`, `Images`, `Waveforms`) may be specified to restrict upload to one or more data types.   For additional parameters, please see output of 
```
python chorus_upload -c config.toml file upload -h
```

> If the file upload is interrupted, you can simply restart by calling the upload command 
> again.  The script will upload all the missed files plus up to 100 previously uploaded 
> files.

> **Recommended**
> ### Using nohup and ssh on a remote server for long upload process 
> Uploading large amount of data may take a significant amount of time.  When using a remote linux machine to submit, `nohup` can be used to avoid the dropped ssh session causing upload to be interrupted.
> ```
> nohup python chorus_upload -c config.toml file upload &
> ```


> **Optional**
> ### Verify file uploads
> This is not required as data upload also verifies the data upload.  Command below would verify the last version uploaded.
> ```
> python chorus_upload -c config.toml file verify
> ```

### 4. Check in Journal file (REQUIRED)
```
python chorus_upload -c config.toml journal checkin 
```

