# chorus-extract-upload
Scripts and tools for organizing uploads to the CHoRUS central data repository

## Site-Specific Upload Environments

| SITE | Upload Environment |
| ---- | ------------------ |
| PITT | AZURE - Linux      |
| UVA  | LOCAL - Windows    | 
| EMRY | AWS - Linux        |
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


# INSTALLATION

First install the python package "flit"

```
pip install flit
```

## Local Dev Installation

This will set up a symlink in the site-package directory to the source tree.

From the root of the source tree, run

```
flit install --symlink
```

## Install Package

This will become available once the tool is released to the public.  We expect the installation to involve
running

```
pip install upload_manifest
```

 
