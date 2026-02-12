import re
from pathlib import Path
import os

import platform

import logging
log = logging.getLogger(__name__)

WINDOWS_STRINGS = {
    "eol": "\n",  # supposed to use "\n" reguardless of os.
    "var_start": "%",
    "var_end": "%",
    "comment": "@REM",
    "set_var": "set ",
    "set_var_quoted": "set \"",
    "set_var_quoted_start": "",
    "set_var_quoted_end": "\"",
    "command_processor": "cmd /c ",
    "export_var": "set"
}

LINUX_STRINGS = {
    "eol": "\n",
    "var_start": "${",
    "var_end": "}",
    "comment": "#",
    "set_var": "",
    "set_var_quoted": "",
    "set_var_quoted_start": "\"",
    "set_var_quoted_end": "\"",
    "command_processor": "",
    "export_var": "export"
}


def _write_auth_params(is_linux: bool, auth_mode: str, transport:str, url: str = None) -> str:
    # check sas vs login
    var_start = LINUX_STRINGS["var_start"] if is_linux else WINDOWS_STRINGS["var_start"]
    var_end = LINUX_STRINGS["var_end"] if is_linux else WINDOWS_STRINGS["var_end"]
    set_var_quoted_start = LINUX_STRINGS["set_var_quoted_start"] if is_linux else WINDOWS_STRINGS["set_var_quoted_start"]
    set_var_quoted_end = LINUX_STRINGS["set_var_quoted_end"] if is_linux else WINDOWS_STRINGS["set_var_quoted_end"]
    
    if transport == "azcli":
        if auth_mode == "sas":    
            return f"--account-name {var_start}account{var_end} --sas-token \"{var_start}sas_token{var_end}\"" 
        elif auth_mode == "login":
            return f"--account-name {var_start}account{var_end} --auth-mode login"
        else:
            raise ValueError("Unknown auth_mode: " + auth_mode)
    elif transport == "azcopy":
        if auth_mode == "sas":    
            return f"?{set_var_quoted_start}{var_start}sas_token{var_end}{set_var_quoted_end}" 
        elif auth_mode == "login":
            if url is not None:
                suffix = ".".join(url.strip("/").split(".")[-2:])
                return f"--trusted-microsoft-suffixes {suffix}"
            else:
                return ""
        else:
            raise ValueError("Unknown auth_mode: " + auth_mode)
    else:
        raise ValueError("Unknown transport: " + transport)
    
    
def _write_auth_login(is_linux: bool, auth_mode: str, f, **kwargs):
    
    out_type = kwargs.get('out_type', '').lower()
    
    is_wsl = "microsoft" in platform.release().lower() if is_linux else False
    
    comment = LINUX_STRINGS["comment"] if is_linux else WINDOWS_STRINGS["comment"]
    eol = LINUX_STRINGS["eol"] if is_linux else WINDOWS_STRINGS["eol"]
    cmd_proc = LINUX_STRINGS["command_processor"] if is_linux else WINDOWS_STRINGS["command_processor"]
    set_var = LINUX_STRINGS["set_var"] if is_linux else WINDOWS_STRINGS["set_var"]
    set_var_quoted = LINUX_STRINGS["set_var_quoted"] if is_linux else WINDOWS_STRINGS["set_var_quoted"]
    set_var_quoted_start = LINUX_STRINGS["set_var_quoted_start"] if is_linux else WINDOWS_STRINGS["set_var_quoted_start"]
    set_var_quoted_end = LINUX_STRINGS["set_var_quoted_end"] if is_linux else WINDOWS_STRINGS["set_var_quoted_end"]
    
    dest_config = kwargs.get('dest_config', {})
    container = dest_config.get('azure_container', '')
    
    f.write(f"{comment} Destination azure credentials{eol}")
    if auth_mode == "sas":
        account_name = dest_config.get('azure_account_name', '')
        sas_token = dest_config.get('azure_sas_token', '')
        f.write(f"{set_var}account={account_name}{eol}")
        if not is_linux:
            f.write(f"{set_var_quoted} sas_token={set_var_quoted_start}{sas_token.replace("%", "%%")}{set_var_quoted_end}{eol}")
        else:                
            f.write(f"{set_var_quoted} sas_token={set_var_quoted_start}{sas_token}{set_var_quoted_end}{eol}")
    f.write(f"{set_var}container={container}{eol}")
    f.write(eol)
    
    if auth_mode == "sas":
        return
    elif auth_mode != "login":
        raise ValueError("Unknown auth_mode: " + auth_mode)
    
    if (out_type == "azcopy"):
        if is_wsl:
            f.write(f"{comment} azcopy login (WSL){eol}")
            f.write(f"{cmd_proc}keyctl session{eol}")
        else:
            f.write(f"{comment} azcopy login{eol}")
        f.write(f"{set_var}AZCOPY_AUTO_LOGIN_TYPE=DEVICE{eol}")
        f.write(f"{cmd_proc}az login --use-device-code{eol}")
        f.write(f"{cmd_proc}azcopy login --login-type azcli{eol}")
        f.write(eol)
    elif (out_type == "azcli"):
        f.write(f"{comment} az cli login{eol}")
        f.write(f"{cmd_proc}az login --use-device-code{eol}")
        f.write(eol)

def _write_auth_logout(is_linux: bool, auth_mode: str, f, **kwargs):
    out_type = kwargs.get('out_type', '').lower()
    
    is_wsl = "microsoft" in platform.release().lower() if is_linux else False
    
    comment = LINUX_STRINGS["comment"] if is_linux else WINDOWS_STRINGS["comment"]
    eol = LINUX_STRINGS["eol"] if is_linux else WINDOWS_STRINGS["eol"]
    cmd_proc = LINUX_STRINGS["command_processor"] if is_linux else WINDOWS_STRINGS["command_processor"]
    set_var = LINUX_STRINGS["set_var"] if is_linux else WINDOWS_STRINGS["set_var"]
    set_var_quoted = LINUX_STRINGS["set_var_quoted"] if is_linux else WINDOWS_STRINGS["set_var_quoted"]
    set_var_quoted_start = LINUX_STRINGS["set_var_quoted_start"] if is_linux else WINDOWS_STRINGS["set_var_quoted_start"]
    set_var_quoted_end = LINUX_STRINGS["set_var_quoted_end"] if is_linux else WINDOWS_STRINGS["set_var_quoted_end"]
    
    dest_config = kwargs.get('dest_config', {})

    
    if auth_mode == "sas":
        return
    elif auth_mode != "login":
        raise ValueError("Unknown auth_mode: " + auth_mode)
    
    if (out_type == "azcopy"):


        f.write(f"{comment} azcopy logout{eol}")
        f.write(f"{cmd_proc}azcopy logout{eol}")
        f.write(f"{cmd_proc}az logout{eol}")
        if is_wsl:
            f.write(f"{cmd_proc}keyctl invalidate @s{eol}")
        f.write(eol)
    elif (out_type == "azcli"):
        f.write(f"{comment} az cli logout{eol}")
        f.write(f"{cmd_proc}az logout{eol}")
        f.write(eol)
    

def _write_journal_download(is_linux: bool, auth_mode: str, f, **kwargs):
    out_type = kwargs.get('out_type', '').lower()
    journal_transport = kwargs.get('journal_transport', out_type).lower()
    cloud_journal = kwargs.get('cloud_journal', "journal.db")

    pattern = re.compile(r"az://[^/]+/(.+)")
    comment = LINUX_STRINGS["comment"] if is_linux else WINDOWS_STRINGS["comment"]
    eol = LINUX_STRINGS["eol"] if is_linux else WINDOWS_STRINGS["eol"]
    cmd_proc = LINUX_STRINGS["command_processor"] if is_linux else WINDOWS_STRINGS["command_processor"]
    set_var = LINUX_STRINGS["set_var"] if is_linux else WINDOWS_STRINGS["set_var"]
    var_start = LINUX_STRINGS["var_start"] if is_linux else WINDOWS_STRINGS["var_start"]
    var_end = LINUX_STRINGS["var_end"] if is_linux else WINDOWS_STRINGS["var_end"]
    
    matches = pattern.match(cloud_journal)
    # DEBUG
    # local_journal = kwargs.get('local_journal', cloud_journal)
    # journal_is_local = False
    if matches:
        cloud_journal = matches.groups()[0]
        local_journal = kwargs.get('local_journal', cloud_journal.split("/")[-1])
        journal_is_local = False
    else:
        # if not storing journal in azure, then don't use azcli for journal transport.
        journal_is_local = True  
        # cloud_journal = cloud_journal
        local_journal = kwargs.get('local_journal', cloud_journal)    
        
    testme = ""
    testme_end = ""

    # set the journal name variables
    if journal_is_local:
        if not is_linux:
            f.write(set_var + "\"local_journal=" + local_journal + "\"" + eol)
        else:
            f.write(set_var + "local_journal=\"" + local_journal + "\"" + eol)
    else:
        f.write(comment + " Checkout the journal" + eol)
        if not is_linux:
            f.write(set_var + "\"local_journal=" + local_journal + "\"" + eol)
            f.write(set_var + "\"cloud_journal=" + cloud_journal + "\"" + eol)
        else:
            f.write(set_var + "local_journal=\"" + local_journal + "\"" + eol)
            f.write(set_var + "cloud_journal=\"" + cloud_journal + "\"" + eol)
            
        # check out the journal
        if (journal_transport == "builtin"):
            raise ValueError("pre generated script does not support built-in journal transport type ")
        elif (journal_transport == "azcli"):
            # use azcli to perform checkout.
            # check if lock file exists.
            # if yes, end.
            # if no, check if journal exists, 
            #    if yes, move it to locked, and copy it to local
            #    if no, create a locked file.  use local journal.
            
            auth_params = _write_auth_params(is_linux, auth_mode, out_type)

            # check if lock file exists.  if yes, end.            
            if not is_linux:
                f.write("for /F \"tokens=*\" %%a in ('az storage blob exists " + auth_params + 
                        " --container-name " + var_start+"container"+var_end + 
                        " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" + " --output tsv') do " + 
                        set_var + "exists=%%a" + eol)
                f.write("if \"" + var_start + "exists" + var_end + "\" == \"True\" (" + eol)
            else:
                f.write(set_var + "exists=`az storage blob exists " + auth_params + 
                        " --container-name " + var_start+"container"+var_end + 
                        " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" + " --output tsv`" + eol)
                f.write("if [[ \"" + var_start + "exists" + var_end + "\" == \"True\" ]]; then" + eol)
                
            f.write("    echo Journal is locked.  Cannot continue." + eol)
            
            if not is_linux:
                f.write("    exit /b 1" + eol)
                f.write(")" + eol)
            else:
                f.write("    exit 1" + eol)
                f.write("fi" + eol)
            
            # if here, then lock does not exist.  create it 
            if not is_linux:
                # check remote journal existence
                f.write("for /F \"tokens=*\" %%a in ('az storage blob exists " + auth_params + 
                        " --container-name " + var_start+"container"+var_end + 
                        " --name \"" + var_start+"cloud_journal"+var_end + "\" --output tsv') do " + 
                    set_var + "exists=%%a" + eol)
                
                # set up the conditional
                f.write("if \"" + var_start + "exists" + var_end + "\" == \"False\" (" + eol)
                # if no remote journal, create a new local journal file
                f.write("    type nul > " + var_start+"local_journal"+var_end + eol)
            else:
                # check lock existence
                f.write(set_var + "exists=`az storage blob exists " + auth_params + 
                        " --container-name " + var_start+"container"+var_end + 
                        " --name \"" + var_start+"cloud_journal"+var_end + "\" --output tsv`" + eol)
                # set up the conditional
                f.write("if [[ \"" + var_start + "exists" + var_end + "\" == \"False\" ]]; then" + eol)
                # if no remote lock, create a new local journal file
                f.write("    touch " + var_start+"local_journal"+var_end + eol)
            # upload as the lock file
            f.write("    " + cmd_proc + "az storage blob upload " + auth_params + 
                    " --container-name " + var_start+"container"+var_end + 
                    " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" +
                    " --file \"" + var_start+"local_journal"+var_end + "\"" + 
                    " --only-show-errors --output none" + eol)
            # else if there is a remote journal, move it to locked and download it.
            if not is_linux:
                f.write(") else (" + eol)
            else:
                f.write("else" + eol)
                
            # move the remote journal to locked
            # create the lock
            f.write(testme + "    " + cmd_proc + "az storage blob copy start" + 
                    " --source-blob \"" + var_start+"cloud_journal"+var_end + "\"" +
                    " --source-container " + var_start+"container"+var_end + 
                    auth_params +
                    " --destination-blob \"" + var_start+"cloud_journal"+var_end + ".locked\"" + 
                    " --destination-container " + var_start+"container"+var_end + 
                    auth_params +  testme_end + eol)
            # download the journal
            f.write(testme + "    " + cmd_proc + "az storage blob download " + 
                    auth_params + 
                    " --container-name " + var_start+"container"+var_end + 
                    " --name \"" + var_start+"cloud_journal"+var_end + "\"" +
                    " --file \"" + var_start+"local_journal"+var_end + "\"" + 
                    " --overwrite --only-show-errors --output none" + testme_end + eol)
            if not is_linux:
                f.write(")" + eol)
            else:
                f.write("fi" + eol)
        elif (journal_transport == "azcopy"):
            # use azcopy to perform checkout.
            # check if lock file exists.
            # if yes, end.
            # if no, check if journal exists, 
            #    if yes, move it to locked, and copy it to local
            #    if no, create a locked file.  use local journal.
            
            if auth_mode == "sas":
                local_auth = "?" + var_start+"mod_local_sas_token"+var_end
                auth_str = "?" + var_start+"sas_token"+var_end
            elif auth_mode == "login":
                local_auth = ""
                auth_str = ""
            else:
                raise ValueError("Unknown auth_mode: " + auth_mode)

            dest_config = kwargs.get('dest_config', {})
            dest_url = "https://" + var_start+"account"+var_end + ".blob.core.windows.net"
            dest_url = dest_config.get('azure_account_url', dest_url)
            auth_params = _write_auth_params(is_linux, auth_mode, out_type, url=dest_url)
            
            # check if lock file exists.  if yes, end.           
            remote_journal_path = "\"" + dest_url + "/" + \
                    var_start+"container"+var_end + "/" + \
                    var_start+"cloud_journal"+var_end + auth_str + "\"" 
            remote_lock_path = "\"" + dest_url + "/" + \
                    var_start+"container"+var_end + "/" + \
                    var_start+"cloud_journal"+var_end + ".locked" + auth_str + "\"" 
            
                    
            if not is_linux:
                f.write("azcopy list " + remote_lock_path + " --location Blob " + auth_params + " | findstr /C:\"Name:\" >nul" + eol)
                f.write("if " + var_start + "ERRORLEVEL" + var_end + " == 0 (" + eol)
            else:
                f.write("if azcopy list " + remote_lock_path + " --location Blob " + auth_params + " | grep -q \"Name:\"; then" + eol)
                
            f.write("    echo Journal is locked.  Cannot continue." + eol)
            
            if not is_linux:
                f.write("    exit /b 1" + eol)
                f.write(")" + eol)
            else:
                f.write("    exit 1" + eol)
                f.write("fi" + eol)
            
            # if here, then lock does not exist.  create it 

            if not is_linux:
                # check remote journal existence
                f.write("azcopy list " + remote_journal_path + " --location Blob " + auth_params + " | findstr /C:\"Name:\" >nul" + eol)
                # set up the conditional
                f.write("if not " + var_start + "ERRORLEVEL" + var_end + " == 0 (" + eol)
                # if no remote journal, create a new local journal file
                f.write("    type nul > " + var_start+"local_journal"+var_end + eol)
            else:
                # check remote journal existentce
                f.write("if ! azcopy list " + remote_journal_path + " --location Blob " + auth_params + " | grep -q \"Name:\"; then" + eol)
                # if no remote lock, create a new local journal file
                f.write("    touch " + var_start+"local_journal"+var_end + eol)
            # upload as the lock file
            f.write("    " + testme + cmd_proc + "azcopy copy " + 
                    "\"" + var_start+"local_journal"+var_end + "\" " + 
                    "\"" + dest_url + "/" + var_start+"container"+var_end + "/" + var_start+"cloud_journal"+var_end + ".locked" + auth_str + "\" " + 
                    "--from-to LocalBlob --overwrite=true " + auth_params + " " + testme_end + eol )
            # else if there is a remote journal, move it to locked and download it.
            if not is_linux:
                f.write(") else (" + eol)
            else:
                f.write("else" + eol)
                
            # move the remote journal to locked
            # create the lock
            f.write("    " + testme + cmd_proc + "azcopy copy " + 
                    "\"" + dest_url + "/" + var_start+"container"+var_end + "/" + var_start+"cloud_journal"+var_end + auth_str + "\" " + 
                    "\"" + dest_url + "/" + var_start+"container"+var_end + "/" + var_start+"cloud_journal"+var_end + ".locked" + auth_str + "\"" +
                    " --from-to BlobBlob " + auth_params + " " + testme_end + eol )
            
            # download the journal
            f.write("    " + testme + cmd_proc + "azcopy copy " + 
                    "\"" + dest_url + "/" + var_start+"container"+var_end + "/" + var_start+"cloud_journal"+var_end + auth_str + "\" " +
                    "\"" + var_start+"local_journal"+var_end + "\"" + 
                    " --from-to BlobLocal --overwrite=true " + auth_params + " " + testme_end + eol )
            if not is_linux:
                f.write(")" + eol)
            else:
                f.write("fi" + eol)
                
    f.write(eol)

def _write_file_upload(root:str, fn:str, info:dict, is_linux: bool, auth_mode: str, f, **kwargs):
    submit_version = info['version']
    
    if not is_linux:
        local_fn = "\\".join([root, fn])
        local_fn = local_fn.replace("/", "\\")
    else:
        local_fn = str(Path(root) / fn)
        
    central_fn = info['central_path']

    out_type = kwargs.get('out_type', '').lower()
    eol = LINUX_STRINGS["eol"] if is_linux else WINDOWS_STRINGS["eol"]
    cmd_proc = LINUX_STRINGS["command_processor"] if is_linux else WINDOWS_STRINGS["command_processor"]
    var_start = LINUX_STRINGS["var_start"] if is_linux else WINDOWS_STRINGS["var_start"]
    var_end = LINUX_STRINGS["var_end"] if is_linux else WINDOWS_STRINGS["var_end"]
    testme = ""
    testme_end = ""
    
    
    if (out_type == "azcli"):
        auth_params = _write_auth_params(is_linux, auth_mode, out_type)
        f.write(testme + cmd_proc + "az storage blob upload " + auth_params + " --container-name " + 
                var_start+"container"+var_end + " --name \"" + submit_version + "/" + central_fn + 
                "\" --file \"" + local_fn + "\" --overwrite --only-show-errors --output none" + testme_end + eol)
    elif (out_type == "azcopy"):
        if auth_mode == "sas":
            local_auth = "?" + var_start+"mod_local_sas_token"+var_end
            auth_str = "?" + var_start+"sas_token"+var_end
        elif auth_mode == "login":
            local_auth = ""
            auth_str = ""
        else:
            raise ValueError("Unknown auth_mode: " + auth_mode)
        
        dest_config = kwargs.get('dest_config', {})
        dest_url = "https://" + var_start+"account"+var_end + ".blob.core.windows.net"
        dest_url = dest_config.get('azure_account_url', dest_url)
        
        auth_params = _write_auth_params(is_linux, auth_mode, out_type, url=dest_url)
        
        if (root.startswith("az://")):
            mod_config = kwargs.get('mod_config', {})
            mod_account_name = mod_config.get('azure_account_name', '')
            mod_url = f"https://{mod_account_name}.blob.core.windows.net"
            mod_url = mod_config.get('azure_account_url', mod_url)
            
            mod_auth_params = _write_auth_params(is_linux, auth_mode, out_type, url=mod_url)
            f.write(testme + cmd_proc + "azcopy copy " + 
                    "\"" + mod_url + "/" + var_start+"mod_local_container"+var_end + "/" + fn + local_auth + "\" " + 
                    "\"" + dest_url + "/" + var_start+"container"+var_end + "/" + submit_version + "/" + central_fn + auth_str + "\" " + 
                    " --from-to BlobBlob --overwrite=true " + auth_params + " " + mod_auth_params + " " + testme_end + eol )
        elif (root.startswith("s3://")):
            f.write(testme + cmd_proc + "azcopy copy " + 
                    "\"https://s3.amazonaws.com/" + var_start+"mod_local_container"+var_end + "/" + fn + "\" " + 
                    "\"" + dest_url + "/" + var_start+"container"+var_end + "/" + submit_version + "/" + central_fn + auth_str + "\" " + 
                    "--from-to S3Blob --overwrite=true " + auth_params + " " + testme_end + eol )
        # elif (root.startswith("gs://")):
        #     ...
        else:
            f.write(testme + cmd_proc + "azcopy copy \"" + local_fn + "\" " + 
                    "\"" + dest_url + "/" + var_start+"container"+var_end + "/" + submit_version + "/" + central_fn + auth_str + "\" " + 
                    "--from-to LocalBlob --overwrite=true " + auth_params + " " + testme_end + eol )
    elif (out_type == "builtin"):
        raise ValueError("pre generated script does not support built-in transport type ")
    else:
        raise ValueError("Unknown out_type: " + out_type)



def _write_journal_upload(is_linux: bool, auth_mode: str, f, **kwargs):
    out_type = kwargs.get('out_type', '').lower()
    journal_transport = kwargs.get('journal_transport', out_type).lower()
    cloud_journal = kwargs.get('cloud_journal', "journal.db")

    pattern = re.compile(r"az://[^/]+/(.+)")
    comment = LINUX_STRINGS["comment"] if is_linux else WINDOWS_STRINGS["comment"]
    eol = LINUX_STRINGS["eol"] if is_linux else WINDOWS_STRINGS["eol"]
    cmd_proc = LINUX_STRINGS["command_processor"] if is_linux else WINDOWS_STRINGS["command_processor"]
    config_fn = kwargs.get('config_fn', '')
    var_start = LINUX_STRINGS["var_start"] if is_linux else WINDOWS_STRINGS["var_start"]
    var_end = LINUX_STRINGS["var_end"] if is_linux else WINDOWS_STRINGS["var_end"]
    
    
    matches = pattern.match(cloud_journal)
    #DEBUG
    # journal_is_local = False
    if matches:
        journal_is_local = False
    else:
        # if not storing journal in azure, then don't use azcli for journal transport.
        journal_is_local = True
        
    testme = ""
    testme_end = ""
    
    # backup to uploaded/versioned directory.  Always call that file journal.db
    f.write(comment + " Backup journal file to upload directory" + eol)
    if (journal_transport in ["builtin", 'list']):
        raise ValueError("pre generated script does not support built-in journal transport type, or 'list' ")
    elif journal_transport == "azcli":
        auth_params = _write_auth_params(is_linux, auth_mode, out_type)

        f.write(testme + cmd_proc + "az storage blob upload " + auth_params + " --container-name " + 
                    var_start+"container"+var_end + " --name \"" + var_start+"ver"+var_end + "/journal.db\"" +
                    " --file \"" + var_start+"local_journal"+var_end + "\" --overwrite --only-show-errors --output none" + testme_end + eol) 
    elif journal_transport == "azcopy":
        if auth_mode == "sas":
            auth_str = "?" + var_start+"sas_token"+var_end
        elif auth_mode == "login":
            auth_str = ""
        else:
            raise ValueError("Unknown auth_mode: " + auth_mode)
        
        dest_config = kwargs.get('dest_config', {})
        dest_url = "https://" + var_start+"account"+var_end + ".blob.core.windows.net"
        dest_url = dest_config.get('azure_account_url', dest_url)

        auth_params = _write_auth_params(is_linux, auth_mode, out_type, url=dest_url)
        # check if lock file exists.  if yes, end.           
        remote_ver_journal_path = "\"" + dest_url + "/" + \
                var_start+"container"+var_end + "/" + \
                var_start+"ver"+var_end + "/journal.db" + auth_str + "\"" 
        f.write(testme + cmd_proc + "azcopy copy " +
                "\"" + var_start+"local_journal"+var_end + "\"" +
                remote_ver_journal_path + 
                " --from-to LocalBlob --overwrite=true " + auth_params + " " + testme_end + eol) 
        
    
    if not journal_is_local:
        f.write(comment + " COPY journal file to upload directory" + eol)
        if (journal_transport in ["builtin", 'list']):
            raise ValueError("pre generated script does not support built-in journal transport type, or 'list' ")
        elif (journal_transport == "azcli"):
            auth_params = _write_auth_params(is_linux, auth_mode, out_type)

            f.write(testme + cmd_proc + "az storage blob upload " + auth_params + " --container-name " + 
                                    var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + 
                                    "\" --file \"" + var_start+"local_journal"+var_end + "\" --overwrite --only-show-errors --output none" + testme_end + eol)
            f.write(testme + cmd_proc + "az storage blob delete " + auth_params + " --container-name " + 
                                    var_start+"container"+var_end + " --name \"" + var_start+"cloud_journal"+var_end + ".locked\"" + 
                                    " --only-show-errors --output none" + testme_end + eol)
        elif (journal_transport == "azcopy"):
            if auth_mode == "sas":
                local_auth = "?" + var_start+"mod_local_sas_token"+var_end
                auth_str = "?" + var_start+"sas_token"+var_end
            elif auth_mode == "login":
                local_auth = ""
                auth_str = ""
            else:
                raise ValueError("Unknown auth_mode: " + auth_mode)
            
            remote_ver_journal_path = "\"" + dest_url + "/" + \
                var_start+"container"+var_end + "/" + \
                var_start+"ver"+var_end + "/journal.db" + auth_str + "\"" 
            remote_journal_path = "\"" + dest_url + "/" + \
                var_start+"container"+var_end + "/" + \
                var_start+"cloud_journal"+var_end + auth_str + "\"" 
            remote_lock_path = "\"" + dest_url + "/" + \
                var_start+"container"+var_end + "/" + \
                var_start+"cloud_journal"+var_end + ".locked" + auth_str + "\"" 
                
            auth_params = _write_auth_params(is_linux, auth_mode, out_type, url=dest_url)
        
            # copy the journal remotely
            f.write(testme + cmd_proc + "azcopy copy " +
                    remote_ver_journal_path + " " +
                    remote_journal_path + " --from-to BlobBlob --overwrite=true " + auth_params + " " + testme_end + eol) 
            # delete the lock file
            f.write(testme + cmd_proc + "azcopy remove " + 
                    remote_lock_path + " --from-to BlobTrash " + auth_params + " " + testme_end + eol)
            
    f.write(eol)





    
# only works with azure dest for now.
# OLD: file_list is a dictionary of structure {modality: (config, {version: [files]})}
# file_list is a dictionary of structure {modality: (config, {fn: {version: .., central_path:..}}})}
def _write_files(file_list: dict, upload_datetime: str, filename : str, **kwargs):
    out_type = kwargs.get('out_type', 'list').lower()
    journal_transport = kwargs.get('journal_transport', out_type).lower()
    dest_config = kwargs.get('dest_config', {})
    pattern = re.compile(r"az://[^/]+/(.+)")
    # cloud_journal = kwargs.get('cloud_journal', "journal.db")
    page_size = kwargs.get('page_size', 1000)  
    max_upload_count = kwargs.get('max_num_files', None)  
    

    if (not dest_config['path'].startswith("az://")):
        log.warning(f"Destination is not an azure path.  cannot generate azcli script, but can genearete a list of source files")
        out_type = "list"
    
 
    auth_mode = dest_config.get('auth_mode', 'login').lower()
    
    # track configuration file
    config_fn = kwargs.get('config_fn', '')
    
    # check source paths.
    source_paths = set(config['path'] for _, (config, _) in file_list.items())
    source_is_cloud = any([(p.startswith("az://")) or (p.startswith("s3://")) or (p.startswith("gs://")) for p in source_paths])
    if (out_type == "azcli") and source_is_cloud:
        log.warning(f"WARNING: source path is in the cloud, cannot generate azcli script.  using 'azcopy' instead")
        out_type = "azcopy"
        
    # get output file name.
    p = Path(filename)
    # fn = "_".join([p.stem, upload_datetime]) + p.suffix
    fn = filename
    log.info(f"list writing to {fn}")

    is_windows = (os.name == 'nt')
    # is_windows = True
    
    eol = WINDOWS_STRINGS["eol"] if is_windows else LINUX_STRINGS["eol"]
    comment = WINDOWS_STRINGS["comment"] if is_windows else LINUX_STRINGS["comment"]
    var_start = WINDOWS_STRINGS["var_start"] if is_windows else LINUX_STRINGS["var_start"]
    var_end = WINDOWS_STRINGS["var_end"] if is_windows else LINUX_STRINGS["var_end"]
    set_var = WINDOWS_STRINGS["set_var"] if is_windows else LINUX_STRINGS["set_var"]
    set_var_quoted = WINDOWS_STRINGS["set_var_quoted"] if is_windows else LINUX_STRINGS["set_var_quoted"]
    set_var_quoted_start = WINDOWS_STRINGS["set_var_quoted_start"] if is_windows else LINUX_STRINGS["set_var_quoted_start"]
    set_var_quoted_end = WINDOWS_STRINGS["set_var_quoted_end"] if is_windows else LINUX_STRINGS["set_var_quoted_end"]
    export_var = WINDOWS_STRINGS["export_var"] if is_windows else LINUX_STRINGS["export_var"]
    cmd_proc = WINDOWS_STRINGS["command_processor"] if is_windows else LINUX_STRINGS["command_processor"]
    testme = ""
    testme_end = ""
    
    with open(fn, 'w') as f:
        # if windows, use double backslash
        
        if (out_type != "azcli") and (out_type != "azcopy"):
            for _, (config, fninfos) in file_list.items():
                root = config_helper.get_path_str(config)            
                filenames = [str(Path(root) / fn) for fn in fninfos.keys()]
                if is_windows:
                    fns = [fn.replace("/", "\\") for fn in filenames]
                else:
                    fns = filenames
                    
                f.write(eol.join(fns))
                f.write(eol)
        
        else:
            
            # this first part of the script is same for both azcli and azcopy.
            if is_windows:
                # BATCH FILE FORMAT
                f.write("@echo off" + eol)
                f.write("setlocal" + eol)
            else:
                f.write("#!/bin/bash" + eol)
                
            f.write(comment + " script to upload files to azure for upload date " + upload_datetime + eol)
            f.write(eol)

            f.write(comment + " set environment variables" + eol)
            f.write(set_var + "dt=" + upload_datetime + eol)
            f.write(export_var + " AZURE_CLI_DISABLE_CONNECTION_VERIFICATION=1" + eol)
            f.write(eol)

            f.write("echo PLEASE add your virtual environment activation string if any." + eol)
            f.write(eol)
                        
            # need to handle azcli and azcopy login
            _write_auth_login(not is_windows, auth_mode, f, **kwargs)
                        
            _write_journal_download(not is_windows, auth_mode, f, **kwargs)
            
            f.write(comment + " upload files" + eol)
            f.write(eol)
            
            # then we iterate over the files, and decide based on out_type.
            upload_count = 0
            for mod, (config, fnInfos) in file_list.items():
                root = config['path']
                
                f.write(eol)
                f.write(comment + " Files upload for " + mod + " at root " + root + eol)
                
                if is_windows:
                    f.write("if exist files_" + var_start+"dt"+var_end + ".txt del files_" + var_start+"dt"+var_end + ".txt" + eol)
                    f.write("type nul > files_" + var_start+"dt"+var_end + ".txt" + eol)
                else:
                    f.write("if [ -e files_" + var_start+"dt"+var_end + ".txt ]; then" + eol)
                    f.write("    rm files_" + var_start+"dt"+var_end + ".txt" + eol)
                    f.write("fi" + eol)
                    f.write("touch files_" + var_start+"dt"+var_end + ".txt" + eol)
                
                if (root.startswith("az://")):
                    mod_local_account = config.get('azure_account_name', '')
                    mod_local_container = config.get('azure_container', '')
                    mod_local_sas_token = config.get('azure_sas_token', '')
                    
                    f.write(set_var + "mod_local_account=" + mod_local_account + eol)
                    f.write(set_var_quoted + "mod_local_sas_token=" + set_var_quoted_start + mod_local_sas_token + set_var_quoted_end + eol)
                    f.write(set_var + "mod_local_container=" + mod_local_container + eol)
                    f.write(eol)
                            
                elif (root.startswith("s3://")):
                    mod_local_container = config.get('s3_bucket', '')
                    mod_local_ACCESS_KEY = config.get('s3_access_key', '')
                    mod_local_SECRET_KEY = config.get('s3_secret_key', '')

                    f.write(export_var + " AWS_ACCESS_KEY_ID=\"" + mod_local_ACCESS_KEY + "\"" + eol)
                    f.write(export_var + " AWS_SECRET_ACCESS_KEY=\"" + mod_local_SECRET_KEY + "\"" + eol)
                    f.write(eol)

                f.write(eol)
                # f.write(set_var + "ver=" + version + eol)                    
                count = 0
                step = page_size
                for fn, info in fnInfos.items():
                    _write_file_upload(root, fn, info, not is_windows, auth_mode, f, **kwargs, **{'mod_config': config})
                    
                    f.write("echo " + fn + " >> files_" + var_start+"dt"+var_end + ".txt" + eol)
                    f.write(eol)
                    
                    count += 1
                    upload_count += 1
                    
                    if (count >= step):
                        # last part is again the same.
                        f.write(eol)
                        f.write(testme + "python chorus_upload -c " + config_fn + 
                                " file mark_as_uploaded_local --local-journal \"" + var_start+"local_journal"+var_end + "\"" +
                                " --file-list files_" + var_start+"dt"+var_end + ".txt --upload-datetime " + 
                                var_start+"dt"+var_end + testme_end + eol)

                        f.write(eol)
                        if is_windows:
                            f.write("if exist files_" + var_start+"dt"+var_end + ".txt del files_" + 
                                    var_start+"dt"+var_end + ".txt" + eol)
                            f.write("type nul > files_" + var_start+"dt"+var_end + ".txt" + eol)
                        else:
                            f.write("if [ -e files_" + var_start+"dt"+var_end + ".txt ]; then" + eol)
                            f.write("    rm files_" + var_start+"dt"+var_end + ".txt" + eol)
                            f.write("fi" + eol)
                            f.write("touch files_" + var_start+"dt"+var_end + ".txt" + eol)
                        f.write(eol)
                        count = 0
                    if (max_upload_count is not None) and (max_upload_count > 0) and (upload_count >= max_upload_count):
                        break
                              
                if count > 0:
                    # last part is again the same.
                    f.write(eol)
                    f.write(testme + "python chorus_upload -c " + config_fn + 
                            " file mark_as_uploaded_local --local-journal \"" + var_start+"local_journal"+var_end + "\"" +
                            " --file-list files_" + var_start+"dt"+var_end + ".txt --upload-datetime " + 
                            var_start+"dt"+var_end + testme_end + eol)
                    f.write(eol)
                    
                    if is_windows:
                        f.write("if exist files_" + var_start+"dt"+var_end + ".txt del files_" + 
                                var_start+"dt"+var_end + ".txt" + eol)
                        f.write("type nul > files_" + var_start+"dt"+var_end + ".txt" + eol)
                    else:
                        f.write("if [ -e files_" + var_start+"dt"+var_end + ".txt ]; then" + eol)
                        f.write("    rm files_" + var_start+"dt"+var_end + ".txt" + eol)
                        f.write("fi" + eol)
                        f.write("touch files_" + var_start+"dt"+var_end + ".txt" + eol)

                if (max_upload_count is not None) and (max_upload_count > 0) and (upload_count >= max_upload_count):
                    break
            
            auth_params = _write_auth_params(not is_windows, auth_mode, out_type)
            f.write(comment + " TEST checkin the journal" + eol)
            f.write(testme + cmd_proc + "az storage blob list " + auth_params + " --container-name " + 
                    var_start+"container"+var_end + " --prefix " + var_start+"ver"+var_end +
                    "/ --output table" + testme_end + eol)     
            f.write(eol)

            _write_journal_upload(not is_windows, auth_mode, f, **kwargs)
            
            _write_auth_logout(not is_windows, auth_mode, f, **kwargs)
