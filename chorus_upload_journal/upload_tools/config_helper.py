import tomli

def load_config(configifle: str):
    with open(configifle, 'rb') as file:
        config = tomli.load(file)
    return config

def get_journal_config(config: dict):
    return config['journal']

def get_central_config(config: dict):
    return config['central_path']

def get_site_config(config: dict, modality: str):
    subconfig = config['site_path']
    if (modality in subconfig.keys()):
        return subconfig[modality]
    elif "default" in subconfig.keys():
        return subconfig["default"]
    else:
        raise ValueError("No site path for modality not found in config file")

def get_auth_param(config: dict, key: str):
    if key in config.keys():
        return config[key]
    else:
        return None
