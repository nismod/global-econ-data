import sys
import os
import pathlib as pl
import json
####


def load_config():
    """Read config.json
    """
    config_path = os.path.join(os.path.dirname(__file__),'config.json')
    
    with open(config_path, 'r') as config_fh:
        config = json.load(config_fh)
    
    return { name : pl.Path(val) for name,val in config.items() }

config = load_config()
print(config)