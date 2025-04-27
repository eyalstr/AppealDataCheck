import json
from utils.logging_utils import log_and_print
import os

def load_tab_config(tab_name: str):
    

    # Make the path relative to the script location
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "tab_config.json")

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    log_and_print(f"📁 טאבים זמינים בקונפיג: {list(config.keys())}", "info", is_hebrew=True)

    if tab_name not in config:
        raise ValueError(f"Tab '{tab_name}' not found in configuration.")

    return config[tab_name]
