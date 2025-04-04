import json
from logging_utils import log_and_print

def load_tab_config(tab_name: str):
    import json

    with open("tab_config.json", encoding="utf-8") as f:
        config = json.load(f)

    log_and_print(f"📁 טאבים זמינים בקונפיג: {list(config.keys())}", "info", is_hebrew=True)


    if tab_name not in config:
        raise ValueError(f"Tab '{tab_name}' not found in configuration.")

    return config[tab_name]