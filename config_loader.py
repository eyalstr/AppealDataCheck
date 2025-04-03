import json

def load_tab_config(tab_name: str, config_path="config.json"):
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    
    if tab_name not in config:
        raise ValueError(f"Tab '{tab_name}' not found in configuration.")
    
    tab_config = config[tab_name]
    return {
        "url": tab_config["url"],
        "sql_file": tab_config["table"],
        "field_map": tab_config["matchingKeys"][0]["columns"],
        "key_field": tab_config["matchingKeys"][0]["key"],
        "json_path": tab_config["matchingKeys"][0]["jsonPath"]
    }
