import json

def load_tab_config(tab_name: str):
    import json

    with open("tab_config.json", encoding="utf-8") as f:
        config = json.load(f)

    print("📁 Available tabs in config:", list(config.keys()))  # Add this debug line

    if tab_name not in config:
        raise ValueError(f"Tab '{tab_name}' not found in configuration.")

    return config[tab_name]