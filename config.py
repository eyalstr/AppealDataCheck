# config.py

import os
import sys
from dotenv import load_dotenv
from logging_utils import log_and_print  # Optional if you have structured logging

def load_configuration():
    """
    Dynamically load the .env file, even from an executable context.
    """
    # Detect execution context
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    env_path = os.path.join(base_dir, '.env')

    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)
        log_and_print(f"✅ Loaded configuration from {env_path}")
    else:
        log_and_print(f"❌ Configuration file not found at {env_path}", "error")
        exit(1)

    required_env_vars = [
        "BEARER_TOKEN",
        "MOJ_APPLICATION_ID",
        "BASE_URL"
    ]

    for var in required_env_vars:
        if not os.getenv(var):
            log_and_print(f"❌ Missing required environment variable: {var}", "error")
            exit(1)
