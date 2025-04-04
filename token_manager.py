# token_manager.py
import json
import os
import time
import requests
from logging_utils import log_and_print
from dotenv import load_dotenv

load_dotenv()

TOKEN_FILE = ".token.json"
TOKEN_URL = os.getenv("TOKEN_URL", "https://login.microsoftonline.com/ce37b124-8959-4332-b77f-27143c359610/oauth2/v2.0/token")
CLIENT_ID = os.getenv("CLIENT_ID", "e3ba9906-6c2b-4214-b8c0-377712ce700e")
SCOPE = os.getenv("SCOPE", "api://e3ba9906-6c2b-4214-b8c0-377712ce700e/moj-az-ar-ecourts")

class TokenManager:
    def __init__(self, client_id=CLIENT_ID, scope=SCOPE, token_file=TOKEN_FILE):
        self.client_id = client_id
        self.scope = scope
        self.token_file = token_file
        self.token_data = self._load_or_initialize_token()

    def _load_or_initialize_token(self):
        if os.path.exists(self.token_file):
            with open(self.token_file, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            token = input("Paste your full token JSON (from F12): ")
            parsed = json.loads(token)
            parsed["expires_at"] = int(time.time()) + parsed.get("expires_in", 3600)
            self._save_token(parsed)
            log_and_print("📝 Initial token saved to .token.json", "info")
            return parsed

    def _save_token(self, data):
        with open(self.token_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        self.token_data = data

    def get_access_token(self):
        return self.token_data.get("access_token")

    def refresh_if_needed(self):
        now = int(time.time())
        exp_time = self.token_data.get("expires_at", 0)

        if now < exp_time - 120:  # still valid for 2 minutes
            return self.token_data["access_token"]

        log_and_print("🔁 Access token is expiring — refreshing...", "info")

        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "refresh_token": self.token_data["refresh_token"],
                "scope": self.scope
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )

        if response.status_code != 200:
            raise Exception(f"Failed to refresh token: {response.text}")

        new_token = response.json()
        new_token["expires_at"] = now + new_token["expires_in"]
        self._save_token(new_token)

        log_and_print("✅ Token refreshed successfully.", "success")
        return new_token["access_token"]
