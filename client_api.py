# api_client.py

import os
import requests
import urllib3
from dotenv import load_dotenv
from logging_utils import log_and_print

# Load environment variables from .env file
load_dotenv()

# Base config
BASE_URL = os.getenv("BASE_URL", "https://bo-casemanagement-int.prod.k8s.justice.gov.il")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")  # Must be set in .env
MOJ_APP_ID = os.getenv("MOJ_APPLICATION_ID", "8Aj5UqGBUtKGh7hxPrfbSQ==")

# Disable SSL warnings (only for internal dev environments)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_case_details(case_id):
    url = f"{BASE_URL}/api/Case/GetCase?CaseId={case_id}"
    
    if not BEARER_TOKEN:
        log_and_print("❌ Error: BEARER_TOKEN is missing from .env", "error")
        return None

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Moj-Application-Id": MOJ_APP_ID,
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        print(f"🔎 Raw response status: {response.status_code}")
        print(f"🔎 Response text: {response.text[:300]}...")

        if response.status_code == 204:
            log_and_print(f"⚠️ No data found for CaseId {case_id} (204 No Content)", "warning")
            return None

        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        log_and_print(f"❌ HTTP error occurred: {http_err}", "error")
    except requests.exceptions.RequestException as req_err:
        log_and_print(f"❌ Request error occurred: {req_err}", "error")
    except Exception as e:
        log_and_print(f"❌ Unexpected error: {e}", "error")

    return None
