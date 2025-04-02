# api_client.py

import os
import requests
import urllib3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base config
BASE_URL = os.getenv("BASE_URL", "https://bo-casemanagement-int.prod.k8s.justice.gov.il")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")  # Must be set in .env
MOJ_APP_ID = os.getenv("MOJ_APPLICATION_ID", "8Aj5UqGBUtKGh7hxPrfbSQ==")

# Disable SSL warnings (only for internal dev environments)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_case_details(case_id):
    """
    Fetch case details by case_id using internal authenticated API.
    Returns parsed JSON or None on failure.
    """
    url = f"{BASE_URL}/api/Case/GetCase?CaseId={case_id}"
    
    if not BEARER_TOKEN:
        print("❌ Error: BEARER_TOKEN is missing from .env")
        return None

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Moj-Application-Id": MOJ_APP_ID,
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, verify=False)  # Skip SSL verification for internal use
        response.raise_for_status()
        print(f"✅ Case {case_id} fetched successfully.")
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"❌ Request error occurred: {req_err}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    return None
