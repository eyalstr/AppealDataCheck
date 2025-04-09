# client_api.py
import os
import requests
import urllib3
import config

from dotenv import load_dotenv
from logging_utils import log_and_print
from urllib.parse import urlencode

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
        print(f"🔎 Response text: {response.text[:50]}...")

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

def fetch_case_documents(case_id: int) -> dict:
    url = "https://ecourtsdocumentsint.justice.gov.il/api/Documents"

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
        "Moj-Application-Id": MOJ_APP_ID
    }

    payload = {
        "defaultValue": {
            "doc_type": "",
            "sub_type": "",
            "department_id": "",
            "item_type": "",
            "r_creation_date_from": "",
            "r_creation_date_to": "",
            "doc_date_from": "",
            "doc_date_to": "",
            "status_date_from": "",
            "status_date_to": ""
        },
        "entitiesOperator": 0,
        "expression": {
            "propertiesList": [
                {
                    "propertyName": "folder_ids",
                    "values": [f"1 {case_id}"],
                    "operator": 0
                }
            ],
            "operator": 0
        },
        "itemsPerPage": 40,
        "pageNo": 1,
        "sorts": [
            {
                "propertyName": "r_creation_date",
                "ascending": False
            }
        ],
        "viewFields": [
            "moj_id",
            "sub_type",
            "doc_type",
            "doc_date",
            "r_creation_date",
            "status_date",
            "object_name",
            "department_id"
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, verify=False)
        log_and_print(f"🔎 Document API response status: {response.status_code}", "info")

        if response.status_code != 200:
            log_and_print(f"❌ Failed to fetch documents for case {case_id}. Status: {response.status_code}", "error")
            return {}

        return response.json()

    except Exception as e:
        log_and_print(f"❌ Exception occurred while fetching documents: {e}", "error")
        return {}

def fetch_case_discussions(case_id: int) -> dict:
    url = f"https://bo-discussions-int.prod.k8s.justice.gov.il/api/DiscussionsBo/All/{case_id}"

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Accept": "application/json",
        "Moj-Application-Id": MOJ_APP_ID
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        log_and_print(f"🔎 Discussion API response status: {response.status_code}", "info")

        if response.status_code != 200:
            log_and_print(f"❌ Failed to fetch discussions for case {case_id}. Status: {response.status_code}", "error")
            return {}

        return response.json()

    except Exception as e:
        log_and_print(f"❌ Exception occurred while fetching discussions: {e}", "error")
        return {}

def fetch_role_contacts(role_ids: list) -> dict:
    if not role_ids:
        return {}

    base_url = "https://bo-contacts-int.prod.k8s.justice.gov.il/api/RoleInCorporation"
    params = "&".join(f"RoleInCorporationIds={rid}" for rid in role_ids)
    url = f"{base_url}?{params}"

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Accept": "application/json",
        "Moj-Application-Id": MOJ_APP_ID
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        log_and_print(f"🔎 Contact API response status: {response.status_code}", "info")

        if response.status_code != 200:
            log_and_print(f"❌ Failed to fetch contact data. Status: {response.status_code}", "error")
            return {}

        return response.json()

    except Exception as e:
        log_and_print(f"❌ Exception occurred while fetching contact data: {e}", "error")
        return {}
