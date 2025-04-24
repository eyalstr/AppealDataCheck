import os
import json
import requests
from dotenv import load_dotenv
from logging_utils import log_and_print

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
MOJ_APP_ID = os.getenv("APPLICATION_ID")

BASE_CACHE_DIR = os.path.join("data")


def get_case_dir(case_id):
    """
    Returns the directory path for a given case ID.
    """
    path = os.path.join(BASE_CACHE_DIR, str(case_id))
    os.makedirs(path, exist_ok=True)
    return path


def get_tab_file_path(case_id, prefix):
    """
    Builds a path like: data/{case_id}/{prefix}_{case_id}.json
    Example: prefix='doc' → data/123456/doc_123456.json
    """
    case_dir = get_case_dir(case_id)
    filename = f"{prefix}_{case_id}.json"
    return os.path.join(case_dir, filename)


def read_cached_json(path):
    """
    Reads a JSON file from disk if it exists.
    """
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log_and_print(f"⚠️ Failed reading {path}: {e}", "warning")
    return None


def write_json_to_cache(path, data):
    """
    Writes JSON data to disk.
    """
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log_and_print(f"💾 Saved JSON to {path}")
    except Exception as e:
        log_and_print(f"❌ Failed to save JSON to {path}: {e}", "error")


def fetch_case_details(case_id):
    """
    Calls the API to get full case details, saves as case_{case_id}.json
    """
    url = f"{BASE_URL}/api/Case/GetCase?CaseId={case_id}"
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Moj-Application-Id": MOJ_APP_ID,
        "Accept": "application/json"
    }

    try:
        log_and_print(f"🌐 Fetching case JSON from API for CaseId {case_id}...")
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        case_json = response.json()

        path = get_tab_file_path(case_id, "case")
        write_json_to_cache(path, case_json)

        return case_json
    except Exception as e:
        log_and_print(f"❌ Failed to fetch case {case_id}: {e}", "error")
        return None


def get_case_data(case_id, force_refresh=False):
    """
    Returns case JSON: from cache or by calling API (if not cached or forced).
    """
    path = get_tab_file_path(case_id, "case")
    if not force_refresh:
        cached = read_cached_json(path)
        if cached:
            return cached
    return fetch_case_details(case_id)
