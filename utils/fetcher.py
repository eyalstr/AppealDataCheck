import os
import json
import requests
from dotenv import load_dotenv
from utils.logging_utils import log_and_print

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
        #log_and_print(f"cached=={cached}")
        if cached:
            log_and_print(f"*******cached******")
            return cached
    return fetch_case_details(case_id)


def fetch_role_contacts(role_ids: list, case_id=None, force_refresh=False) -> dict:
    """
    Fetch role contact data, using cache if available unless force_refresh=True.
    Saves as 'role_{case_id}.json' under data/{case_id}/
    """
    if not role_ids:
        return {}

    #from fetcher import get_tab_file_path, read_cached_json, write_json_to_cache

    if case_id is None:
        case_id = "general_roles"

    path = get_tab_file_path(case_id, "role")

    if not force_refresh:
        cached = read_cached_json(path)
        if cached:
            log_and_print(f"*******cached role contacts******")
            return cached

    base_url = "https://bo-contacts-int.prod.k8s.justice.gov.il/api/RoleInCorporation"
    params = "&".join(f"RoleInCorporationIds={rid}" for rid in role_ids)
    url = f"{base_url}?{params}"

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Accept": "application/json",
        "Moj-Application-Id": MOJ_APP_ID
    }

    try:
        log_and_print(f"🌐 Fetching role contacts from API for {len(role_ids)} roles...")
        response = requests.get(url, headers=headers, verify=False)
        log_and_print(f"🔎 Contact API response status: {response.status_code}", "info")

        if response.status_code != 200:
            log_and_print(f"❌ Failed to fetch contact data. Status: {response.status_code}", "error")
            return {}

        role_json = response.json()
        write_json_to_cache(path, role_json)

        return role_json

    except Exception as e:
        log_and_print(f"❌ Exception occurred while fetching contact data: {e}", "error")
        return {}


def fetch_case_discussions(case_id: int, force_refresh: bool = False) -> dict:
    """
    Fetches discussions for a case, with cache support.
    Saves to: data/{case_id}/dist_{case_id}.json
    """
    cache_path = get_tab_file_path(case_id, "disc")

    if not force_refresh:
        cached = read_cached_json(cache_path)
        if cached:
            log_and_print(f"*******cached discussions******")
            return cached

    url = f"https://bo-discussions-int.prod.k8s.justice.gov.il/api/DiscussionsBo/All/{case_id}"

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Accept": "application/json",
        "Moj-Application-Id": MOJ_APP_ID
    }

    try:
        log_and_print(f"🌐 Fetching discussion JSON from API for CaseId {case_id}...")
        response = requests.get(url, headers=headers, verify=False)
        log_and_print(f"🔎 Discussion API response status: {response.status_code}", "info")

        if response.status_code != 200:
            log_and_print(f"❌ Failed to fetch discussions for case {case_id}. Status: {response.status_code}", "error")
            return {}

        discussion_json = response.json()
        write_json_to_cache(cache_path, discussion_json)

        return discussion_json

    except Exception as e:
        log_and_print(f"❌ Exception occurred while fetching discussions: {e}", "error")
        return {}
    
def fetch_distribution_data(case_id: int) -> dict:
    """
    Fetches and caches distribution data for a given case ID.
    """
    from utils.fetcher import get_tab_file_path, read_cached_json, write_json_to_cache

    cache_path = get_tab_file_path(case_id, "dist")

    # Try loading from cache first
    cached_data = read_cached_json(cache_path)
    if cached_data:
        log_and_print(f"📁 Loaded distribution data from cache: {cache_path}", "debug")
        return cached_data

    # Fetch from API if no cache
    url = f"https://bo-distribution-int.prod.k8s.justice.gov.il/api/Distribution/GetDistributionsByCaseOrRequest?CaseId={case_id}"
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Accept": "application/json",
        "Moj-Application-Id": MOJ_APP_ID
    }

    try:
        log_and_print(f"🌐 Fetching distribution data from API for CaseId {case_id}...", "info")
        response = requests.get(url, headers=headers, verify=False)
        log_and_print(f"🔎 Distribution API response status: {response.status_code}", "info")

        if response.status_code == 200:
            json_data = response.json()
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            write_json_to_cache(cache_path, json_data)
            log_and_print(f"💾 Cached distribution data to: {cache_path}", "debug")
            return json_data
        else:
            log_and_print(f"❌ Failed to fetch distribution data for case {case_id}. Status: {response.status_code}", "error")
            return {}

    except Exception as e:
        log_and_print(f"❌ Exception occurred while fetching distribution data: {e}", "error")
        return {}
