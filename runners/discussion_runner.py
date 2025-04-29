import pandas as pd
from apis.sql_client import fetch_menora_discussion_data
from utils.logging_utils import log_and_print
from jsonpath_ng import parse
from configs.config_loader import load_tab_config
from dateutil.parser import parse
from datetime import datetime
from utils.fetcher import fetch_case_discussions
import os
import json
from dotenv import load_dotenv


def run_discussion_comparison(case_id, appeal_number, conn, tab_config=None):
    tab_key = "discussion"
    tab_label = "דיונים"
    log_and_print(f"\n📂 Running {tab_label} comparison...", "info")
    if tab_config is None:
        tab_config = load_tab_config(tab_label)
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    try:
        menora_df = fetch_menora_discussion_data(appeal_number, conn)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} discussions from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    # --- Load discussion tab JSON from cache or fetch and save ---
    cache_path = f"data/{case_id}/disc_{case_id}.json"
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        log_and_print(f"📁 Loaded discussion data from cache: {cache_path}", "debug")
    else:
        json_data = fetch_case_discussions(case_id,True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        log_and_print(f"💾 Cached discussion data to: {cache_path}", "debug")

    json_df = pd.DataFrame()
    if json_data:
        try:
            if isinstance(json_data, list):
                matches = json_data
            else:
                json_path_expr = parse("$[*]")
                matches = [match.value for match in json_path_expr.find(json_data)]

            json_df = pd.json_normalize(matches)
            log_and_print(f"✅ Extracted {len(json_df)} discussions from API for case {case_id}", "success")
        except Exception as e:
            log_and_print(f"❌ Failed to parse JSON discussion data: {e}", "error")
    else:
        log_and_print(f"⚠️ No discussion JSON data found for case_id {case_id}", "warning")

    json_df = json_df.rename(columns=lambda x: x.strip())
    json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()

    menora_ids = set(menora_df["Moj_ID"].dropna().astype(str).str.strip()) if "Moj_ID" in menora_df.columns else set()
    json_ids = set(json_df["protocolDocMojId"].dropna().astype(str).str.strip()) if "protocolDocMojId" in json_df.columns else set()

    missing_json_dates = sorted(list(menora_ids - json_ids))
    missing_menora_dates = sorted(list(json_ids - menora_ids))
    mismatched_fields = []

    status_tab = "pass" if not missing_json_dates and not missing_menora_dates and not mismatched_fields else "fail"
    if status_tab == "pass":
        log_and_print(f"🟡 {tab_label} - PASS", "info", is_hebrew=True)
    else:
        log_and_print(f"❌ {tab_label} - FAIL", "warning", is_hebrew=True)

    return {
        tab_key: {
            "status_tab": status_tab,
            "missing_json_dates": missing_json_dates,
            "missing_menora_dates": missing_menora_dates,
            "mismatched_fields": mismatched_fields
        }
    }
