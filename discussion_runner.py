import pandas as pd
from sql_client import fetch_menora_discussion_data
from client_api import fetch_case_discussions
from logging_utils import log_and_print
from jsonpath_ng import parse
from config_loader import load_tab_config
from dateutil.parser import parse


def run_discussion_comparison(case_id, appeal_number):
    tab_key = "discussion"
    tab_label = "דיונים"
    log_and_print(f"\n📂 Running {tab_label} comparison...", "info")

    tab_config = load_tab_config(tab_label)
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    try:
        menora_df = fetch_menora_discussion_data(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} discussions from Menora for appeal {appeal_number}", "success")
        #log_and_print(f"📋 Menora columns: {list(menora_df.columns)}", "debug")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    json_data = fetch_case_discussions(case_id)
    json_df = pd.DataFrame()
    if json_data:
        try:
            if isinstance(json_data, list):
                matches = json_data
            else:
                json_path_expr = parse("$[*]")
                matches = [match.value for match in json_path_expr.find(json_data)]

            json_df = pd.json_normalize(matches)
            #if not json_df.empty:
                #log_and_print(f"📋 Extracted discussion DataFrame preview:\n{json_df[['protocolDocMojId', 'startTime', 'endTime']].head(3)}", "info")
                #log_and_print(f"🗞 protocolDocMojId from JSON:\n{json_df['protocolDocMojId'].dropna().tolist()}", "debug")
            log_and_print(f"✅ Extracted {len(json_df)} discussions from API for case {case_id}", "success")
        except Exception as e:
            log_and_print(f"❌ Failed to parse JSON discussion data: {e}", "error")
    else:
        log_and_print(f"⚠️ No discussion JSON data found for case_id {case_id}", "warning")

    json_df = json_df.rename(columns=lambda x: x.strip())
    json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()

    menora_ids = set(menora_df["Moj_ID"].dropna().astype(str).str.strip()) if "Moj_ID" in menora_df.columns else set()
    json_ids = set(json_df["protocolDocMojId"].dropna().astype(str).str.strip()) if "protocolDocMojId" in json_df.columns else set()

    #log_and_print(f"🗟 mojId from Menora:\n{sorted(menora_ids)}", "debug")
    #log_and_print(f"🗟 protocolDocMojId from JSON:\n{sorted(json_ids)}", "debug")

    missing_json_dates = sorted(list(menora_ids - json_ids))
    missing_menora_dates = sorted(list(json_ids - menora_ids))
    mismatched_fields = []

    status_tab = "pass" if not missing_json_dates and not missing_menora_dates and not mismatched_fields else "fail"
    if status_tab == "pass":
        log_and_print(f"🟡 {tab_label} - PASS", "info")
    else:
        log_and_print(f"❌ {tab_label} - FAIL", "warning")

    return {
        tab_key: {
            "status_tab": status_tab,
            "missing_json_dates": missing_json_dates,
            "missing_menora_dates": missing_menora_dates,
            "mismatched_fields": mismatched_fields
        }
    }