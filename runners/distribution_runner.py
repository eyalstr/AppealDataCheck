import pandas as pd
from apis.sql_client import fetch_menora_distributions
from utils.fetcher import fetch_distribution_data
from utils.logging_utils import log_and_print
from tabulate import tabulate
from utils.logging_utils import normalize_hebrew
from collections import defaultdict
from configs.config_loader import load_tab_config

def run_distribution_comparison(case_id, appeal_number, conn,tab_config=None):
    log_and_print("\n\U0001F4C2 Running distribution comparison...", "info")

    if tab_config is None:
        tab_config = load_tab_config("הפצות")
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_distributions(appeal_number,conn)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()

        menora_df["SendDate"] = pd.to_datetime(menora_df["SendDate"], errors="coerce")
        menora_df["SendDate"] = menora_df["SendDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

        log_and_print(f"✅ Retrieved {len(menora_df)} distributions from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        return {}

    # Step 2: Fetch JSON API data
    try:
        json_data = fetch_distribution_data(case_id)
        if not json_data:
            raise ValueError("Empty or invalid JSON returned from API")

        json_df = pd.json_normalize(json_data)
        json_df.rename(columns={
            "createDate": "SendDate",
            "subject": "SendSubject"
        }, inplace=True)

        for ui_field in field_map.keys():
            if ui_field in json_df.columns and not ui_field.endswith("_json"):
                json_df.rename(columns={ui_field: f"{ui_field}_json"}, inplace=True)

        json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()

        date_col = "SendDate_json" if "SendDate_json" in json_df.columns else "SendDate"
        json_df[date_col] = pd.to_datetime(json_df[date_col], errors="coerce")
        json_df[date_col] = json_df[date_col].dt.strftime("%Y-%m-%d %H:%M:%S")

        log_and_print(f"✅ Extracted {len(json_df)} distributions from API for case {case_id}", "success")
    except Exception as e:
        log_and_print(f"❌ Failed to fetch or parse distribution data: {e}", "error")
        return {}

    # Step 3: Compare
    try:
        json_send_date_col = "SendDate_json" if "SendDate_json" in json_df.columns else "SendDate"
        json_send_to_col = "sendTo_json" if "sendTo_json" in json_df.columns else "sendTo"

        menora_df["SendKey"] = menora_df["SendDate"].astype(str) + "|" + menora_df["SendTo"].astype(str).str.strip()
        json_df["SendKey"] = json_df[json_send_date_col].astype(str) + "|" + json_df[json_send_to_col].astype(str).str.strip()

        menora_keys = set(menora_df["SendKey"].dropna().unique())
        json_keys = set(json_df["SendKey"].dropna().unique())

        missing_in_json = sorted(list(menora_keys - json_keys))
        missing_in_menora = sorted(list(json_keys - menora_keys))

        mismatched_fields = []

        status_tab = "pass" if not missing_in_json and not missing_in_menora and not mismatched_fields else "fail"

        return {
            "distribution": {
                "status_tab": status_tab,
                "missing_json_dates": missing_in_json,
                "missing_menora_dates": missing_in_menora,
                "mismatched_fields": mismatched_fields
            }
        }

    except Exception as e:
        log_and_print(f"❌ Error during distribution comparison logic: {e}", "error")
        return {"distribution": {"status_tab": "fail", "reason": str(e)}}
