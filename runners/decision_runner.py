from configs.config_loader import load_tab_config
from apis.client_api import fetch_case_details
from apis.sql_client import fetch_menora_decision_data
from utils.json_parser import extract_decision_data_from_json
from utils.logging_utils import log_and_print
from utils.fetcher import get_case_data
from dateutil.parser import parse
from tabulate import tabulate
from collections import defaultdict
from dotenv import load_dotenv
import pandas as pd
import os
from datetime import datetime

from datetime import datetime

from datetime import datetime
import os
import json
from dotenv import load_dotenv
from dateutil.parser import parse
from collections import defaultdict

from dateutil.parser import parse as dateutil_parse
import pandas as pd
import os
from dotenv import load_dotenv
from utils.logging_utils import log_and_print
from configs.config_loader import load_tab_config
from utils.json_parser import extract_decision_data_from_json, is_case_type_supported

from apis.sql_client import fetch_menora_decision_data

def safe_parse_date(val):
    try:
        return dateutil_parse(str(val)).replace(tzinfo=None)
    except Exception:
        return pd.NaT

def run_decision_comparison(case_id: int, appeal_number: int, conn, tab_config=None):
    log_and_print("\n📂 Running decision comparison...", "info")

    if tab_config is None:
        tab_config = load_tab_config("החלטות")

    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    try:
        menora_df = fetch_menora_decision_data(appeal_number, conn)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        if "Moj_ID" in menora_df.columns:
            menora_df.rename(columns={"Moj_ID": "mojId"}, inplace=True)
        log_and_print(f"✅ Retrieved {len(menora_df)} decisions from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    if not is_case_type_supported(case_id):
        return {
            "decision": {
                "status_tab": "skip",
                "reason": "Not relevant case type"
            }
        }

    json_data = get_case_data(case_id)
    json_df = pd.DataFrame()

    if json_data:
        try:
            log_and_print("🔍 Extracting decision data from JSON...", "debug")
            json_df = extract_decision_data_from_json(json_data)
            json_df.columns = [col.strip() for col in json_df.columns]
            for ui_field in field_map:
                for col in json_df.columns:
                    if col.strip().lower() == ui_field.strip().lower() and not col.endswith("_json"):
                        json_df.rename(columns={col: f"{ui_field}_json"}, inplace=True)
            if "decisionDate" in json_df.columns:
                json_df.rename(columns={"decisionDate": "decisionDate_json"}, inplace=True)
            json_df["decisionDate_json"] = json_df["decisionDate_json"].apply(safe_parse_date)
            log_and_print(str(json_df.get("decisionDate_json", "❌ Not found")), "debug")
        except Exception as e:
            log_and_print(f"❌ Failed to parse JSON decision data: {e}", "error")

    menora_keys = set(menora_df["mojId"].dropna().astype(str)) if "mojId" in menora_df.columns else set()
    json_keys = set(json_df["mojId"].dropna().astype(str)) if "mojId" in json_df.columns else set()

    missing_json_dates = sorted(list(menora_keys - json_keys))
    missing_menora_dates = sorted(list(json_keys - menora_keys))

    mismatched_fields = []
    if not json_df.empty and not menora_df.empty:
        log_and_print("🔍 Performing field-level comparison...", "debug")
        comparison_results = compare_decision_data(json_df, menora_df, field_map)
        for row in comparison_results:
            if row.get("Match") == "✗":
                mismatched_fields.append({
                    "Status_Date": row.get("mojId"),
                    "Field": row.get("Field"),
                    "Menora": row.get("Menora Value"),
                    "JSON": row.get("JSON Value")
                })

    load_dotenv()
    raw_cutoff = os.getenv("CUTOFF")
    if not raw_cutoff or len(raw_cutoff) != 6 or not raw_cutoff.isdigit():
        raise ValueError("❌ Invalid or missing CUTOFF in environment. Expected format: ddmmyy (e.g., 250421)")

    formatted_cutoff = f"20{raw_cutoff[4:6]}-{raw_cutoff[2:4]}-{raw_cutoff[0:2]}T00:00:00"
    CUTOFF_DATETIME = safe_parse_date(formatted_cutoff)
    log_and_print(f"🔍 CUTOFF datetime: {CUTOFF_DATETIME}", level="debug")

    filtered_missing_json = []
    for moj in missing_json_dates:
        date_row = menora_df[menora_df["mojId"] == moj]
        if not date_row.empty:
            dec_date = safe_parse_date(date_row.iloc[0].get("Decision_Date"))
            if dec_date and dec_date <= CUTOFF_DATETIME:
                filtered_missing_json.append(moj)

    filtered_missing_menora = []
    for moj in missing_menora_dates:
        date_row = json_df[json_df["mojId"] == moj]
        if not date_row.empty:
            dec_date = safe_parse_date(date_row.iloc[0].get("decisionDate_json"))
            if dec_date and dec_date <= CUTOFF_DATETIME:
                filtered_missing_menora.append(moj)

    filtered_mismatched_fields = []
    for row in mismatched_fields:
        date_row = json_df[json_df["mojId"] == row["Status_Date"]]
        if not date_row.empty:
            dec_date = safe_parse_date(date_row.iloc[0].get("decisionDate_json"))
            if dec_date and dec_date <= CUTOFF_DATETIME:
                filtered_mismatched_fields.append(row)

    if not filtered_missing_json and not filtered_missing_menora and not filtered_mismatched_fields:
        status_tab = "pass"
        log_and_print("🔹 החלטות - PASS", "info", is_hebrew=True)
    else:
        status_tab = "fail"
        log_and_print("❌ החלטות - FAIL", "warning", is_hebrew=True)

    return {
        "decision": {
            "status_tab": status_tab,
            "missing_json_dates": filtered_missing_json,
            "missing_menora_dates": filtered_missing_menora,
            "mismatched_fields": filtered_mismatched_fields
        }
    }


def values_match(field, menora_value, json_value):
    menora_str = str(menora_value).strip()
    json_str = str(json_value).strip()

    if field in ["document_Type_Id", "Source_Type"]:
        try:
            return int(menora_value) == int(json_value)
        except (TypeError, ValueError):
            return False

    if field.lower().endswith("date"):
        try:
            return parse(menora_str).replace(tzinfo=None) == parse(json_str).replace(tzinfo=None)
        except Exception:
            return False

    return menora_str.lower() == json_str.lower()

def compare_decision_data(json_df, menora_df, field_map):
    menora_df = menora_df.rename(columns={"Moj_ID": "mojId"})
    json_df = json_df.rename(columns={"moj_id": "mojId"})

    menora_df = menora_df.rename(columns={k: f"{k}_menora" for k in field_map.keys()})
    json_df = json_df.rename(columns={v: f"{v}_json" for v in field_map.values()})

    # print("📋 Menora columns:", menora_df.columns.tolist())
    # print("📋 JSON columns:", json_df.columns.tolist())

    merged = pd.merge(menora_df, json_df, on="mojId", how="inner")

    results = []
    for _, row in merged.iterrows():
        for menora_field, json_field in field_map.items():
            left = row.get(f"{menora_field}_menora", "⛔")
            right = row.get(f"{json_field}_json", "⛔")
            match = values_match(menora_field, left, right)
            results.append({
                "mojId": row["mojId"],
                "Field": menora_field,
                "Menora Value": left,
                "JSON Value": right,
                "Match": "✓" if match else "✗"
            })

    mismatches = defaultdict(list)
    for row in results:
        if row["Match"] == "✗":
            mismatches[row["mojId"]].append({
                "Field": row["Field"],
                "Menora Value": row["Menora Value"],
                "JSON Value": row["JSON Value"]
            })

    if not results:
        log_and_print("⚠️ No decision comparison results found.")
    elif mismatches:
        log_and_print(f"❌ Found mismatches in {len(mismatches)} decisions.")
        for moj_id, rows in mismatches.items():
            log_and_print(f"\n🔎 Mismatched Fields for mojId {moj_id}:")
            log_and_print(tabulate(rows, headers="keys", tablefmt="grid", showindex=False))
    else:
        log_and_print("✅ All matched decision fields are identical.")

    return results

def compare_decision_counts(json_df, menora_df):
    if "Moj_ID" in menora_df.columns:
        menora_df = menora_df.rename(columns={"Moj_ID": "mojId"})

    json_ids = set(json_df["mojId"])
    menora_ids = set(menora_df["mojId"])

    menora_only = menora_ids - json_ids
    json_only = json_ids - menora_ids

    results = []

    if menora_only:
        results.append({
            "Type": "Missing in JSON",
            "Count": len(menora_only),
            "mojIds": ", ".join(sorted(str(i) for i in menora_only if i is not None))
        })

    if json_only:
        results.append({
            "Type": "Missing in Menora",
            "Count": len(json_only),
            "mojIds": ", ".join(sorted(str(i) for i in json_only if i is not None)) or "(no mojIds)"
        })

    if not results:
        results.append({
            "Type": "✅ Count Match",
            "Count": len(json_ids),
            "mojIds": "-"
        })

    return results
