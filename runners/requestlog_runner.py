from configs.config_loader import load_tab_config
from apis.client_api import fetch_case_details
from apis.sql_client import fetch_menora_log_requests
from utils.json_parser import extract_request_logs_from_json
from utils.logging_utils import log_and_print
from dateutil.parser import parse
from datetime import datetime
from dotenv import load_dotenv
from utils.fetcher import get_case_data
import pandas as pd
import os


from datetime import datetime
from datetime import datetime

def run_request_log_comparison(case_id, appeal_number, conn, tab_config=None):
    if tab_config is None:
        tab_config = load_tab_config("יומן תיק")

    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    try:
        menora_df = fetch_menora_log_requests(case_id,appeal_number, conn)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    full_json = get_case_data(case_id)
    #full_json =  fetch_case_details(case_id)

    json_df = extract_request_logs_from_json(full_json)
    # ✅ Skip all request logs with remark == "סוג החלטה שינוי מותב"
    if "remark" in json_df.columns:
        before_count = len(json_df)
        json_df = json_df[json_df["remark"] != "סוג החלטה שינוי מותב"].copy()
        skipped = before_count - len(json_df)
        if skipped > 0:
            log_and_print(f"⚠️ Skipped {skipped} JSON request log(s) with remark 'סוג החלטה שינוי מותב'", "debug", is_hebrew=True)

    from utils.json_parser import is_case_type_supported
    if not is_case_type_supported(case_id):
        return {
            "request_log": {
                "status_tab": "skip",
                "reason": "Not relevant case type"
            }
        }

    # Safely rename based on what exists
    # Prefer createActionDate over createLogDate to x`match Menora behavior
    if "createActionDate" in json_df.columns:
        json_df.rename(columns={"createActionDate": "Status_Date"}, inplace=True)
    elif "createLogDate" in json_df.columns:
        json_df.rename(columns={"createLogDate": "Status_Date"}, inplace=True)

    if "Status_Date" in json_df.columns:
        json_df.rename(columns={"Status_Date": "Status_Date_json"}, inplace=True)
    else:
        log_and_print("❌ 'Status_Date' column not found after rename attempt. Check JSON extraction.", "error")

    for _, json_field in field_map.items():
        if json_field in json_df.columns and not json_field.endswith("_json"):
            json_df.rename(columns={json_field: f"{json_field}_json"}, inplace=True)

    json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()

    def safe_parse_datetime(value):
        try:
            return parse(value).replace(tzinfo=None)
        except Exception:
            return None

    load_dotenv()
    raw_cutoff = os.getenv("CUTOFF")
    if not raw_cutoff or len(raw_cutoff) != 6 or not raw_cutoff.isdigit():
        raise ValueError("❌ Invalid or missing CUTOFF in environment. Expected format: ddmmyy (e.g., 250421)")

    formatted_cutoff = f"20{raw_cutoff[4:6]}-{raw_cutoff[2:4]}-{raw_cutoff[0:2]}T00:00:00"
    CUTOFF_DATETIME = parse(formatted_cutoff).replace(tzinfo=None)
    log_and_print(f"🔍 CUTOFF datetime: {CUTOFF_DATETIME}", level="debug")

    for df, label in [(menora_df, "Menora"), (json_df, "JSON")]:
        col_name = "Status_Date" if label == "Menora" else "Status_Date_json"
        dt_col = f"{col_name}_dt"
        if col_name in df.columns:
            try:
                df[dt_col] = df[col_name].astype(str).apply(safe_parse_datetime)
                df[col_name] = df[dt_col].apply(lambda x: x.strftime("%m-%d %H:%M:%S") if x else None)
            except Exception as e:
                log_and_print(f"❌ Failed to normalize '{col_name}' in {label}: {e}", "error")

    menora_keys = set(menora_df["Status_Date"].dropna())
    json_keys = set(json_df["Status_Date_json"].dropna())

    missing_json_dates = sorted(list(menora_keys - json_keys))
    missing_menora_dates = sorted(list(json_keys - menora_keys))

    expected_json_fields = [f"{v}_json" for v in field_map.values()]
    available_json_fields = list(json_df.columns)
    valid_json_fields = [col for col in expected_json_fields if col in available_json_fields]

    if not valid_json_fields:
        log_and_print("🧪 DEBUG: Field rename skipped, injecting expected column for validation.", "debug")
        for _, json_field in field_map.items():
            if json_field not in json_df.columns:
                log_and_print(f"❗ Field '{json_field}' missing in raw JSON. Confirm it is extracted in extract_request_logs_from_json().", "warning")

    log_and_print(f"📋 Menora DataFrame columns to compare: {list(field_map.keys())}", "debug")
    log_and_print(f"📋 JSON DataFrame expected fields: {expected_json_fields}", "debug")
    log_and_print(f"📋 JSON DataFrame available columns: {available_json_fields}", "debug")
    log_and_print(f"📋 JSON fields that will be compared (found): {valid_json_fields}", "debug")

    if len(valid_json_fields) < len(expected_json_fields):
        missing = set(expected_json_fields) - set(valid_json_fields)
        log_and_print(f"⚠️ Warning: Missing expected JSON columns: {missing}", "warning")

    mismatched_fields = []

    if menora_df.empty or json_df.empty:
        log_and_print("⚠️ Empty DataFrame detected, skipping detailed comparison.", "warning")
    else:
        merged = pd.merge(
            menora_df[["Status_Date", "Status_Date_dt", *field_map.keys()]],
            json_df[["Status_Date_json", "Status_Date_json_dt", *valid_json_fields]],
            left_on="Status_Date",
            right_on="Status_Date_json",
            how="inner"
        )

        for _, row in merged.iterrows():
            status_date = row["Status_Date"]
            for menora_col, json_col_base in field_map.items():
                json_col = f"{json_col_base}_json"
                if json_col not in row:
                    continue
                menora_val = str(row.get(menora_col, "")).strip()
                json_val = str(row.get(json_col, "")).strip()
                log_and_print(f"🔍 Comparing for Status_Date {status_date}: {menora_col}='{menora_val}' vs {json_col}='{json_val}'", "debug")
                if (menora_val == "" and json_val.lower() == "none") or (menora_val.lower() == "none" and json_val == ""):
                    continue
                if menora_val != json_val:
                    mismatched_fields.append({
                        "Status_Date": status_date,
                        "Field": menora_col,
                        "Menora": menora_val,
                        "JSON": json_val
                    })

    
    all_dates = (
    [d if isinstance(d, datetime) else parse(d).replace(tzinfo=None) for d in missing_json_dates if d] +
    [d if isinstance(d, datetime) else parse(d).replace(tzinfo=None) for d in missing_menora_dates if d] +
    [parse(row["Status_Date"]).replace(tzinfo=None) for row in mismatched_fields if row.get("Status_Date")]
)

    all_issues_after_cutoff = all(dt > CUTOFF_DATETIME for dt in all_dates if dt)

    if not missing_json_dates and not missing_menora_dates and not mismatched_fields:
        status_tab = "pass"
        log_and_print(f"🟡 יומן תיק - PASS", "info", is_hebrew=True)
    elif all_issues_after_cutoff:
        status_tab = "pass"
        log_and_print(f"✅ Discrepancies occurred after cutoff {CUTOFF_DATETIME.strftime('%d/%m/%Y')}. Ignored by policy.", "info")
    else:
        status_tab = "fail"
        log_and_print(f"❌ יומן תיק - FAIL with mismatches or missing entries.", "warning", is_hebrew=True)

    return {
        "request_log": {
            "status_tab": status_tab,
            "missing_json_dates": missing_json_dates,
            "missing_menora_dates": missing_menora_dates,
            "mismatched_fields": mismatched_fields
        }
    }
