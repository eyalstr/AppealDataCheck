import pandas as pd
from sql_client import fetch_menora_discussion_data, fetch_menora_log_requests
from client_api import fetch_case_discussions, fetch_case_details
from logging_utils import log_and_print
from jsonpath_ng import parse
from json_parser import extract_request_logs_from_json
from config_loader import load_tab_config
from tabulate import tabulate
from dateutil.parser import parse


from dateutil.parser import parse

def run_request_log_comparison(case_id, appeal_number):
    #log_and_print("\n\ud83d\udcc2 Running יומן תיק comparison...", "info")

    tab_config = load_tab_config("יומן תיק")
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    try:
        menora_df = fetch_menora_log_requests(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} request log entries from Menora for appeal {appeal_number}", "success")
        log_and_print(f"📋 Menora data preview:\n{menora_df.head(5)}", "info")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    full_json = fetch_case_details(case_id)
    json_df = extract_request_logs_from_json(full_json)

    if "createActionDate" in json_df.columns:
        json_df.rename(columns={"createActionDate": "Status_Date"}, inplace=True)

    for ui_field, _ in field_map.items():
        if ui_field in json_df.columns and not ui_field.endswith("_json"):
            json_df.rename(columns={ui_field: f"{ui_field}_json"}, inplace=True)

    if "Status_Date" in json_df.columns:
        json_df.rename(columns={"Status_Date": "Status_Date_json"}, inplace=True)

    json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()

    def safe_parse_datetime(value):
        try:
            return parse(value).strftime("%m-%d %H:%M:%S")
        except Exception:
            return None

    for df, label in [(menora_df, "Menora"), (json_df, "JSON")]:
        col_name = "Status_Date" if label == "Menora" else "Status_Date_json"
        if col_name in df.columns:
            try:
                dt_series = df[col_name].astype(str).apply(safe_parse_datetime)
                df[col_name] = dt_series
                log_and_print(f"🕒 Normalized '{col_name}' in {label} to MM-DD HH:MM:SS.", "debug")
            except Exception as e:
                log_and_print(f"❌ Failed to normalize '{col_name}' in {label}: {e}", "error")

    field_mismatches = 0
    try:
        merged = pd.merge(
            menora_df[["Status_Date", "Action_Description"]],
            json_df[["Status_Date_json", "Action_Description_json"]],
            left_on="Status_Date",
            right_on="Status_Date_json",
            how="outer",
            suffixes=("_menora", "_json"),
            indicator=True
        )

        for _, row in merged.iterrows():
            if row['_merge'] == 'both':
                menora_desc = str(row.get('Action_Description') or '').strip()
                json_desc = str(row.get('Action_Description_json') or '').strip()
                if menora_desc != json_desc:
                    log_and_print(f"🔍 Status_Date {row['Status_Date']} mismatched Action_Description:", "warning")
                    log_and_print(f"    Menora: {menora_desc}", "warning")
                    log_and_print(f"    JSON:   {json_desc}", "warning")
                    field_mismatches += 1
    except Exception as e:
        log_and_print(f"❌ Comparison failed: {e}", "error")

    json_keys = json_df["Status_Date_json"].dropna().unique() if "Status_Date_json" in json_df.columns else []
    menora_keys = menora_df["Status_Date"].dropna().unique() if "Status_Date" in menora_df.columns else []
    missing_in_json = len([ts for ts in menora_keys if ts not in json_keys])
    missing_in_menora = len([ts for ts in json_keys if ts not in menora_keys])
    matched = len(set(json_keys) & set(menora_keys)) - field_mismatches

    summary = {
    "case_id": case_id,
    "Menora Request Logs": len(menora_df) if not menora_df.empty else 0,
    "JSON Request Logs": len(json_df) if not json_df.empty else 0,
    "Missing in JSON": missing_in_json,
    "Missing in Menora": missing_in_menora,
    "Field Mismatches": field_mismatches,
    "Fully Matched": matched
}


    all_clear = (summary['Missing in JSON'] == 0 and summary['Missing in Menora'] == 0 and summary['Field Mismatches'] == 0)
    if all_clear:
        log_and_print(f"🟡 יומן תיק - PASS", "info")
    else:
        log_and_print(f"\n💚 Test Result Summary for Case ID {case_id} [יומן תיק]", "info")
        log_and_print(f"✅ {summary['Missing in Menora']} log entry(ies) missing in Menora.", "success")
        log_and_print(f"✅ {summary['Missing in JSON']} log entry(ies) missing in JSON.", "success")
        log_and_print(f"✅ {summary['Field Mismatches']} log entry(ies) with field mismatch(es).", "success")
        log_and_print(f"✅ {summary['Fully Matched']} log entry(ies) fully matched.", "success")

    return {"Request Log": summary}  # Wrapped with tab label


def requestlog_values_match(field, val1, val2):
    # Normalize both values
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False

    # For datetime, compare without time zone differences
    if isinstance(val1, pd.Timestamp) and isinstance(val2, pd.Timestamp):
        return val1.normalize() == val2.normalize()

    # Trim and lower strings
    if isinstance(val1, str) and isinstance(val2, str):
        return val1.strip() == val2.strip()

    return val1 == val2

# In requestlog_comparator.py

def compare_request_logs(json_df, menora_df, field_map):
    import pandas as pd
    from collections import defaultdict

    json_df = json_df.copy()
    menora_df = menora_df.copy()

    # Rename for comparison
    json_df = json_df.rename(columns={v: f"{v}_json" for v in field_map.values()})
    menora_df = menora_df.rename(columns={k: f"{k}_menora" for k in field_map.keys()})

    # Round datetime columns to the nearest second and sort for asof
    json_df["Status_Date_json"] = pd.to_datetime(json_df["Status_Date_json"]).dt.round("s")
    menora_df["Status_Date_menora"] = pd.to_datetime(menora_df["Status_Date_menora"]).dt.round("s")

    json_df = json_df.sort_values("Status_Date_json")
    menora_df = menora_df.sort_values("Status_Date_menora")

    # Merge using tolerance of 1 second
    merged = pd.merge_asof(
        menora_df,
        json_df,
        left_on="Status_Date_menora",
        right_on="Status_Date_json",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=1)
    )

    results = []
    mismatches = defaultdict(list)

    for _, row in merged.iterrows():
        row_key = row.get("Status_Date_menora")
        for menora_field, json_field in field_map.items():
            left = row.get(f"{menora_field}_menora", "⛔")
            right = row.get(f"{json_field}_json", "⛔")

            if pd.isna(left):
                left = "⛔"
            if pd.isna(right):
                right = "⛔"

            match = requestlog_values_match(menora_field, left, right)
            results.append({
                "Status_Date": row_key,
                "Field": menora_field,
                "Menora Value": left,
                "JSON Value": right,
                "Match": "✓" if match else "✗"
            })

            if not match:
                mismatches[row_key].append(menora_field)

    if mismatches:
        log_and_print(f"❌ Found mismatches in {len(mismatches)} request log entries.", "warning")
        for status_date, fields in mismatches.items():
            log_and_print(f"  🔍 Status_Date {status_date} mismatched fields: {', '.join(fields)}", "info")
    else:
        log_and_print("✅ All matched request log fields are identical.", "success")

    return results
