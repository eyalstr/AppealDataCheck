import pandas as pd
from sql_client import fetch_menora_discussion_data, fetch_menora_log_requests
from client_api import fetch_case_discussions, fetch_case_details
from logging_utils import log_and_print
from jsonpath_ng import parse
from json_parser import extract_request_logs_from_json

def run_request_log_comparison(case_id, appeal_number):
    log_and_print("\n📂 Running request log comparison...", "info")

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_log_requests(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        log_and_print(f"✅ Retrieved {len(menora_df)} request log entries from Menora for appeal {appeal_number}", "success")
        log_and_print(f"📋 Menora data preview:\n{menora_df.head(5)}", "info")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    # Step 2: Fetch JSON API data
    full_json = fetch_case_details(case_id)
    json_df = extract_request_logs_from_json(full_json)

    # Step 3: Normalize datetime columns and ensure consistent types
    for df, label in [(menora_df, "Menora"), (json_df, "JSON")]:
        if "Status_Date" in df.columns:
            df["Status_Date"] = (
                pd.to_datetime(df["Status_Date"], errors="coerce")
                .dt.round("s")
                .dt.tz_localize(None)
            )
            df["Status_Date"] = df["Status_Date"].astype(str)
            log_and_print(f"🕒 Normalized 'Status_Date' in {label} to string timestamps.", "debug")

    # Step 4: Compare by Status_Date key and Action_Description
    field_mismatches = 0
    try:
        merged = pd.merge(
            menora_df[["Status_Date", "Action_Description"]],
            json_df[["Status_Date", "Action_Description"]],
            on="Status_Date",
            how="outer",
            suffixes=("_menora", "_json"),
            indicator=True
        )

        for _, row in merged.iterrows():
            if row['_merge'] == 'both':
                menora_desc = str(row.get('Action_Description_menora') or '').strip()
                json_desc = str(row.get('Action_Description_json') or '').strip()
                if menora_desc != json_desc:
                    log_and_print(f"🔍 Status_Date {row['Status_Date']} mismatched Action_Description:", "warning")
                    log_and_print(f"    Menora: {menora_desc}", "warning")
                    log_and_print(f"    JSON:   {json_desc}", "warning")
                    field_mismatches += 1
    except Exception as e:
        log_and_print(f"❌ Comparison failed: {e}", "error")

    json_keys = json_df["Status_Date"].dropna().unique()
    menora_keys = menora_df["Status_Date"].dropna().unique()
    missing_in_json = len([ts for ts in menora_keys if ts not in json_keys])
    missing_in_menora = len([ts for ts in json_keys if ts not in menora_keys])
    matched = len(json_keys) - missing_in_menora

    # Final debug print for structure validation
    log_and_print(f"📋 Menora columns: {menora_df.columns.tolist()}", "info")
    log_and_print(f"📋 JSON columns: {json_df.columns.tolist()}", "info")

    summary = {
        "Case ID": case_id,
        "Menora Request Logs": len(menora_df),
        "JSON Request Logs": len(json_df),
        "Missing in JSON": missing_in_json,
        "Missing in Menora": missing_in_menora,
        "Field Mismatches": field_mismatches,
        "Fully Matched": matched - field_mismatches
    }

    log_and_print(f"\n💚 Test Result Summary for Case ID {case_id} [Request Log]", "info")
    log_and_print(f"✅ {summary['Missing in Menora']} log entry(ies) missing in Menora.", "success")
    log_and_print(f"✅ {summary['Missing in JSON']} log entry(ies) missing in JSON.", "success")
    log_and_print(f"✅ {summary['Field Mismatches']} log entry(ies) with field mismatch(es).", "success")
    log_and_print(f"✅ {summary['Fully Matched']} log entry(ies) fully matched.", "success")

    return summary
