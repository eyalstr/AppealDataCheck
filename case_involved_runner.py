# case_involved_runner.py
import pandas as pd
from sql_client import fetch_menora_case_involved_data
from client_api import fetch_case_details
from logging_utils import log_and_print
import json

def run_case_involved_comparison(case_id, appeal_number):
    log_and_print("\n📂 Running case involved comparison...", "info")

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_case_involved_data(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df["PrivateCompanyNumber"] = menora_df["PrivateCompanyNumber"].astype(str)
        menora_df["Main_Id_Number"] = menora_df["Main_Id_Number"].astype(str)
        log_and_print(f"✅ Retrieved {len(menora_df)} case involved entries from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    # Step 2: Fetch JSON API data
    json_data = fetch_case_details(case_id)
    json_df = pd.DataFrame()

    # Log full JSON response
    # log_and_print("🔎 Full JSON response from API:", "info")
    # log_and_print(json.dumps(json_data, indent=2, ensure_ascii=False), "info")

    try:
        case_involveds = json_data.get("caseInvolveds", [])
        expanded_records = []

        for involved in case_involveds:
            base_data = {
                "CompanyName": involved.get("caseInvolvedName"),
                "PrivateCompanyNumber": str(involved.get("caseInvolvedIdentifyId")),
                "CompanyType": involved.get("caseInvolvedIdentifyType"),
            }

            for rep in involved.get("representors", []):
                if rep.get("appointmentEndDate") is None:
                    record = base_data.copy()
                    record.update({
                        "Main_Id_Number": str(rep.get("caseInvolvedIdentifyId")),
                        "orer": rep.get("caseInvolvedName"),
                        "Representor_Type_Id": rep.get("representorTypeId")
                    })
                    expanded_records.append(record)

        json_df = pd.DataFrame(expanded_records)

        for col in ["PrivateCompanyNumber", "Main_Id_Number"]:
            if col in json_df.columns:
                json_df[col] = json_df[col].astype(str)

        log_and_print(f"📋 Extracted case involved DataFrame preview:\n{json_df.head(3)}", "info",is_hebrew=True)
        log_and_print(f"✅ Extracted {len(json_df)} case involved entries from API for case {case_id}", "success")

    except Exception as e:
        log_and_print(f"❌ Failed to parse JSON case involved data: {e}", "error")
        json_df = pd.DataFrame()

    # Step 3: Count comparisons
    menora_count = len(menora_df)
    json_count = len(json_df)

    missing_in_json = 0
    missing_in_menora = 0
    mismatched_fields = 0
    fully_matched = 0

    if not json_df.empty and not menora_df.empty:
        try:
            merged = pd.merge(json_df, menora_df, how="outer", indicator=True)
            missing_in_json = merged[merged['_merge'] == 'right_only'].shape[0]
            missing_in_menora = merged[merged['_merge'] == 'left_only'].shape[0]
            fully_matched = merged[merged['_merge'] == 'both'].shape[0]
        except Exception as e:
            log_and_print(f"❌ Merge failed: {e}", "error")
            missing_in_json = menora_count
            missing_in_menora = json_count
    else:
        if json_df.empty:
            missing_in_json = menora_count
        if menora_df.empty:
            missing_in_menora = json_count

    summary = {
        "Case ID": case_id,
        "Menora Case Involved": menora_count,
        "JSON Case Involved": json_count,
        "Missing in JSON": max(missing_in_json, 0),
        "Missing in Menora": max(missing_in_menora, 0),
        "Field Mismatches": mismatched_fields,
        "Fully Matched": max(fully_matched, 0)
    }

    log_and_print(f"\n🧪 Test Result Summary for Case ID {case_id} [Case Involved]", "info")
    log_and_print(f"✅ {summary['Missing in Menora']} entry(ies) missing in Menora.", "success")
    log_and_print(f"✅ {summary['Missing in JSON']} entry(ies) missing in JSON.", "success")
    log_and_print(f"✅ {summary['Field Mismatches']} entry(ies) with field mismatch(es).", "success")
    log_and_print(f"✅ {summary['Fully Matched']} entry(ies) fully matched.", "success")

    return summary