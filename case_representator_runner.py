# case_involved_runner.py
import pandas as pd
from sql_client import fetch_menora_case_involved_data
from client_api import fetch_case_details
from logging_utils import log_and_print
from config_loader import load_tab_config
import json

def run_representator_comparison(case_id, appeal_number):
    tab_config = load_tab_config("מעורבים בתיק")
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    log_and_print("\n📂 Running case involved comparison...", "info")

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_case_involved_data(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df["PrivateCompanyNumber"] = menora_df["PrivateCompanyNumber"].astype(str)
        menora_df["Main_Id_Number"] = menora_df["Main_Id_Number"].astype(str)
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} case involved entries from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    # Step 2: Fetch JSON API data
    json_data = fetch_case_details(case_id)
    json_df = pd.DataFrame()

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

        for ui_field, _ in field_map.items():
            if ui_field in json_df.columns and not ui_field.endswith("_json"):
                json_df.rename(columns={ui_field: f"{ui_field}_json"}, inplace=True)

        json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()
        log_and_print(f"📋 Extracted case involved DataFrame preview:\n{json_df.head(3)}", "info", is_hebrew=True)
        log_and_print(f"✅ Extracted {len(json_df)} case involved entries from API for case {case_id}", "success")

    except Exception as e:
        log_and_print(f"❌ Failed to parse JSON case involved data: {e}", "error")
        json_df = pd.DataFrame()

    # Step 3: Compare and prepare return-compatible structure
    missing_json_list = []
    missing_menora_list = []
    mismatched_fields = []

    if not json_df.empty and not menora_df.empty:
        try:
            json_df["_source"] = "json"
            menora_df["_source"] = "menora"

            compare_keys = ["PrivateCompanyNumber", "Main_Id_Number"]

            merged = pd.merge(json_df, menora_df, how="outer", on=compare_keys, indicator=True)

            missing_in_json_df = merged[merged['_merge'] == 'right_only']
            missing_in_menora_df = merged[merged['_merge'] == 'left_only']
            fully_matched_df = merged[merged['_merge'] == 'both']

            missing_json_list = missing_in_json_df["Main_Id_Number"].dropna().unique().tolist()
            missing_menora_list = missing_in_menora_df["Main_Id_Number"].dropna().unique().tolist()

            status_tab = "pass" if not missing_json_list and not missing_menora_list else "fail"

        except Exception as e:
            log_and_print(f"❌ Merge failed: {e}", "error")
            missing_json_list = menora_df["Main_Id_Number"].dropna().unique().tolist()
            missing_menora_list = json_df["Main_Id_Number"].dropna().unique().tolist()
            status_tab = "fail"
    else:
        if json_df.empty:
            missing_json_list = menora_df["Main_Id_Number"].dropna().unique().tolist()
        if menora_df.empty:
            missing_menora_list = json_df["Main_Id_Number"].dropna().unique().tolist()
        status_tab = "fail"

    # Final output aligned with dashboard schema
    return {
        str(case_id): {
            "representator_log": {
                "status_tab": status_tab,
                "missing_json_dates": missing_json_list,
                "missing_menora_dates": missing_menora_list,
                "mismatched_fields": mismatched_fields
            }
        }
    }
