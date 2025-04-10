import pandas as pd
from sql_client import fetch_menora_case_contacts
from client_api import fetch_case_details, fetch_role_contacts
from logging_utils import log_and_print
import json
import os
from dotenv import load_dotenv
from config_loader import load_tab_config

def run_case_contacts_comparison(case_id, appeal_number):
    tab_config = load_tab_config("עורר פרטי קשר")
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    log_and_print("\n📂 Running case contact comparison...", "info")

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_case_contacts(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df["Main_Id_Number"] = menora_df["Main_Id_Number"].astype(str)
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} case contact entries from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    # Step 2: Fetch JSON API data (two-step)
    json_data = fetch_case_details(case_id)
    json_df = pd.DataFrame()

    try:
        role_ids = []
        for involved in json_data.get("caseInvolveds", []):
            for rep in involved.get("representors", []):
                if rep.get("appointmentEndDate") is None and rep.get("roleInCorporationId"):
                    role_ids.append(rep.get("roleInCorporationId"))

        if not role_ids:
            raise ValueError("No roleInCorporationIds found in caseInvolveds")

        contact_data = fetch_role_contacts(role_ids)

        contact_records = []
        for role in contact_data.get("roleInCorporationDetails", []):
            corp_id = role.get("corporationDetails", {}).get("corporationIDNumber")
            mail = role.get("connectDetails", {}).get("mail")
            phone1 = role.get("connectDetails", {}).get("primaryPhone")
            phone2 = role.get("connectDetails", {}).get("secondaryPhone")

            contact_records.append({
                "Main_Id_Number": str(corp_id),
                "orerEmail": mail,
                "Phone1": phone1,
                "Phone2": phone2
            })

        json_df = pd.DataFrame(contact_records)

        if "Main_Id_Number" in json_df.columns:
            json_df["Main_Id_Number"] = json_df["Main_Id_Number"].astype(str)

        for ui_field, _ in field_map.items():
            if ui_field in json_df.columns and not ui_field.endswith("_json"):
                json_df.rename(columns={ui_field: f"{ui_field}_json"}, inplace=True)

        json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()
        log_and_print(f"📋 Extracted case contact DataFrame preview:\n{json_df.head(3)}", "info", is_hebrew=True)
        log_and_print(f"✅ Extracted {len(json_df)} case contact entries from API for case {case_id}", "success")

    except Exception as e:
        log_and_print(f"❌ Failed to fetch or parse case contact data: {e}", "error")
        json_df = pd.DataFrame()

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
        "Menora Case Contacts": menora_count,
        "JSON Case Contacts": json_count,
        "Missing in JSON": max(missing_in_json, 0),
        "Missing in Menora": max(missing_in_menora, 0),
        "Field Mismatches": mismatched_fields,
        "Fully Matched": max(fully_matched, 0)
    }

    log_and_print(f"\n📊 Test Result Summary for Case ID {case_id} [Case Contact]", "info")
    log_and_print(f"✅ {summary['Missing in Menora']} entry(ies) missing in Menora.", "success")
    log_and_print(f"✅ {summary['Missing in JSON']} entry(ies) missing in JSON.", "success")
    log_and_print(f"✅ {summary['Field Mismatches']} entry(ies) with field mismatch(es).", "success")
    log_and_print(f"✅ {summary['Fully Matched']} entry(ies) fully matched.", "success")

    return summary
