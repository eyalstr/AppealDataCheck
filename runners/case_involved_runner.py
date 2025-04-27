import pandas as pd
from apis.sql_client import fetch_menora_case_contacts
from apis.client_api import fetch_case_details, fetch_connect_contacts
from utils.logging_utils import log_and_print
import json
import os
from dotenv import load_dotenv
from utils.fetcher import get_case_data,fetch_role_contacts
from configs.config_loader import load_tab_config

def run_case_involved_comparison(case_id, appeal_number, conn, tab_config=None):
    if tab_config is None:
        tab_config = load_tab_config("עורר פרטי קשר")
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    log_and_print("\n📂 Running case contact comparison...", "info")

    try:
        menora_df = fetch_menora_case_contacts(appeal_number, conn)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df["Main_Id_Number"] = menora_df["Main_Id_Number"].astype(str)
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} case contact entries from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    json_data = get_case_data(case_id)
    json_df = pd.DataFrame()

    try:
        first_involved = next((ci for ci in json_data.get("caseInvolveds", []) if ci.get("representors")), None)
        use_role_based = any(
            rep.get("appointmentEndDate") is None and rep.get("roleInCorporationId")
            for rep in first_involved.get("representors", [])
        ) if first_involved else False

        contact_records = []

        if use_role_based:
            role_ids = [rep.get("roleInCorporationId") for rep in first_involved.get("representors", []) if rep.get("appointmentEndDate") is None and rep.get("roleInCorporationId")]
            contact_data = fetch_role_contacts(role_ids, case_id)
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
        else:
            connect_map = {}
            for rep in first_involved.get("representors", []):
                connect_id = rep.get("connectDetailsId")
                case_ident = rep.get("caseInvolvedIdentifyId")
                if connect_id and case_ident:
                    connect_map[connect_id] = case_ident

            contact_data = fetch_connect_contacts(list(connect_map.keys()))

            for role in contact_data.get("connectDetails", []):
                connect_id = role.get("connectDetailsId")
                corp_id = connect_map.get(connect_id)
                mail = role.get("mail")
                phone1 = role.get("primaryPhone")
                phone2 = role.get("secondaryPhone")
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

    missing_json_list = []
    missing_menora_list = []
    mismatched_fields = []

    if not json_df.empty and not menora_df.empty:
        try:
            json_df["_source"] = "json"
            menora_df["_source"] = "menora"

            merged = pd.merge(json_df, menora_df, how="outer", on="Main_Id_Number", indicator=True)

            missing_json_list = merged[merged["_merge"] == "right_only"]["Main_Id_Number"].dropna().unique().tolist()
            missing_menora_list = merged[merged["_merge"] == "left_only"]["Main_Id_Number"].dropna().unique().tolist()

            both_matched = merged[merged["_merge"] == "both"]

            for _, row in both_matched.iterrows():
                for field_ui, field_db in field_map.items():
                    json_field = f"{field_ui}_json"
                    menora_field = field_ui

                    if json_field in row and menora_field in row:
                        json_val = str(row[json_field]).strip() if pd.notnull(row[json_field]) else ""
                        menora_val = str(row[menora_field]).strip() if pd.notnull(row[menora_field]) else ""

                        if (json_val == "" and menora_val.lower() == "none") or (json_val.lower() == "none" and menora_val == ""):
                            continue
                        if json_val != menora_val:
                            mismatched_fields.append({
                                "Main_Id_Number": row["Main_Id_Number"],
                                "Field": field_ui,
                                "Menora": menora_val,
                                "JSON": json_val
                            })

        except Exception as e:
            log_and_print(f"❌ Merge failed: {e}", "error")
            missing_json_list = menora_df["Main_Id_Number"].dropna().unique().tolist()
            missing_menora_list = json_df["Main_Id_Number"].dropna().unique().tolist()
    else:
        if json_df.empty:
            missing_json_list = menora_df["Main_Id_Number"].dropna().unique().tolist()
        if menora_df.empty:
            missing_menora_list = json_df["Main_Id_Number"].dropna().unique().tolist()

    status_tab = "pass" if not missing_json_list and not missing_menora_list and not mismatched_fields else "fail"

    return {
        str(case_id): {
            "case_contact": {
                "status_tab": status_tab,
                "missing_json_dates": missing_json_list,
                "missing_menora_dates": missing_menora_list,
                "mismatched_fields": mismatched_fields
            }
        }
    }
