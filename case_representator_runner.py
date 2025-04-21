from client_api import fetch_case_details
from config_loader import load_tab_config
from logging_utils import log_and_print,normalize_whitespace
from sql_client import fetch_menora_case_involved_data


def run_representator_comparison(case_id, appeal_number, conn, tab_config=None):
    tab_config = load_tab_config("מעורבים בתיק")
    log_and_print(f"\n📂 Running case involved comparison for case_id {case_id}...", "info")

    try:
        menora_df = fetch_menora_case_involved_data(appeal_number, conn)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} case involved entries from Menora for appeal {appeal_number}", "success")
        log_and_print(f"📋 Menora DataFrame:\n{menora_df}", "debug")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        return {str(case_id): {"representator_log": {"status_tab": "fail", "missing_json_dates": [], "missing_menora_dates": [], "mismatched_fields": []}}}

    json_data = fetch_case_details(case_id)
    try:
        case_involveds = json_data.get("caseInvolveds", [])

        orer_rep = next((r for r in case_involveds[0].get("representors", []) if r.get("appointmentEndDate") is None), {}) if len(case_involveds) > 0 else {}
        meshiva_rep = next((r for r in case_involveds[1].get("representors", []) if r.get("appointmentEndDate") is None), {}) if len(case_involveds) > 1 else {}

        # log_and_print(f"📋 Parsed JSON orer_rep: {orer_rep}", "debug")
        # log_and_print(f"📋 Parsed JSON meshiva_rep: {meshiva_rep}", "debug")
    except Exception as e:
        log_and_print(f"❌ Failed to parse JSON structure: {e}", "error")
        return {str(case_id): {"representator_log": {"status_tab": "fail", "missing_json_dates": [], "missing_menora_dates": [], "mismatched_fields": []}}}

    def safe(val):
        import numpy as np
        if isinstance(val, (np.generic,)):
            return val.item()
        return val

    mismatched_fields = []

    row = menora_df.iloc[0] if not menora_df.empty else None

    if row is not None:
        menora_orer_id = str(row.get("Main_Id_Number")).strip()
        menora_orer_name = str(row.get("orer")).strip()
        menora_meshiva_name = str(row.get("meshiva")).strip()
        menora_meshiva_id = str(row.get("meshivaID")).strip()

        json_orer_id = str(orer_rep.get("caseInvolvedIdentifyId")).strip()
        json_orer_name = str(orer_rep.get("caseInvolvedName")).strip()
        json_meshiva_id = str(meshiva_rep.get("caseInvolvedIdentifyId")).strip()
        json_meshiva_name = str(meshiva_rep.get("caseInvolvedName")).strip()

        if menora_orer_id != json_orer_id:
            mismatched_fields.append({"Field": "orerID", "Menora": safe(menora_orer_id), "JSON": safe(json_orer_id)})
        if normalize_whitespace(menora_orer_name) != normalize_whitespace(json_orer_name):
            mismatched_fields.append({"Field": "orer", "Menora": safe(menora_orer_name), "JSON": safe(json_orer_name)})
        if normalize_whitespace(menora_meshiva_name) != normalize_whitespace(json_meshiva_name):
            mismatched_fields.append({"Field": "meshiva", "Menora": safe(menora_meshiva_name), "JSON": safe(json_meshiva_name)})
        if menora_meshiva_id != json_meshiva_id:
            mismatched_fields.append({"Field": "meshivaID", "Menora": safe(menora_meshiva_id), "JSON": safe(json_meshiva_id)})

    status_tab = "pass" if not mismatched_fields else "fail"

    return {
        str(case_id): {
            "representator_log": {
                "status_tab": status_tab,
                "missing_json_dates": [],
                "missing_menora_dates": [],
                "mismatched_fields": mismatched_fields
            }
        }
    }
