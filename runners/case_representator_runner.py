from apis.client_api import fetch_case_details
from configs.config_loader import load_tab_config
from utils.logging_utils import log_and_print,normalize_whitespace
from apis.sql_client import fetch_menora_case_involved_data
from utils.fetcher import get_case_data

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

    json_data = get_case_data(case_id)
    try:
        case_involveds = json_data.get("caseInvolveds", [])

        orer_rep = next((r for r in case_involveds[0].get("representors", []) if r.get("appointmentEndDate") is None), {}) if len(case_involveds) > 0 else {}
        meshiva_rep = next((r for r in case_involveds[1].get("representors", []) if r.get("appointmentEndDate") is None), {}) if len(case_involveds) > 1 else {}

    except Exception as e:
        log_and_print(f"❌ Failed to parse JSON structure: {e}", "error")
        return {str(case_id): {"representator_log": {"status_tab": "fail", "missing_json_dates": [], "missing_menora_dates": [], "mismatched_fields": []}}}

    def normalize_whitespace(s):
        return " ".join(str(s).split()) if s is not None else ""

    def safe_id(val):
        val = str(val).strip()
        return val.zfill(9) if val.isdigit() else ""

    def safe(val):
        import numpy as np
        if isinstance(val, (np.generic,)):
            return val.item()
        return val

    mismatched_fields = []

    row = menora_df.iloc[0] if not menora_df.empty else None

    if row is not None:
        # Extract and clean Menora values
        menora_orer_id = safe_id(row.get("Main_Id_Number"))
        menora_orer_name = normalize_whitespace(row.get("orer", ""))
        menora_meshiva_name = normalize_whitespace(row.get("meshiva", ""))
        menora_meshiva_id = safe_id(row.get("meshivaID"))

        # Extract and clean JSON values
        json_orer_id = safe_id(orer_rep.get("caseInvolvedIdentifyId"))
        json_orer_name = normalize_whitespace(orer_rep.get("caseInvolvedName", ""))
        json_meshiva_id = safe_id(meshiva_rep.get("caseInvolvedIdentifyId"))
        json_meshiva_name = normalize_whitespace(meshiva_rep.get("caseInvolvedName", ""))

        # Compare fields
        if menora_orer_id != json_orer_id:
            mismatched_fields.append({"Field": "orerID", "Menora": safe(menora_orer_id), "JSON": safe(json_orer_id)})
        if menora_orer_name != json_orer_name:
            mismatched_fields.append({"Field": "orer", "Menora": safe(menora_orer_name), "JSON": safe(json_orer_name)})
        if menora_meshiva_name != json_meshiva_name:
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
