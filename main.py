from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_menora_decision_data
from json_parser import extract_decision_data_from_json
from decision_comparator import compare_decision_data
from logging_utils import log_and_print

if __name__ == "__main__":
    load_configuration()

    case_id = 2004759
    appeal_number = 139124  # From Menora
    menora_df = fetch_menora_decision_data(appeal_number)

    json_data = fetch_case_details(case_id)
    if not json_data:
        exit(1)

    json_df = extract_decision_data_from_json(json_data)
    menora_df = fetch_menora_decision_data(appeal_number)

    field_map = {
        "Decision_Date": "decisionDate",
        "Decision_Type_Id": "decisionTypeToCourtId",
        "Decision_Status": "decisionStatusTypeId",
        "Is_For_Advertisement": "isForPublication",
        "Create_User": "decisionJudge"
    }

    mismatches = compare_decision_data(json_df, menora_df, field_map)

    if not mismatches:
        log_and_print("✅ All compared fields match between JSON and Menora.", "success")
    else:
        log_and_print(f"⚠️ Found {len(mismatches)} mismatches:", "warning", is_hebrew=True)
        for m in mismatches:
            log_and_print(f"🔎 mojId {m['mojId']}: {m['field']} → Menora='{m['menora']}' | JSON='{m['json']}'", "warning", is_hebrew=True)
