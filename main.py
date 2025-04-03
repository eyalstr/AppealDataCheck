from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_menora_decision_data
from json_parser import extract_decision_data_from_json
from decision_comparator import compare_decision_data
from logging_utils import log_and_print
from tabulate import tabulate  # Add at the top (install if needed)



def display_flat_fields(menora_df, json_df, field_map):
    rows = []

    # Menora side
    for _, row in menora_df.iterrows():
        rows.append({
            "Source": "Menora",
            **{k: row.get(k, "⛔") for k in field_map.keys()}
        })

    # JSON side
    for _, row in json_df.iterrows():
        rows.append({
            "Source": "JSON",
            **{k: row.get(v, "⛔") for k, v in field_map.items()}
        })

    print("\n🔍 Side-by-Side Field Display:")
    print(tabulate(rows, headers="keys", tablefmt="grid", showindex=True))

if __name__ == "__main__":
    load_configuration()

    case_id = 2004759
    appeal_number = 139124  # From Menora
    #menora_df = fetch_menora_decision_data(appeal_number)

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

    comparison_results = compare_decision_data(json_df, menora_df, field_map)

    if comparison_results:
        print("\n🔍 Detailed Field Comparison (Matched by mojId):")
        print(tabulate(comparison_results, headers="keys", tablefmt="grid", showindex=False))

        mismatches = [r for r in comparison_results if r["Match"] == "✗"]
        if not mismatches:
            log_and_print("✅ All matched fields are identical.", "success")
        else:
            log_and_print(f"⚠️ Found {len(mismatches)} mismatches", "warning")
    else:
        log_and_print("⚠️ No matching mojIds found between Menora and JSON.", "warning")

