from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_menora_decision_data
from json_parser import extract_decision_data_from_json
from decision_comparator import compare_decision_data, compare_decision_counts
from logging_utils import log_and_print
from tabulate import tabulate


def run_decision_comparison(case_id: int, appeal_number: int):
    # Fetch and parse data
    json_data = fetch_case_details(case_id)
    if not json_data:
        log_and_print("❌ Failed to fetch JSON data.", "error")
        return

    json_df = extract_decision_data_from_json(json_data)
    menora_df = fetch_menora_decision_data(appeal_number)

    # Define which fields to compare
    field_map = {
        "Decision_Date": "decisionDate",
        "Decision_Type_Id": "decisionTypeToCourtId",
        "Decision_Status": "decisionStatusTypeId",
        "Is_For_Advertisement": "isForPublication",
        "Create_User": "decisionJudge"
    }

    # Compare detailed fields
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

    # Compare counts
    count_comparison = compare_decision_counts(json_df, menora_df)
    if count_comparison:
        print("\n📊 Decision Count Comparison:")
        print(tabulate(count_comparison, headers="keys", tablefmt="grid", showindex=False))


if __name__ == "__main__":
    load_configuration()

    case_id = 2004759
    appeal_number = 139124

    run_decision_comparison(case_id, appeal_number)
