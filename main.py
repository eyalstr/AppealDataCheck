from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_menora_decision_data
from json_parser import extract_decision_data_from_json
from decision_comparator import compare_decision_data, compare_decision_counts
from config_loader import load_tab_config
from logging_utils import log_and_print
from sql_client import fetch_appeal_number_by_case_id
from tabulate import tabulate


def run_comparison(case_id: int, appeal_number: int, tab_name: str):
    tab_config = load_tab_config(tab_name)

    # Fetch and parse data
    json_data = fetch_case_details(case_id)
    if not json_data:
        log_and_print("❌ Failed to fetch JSON data.", "error")
        return

    json_df = extract_decision_data_from_json(json_data)  # For החלטות
    menora_df = fetch_menora_decision_data(appeal_number)  # Uses static SQL for now

    field_map = tab_config["field_map"]

    # Compare detailed fields
    comparison_results = compare_decision_data(json_df, menora_df, field_map)
    if comparison_results:
        print(f"\n🔍 Detailed Field Comparison (Matched by {tab_config['key_field']}):")
        print(tabulate(comparison_results, headers="keys", tablefmt="grid", showindex=False))

        mismatches = [r for r in comparison_results if r["Match"] == "✗"]
        if not mismatches:
            log_and_print("✅ All matched fields are identical.", "success")
        else:
            log_and_print(f"⚠️ Found {len(mismatches)} mismatches", "warning")
    else:
        log_and_print("⚠️ No matching keys found between Menora and JSON.", "warning")

    # Compare counts
    count_comparison = compare_decision_counts(json_df, menora_df)
    if count_comparison:
        print("\n📊 Decision Count Comparison:")
        print(tabulate(count_comparison, headers="keys", tablefmt="grid", showindex=False))


if __name__ == "__main__":
    load_configuration()

    case_id = 2004759

    appeal_number = fetch_appeal_number_by_case_id(case_id)
    if not appeal_number:
        log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Exiting.", "error")
        exit(1)

    tab_name = "החלטות"  # ✅ define this before using it
    run_comparison(case_id, appeal_number, tab_name)

