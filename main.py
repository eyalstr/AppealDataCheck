from config import load_configuration
from client_api import fetch_case_details, fetch_case_documents
from sql_client import fetch_appeal_number_by_case_id, fetch_menora_decision_data, fetch_menora_document_data
from json_parser import extract_decision_data_from_json
from decision_comparator import compare_decision_data, compare_decision_counts
from document_comparator import extract_document_data_from_json, compare_document_data, compare_document_counts
from logging_utils import log_and_print
from tabulate import tabulate
from decision_runner import run_decision_comparison
from document_runner import run_document_comparison


def main():
    load_configuration()
    case_ids = [2004759]

    all_summaries = {
        "decision": [],
        "document": []
    }

    for case_id in case_ids:
        log_and_print(f"\n\n🔁 Processing case_id {case_id}...", "info")
        appeal_number = fetch_appeal_number_by_case_id(case_id)
        if not appeal_number:
            log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Skipping.", "error")
            continue

        decision_summary = run_decision_comparison(case_id, appeal_number)
        if decision_summary:
            all_summaries["decision"].append(decision_summary)

        document_summary = run_document_comparison(case_id, appeal_number)
        if document_summary:
            all_summaries["document"].append(document_summary)

    # Final summaries per tab
    if all_summaries["decision"]:
        log_and_print("\n📋 Final Summary for Decision Tab:", "info")
        print(tabulate(all_summaries["decision"], headers="keys", tablefmt="grid", showindex=True))

    if all_summaries["document"]:
        log_and_print("\n📋 Final Summary for Document Tab:", "info")
        print(tabulate(all_summaries["document"], headers="keys", tablefmt="grid", showindex=True))


if __name__ == "__main__":
    main()
