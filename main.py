from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_appeal_number_by_case_id
from requestlog_runner import run_request_log_comparison
from discussion_runner import run_discussion_comparison
from decision_runner import run_decision_comparison
from document_runner import run_document_comparison  # ✅ Add this import
from case_representator_runner import run_representator_comparison
from logging_utils import log_and_print
import json

def main():
    load_configuration()
    case_ids = [2004759, 2005285, 2005281, 2005287, 2004338, 2004339]
    #case_ids = [2004339]
    dashboard_results = {}

    for case_id in case_ids:
        log_and_print(f"\n\n🔁 Processing case_id {case_id}...", "info")
        appeal_number = fetch_appeal_number_by_case_id(case_id)
        if not appeal_number:
            log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Skipping.", "error")
            continue

        case_results = {}

        # ---- Active comparisons ----
        request_log_result = run_request_log_comparison(case_id, appeal_number)
        if request_log_result:
            case_results["request_log"] = request_log_result["request_log"]  # Flatten one level

        discussion_result = run_discussion_comparison(case_id, appeal_number)
        if discussion_result:
            case_results["discussion"] = discussion_result["discussion"]  # Flatten one level

        decision_result = run_decision_comparison(case_id, appeal_number)
        if decision_result:
            case_results["decision"] = decision_result["decision"]  # Flatten one level

        document_result = run_document_comparison(case_id, appeal_number)
        if document_result:
            case_results["document"] = document_result["document"]  # Flatten one level

        representator_result = run_representator_comparison(case_id, appeal_number)  # ✅ New
        representator_section = representator_result.get(str(case_id), {})
        if "representator_log" in representator_section:
            case_results["representator_log"] = representator_section["representator_log"]


        if case_results:
            dashboard_results[str(case_id)] = case_results

    # Save the comparison summary for dashboard
    with open("comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(dashboard_results, f, indent=2, ensure_ascii=False)

    log_and_print("✅ All comparisons completed. Use streamlit run dashboard_app.py to view results.", "success")

if __name__ == "__main__":
    main()
