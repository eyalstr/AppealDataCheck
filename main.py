# main.py
from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_appeal_number_by_case_id
from requestlog_runner import run_request_log_comparison
from discussion_runner import run_discussion_comparison  # ✅ Add this import
from logging_utils import log_and_print
import json

def main():
    load_configuration()
    case_ids = [2004759]

    #        , 2005285, 2005281, 2005287, 2004338, 2004339]
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

        # ---- Add other modules as needed ----
        # case_results["document"] = run_document_comparison(...)
        # case_results["decision"] = run_decision_comparison(...)
        # case_results["distribution"] = run_distribution_comparison(...)

        if case_results:
            dashboard_results[str(case_id)] = case_results

    # Save the comparison summary for dashboard
    with open("comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(dashboard_results, f, indent=2, ensure_ascii=False)

    log_and_print("✅ All comparisons completed. Use `streamlit run dashboard_app.py` to view results.", "success")

if __name__ == "__main__":
    main()
