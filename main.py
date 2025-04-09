# main.py
from config import load_configuration
from client_api import fetch_case_details, fetch_case_documents, fetch_case_discussions
from sql_client import fetch_appeal_number_by_case_id
from decision_runner import run_decision_comparison
from document_runner import run_document_comparison
from discussion_runner import run_discussion_comparison
from case_representator_runner import run_case_involved_comparison
from case_contact_runner import run_case_contacts_comparison
from requestlog_runner import run_request_log_comparison
from logging_utils import log_and_print
from tabulate import tabulate

def main():
    load_configuration()
    case_ids = [2004759]

    all_summaries = {
        "decision": [],
        "document": [],
        "discussion": [],
        "case_involved": [],
        "case_contacted": [],
        "request_log": []
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

        discussion_summary = run_discussion_comparison(case_id, appeal_number)
        if discussion_summary:
            all_summaries["discussion"].append(discussion_summary)

        # ב"כ צדדים - עורר ומשיבה
        case_involved_summary = run_case_involved_comparison(case_id, appeal_number)
        if case_involved_summary:
            all_summaries["case_involved"].append(case_involved_summary)

        # פרטי עורר
        case_contacts_summary = run_case_contacts_comparison(case_id, appeal_number)
        if case_contacts_summary:
            all_summaries["case_contacted"].append(case_contacts_summary)


        # Run request log comparison
        request_log_summary = run_request_log_comparison(case_id, appeal_number)
        if request_log_summary:
            all_summaries["request_log"].append(request_log_summary)


    # if all_summaries["decision"]:
    #     log_and_print("\n📋 Final Summary for Decision Tab:", "info")
    #     print(tabulate(all_summaries["decision"], headers="keys", tablefmt="grid", showindex=True))

    # if all_summaries["document"]:
    #     log_and_print("\n📋 Final Summary for Document Tab:", "info")
    #     print(tabulate(all_summaries["document"], headers="keys", tablefmt="grid", showindex=True))

    # if all_summaries["discussion"]:
    #     log_and_print("\n📋 Final Summary for Discussion Tab:", "info")
    #     print(tabulate(all_summaries["discussion"], headers="keys", tablefmt="grid", showindex=True))

    # if all_summaries["case_involved"]:
    #     log_and_print("\n📋 Final Summary for Case Involved Tab:", "info")
    #     print(tabulate(all_summaries["case_involved"], headers="keys", tablefmt="grid", showindex=True))

if __name__ == "__main__":
    main()
