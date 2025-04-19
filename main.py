from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_appeal_number_by_case_id
from requestlog_runner import run_request_log_comparison
from discussion_runner import run_discussion_comparison
from decision_runner import run_decision_comparison
from document_runner import run_document_comparison
from case_representator_runner import run_representator_comparison
from case_involved_runner import run_case_involved_comparison  # ✅ NEW
from logging_utils import log_and_print
from config_loader import load_tab_config
import json

def main():
    load_configuration()
    #case_ids = [2004759, 2005285, 2005281, 2005287, 2004338, 2004339]
    case_ids = [2004339]
    # case_ids = [  2004759, 2005285, 2005281, 2005287, 2004338, 2004339, 2001968, 2004759, 2004761, 2004762,
    #             2004771, 2004804, 2004805, 2004854, 2004860, 2004891, 2004892, 2004893, 2004905, 2004907,
    #             2004916, 2004917, 2004928, 2004951, 2004952, 2004956, 2004964, 2004968, 2004970, 2004971,
    #             2004974, 2004984, 2004995, 2004996, 2004999, 2005006, 2005007, 2005011, 2005013, 2005019,
    #             2005020, 2005031, 2005036, 2005040, 2005045, 2005048, 2005055, 2004693, 2004930, 2004943,
    #             2004944, 2005111, 2005112, 2004361, 2004807, 2004969, 2005288, 2005289, 2005056, 2005058,
    #             2005060, 2005061, 2005063, 2005067, 2005068, 2005070, 2005071, 2005074, 2005075, 2005077,
    #             2005078, 2005091, 2005094, 2005102, 2005104, 2005106, 2005109, 2005117, 2005119, 2005122,
    #             2005125, 2005128, 2005129, 2005130, 2005131, 2005132, 2005134, 2005135, 2005137, 2005145,
    #             2005146, 2005147, 2005151, 2005152, 2005154, 2005155, 2005158, 2005159, 2005160, 2005161,
    #             2005162, 2005164, 2005263, 2005284, 2005166, 2005167, 2005168, 2005169, 2005170, 2005176,
    #             2005177, 2005178, 2005179, 2005183, 2005184, 2005186, 2005187, 2005188, 2005195, 2005196,
    #             2005197, 2005198, 2005199, 2005200, 2005202, 2005203, 2005207, 2005208, 2005214, 2005216]
    dashboard_results = {}

    document_tab_config = load_tab_config("מסמכים")
    decision_tab_config = load_tab_config("החלטות")
    discussion_tab_config = load_tab_config("דיונים")
    request_log_tab_config = load_tab_config("יומן תיק")
    representator_tab_config = load_tab_config("מעורבים בתיק")
    case_contact_tab_config = load_tab_config("עורר פרטי קשר")
    
    for case_id in case_ids:
        log_and_print(f"\n\n🔁 Processing case_id {case_id}...", "info")
        appeal_number = fetch_appeal_number_by_case_id(case_id)
        if not appeal_number:
            log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Skipping.", "error")
            continue

        case_results = {}

        # ---- Active comparisons ----
        request_log_result = run_request_log_comparison(case_id, appeal_number,tab_config=request_log_tab_config)
        if request_log_result:
            case_results["request_log"] = request_log_result["request_log"]

        discussion_result = run_discussion_comparison(case_id, appeal_number,tab_config=discussion_tab_config)
        if discussion_result:
            case_results["discussion"] = discussion_result["discussion"]

        decision_result = run_decision_comparison(case_id, appeal_number,tab_config=decision_tab_config)
        if decision_result:
            case_results["decision"] = decision_result["decision"]

        document_result = run_document_comparison(case_id, appeal_number,tab_config=document_tab_config)
        if document_result:
            case_results["document"] = document_result["document"]

        representator_result = run_representator_comparison(case_id, appeal_number,tab_config=representator_tab_config)
        representator_section = representator_result.get(str(case_id), {})
        if "representator_log" in representator_section:
            case_results["representator_log"] = representator_section["representator_log"]

        case_contact_result = run_case_involved_comparison(case_id, appeal_number,tab_config=case_contact_tab_config)  # ✅ NEW
        case_contact_section = case_contact_result.get(str(case_id), {})
        if "case_contact" in case_contact_section:
            case_results["case_contact"] = case_contact_section["case_contact"]

        if case_results:
            dashboard_results[str(case_id)] = case_results

    # Save the comparison summary for dashboard
    with open("comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(dashboard_results, f, indent=2, ensure_ascii=False)

    log_and_print("✅ All comparisons completed. Use streamlit run dashboard_app.py to view results.", "success")

if __name__ == "__main__":
    main()
