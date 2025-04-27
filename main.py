from concurrent.futures import ThreadPoolExecutor, as_completed
from config import load_configuration
from apis.client_api import fetch_case_details
from apis.sql_client import fetch_appeal_number_by_case_id
from runners.requestlog_runner import run_request_log_comparison
from runners.discussion_runner import run_discussion_comparison
from runners.decision_runner import run_decision_comparison
from runners.document_runner import run_document_comparison
from runners.case_representator_runner import run_representator_comparison
from runners.case_involved_runner import run_case_involved_comparison
from utils.logging_utils import log_and_print
from configs.config_loader import load_tab_config
from utils.sql_connection import get_sql_connection
import json
from utils.fetcher import get_case_data
from utils.json_parser import extract_decisions


def process_case(case_id, tab_configs):
    conn = get_sql_connection()  # Each thread gets its own connection
    try:
        log_and_print(f"\n\n🔁 Processing case_id {case_id}...", "info")
        appeal_number = fetch_appeal_number_by_case_id(case_id, conn)
        if not appeal_number:
            log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Skipping.", "error")
            return case_id, {}

        case_results = {}
        from utils.json_parser import is_case_type_supported

        if not is_case_type_supported(case_id):
            return case_id, {
                "document": {
                    "status_tab": "skip",
                    "reason": "Not relevant case type"
                }
            }

        case_json = get_case_data(case_id)
        #log_and_print(f"case_json={case_json}")               



        request_log_result = run_request_log_comparison(case_id, appeal_number, conn, tab_config=tab_configs["request_log"])
        if request_log_result:
            case_results["request_log"] = request_log_result["request_log"]

        discussion_result = run_discussion_comparison(case_id, appeal_number, conn, tab_config=tab_configs["discussion"])
        if discussion_result:
            case_results["discussion"] = discussion_result["discussion"]

        decision_result = run_decision_comparison(case_id, appeal_number, conn, tab_config=tab_configs["decision"])
        if decision_result:
            case_results["decision"] = decision_result["decision"]

        document_result = run_document_comparison(case_id, appeal_number, conn, tab_config=tab_configs["document"])
        if document_result:
            case_results["document"] = document_result["document"]

        representator_result = run_representator_comparison(case_id, appeal_number, conn, tab_config=tab_configs["representator_log"])
        representator_section = representator_result.get(str(case_id), {})
        if "representator_log" in representator_section:
            case_results["representator_log"] = representator_section["representator_log"]

        case_contact_result = run_case_involved_comparison(case_id, appeal_number, conn, tab_config=tab_configs["case_contact"])
        case_contact_section = case_contact_result.get(str(case_id), {})
        if "case_contact" in case_contact_section:
            case_results["case_contact"] = case_contact_section["case_contact"]

        return case_id, case_results

    except Exception as e:
        log_and_print(f"❌ Unexpected error during case_id {case_id}: {e}", "error")
        return case_id, {}
    finally:
        conn.close()


def main():
    load_configuration()

    case_ids = [2004759, 2005285, 2005281, 2005287, 2004338, 2004339, 2001968, 2004759, 2004761, 2004762,
                2004771, 2004804, 2004805, 2004854, 2004860, 2004891, 2004892, 2004893, 2004905, 2004907]
    
    # case_ids = [2004759, 2005285, 2005281, 2005287, 2004338, 2004339, 2001968, 2004759, 2004761, 2004762,
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
    #             2005197, 2005198, 2005199, 2005200, 2005202, 2005203, 2005207, 2005208, 2005214, 2005216,
    #             2005217, 2005219, 2005220, 2005221, 2005224, 2005226, 2005227, 2005228, 2005229, 2005231,
    #             2005232, 2005233, 2005234, 2005235, 2005239, 2005242, 2005244, 2005245, 2004758, 2004975,
    #             2004982, 2005025, 2005062, 2005088, 2005090, 2005120, 2005201, 2005262, 2000741, 2004814,
    #             2005015, 2005049, 2005052, 2005079, 2005085, 2005087, 2005110, 2005181, 2005182, 2005014,
    #             2005053, 2005054, 2005194, 2005222, 2005252, 2004856, 2004887, 2004894, 2004909, 2005059,
    #             2005072, 2005073, 2005173, 2005191, 2005218, 2005269, 2004855, 2005127, 2004866, 2004877,
    #             2004927, 2004960, 2004961, 2004991, 2004992, 2005089, 2005148, 2005149, 2005163, 2005180,
    #             2005241, 2005246, 2005247, 2005248, 2005249, 2005250, 2005251, 2005253, 2005256, 2005257,
    #             2005258, 2005259, 2005261, 2005264, 2005268, 2005271, 2005272, 2005273, 2005274, 2005275,
    #             2005276, 2005277, 2005278, 2005279, 2005280, 2005282, 2004758, 2004759, 2004854, 2004930,
    #             2004980, 2005003, 2005011, 2005025, 2005029, 2005042, 2005055, 2005058, 2005071, 2005088,
    #             2005090, 2004674, 2004943, 2004944, 2004982, 2004982, 2005024, 2004826, 2004955, 2004997,
    #             2005041, 2004869, 2005069, 2004998, 2004880, 2004881, 2005142, 2004925, 2005057, 2004828,
    #             2004827, 2004989, 2004889, 2005081, 2005080]  # Replace or extend as needed

    tab_configs = {
        "document": load_tab_config("מסמכים"),
        "decision": load_tab_config("החלטות"),
        "discussion": load_tab_config("דיונים"),
        "request_log": load_tab_config("יומן תיק"),
        "representator_log": load_tab_config("מעורבים בתיק"),
        "case_contact": load_tab_config("עורר פרטי קשר")
    }

    dashboard_results = {}

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(process_case, case_id, tab_configs) for case_id in case_ids]
        for future in as_completed(futures):
            case_id, case_results = future.result()
            if case_results:
                dashboard_results[str(case_id)] = case_results

    with open("comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(dashboard_results, f, indent=2, ensure_ascii=False)

    log_and_print("✅ All comparisons completed. Use streamlit run dashboard_app.py to view results.", "success")


if __name__ == "__main__":
    main()