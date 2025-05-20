from concurrent.futures import ThreadPoolExecutor, as_completed
from apis.client_api import fetch_case_details
from apis.sql_client import fetch_appeal_number_by_case_id
from runners.requestlog_runner import run_request_log_comparison
from runners.discussion_runner import run_discussion_comparison
from runners.decision_runner import run_decision_comparison
from runners.document_runner import run_document_comparison
from runners.distribution_runner import run_distribution_comparison
from runners.case_representator_runner import run_representator_comparison
from runners.case_involved_runner import run_case_involved_comparison
from utils.logging_utils import log_and_print
from configs.config_loader import load_tab_config
from utils.sql_connection import get_sql_connection
from utils.fetcher import get_case_data
from dotenv import load_dotenv
from utils.json_parser import extract_decisions
import json
import os
import sys


def load_configuration():
    """
    Dynamically load the .env file, even from an executable context.
    """
    # Detect execution context
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    env_path = os.path.join(base_dir, '.env')

    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)
        log_and_print(f"✅ Loaded configuration from {env_path}")
    else:
        log_and_print(f"❌ Configuration file not found at {env_path}", "error")
        exit(1)

    required_env_vars = [
        "BEARER_TOKEN",
        "MOJ_APPLICATION_ID",
        "BASE_URL"
    ]

    for var in required_env_vars:
        if not os.getenv(var):
            log_and_print(f"❌ Missing required environment variable: {var}", "error")
            exit(1)


def process_case(case_id, tab_configs):
    conn = get_sql_connection()  # Each thread gets its own connection
    try:
        log_and_print(f"\n\n🔁 Processing case_id {case_id}...", "info")
        appeal_number = fetch_appeal_number_by_case_id(case_id, conn)
        # if not appeal_number:
        #     log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Skipping.", "error")
        #     return case_id, {}

        case_results = {}
        
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

        #document_result = run_document_comparison(case_id, appeal_number, conn, tab_config=tab_configs["document"])
        # if document_result:
        #     case_results["document"] = document_result["document"]

        representator_result = run_representator_comparison(case_id, appeal_number, conn, tab_config=tab_configs["representator_log"])
        representator_section = representator_result.get(str(case_id), {})
        if "representator_log" in representator_section:
            case_results["representator_log"] = representator_section["representator_log"]

        case_contact_result = run_case_involved_comparison(case_id, appeal_number, conn, tab_config=tab_configs["case_contact"])
        case_contact_section = case_contact_result.get(str(case_id), {})
        if "case_contact" in case_contact_section:
            case_results["case_contact"] = case_contact_section["case_contact"]

        # # inside your main flow (per case_id)

        
        distribution_result = run_distribution_comparison(case_id, appeal_number, conn,tab_config=tab_configs.get("distribution"))

        if distribution_result:
            case_results["distribution"] = distribution_result["distribution"]


        return case_id, case_results

    except Exception as e:
        log_and_print(f"❌ Unexpected error during case_id {case_id}: {e}", "error")
        return case_id, {}
    finally:
        conn.close()


def main():
    #load_configuration()
    #case_ids = [2005049,2004905,2005468]
    #20.5 תיקי הסבה + API
    case_ids = [
            2005468, 2005510, 2005515, 2005516, 2005078, 2005224, 2005050, 2005203, 2005217, 2005183,
            2005109, 2005154, 2005049, 2005200, 2005114, 2005113, 2005199, 2005177, 2004893, 2005013,
            2005309, 2005311, 2005313, 2005389, 2005385, 2005309, 2005062, 2005071, 2005088, 2005090,
            2005102, 2005060, 2005239, 2005187, 2005091, 2005072, 2005073, 2005151, 2005234, 2005310,
            2005339, 2000741, 2005235, 2005344, 2005281, 2005246, 2005074, 2005075, 2005163, 2005195,
            2004968, 2004964, 2004905, 2005130, 2005131, 2004814, 2004956, 2004928, 2004907, 2005132,
            2005119, 2004433, 2004693, 2005208, 2005048, 2005009, 2004858, 2004877, 2005089, 2004758,
            2004851, 2004894, 2004960, 2004961, 2005006, 2005007, 2005052, 2005074, 2005075, 2005120,
            2005122, 2005125, 2005128, 2005129, 2005137, 2005148, 2005149, 2005165, 2005191, 2005196,
            2005197, 2005198, 2005199, 2005200, 2005203, 2005218, 2005220, 2005225, 2005229, 2005230,
            2005231, 2005236, 2005237, 2005241, 2005242, 2005253, 2005269, 2005270, 2005272, 2005277,
            2005282, 2005287, 2005288, 2005299, 2005304, 2005305, 2005307, 2005312, 2005314, 2005315,
            2005317, 2005319, 2005320, 2005323, 2005324, 2005325, 2005326, 2005328, 2005331, 2005334,
            2005336, 2005338, 2005340, 2005342, 2005346, 2005347, 2005348, 2005351, 2005352, 2005357,
            2005358, 2005360, 2005363, 2005365, 2005367, 2005368, 2005369, 2005370, 2005371, 2005373,
            2005377, 2005378, 2005382, 2005386, 2005388, 2005390, 2005391, 2004433, 2005289, 2005295,
            2005466, 2005467, 2005469, 2005470, 2005473, 2005474, 2005475, 2005476, 2005477, 2005480,
            2005481, 2005482, 2005483, 2005484, 2005485, 2005486, 2005487, 2005488, 2005489, 2005490,
            2005491, 2005492, 2005493, 2005494, 2005495, 2005496, 2005497, 2005498, 2005499, 2005500,
            2005501, 2005502, 2005503, 2005504, 2005505, 2005507, 2005508, 2005509, 2005392, 2005396,
            2005397, 2005399, 2005401, 2005402, 2005403, 2005404, 2005405, 2005407, 2005408, 2005409,
            2005410, 2005411, 2005412, 2005415, 2005417, 2005418, 2005421, 2005422, 2005424, 2005425,
            2005427, 2005429, 2005430, 2005431, 2005434, 2005435, 2005436, 2005437, 2005438, 2005439,
            2005442, 2005447, 2005448, 2005451, 2005452, 2005454, 2005460, 2005461, 2005462, 2005463,
            2005464, 2005465, 2005506, 2004338, 2004339, 2004974, 2005085, 2005219, 2005264, 2005291,
            2005329, 2005330, 2005343, 2005355, 2005155, 2005296, 2005376, 2005393, 2005458, 2005459,
            2005276, 2005366, 2005453, 2005060, 2005228, 2005395, 2005294, 2005161, 2005160, 2005159,
            2004762, 2001968, 2005381, 2005379, 2005372, 2005359, 2005332, 2005280, 2005279, 2005275,
            2005247, 2005226, 2005217, 2005216, 2005205, 2005188, 2005102, 2005067, 2005042, 2004996,
            2004995, 2004975, 2004759, 2005078, 2005184, 2005394, 2005400, 2005479, 2005511, 2005512,
            2005514
        ]


    # # 13.5 - appeals
    #case_ids = [2004877]#, 2005042, 2005055, 2005058, 2005006]
    # case_ids = [
    #     2005393, 2005459, 2005042, 2005055, 2005058, 2005006, 2005078, 2005119, 2005224, 2005050,
    #     2005036, 2004951, 2004968, 2005203, 2005249, 2005250, 2005259, 2005261, 2005262, 2005217,
    #     2005183, 2005104, 2005195, 2005109, 2005132, 2005154, 2004880, 2004881, 2005110, 2005049,
    #     2001968, 2004907, 2005073, 2005072, 2005170, 2005200, 2000741, 2005114, 2005113, 2005234,
    #     2005199, 2004928, 2005177, 2005015, 2005061, 2004893, 2004956, 2005013, 2004814, 2005309,
    #     2005311, 2005313, 2005389, 2005385, 2005309, 2005062, 2005071, 2005088, 2005090, 2005102,
    #     2005208, 2005214, 2004433, 2004964, 2005060, 2005020, 2005019, 2005131, 2005130, 2005239,
    #     2005187, 2004889, 2004905, 2005091, 2005290, 2004693, 2005048, 2005054, 2005009, 2004858,
    #     2004877, 2005089, 2005166, 2005287, 2005288, 2005289, 2005295, 2005296, 2005299, 2005304,
    #     2005305, 2005307, 2005308, 2005312, 2005314, 2005315, 2005317, 2005319, 2005320, 2005323,
    #     2005324, 2005326, 2005328, 2005331, 2005334, 2005335, 2005336, 2005338, 2005339, 2005340,
    #     2005342, 2005343, 2005344, 2005346, 2005347, 2005348, 2005351, 2005352, 2005354, 2005356,
    #     2005357, 2005358, 2005360, 2005282, 2005361, 2005363, 2005365, 2005366, 2005367, 2005368,
    #     2005369, 2005370, 2005371, 2005373, 2005378, 2005382, 2005386, 2005388, 2005390, 2005391,
    #     2005392, 2005394, 2005396, 2005397, 2005399, 2005401, 2005402, 2005403, 2005404, 2005405,
    #     2005407, 2005408, 2005409, 2005410, 2005411, 2005412, 2005415, 2005417, 2005418, 2005421,
    #     2005422, 2005424, 2005425, 2005427, 2005428, 2005429, 2005430, 2005431, 2005434, 2005436,
    #     2005437, 2005438, 2005439, 2005440, 2005442, 2005447, 2005448, 2005451, 2005456, 2005457,
    #     2005460, 2005461, 2005462, 2005463, 2005464, 2005465, 2005466, 2005467, 2005469, 2005470,
    #     2005473, 2005474, 2005475, 2005476, 2005477, 2004894, 2004960, 2004961, 2005007, 2005074,
    #     2005075, 2005113, 2005114, 2005122, 2005125, 2005128, 2005129, 2005137, 2005148, 2005149,
    #     2005155, 2005158, 2005163, 2005165, 2005191, 2005196, 2005199, 2005200, 2005203, 2005218,
    #     2005220, 2005225, 2005228, 2005229, 2005230, 2005235, 2005241, 2005242, 2005244, 2005245,
    #     2005251, 2005253, 2005257, 2005258, 2005264, 2005269, 2005270, 2005272, 2005277, 2005226,
    #     2005249, 2005250, 2005279, 2005332, 2005372, 2005400, 2005443, 2004974, 2005015, 2005219,
    #     2005291, 2005294, 2005329, 2005330, 2005355, 2005170, 2005179, 2005234, 2005284, 2005376,
    #     2005377, 2005455, 2005458, 2005151, 2005398, 2005453, 2004991, 2004992, 2005060, 2005157,
    #     2005306, 2005395, 2005145, 2005073, 2005072, 2005069, 2005061, 2005161, 2005160, 2005159,
    #     2005085, 2005079, 2004762, 2001968, 2004339, 2004338, 2005217, 2005216, 2005198, 2005197,
    #     2005188, 2005014, 2004975, 2005078, 2005259, 2005261, 2005262, 2005478, 2005479, 2005471,
    #     2005472
    #     ]
    
    
    # case_ids = [2004759, 2005285, 2005281, 2005287, 2004338, 2004339, 2001968, 2004759, 2004761, 2004762,
    #             2004771, 2004804, 2004805, 2004854, 2004860, 2004891, 2004892, 2004893, 2004905, 2004907]
    
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

    #close appeals
   

    tab_configs = {
        "document": load_tab_config("מסמכים"),
        "decision": load_tab_config("החלטות"),
        "discussion": load_tab_config("דיונים"),
        "request_log": load_tab_config("יומן תיק"),
        "representator_log": load_tab_config("מעורבים בתיק"),
        "case_contact": load_tab_config("עורר פרטי קשר"),
        "distribution": load_tab_config("הפצות")
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