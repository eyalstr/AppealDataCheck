from config import load_configuration
from client_api import fetch_case_details
from sql_client import fetch_appeal_number_by_case_id, fetch_menora_decision_data
from json_parser import extract_decision_data_from_json
from decision_comparator import compare_decision_data, compare_decision_counts
from config_loader import load_tab_config
from logging_utils import log_and_print
from tabulate import tabulate
from document_comparator import extract_document_data_from_json, compare_document_data, compare_document_counts


def run_comparison(case_id: int, appeal_number: int, tab_name: str):
    tab_config = load_tab_config(tab_name)

    json_data = fetch_case_details(case_id)
    if not json_data:
        log_and_print(f"❌ Failed to fetch JSON data for case_id {case_id}.", "error")
        return

    json_df = extract_decision_data_from_json(json_data)
    menora_df = fetch_menora_decision_data(appeal_number)

    field_map = tab_config["field_map"]

    # Compare detailed fields
    comparison_results = compare_decision_data(json_df, menora_df, field_map)

    from collections import defaultdict
    mismatch_by_mojid = defaultdict(list)
    all_mojids_in_compare = set()

    test_status = {
        "missing_in_json": [],
        "missing_in_menora": [],
        "field_mismatches": [],
        "fully_matched_mojIds": set(),
        "mojId_matched": bool(comparison_results),
    }

    if comparison_results:
        for row in comparison_results:
            moj = row["mojId"]
            all_mojids_in_compare.add(moj)
            if row["Match"] == "✗":
                test_status["field_mismatches"].append(row)
                mismatch_by_mojid[moj].append(row["Field"])
        test_status["fully_matched_mojIds"] = all_mojids_in_compare - mismatch_by_mojid.keys()

    # Compare counts
    count_comparison = compare_decision_counts(json_df, menora_df)

    if count_comparison:
        for row in count_comparison:
            if row["Type"] == "Missing in JSON":
                test_status["missing_in_json"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]
            elif row["Type"] == "Missing in Menora":
                test_status["missing_in_menora"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]

    # 🧪 Final Summary (Clean & Logged)
    log_and_print(f"\n🧪 Test Result Summary for Case ID {case_id}:", "info")

    log_and_print(f"{'✅' if not test_status['missing_in_menora'] else '❌'} {len(test_status['missing_in_menora'])} decision(s) missing in Menora.", "info")
    log_and_print(f"{'✅' if not test_status['missing_in_json'] else '❌'} {len(test_status['missing_in_json'])} decision(s) missing in JSON.", "info")
    log_and_print(f"{'✅' if not test_status['field_mismatches'] else '❌'} {len(mismatch_by_mojid)} field mismatch(es).", "info")
    log_and_print(f"✅ {len(test_status['fully_matched_mojIds'])} decision(s) fully matched across all fields.", "info")

    log_and_print("=" * 80 + "\n", "info")

    return {
    "case_id": case_id,
    "missing_in_menora": len(test_status["missing_in_menora"]),
    "missing_in_json": len(test_status["missing_in_json"]),
    "field_mismatches": len(mismatch_by_mojid),
    "fully_matched": len(test_status["fully_matched_mojIds"]),
    }



if __name__ == "__main__":
    load_configuration()

    tab_name = "החלטות"
    case_ids = [2005285,2004759]
    #[2004759, 2005285, 2005281, 2005287]

    for case_id in case_ids:
        print(f"\n🔁 Processing case_id {case_id}...")
        appeal_number = fetch_appeal_number_by_case_id(case_id)
        if not appeal_number:
            log_and_print(f"⚠️ Skipping case {case_id} due to missing appeal number.", "warning")
            continue

        run_comparison(case_id, appeal_number, tab_name)

    all_summaries = []

    for case_id in [2004759, 2001968, 2004943, 2005177]:
        log_and_print(f"\n🔁 Processing case_id {case_id}...", "info")
        appeal_number = fetch_appeal_number_by_case_id(case_id)
        if not appeal_number:
            log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Skipping.", "error")
            continue
        summary = run_comparison(case_id, appeal_number, "החלטות")
        if summary:
            all_summaries.append(summary)

    # 🧾 Final Summary Table
    if all_summaries:
        log_and_print("\n📋 Final Summary Across All Cases:", "info")
        print(tabulate(all_summaries, headers="keys", tablefmt="grid", showindex=True))