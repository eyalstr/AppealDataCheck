from config_loader import load_tab_config
from client_api import fetch_case_documents
from sql_client import fetch_menora_document_data
from document_comparator import extract_document_data_from_json, compare_document_data, compare_document_counts
from logging_utils import log_and_print
from collections import defaultdict

def run_document_comparison(case_id: int, appeal_number: int):
    tab_config = load_tab_config("מסמכים")
    json_data = fetch_case_documents(case_id)
    if not json_data:
        log_and_print(f"❌ Failed to fetch documents for case_id {case_id}.", "error")
        return

    json_df = extract_document_data_from_json(json_data)
    menora_df = fetch_menora_document_data(appeal_number)
    field_map = tab_config["field_map"]

    comparison_results = compare_document_data(json_df, menora_df, field_map)
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

    count_comparison = compare_document_counts(json_df, menora_df)
    if count_comparison:
        for row in count_comparison:
            if row["Type"] == "Missing in JSON":
                test_status["missing_in_json"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]
            elif row["Type"] == "Missing in Menora":
                test_status["missing_in_menora"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]

    log_and_print(f"\n🧪 Test Result Summary for Case ID {case_id} [Documents]:", "info")
    log_and_print(f"{'✅' if not test_status['missing_in_menora'] else '❌'} {len(test_status['missing_in_menora'])} document(s) missing in Menora.", "info")
    log_and_print(f"{'✅' if not test_status['missing_in_json'] else '❌'} {len(test_status['missing_in_json'])} document(s) missing in JSON.", "info")
    log_and_print(f"{'✅' if not test_status['field_mismatches'] else '❌'} {len(mismatch_by_mojid)} document(s) with field mismatch(es).", "info")
    log_and_print(f"✅ {len(test_status['fully_matched_mojIds'])} document(s) fully matched.", "info")

    return {
        "case_id": case_id,
        "missing_in_menora": len(test_status["missing_in_menora"]),
        "missing_in_json": len(test_status["missing_in_json"]),
        "field_mismatches": len(mismatch_by_mojid),
        "fully_matched": len(test_status["fully_matched_mojIds"]),
    }
