from config_loader import load_tab_config
from client_api import fetch_case_documents
from json_parser import extract_document_data_from_json
from sql_client import fetch_menora_document_data
from document_comparator import compare_document_data, compare_document_counts
from logging_utils import log_and_print
from tabulate import tabulate
from collections import defaultdict

def run_document_comparison(case_id, appeal_number):
    tab_config = load_tab_config("מסמכים")
    log_and_print(f"📁 Available tabs in config: {list(tab_config.keys())}", is_hebrew=True)

    json_data = fetch_case_documents(case_id)
    if not json_data:
        log_and_print(f"❌ Failed to fetch JSON documents for case_id {case_id}.", "error")
        return

    # Fix for correct extraction path
    documents = json_data.get("documentList")
    if not documents:
        log_and_print("⚠️ No 'documentList' found in JSON.", "warning")
        return

    json_df = extract_document_data_from_json(documents)
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
                field_info = {
                    "Field": row["Field"],
                    "Menora Value": row["Menora Value"],
                    "JSON Value": row["JSON Value"]
                }
                test_status["field_mismatches"].append(row)
                mismatch_by_mojid[moj].append(field_info)

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

    for moj_id, mismatches in mismatch_by_mojid.items():
        log_and_print(f"🔎 Mismatched Fields for mojId {moj_id}:", "warning")
        print(tabulate(mismatches, headers="keys", tablefmt="grid"))

    return {
        "case_id": case_id,
        "missing_in_menora": len(test_status["missing_in_menora"]),
        "missing_in_json": len(test_status["missing_in_json"]),
        "field_mismatches": len(test_status["field_mismatches"]),
        "fully_matched": len(test_status["fully_matched_mojIds"]),
        "field_mismatch_details": dict(mismatch_by_mojid)
    }
