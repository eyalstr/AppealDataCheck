from config import load_configuration
from client_api import fetch_case_details, fetch_case_documents
from sql_client import fetch_appeal_number_by_case_id, fetch_menora_decision_data, fetch_menora_document_data
from json_parser import extract_decision_data_from_json
from decision_comparator import compare_decision_data, compare_decision_counts
from document_comparator import extract_document_data_from_json, compare_document_data, compare_document_counts
from logging_utils import log_and_print
from tabulate import tabulate

def run_decision_comparison(case_id, appeal_number):
    json_data = fetch_case_details(case_id)
    if not json_data:
        log_and_print(f"❌ No JSON data for case_id {case_id}", "error")
        return

    json_df = extract_decision_data_from_json(json_data)
    menora_df = fetch_menora_decision_data(appeal_number)

    field_map = {
        "Decision_Date": "decisionDate",
        "Decision_Type_Id": "decisionTypeToCourtId",
        "Decision_Status": "decisionStatusTypeId",
        "Is_For_Advertisement": "isForPublication",
        "Create_User": "decisionJudge"
    }

    comparison_results = compare_decision_data(json_df, menora_df, field_map)

    if comparison_results:
        log_and_print(f"\n🔍 Detailed Decision Comparison for Case ID {case_id} (Matched by Moj_ID):", "info")
        log_and_print(tabulate(comparison_results, headers="keys", tablefmt="grid", showindex=False), "info")

        mismatches = [r for r in comparison_results if r["Match"] == "✗"]
        if not mismatches:
            log_and_print("✅ All matched decision fields are identical.", "success")
        else:
            log_and_print(f"⚠️ Found {len(mismatches)} decision field mismatches", "warning")
    else:
        log_and_print(f"⚠️ No matching mojIds found between Menora and JSON for decisions in case {case_id}.", "warning")

    count_comparison = compare_decision_counts(json_df, menora_df)
    if count_comparison:
        log_and_print("\n📊 Decision Count Comparison:", "info")
        log_and_print(tabulate(count_comparison, headers="keys", tablefmt="grid", showindex=False), "info")

def run_document_comparison(case_id, appeal_number):
    json_data = fetch_case_documents(case_id)
    if not json_data:
        log_and_print(f"❌ No JSON document data for case_id {case_id}", "error")
        return

    json_df = extract_document_data_from_json(json_data)
    menora_df = fetch_menora_document_data(appeal_number)

    # Normalize moj_id to mojId if necessary
    if "moj_id" in menora_df.columns:
        menora_df = menora_df.rename(columns={"moj_id": "mojId"})
    if "moj_id" in json_df.columns:
        json_df = json_df.rename(columns={"moj_id": "mojId"})

    field_map = {
        "document_Type_Id": "subType"
    }

    comparison_results = compare_document_data(json_df, menora_df, field_map)

    fully_matched = 0
    mismatches = [r for r in comparison_results if r["Match"] == "✗"]
    if comparison_results:
        unique_mojids = set(r["mojId"] for r in comparison_results)
        mismatched_ids = set(r["mojId"] for r in mismatches)
        fully_matched = len(unique_mojids - mismatched_ids)

    count_comparison = compare_document_counts(json_df, menora_df)
    missing_in_menora = 0
    missing_in_json = 0
    for row in count_comparison:
        if row["Type"] == "Missing in JSON":
            missing_in_json = row["Count"]
        elif row["Type"] == "Missing in Menora":
            missing_in_menora = row["Count"]

    log_and_print(f"\n🧪 Test Result Summary for Case ID {case_id}:", "info")
    log_and_print(f"{'✅' if not missing_in_menora else '❌'} {missing_in_menora} documents missing in Menora.", "info")
    log_and_print(f"{'✅' if not missing_in_json else '❌'} {missing_in_json} documents missing in JSON.", "info")
    log_and_print(f"{'✅' if not mismatches else '❌'} {len(mismatches)} field mismatches.", "info")
    log_and_print(f"✅ {fully_matched} documents fully matched across all fields.", "info")

    if comparison_results:
        log_and_print(f"\n🔍 Detailed Document Comparison for Case ID {case_id}:", "info")
        log_and_print(tabulate(comparison_results, headers="keys", tablefmt="grid"), "info")

    if count_comparison:
        log_and_print("\n📊 Document Count Comparison:", "info")
        log_and_print(tabulate(count_comparison, headers="keys", tablefmt="grid"), "info")

def main():
    load_configuration()
    case_ids = [2004759]
    #, 2005285, 2005281, 2005287]

    for case_id in case_ids:
        log_and_print(f"\n\n🔁 Processing case_id {case_id}...", "info")
        appeal_number = fetch_appeal_number_by_case_id(case_id)
        if not appeal_number:
            log_and_print(f"❌ Could not find appeal number for case ID {case_id}. Skipping.", "error")
            continue

        run_decision_comparison(case_id, appeal_number)
        run_document_comparison(case_id, appeal_number)

if __name__ == "__main__":
    main()