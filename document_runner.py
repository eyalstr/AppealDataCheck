from config_loader import load_tab_config
from client_api import fetch_case_documents
from json_parser import extract_document_data_from_json
from sql_client import fetch_menora_document_data
from logging_utils import log_and_print
from tabulate import tabulate
from collections import defaultdict
import pandas as pd

def run_document_comparison(case_id, appeal_number):
    tab_config = load_tab_config("מסמכים")
    log_and_print(f"📁 Available tabs in config: {list(tab_config.keys())}", is_hebrew=True)

    json_data = fetch_case_documents(case_id)
    if not json_data:
        log_and_print(f"❌ Failed to fetch JSON documents for case_id {case_id}.", "error")
        return

    # Extract documents from JSON response
    documents = json_data.get("documentList")
    if not documents:
        log_and_print("⚠️ No 'documentList' found in JSON.", "warning")
        return

    # Extract dataframes from sources
    json_df = extract_document_data_from_json(documents)
    menora_df = fetch_menora_document_data(appeal_number)

    # Field map config
    field_map = tab_config["field_map"]

    # Compare field values
    comparison_results = compare_document_data(json_df, menora_df, field_map)

    # Prepare tracking
    mismatch_by_mojid = defaultdict(list)
    mojid_match_counts = defaultdict(lambda: {"total": 0, "matched": 0})

    test_status = {
        "missing_in_json": [],
        "missing_in_menora": [],
        "field_mismatches": [],
        "fully_matched_mojIds": set(),
    }

    # Process comparison results
    for row in comparison_results:
        moj = row["mojId"]
        mojid_match_counts[moj]["total"] += 1

        if row["Match"] == "✓":
            mojid_match_counts[moj]["matched"] += 1
        else:
            field_info = {
                "Field": row["Field"],
                "Menora Value": row["Menora Value"],
                "JSON Value": row["JSON Value"]
            }
            test_status["field_mismatches"].append(row)
            mismatch_by_mojid[moj].append(field_info)

    # Identify fully matched mojIds
    test_status["fully_matched_mojIds"] = {
        moj for moj, counts in mojid_match_counts.items()
        if counts["total"] == counts["matched"]
    }

    # Compare document counts
    count_comparison = compare_document_counts(json_df, menora_df)
    if count_comparison:
        for row in count_comparison:
            if row["Type"] == "Missing in JSON":
                test_status["missing_in_json"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]
            elif row["Type"] == "Missing in Menora":
                test_status["missing_in_menora"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]

    # Logging results
    log_and_print(f"\n🧪 Test Result Summary for Case ID {case_id} [Documents]:", "info")
    log_and_print(f"{'✅' if not test_status['missing_in_menora'] else '❌'} {len(test_status['missing_in_menora'])} document(s) missing in Menora.", "info")
    log_and_print(f"{'✅' if not test_status['missing_in_json'] else '❌'} {len(test_status['missing_in_json'])} document(s) missing in JSON.", "info")
    log_and_print(f"{'✅' if not test_status['field_mismatches'] else '❌'} {len(mismatch_by_mojid)} document(s) with field mismatch(es).", "info")
    log_and_print(f"✅ {len(test_status['fully_matched_mojIds'])} document(s) fully matched.", "info")

    for moj_id, mismatches in mismatch_by_mojid.items():
        log_and_print(f"🔎 Mismatched Fields for mojId {moj_id}:", "warning")
        print(tabulate(mismatches, headers="keys", tablefmt="grid"))

    status_tab = "pass" if not test_status["missing_in_menora"] and not test_status["missing_in_json"] and not test_status["field_mismatches"] else "fail"

    return {
        "document": {
            "status_tab": status_tab,
            "missing_json_dates": test_status["missing_in_json"],
            "missing_menora_dates": test_status["missing_in_menora"],
            "mismatched_fields": [
                {
                    "Status_Date": row["mojId"],
                    "Field": row["Field"],
                    "Menora": row["Menora Value"],
                    "JSON": row["JSON Value"]
                }
                for row in test_status["field_mismatches"]
            ]
        }
    }

from dateutil.parser import parse
from tabulate import tabulate
from logging_utils import log_and_print


def values_match(field, menora_value, json_value):
    menora_str = str(menora_value).strip()
    json_str = str(json_value).strip()

    if field == "document_Type_Id":
        return menora_str == json_str

    if field.lower().endswith("date"):
        try:
            return parse(menora_str).replace(tzinfo=None) == parse(json_str).replace(tzinfo=None)
        except Exception:
            return False

    return menora_str.lower() == json_str.lower()


def compare_document_data(json_df, menora_df, field_map):
    import pandas as pd
    from collections import defaultdict

    # Unify the merge key
    # print("Before renaming JSON:")
    # print(json_df[["mojId", "doc_type"]].head(10))


    menora_df = menora_df.rename(columns={"moj_id": "mojId"})
    json_df = json_df.rename(columns={"moj_id": "mojId"})  # Just in case, even though yours looks good

    # Apply suffixes manually for fields being compared
    menora_df = menora_df.rename(columns={k: f"{k}_menora" for k in field_map.keys()})
    json_df = json_df.rename(columns={v: f"{v}_json" for v in field_map.values()})

    # print("After renaming JSON:")
    # print(json_df[["mojId", "doc_type_json"]].head(10))


    log_and_print(f"Menora columns: {menora_df.columns.tolist()}", is_hebrew=True)
    log_and_print(f"JSON columns: {json_df.columns.tolist()}", is_hebrew=True)

    merged = pd.merge(menora_df, json_df, on="mojId", how="inner")


    merged = pd.merge(menora_df, json_df, on="mojId", how="inner", suffixes=("_menora", "_json"))

    results = []
    mismatches = defaultdict(list)

    for _, row in merged.iterrows():
        for menora_field, json_field in field_map.items():
            left = row.get(f"{menora_field}_menora")
            right = row.get(f"{json_field}_json")

            if pd.isna(left):
                left = "⛔"
            if pd.isna(right):
                right = "⛔"

            match = values_match(menora_field, left, right)

            results.append({
                "mojId": row["mojId"],
                "Field": menora_field,
                "Menora Value": left,
                "JSON Value": right,
                "Match": "✓" if match else "✗"
            })

            if not match:
                mismatches[row["mojId"]].append(menora_field)


    if not results:
        log_and_print("⚠️ No document comparison results found.", "warning")

    if mismatches:
        log_and_print(f"❌ Found mismatches in {len(mismatches)} documents.", "warning")
        for moj_id, fields in mismatches.items():
            log_and_print(f"  🔍 mojId {moj_id} mismatched fields: {', '.join(fields)}", "info")
    else:
        log_and_print("✅ All matched document fields are identical.", "success")

    return results


def compare_document_counts(json_df, menora_df):
    # Normalize column names to match 'mojId'
    if "moj_id" in menora_df.columns:
        menora_df = menora_df.rename(columns={"moj_id": "mojId"})
    if "moj_id" in json_df.columns:
        json_df = json_df.rename(columns={"moj_id": "mojId"})

    json_ids = set(json_df["mojId"].dropna())
    menora_ids = set(menora_df["mojId"].dropna())

    menora_only = menora_ids - json_ids
    json_only = json_ids - menora_ids

    results = []

    if menora_only:
        results.append({
            "Type": "Missing in JSON",
            "Count": len(menora_only),
            "mojIds": ", ".join(sorted(menora_only))
        })

    if json_only:
        results.append({
            "Type": "Missing in Menora",
            "Count": len(json_only),
            "mojIds": ", ".join(sorted(json_only))
        })

    if not results:
        results.append({
            "Type": "✅ Count Match",
            "Count": len(json_ids),
            "mojIds": "-"
        })

    return results
