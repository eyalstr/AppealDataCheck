from client_api import fetch_case_details
from sql_client import fetch_menora_decision_data
from json_parser import extract_decision_data_from_json
from logging_utils import log_and_print
from config_loader import load_tab_config
from tabulate import tabulate
from dateutil.parser import parse
import pandas as pd
from collections import defaultdict

def run_decision_comparison(case_id: int, appeal_number: int):
    tab_config = load_tab_config("החלטות")
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    log_and_print("\n📂 Running decision comparison...", "info")

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_decision_data(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        log_and_print(f"✅ Retrieved {len(menora_df)} decisions from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    # Step 2: Fetch JSON API data
    json_data = fetch_case_details(case_id)
    json_df = pd.DataFrame()

    if json_data:
        try:
            json_df = extract_decision_data_from_json(json_data)

            for ui_field, _ in field_map.items():
                if ui_field in json_df.columns and not ui_field.endswith("_json"):
                    json_df.rename(columns={ui_field: f"{ui_field}_json"}, inplace=True)

            json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()

            if not json_df.empty:
                preview_cols = [col for col in ['mojId', 'decisionDate_json', 'createUser_json'] if col in json_df.columns]
                log_and_print(f"📋 Extracted decision DataFrame preview:\n{json_df[preview_cols].head(3)}", "info")
            log_and_print(f"✅ Extracted {len(json_df)} decisions from API for case {case_id}", "success")

        except Exception as e:
            log_and_print(f"❌ Failed to parse JSON decision data: {e}", "error")
            json_df = pd.DataFrame()
    else:
        log_and_print(f"⚠️ No decision JSON data found for case_id {case_id}", "warning")

    # Step 3: Field Comparison
    comparison_results = compare_decision_data(json_df, menora_df, field_map)
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

    # Step 4: Count Comparison
    count_comparison = compare_decision_counts(json_df, menora_df)
    if count_comparison:
        for row in count_comparison:
            if row["Type"] == "Missing in JSON":
                test_status["missing_in_json"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]
            elif row["Type"] == "Missing in Menora":
                test_status["missing_in_menora"] = [moj.strip() for moj in row["mojIds"].split(",") if moj.strip()]

    # Step 5: Summary
    log_and_print(f"\n🧪 Test Result Summary for Case ID {case_id} [Decisions]:", "info")
    log_and_print(f"{'✅' if not test_status['missing_in_menora'] else '❌'} {len(test_status['missing_in_menora'])} decision(s) missing in Menora.", "info")
    log_and_print(f"{'✅' if not test_status['missing_in_json'] else '❌'} {len(test_status['missing_in_json'])} decision(s) missing in JSON.", "info")
    log_and_print(f"{'✅' if not test_status['field_mismatches'] else '❌'} {len(mismatch_by_mojid)} decision(s) with field mismatch(es).", "info")
    log_and_print(f"✅ {len(test_status['fully_matched_mojIds'])} decision(s) fully matched.", "info")

    return {
        "case_id": case_id,
        "missing_in_menora": len(test_status["missing_in_menora"]),
        "missing_in_json": len(test_status["missing_in_json"]),
        "field_mismatches": len(mismatch_by_mojid),
        "fully_matched": len(test_status["fully_matched_mojIds"]),
    }

def values_match(field, menora_value, json_value):
    menora_str = str(menora_value).strip()
    json_str = str(json_value).strip()

    if field in ["document_Type_Id", "Source_Type"]:
        try:
            return int(menora_value) == int(json_value)
        except (TypeError, ValueError):
            return False

    if field.lower().endswith("date"):
        try:
            return parse(menora_str).replace(tzinfo=None) == parse(json_str).replace(tzinfo=None)
        except Exception:
            return False

    return menora_str.lower() == json_str.lower()


def compare_decision_data(json_df, menora_df, field_map):
    import pandas as pd
    from collections import defaultdict
    from tabulate import tabulate

    # Normalize 'mojId' for merge
    menora_df = menora_df.rename(columns={"Moj_ID": "mojId"})
    json_df = json_df.rename(columns={"moj_id": "mojId"})  # Just in case

    # Apply suffixes manually to comparison fields
    menora_df = menora_df.rename(columns={k: f"{k}_menora" for k in field_map.keys()})
    json_df = json_df.rename(columns={v: f"{v}_json" for v in field_map.values()})

    print("📋 Menora columns:", menora_df.columns.tolist())
    print("📋 JSON columns:", json_df.columns.tolist())

    # Merge both datasets on 'mojId'
    merged = pd.merge(menora_df, json_df, on="mojId", how="inner")

    results = []

    # Compare fields row by row
    for _, row in merged.iterrows():
        for menora_field, json_field in field_map.items():
            left = row.get(f"{menora_field}_menora", "⛔")
            right = row.get(f"{json_field}_json", "⛔")

            match = values_match(menora_field, left, right)

            results.append({
                "mojId": row["mojId"],
                "Field": menora_field,
                "Menora Value": left,
                "JSON Value": right,
                "Match": "✓" if match else "✗"
            })

    # Group mismatches for table output
    mismatches = defaultdict(list)
    for row in results:
        if row["Match"] == "✗":
            mismatches[row["mojId"]].append({
                "Field": row["Field"],
                "Menora Value": row["Menora Value"],
                "JSON Value": row["JSON Value"]
            })

    if not results:
        print("⚠️ No decision comparison results found.")
    elif mismatches:
        print(f"❌ Found mismatches in {len(mismatches)} decisions.")
        for moj_id, rows in mismatches.items():
            print(f"\n🔎 Mismatched Fields for mojId {moj_id}:")
            print(tabulate(rows, headers="keys", tablefmt="grid", showindex=False))
    else:
        print("✅ All matched decision fields are identical.")

    return results


def compare_decision_counts(json_df, menora_df):
    # Normalize column names just like in compare_decision_data
    if "Moj_ID" in menora_df.columns:
        menora_df = menora_df.rename(columns={"Moj_ID": "mojId"})

    json_ids = set(json_df["mojId"])
    menora_ids = set(menora_df["mojId"])

    menora_only = menora_ids - json_ids
    json_only = json_ids - menora_ids

    results = []

    if menora_only:
        results.append({
            "Type": "Missing in JSON",
            "Count": len(menora_only),
            "mojIds": ", ".join(sorted(str(i) for i in menora_only if i is not None))
        })

    if json_only:
        results.append({
            "Type": "Missing in Menora",
            "Count": len(json_only),
            "mojIds": ", ".join(sorted(str(i) for i in json_only if i is not None)) or "(no mojIds)"

        })

    if not results:
        results.append({
            "Type": "✅ Count Match",
            "Count": len(json_ids),
            "mojIds": "-"
        })

    return results
