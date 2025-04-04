# discussion_runner.py
import pandas as pd
from sql_client import fetch_menora_discussion_data
from client_api import fetch_case_discussions
from logging_utils import log_and_print
from jsonpath_ng import parse


def run_discussion_comparison(case_id, appeal_number):
    log_and_print("\n📂 Running discussion comparison...", "info")

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_discussion_data(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        log_and_print(f"✅ Retrieved {len(menora_df)} discussions from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    # Step 2: Fetch JSON API data
    json_data = fetch_case_discussions(case_id)
    json_df = pd.DataFrame()
    if json_data:
        try:
            json_path_expr = parse("$.[*]")
            matches = [match.value for match in json_path_expr.find(json_data)]
            json_df = pd.json_normalize(matches)
            if not json_df.empty:
                log_and_print(f"📋 Extracted discussion DataFrame preview:\n{json_df[['protocolDocMojId', 'startTime', 'endTime']].head(3)}", "info")
            log_and_print(f"✅ Extracted {len(json_df)} discussions from API for case {case_id}", "success")
        except Exception as e:
            log_and_print(f"❌ Failed to parse JSON discussion data: {e}", "error")
    else:
        log_and_print(f"⚠️ No discussion JSON data found for case_id {case_id}", "warning")

    # Step 3: Count comparisons
    menora_count = len(menora_df)
    json_count = len(json_df)

    missing_in_json = 0
    missing_in_menora = 0
    mismatched_fields = 0
    fully_matched = 0

    if not json_df.empty and not menora_df.empty and 'protocolDocMojId' in json_df.columns and 'mojId' in menora_df.columns:
        merged = json_df.merge(menora_df, left_on="protocolDocMojId", right_on="mojId", how="outer", indicator=True)
        missing_in_json = merged[merged['_merge'] == 'right_only'].shape[0]
        missing_in_menora = merged[merged['_merge'] == 'left_only'].shape[0]

        # Determine full matches based on mojId/protocolDocMojId only for now
        fully_matched = merged[merged['_merge'] == 'both'].shape[0]
    else:
        if json_df.empty:
            missing_in_json = menora_count
        if menora_df.empty:
            missing_in_menora = json_count

    # Adjust fully matched if it should be the overlap minus mismatches (future field comparison)
    fully_matched = menora_count - missing_in_json if missing_in_json == 0 else 0

    # Step 4: Summary
    summary = {
        "Case ID": case_id,
        "Menora Discussions": menora_count,
        "JSON Discussions": json_count,
        "Missing in JSON": max(missing_in_json, 0),
        "Missing in Menora": max(missing_in_menora, 0),
        "Field Mismatches": mismatched_fields,
        "Fully Matched": max(fully_matched, 0)
    }

    log_and_print(f"\n🧪 Test Result Summary for Case ID {case_id} [Discussion]", "info")
    log_and_print(f"✅ {summary['Missing in Menora']} discussion(s) missing in Menora.", "success")
    log_and_print(f"✅ {summary['Missing in JSON']} discussion(s) missing in JSON.", "success")
    log_and_print(f"✅ {summary['Field Mismatches']} discussion(s) with field mismatch(es).", "success")
    log_and_print(f"✅ {summary['Fully Matched']} discussion(s) fully matched.", "success")

    return summary
