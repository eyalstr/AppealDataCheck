import pandas as pd
from sql_client import fetch_menora_distributions
from client_api import fetch_distribution_data
from logging_utils import log_and_print
from tabulate import tabulate
from logging_utils import normalize_hebrew
from collections import defaultdict


def run_distribution_comparison(case_id, appeal_number):
    log_and_print("\n📂 Running distribution comparison...", "info")

    # Step 1: Fetch Menora SQL data
    try:
        menora_df = fetch_menora_distributions(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df["SendDate"] = pd.to_datetime(menora_df["SendDate"], errors="coerce")
        menora_df["SendDate"] = menora_df["SendDate"].dt.strftime("%Y-%m-%d %H:%M:%S")
        log_and_print(f"✅ Retrieved {len(menora_df)} distributions from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        return {}

    # Step 2: Fetch JSON API data
    try:
        json_data = fetch_distribution_data(case_id)
        if not json_data:
            raise ValueError("Empty or invalid JSON returned from API")

        json_df = pd.json_normalize(json_data)
        json_df.rename(columns={
            "createDate": "SendDate",
            "subject": "SendSubject"
        }, inplace=True)

        json_df["SendDate"] = pd.to_datetime(json_df["SendDate"], errors="coerce")
        json_df["SendDate"] = json_df["SendDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # log_and_print(f"📋 Extracted distribution DataFrame preview:\n{json_df.head(3)}", "info", is_hebrew=True)
        # log_and_print(f"✅ Extracted {len(json_df)} distributions from API for case {case_id}", "success")

    except Exception as e:
        log_and_print(f"❌ Failed to fetch or parse distribution data: {e}", "error")
        return {}

    # Step 3: Compare based on (SendDate + SendTo) tuple
    try:
        menora_df["SendKey"] = menora_df["SendDate"].astype(str) + "|" + menora_df["SendTo"].astype(str).str.strip()
        json_df["SendKey"] = json_df["SendDate"].astype(str) + "|" + json_df["sendTo"].astype(str).str.strip()

        menora_keys = set(menora_df["SendKey"].dropna().unique())
        json_keys = set(json_df["SendKey"].dropna().unique())

        # log_and_print(f"🔎 Unique Menora SendKeys: {sorted(menora_keys)}", "debug")
        # log_and_print(f"🔎 Unique JSON SendKeys: {sorted(json_keys)}", "debug")

        missing_in_json = menora_keys - json_keys
        missing_in_menora = json_keys - menora_keys
        fully_matched = json_keys & menora_keys

        summary = {
            "Case ID": case_id,
            "Menora Distributions": len(menora_df),
            "JSON Distributions": len(json_df),
            "Missing in JSON": len(missing_in_json),
            "Missing in Menora": len(missing_in_menora),
            "Fully Matched": len(fully_matched)
        }

        log_and_print(f"\n📊 Test Result Summary for Case ID {case_id} [Distributions]", "info")
        log_and_print(f"✅ {summary['Missing in Menora']} distribution(s) missing in Menora.", "success")
        log_and_print(f"✅ {summary['Missing in JSON']} distribution(s) missing in JSON.", "success")
        log_and_print(f"✅ {summary['Fully Matched']} distribution(s) fully matched.", "success")

        return summary

    except Exception as e:
        log_and_print(f"❌ Error during distribution comparison logic: {e}", "error")
        return {}


