from tabulate import tabulate
from dateutil.parser import parse
import pandas as pd
from logging_utils import log_and_print

def requestlog_values_match(field, val1, val2):
    # Normalize both values
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False

    # For datetime, compare without time zone differences
    if isinstance(val1, pd.Timestamp) and isinstance(val2, pd.Timestamp):
        return val1.normalize() == val2.normalize()

    # Trim and lower strings
    if isinstance(val1, str) and isinstance(val2, str):
        return val1.strip() == val2.strip()

    return val1 == val2

# In requestlog_comparator.py

def compare_request_logs(json_df, menora_df, field_map):
    import pandas as pd
    from collections import defaultdict

    json_df = json_df.copy()
    menora_df = menora_df.copy()

    # Rename for comparison
    json_df = json_df.rename(columns={v: f"{v}_json" for v in field_map.values()})
    menora_df = menora_df.rename(columns={k: f"{k}_menora" for k in field_map.keys()})

    # Round datetime columns to the nearest second and sort for asof
    json_df["Status_Date_json"] = pd.to_datetime(json_df["Status_Date_json"]).dt.round("s")
    menora_df["Status_Date_menora"] = pd.to_datetime(menora_df["Status_Date_menora"]).dt.round("s")

    json_df = json_df.sort_values("Status_Date_json")
    menora_df = menora_df.sort_values("Status_Date_menora")

    # Merge using tolerance of 1 second
    merged = pd.merge_asof(
        menora_df,
        json_df,
        left_on="Status_Date_menora",
        right_on="Status_Date_json",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=1)
    )

    results = []
    mismatches = defaultdict(list)

    for _, row in merged.iterrows():
        row_key = row.get("Status_Date_menora")
        for menora_field, json_field in field_map.items():
            left = row.get(f"{menora_field}_menora", "⛔")
            right = row.get(f"{json_field}_json", "⛔")

            if pd.isna(left):
                left = "⛔"
            if pd.isna(right):
                right = "⛔"

            match = requestlog_values_match(menora_field, left, right)
            results.append({
                "Status_Date": row_key,
                "Field": menora_field,
                "Menora Value": left,
                "JSON Value": right,
                "Match": "✓" if match else "✗"
            })

            if not match:
                mismatches[row_key].append(menora_field)

    if mismatches:
        log_and_print(f"❌ Found mismatches in {len(mismatches)} request log entries.", "warning")
        for status_date, fields in mismatches.items():
            log_and_print(f"  🔍 Status_Date {status_date} mismatched fields: {', '.join(fields)}", "info")
    else:
        log_and_print("✅ All matched request log fields are identical.", "success")

    return results
