# comparison_utils.py
import pandas as pd
from logging_utils import log_and_print
from collections import defaultdict

def compare_generic_data(json_df, sql_df, field_map, join_key='mojId', tab_name='Tab'):
    results = []
    mismatches = defaultdict(list)

    # Normalize join key names
    json_df = json_df.rename(columns={join_key: 'mojId'})
    sql_df = sql_df.rename(columns={join_key: 'mojId'})

    merged = pd.merge(sql_df, json_df, on='mojId', suffixes=('_sql', '_json'))

    for _, row in merged.iterrows():
        for sql_field, json_field in field_map.items():
            left = row.get(f"{sql_field}_sql", "⛔")
            right = row.get(f"{json_field}_json", "⛔")
            match = str(left).strip() == str(right).strip()
            results.append({
                "mojId": row['mojId'],
                "Field": sql_field,
                "SQL Value": left,
                "JSON Value": right,
                "Match": "✓" if match else "✗"
            })
            if not match:
                mismatches[row['mojId']].append(sql_field)

    if mismatches:
        log_and_print(f"❌ Found mismatches in {len(mismatches)} {tab_name.lower()}(s).", "warning")
        for moj_id, fields in mismatches.items():
            log_and_print(f"  🔍 mojId {moj_id} mismatched fields: {', '.join(fields)}", "info")
    else:
        log_and_print(f"✅ All matched {tab_name.lower()} fields are identical.", "success")

    return results
