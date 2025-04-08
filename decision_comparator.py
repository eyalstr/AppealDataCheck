from tabulate import tabulate
from dateutil.parser import parse

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
