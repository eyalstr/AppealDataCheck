from tabulate import tabulate
from dateutil.parser import parse
from dateutil.parser import parse

def values_match(field, menora_value, json_value):
    menora_str = str(menora_value).strip()
    json_str = str(json_value).strip()

    if field == "Create_User":
        return menora_str.lower() == json_str.lower()

    if field == "Decision_Date":
        try:
            # Parse and remove timezone info for fair comparison
            menora_dt = parse(menora_str).replace(tzinfo=None)
            json_dt = parse(json_str).replace(tzinfo=None)
            return menora_dt == json_dt
        except Exception:
            return False

    return menora_str == json_str




def compare_decision_data(json_df, menora_df, field_map):
    import pandas as pd

    # Normalize column names
    #menora_df = menora_df.rename(columns={"Moj_ID": "mojId"})
    menora_df = menora_df.rename(columns={"Moj_ID": "mojId"})
    for k in field_map.keys():
        if k in menora_df.columns:
            menora_df = menora_df.rename(columns={k: f"{k}_menora"})

    for v in field_map.values():
        if v in json_df.columns:
            json_df = json_df.rename(columns={v: f"{v}_json"})

    print("📋 Menora columns:", menora_df.columns.tolist())
    print("📋 JSON columns:", json_df.columns.tolist())
      
    # Merge on mojId
    merged = pd.merge(
        menora_df,
        json_df,
        on="mojId",
        how="inner",
        suffixes=("_menora", "_json")
    )

    results = []

    for _, row in merged.iterrows():
        for menora_field, json_field in field_map.items():
            left_val = row.get(f"{menora_field}_menora", "⛔")
            right_val = row.get(f"{json_field}_json", "⛔")
            results.append({
                "mojId": row["mojId"],
                "Field": menora_field,
                "Menora Value": left_val,
                "JSON Value": right_val,
                "Match": "✓" if values_match(menora_field, left_val, right_val) else "✗"
            })

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