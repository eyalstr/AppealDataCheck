from tabulate import tabulate

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
                "Match": "✓" if str(left_val).strip() == str(right_val).strip() else "✗"
            })

    return results
