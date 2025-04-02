def compare_decision_data(json_df, menora_df, field_map):
    import pandas as pd

    merged = pd.merge(
        menora_df,
        json_df,
        left_on="Moj_ID",
        right_on="mojId",
        how="inner",
        suffixes=("_menora", "_json")
    )

    mismatches = []
    for _, row in merged.iterrows():
        for left, right in field_map.items():
            lval = row.get(f"{left}_menora")
            rval = row.get(f"{right}_json")
            if str(lval).strip() != str(rval).strip():
                mismatches.append({
                    "mojId": row["mojId"],
                    "field": left,
                    "menora": lval,
                    "json": rval
                })
    return mismatches
