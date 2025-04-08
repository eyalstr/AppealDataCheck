from tabulate import tabulate
import pandas as pd
from logging_utils import log_and_print


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

    json_df = json_df.rename(columns={"moj_id": "mojId"})
    menora_df = menora_df.rename(columns={"moj_id": "mojId"})

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

def extract_document_data_from_json(case_json):
    documents = case_json.get("documentList")

    if not documents:
        log_and_print("⚠️ No 'documentList' found in JSON.", "warning")
        return pd.DataFrame()

    extracted = []

    for doc in documents:
        moj_id = doc.get("mojId")
        sub_type = doc.get("subType")
        doc_type = doc.get("docType")

        if moj_id is None:
            log_and_print("⚠️ Skipping document without mojId.", "warning")
            continue
        
        if doc_type is None:
            log_and_print(f"⚠️ Document {moj_id} is missing 'docType'.", "warning")
        #elif doc_type not in DOCUMENT_TYPE_MAPPING:
        #    log_and_print(f"❓ Unknown 'docType' ID {doc_type} in document {moj_id}.", "warning")


        extracted.append({
            "mojId": moj_id,
            "subType": sub_type,
            "docType": doc_type
        })

    df = pd.DataFrame(extracted)

    if "mojId" not in df.columns:
        log_and_print(f"❌ Extracted document DataFrame is missing 'mojId' column. Columns: {df.columns.tolist()}", "error")

    return df


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
