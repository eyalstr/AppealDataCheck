import pandas as pd
from sql_client import fetch_case_contacts_data
from client_api import fetch_case_details
from logging_utils import log_and_print
from jsonpath_ng import parse


def extract_contacts_from_json(json_data):
    path = parse("$.roleInCorporationDetails[*]")
    matches = [match.value for match in path.find(json_data)]
    extracted = []
    for match in matches:
        corporation_id = match.get("corporationDetails", {}).get("corporationIDNumber")
        email = match.get("connectDetails", {}).get("mail")
        phone1 = match.get("connectDetails", {}).get("primaryPhone")
        phone2 = match.get("connectDetails", {}).get("secondaryPhone")

        extracted.append({
            "Main_Id_Number": corporation_id,
            "orerEmail": email,
            "Phone1": phone1,
            "Phone2": phone2,
        })
    return pd.DataFrame(extracted)


def run_case_contacts_comparison(case_id):
    log_and_print("\n📂 Running case contact comparison...", "info")

    try:
        menora_df = fetch_case_contacts_data(case_id)
        log_and_print(f"✅ Retrieved {len(menora_df)} contact entries from Menora.", "success")
        log_and_print(f"📋 Menora data preview:\n{menora_df.head(5)}", "info")
    except Exception as e:
        log_and_print(f"❌ Failed to fetch Menora contacts: {e}", "error")
        return {}

    full_json = fetch_case_details(case_id)
    json_df = extract_contacts_from_json(full_json)
    log_and_print(f"📋 Extracted {len(json_df)} contact entries from JSON.", "info")
    log_and_print(f"📋 JSON data preview:\n{json_df.head(5)}", "info")

    merged = pd.merge(
        menora_df,
        json_df,
        on="Main_Id_Number",
        how="outer",
        suffixes=("_menora", "_json"),
        indicator=True
    )

    field_mismatches = 0
    for _, row in merged.iterrows():
        if row['_merge'] == 'both':
            for field in ["orerEmail", "Phone1", "Phone2"]:
                menora_val = str(row.get(f"{field}_menora") or '').strip()
                json_val = str(row.get(f"{field}_json") or '').strip()
                if menora_val != json_val:
                    log_and_print(f"🔍 Main_Id_Number {row['Main_Id_Number']} mismatched {field}:", "warning")
                    log_and_print(f"    Menora: {menora_val}", "warning")
                    log_and_print(f"    JSON:   {json_val}", "warning")
                    field_mismatches += 1

    missing_in_json = merged['_merge'].value_counts().get('left_only', 0)
    missing_in_menora = merged['_merge'].value_counts().get('right_only', 0)
    matched = len(json_df) - missing_in_menora

    summary = {
        "Case ID": case_id,
        "Menora Contacts": len(menora_df),
        "JSON Contacts": len(json_df),
        "Missing in JSON": missing_in_json,
        "Missing in Menora": missing_in_menora,
        "Field Mismatches": field_mismatches,
        "Fully Matched": matched - field_mismatches
    }

    log_and_print(f"\n📊 Test Result Summary for Case ID {case_id} [Contact Tab]", "info")
    log_and_print(f"✅ {summary['Missing in Menora']} contact(s) missing in Menora.", "success")
    log_and_print(f"✅ {summary['Missing in JSON']} contact(s) missing in JSON.", "success")
    log_and_print(f"✅ {summary['Field Mismatches']} contact(s) with field mismatch(es).", "success")
    log_and_print(f"✅ {summary['Fully Matched']} contact(s) fully matched.", "success")

    return summary
