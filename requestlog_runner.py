import pandas as pd
from sql_client import fetch_menora_log_requests
from client_api import fetch_case_details
from logging_utils import log_and_print
from json_parser import extract_request_logs_from_json
from config_loader import load_tab_config
from dateutil.parser import parse
def run_request_log_comparison(case_id, appeal_number):
    from config_loader import load_tab_config
    from client_api import fetch_case_details
    from sql_client import fetch_menora_log_requests
    from json_parser import extract_request_logs_from_json
    from logging_utils import log_and_print
    from dateutil.parser import parse
    import pandas as pd

    tab_config = load_tab_config("יומן תיק")
    matching_keys = tab_config.get("matchingKeys", [])
    field_map = matching_keys[0].get("columns", {}) if matching_keys else {}

    try:
        menora_df = fetch_menora_log_requests(appeal_number)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
    except Exception as e:
        log_and_print(f"❌ SQL query execution failed: {e}", "error")
        menora_df = pd.DataFrame()

    full_json = fetch_case_details(case_id)
    json_df = extract_request_logs_from_json(full_json)

    if "createActionDate" in json_df.columns:
        json_df.rename(columns={"createActionDate": "Status_Date"}, inplace=True)

    for ui_field, _ in field_map.items():
        if ui_field in json_df.columns and not ui_field.endswith("_json"):
            json_df.rename(columns={ui_field: f"{ui_field}_json"}, inplace=True)

    if "Status_Date" in json_df.columns:
        json_df.rename(columns={"Status_Date": "Status_Date_json"}, inplace=True)

    json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()

    def safe_parse_datetime(value):
        try:
            return parse(value).strftime("%m-%d %H:%M:%S")
        except Exception:
            return None

    for df, label in [(menora_df, "Menora"), (json_df, "JSON")]:
        col_name = "Status_Date" if label == "Menora" else "Status_Date_json"
        if col_name in df.columns:
            try:
                dt_series = df[col_name].astype(str).apply(safe_parse_datetime)
                df[col_name] = dt_series
            except Exception as e:
                log_and_print(f"❌ Failed to normalize '{col_name}' in {label}: {e}", "error")

    menora_keys = set(menora_df["Status_Date"].dropna())
    json_keys = set(json_df["Status_Date_json"].dropna())

    missing_json_dates = sorted(list(menora_keys - json_keys))
    missing_menora_dates = sorted(list(json_keys - menora_keys))

    expected_json_fields = [f"{v}_json" for v in field_map.values()]
    available_json_fields = list(json_df.columns)
    valid_json_fields = [col for col in expected_json_fields if col in available_json_fields]

    if len(valid_json_fields) < len(expected_json_fields):
        missing = set(expected_json_fields) - set(valid_json_fields)
        log_and_print(f"⚠️ Warning: Missing expected JSON columns: {missing}", "warning")

    merged = pd.merge(
        menora_df[["Status_Date", *field_map.keys()]],
        json_df[["Status_Date_json", *valid_json_fields]],
        left_on="Status_Date",
        right_on="Status_Date_json",
        how="inner"
    )

    mismatched_fields = []

    for _, row in merged.iterrows():
        status_date = row["Status_Date"]
        for menora_col, json_col_base in field_map.items():
            json_col = f"{json_col_base}_json"
            if json_col not in row:
                continue
            menora_val = str(row.get(menora_col, "")).strip()
            json_val = str(row.get(json_col, "")).strip()
            if menora_val != json_val:
                mismatched_fields.append({
                    "Status_Date": status_date,
                    "Field": menora_col,
                    "Menora": menora_val,
                    "JSON": json_val
                })

    if not missing_json_dates and not missing_menora_dates and not mismatched_fields:
        status_tab = "pass"
        log_and_print(f"🟡 יומן תיק - PASS", "info")
    else:
        status_tab = "fail"
        log_and_print(f"❌ יומן תיק - FAIL with mismatches or missing entries.", "warning")

    return {
        "request_log": {
            "status_tab": status_tab,
            "missing_json_dates": missing_json_dates,
            "missing_menora_dates": missing_menora_dates,
            "mismatched_fields": mismatched_fields
        }
    }
