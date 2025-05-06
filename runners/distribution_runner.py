import pandas as pd
from apis.sql_client import fetch_menora_distributions
from utils.fetcher import fetch_distribution_data
from utils.logging_utils import log_and_print
from tabulate import tabulate
from utils.logging_utils import normalize_hebrew
from collections import defaultdict
from configs.config_loader import load_tab_config
from dateutil.parser import parse
import json
import os

import pandas as pd
from apis.sql_client import fetch_menora_distributions
from utils.fetcher import fetch_distribution_data
from utils.logging_utils import log_and_print
from tabulate import tabulate
from utils.logging_utils import normalize_hebrew
from collections import defaultdict
from configs.config_loader import load_tab_config
from dateutil.parser import parse
import json as json_module
import os

def run_distribution_comparison(case_id, appeal_number, conn, tab_config=None):
    tab_key = "distribution"
    tab_label = "תוצפה"
    log_and_print(f"\n📂 Running {tab_label} comparison...", "info")

    if tab_config is None:
        tab_config = load_tab_config(tab_label)

    matching_keys = tab_config.get("matchingKeys", [])
    key_sql = matching_keys[0].get("key", "SendDate")
    key_json = matching_keys[0].get("jsonPath", "createDate").split(".")[-1]
    field_map = matching_keys[0].get("columns", {})
    json_subject_field = list(field_map.values())[0] if field_map else "subject"
    sql_subject_field = list(field_map.keys())[0] if field_map else "SendSubject"

    try:
        menora_df = fetch_menora_distributions(appeal_number, conn)
        menora_df = menora_df.rename(columns=lambda x: x.strip())
        menora_df = menora_df.loc[:, ~menora_df.columns.duplicated()].copy()
        menora_df[key_sql] = pd.to_datetime(menora_df[key_sql], errors="coerce")
        menora_df[key_sql] = menora_df[key_sql].dt.floor("s")
        menora_df[f"{key_sql}_str"] = menora_df[key_sql].dt.strftime("%Y-%m-%d %H:%M:%S")
        menora_df[sql_subject_field] = menora_df[sql_subject_field].fillna("").astype(str).str.strip()
        log_and_print(f"✅ Retrieved {len(menora_df)} distributions from Menora for appeal {appeal_number}", "success")
    except Exception as e:
        log_and_print(f"❌ Menora fetch error: {e}", "error")
        return {tab_key: {"status_tab": "error", "error": str(e)}}

    try:
        cache_path = f"data/{case_id}/dist_{case_id}.json"
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                json_data = json_module.load(f)
            log_and_print(f"📁 Loaded distribution data from cache: {cache_path}", "debug")
        else:
            json_data = fetch_distribution_data(case_id)
            with open(cache_path, "w", encoding="utf-8") as f:
                json_module.dump(json_data, f, ensure_ascii=False, indent=2)

        json_df = pd.json_normalize(json_data)

        def safe_parse(val):
            try:
                return pd.to_datetime(val, utc=True)
            except Exception:
                return pd.NaT

        json_df[key_json] = json_df[key_json].apply(safe_parse)

        #log_and_print(f"🔎 Raw {key_json} values before parsing:", "debug")
        # for idx, raw_entry in enumerate(json_data):
        #     raw_val = raw_entry.get(key_json, "MISSING")
        #     parsed_val = json_df.at[idx, key_json]
        #     log_and_print(f"  - Raw: {raw_val} | Parsed: {parsed_val}", "debug")

        # unparsed = json_df[json_df[key_json].isna()]
        # if not unparsed.empty:
        #     log_and_print(f"❗ Unparsed {key_json} values in JSON (NaT count = {len(unparsed)}):", "warning")
        #     for i in unparsed.index:
        #         raw_val = json_data[i].get(key_json, "MISSING")
        #         log_and_print(f"  - Index {i} → Raw: {raw_val}", "debug")

        if not pd.api.types.is_datetime64_any_dtype(json_df[key_json]):
            raise ValueError(f"{key_json} column could not be converted to datetime.")

        json_df[key_json] = json_df[key_json].dt.tz_convert("Asia/Jerusalem").dt.tz_localize(None)
        json_df[key_json] = json_df[key_json].dt.floor("s")
        json_df[f"{key_json}_str"] = json_df[key_json].dt.strftime("%Y-%m-%d %H:%M:%S")
        json_df[json_subject_field] = json_df[json_subject_field].fillna("").astype(str).str.strip()

        log_and_print(f"✅ Extracted {len(json_df)} distributions from JSON", "success")
    except Exception as e:
        log_and_print(f"❌ JSON parse error: {e}", "error")
        return {tab_key: {"status_tab": "error", "error": str(e)}}

    menora_group = menora_df.groupby(f"{key_sql}_str")
    json_group = json_df.groupby(f"{key_json}_str")

    mismatched_fields = []
    missing_in_json = []
    missing_in_menora = []

    for date_key, menora_rows in menora_group:
        if date_key not in json_group.groups:
            log_and_print(f"❌ JSON is missing expected timestamp {date_key}", "warning")
            missing_in_json.append(date_key)
            continue

        json_rows = json_group.get_group(date_key)

        menora_subjects = sorted(menora_rows[sql_subject_field].dropna().astype(str).str.strip().unique())
        json_subjects = sorted(json_rows[json_subject_field].dropna().astype(str).str.strip().unique())

        if menora_subjects != json_subjects:
            mismatched_fields.append({
                "date": date_key,
                "Field": sql_subject_field,
                "Menora": "; ".join(menora_subjects),
                "JSON": "; ".join(json_subjects)
            })

    for date_key in json_group.groups.keys():
        if date_key not in menora_group.groups:
            missing_in_menora.append(date_key)

    status_tab = "pass" if not (missing_in_json or missing_in_menora or mismatched_fields) else "fail"
    if status_tab == "pass":
        log_and_print(f"🔹 {tab_label} - PASS", "info", is_hebrew=True)
    else:
        log_and_print(f"❌ {tab_label} - FAIL", "warning", is_hebrew=True)

    return {
        tab_key: {
            "status_tab": status_tab,
            "missing_json_dates": sorted(missing_in_json),
            "missing_menora_dates": sorted(missing_in_menora),
            "mismatched_fields": mismatched_fields
        }
    }
