import streamlit as st
import json
import os

st.set_page_config(page_title="Case Summary Dashboard", layout="wide")
st.title("📊 Case Summary Dashboard")

if not os.path.exists("comparison_summary.json"):
    st.error("❌ comparison_summary.json not found. Please run the main comparison script first.")
    st.stop()

with open("comparison_summary.json", encoding="utf-8") as f:
    raw_summary_data = json.load(f)

# Flatten logic
summary_data = {}
for case_id, tabs in raw_summary_data.items():
    summary_data[case_id] = {}
    for tab_key, tab_content in tabs.items():
        # Flatten: if there's only one subkey under tab_content, extract it
        if isinstance(tab_content, dict) and len(tab_content) == 1:
            subkey = next(iter(tab_content))
            summary_data[case_id][tab_key] = tab_content[subkey]
        else:
            summary_data[case_id][tab_key] = tab_content

key_map = {
    "request_log": "יומן תיק",
    "decision": "Decisions",
    "document": "Documents",
    "discussion": "Discussions",
    "case_involved": "Case Involved",
    "case_contacted": "Case Contacts",
    "distribution": "Distributions"
}

for case_id, tabs in summary_data.items():
    st.header(f"📁 Case ID: {case_id}")

    for tab_key_raw, data in tabs.items():
        tab_key = tab_key_raw.strip().lower().replace(" ", "_")
        tab_label = key_map.get(tab_key, tab_key_raw)

        # Extract metrics safely
        debug_metrics = {
            "Missing in JSON": int(data.get("Missing in JSON", 0)),
            "Missing in Menora": int(data.get("Missing in Menora", 0)),
            "Field Mismatches": int(data.get("Field Mismatches", 0)),
            "Fully Matched": int(data.get("Fully Matched", 0)),
            "Total Menora": int(data.get("Menora Request Logs", data.get("Total Menora", 0))),
            "Total JSON": int(data.get("JSON Request Logs", data.get("Total JSON", 0)))
        }

        missing_json = debug_metrics["Missing in JSON"]
        missing_menora = debug_metrics["Missing in Menora"]
        mismatches = debug_metrics["Field Mismatches"]
        matched = debug_metrics["Fully Matched"]
        total_menora = debug_metrics["Total Menora"]
        total_json = debug_metrics["Total JSON"]

        all_clear = (
            missing_json == 0 and
            missing_menora == 0 and
            mismatches == 0 and
            matched == total_menora and
            matched == total_json and
            total_menora > 0 and
            total_json > 0
        )

        if all_clear:
            st.markdown(f"### 🟡 {tab_label} - PASS")
        else:
            st.subheader("🗂️ " + tab_label)
            filtered = {k: v for k, v in data.items() if k != "field_mismatch_details"}
            st.table(filtered)

            if data.get("field_mismatch_details"):
                st.markdown("**Mismatches:**")
                for moj_id, fields in data["field_mismatch_details"].items():
                    st.markdown(f"- `{moj_id}`")
                    st.json(fields, expanded=False)

        with st.expander(f"🔍 Debug info for {tab_label} (raw + derived)", expanded=False):
            st.json(debug_metrics)
            st.markdown("**Derived checks:**")
            st.write({
                "missing_json == 0": missing_json == 0,
                "missing_menora == 0": missing_menora == 0,
                "mismatches == 0": mismatches == 0,
                "matched == total_menora": matched == total_menora,
                "matched == total_json": matched == total_json,
                "total_menora > 0": total_menora > 0,
                "total_json > 0": total_json > 0,
                "PASS (all_clear)": all_clear
            })
