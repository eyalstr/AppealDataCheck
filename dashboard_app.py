import streamlit as st
import json
import os

st.set_page_config(page_title="Case Summary Dashboard", layout="wide")
st.title("📊 Case Summary Dashboard")

if not os.path.exists("comparison_summary.json"):
    st.error("❌ comparison_summary.json not found. Please run the main comparison script first.")
    st.stop()

with open("comparison_summary.json", encoding="utf-8") as f:
    summary_data = json.load(f)

for case_id, tabs in summary_data.items():
    st.markdown(f"### 📁 Case ID: {case_id}")

    tab_keys = list(tabs.keys())[::-1]  # Reverse to ensure RTL display
    tab_labels = {
        "decision": "החלטות",
        "discussion": "דיונים",
        "request_log": "יומן תיק",
        "document": "מסמכים"
    }

    tab_names = [tab_labels.get(key, key) for key in tab_keys]
    tab_status = [tabs[key].get("status_tab", "fail") if isinstance(tabs[key], dict) else "fail" for key in tab_keys]

    status_symbols = ["🟡 PASS" if status == "pass" else "🔴 FAIL" for status in tab_status]

    # RTL table alignment for headers and statuses
    table_rows = zip(tab_names, status_symbols)
    tab_row = " | ".join([name for name, _ in table_rows])
    status_row = " | ".join([status for _, status in zip(tab_names, status_symbols)])

    st.markdown(f"<div style='text-align: right; font-size: 16px;'>{tab_row}</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='text-align: right; font-size: 16px;'>{status_row}</div>", unsafe_allow_html=True)

    for key in tab_keys:
        tab_data = tabs.get(key, {})
        if isinstance(tab_data, dict) and tab_data.get("status_tab") == "fail":
            tab_name = tab_labels.get(key, key)
            with st.expander(f"🔍 {tab_name} - Show Details"):
                if tab_data.get("missing_json_dates"):
                    st.markdown("**❌ Missing in JSON:**")
                    st.write(tab_data["missing_json_dates"])

                if tab_data.get("missing_menora_dates"):
                    st.markdown("**❌ Missing in Menora:**")
                    st.write(tab_data["missing_menora_dates"])

                if tab_data.get("mismatched_fields"):
                    st.markdown("**❌ Field Mismatches:**")
                    st.dataframe(tab_data["mismatched_fields"])

    st.divider()
