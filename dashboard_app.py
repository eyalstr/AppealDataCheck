import streamlit as st
import json
import os

st.set_page_config(page_title="Case Summary Dashboard", layout="wide")
st.markdown("<h1 style='font-size:14px;'>📊 Case Summary Dashboard</h1>", unsafe_allow_html=True)

if not os.path.exists("comparison_summary.json"):
    st.error("❌ comparison_summary.json not found. Please run the main comparison script first.")
    st.stop()

with open("comparison_summary.json", encoding="utf-8") as f:
    summary_data = json.load(f)

TAB_LABELS = {
    "request_log": "יומן תיק",
    "discussion": "דיונים",
    "decision": "החלטות",
    # Add more tab keys and labels as needed
}

for case_id, tabs in summary_data.items():
    st.markdown(f"<h2 style='font-size:12px;'>📁 Case ID: {case_id}</h2>", unsafe_allow_html=True)

    labels = []
    statuses = []
    tab_details = {}

    for tab_key in TAB_LABELS:
        if tab_key in tabs:
            tab_data = tabs[tab_key]
            tab_label = TAB_LABELS[tab_key]
            status = tab_data.get("status_tab", "fail")

            labels.append(tab_label)
            if status == "pass":
                statuses.append("🟡 PASS")
            else:
                statuses.append("🔴 FAIL")
                tab_details[tab_label] = tab_data

    rtl_labels_row = " | ".join(reversed(labels))
    rtl_status_row = " | ".join(reversed(statuses))

    st.markdown(f"<div style='font-size:13px; font-weight: bold; direction: rtl;'>{rtl_labels_row}</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:13px; direction: rtl;'>{rtl_status_row}</div>", unsafe_allow_html=True)

    for tab_label, tab_data in tab_details.items():
        with st.expander(f"🔍 Show Details for {tab_label}"):
            missing_json = tab_data.get("missing_json_dates", [])
            missing_menora = tab_data.get("missing_menora_dates", [])
            mismatched_fields = tab_data.get("mismatched_fields", [])

            if missing_json:
                st.markdown("**❌ Missing in JSON:**")
                st.write(missing_json)

            if missing_menora:
                st.markdown("**❌ Missing in Menora:**")
                st.write(missing_menora)

            if mismatched_fields:
                st.markdown("**❌ Field Mismatches:**")
                st.dataframe(mismatched_fields)

    st.divider()
