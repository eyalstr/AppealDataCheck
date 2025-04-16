import streamlit as st
import json
import os

st.set_page_config(page_title="Case Summary Dashboard", layout="wide")
st.markdown("<h1 style='font-size:16px;'>📊 Case Summary Dashboard</h1>", unsafe_allow_html=True)

if not os.path.exists("comparison_summary.json"):
    st.error("❌ comparison_summary.json not found. Please run the main comparison script first.")
    st.stop()

with open("comparison_summary.json", encoding="utf-8") as f:
    summary_data = json.load(f)

TAB_LABELS = {
    "request_log": "יומן תיק",
    "discussion": "דיונים",
    # Add more tab keys and labels as needed
}

for case_id, tabs in summary_data.items():
    st.markdown(f"<h2 style='font-size:13px;'>📁 Case ID: {case_id}</h2>", unsafe_allow_html=True)

    tab_labels = []
    status_icons = []
    tab_details = {}

    for tab_key in TAB_LABELS:
        if tab_key in tabs:
            tab_data = tabs[tab_key]
            tab_label = TAB_LABELS[tab_key]
            status = tab_data.get("status_tab", "fail")

            tab_labels.append(tab_label)
            if status == "pass" or (
                not tab_data.get("missing_json_dates") and 
                not tab_data.get("missing_menora_dates") and 
                not tab_data.get("mismatched_fields")
            ):
                status_icons.append("🟡 PASS")
            else:
                status_icons.append("🔴 FAIL")
                tab_details[tab_label] = tab_data

    # Create a table-like layout using columns
    cols = st.columns(len(tab_labels))
    for idx, col in enumerate(cols):
        with col:
            st.markdown(f"<div style='text-align: center; font-size:13px; direction: rtl;'><b>{tab_labels[idx]}</b><br>{status_icons[idx]}</div>", unsafe_allow_html=True)

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