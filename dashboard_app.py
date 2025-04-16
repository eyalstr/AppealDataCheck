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

    for tab_key, tab_data in tabs.items():
        # Flatten directly if already in final format
        if tab_key == "request_log" and "status_tab" in tab_data:
            tab_label = "יומן תיק"
            status = tab_data.get("status_tab", "fail")

            if status == "pass":
                st.markdown(f"### 🟡 {tab_label} - PASS")
            else:
                st.subheader(f"🗂️ {tab_label}")

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
        else:
            # Fallback for non-request log tabs or legacy format
            if isinstance(tab_data, dict):
                # Flatten one level if extra nesting exists
                if len(tab_data) == 1 and isinstance(list(tab_data.values())[0], dict):
                    tab_data = list(tab_data.values())[0]

                tab_label = "יומן תיק" if "request_log" in tab_key.lower() else tab_key

                status = tab_data.get("status_tab", "fail")

                # Determine PASS/FAIL layout
                if status == "pass":
                    st.markdown(f"### 🟡 {tab_label} - PASS")
                else:
                    st.subheader(f"🗂️ {tab_label}")

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
