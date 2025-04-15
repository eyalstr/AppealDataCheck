import streamlit as st
import json
import os

st.set_page_config(page_title="📊 Case Summary Dashboard", layout="wide")
st.title("📊 Case Summary Dashboard")

if not os.path.exists("comparison_summary.json"):
    st.error("❌ comparison_summary.json not found. Please run the main comparison script first.")
    st.stop()

with open("comparison_summary.json", encoding="utf-8") as f:
    summary_data = json.load(f)

for case_id, tabs in summary_data.items():
    st.markdown(f"### 📁 Case ID: {case_id}")

    for tab_key, result in tabs.items():
        if tab_key == "request_log":
            tab_label = "יומן תיק"
            status_tab = result.get("status_tab")
            missing_json = result.get("missing_json_dates", [])
            missing_menora = result.get("missing_menora_dates", [])
            mismatches = result.get("mismatched_fields", [])

            # Compute diagnostic info for debugging
            debug_info = {
                "Missing in JSON": len(missing_json),
                "Missing in Menora": len(missing_menora),
                "Field Mismatches": len(mismatches),
                "Total Issues": len(missing_json) + len(missing_menora) + len(mismatches),
            }

            if status_tab == "pass":
                st.markdown(f"### 🟡 {tab_label} - PASS")
            else:
                st.subheader(f"🗂️ {tab_label}")
                st.write("### 🔎 Debug Summary:")
                st.json(debug_info, expanded=False)

                if missing_json:
                    st.write("#### ❌ Missing in JSON (from Menora):")
                    st.table(missing_json)

                if missing_menora:
                    st.write("#### ❌ Missing in Menora (from JSON):")
                    st.table(missing_menora)

                if mismatches:
                    st.write("#### 🔍 Mismatched Fields:")
                    st.table(mismatches)
