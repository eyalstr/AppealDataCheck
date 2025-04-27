import streamlit as st
import json
import os
from utils.logging_utils import log_and_print

# Streamlit setup
st.set_page_config(page_title="Case Summary Dashboard", layout="wide")
st.title("📊 Case Summary Dashboard")

# Prepare safe relative path to comparison_summary.json
current_dir = os.path.dirname(os.path.abspath(__file__))
summary_file = os.path.join(current_dir, "comparison_summary.json")

# Load comparison data
if not os.path.exists(summary_file):
    st.error("❌ comparison_summary.json not found. Please run the main comparison script first.")
    st.stop()

with open(summary_file, encoding="utf-8") as f:
    summary_data = json.load(f)

# RTL unicode helpers
RTL_EMBED = "\u202B"
POP_DIRECTIONAL = "\u202C"

# Hebrew tab labels
tab_labels = {
    "representator_log": "מייצגים",
    "document": "מסמכים",
    "decision": "החלטות",
    "discussion": "דיונים",
    "request_log": "יומן תיק",
    "case_contact": "פרטי עורר"
}

# Process each case
for index, (case_id, tabs) in enumerate(summary_data.items(), start=1):
    st.markdown(f"### {index}. 📁 תיק מספר: {case_id}")

    # Build tab status line in RTL order
    tab_keys_rtl = list(tab_labels.keys())[::-1]
    summary_parts = []

    for key in tab_keys_rtl:
        label = tab_labels.get(key, key).strip()
        tab_data = tabs.get(key, {})
        status = tab_data.get("status_tab", "fail") if isinstance(tab_data, dict) else "fail"
        if status == "pass":
            icon = "🟡"
        elif status == "fail":
            icon = "🔴"
        elif status == "skip":
            icon = "⚪"
        else:
            icon = "❓"
        summary_parts.append(f"{icon} {label}")

    summary_line = RTL_EMBED + " | ".join(summary_parts) + POP_DIRECTIONAL

    # Display aligned summary
    st.markdown(f"<div style='text-align: right; font-size: 16px;'>{summary_line}</div>", unsafe_allow_html=True)

    # Show expanders for failed and skipped tabs
    for key in tab_keys_rtl:
        tab_data = tabs.get(key, {})
        if isinstance(tab_data, dict) and tab_data.get("status_tab") in ["fail", "skip"]:
            tab_name = tab_labels.get(key, key)
            with st.expander(f"🔍 {tab_name} - פירוט תקלות"):
                if tab_data.get("status_tab") == "skip":
                    st.info(f"⚪ לא נדרש לבדיקה")
                else:
                    if tab_data.get("missing_json_dates"):
                        st.markdown("**❌ חסרים ב-JSON:**")
                        st.write(tab_data["missing_json_dates"])

                    if tab_data.get("missing_menora_dates"):
                        st.markdown("**❌ חסרים ב-Menora:**")
                        st.write(tab_data["missing_menora_dates"])

                    if tab_data.get("mismatched_fields"):
                        st.markdown("**❌ שדות לא תואמים:**")
                        st.dataframe(tab_data["mismatched_fields"])

    st.divider()
