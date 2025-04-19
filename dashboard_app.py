import streamlit as st
import json
import os

# Streamlit setup
st.set_page_config(page_title="Case Summary Dashboard", layout="wide")
st.title("📊 Case Summary Dashboard")

# Load comparison data
if not os.path.exists("comparison_summary.json"):
    st.error("❌ comparison_summary.json not found. Please run the main comparison script first.")
    st.stop()

with open("comparison_summary.json", encoding="utf-8") as f:
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
    "case_contact": "פרטי עורר"  # ✅ NEW
}

# Process each case
for case_id, tabs in summary_data.items():
    st.markdown(f"### 📁 תיק מספר: {case_id}")

    # Build tab status line in RTL order
    tab_keys_rtl = list(tab_labels.keys())[::-1]
    summary_parts = []

    for key in tab_keys_rtl:
        label = tab_labels.get(key, key).strip()
        tab_data = tabs.get(key, {})
        status = tab_data.get("status_tab", "fail") if isinstance(tab_data, dict) else "fail"
        icon = "🟡" if status == "pass" else "🔴"
        summary_parts.append(f"{icon} {label}")

    summary_line = RTL_EMBED + " | ".join(summary_parts) + POP_DIRECTIONAL

    # Display aligned summary
    st.markdown(f"<div style='text-align: right; font-size: 16px;'>{summary_line}</div>", unsafe_allow_html=True)

    # Show expanders for failed tabs only
    for key in tab_keys_rtl:
        tab_data = tabs.get(key, {})
        if isinstance(tab_data, dict) and tab_data.get("status_tab") == "fail":
            tab_name = tab_labels.get(key, key)
            with st.expander(f"🔍 {tab_name} - פירוט תקלות"):
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
