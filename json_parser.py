import pandas as pd
from logging_utils import log_and_print

def extract_decision_data_from_json(case_json):
    decisions = case_json.get("decisions")

    if not decisions:
        log_and_print("⚠️ No 'decisions' found in JSON response.", "warning")
        return pd.DataFrame(columns=[
            "mojId", "decisionDate", "decisionStatusTypeId",
            "isForPublication", "decisionJudge", "decisionTypeToCourtId"
        ])

    extracted = []
    for decision in decisions:
        moj_id = decision.get("mojId")
        decision_date = decision.get("decisionDate")
        status_type = decision.get("decisionStatusTypeId")
        is_for_publication = decision.get("isForPublication")

        # Get judge from first item in decisionJudges if exists
        judge_list = decision.get("decisionJudges", [])
        judge = judge_list[0].get("judgeFullName") if judge_list else None

        # Get type from first subDecision
        type_to_court_id = None
        decision_requests = decision.get("decisionRequests", [])
        if decision_requests:
            sub_decisions = decision_requests[0].get("subDecisions", [])
            if sub_decisions:
                type_to_court_id = sub_decisions[0].get("decisionTypeToCourtId")

        if moj_id is None:
            log_and_print("⚠️ Skipping decision without mojId.", "warning")
            continue

        extracted.append({
            "mojId": moj_id,
            "decisionDate": decision_date,
            "decisionStatusTypeId": status_type,
            "isForPublication": is_for_publication,
            "decisionJudge": judge,
            "decisionTypeToCourtId": type_to_court_id
        })

    df = pd.DataFrame(extracted)

    if "mojId" not in df.columns:
        log_and_print(f"❌ Extracted decision DataFrame is missing 'mojId' column. Columns: {df.columns.tolist()}", "error")
    else:
        log_and_print(f"📋 Extracted decision DataFrame preview:\n{df.head(3)}", "info")

    return df



def extract_document_data_from_json(documents):
    if not isinstance(documents, list):
        log_and_print("❌ extract_document_data_from_json expected a list of documents.", "error")
        return pd.DataFrame()

    log_and_print(f"🔎 Extracting {len(documents)} documents from JSON.", "info")

    extracted = []

    for doc in documents:
        moj_id = doc.get("mojId")
        sub_type = doc.get("subType")
        doc_type = doc.get("docType")

        if moj_id is None:
            log_and_print("⚠️ Skipping document without mojId.", "warning")
            continue

        extracted.append({
            "mojId": moj_id,
            "subType": sub_type,
            "doc_type": doc_type
        })

    df = pd.DataFrame(extracted)

    if "mojId" not in df.columns:
        log_and_print(f"❌ Extracted document DataFrame is missing 'mojId' column. Columns: {df.columns.tolist()}", "error")

    log_and_print(f"📋 Extracted document DataFrame preview:\n{df.head(3)}", "info")

    return df
