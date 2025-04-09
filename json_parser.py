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

def extract_request_logs_from_json(case_json):
    import pandas as pd

    if not isinstance(case_json, dict):
        log_and_print("❌ extract_request_logs_from_json expected a single case JSON object (dict).", "error")
        return pd.DataFrame()

    logs = []

    requests = case_json.get("requests", [])
    for req in requests:
        for log_entry in req.get("requestLogs", []):
            logs.append({
                "Status_Date": log_entry.get("createLogDate"),
                "Action_Description": log_entry.get("remark"),
                "Request_Status_Id": log_entry.get("requestStatusId"),
                "Action_Log_Type_Id": log_entry.get("actionLogTypeId"),
                "Create_Action_User": log_entry.get("createActionUser"),
            })

    df = pd.DataFrame(logs)

    if df.empty:
        log_and_print("⚠️ No request logs found in JSON response.", "warning")
    else:
        log_and_print(f"📋 Extracted {len(df)} request logs from JSON.", "success")
        log_and_print(f"📋 Extracted request log DataFrame preview:\n{df.head(3)}", "info", is_hebrew=True)

    return df
