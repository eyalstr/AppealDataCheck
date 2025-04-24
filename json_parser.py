import pandas as pd
from logging_utils import log_and_print
from fetcher import get_case_data


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
        status_date = doc.get("rCreationDate") or doc.get("statusDate")

        if moj_id is None:
            log_and_print("⚠️ Skipping document without mojId.", "warning")
            continue

        extracted.append({
            "mojId": moj_id,
            "subType": sub_type,
            "doc_type": doc_type,
            "statusDate": status_date
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


def extract_decisions(case_data):
    return case_data.get("decisions", [])

def extract_documents(case_data):
    return case_data.get("documents", [])

def extract_discussions(case_data):
    return case_data.get("discussions", [])



def is_case_type_supported(case_id, expected_type=328):
    """
    Loads case JSON and checks if the caseTypeId matches the expected type.

    Args:
        case_id (int): The case ID to check.
        expected_type (int): The expected caseTypeId.

    Returns:
        bool: True if caseTypeId matches, False otherwise.
    """
    case_json = get_case_data(case_id)
    if not case_json:
        log_and_print(f"❌ Cannot validate caseTypeId — case {case_id} not found", "error")
        return False

    actual_type = case_json.get("caseTypeId")
    if actual_type != expected_type:
        log_and_print(
            f"⚠️ Skipping case {case_id}: caseTypeId is {actual_type}, expected {expected_type}",
            "warning"
        )
        return False

    return True

def get_first_request_id(case_id):
    """
    Returns the requestId of the first request in the case JSON.
    
    Args:
        case_id (int): The case ID to extract from.
    
    Returns:
        int | None: The requestId of the first request, or None if not found.
    """
    case_json = get_case_data(case_id)
    if not case_json:
        log_and_print(f"❌ Could not load case JSON for {case_id}", "error")
        return None

    requests = case_json.get("requests")
    if not requests or not isinstance(requests, list):
        log_and_print(f"⚠️ No 'requests' array found in case {case_id}", "warning")
        return None

    first_request = requests[0]
    request_id = first_request.get("requestId")

    if request_id is None:
        log_and_print(f"⚠️ First request in case {case_id} has no 'requestId'", "warning")

    return request_id
