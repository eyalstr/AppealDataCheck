import pandas as pd
from utils.logging_utils import log_and_print
from utils.fetcher import get_case_data


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
    """
    Extracts request log data from case JSON and returns a DataFrame.
    Expected structure:
    - case_json["requests"][*]["requestLogs"][*]
    """
    rows = []

    try:
        for request in case_json.get("requests", []):
            for log in request.get("requestLogs", []):
                row = {
                    "remark": log.get("remark"),
                    "createActionDate": log.get("createActionDate"),
                    "Request_Status_Id": log.get("requestStatusId"),
                    "Action_Log_Type_Id": log.get("actionLogTypeId"),
                    "Create_Action_User": log.get("createActionUser")
                }
                rows.append(row)
    except Exception as e:
        log_and_print(f"❌ Error parsing request logs: {e}", "error")

    return pd.DataFrame(rows)



def extract_decisions(case_data):
    return case_data.get("decisions", [])

def extract_documents(case_data):
    return case_data.get("documents", [])

def extract_discussions(case_data):
    return case_data.get("discussions", [])


def is_case_type_supported(case_id, expected_types=None):
    """
    Loads case JSON and checks if the caseTypeId is in the expected types list.

    Args:
        case_id (int): The case ID to check.
        expected_types (list[int]): A list of valid caseTypeIds.

    Returns:
        bool: True if caseTypeId is in the list, False otherwise.
    """
    if expected_types is None:
        expected_types = [328, 329, 330, 331]

    case_json = get_case_data(case_id)
    if not case_json:
        log_and_print(f"❌ Cannot validate caseTypeId — case {case_id} not found", "error")
        return False

    actual_type = case_json.get("caseTypeId")
    if actual_type not in expected_types:
        log_and_print(
            f"⚠️ Skipping case {case_id}: caseTypeId is {actual_type}, expected one of {expected_types}",
            "warning"
        )
        return False

    return True




def is_case_type_support_doc(case_id, expected_types=None):
    """
    Loads case JSON and checks if the caseTypeId is in the expected types list.

    Args:
        case_id (int): The case ID to check.
        expected_types (list[int]): A list of valid caseTypeIds.

    Returns:
        bool: True if caseTypeId is in the list, False otherwise.
    """
    if expected_types is None:
        expected_types = [328]

    case_json = get_case_data(case_id)
    if not case_json:
        log_and_print(f"❌ Cannot validate caseTypeId — case {case_id} not found", "error")
        return False

    actual_type = case_json.get("caseTypeId")
    if actual_type not in expected_types:
        log_and_print(
            f"⚠️ Skipping case {case_id}: caseTypeId is {actual_type}, expected one of {expected_types}",
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
