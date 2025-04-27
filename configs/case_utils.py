from fetcher import get_case_data
from logging_utils import log_and_print

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
