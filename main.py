from config import load_configuration
from api_client import fetch_case_details

if __name__ == "__main__":
    load_configuration()

    case_id = 2005316
    data = fetch_case_details(case_id)

    if data:
        print("✅ Data received!")
    else:
        print("❌ No data returned.")
