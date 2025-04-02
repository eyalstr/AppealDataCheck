import pandas as pd

def extract_decision_data_from_json(case_json):
    decisions = case_json.get("decisions", [])
    extracted = []

    for decision in decisions:
        base = {
            "mojId": decision.get("mojId"),
            "decisionDate": decision.get("decisionDate"),
            "decisionStatusTypeId": decision.get("decisionStatusTypeId"),
            "isForPublication": decision.get("isForPublication"),
        }

        for req in decision.get("decisionRequests", []):
            for sub in req.get("subDecisions", []):
                row = base.copy()
                row["decisionTypeToCourtId"] = sub.get("decisionTypeToCourtId")
                extracted.append(row)

        if decision.get("decisionJudges"):
            judge_usernames = [j.get("userName") for j in decision["decisionJudges"] if j.get("userName")]
            for e in extracted:
                e["decisionJudge"] = ";".join(judge_usernames)

    return pd.DataFrame(extracted)
