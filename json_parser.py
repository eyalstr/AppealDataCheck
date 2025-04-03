import pandas as pd

def extract_decision_data_from_json(case_json):
    decisions = case_json.get("decisions", [])
    extracted = []

    for decision in decisions:
        base = {
            "mojId": decision.get("mojId", "⛔"),
            "decisionDate": decision.get("decisionDate", "⛔"),
            "decisionStatusTypeId": decision.get("decisionStatusTypeId", "⛔"),
            "isForPublication": decision.get("isForPublication", "⛔"),
            "decisionJudge": ";".join(
                [j.get("userName", "⛔") for j in decision.get("decisionJudges", [])]
            )
        }

        # Flag to check if we have subDecisions to loop
        added = False

        for req in decision.get("decisionRequests", []):
            for sub in req.get("subDecisions", []):
                row = base.copy()
                row["decisionTypeToCourtId"] = sub.get("decisionTypeToCourtId", "⛔")
                extracted.append(row)
                added = True

        if not added:
            # Still record base-level data even if no subDecisions exist
            row = base.copy()
            row["decisionTypeToCourtId"] = "⛔"
            extracted.append(row)

    return pd.DataFrame(extracted)