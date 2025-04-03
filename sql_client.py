# sql_client.py

import pyodbc
import pandas as pd
from logging_utils import log_and_print
from sql_connection import get_sql_connection  # Or however you're managing connections
import os


def fetch_appeal_number_by_case_id(case_id):
    query = """
        SELECT a.Appeal_Number_Display
        FROM Menora_Conversion.dbo.Appeal a 
        JOIN External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn 
            ON a.Appeal_Status = cn.Case_Status_BO
        JOIN cases_bo.dbo.CT_Case_Status_Types c 
            ON c.Case_Status_Type_Id = cn.Case_Status_Type_Id
        JOIN cases_bo.dbo.CT_Request_Status_Types r 
            ON r.Request_Status_Type_Id = c.Request_Status_Type_Id
        WHERE cn.Court_Id = 11 AND a.Case_id = ?
    """

    try:
        conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[case_id])
        conn.close()

        if df.empty:
            log_and_print(f"⚠️ No appeal number found for case_id {case_id}", "warning")
            return None

        appeal_number = df.iloc[0]["Appeal_Number_Display"]
        log_and_print(f"✅ Retrieved appeal number {appeal_number} for case_id {case_id}", "success")
        return appeal_number

    except Exception as e:
        log_and_print(f"❌ Error fetching appeal number: {e}", "error")
        return None
    
def fetch_menora_decision_data(appeal_number):
    """
    Fetch decision data from Menora using a parameterized SQL query.
    Returns: DataFrame
    """
    try:
        # Load env values
        server = os.getenv("SQL_SERVER")
        db = os.getenv("SQL_DATABASE")
        user = os.getenv("SQL_USER")
        password = os.getenv("SQL_PASSWORD")

        if not all([server, db, user, password]):
            log_and_print("❌ Missing SQL connection environment variables.", "error")
            return pd.DataFrame()

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={db};"
            f"UID={user};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )

        query = """
        SELECT DISTINCT 
            d.Decision_Date,
            d.Create_User, 
            d.Decision_Id, 
            a.Appeal_Number_Display, 
            CASE 
                WHEN ec_dt.Decision_Type_Id BETWEEN 70 AND 78 THEN ec_dt.Decision_Type_Id + 22
                ELSE ec_dt.Decision_Type_Id
            END as Decision_Type_Id,
            dt.Name,
            d.Is_For_Advertisement,
            d.Moj_ID,
            ec_ds.Decision_Status_Type_Id as Decision_Status
        FROM Menora_Conversion.dbo.Decision d
        JOIN Menora_Conversion.dbo.Link_Request_Decision ld ON ld.Decision_Id = d.Decision_Id
        JOIN External_Courts.cnvrt.Decision_Status_Types_To_BO ec_ds ON ec_ds.Decision_Status_Type_BO = d.Status
        JOIN Menora_Conversion.dbo.CT_Decision_Type dt ON dt.Code = d.Decision_Type
        JOIN Menora_Conversion.dbo.Appeal a ON ld.appeal_id = a.Appeal_ID
        JOIN External_Courts.cnvrt.Decision_Types_To_BO_Decision_Type ec_dt ON ec_dt.BO_Decision_Type_Id = d.Decision_Type
        WHERE a.Appeal_Number_Display = ? AND ec_dt.Court_ID = 11
        """

        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn, params=[appeal_number])

        log_and_print(f"✅ Retrieved {len(df)} decisions from Menora for appeal {appeal_number}", "success")
        return df

    except Exception as e:
        log_and_print(f"❌ Error while fetching Menora decision data: {e}", "error")
        return pd.DataFrame()
