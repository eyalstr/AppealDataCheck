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

    try:
        conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        conn.close()

        log_and_print(f"✅ Retrieved {len(df)} decisions from Menora for appeal {appeal_number}", "success")
        return df

    except Exception as e:
        log_and_print(f"❌ Error while fetching Menora decision data: {e}", "error")
        return pd.DataFrame()
    

def fetch_menora_document_data(appeal_number):
    query = """
    SELECT 
        a.Appeal_Number_Display AS m_tik,
        d1.t_mismach,
        d1.sug_mismach,
        d1.tat_sug_mismach,
        dt.sug_mismach_teur,
        d1.filename,
        d1.moj_id,
        d1.UserIdNo,
        ds.Name,
        e_dt.document_Type_Id,
        ec_ds.Document_Source_Type_Id,
        COALESCE(ec_ds.Document_Source_Type_Id, 
                 CASE WHEN e_dt.Document_Type_Id IN (3, 13, 30) THEN 3 ELSE 4 END) AS Source_Type
    FROM (
        SELECT DISTINCT d.moj_id
        FROM Menora_Conversion.dbo.documents d
        JOIN Menora_Conversion.dbo.Appeal a ON d.mis_tik = a.Appeal_ID
        WHERE a.Appeal_Number_Display = ? AND d.user_mochek = 0
    ) AS d_base
    OUTER APPLY (
        SELECT TOP 1 d1.*
        FROM Menora_Conversion.dbo.documents d1
        JOIN Menora_Conversion.dbo.Appeal a ON d1.mis_tik = a.Appeal_ID
        WHERE d1.moj_id = d_base.moj_id AND a.Appeal_Number_Display = ?
        ORDER BY d1.DocStatus DESC
    ) AS d1
    JOIN Menora_Conversion.dbo.doc_types dt 
        ON d1.sug_mismach = dt.sug_mismach_rashi AND d1.tat_sug_mismach = dt.sug_mismach_mishani
    JOIN Menora_Conversion.dbo.CT_DocStatus ds 
        ON d1.DocStatus = ds.Code
    JOIN Menora_Conversion.dbo.Appeal a 
        ON d1.mis_tik = a.Appeal_ID
    JOIN [External_Courts].[cnvrt].[Document_Type_To_BO_Document] e_dt 
        ON e_dt.sug_mismach_BO = dt.sug_mismach
    LEFT JOIN [External_Courts].[cnvrt].[Document_Source_Types_To_DocSource_Code_BO] ec_ds 
        ON ec_ds.DocSource_Code_BO = doc_source_Id AND ec_ds.Court_id = 11
    WHERE e_dt.Court_Id = 11
    """

    try:
        conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number, appeal_number])
        conn.close()

        log_and_print(f"✅ Retrieved {len(df)} documents from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching Menora document data: {e}", "error")
        return pd.DataFrame()


def fetch_menora_discussion_data(appeal_number):
    query = """
    SELECT 
        FORMAT(Discussion_Date, 'dd/MM/yyyy') + ' ' + CONVERT(VARCHAR, d.Discussion_Strat_Time, 8) AS Strat_Time,
        FORMAT(Discussion_Date, 'dd/MM/yyyy') + ' ' + CONVERT(VARCHAR, d.Discussion_End_Time, 8) AS End_Time, 
        d.Discussion_Id,
        d.Discussion_Strat_Time,
        CASE WHEN d.discussionLink IS NOT NULL THEN 2 ELSE NULL END AS PlatphormType,
        ec_ds.Discussion_Status_Id,
        dc.Discussion_Conference_Type_ID,
        d.discussionLink, 
        a.Appeal_Number_Display AS m_tik,
        d.Discussion_Room,
        ec_cr.Discussion_Change_Reason_Id,
        cr.Name AS CancelReason,
        d.Moj_ID
    FROM Menora_Conversion.dbo.Discussion d
    JOIN Menora_Conversion.dbo.Link_Request_Discussion lr ON lr.Discussion_Id = d.Discussion_Id
    JOIN External_Courts.cnvrt.Discussion_Status_To_BO ec_ds ON ec_ds.Discussion_Status_BO = d.Status
    LEFT JOIN Menora_Conversion.dbo.CT_DiscussionCancelationReason cr ON cr.Code = d.CancelationReason
    LEFT JOIN External_Courts.cnvrt.Discussion_Change_Reason_To_BO ec_cr ON cr.Code = ec_cr.Discussion_Change_Reason_BO
    JOIN Menora_Conversion.dbo.Appeal a ON lr.appeal_id = a.Appeal_ID
    JOIN Discussions.code.CT_Discussion_Conference_Types dc ON dc.Discussion_Conference_Type_ID = d.virtualDiscussion
    WHERE a.Appeal_Number_Display = ?
    """

    try:
        conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        conn.close()
        log_and_print(f"✅ Retrieved {len(df)} discussions from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching Menora discussion data: {e}", "error")
        return pd.DataFrame()
