# sql_client.py

import pyodbc
import pandas as pd
from utils.logging_utils import log_and_print
from utils.sql_connection import get_sql_connection
import os

def fetch_appeal_number_by_case_id(case_id,conn):
    query = """
    SELECT a.Appeal_Number_Display
    FROM Menora_Conversion.dbo.Appeal a 
    left JOIN External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn 
        ON a.Appeal_Status = cn.Case_Status_BO
    left JOIN cases_bo.dbo.CT_Case_Status_Types c 
        ON c.Case_Status_Type_Id = cn.Case_Status_Type_Id
    left JOIN cases_bo.dbo.CT_Request_Status_Types r 
        ON r.Request_Status_Type_Id = c.Request_Status_Type_Id
    WHERE cn.Court_Id = 11 AND a.Case_id = ?
    """
    try:
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[case_id])
        #conn.close()

        if df.empty:
            log_and_print(f"⚠️ No appeal number found for case_id {case_id}", "warning")
            return None

        appeal_number = df.iloc[0]["Appeal_Number_Display"]
        log_and_print(f"✅ Retrieved appeal number {appeal_number} for case_id {case_id}", "success")
        return appeal_number

    except Exception as e:
        log_and_print(f"❌ Error fetching appeal number: {e}", "error")
        return None

def fetch_menora_decision_data(appeal_number, conn):
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
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        #conn.close()
        log_and_print(f"✅ Retrieved {len(df)} decisions from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching Menora decision data: {e}", "error")
        return pd.DataFrame()

def fetch_menora_document_data(appeal_number, conn):
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
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number, appeal_number])
        #conn.close()
        log_and_print(f"✅ Retrieved {len(df)} documents from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching Menora document data: {e}", "error")
        return pd.DataFrame()

def fetch_menora_discussion_data(appeal_number, conn):
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
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        #conn.close()
        log_and_print(f"✅ Retrieved {len(df)} discussions from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching Menora discussion data: {e}", "error")
        return pd.DataFrame()

def fetch_menora_case_involved_data(appeal_number, conn):
    query = """
SELECT
    COALESCE(p.Main_Id_Number, r.ID_Num) AS Main_Id_Number,
    r.ID_Num AS meshivaID,
    a.PrivateCompanyNumber,
    a.CompanyName,
    CASE 
        WHEN p.Main_Id_Number IS NOT NULL THEN p.Ful_Name 
        ELSE NULL 
    END AS orer,
    CASE 
        WHEN r.ID_Num IS NOT NULL THEN r.First_Name + ' ' + r.Last_Name 
        ELSE NULL 
    END AS meshiva
FROM Menora_Conversion.dbo.Appeal a
LEFT JOIN Menora_Conversion.dbo.Appeal_Presenter ap 
    ON a.Appeal_ID = ap.Appeal_ID AND ap.IsActive = 1
LEFT JOIN Menora_Conversion.dbo.Person p 
    ON ap.Person_ID = p.Person_ID
LEFT JOIN Menora_Conversion.dbo.Respondents r 
    ON a.Respondent_Code = r.RespondentID
WHERE a.Appeal_Number_Display = ?
    """
    try:
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        #conn.close()
        log_and_print(f"✅ Retrieved {len(df)} case involved entries from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching case involved data: {e}", "error")
        return pd.DataFrame()
    


def fetch_menora_case_contacts(appeal_number, conn):
    query = """
    SELECT
    MAX(a.CompanyName) AS CompanyName,
    MAX(a.Open_Date) AS Open_Date, 
    MAX(TRIM(ud.shem_prati) + ' ' + TRIM(ud.shem_mishpacha)) AS 'your vaada',
    MAX(up.C_ShemMale) AS 'chaver vaada',
    MAX(p.Ful_Name) AS 'orer',
    p.Main_Id_Number,
    MAX(e.Email) AS 'orerEmail',
    MAX(CASE WHEN cp.Phone_Type = 1 THEN cp.Phone ELSE NULL END) AS Phone1,
    MAX(CASE WHEN cp.Phone_Type = 2 THEN cp.Phone ELSE NULL END) AS Phone2,
    MAX(r.First_Name + ' ' + r.Last_Name) AS 'meshiva',
    MAX(r.ID_Num) AS 'meshivaID',
    MAX(r.Email) AS 'meshivaEmail',
    MAX(ec_pt.[Entitlement_Periods_Type_Id]) AS Entitlement_Periods,
    MAX(a.Case_Id) AS Case_Id,
    MAX(a.Appeal_Number_Display) AS 'מספר תיק',
    MAX(a.Tax_RequestNumber) AS Tax_RequestNumber,
    MAX(a.Tax_ObjectionDate) AS Tax_ObjectionDate,
    MAX(ec_st.Case_Type_Id) AS Case_Type_Id,
    MAX(a.PublicDelegateCode) AS PublicDelegateCode,
    MAX(a.Appeal_Status) AS Appeal_Status,
    MAX(a.PrivateCompanyNumber) AS PrivateCompanyNumber,
    MAX(a.CompanyCity) AS 'עיר עסק',
    MAX(CASE 
        WHEN a.CompanyType BETWEEN 1 AND 5 THEN 3
        WHEN a.CompanyType = 6 THEN 10
        ELSE a.CompanyType
    END) AS CompanyType,
    MAX(ap.Presenter_Type) AS Presenter_Type,
    MAX(apt.Name) AS 'סוג ייצוג',
    MAX(p.Hebrew_First_Name) AS 'שם פרטי עוסק ב"כ',
    MAX(p.Hebrew_Last_Name) AS 'שם משפחה עוסק ב"כ',
    p.Main_Id_Number AS 'ת"ז עוסק ב"כ',
    MAX(ap.Lawyer_License_number) AS 'מספר רשיון',
    MAX(e.Email) AS 'EmailOsek',
    MAX(r.ID_Num) AS 'מספר זיהוי בא כח משיבה',
    MAX(r.Email) AS 'דוא"ל ב"כ משיבה',
    MAX(cb_s.Request_Status_Type_Id) AS 'Request_Status',
    MAX(a.Annual_Turnover) AS Annual_Turnover,
    MAX(ec_rt.Representor_Type_Id) AS Representor_Type_Id
FROM Menora_Conversion.dbo.Appeal a
JOIN Menora_Conversion.dbo.Appeal_Presenter ap ON a.Appeal_ID = ap.Appeal_ID and ap.IsActive = 1
LEFT JOIN external_Courts.cnvrt.Representing_Types_BO_To_Representing_Types ec_rt 
    ON ap.Presenter_Type = ec_rt.Representor_Type_Id_BO 
    AND ec_rt.Court_Id = 11
LEFT JOIN [Menora_Conversion].[dbo].[Person] p ON ap.[Person_ID] = p.[Person_ID]
JOIN Menora_Conversion.dbo.CT_Appeal_Presenter_Type apt ON ap.Presenter_Type = apt.Code
JOIN Menora_Conversion.dbo.Contact_Email e ON p.Person_ID = e.Person_ID 
LEFT JOIN Menora_Conversion.dbo.Contact_Phone cp ON p.Person_ID = cp.Person_ID
LEFT JOIN Menora_Conversion.dbo.Respondents r ON a.Respondent_Code = r.RespondentID
LEFT JOIN Menora_Conversion.dbo.users ud ON a.Dayan_Code = ud.UserId
LEFT JOIN Menora_Conversion.dbo.users up ON a.PublicDelegateCode = up.UserId
JOIN [External_Courts].[cnvrt].[Entitlement_Periods_Types_To_BO] ec_pt  
    ON ec_pt.BO_Entitlement_Periods_Type_Id = a.Eligibility_Period
LEFT JOIN [External_Courts].[cnvrt].[Case_Status_To_Case_Status_BO] ec_s 
    ON a.Appeal_Status = ec_s.[Case_Status_BO]
JOIN [Cases_BO].[dbo].[CT_Case_Status_Types] cb_s 
    ON cb_s.Case_Status_Type_Id = ec_s.[Case_Status_Type_Id] 
    AND ec_s.[Court_Id] = 11
LEFT JOIN [External_Courts].[cnvrt].[Case_Subject_Type_And_Case_Type_To_BO_Case_Type] ec_st 
    ON ec_st.BO_Case_Type = a.Main_Subject 
    AND ec_pt.[Entitlement_Periods_Type_Id] = ec_st.Entitlement_Period_Id
WHERE a.Appeal_Number_Display = ?
GROUP BY p.Main_Id_Number;
    """
    try:
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        #conn.close()
        log_and_print(f"✅ Retrieved {len(df)} case involved entries from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching case involved data: {e}", "error")
        return pd.DataFrame()
    

def fetch_menora_distributions(appeal_number, conn):
    query = """ 
        select d.SendDate,d.SendUser,d.SendFrom,d.SendTo,d.SendSubject,d.SendBody,d.AttachmentsDocMojID,d.Discussion_Id,
        d.SendErrorCode,d.SendErrorDesc,d.Distribution_Status,d.Distribution_Status_Desc,
        d.Distribution_type, dt.Name 'סוג הפצה'
        from [Menora_Conversion].[dbo].[Log_DistributionService] d
        join [Menora_Conversion].dbo.CT_Distribution_Type dt on d.Distribution_type=dt.Code
        join [Menora_Conversion].dbo.Appeal a on d.appeal_id=a.Appeal_ID
        where  a.Appeal_Number_Display=?
    """
    try:
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        #conn.close()
        log_and_print(f"✅ Retrieved {len(df)} distribution entries from Menora for appeal {appeal_number}", "success")
        return df
    except Exception as e:
        log_and_print(f"❌ Error while fetching distribution data: {e}", "error")
        return pd.DataFrame()
    

def fetch_menora_log_requests(appeal_number, conn):
    query = """
        SELECT 
            la.Status_Date,
            CASE 
                WHEN la.Action_Description = 'Case Create' THEN N'הגשת תיק ערר'
                ELSE la.Action_Description
            END AS Action_Description,
            la.Status_Reason,
            la.Action_Type,
            la.Create_User
        FROM [Menora_Conversion].[dbo].[Log_Appeal_Status] la
        JOIN Menora_Conversion.dbo.Appeal a 
            ON la.Appeal_ID = a.Appeal_ID
        WHERE a.Appeal_Number_Display = ?
        ORDER BY la.Log_Code DESC;
    """
    try:
        #conn = get_sql_connection()
        df = pd.read_sql(query, conn, params=[appeal_number])
        #conn.close()

        required_columns = ["Status_Date", "Action_Description", "Status_Reason", "Action_Type", "Create_User"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            log_and_print(f"❌ Missing expected columns in Menora response: {missing_columns}", "error")
            return pd.DataFrame()

        log_and_print(f"✅ Retrieved {len(df)} request log entries from Menora for appeal number {appeal_number}", "success")
        return df

    except Exception as e:
        log_and_print(f"❌ Error while fetching request log data for appeal number {appeal_number}: {e}", "error")
        return pd.DataFrame()
