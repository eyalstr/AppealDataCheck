select distinct 
    d.Decision_Date,
    d.Create_User, 
    d.Decision_Id, 
    a.Appeal_Number_Display, 
    CASE 
        WHEN ec_dt.[Decision_Type_Id] BETWEEN 70 AND 78 THEN ec_dt.[Decision_Type_Id] + 22
        ELSE ec_dt.[Decision_Type_Id]
    END as Decision_Type_Id,
    dt.Name,
    d.Is_For_Advertisement,
    d.Moj_ID,
    ec_ds.[Decision_Status_Type_Id] as 'Decision_Status' 
from Menora_Conversion.dbo.Decision d
join Menora_Conversion.dbo.Link_Request_Decision ld on ld.Decision_Id = d.Decision_Id
join [External_Courts].[cnvrt].[Decision_Status_Types_To_BO] ec_ds on ec_ds.[Decision_Status_Type_BO] = d.Status
join Menora_Conversion.dbo.CT_Decision_Type dt on dt.Code = d.Decision_Type
join Menora_Conversion.dbo.Appeal a on ld.appeal_id = a.Appeal_ID
join [External_Courts].[cnvrt].[Decision_Types_To_BO_Decision_Type] ec_dt on ec_dt.[BO_Decision_Type_Id] = d.Decision_Type
where a.Appeal_Number_Display = @ParameterValue and ec_dt.Court_ID = 11
