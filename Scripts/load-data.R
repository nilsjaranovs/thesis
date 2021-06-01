library(jsonlite)
library(AzureStor)
library(arrow)
library(tidyr)
library(stargazer)
library(data.table)
library(odbc)
library(DBI)

remove(list = ls())

# Clear console
cat("\f")

#### Download data from SQL and save as RDS data.table ####
## CRM Data ##
con <- dbConnect(
  odbc(),
  Driver = "ODBC Driver 13 for SQL Server",
  Server = "fatqtyfkag.database.windows.net",
  Database = "ISMCRMMirror",
  UID = "reporting@fatqtyfkag",
  PWD = "t?6MNa*2"
)

#Account Table
accountTable <-
  dbGetQuery(
    con,
    "select 
      [name] as [account_name],
      [accountid],
      [parentaccountid],
      [territoryidname] as [territory_name],
      [new_sana_wholesale] as [type_wholesale],
      [new_sana_distributor] as [type_distributor],
      [new_sana_retail] as [type_retail],
      [new_sana_manufacturing] as [type_manufacturing],
      [new_sana_services] as [type_services],
      [new_accountclassification_displayname] as [account_classification],
      [new_annualrevenueusdiv] as [annual_revenue_usd],
      [emailaddress1] as [emailaddress],
      [numberofemployees],
      [new_businesssize_displayname] as [business_size],
      [new_erpsystem_displayname] as [erpsystem],
      [new_sana_accountstage_displayname] as [accountstage],
      [SCRIBE_DELETEDON],
      [statuscode],
      [new_leadsourceidname] as [lead_source],
      [new_leadscore],
      [new_onlinepresence_displayname] as [online_presence],
      [new_industry_displayname] as [industry],
      [new_sana_maldate]
      [new_mal]
    from [dbo].[account]
    FOR SYSTEM_TIME AS OF '2021-03-31 T23:59:59.9999'"
  )
saveRDS(setDT(accountTable), file="./Data/accountTable.RDS")

# Contacts Table
contactTable <-
  dbGetQuery(
    con,
    "select
      [fullname] as [contactname],
      [contactid],
      [new_mkt_leadid] as [leadid],
      [parentcustomerid] as [accountid],
      [SCRIBE_DELETEDON],
      [statuscode],
      [new_sana_department_displayname] as [sana_department],
      [new_department_displayname] as [department],
      [new_sana_seniority_level_displayname] as [seniority],
      [new_keycontact],
      [new_erpsystem_displayname] as [contact_erpsystem],
      [mkt_leadscore],
      [new_mkt_demographicscore]
    from [dbo].[contact]
    FOR SYSTEM_TIME AS OF '2021-03-31 T23:59:59.9999'"
  )

saveRDS(setDT(contactTable), file="./Data/contactTable.RDS")

# Opportunity Table
oppTable <-
  dbGetQuery(
    con,
    "select
      [customerid] as [accountid],
      [opportunityid],
      [new_qorating_displayname],
      [createdon],
      [statecode_displayname],
      [new_primarybu],
      [new_processstage],
      [new_sanapotentialcustomertype_displayname],
      [new_salesprocess_displayname],
      [SCRIBE_DELETEDON],
      [purchasetimeframe_displayname],
      [new_reengagedopportunity_displayname] as [reengagedopportunity],
      [new_validatedcreationsource_displayname] as [validatedcreationsource],
      [new_opportunitycreationsource_displayname] as [opportunitycreationsource],
      [actualclosedate]
    from [dbo].[opportunity]
    FOR SYSTEM_TIME AS OF '2021-03-31 T23:59:59.9999'"
  )

saveRDS(setDT(oppTable), file="./Data/oppTable.RDS")

# Funnel Dates
stageDates <-
  dbGetQuery(
    con,
    "select [opportunityid],
      [PSLDate],
      [SALDate],
      [QODate],
      [DealDate],
      [Stage0Date],
      [Stage1Date],
      [Stage2Date],
      [Stage3Date],
      [Stage4Date],
      [Stage5Date],
      [Stage6Date]
    from [dbo].[vw_FunnelDates]"
  )

saveRDS(setDT(stageDates), file="./Data/stageDates.RDS")

# Sales Activities View (Appointment, Phonecall, )

salesActivities  <-
  dbGetQuery(
    con,
    "SELECT [ActivityID]
      ,[CreatedOn_Date]
      ,[RegardingID]
      ,[regardingobjecttypecode] as [IDtype]
      ,[RegardingID_Name]
      ,[ActivityType]
      ,[Statecode_Name]
      ,[Statuscode_Name]
      ,[CallDuration]
    FROM [dbo].[vw_activities]
    WHERE (CAST([CreatedOn_Date] as DATE) <= '2021-03-31 T23:59:59.9999')
    AND RegardingID IS NOT NULL
    AND Statuscode_Name != 'Sent'
    AND (NOT ([ActivityType] = 'new_partnerreadiness'
      OR [ActivityType] = 'new_pipelineupdate'))
    AND (regardingobjecttypecode = 'opportunity' 
      OR regardingobjecttypecode = 'account'
      OR regardingobjecttypecode = 'contact');"
  )

saveRDS(setDT(salesActivities), file="./Data/salesActivities.RDS")


eventVisits  <-
  dbGetQuery(
    con,
    "SELECT
       [createdon]
      ,[activityid]
      ,[new_eventname]
      ,[new_attendee] as contactid
      ,[new_potentialcustomer] as accountid
      ,[new_attendeename] as contactname
      ,[new_nonattendancereason_displayname] as non_attendance_reason
      ,[new_subscriptionstatus_displayname] as subscription_status
      ,[new_potentialcustomername] as accountname
      ,[new_priority_displayname]
    FROM [dbo].[new_eventvisit]
    WHERE (CAST([createdon] as DATE) <= '2021-03-31 T23:59:59.9999')
      AND SCRIBE_DELETEDON IS NULL
      AND new_attendee IS NOT NULL;"
  )

saveRDS(setDT(eventVisits), file="./Data/eventVisits.RDS")


dbDisconnect(con) # Disconnect from CRM database


## Marketo Data ##
con_marketo <- dbConnect(
  odbc(),
  Driver = "ODBC Driver 13 for SQL Server",
  Server = "fatqtyfkag.database.windows.net",
  Database = "MarketoArchive",
  UID = "reporting@fatqtyfkag",
  PWD = "t?6MNa*2"
)
# Activities, activity types, and activity attributes
activities <-
  dbGetQuery(
    con_marketo,
    "select [_].[activityDate],
    [_].[activityTypeId],
    [_].[id],
    [_].[campaignId],
    [_].[leadId],
    [_].[marketoGUID],
    [_].[primaryAttributeValue],
    [_].[primaryAttributeValueId]
from [dbo].[Activities] as [_]
where ((([_].[activityTypeId] = 1 or [_].[activityTypeId] = 2) or ([_].[activityTypeId] = 3 or [_].[activityTypeId] = 9)) or (([_].[activityTypeId] = 10 or [_].[activityTypeId] = 11) or ([_].[activityTypeId] = 39 or [_].[activityTypeId] = 40))) or ((([_].[activityTypeId] = 41 or [_].[activityTypeId] = 46) or ([_].[activityTypeId] = 110 or [_].[activityTypeId] = 147)) or [_].[activityTypeId] = 100021)"
  )

activityAttributes <-
  dbGetQuery(
    con_marketo,
    "select [_].[Id],
    [_].[name],
    [_].[Activity_id],
    [_].[value]
from [dbo].[ActivityAttributes] as [_]
where ([_].[name] = 'Form Name' and [_].[name] is not null or [_].[name] = 'Video watched (percentage)' and [_].[name] is not null) or ([_].[name] = 'Webpage URL' and [_].[name] is not null or [_].[name] = 'Video link' and [_].[name] is not null)"
  )

activityTypes <-
  dbGetQuery(
    con_marketo,
    "select [name],
      [description],
      [apiName],
      [ActivityTypeId]
    from [dbo].[ActivityTypes]"
  )

saveRDS(setDT(activities), file="./Data/activities.RDS")
saveRDS(setDT(activityAttributes), file="./Data/activityAttributes.RDS")
saveRDS(setDT(activityTypes), file="./Data/activityTypes.RDS")
# 
# # Get activities from Parquet
# library(jsonlite)
# library(AzureStor)
# library(AzureContainers)
# 
# 
# secretsjson <- file.path( "./Data/secrets.json")
# apikey <- fromJSON(secretsjson)
# 
# cont <- blob_container(
#   "https://sanadatasciencestore.blob.core.windows.net/datascienceprojects",
#   key = apikey$blobstoragekey
# )
# list_blobs(cont)
# 
# download_blob(
#   cont,
#   "202102_Mark_LeadScoring/Activities.parquet",
#   dest = "./Data/Activities.parquet",
#   overwrite = TRUE
# )
# download_blob(
#   cont,
#   "202102_Mark_LeadScoring/ActivityAttributes.parquet",
#   dest = "./Data/ActivityAttributes.parquet",
#   overwrite = TRUE
# )
# download_blob(
#   cont,
#   "202102_Mark_LeadScoring/ActivityTypes.parquet",
#   dest = "./Data/ActivityTypes.parquet",
#   overwrite = TRUE
# )

