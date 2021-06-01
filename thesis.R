library(data.table)
library(jsonlite)
library(ggplot2)
library(arrow)
library(AzureStor)
library(corrplot)
library(xgboost)
library(Matrix)
library(stargazer)


# Bogus -------------------------------------------------------------------



full_dt <- readRDS("./Data/full_dt.RDS")
full_dt_unpivot <- readRDS("./Data/full_dt_unpivot.RDS")
dt <- readRDS("./Data/dt.RDS")

ggplot(full_dt[,length(unique(opportunityid)),keyby=.(territory_name,QO)], aes(x=reorder(territory_name,-V1), y=V1, fill=as.factor(QO))) +
  geom_col(position="dodge2")

# Account Classification
ggplot(full_dt[,length(unique(opportunityid)),keyby=.(account_classification,QO)], aes(x=reorder(account_classification,-V1), y=V1, fill=as.factor(QO))) +
  geom_col(position="dodge2") +
  ylab("Count of Opportunities") +
  xlab("Account Classification") +
  labs(fill = "QO") +
  guides(fill = guide_legend(reverse=TRUE))

# Email Known
ggplot(full_dt[,length(unique(opportunityid)),keyby=.(email_known,QO)], aes(x=reorder(email_known,-V1), y=V1, fill=as.factor(QO))) +
  geom_col(position="dodge2") +
  ylab("Count of Opportunities") +
  xlab("Email Known") +
  labs(fill = "QO") +
  guides(fill = guide_legend(reverse=TRUE))

# Seniority
ggplot(full_dt[,length(unique(opportunityid)),keyby=.(seniority,QO)], aes(x=reorder(seniority,-V1), y=V1, fill=as.factor(QO))) +
  geom_col(position="dodge2") +
  ylab("Count of Opportunities") +
  xlab("Seniority") +
  labs(fill = "QO") +
  guides(fill = guide_legend(reverse=TRUE))

# Online Presence
ggplot(full_dt[,length(unique(opportunityid)),keyby=.(online_presence,QO)], aes(x=reorder(online_presence,-V1), y=V1, fill=as.factor(QO))) +
  geom_col(position="dodge2") +
  ylab("Count of Opportunities") +
  xlab("Email Known") +
  labs(fill = "QO") +
  guides(fill = guide_legend(reverse=TRUE))

# Department
ggplot(full_dt[,length(unique(opportunityid)),keyby=.(department,QO)], aes(x=reorder(department,-V1), y=V1, fill=as.factor(QO))) +
  geom_col(position="dodge2") +
  ylab("Count of Opportunities") +
  xlab("Email Known") +
  labs(fill = "QO") +
  guides(fill = guide_legend(reverse=TRUE))

# Annual Revenue
ggplot(full_dt[,.(meanQO = mean(QO)),by=annual_revenue_usd], aes(x=log(annual_revenue_usd), y=meanQO)) +
  geom_point()

lm(meanQO ~ log(annual_revenue_usd), data = full_dt[,.(meanQO = mean(QO)),by=annual_revenue_usd])


full_dt[]


### Activities stuff
ggplot(activities_unpivot[,.N,keyby=
                            .(factor(recency, levels = c("3days","7days","30days","30daysplus")),
                              factor(is.na(QODate), labels=c("QO=1","QO=0")))],
       aes(x=factor, y=N)) + 
  geom_col() +
  theme_bw() +
  ylab("Count of Activities") +
  xlab("Recency") +
  facet_wrap(~ factor.1, scales = "free")
  
activities_unpivot[,.N,by=.(content_type)]

temp_activities <- activities_unpivot[,.(num_activities=length(id)),by=.(leadId,
                                                                         factor(recency, levels = c("3days","7days","30days","30daysplus")),
                                                                         factor(is.na(QODate), labels=c("QO=1","QO=0")))]

temp_activities2 <- temp_activities[,.(meanactivities=mean(num_activities)), keyby=.(factor,factor.1)]

ggplot(temp_activities2, 
       aes(x=factor, y=meanactivities, fill =factor.1)) +
  geom_col(position = "dodge") +
  theme_bw() +
  xlab("Recency") +
  ylab("Mean Activities per Lead") +
  labs(fill="QO")

activities_unpivot[,.N,by=.(recency==recency2)]


t.test(temp_activities[factor=="7days",num_activities] ~ temp_activities[factor=="7days",factor.1])


# Test train test split.
var.test(model_test[,as.integer(QO)-1],model_train[,as.integer(QO)-1])
t.test(model_test[,as.integer(QO)-1],  model_train[,as.integer(QO)-1], var.equal=F)

bungo <- dcast.data.table(model_performance, iteration ~ .metric, value.var = ".estimate")
write.csv(bungo, file="./Output/model_performance.csv")



# Live test ---------------------------------------------------------------

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
    from [dbo].[account]"
  )
saveRDS(setDT(accountTable), file="./Data/LiveTest/accountTable.RDS")

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
    from [dbo].[contact]"
  )

saveRDS(setDT(contactTable), file="./Data/LiveTest/contactTable.RDS")

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
    from [dbo].[opportunity]")

saveRDS(setDT(oppTable), file="./Data/LiveTest/oppTable.RDS")

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

saveRDS(setDT(stageDates), file="./Data/LiveTest/stageDates.RDS")

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
    WHERE RegardingID IS NOT NULL
    AND Statuscode_Name != 'Sent'
    AND (NOT ([ActivityType] = 'new_partnerreadiness'
      OR [ActivityType] = 'new_pipelineupdate'))
    AND (regardingobjecttypecode = 'opportunity' 
      OR regardingobjecttypecode = 'account'
      OR regardingobjecttypecode = 'contact');"
  )

saveRDS(setDT(salesActivities), file="./Data/LiveTest/salesActivities.RDS")


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
    WHERE SCRIBE_DELETEDON IS NULL
      AND new_attendee IS NOT NULL;"
  )

saveRDS(setDT(eventVisits), file="./Data/LiveTest/eventVisits.RDS")



# OCA merge ---------------------------------------------------------------

library(data.table)

# Load Downloaded Data ----------------------------------------------------
contactTable <- readRDS("./Data/LiveTest/contactTable.RDS")
oppTable <- readRDS("./Data/LiveTest/oppTable.RDS")
stageDates <- readRDS("./Data/LiveTest/stageDates.RDS")
accountTable <- readRDS("./Data/LiveTest/accountTable.RDS")

# Opportunity Filters -----------------------------------------------------
oppTable <- oppTable[(is.na(SCRIBE_DELETEDON))]
oppTable <- oppTable[(createdon>as.Date("2021-03-31"))]
oppTable <- oppTable[(new_primarybu == 200000)]
oppTable <- oppTable[(new_processstage != 100000010) | is.na(new_processstage)]
oppTable <- oppTable[(new_salesprocess_displayname != "Existing Customer")]
oppTable <- oppTable[(opportunitycreationsource != "Automatic through Re-engagement process") |
                       is.na(opportunitycreationsource)]
oppTable <- oppTable[(accountid != "09DC5EF2-0020-EA11-A810-000D3AB5D511") | (is.na(accountid))]
oppTable <- oppTable[(accountid != "FEFD53B6-0020-EA11-A810-000D3AB5D511") | (is.na(accountid))]
oppTable <- oppTable[(accountid != "A96A7BDA-0020-EA11-A810-000D3AB5D511") | (is.na(accountid))]
oppTable <- oppTable[(accountid != "2D5AA8D5-0154-E611-8136-3863BB356F90") | (is.na(accountid))]
#oppTable <- oppTable[(new_sanapotentialcustomertype_displayname == "End-Customer")]
#oppTable <- oppTable[(validatedcreationsource == "Prospect Inbound") | (is.na(validatedcreationsource))]


# Merges for Main Data ------------------------------------------------------------------
# Add Funnel Dates and Filter PSLs
dt1 <- merge(oppTable[,.(accountid, 
                         opportunityid, 
                         new_qorating_displayname,
                         createdon,
                         statecode_displayname,
                         actualclosedate,
                         opportunitycreationsource,
                         validatedcreationsource)],
             stageDates[,.(opportunityid,
                           PSLDate,
                           SALDate,
                           QODate,
                           DealDate,
                           Stage3Date)], by="opportunityid", all.x = T)

dt12 <-
  dt1[((PSLDate > as.POSIXct('2018-10-31 23:59:59', format = "%Y-%m-%d %H:%M:%OS")))]

dt2 <- merge(dt12, accountTable, by = "accountid", all.x = T)

dt <- merge(dt2, contactTable[,.(contactname,
                                 contactid,
                                 leadid,
                                 accountid,
                                 SCRIBE_DELETEDON,
                                 statuscode,
                                 sana_department,
                                 department,
                                 seniority,
                                 new_keycontact,
                                 mkt_leadscore,
                                 new_mkt_demographicscore,
                                 contact_erpsystem
)], by = "accountid", all.x = T)

# Some checks
dt[,length(unique(opportunityid)),by=statecode_displayname]
# 
# dt[is.na(SCRIBE_DELETEDON.y),length(unique(opportunityid)),by=statecode_displayname]
# View(dt[accountid=="00251BAB-CAFF-E511-8127-3863BB342B00"]) # <- Example why we should remove scribe Deleted on is not na
# 
# dt[statuscode.y==1,length(unique(opportunityid)),by=statecode_displayname] # Removing accounts with status != 1 doesn't change much (recommend remove)
# 
# dt[new_mal==1,length(unique(opportunityid)),by=statecode_displayname] # new_mal = 1 removes a lot!

# Dt recommended filters and subset columns
dt <- dt[is.na(SCRIBE_DELETEDON.y)]
dt <- dt[statuscode.y==1]
dt <- dt[,c("SCRIBE_DELETEDON.x","statuscode.x","statuscode.y","SCRIBE_DELETEDON.y"):=NULL]

# Remove
rm(accountTable,contactTable,dt1,dt12,dt2,oppTable,stageDates)

# Check factor variables and some feature engineering --------------------------------------------------

### Persona (Contact) ###

# department
(dt[is.na(department),length(unique(contactid))])/(dt[,length(unique(contactid))])
# Sana department
(dt[is.na(sana_department),length(unique(contactid))])/(dt[,length(unique(contactid))])

# Key contact
(dt[is.na(new_keycontact),length(unique(contactid))])/(dt[,length(unique(contactid))]) 

# Seniority
(dt[is.na(seniority),length(unique(contactid))])/(dt[,length(unique(contactid))])

### Profile (Account) ###

# Account ERP System
(dt[is.na(erpsystem),length(unique(accountid))])/(dt[,length(unique(accountid))]) 
#View(dt[erpsystem!=contact_erpsystem,.(account_name,contactname,erpsystem,contact_erpsystem)])
#View(dt[is.na(erpsystem)&!is.na(contact_erpsystem),.(account_name,contactname,erpsystem,contact_erpsystem)])

dt[is.na(erpsystem),erpsystem:=contact_erpsystem] # Fill missing account ERP with contact ERP (11 cases)
dt[,contact_erpsystem:=NULL] 

# Territory Name
(dt[is.na(territory_name),length(unique(accountid))])/(dt[,length(unique(accountid))])
dt[,unique(territory_name)]
dt[territory_name=="Australia & New Zeeland", territory_name:="Australia & New Zealand"] # Fix new Zeeland

# Number of employees
(dt[is.na(numberofemployees),length(unique(accountid))])/(dt[,length(unique(accountid))])
dt[!is.na(numberofemployees),median(numberofemployees)]
ggplot(data = dt, aes(x=(numberofemployees))) + geom_histogram()
ggplot(data = dt, aes(x=log(numberofemployees))) + geom_density()

# Missing email
dt[,email_known := 0]
dt[!is.na(emailaddress),email_known := 1]
dt[,emailaddress:=NULL]

# Business Size
(dt[is.na(business_size),length(unique(accountid))])/(dt[,length(unique(accountid))])

# Account classification
(dt[is.na(account_classification),length(unique(accountid))])/(dt[,length(unique(accountid))])

dt[is.na(account_classification),account_classification:="Unknown"]


# Annual Revenue
(dt[is.na(annual_revenue_usd),length(unique(accountid))])/(dt[,length(unique(accountid))])

dt[,annual_revenue_usd:=as.numeric(annual_revenue_usd)]


# Lead Source
(dt[is.na(lead_source),length(unique(accountid))])/(dt[,length(unique(accountid))])

# Online Presence
(dt[is.na(online_presence),length(unique(accountid))])/(dt[,length(unique(accountid))])

dt[is.na(online_presence) |online_presence=="Not known yet" ,online_presence:="Not known yet/missing"]

## Make Other category for variable that needs it ##---------------

#lead_source
dt[,count_col:=length(unique(contactid)),by=lead_source] # Other threshold count <1000
dt[,lead_source:=ifelse(count_col<1000,"Other",lead_source)]

#erpsystem
dt[,count_col:=length(unique(contactid)),by=erpsystem] # Other threshold count <1000
dt[,erpsystem:=ifelse(count_col<1000,"Other",erpsystem)]

#industry
dt[,count_col:=length(unique(contactid)),by=industry] # Other threshold count <1000
dt[,industry:=ifelse(count_col<1000,"Other",industry)]

dt[,count_col:=NULL]

#annual_revenue_usd,numberofemployees--------------
# annual_revenue_usd_breaks <- c(seq(0,max(dt_1$annual_revenue_usd,na.rm = TRUE),5e+09))
annual_revenue_usd_breaks <- c(seq(0,1.2e+11,5e+09))

annual_revenue_usd_labels<-c()
for (i in 1:(length(annual_revenue_usd_breaks)-1)) {
  #annual_revenue_usd_breaks[i]=annual_revenue_usd_breaks[i]
  annual_revenue_usd_labels[i]<-annual_revenue_usd_breaks[i]
}
annual_revenue_usd_labels<-annual_revenue_usd_labels/(1e+09)
annual_revenue_usd_labels<-as.character(annual_revenue_usd_labels)


setDT(dt)[ , annual_revenue_usd_Bn := cut(annual_revenue_usd, 
                                          breaks = annual_revenue_usd_breaks, 
                                          right = FALSE, 
                                          labels = annual_revenue_usd_labels)]


#numberofemployees_breaks <- c(seq(0,15000,500),max(dt_1$numberofemployees,na.rm = TRUE))
#numberofemployees_breaks <- c(0,50,100,250,500,1000,5000,max(dt_1$numberofemployees,na.rm = TRUE))
numberofemployees_breaks <- c(0,50,100,250,500,1000,5000,680000)
numberofemployees_labels<-c()
for (i in 1:(length(numberofemployees_breaks)-1)) {
  #annual_revenue_usd_breaks[i]=annual_revenue_usd_breaks[i]
  numberofemployees_labels[i]<-numberofemployees_breaks[i]
}

numberofemployees_labels<-as.character(numberofemployees_labels)

setDT(dt)[ , numberofemployees_bucket := cut(numberofemployees, 
                                             breaks = numberofemployees_breaks, 
                                             right = FALSE, 
                                             labels = numberofemployees_labels)]




dt[,c("numberofemployees","annual_revenue_usd"):=NULL]


# Save dt
saveRDS(dt, "./Data/LiveTest/dt.RDS")



# Sales -------------------------------------------------------------------

salesActivities <- readRDS("./Data/LiveTest/salesActivities.RDS")
eventVisits <- readRDS("./Data/LiveTest/eventVisits.RDS")


# Drop sales activities not connected to a relevant contact/account/opportunity from dt
salesActivities <- salesActivities[(RegardingID%in%(unique(dt[,contactid])))
                                   |(RegardingID%in%(unique(dt[,accountid])))
                                   |(RegardingID%in%(unique(dt[,opportunityid])))]

#Combine consultancy and discovery calls
salesActivities[ActivityType=="new_consultancycall",ActivityType:="othercall"]
salesActivities[ActivityType=="new_discoverycall",ActivityType:="othercall"]

# Drop sales activities after QO date
salesActivities_opps <- salesActivities[IDtype=="opportunity"]
salesActivities_opps <- merge(salesActivities, dt[,.(opportunityid, QODate)], by.y = "opportunityid", by.x = "RegardingID", allow.cartesian=T)
salesActivities_opps[,activity_after_QO:=0]
salesActivities_opps[(CreatedOn_Date > QODate),activity_after_QO:=1]
salesActivities_opps <- salesActivities_opps[,QODate:=NULL]
salesActivities_opps <- salesActivities_opps[activity_after_QO==0]

salesActivities_cont <- salesActivities[IDtype=="contact"]
salesActivities_cont <- merge(salesActivities, dt[,.(contactid, QODate)], by.y = "contactid", by.x = "RegardingID", allow.cartesian=T)
salesActivities_cont[,activity_after_QO:=0]
salesActivities_cont[(CreatedOn_Date > QODate),activity_after_QO:=1]
salesActivities_cont <- salesActivities_cont[,QODate:=NULL]
salesActivities_cont <- salesActivities_cont[activity_after_QO==0]

salesActivities_accs <- salesActivities[IDtype=="account"]
salesActivities_accs <- merge(salesActivities, dt[,.(accountid, QODate)], by.y = "accountid", by.x = "RegardingID", allow.cartesian=T)
salesActivities_accs[,activity_after_QO:=0]
salesActivities_accs[(CreatedOn_Date > QODate),activity_after_QO:=1]
salesActivities_accs <- salesActivities_accs[,QODate:=NULL]
salesActivities_accs <- salesActivities_accs[activity_after_QO==0]

# Drop event visits after QO Date
eventVisits <- merge(eventVisits, dt[,.(contactid, QODate)], by = "contactid", allow.cartesian=T)
eventVisits[,activity_after_QO:=0]
eventVisits[(createdon > QODate),activity_after_QO:=1]
eventVisits <- eventVisits[,QODate:=NULL]
eventVisits <- eventVisits[activity_after_QO==0]

# sum and pivot for account activities

salesActivities_accs_sum <-
  salesActivities_accs[, .(ActivityCount = length(unique(ActivityID)),
                           AvgCallDuration = mean(CallDuration)), by = .(RegardingID, ActivityType, Statecode_Name)]

salesActivities_accs_pivot <-
  dcast.data.table(
    salesActivities_accs_sum,
    RegardingID ~ ActivityType + Statecode_Name,
    value.var = "ActivityCount"
  )

salesActivities_accs_pivot <- merge(salesActivities_accs_pivot, salesActivities_accs_sum[ActivityType=="phonecall",.(AvgCallDuration=mean(AvgCallDuration)),by=RegardingID], by="RegardingID", all.x=T)

rm(salesActivities_accs_sum)

salesActivities_accs[,length(unique(RegardingID))] #Check that it equals to pivot obs.

# sum and pivot for contact activities

salesActivities_cont_sum <-
  salesActivities_cont[, .(ActivityCount = length(unique(ActivityID)),
                           AvgCallDuration = mean(CallDuration)), by = .(RegardingID, ActivityType, Statecode_Name)]

salesActivities_cont_pivot <-
  dcast.data.table(
    salesActivities_cont_sum,
    RegardingID ~ ActivityType + Statecode_Name,
    value.var = "ActivityCount"
  )

salesActivities_cont_pivot <- merge(salesActivities_cont_pivot, salesActivities_cont_sum[ActivityType=="phonecall",.(AvgCallDuration=mean(AvgCallDuration)),by=RegardingID], by="RegardingID", all.x=T)

rm(salesActivities_cont_sum)

salesActivities_cont[,length(unique(RegardingID))] #Check that it equals to pivot obs.

# Add event visits to opportunity activities

eventVisits_sum <-
  eventVisits[, .(ActivityCount = length(unique(activityid))), by = .(contactid,
                                                                      subscription_status,
                                                                      non_attendance_reason)]

eventVisits_pivot <-
  dcast.data.table(
    eventVisits_sum,
    contactid ~ subscription_status + non_attendance_reason,
    value.var = "ActivityCount"
  )

eventVisits_priority <- eventVisits[!is.na(new_priority_displayname),.N,by=.(contactid, new_priority_displayname)]

eventVisits_priority_pivot <-
  dcast.data.table(
    eventVisits_priority,
    contactid ~ new_priority_displayname,
    value.var = "N"
  )

eventVisits_pivot <- merge(eventVisits_pivot,eventVisits_priority_pivot,all.x=T)

eventVisits[,length(unique(contactid))]

salesActivities_cont_pivot <- merge(salesActivities_cont_pivot,eventVisits_pivot,by.x="RegardingID",by.y="contactid",all=T)

# sum and pivot for opportunity activities

salesActivities_opps_sum <-
  salesActivities_opps[, .(ActivityCount = length(unique(ActivityID)),
                           AvgCallDuration = mean(CallDuration)), by = .(RegardingID, ActivityType, Statecode_Name)]

salesActivities_opps_pivot <-
  dcast.data.table(
    salesActivities_opps_sum,
    RegardingID ~ ActivityType + Statecode_Name,
    value.var = "ActivityCount"
  )

salesActivities_opps_pivot <- merge(salesActivities_opps_pivot, salesActivities_opps_sum[ActivityType=="phonecall",.(AvgCallDuration=mean(AvgCallDuration)),by=RegardingID], by="RegardingID", all.x=T)

rm(salesActivities_opps_sum)

salesActivities_opps[,length(unique(RegardingID))] #Check that it equals to pivot obs.

# Remove everything you dont use for full dt
rm(
  eventVisits,
  eventVisits_pivot,
  eventVisits_priority,
  eventVisits_priority_pivot,
  eventVisits_sum,
  salesActivities,
  salesActivities_accs,
  salesActivities_cont,
  salesActivities_opps
)


# Merge Sales Activites to DT, and sum ------------------------------------
dt_cont <- merge(dt[,.(accountid,contactid,opportunityid)], salesActivities_cont_pivot, by.x="contactid",by.y="RegardingID", all.x=T, suffixes = c(".x", ".cont"))
dt_cont_accs <- merge(dt_cont, salesActivities_accs_pivot, by.x="accountid",by.y="RegardingID", all.x=T, suffixes = c(".x", ".accs"))
dt_cont_accs_opps <- merge(dt_cont_accs, salesActivities_opps_pivot, by.x="opportunityid",by.y="RegardingID", all.x=T, suffixes = c(".x", ".opps"))

melt_dt <- melt(dt_cont_accs_opps, id.vars = c("contactid","accountid","opportunityid"))
melt_dt <- melt_dt[!is.na(value)]
melt_dt <- separate(melt_dt, variable, sep = "\\.", into = "Variable")
melt_dt[,value:=as.numeric(value)]
melt_dt <- melt_dt[(Variable=="phonecall_Completed")|(Variable=="email_Completed")|(Variable=="Hot")|(Variable=="Warm")|(Variable=="Cold")]

cast_dt <- dcast.data.table(melt_dt, contactid + accountid + opportunityid ~ Variable, value.var = "value", fun.aggregate = sum)

dt_sales <- merge(dt, cast_dt, by=c("contactid","accountid","opportunityid"),all.x=T)

rm(
  cast_dt,
  dt_cont,
  dt_cont_accs,
  dt_cont_accs_opps,
  melt_dt,
  salesActivities_accs_pivot,
  salesActivities_cont_pivot,
  salesActivities_opps_pivot
)

saveRDS(dt_sales, "./Data/LiveTest/dt_sales.RDS")


# Activities Pivot --------------------------------------------------------

activityAttributes <- setDT(read_parquet("./Data/LiveTest/ActivityAttributes.parquet"))
activities <- setDT(read_parquet("./Data/LiveTest/Activities.parquet"))
activityTypes <- setDT(read_parquet("./Data/ActivityTypes.parquet"))


drop_activityTypes <- c(
  3,12,13,22,24,25,27,32,37,38,
  39,40,41,46,47,101,102,104,108,
  110,111,112,113,114,115,123,145,
  300,302,100001,100002,100003,
  100004,100005,100006,100007,
  100010,100022
)

activities <- activities[!activityTypeId %in% drop_activityTypes]

activities <-
  merge(
    activities,
    activityTypes,
    by.x = "activityTypeId",
    by.y = "ActivityTypeId",
    all.x = TRUE
  )

activities <- activities[, c("campaignId", "apiName") := NULL]
activityAttributes.pivot <- (dcast(activityAttributes, Activity_id ~ name))

activities <-
  merge(
    activities,
    activityAttributes.pivot,
    by.x = "id",
    by.y = "Activity_id",
    all.x = TRUE
  )

# Drop unnecessary
rm(activityAttributes.pivot,activityTypes, activityAttributes)

# Rename name
setnames(activities, "name", "activityName")

# Drop activities that occur after an opportunities QO date
dt <- readRDS("./Data/LiveTest/dt_sales.RDS")

activities_unpivot <- merge(activities, dt[,.(leadid,opportunityid, QODate)], by.y = "leadid", by.x = "leadId", allow.cartesian=T) # Sarah's graphs: all activities
activities_unpivot[,activity_after_QO:=0]
activities_unpivot[(activityDate > QODate),activity_after_QO:=1]
activities_unpivot <- activities_unpivot[activity_after_QO==0]

#Get max activity date by the leadids,activity_typeid and add it to dataset as a column
activities_unpivot[,max_activity_date:=max(activityDate),by=.(leadId,activityTypeId)]

#If QO then days diff betwen QO and Activity date
#No QO then days diff between Max activity date Vs Activity Date

activities_unpivot$recency_days<-ifelse(is.na(activities_unpivot$QODate),
                                        (difftime(activities_unpivot$max_activity_date,activities_unpivot$activityDate,units="days")),
                                        (difftime(activities_unpivot$QODate,activities_unpivot$activityDate,units="days"))
)
activities_unpivot$recency<-ifelse(activities_unpivot$recency_days>=0 & activities_unpivot$recency_days<=3,"3days",
                                   ifelse(activities_unpivot$recency_days>3 & activities_unpivot$recency_days<=7,"7days",
                                          ifelse(activities_unpivot$recency_days>7 & activities_unpivot$recency_days<=30,"30days","30daysplus")))


pos_activities <- c(1,2,11,10,100021,147)

activities_unpivot$pos_activity <- "Negative_Mkto"
activities_unpivot[activityTypeId %in% pos_activities, pos_activity:="Positive_Mkto"]

activities_pivot <- dcast.data.table(activities_unpivot,leadId + opportunityid ~ pos_activity + recency) # +recency


full_dt <- merge(dt[,.(contactid,
                       accountid,
                       opportunityid,
                       leadid,
                       QODate,
                       territory_name,
                       type_wholesale,
                       type_distributor,
                       type_retail,
                       type_manufacturing,
                       type_services,
                       account_classification,
                       annual_revenue_usd_Bn,
                       numberofemployees_bucket,
                       erpsystem,
                       lead_source,
                       online_presence,
                       department,
                       seniority,
                       Cold,
                       Hot,
                       Warm,
                       email_Completed,
                       phonecall_Completed)],
                 activities_pivot, by.x=c("leadid","opportunityid"),by.y=c("leadId","opportunityid"), all.x=T)


full_dt[,QO:=0]
full_dt[!is.na(QODate),QO:=1]
full_dt[,QO:=as.integer(QO)]


# Predict -----------------------------------------------------------------

predict <- setDT(predict(xgb_final_wf, new_data=model_dt, type="prob"))

predict[,.pred_0:=NULL] # Remove not-QO probability column
setnames(predict, ".pred_1", "lead_probability") # Rename prediction to lead_probability
predict[,lead_probability:=round(lead_probability,4)] # Round to 4 decimals (to display in percent with 2 decimals)

output <- cbind(input, predict) # Bind predictions back to original data
rm(input,predict)




ggplot(baked_data[,.N,by=eval(paste0(i))], aes_(x=paste0(i),y="N")) + 
  geom_col() +
  ylab("% of Total") +
  ggtitle(paste0("Distribution of ",i)) +
  scale_y_continuous(labels = scales::percent) +
  theme_bw()


ggplot(baked_data[,.N,by=eval(paste0(i))], aes_(x=(as.name(i)), y=as.name("N"))) +
  geom_col(fill="skyblue") +
  scale_x_discrete(limits=c(0,1)) +
  ylab("Count") +
  ggtitle(paste0("Distribution of ",i)) +
  theme_bw()
