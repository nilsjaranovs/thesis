library(data.table)
library(tidyr)

salesActivities <- readRDS("./Data/salesActivities.RDS")
eventVisits <- readRDS("./Data/eventVisits.RDS")
dt <- readRDS("./Data/dt.RDS")

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

saveRDS(dt_sales, "./Data/dt_sales.RDS")
