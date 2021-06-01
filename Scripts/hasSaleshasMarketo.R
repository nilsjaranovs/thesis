library(data.table)
library(arrow)
library(dplyr)
library(stringr)
# rm(list = ls())
# Load Downloaded Data ----------------------------------------------------
activities <- readRDS("./Data/activities.RDS")
activityTypes <- readRDS("./Data/activityTypes.RDS")
activityAttributes <- setDT(read_parquet("./Data/ActivityAttributes.parquet"))


# Merge Activities to Types and Attributes --------------------------------

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

#Indicator for having marketo activitiy/s
activities[,hadMarketoActivity:=1]

# Rename name
setnames(activities, "name", "activityName")

# Drop activities that occur after an opportunities QO date
dt <- readRDS("./Data/dt_sales.RDS")


#Sales Activity columns
sales_act_cols<-c("Cold","Hot","Warm","email_Completed","phonecall_Completed")
full_dt[, sums := rowSums(.SD, na.rm=T),.SDcols=sales_act_cols][,hadSalesActivity:=ifelse(sums>=1,1,0)][,sums:=NULL]
full_dt[,c(sales_act_cols) := NULL]

#Marketo Activity columns
mkto_act_cols<-c("Click Email","Fill Out Form",
                  "Fill Out LinkedIn Lead Gen Form",
                  "Open Email","Unsubscribe Email",
                  "Video view", "Visit Webpage")

model_dt[,"Fill Out Form":=(ifelse(`Fill Out Form`>0,as.integer(1),as.integer(0)))]
model_dt[,"Fill Out LinkedIn Lead Gen Form":=(ifelse(`Fill Out LinkedIn Lead Gen Form`>0,as.integer(1),as.integer(0)))]
model_dt[,"Open Email":=(ifelse(`Open Email`>0,as.integer(1),as.integer(0)))]
model_dt[,"Click Email":=(ifelse(`Click Email`>0,as.integer(1),as.integer(0)))]
model_dt[,"Fill Out Form":=(ifelse(`Fill Out Form`>0,as.integer(1),as.integer(0)))]
model_dt[,"Unsubscribe Email":=(ifelse(`Unsubscribe Email`>0,as.integer(1),as.integer(0)))]
model_dt[,"Video view":=(ifelse(`Video view`>0,as.integer(1),as.integer(0)))]
model_dt[,"Visit Webpage":=(ifelse(`Visit Webpage`>0,as.integer(1),as.integer(0)))]

#for dt_1_10
model_dt[,"Negative_Mkto_direct":=(ifelse(`Negative_Mkto_direct`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_NA":=(ifelse(`Positive_Mkto_NA`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_Paid Search":=(ifelse(`Positive_Mkto_Paid Search`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_Paid Social":=(ifelse(`Positive_Mkto_Paid Social`>0,as.integer(1),as.integer(0)))]
model_dt[,"Negative_Mkto_NA":=(ifelse(`Negative_Mkto_NA`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_direct":=(ifelse(`Positive_Mkto_direct`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_display":=(ifelse(`Positive_Mkto_display`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_email":=(ifelse(`Positive_Mkto_email`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_other":=(ifelse(`Positive_Mkto_other`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_print":=(ifelse(`Positive_Mkto_print`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_referral":=(ifelse(`Positive_Mkto_referral`>0,as.integer(1),as.integer(0)))]
model_dt[,"Positive_Mkto_social":=(ifelse(`Positive_Mkto_social`>0,as.integer(1),as.integer(0)))]


full_dt[,"Negative_Mkto_30days":=(ifelse(`Negative_Mkto_30days`>0,as.integer(1),as.integer(0)))]
full_dt[,"Negative_Mkto_30daysplus":=(ifelse(`Negative_Mkto_30daysplus`>0,as.integer(1),as.integer(0)))]
full_dt[,"Negative_Mkto_3days":=(ifelse(`Negative_Mkto_3days`>0,as.integer(1),as.integer(0)))]
full_dt[,"Positive_Mkto_30days":=(ifelse(`Positive_Mkto_30days`>0,as.integer(1),as.integer(0)))]
full_dt[,"Positive_Mkto_30daysplus":=(ifelse(`Positive_Mkto_30daysplus`>0,as.integer(1),as.integer(0)))]
full_dt[,"Positive_Mkto_3days":=(ifelse(`Positive_Mkto_3days`>0,as.integer(1),as.integer(0)))]
full_dt[,"Positive_Mkto_7days":=(ifelse(`Positive_Mkto_7days`>0,as.integer(1),as.integer(0)))]


model_dt[,c(mkto_act_cols) := NULL]



activities_unpivot <- merge(activities, dt[,.(leadid,opportunityid, QODate)], by.y = "leadid", by.x = "leadId", allow.cartesian=T) # Sarah's graphs: all activities
activities_unpivot[,activity_after_QO:=0]
activities_unpivot[(activityDate > QODate),activity_after_QO:=1]
activities_unpivot <- activities_unpivot[activity_after_QO==0]

#Pivot table for dt_1
activity_pivot<-dcast.data.table(activities_unpivot,leadId + opportunityid ~ hadMarketoActivity)


dt_1<-
  merge(
    dt,
    activity_pivot,
    by.x=c("leadid","opportunityid"),
    by.y=c("leadId","opportunityid"),
    all.x = TRUE
  )


setnames(dt_1, "1", "hasMarketoActivity")


dt_1$hasMarketoActivity[is.na(dt_1$hasMarketoActivity)]<-0
dt_1[,hasMarketoActivity:=ifelse(hasMarketoActivity>0,1,0)]

dt_1<-dt_1[,QO:=0]
dt_1<-dt_1[!is.na(QODate),QO:=1]
dt_1<-dt_1[,QO:=as.integer(QO)]

dt_1[,c("Cold","Hot","Warm", "email_Completed","phonecall_Completed"):=NULL]

#annual_revenue_usd,numberofemployees
annual_revenue_usd_breaks <- c(seq(0,max(model_dt$annual_revenue_usd,na.rm = TRUE),5e+09))

annual_revenue_usd_labels<-c()
for (i in 1:(length(annual_revenue_usd_breaks)-1)) {
  #annual_revenue_usd_breaks[i]=annual_revenue_usd_breaks[i]
  annual_revenue_usd_labels[i]<-annual_revenue_usd_breaks[i]
}
annual_revenue_usd_labels<-annual_revenue_usd_labels/(1e+09)
annual_revenue_usd_labels<-as.character(annual_revenue_usd_labels)


setDT(model_dt)[ , annual_revenue_usd_Bn := cut(annual_revenue_usd, 
                                            breaks = annual_revenue_usd_breaks, 
                                            right = FALSE, 
                                            labels = annual_revenue_usd_labels)]


#Checks
full_dt[,max(annual_revenue_usd),by=annual_revenue_usd_Bn][order(-annual_revenue_usd_Bn)]
dt_1[,min(annual_revenue_usd),by=annual_revenue_usd_Bn][order(-annual_revenue_usd_Bn)]


#numberofemployees_breaks <- c(seq(0,15000,500),max(dt_1$numberofemployees,na.rm = TRUE))
numberofemployees_breaks <- c(0,50,100,250,500,1000,5000,max(model_dt$numberofemployees,na.rm = TRUE))
numberofemployees_labels<-c()
for (i in 1:(length(numberofemployees_breaks)-1)) {
  #annual_revenue_usd_breaks[i]=annual_revenue_usd_breaks[i]
  numberofemployees_labels[i]<-numberofemployees_breaks[i]
}

numberofemployees_labels<-as.character(numberofemployees_labels)

setDT(model_dt)[ , numberofemployees_bucket := cut(numberofemployees, 
                                               breaks = numberofemployees_breaks, 
                                               right = FALSE, 
                                               labels = numberofemployees_labels)]


dt_1[,max(numberofemployees),by=numberofemployees_bucket][order(-numberofemployees_bucket)]
dt_1[,min(numberofemployees),by=numberofemployees_bucket][order(-numberofemployees_bucket)]



model_dt[,c("numberofemployees","annual_revenue_usd"):=NULL]

cols_to_Drop <-c("new_qorating_displayname","createdon",
                 "statecode_displayname", "actualclosedate",
                 "opportunitycreationsource" ,"validatedcreationsource","PSLDate",
                 "SALDate","QODate","DealDate","Stage3Date","account_name",
                 "parentaccountid","business_size","accountstage","new_leadscore","new_mal","contactname","leadid",
                 "new_keycontact","mkt_leadscore",
                 "new_mkt_demographicscore","sana_department")

full_dt<-full_dt[,(cols_to_Drop):=NULL]

saveRDS(setDT(full_dt), file="./Data/LiveTest/livetest.RDS")


#Checks 
dt_1[,length(unique(contactid)),by=numberofemployees_bucket][order(-numberofemployees_bucket)]
dt_1[,length(unique(contactid)),by=annual_revenue_usd_Bn][order(-annual_revenue_usd_Bn)]
