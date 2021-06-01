library(data.table)

# Load Downloaded Data ----------------------------------------------------
contactTable <- readRDS("./Data/contactTable.RDS")
oppTable <- readRDS("./Data/oppTable.RDS")
stageDates <- readRDS("./Data/stageDates.RDS")
accountTable <- readRDS("./Data/accountTable.RDS")

# Opportunity Filters -----------------------------------------------------
oppTable <- oppTable[(is.na(SCRIBE_DELETEDON))]
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
saveRDS(dt, "./Data/dt.RDS")

