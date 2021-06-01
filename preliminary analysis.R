library(data.table)
library(jsonlite)
library(ggplot2)
library(arrow)
library(AzureStor)
library(corrplot)
library(xgboost)
library(Matrix)
library(stargazer)

# Load data  -----------------------------------------------------

full_dt <- readRDS("./Data/full_dt.RDS")
full_dt_unpivot <- readRDS("./Data/full_dt_unpivot.RDS")
dt <- readRDS("./Data/dt.RDS")

# Counting Checks -------------------------------------------------------

# Total unique contacts? 
full_dt[,length(unique(contactid))]

# Total unique accounts
full_dt[,length(unique(accountid))]

# Total unique opportunities
full_dt[,length(unique(opportunityid))]

# Total unique opportunities by state
full_dt[,length(unique(opportunityid)),by=statecode_displayname]

# Total QOs
full_dt[!is.na(QODate),length(unique(opportunityid))]

# Opportunities by opportunity creation source
full_dt[,length(unique(opportunityid)),by=opportunitycreationsource]
full_dt[,length(unique(opportunityid)),by=validatedcreationsource]

# Total contacts who have a lead id (in marketo)
full_dt[!is.na(leadid),length(unique(contactid))]

# Total contacts who have at least one activity
full_dt[total_activities>0,length(unique(contactid))]

# Total contacts who have at least one activity, by state code
full_dt[total_activities>=0,length(unique(opportunityid)),by=statecode_displayname] # - almost all won opps had activity!

# Opportunities that become QOs
full_dt[,length(unique(opportunityid)),by=QO]  # 1479 opportunities became QO
full_dt[,mean(QO),by=opportunityid][,mean(V1)] # 18% of opportunities became QO

# Time frame of opportunities
paste("The timeframe of creation of opportunities is between",min(full_dt[(!is.na(PSLDate)),createdon]),"and",max(full_dt[(!is.na(PSLDate)),createdon]))
max(full_dt[(!is.na(PSLDate)),createdon]) - min(full_dt[(!is.na(PSLDate)),createdon])
ggplot(full_dt, aes(x=PSLDate)) + geom_density()

# Time frame of QOs
paste("The timeframe of QOs is between",min(full_dt[(!is.na(QODate)),QODate]),"and",max(full_dt[(!is.na(QODate)),QODate]))
max(full_dt[(!is.na(QODate)),QODate]) - min(full_dt[(!is.na(QODate)),QODate])
ggplot(full_dt, aes(x=QODate)) + geom_density()

# Time frame of activities
paste("The timeframe of activities is between",min(full_dt_unpivot[(!is.na(activityDate)),activityDate]),"and",max(full_dt_unpivot[(!is.na(activityDate)),activityDate]) )
max(full_dt_unpivot[(!is.na(activityDate)),activityDate]) - min(full_dt_unpivot[(!is.na(activityDate)),activityDate])
ggplot(full_dt_unpivot, aes(x=activityDate)) + geom_density()

# Activities by Time and Accounts (contacts & opp analysis) ------

allaccounts <- full_dt_unpivot[,unique(accountid)]

raccountslist <- sample(allaccounts,50)
Cairo::CairoPDF("Output/Visualizations/Granular data checks.pdf",width=20,height = 10)

for(ra in raccountslist){
  # ra <- sample(allaccounts,1)
  sdt <- full_dt_unpivot[accountid==toupper(ra)]
  sdt[,new_sana_seniority_level_displayname:=seniority]
  rca <- sample(unique(sdt$contactid),1)
  mindate <- as.Date(min(c(sdt$activityDate,sdt$PSLDate,sdt$QODate),na.rm = T))
  maxdate <- as.Date(max(c(sdt$activityDate,sdt$PSLDate,sdt$QODate),na.rm = T))
  seriesdates <- seq.Date(as.Date(mindate),as.Date(maxdate),by = "day")
  sdt[,activityDay:=as.Date(activityDate)]
  asdt <- unique(sdt[,.(nractivities=.N),by=.(opportunityid,PSLDate,QODate,accountid,contactid,activityName,activityDay,
                                              # new_jobtitle,
                                              new_sana_seniority_level_displayname)]
                 [,.(contactid,activityName,activityDay,nractivities,
                     # new_jobtitle,
                     new_sana_seniority_level_displayname)])
  osdt <- unique(sdt[,.(opportunityid,PSLDate,QODate,Stage3Date,statecode_displayname,accountid)])
  setkey(osdt,PSLDate)
  osdt[,id:=1:.N]
  # sgfull_dt <- asdt[contactid==rca]
  print(ggplot(asdt,aes(x=activityDay,y=nractivities,col=activityName))+scale_x_date(limits = c(mindate,maxdate))+
          theme_bw()+geom_point(size=3)+ggtitle(paste(ra,sdt$account_name,sep=" - "))+ scale_colour_brewer(palette = "Set1")+
          sapply(as.Date(osdt$PSLDate), function(xint) geom_vline(aes(xintercept = xint),col="black",linetype="dashed"))+
          sapply(as.Date(osdt$QODate), function(xint) geom_vline(aes(xintercept = xint),col="red"))+
          facet_wrap(~contactid)+geom_text(data=osdt,aes(x=as.Date(PSLDate),y=0,label=id),col="black")+
          geom_text(data=osdt,aes(x=as.Date(QODate),y=max(asdt$nractivities),label=id),col="black")+
          geom_text(data=unique(asdt[,.(contactid,new_sana_seniority_level_displayname)]),
                    aes(x=mindate+50,y=max(asdt$nractivities),label=new_sana_seniority_level_displayname),col="black")+
          geom_text(data=osdt,aes(x=as.Date(PSLDate),y=max(asdt$nractivities)/2,label=statecode_displayname),angle=90,col="black",size=3))
}

dev.off()

# Good examples:
# dddd56b4-4263-e311-afa1-005056ac0042 -> Pollet Water Group (PWG) NV
# that one has no QO date for an opp that is won, even though the opp went to stage 3, why?
# it's a customer
# bc8ade8b-6b3d-e811-814b-e0071b647f41 -> BBB Industries LLC
# lost of reegaged opps? Lost every time
# 6d07d9e8-affe-e811-8162-e0071b65edd1
# one contact has activities, the other not. No opp yet

# Yellow Color Checks -----------------------------------------------------

# 3 outcomes
# full_dt$outcome <- "Not ready yet"
# full_dt[!is.na(QODate),outcome:="QO"]
# full_dt[is.na(QODate)&!is.na(opportunityid),outcome:="Opportunity but not QO"]
# full_dt[,.N,by=.(opportunityid,outcome)][,]
# full_dt[,mean(QO),by=opportunityid][,mean(V1)] 

# QO Rate by different profiles?

profiles <- c("territory_name","business_size","erpsystem",
              "new_leadsourceidname","new_onlinepresence_displayname")
personas <- c("sana_department","department","seniority")

for (i in profiles) {
  print(paste("QO Rate by", i))
  print(full_dt[,.(QORate=mean(QO)),by=c(i)])
}

for (i in personas) {
  print(paste("QO Rate by", i))
  print(full_dt[,.(QORate=mean(QO)),by=c(i)])
}

# Validated creation source and total activities NA

full_dt[,mean(QO),keyby=.(validatedcreationsource,(total_activities==0))]

#check relation of number of employees with QO rate
ggplot(full_dt[,.(QO=mean(QO)),by=numberofemployees], aes(x=(numberofemployees), y=QO)) + geom_point()
ggplot(full_dt[,.(QO=mean(QO)),by=numberofemployees], aes(x=log(numberofemployees), y=QO)) + geom_point()

# Does having activities affect QO Rate

ggplot(full_dt[,mean(QO),keyby=(is.na(total_activities))],aes(x=))

# Do different profiles have different levels of activities?
log_model_activities <- (QO ~`Click Email`+`Click Link`+
                           `Fill Out Form`+`Fill Out LinkedIn Lead Gen Form`+
                           `Open Email`+`Unsubscribe Email`+`Visit Webpage`)

log_model_activities_behaviour <- (QO ~`Click Email`+`Click Link`+
                           `Fill Out Form`+`Fill Out LinkedIn Lead Gen Form`+
                           `Open Email`+`Unsubscribe Email`+`Visit Webpage`
                            )

summary(logm <- glm(log_model_activities,
            data = full_dt,family = "binomial"))

summary(logm <- glm(log_model_activities_behaviour,
                    data = full_dt,family = "binomial"))


# Levels of activities

tmp <- melt(full_dt[total_activities<=10|is.na(total_activities)],id.vars = c("contactid","opportunityid","accountid","QO","SALDate","validatedcreationsource"),
            measure.vars = c("Click Email", "Click Link", "Click Sales Email", "Fill Out Form", "Fill Out LinkedIn Lead Gen Form", "Open Email", 
                             "Open Sales Email", "Unsubscribe Email", "Visit Webpage",  
                             "total_activities"))
tmp[is.na(value),value:=0]
tmp[,SAL:=0]
tmp[!is.na(SALDate),SAL:=1]

tmp[variable=="total_activities",mean(QO),keyby=.(validatedcreationsource,value==0,variable)]
tmp[variable=="total_activities",mean(SAL),keyby=.(value==0,variable)]
tmp[,mean(SAL),keyby=.(value==0,variable)]

#ggplot(tmp, aes(x=value, y=QO)) + geom_point() + facet_wrap(~variable) --- This one takes a long time to run. Commented

tmp2 <- tmp[,.(sum(value)),by=.(accountid,opportunityid,QO,SAL,variable)]
tmp2[variable=="total_activities",mean(QO),keyby=.(V1==0,variable)]

ggplot(tmp,aes(x=value))+theme_bw()+geom_density()+facet_wrap(~variable,scales = "free")

# Some graphs -------------------------------------------------------------

# Levels of activities by business size

dt_bybusinesssize <- full_dt[,.(total_activities=mean(total_activities)),keyby=.(accountid, QO, validatedcreationsource)]

ggplot(data = dt_bybusinesssize[total_activities<125], aes(x=total_activities, color=as.factor(QO))) +
  geom_density() + 
  facet_wrap(~ validatedcreationsource, scales = "free_x") +
  theme_bw() + 
  xlab("Business Size") +
  ylab("Average Count of Activities per contact") +
  labs(fill = "QO")

ggplot(data = dt_bybusinesssize[total_activities<20], aes(x=total_activities, color=as.factor(QO))) +
  geom_density() + 
  theme_bw() + 
  xlab("Business Size") +
  ylab("Average Count of Activities per contact") +
  labs(fill = "QO")


dt_bybusinesssize <- full_dt[,.(total_activities=mean(total_activities)),by=.(contactid,QO,business_size)]
dt_bybusinesssize <- dt_bybusinesssize[,.(total_activities=mean(total_activities)),by=.(QO,business_size)]

dt_bybusinesssize$business_size <- factor(dt_bybusinesssize$business_size, ordered=T,levels=c("Less than $25 M", "Between $25 and $200 M","Between $200 and $500 M","More than $500 M"))

ggplot(data = dt_bybusinesssize, aes(x=factor(business_size),levels(business_size)[c()], y=total_activities, fill=as.factor(QO))) +
  geom_col(position="dodge") + 
  theme_bw() + 
  xlab("Business Size") +
  ylab("Average Count of Activities per contact") +
  labs(fill = "QO")

# Levels of activities by seniority level

dt_by_seniority <- full_dt[,.(total_activities=mean(total_activities)),by=.(QO,seniority)]
dt_by_seniority$seniority <- factor(dt_bybusinesssize$seniority, ordered=T,levels=c("Entry Level","Manager","Director","C-Level"))

ggplot(data = dt_by_seniority, aes(x=factor(seniority),levels(seniority)[c()], y=total_activities, fill=as.factor(QO))) +
  geom_col(position="dodge") + 
  theme_bw() + 
  xlab("Seniority") +
  ylab("Average Count of Activities per contact") +
  labs(fill = "QO")

# Venn-Diagram Checks -----------------------------------------------------
# ## Contacts - Activities Merge ##
# venn_temp1 <- merge(activityNamesAttributes, contactTable, by.x="leadId", by.y="new_mkt_leadid", all.y=TRUE, all.x=TRUE)
# 
# contact_nolead <- venn_temp1[is.na(leadId),length(unique(contactid))]
# contact_yeslead <- venn_temp1[!is.na(leadId),length(unique(contactid))]
# lead_nocontact <- venn_temp1[is.na(contactid),length(unique(leadId))]
# 
# total_contacts <- venn_temp1[,length(unique(contactid))]
# total_leads <- venn_temp1[,length(unique(leadId))]
# 
# total_contacts
# contact_nolead # Contacts with no lead (no activities) - 23465 contacts
# contact_yeslead # Contacts with leads - 216849 contacts
# lead_nocontact  # Leads with no contact - 26647 leads
# contact_nolead/(total_contacts) # Share of contacts with no activities out of all contacts - 9.8%
# lead_nocontact/(total_leads) # Share of leads with no contact out of all leads - 10.9%
# 
# rm(venn_temp1)
# 
# # Contacts/Activities - Accounts Merge #
# venn_temp2 <- merge(activityContacts, accountTable, by.x="parentcustomerid",by.y="accountid", all.y=TRUE, all.x=TRUE)
# venn_temp2[,length(unique(parentcustomerid))]
# 
# 
# contactivities_noaccount <- venn_temp2[is.na(parentcustomerid),length(unique(contactid))]
# contactivities_yesaccount <- venn_temp2[!is.na(parentcustomerid),length(unique(contactid))]
# contactivities_yesaccount2 <- venn_temp2[!is.na(contactid),length(unique(parentcustomerid))]
# account_nocontactivities <- venn_temp2[is.na(contactid),length((parentcustomerid))]
# 
# contactivities_noaccount  # Contacts with no account - 0 contacts
# contactivities_yesaccount # Contacts with accounts - 240314 contacts
# contactivities_yesaccount2 # Accounts with contacts - 91564 accounts
# account_nocontactivities  # Accounts with no contact/activities - 21406 accounts
# contactivities_noaccount/(contactivities_noaccount + contactivities_yesaccount) # % contacts with no account out of all contacts - 0%
# account_nocontactivities/(account_nocontactivities + contactivities_yesaccount2) # % accounts with no contact out of all accounts - 16%
# 
# # Funnel Dates and Opportunities
# oppTable[,length(unique(opportunityid))]
# stageDates[,length(unique(opportunityid))]
# 
# # Contacts/Activities and Opportunities
# venn_temp3 <- merge(activityAccounts, oppsDates, by.x="parentcustomerid",by.y="parentaccountid",all.x=TRUE,all.y=TRUE, allow.cartesian=TRUE)
# 
# contactivities_noopp <- venn_temp3[is.na(opportunityid),length(unique(contactid))]
# contactivities_yesopp <- venn_temp3[!is.na(opportunityid),length(unique(contactid))]
# contactivities_yesQO <- venn_temp3[!is.na(QODate),length(unique(contactid))]
# contactivities_yesQOwon <- venn_temp3[(!is.na(QODate)&(statecode_displayname == "Won")),length(unique(contactid))]
# contactivities_yesQOlost <- venn_temp3[(!is.na(QODate)&(statecode_displayname == "Lost")),length(unique(parentaccountid))]
# contactivities_yesQOopen <- venn_temp3[(!is.na(QODate)&(statecode_displayname == "Open")),length(unique(contactid))]
# 
# contact_yesQO_noactivity <- venn_temp3[(!is.na(QODate)&(is.na(leadId))),length(unique(contactid))]
# 
# 
# contacts_total <- venn_temp3[,length(unique(contactid))]
# 
# contactivities_noopp # Contacts without opportunities
# contactivities_yesopp # Contacts with opportunities
# contactivities_yesQO # Contacts with QO
# contactivities_yesQOwon # Contacts with QO won
# contactivities_yesQOlost # Contacts with QO lost
# contactivities_yesQOopen # Contacts with QO open
# 
# contactivities_noopp/contacts_total # Contacts with no opportunity out of all contacts
# contactivities_yesopp/contacts_total # Contacts with opportunity out of all contacts
# 
# 

