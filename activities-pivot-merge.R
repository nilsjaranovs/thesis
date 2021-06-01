library(data.table)
library(arrow)
library(dplyr)
library(stringr)

# Load Downloaded Data ----------------------------------------------------
activities <- readRDS("./Data/activities.RDS")
activityTypes <- readRDS("./Data/activityTypes.RDS")
activityAttributes <- setDT(read_parquet("./Data/ActivityAttributes.parquet"))

activityAttributes <- setDT(read_parquet("./Data/LiveTest/ActivityAttributes.parquet"))
activities <- setDT(read_parquet("./Data/LiveTest/Activities.parquet"))
activityTypes <- setDT(read_parquet("./Data/ActivityTypes.parquet"))


# Test test

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

# Rename name
setnames(activities, "name", "activityName")

# Drop activities that occur after an opportunities QO date
dt <- readRDS("./Data/dt_sales.RDS")

activities_unpivot <- merge(activities, dt[,.(leadid,opportunityid, QODate)], by.y = "leadid", by.x = "leadId", allow.cartesian=T) # Sarah's graphs: all activities
activities_unpivot[,activity_after_QO:=0]
activities_unpivot[(activityDate > QODate),activity_after_QO:=1]
activities_unpivot <- activities_unpivot[activity_after_QO==0]


# Source of Session -------------------------------------------------------

activities_unpivot[`Query Parameters`== "",`Query Parameters`:= NA]
activities_unpivot[`Referrer URL`== "",`Referrer URL`:= NA]
# First check Query Parameter has utm_source, 
#if not check the Referrer URL has utm_source,
#if not check Referrer URL for source

  ## utm_source ##

pattern_QueryPara<-"utm_source=\\s*(.*?)\\s*&"
pattern_RefURL <- "source=\\s*(.*?)\\s*&"
activities_unpivot[,Session_source:=with(activities_unpivot,ifelse(!is.na(`Query Parameters`) & str_detect(`Query Parameters`,"utm_source="),
                                                                   lapply(`Query Parameters`, function(x) str_match(x, pattern_QueryPara)[2]),
                                                                   ifelse(!is.na(`Referrer URL`) & str_detect(`Referrer URL`,"source="),
                                                                          lapply(`Referrer URL`, function(x) str_match(x, pattern_RefURL)[2]),
                                                                          ifelse(!is.na(`Query Parameters`) | !is.na(`Referrer URL`),"Direct",NA
                                                                          )
                                                                   )
)
)]
activities_unpivot[,Session_source:=unlist(Session_source)]
# Remove punctuation and lowercase 
activities_unpivot[,Session_source:=tolower(str_replace_all(activities_unpivot$Session_source, "[[:punct:]]", " "))]
# remove stop words,this list need can be updated
stopwords<-c("com","www","co","nl","uk","org","au","nz","be")
activities_unpivot[,Session_source:=gsub(paste0("\\b(",paste(stopwords, collapse="|"),")\\b"), "", activities_unpivot$Session_source)]
#Merge words containing linkedin,google,twitter,email,facebook,blo
activities_unpivot[,Session_source:=ifelse(str_detect(Session_source,"email"),"email",
                                           ifelse(str_detect(Session_source,"google"),"google",
                                                  ifelse(str_detect(Session_source,"linkedin"),"linkedin",
                                                         ifelse(str_detect(Session_source,"facebook"),"facebook",
                                                                ifelse(str_detect(Session_source,"twitter"),"twitter",Session_source
                                                                )
                                                         )
                                                  )
                                           )
)
]

  ## utm_medium ##

pattern_QueryPara_medium<-"utm_medium=\\s*(.*?)\\s*&"
pattern_RefURL_medium <- "medium=\\s*(.*?)\\s*&"
activities_unpivot[,Session_source_medium:=with(activities_unpivot,ifelse(!is.na(`Query Parameters`) & str_detect(`Query Parameters`,"utm_medium="),
                                                                          lapply(`Query Parameters`, function(x) str_match(x, pattern_QueryPara_medium)[2]),
                                                                          ifelse(!is.na(`Referrer URL`) & str_detect(`Referrer URL`,"medium="),
                                                                                 lapply(`Referrer URL`, function(x) str_match(x, pattern_RefURL_medium)[2]),
                                                                                 ifelse(!is.na(`Query Parameters`) | !is.na(`Referrer URL`),"Direct",NA
                                                                                 )
                                                                          )
)
)]
activities_unpivot[,Session_source_medium:=unlist(Session_source_medium)]
# Remove punctuation and lowercase 
activities_unpivot[,Session_source_medium:=tolower(str_replace_all(activities_unpivot$Session_source_medium, "[[:punct:]]", " "))]
# remove stop words,this list need can be updated
activities_unpivot[,Session_source_medium:=gsub(paste0("\\b(",paste(stopwords, collapse="|"),")\\b"), "", activities_unpivot$Session_source_medium)]
#Merge words containing linkedin,google,twitter,email,facebook,blo
activities_unpivot[,Session_source_medium:=ifelse(str_detect(Session_source_medium,"email"),"email",
                                                  ifelse(str_detect(Session_source_medium,"google"),"google",
                                                         ifelse(str_detect(Session_source_medium,"linkedin"),"linkedin",
                                                                ifelse(str_detect(Session_source_medium,"facebook"),"facebook",
                                                                       ifelse(str_detect(Session_source_medium,"twitter"),"twitter",Session_source_medium
                                                                       )
                                                                )
                                                         )
                                                  )
)
]

#concatenated column utm_source + utm_medium
activities_unpivot[,Session_source_cont:=ifelse(Session_source!=Session_source_medium & !is.na(Session_source) & !is.na(Session_source_medium),str_squish(paste(Session_source,Session_source_medium)),
                                                ifelse(!is.na(Session_source),str_squish(Session_source),str_squish(Session_source_medium))
)]

#activities_unpivot$Session_source_cont<-NULL
#str_squish
# View(activities_unpivot)
# x<-unique(activities_unpivot[,c("Session_source","Session_source_medium","Session_source_cont")])
# write.csv(x,"utm_source_medium_concat_2.csv",row.names = FALSE)

# Topic of behavior ----------------------------------------------------------------------------------------------------

activities_unpivot[,topic_of_behavior:=
                     ifelse(str_detect(`Webpage URL`,"blog"),"Blog"
                            ,ifelse(str_detect(`Webpage URL`,"_wp_"),"White Paper"
                                    ,ifelse(str_detect(`Webpage URL`,"_fs_"),"Fact Sheet"
                                            ,ifelse(str_detect(`Webpage URL`,"/products/") 
                                                    | str_detect(`Webpage URL`,"/productos/") 
                                                    |str_detect(`Webpage URL`,"/produkte/")
                                                    | str_detect(`Webpage URL`,"/producten/"),"Products"
                                                    ,ifelse(str_detect(`Webpage URL`,"/e-commerce")
                                                            | str_detect(`Webpage URL`,"/comercio-electronico/")
                                                            | str_detect(`Webpage URL`,"/solucion/")
                                                            | str_detect(`Webpage URL`,"/solutions/")
                                                            | str_detect(`Webpage URL`,"/oplossingen/")
                                                            | str_detect(`Webpage URL`,"/loesung/"),"E-commerce/Solutions"            
                                                            ,ifelse(str_detect(`Webpage URL`,"resources")
                                                                    | str_detect(`Webpage URL`,"recursos")
                                                                    | str_detect(`Webpage URL`,"ressourcen"),"Resources"
                                                                    ,ifelse(`Webpage URL` == "/"
                                                                            |`Webpage URL` == "/nl/"
                                                                            |`Webpage URL` == "/de/"
                                                                            |`Webpage URL` == "/es/","Homepage"
                                                                            ,ifelse(str_detect(`Webpage URL`,"events"),"Events"
                                                                                    ,ifelse(str_detect(`Webpage URL`,"/jobs/")
                                                                                            | str_detect(`Webpage URL`,"/careers/"),"Recruitment"
                                                                                            ,ifelse(str_detect(`Webpage URL`,"_webinar"),"Webinar"
                                                                                                    ,ifelse(str_detect(`Webpage URL`,"/industries/")
                                                                                                            | str_detect(`Webpage URL`,"/industria/")
                                                                                                            | str_detect(`Webpage URL`,"/industrie/"),"Industries"
                                                                                                            ,ifelse(str_detect(`Webpage URL`,"/customers/")
                                                                                                                    | str_detect(`Webpage URL`,"/kunden/")
                                                                                                                    | str_detect(`Webpage URL`,"/klanten/")
                                                                                                                    | str_detect(`Webpage URL`,"/clientes/"),"Customers"
                                                                                                                    ,ifelse(str_detect(`Webpage URL`,"_ty_"),"Thank you Page"
                                                                                                                            ,ifelse(str_detect(`Webpage URL`,"demo"),"Demo "
                                                                                                                                    ,"Other"
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            )
                     )
]

# Recency -----------------------------------------------------------------

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
### NEW METHOD TEST ###
# If QO, get days diff between last activity before QO date and activity date
# If no QO, get days diff between last activity and activity date
# activities_unpivot$recency_days2 <- (difftime(activities_unpivot$max_activity_date,
#                                               activities_unpivot$activityDate,
#                                               units="days"))
# activities_unpivot$recency2 <- ifelse(activities_unpivot$recency_days>=0 & activities_unpivot$recency_days<=3,"3days",
#                                    ifelse(activities_unpivot$recency_days>3 & activities_unpivot$recency_days<=7,"7days",
#                                           ifelse(activities_unpivot$recency_days>7 & activities_unpivot$recency_days<=30,"30days","30daysplus")))
# 
# 
# activities_unpivot[,.(changed=mean(recency_days - recency_days2))]

# Content Type ------------------------------------------------------------
activities_unpivot[,content_type:=NULL]
activities_unpivot[,content_type:=ifelse(grepl("roi",`Webpage URL`, ignore.case = T),"roi",
                                         ifelse((grepl("buyer",`Webpage URL`, ignore.case = T)
                                                 |grepl("koper",`Webpage URL`, ignore.case = T)
                                                 |grepl("einkauf",`Webpage URL`, ignore.case = T)), "b2b buyer",
                                                ifelse(grepl("beginner",primaryAttributeValue, ignore.case = T),"beginners handbook",
                                                       ifelse(grepl("indust",`Webpage URL`, ignore.case = T), "industry focus",
                                                              ifelse(grepl("transf",`Webpage URL`, ignore.case = T), "digital transformation",
                                                                     "Other")
                                                 )
                                          )
                                   )
                            )
                   ]


# Source of Session -------------------------------------------------------

activities_unpivot[,Session_source_category:=
                     ifelse(str_detect(Session_source_cont,"referral"),"referral"
                            ,ifelse(str_detect(Session_source_cont,"google") & str_detect(Session_source_cont,"cpc")
                                    |str_detect(Session_source_cont,"adwords") & str_detect(Session_source_cont,"cpc")
                                    |str_detect(Session_source_cont,"bing") & str_detect(Session_source_cont,"cpc")
                                    |str_detect(Session_source_cont,"adform") & str_detect(Session_source_cont,"cpc")
                                    |str_detect(Session_source_cont,"appwiki") & str_detect(Session_source_cont,"cpc"),"Paid Search"
                                    ,ifelse(str_detect(Session_source_cont,"social") & str_detect(Session_source_cont,"cpc")
                                            |str_detect(Session_source_cont,"facebook") & str_detect(Session_source_cont,"cpc")
                                            |str_detect(Session_source_cont,"twitter") & str_detect(Session_source_cont,"cpc")
                                            |str_detect(Session_source_cont,"linkedin") & str_detect(Session_source_cont,"cpc"),"Paid Social"
                                            ,ifelse(str_detect(Session_source_cont,"social")
                                                    |str_detect(Session_source_cont,"blog")
                                                    |str_detect(Session_source_cont,"facebook")
                                                    |str_detect(Session_source_cont,"twitter")
                                                    |str_detect(Session_source_cont,"linkedin"),"social"
                                                    ,ifelse(str_detect(Session_source_cont,"email")
                                                            |str_detect(Session_source_cont,"e mail"),"email"
                                                            ,ifelse(str_detect(Session_source_cont,"direct"),"direct"
                                                                    ,ifelse(str_detect(Session_source_cont,"banner")
                                                                            |str_detect(Session_source_cont,"display"),"display"
                                                                            ,ifelse(str_detect(Session_source_cont,"print")
                                                                                    |str_detect(Session_source_cont,"asset"),"print"
                                                                                    ,ifelse(str_detect(Session_source_cont,"phone"),"phone"
                                                                                            ,"other"
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            )
                     )
                   ]



# Positive/Negative Marketo -----------------------------------------------

pos_activities <- c(1,2,11,10,100021,147)

activities_unpivot$pos_activity <- "Negative_Mkto"
activities_unpivot[activityTypeId %in% pos_activities, pos_activity:="Positive_Mkto"]


# Finalize full_dt and full_dt_unpivot ------------------------------------
#Creating pivot making dummy variable for categoricals 
activities_pivot <- dcast.data.table(activities_unpivot,leadId + opportunityid ~ pos_activity + Session_source_category) # +recency


# Remove unsubscribe and make has unsubscribed,.
# activities_pivot[,has_unsubscribed:=0]
# activities_pivot[activities_pivot[,rowSums(.SD)>0, .SDcols=activities_pivot[,grep("Unsubscribe", names(activities_pivot))]],
#                  has_unsubscribed:=1]
# set(activities_pivot, ,grep("Unsubscribe", names(activities_pivot)), NULL)

#activities_pivot_base <- dcast.data.table(activities_unpivot,leadId + opportunityid ~ activityName)
#activities_pivot <- dcast.data.table(activities_unpivot,leadId + opportunityid ~ activityName)

# Merge to create full_dt

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
                       annual_revenue_usd,
                       numberofemployees,
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
full_dt_unpivot <- merge(dt, activities_unpivot,by.x=c("leadid","opportunityid"),by.y=c("leadId","opportunityid"), all.x=T)

# Add QO outcome for full_dt and full_dt_unpivot

full_dt[,QO:=0]
full_dt[!is.na(QODate),QO:=1]
full_dt[,QO:=as.integer(QO)]

full_dt_unpivot[,QO:=0]
full_dt_unpivot[!is.na(QODate.x),QO:=1]
full_dt_unpivot[,QO:=as.integer(QO)]

# Save Full DT ------------------------------------------------------------

saveRDS(setDT(full_dt), file="./Data/Modeling Data/dt_1_16.RDS")
saveRDS(setDT(full_dt_unpivot), file="./Data/full_dt_unpivot_1.RDS")



