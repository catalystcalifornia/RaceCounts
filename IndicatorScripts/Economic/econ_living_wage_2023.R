### Living Wage RC v5###

#Load libraries
library(data.table)
library(stringr)
library(dplyr)
library(RPostgreSQL)
library(dbplyr)
library(srvyr)
library(tidycensus)
library(tidyr)
library(rpostgis)
library(tidyr)
library(here)
library(sf)

options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

####### GET PUMS DATA & PUMA-COUNTYCROSSWALK #######
# crosswalk
crosswalk <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2020")

# PUMS Data
## path where my data lives 
root <- "W:/Data/Demographics/PUMS/"

## Load the people PUMS data
ppl <- fread(paste0(root, "CA_2017_2021/psam_p06.csv"), header = TRUE, data.table = FALSE, 
             colClasses = list(character = c("PUMA", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH", "AGEP", "ADJINC","WAGP","PERNP", "COW", "WKHP","WKW","WKL", "ESR","WRK","WKWN")))

             
## Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with crosswalks.puma_county_2020
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid, ppl$PUMA)

## create list of replicate weights
repwlist = rep(paste0("PWGTP", 1:80))

# join county crosswalk 
## use left join function
ppl <- left_join(ppl, crosswalk, by=c("puma_id" = "puma"))    # specify the field join


####### Data Dictionary: W:\Data\Demographics\PUMS\CA_2017_2021\PUMS_Data_Dictionary_2017-2021.pdf #######

####### Load RC PUMS Functions #######
source("W:/RDA Team/R/Functions/PUMS_Functions.R")

####### Reclassify Race/Ethnicity #######
# check how many records there are for RACAIAN (AIAN alone/combo) versus RAC1P (AIAN alone) and same for NHPI
#View(subset(ppl, RACAIAN =="1"))
#View(subset(ppl, RAC1P >= 3 & ppl$RAC1P <=5))
#View(subset(ppl, RACNH =="1" | RACPI =="1"))
#View(subset(ppl, RAC1P == 7))

ppl <- race_reclass(ppl)

# review data 
# View(ppl[c("RT","SERIALNO","RAC1P","HISP","latino", "race","RACAIAN", "aian", "RACNH","RACPI","pacisl")])
# table(ppl$race, useNA = "always")
# table(ppl$race, ppl$latino, useNA = "always")
# table(ppl$race, ppl$aian, useNA = "always")
# table(ppl$race, ppl$pacisl, useNA = "always")
# table(ppl$aian, useNA = "always")
# table(ppl$pacisl, useNA = "always")

# save copy unadultered prior to filtering
ppl_orig <- ppl

####### Subset Data for Living Wage ########
# Adjust wage or salary income in past 12 months: WAGP (adjust with ADJINC)----
# trying wages first then will try earnings 
ppl$wages_adj <- (as.numeric(ppl$WAGP)*(as.numeric(ppl$ADJINC)/1000000))

# Filter data for pop of interest  ----
# Keep records only for those ages 18-24
ppl <- ppl %>% filter(AGEP >= 18 & AGEP <= 64)

# Keep records for those with non-zero earnings in past year
ppl <- ppl %>% filter(wages_adj>0)

# Keep records for those who were at work last week OR had a job but were not at work last week
ppl <- ppl %>% filter(WRK=='1' | ESR %in% c(1, 2, 3, 4, 5))

# Filter for those who were not self-employed or unpaid family workers
# Note this is a different from v3 which didn't do this step
# View(ppl[c("RT","SERIALNO","COW","ESR","wages_adj","WKW","WRK","WKWN")])
# mean_cow <- ppl%>%
#   group_by(COW)%>%
#     summarize(mean_wages=weighted.mean(wages_adj,PWGTP))
# 6-8 which are self-employed and then employed in family business do seem to have different average earnings than others
ppl <- ppl %>% filter(!COW %in% c('6','7','8'))

####### Calculate Living Wage #######
# Calculate hourly wage ----
# First calculate number of hours worked based on weekly hours and weeks worked
## convert usual hours worked per week past 12 months: WKHP to integer
ppl$wkly_hrs <- as.integer(ppl$WKHP)

## average number of weeks worked in past 12 months for each value 1-6: WKW is the pre-2019 variable
ppl$wks_worked_avg <- as.numeric(ifelse(ppl$WKW == 1, 51,
                                        ifelse(ppl$WKW == 2, 48.5,
                                               ifelse(ppl$WKW == 3, 43.5,
                                                      ifelse(ppl$WKW == 4, 33,
                                                             ifelse(ppl$WKW == 5, 20, 
                                                                    ifelse(ppl$WKW == 6, 7, 0)))))))

## create final weeks worked variable using WKWN variable for 2019 or later
ppl$wks_worked <- as.numeric(ifelse(ppl$WKWN>=1, ppl$WKWN, ppl$wks_worked_avg))
## View(ppl[c("RT","SERIALNO","wages_adj","WKW","WKWN","wks_worked","wks_worked_avg")])

# Then calculate hourly wage
## Use 15.50 since that is going into effect January 2023
ppl$hrly_wage <- as.numeric(ppl$wages_adj/(ppl$wks_worked * ppl$wkly_hrs))
## View(ppl[c("RT","SERIALNO","wages_adj","WKHP","WKW","WKWN","wks_worked","wkly_hrs","hrly_wage")])

#* Code for Living Wage Indicator ----
# When $15.50 or more, code as livable. When less than $15.50 code as not livable. All other values code as NULL.
ppl$living_wage <- case_when(ppl$hrly_wage >= 15.50 ~ "livable", ppl$hrly_wage < 15.50 ~ "not livable", TRUE ~ "NA")
# View(ppl[c("RT","SERIALNO","COW","ESR","wages_adj","WKHP","WKW","WKWN","wks_worked","wkly_hrs","hrly_wage","living_wage")])

# Convert to factor for indicator
ppl$indicator <- as.factor(ppl$living_wage)

#review
# summary(ppl$indicator)
# table(ppl$indicator, useNA = "always")

# Test disparities for state
# install.packages("spatstat")
# library(spatstat)
# median_race<-ppl%>%
#   group_by(race)%>%
#   summarize(median_hrly_wages=weighted.median(hrly_wage,PWGTP, na.rm=TRUE))
# ppl$living_wage_num <- ifelse(ppl$hrly_wage >= 15.50, 1, 0)
# living_wage_race<-ppl%>%
#   group_by(race)%>%
#   summarize(living_wage=weighted.mean(living_wage_num,PWGTP, na.rm=TRUE))
## looks as expected

######## Test using earnings -- no need to QA ########
# ppl_test<-ppl_orig #save data under different name
# # Subset Data for Living Wage 
# # Adjust wage or salary income in past 12 months: WAGP (adjust with ADJINC)
# # trying wages first then will try earnings 
# ppl_test$earnings <- (as.numeric(ppl_test$PERNP)*(as.numeric(ppl_test$ADJINC)/1000000))
# 
# # Filter data for pop of interest  
# # Keep records only for those ages 18-24
# ppl_test <- ppl_test %>% filter(AGEP >= 18 & AGEP <= 64)
# 
# # Keep records for those with non-zero earnings in past year
# ppl_test <- ppl_test %>% filter(earnings>0)
# 
# # Keep records for those who were at work last week or had a job but were not at work last week
# ppl_test <- ppl_test %>% filter(WRK=='1' | ESR %in% c(1, 2, 3, 4, 5))
# 
# # Filter for those who were not self-employed or unpaid family workers
# ppl_test <- ppl_test %>% filter(!COW %in% c('6','7','8'))
# 
# # Calculate Living Wage 
# # Calculate average number of hours worked per week
# # convert usual hours worked per week past 12 months: WKHP to integer
# ppl_test$wkly_hrs <- as.integer(ppl_test$WKHP)
# 
# # average number of weeks worked in past 12 months for each value 1-6: WKW pre-2019 data
# ppl_test$wks_worked_avg<-as.numeric(ifelse(ppl_test$WKW == 1, 51,
#                                       ifelse(ppl_test$WKW == 2, 48.5,
#                                              ifelse(ppl_test$WKW == 3, 43.5,
#                                                     ifelse(ppl_test$WKW == 4, 33,
#                                                            ifelse(ppl_test$WKW == 5, 20, 
#                                                                   ifelse(ppl_test$WKW == 6, 7, 0)))))))
# 
# # created final weeks worked variable using WKWN variable for 2019 or later
# ppl_test$wks_worked<-as.numeric(ifelse(ppl_test$WKWN>=1, ppl_test$WKWN, ppl_test$wks_worked_avg))
# #View(ppl_test[c("RT","SERIALNO","earnings","WKW","WKWN","wks_worked","wks_worked_avg")])
# 
# # Calculate hourly wage 
# # Use 15.50 since that is going into effect January 2023
# ppl_test$hrly_wage <- as.numeric(ppl_test$earnings/(ppl_test$wks_worked * ppl_test$wkly_hrs))
# # View(ppl_test[c("RT","SERIALNO","earnings","WKHP","WKW","WKWN","wks_worked","wkly_hrs","hrly_wage")])
# 
# # Code for Living Wage Indicator 
# # When $15.50 or more, code as livable. When less than $15.50 code as not livable. All other values code as NULL.
# ppl_test$living_wage <- case_when(ppl_test$hrly_wage >= 15.50 ~ "livable", ppl_test$hrly_wage < 15.50 ~ "not livable", TRUE ~ "NA")
# # View(ppl_test[c("RT","SERIALNO","COW","ESR","earnings","WKHP","WKW","WKWN","wks_worked","wkly_hrs","hrly_wage","living_wage")])
# 
# # test disparities for state
# median_race_earnings<-ppl_test%>%
#   group_by(race)%>%
#   summarize(median_hrly_wages=weighted.median(hrly_wage,PWGTP, na.rm=TRUE))
# ppl_test$living_wage_num <- ifelse(ppl_test$hrly_wage >= 15.50, 1, 0)
# living_wage_race_earnings<-ppl_test%>%
#   group_by(race)%>%
#   summarize(living_wage=weighted.mean(living_wage_num,PWGTP, na.rm=TRUE))
## not a huge difference compared to wages, go with wages

############### CALC COUNTY AND STATE ESTIMATES/CVS ETC. ############### 
# Define indicator and weight variables for function
# You must use to WGTP (if you are using psam_h06.csv and want housing units, like for Low Quality Housing) or PWGTP (if you want person units, like for Connected Youth)
key_indicator <- 'livable'  # update this to the indicator you are working with
weight <- 'PWGTP' 
# You must specify the population base you want to use for the rate calc. Ex. 100 for percents, or 1000 for rate per 1k.
pop_base <- 100
rc_state <- state_pums(ppl)
View(rc_state)

rc_county <- county_pums(ppl)
View(rc_county)


############ COMBINE & SCREEN COUNTY/STATE DATA ############# 
# Define threshold variables for function
cv_threshold <- 30          # threshold and CV must be displayed as a percentage (not decimal)
raw_rate_threshold <- 0
pop_threshold <- 400

screened <- pums_screen(rc_state, rc_county)
View(screened)

d <- screened


############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
View(county_table)

###update info for postgres tables###
county_table_name <- "arei_econ_living_wage_county_2023"
state_table_name <- "arei_econ_living_wage_state_2023"
indicator <- "Percent of workers earning above living wage for 2023 ($15.50). Includes workers ages 18-64 who were at work last week or were employed but not at work.  Excludes those with zero earnings and self-employed or unpaid family workers. PUMAs contained by 1 county and PUMAs with 60%+ of their area contained by 1 county are included in the calcs. We also screened by pop (400) and CV (30%). White, Black, Asian, Other are one race alone and Latinx-exclusive.Two or More is Latinx-exclusive. AIAN and NHPI are Latinx-inclusive so they are also included in Latinx counts. AIAN and NHPI include AIAN and NHPI Alone and in Combo, so non-Latinx AIAN and NHPI in combo are also included in Two or More."
source <- "ACS PUMS (2017-2021)"
rc_schema <- "v5"

#send tables to postgres
to_postgres()


