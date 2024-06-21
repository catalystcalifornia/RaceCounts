### Living Wage RC v6###


# Install packages if not already installed
packages <- c("data.table", "stringr", "dplyr", "RPostgreSQL", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "here", "sf", "usethis") 

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(install_packages) > 0) {
  install.packages(install_packages)
} else {
  print("All required packages are already installed.")
}

for(pkg in packages){
  library(pkg, character.only = TRUE)
}

options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# define variables used throughout - update each year
curr_yr <- 2022 
rc_yr <- '2024'
rc_schema <- 'v6'

##### GET PUMA-COUNTYCROSSWALKS ######
# and rename fields to distinguish vintages
crosswalk10 <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2021")
crosswalk10 <- crosswalk10 %>% rename(puma10 = puma, geoid10 = geoid, geoname10 = geoname) 

crosswalk20 <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2022")
crosswalk20 <- crosswalk20 %>% rename(puma20 = puma, geoid20 = geoid, geoname20 = geoname) 

# Get PUMS Data -----------------------------------------------------------
# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2022.pdf
# path where my data lives (not pulling pums data from the postgres db, takes too long to run calcs that way) 
start_yr <- curr_yr - 4  # autogenerate start yr of 5yr estimates
root <- paste0("W:/Data/Demographics/PUMS/CA_", start_yr, "_", curr_yr, "/")

# Load ONLY the PUMS columns needed for this indicator
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0)) # get all PUMS cols 
cols_ <- grep("^PWGTP*", cols, value = TRUE)                                # filter for PUMS weight colnames

ppl <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE,  select = c(cols_, "AGEP", "ESR", "SCH", "PUMA10",
             "PUMA20", "ANC1P", "ANC2P", "HISP", "RAC1P", "RAC2P", "RAC3P", "RACAIAN", "RACPI", "RACNH", "ADJINC",
             "WAGP","PERNP", "COW", "WKHP","WKW","WKL", "WRK","WKWN"),
             colClasses = list(character = c("PUMA10", "PUMA20", "ANC1P", "ANC2P", "HISP", "RAC1P", "RAC2P", "RAC3P", "RACAIAN", "RACPI", "RACNH", "AGEP", "ADJINC",
                                             "WAGP","PERNP", "COW", "WKHP","WKW","WKL", "ESR","WRK","WKWN")))

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with crosswalks.puma_county_2020
ppl$state_geoid <- "06"
ppl$puma_id10 <- paste0(ppl$state_geoid, ppl$PUMA10)
ppl$puma_id20 <- paste0(ppl$state_geoid, ppl$PUMA20)

# create list of replicate weights
repwlist = rep(paste0("PWGTP", 1:80))

# save copy of original data
orig_data <- ppl

# join county crosswalk using left join function
ppl <- left_join(orig_data, crosswalk10, by=c("puma_id10" = "puma10"))    # specify the field join
ppl <- left_join(ppl, crosswalk20, by=c("puma_id20" = "puma20"))    # specify the field join

# create one field using both crosswalks
ppl <- ppl %>% mutate(geoid = ifelse(is.na(ppl$geoid10) & is.na(ppl$geoid20), NA, 
                                     ifelse(is.na(ppl$geoid10), ppl$geoid20, ppl$geoid10)))


ppl <- ppl %>% mutate(geoname = ifelse(is.na(ppl$geoname10) & is.na(ppl$geoname20), NA, 
                                       ifelse(is.na(ppl$geoname10), ppl$geoname20, ppl$geoname10)))


############## Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2022.pdf ###############

##### Reclassify Race/Ethnicity ########
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/PUMS_Functions_new.R")
# check how many records there are for RACAIAN (AIAN alone/combo) versus RAC1P (AIAN alone) and same for NHPI
#View(subset(ppl, RACAIAN =="1"))
#View(subset(ppl, RAC1P >= 3 & ppl$RAC1P <=5))
#View(subset(ppl, RACNH =="1" | RACPI =="1"))
#View(subset(ppl, RAC1P == 7))

# latino includes all races. AIAN is AIAN alone/combo latino/non-latino, NHPI is alone/combo latino/non-latino, SWANA includes all races and latino/non-latino
ppl <- race_reclass(ppl)


# review data 
View(ppl[c("HISP","latino","RAC1P","race","RAC2P","RAC3P","ANC1P","ANC2P", "aian", "pacisl", "swana")])
# table(ppl$race, useNA = "always")
# table(ppl$race, ppl$latino, useNA = "always")
# table(ppl$race, ppl$aian, useNA = "always")
# table(ppl$race, ppl$pacisl, useNA = "always")
# table(ppl$race, ppl$swana, useNA = "always")
# table(ppl$aian, useNA = "always")
# table(ppl$pacisl, useNA = "always")
# table(ppl$swana, useNA = "always")

####### Subset Data for Living Wage ########
# Adjust wage or salary income in past 12 months: WAGP (adjust with ADJINC)----
# trying wages first then will try earnings 
ppl$wages_adj <- (as.numeric(ppl$WAGP)*(as.numeric(ppl$ADJINC)/1000000))

# Filter data for pop of interest  ----
# Keep records only for those ages 18-64
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
## Used 15.50 since that went into effect January 2023
ppl$hrly_wage <- as.numeric(ppl$wages_adj/(ppl$wks_worked * ppl$wkly_hrs))
## View(ppl[c("RT","SERIALNO","wages_adj","WKHP","WKW","WKWN","wks_worked","wkly_hrs","hrly_wage")])

#* Code for Living Wage Indicator ----
# When $15.50 or more, code as livable. When less than $15.50 code as not livable. All other values code as NULL.
ppl$living_wage <- case_when(ppl$hrly_wage >= 15.50 ~ "livable", ppl$hrly_wage < 15.50 ~ "not livable", TRUE ~ "NA")
# View(ppl[c("RT","SERIALNO","COW","ESR","wages_adj","WKHP","WKW","WKWN","wks_worked","wkly_hrs","hrly_wage","living_wage")])

# Convert to factor for indicator
ppl$indicator <- as.factor(ppl$living_wage)

#review
summary(ppl$indicator)
table(ppl$indicator, useNA = "always")

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
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

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
county_table_name <- paste0("arei_econ_living_wage_county_", rc_yr)
state_table_name <- paste0("arei_econ_living_wage_state_", rc_yr)
indicator <- "Percent of workers earning above living wage ($15.50). Includes workers ages 18-64 who were at work last week or were employed but not at work.  Excludes those with zero earnings and self-employed or unpaid family workers. PUMAs contained by 1 county and PUMAs with 60%+ of their area contained by 1 county are included in the calcs. We also screened by pop (400) and CV (30%). White, Black, Asian, Other are one race alone and Latinx-exclusive. Two or More is Latinx-exclusive. AIAN and NHPI are Latinx-inclusive so they are also included in Latinx counts. AIAN, SWANA, and NHPI include AIAN, SWANA, and NHPI Alone and in combo, so non-Latinx AIAN and NHPI in combo are also included in Two or More. This data is"
source <- paste0("ACS PUMS (", start_yr, "-", curr_yr, ")")

#send tables to postgres
to_postgres()


