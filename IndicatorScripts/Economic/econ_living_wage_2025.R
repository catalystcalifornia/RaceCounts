### Living Wage RC v7###

# Set up workspace --------------------------------------------------------
# Install packages if not already installed
packages <- c("data.table", "stringr", "dplyr", "RPostgres", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "here", "sf", "DBI", "usethis") 

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
curr_yr <- 2023 
rc_yr <- '2025'
rc_schema <- 'v7'

##### GET PUMA CROSSWALKS ######
crosswalk <- dbGetQuery(con, "select county_id AS geoid, county_name AS geoname, geo_id AS puma, num_county from crosswalks.puma_2022_county_2020")
assm_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldl24 AS geoid, num_dist AS num_assm from crosswalks.puma_2020_state_assembly_2024") %>%
                  rename(assm_geoid = geoid)
sen_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldu24 AS geoid, num_dist AS num_sen from crosswalks.puma_2020_state_senate_2024") %>%
                  rename(sen_geoid = geoid)

# Get PUMS Data -----------------------------------------------------------
# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf
# path where my data lives (not pulling pums data from the postgres db, takes too long to run calcs that way) 
start_yr <- curr_yr - 4  # autogenerate start yr of 5yr estimates
root <- paste0("W:/Data/Demographics/PUMS/CA_", start_yr, "_", curr_yr, "/")

# Load ONLY the PUMS columns needed for this indicator
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0))    # get all PUMS cols 
cols_wts <- grep("^PWGTP*", cols, value = TRUE)                   # filter for PUMS weight colnames

ppl <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE,  select = c(cols_wts, "AGEP", "ESR", "SCH", "PUMA",
             "ANC1P", "ANC2P", "HISP", "RAC1P", "RAC2P19", "RAC2P23", "RAC3P", "RACAIAN", "RACPI", "RACNH", "ADJINC",
             "WAGP", "PERNP", "COW", "WKHP", "WKL", "WRK", "WKWN"),
             colClasses = list(character = c("PUMA", "ANC1P", "ANC2P", "HISP", "RAC1P", "RAC2P19", "RAC2P23", "RAC3P", "RACAIAN", "RACPI", "RACNH", "ADJINC",
                                             "WAGP", "PERNP", "COW", "WKHP", "WKL", "ESR", "WRK", "WKWN")))

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with crosswalks.puma_county_2020
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid, ppl$PUMA)

# create list of replicate weights
repwlist = rep(paste0("PWGTP", 1:80))

# save copy of original data
orig_data <- ppl

# join county crosswalk using left join function
ppl <- left_join(orig_data, crosswalk, by=c("puma_id" = "puma"))   
ppl <- left_join(ppl, assm_crosswalk, by=c("puma_id" = "puma"), relationship = "many-to-many")
ppl <- left_join(ppl, sen_crosswalk, by=c("puma_id" = "puma"), relationship = "many-to-many")


############## Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf ###############

##### Reclassify Race/Ethnicity ########
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/PUMS_Functions_new.R")        # TEMPORARY re-direct to LF branch
# check how many records there are for RACAIAN (AIAN alone/combo) versus RAC1P (AIAN alone) and same for NHPI
#View(subset(ppl, RACAIAN =="1"))
#View(subset(ppl, RAC1P >= 3 & ppl$RAC1P <=5))
#View(subset(ppl, RACNH =="1" | RACPI =="1"))
#View(subset(ppl, RAC1P == 7))

# latino includes all races. AIAN is AIAN alone/combo latino/non-latino, NHPI is alone/combo latino/non-latino, SWANA includes all races and latino/non-latino
ppl <- race_reclass(ppl, start_yr, curr_yr)


# review data 
#View(ppl[c("HISP","latino","RAC1P","race","RAC2P","RAC3P","ANC1P","ANC2P", "aian", "pacisl", "swana")])
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
source("./Functions/RC_Functions.R")

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
indicator <- paste0("Created on ", Sys.Date(), ". Percent of workers earning above living wage ($15.50). Includes workers ages 18-64 who were at work last week or were employed but not at work.  Excludes those with zero earnings and self-employed or unpaid family workers. PUMAs contained by 1 county and PUMAs with 60%+ of their area contained by 1 county are included in the calcs. We also screened by pop (400) and CV (30%). White, Black, Asian, Other are one race alone and Latinx-exclusive. Two or More is Latinx-exclusive. AIAN and NHPI are Latinx-inclusive so they are also included in Latinx counts. AIAN, SWANA, and NHPI include AIAN, SWANA, and NHPI Alone and in combo, so non-Latinx AIAN and NHPI in combo are also included in Two or More. This data is")
source <- paste0("ACS PUMS (", start_yr, "-", curr_yr, ")")

#send tables to postgres
to_postgres()

#close connection
dbDisconnect(con)
