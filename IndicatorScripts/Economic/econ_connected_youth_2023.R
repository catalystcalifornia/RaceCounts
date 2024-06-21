### Connected Youth RC v5 ###

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

##### GET PUMA-COUNTYCROSSWALK ######
crosswalk <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2020")


# Get PUMS Data -----------------------------------------------------------
# path where my data lives (not pulling pums data from the postgres db, takes too long to run calcs that way) 
root <- "W:/Data/Demographics/PUMS/"

# Load the people PUMS data
ppl <- fread(paste0(root, "CA_2017_2021/psam_p06.csv"), header = TRUE, data.table = FALSE, 
             colClasses = list(character = c("PUMA", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH", "ESR", "SCH")))

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with crosswalks.puma_county_2020
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid, ppl$PUMA)

# create list of replicate weights
repwlist = rep(paste0("PWGTP", 1:80))

# join county crosswalk using left join function
ppl <- left_join(ppl, crosswalk, by=c("puma_id" = "puma"))    # specify the field join

############## Data Dictionary: W:\Data\Demographics\PUMS\CA_2017-2021\PUMS_Data_Dictionary_2017-2021.pdf ###############

##### Reclassify Race/Ethnicity ########
# check how many records there are for RACAIAN (AIAN alone/combo) versus RAC1P (AIAN alone) and same for NHPI
#View(subset(ppl, RACAIAN =="1"))
#View(subset(ppl, RAC1P >= 3 & ppl$RAC1P <=5))
#View(subset(ppl, RACNH =="1" | RACPI =="1"))
#View(subset(ppl, RAC1P == 7))

source("W:/RDA Team/R/Functions/PUMS_Functions.R")
ppl <- race_reclass(ppl)

# review data 
  # table(ppl$race, useNA = "always")
  # table(ppl$race, ppl$latino, useNA = "always")
  # table(ppl$race, ppl$aian, useNA = "always")
  # table(ppl$race, ppl$pacisl, useNA = "always")
  # #table(ppl$aian, useNA = "always")
  # #table(ppl$pacisl, useNA = "always")

##### Define Connected Youth ###########
# Keep records only for those ages 16-24
ppl <- ppl %>% filter(AGEP >= 16 & AGEP <= 24)

# Filtering for those unemployed or not in the labor force 
ppl$employment <- ifelse(ppl$ESR %in% c(3,6), "not employed", "employed")

# Filtering for those not attending school and in school
ppl$schl_enroll <-  ifelse(ppl$SCH == 1, "not attending school", "in school")

# verify coding 
table(ppl$employment, ppl$ESR, useNA = "always")
table(ppl$SCH, ppl$schl_enroll, useNA = "always")
table(ppl$employment, ppl$schl_enroll, useNA = "always")

# Combine both conditions to create connected/disconnected variable 
ppl$indicator <- as.factor(ifelse(ppl$employment == "not employed" & ppl$schl_enroll =="not attending school", "disconnected", "connected"))

#review
summary(ppl$indicator)
table(ppl$indicator, useNA = "always")

############### CALC COUNTY AND STATE ESTIMATES/CVS ETC. ############### 
# Define indicator and weight variables for function
  key_indicator <- 'connected'  # update this to the desired ppl$indicator value you are working with
# You must use to WGTP (if you are using psam_h06.csv and want housing units, like for Low Quality Housing) or PWGTP (if you want person units, like for Connected Youth)
  weight <- 'PWGTP'           
# You must specify the population base you want to use for the rate calc. Ex. 100 for percents, or 1000 for rate per 1k.
  pop_base <- 100
  
rc_state <- state_pums(ppl)
View(rc_state)

rc_county <- county_pums(ppl)
View(rc_county)


############ COMBINE & SCREEN COUNTY/STATE DATA ############# 
# Define threshold variables for function
  cv_threshold <- 20          # threshold and CV must be displayed as a percentage (not decimal)
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
county_table_name <- "arei_econ_connected_youth_county_2023"
state_table_name <- "arei_econ_connected_youth_state_2023"
indicator <- "Connected Youth out of all Youth (%). Connected Youth are those ages 16-24 who are in school and/or employed. PUMAs contained by 1 county and PUMAs with 60%+ of their area contained by 1 county are included in the calcs, we also screened by pop and CV. White, Black, Asian, Other are one race alone and Latinx-exclusive. Two or More is Latinx-exclusive. AIAN and NHPI are Latinx-inclusive so they are also included in Latinx counts. AIAN and NHPI include AIAN and NHPI Alone and in Combo, so non-Latinx AIAN and NHPI in combo are also included in Two or More. This data is"
source <- "ACS PUMS (2017-2021)"
rc_schema <- "v5"

#send tables to postgres
to_postgres()

