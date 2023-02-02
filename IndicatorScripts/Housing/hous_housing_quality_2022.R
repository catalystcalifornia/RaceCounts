## Percentage of Low Quality Housing Units RC v4 ##
# Install packages if not already installed
list.of.packages <- c("data.table", "stringr", "dplyr", "RPostgreSQL", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "here", "sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

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
library(here)
library(sf)

options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

######################Data set-up######################

##### GET PUMA-COUNTYCROSSWALK ######
crosswalk <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2020")
# View(head(crosswalk)) # use View(head()) to check data and avoid lagging
#unique(crosswalk$geoname)

# path where my data lives (not pulling pums data from the postgres db, too time-consuming) 

root <- "W:/Data/Demographics/PUMS/"

# Load the people PUMS data
ppl <- fread(paste0(root, "CA_2016_2020/psam_p06.csv"), header = TRUE, data.table = FALSE, 
            colClasses = list(character = c("PUMA", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH")))

# Load the housing PUMS data
housing <- fread(paste0(root, "CA_2016_2020/psam_h06.csv"), header = TRUE, data.table = FALSE, 
            colClasses = list(character = c("PUMA", "KIT", "HFL")))
# intersect(names(ppl), names(housing)) #use to find column names in common to remove when joining later


# Add state_id to ppl
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid,ppl$PUMA)
# View(head(ppl))
# create list of replicate weights: WGTP for housing file, PWGTP for person file
repwlist = rep(paste0("WGTP",1:80))

# join housing data to people data
# drop non-joined duplicate columns in housing
ppl <- left_join(ppl, housing[,-c(1, 3:6, 8)],  # left join  to the ppl column. Removed a few columns from housing before join.
                 by=c("SERIALNO"))    # specify the join field

# join county crosswalk using left join function
ppl <- left_join(ppl, crosswalk, by=c("puma_id" = "puma"))    # specify the field join

#################################### PUMS Functions ###########################################################
source("W:/RDA Team/R/Functions/PUMS_Functions.R")
ppl <- race_reclass(ppl)
# View(head(ppl))
# review data
table(ppl$race, useNA = "always")
table(ppl$race, ppl$latino, useNA = "always")
table(ppl$race, ppl$aian, useNA = "always")
table(ppl$race, ppl$pacisl, useNA = "always")
table(ppl$aian, useNA = "always")
table(ppl$pacisl, useNA = "always")

##### Define Housing Quality ###########
# filter data to include people who live in houses & householder records only
ppl <- ppl[ppl$TYPEHUGQ == "1" & ppl$SPORDER == "1", ]

# create indicator for low-quality houses. 1=low quality, 0=not low quality.
ppl$indicator <- as.factor(ifelse((ppl$PLM == "2" | ppl$KIT == "2" | ppl$HFL == "9"), "low_quality", "not_low_quality"))

#review
summary(ppl$indicator)
table(ppl$indicator, useNA = "always")

############### CALC COUNTY AND STATE ESTIMATES/CVS ETC. ############### 
# Define indicator and weight variables for function
  key_indicator <- 'low_quality'    # You must update this dependent on the indicator you're working with
# You must use to WGTP (if you are using psam_h06.csv and want housing units, like for Low Quality Housing) or PWGTP (if you want person units, like for Connected Youth)
  weight <- 'WGTP'           
# You must specify the population base you want to use for the rate calc. Ex. 100 for percents, or 1000 for rate per 1k.
  pop_base <- 100
  
rc_state <- state_pums(ppl)
View(rc_state)

rc_county <- county_pums(ppl)
View(rc_county)


############### COMBINE & SCREEN COUNTY/STATE DATA ############### 
# Define threshold variables for function
cv_threshold <- 35          # threshold and CV must be displayed as a percentage (not decimal)
raw_rate_threshold <- 0
pop_threshold <- 100

screened <- pums_screen(rc_state, rc_county)

# Extra layer of screening needed for this indicator --------------------
  hous_threshold <- 10
    screened$total_raw[screened$num_total < hous_threshold] <- NA
    screened$total_rate[screened$num_total < hous_threshold] <- NA
    screened$latino_raw[screened$num_latino < hous_threshold] <- NA
    screened$latino_rate[screened$num_latino < hous_threshold] <- NA
    screened$nh_white_raw[screened$num_nh_white < hous_threshold] <- NA
    screened$nh_white_rate[screened$num_nh_white < hous_threshold] <- NA
    screened$nh_black_raw[screened$num_nh_black < hous_threshold] <- NA
    screened$nh_black_rate[screened$num_nh_black < hous_threshold] <- NA
    screened$nh_asian_raw[screened$num_nh_asian < hous_threshold] <- NA
    screened$nh_asian_rate[screened$num_nh_asian < hous_threshold] <- NA
    screened$aian_raw[screened$num_aian < hous_threshold] <- NA
    screened$aian_rate[screened$num_aian < hous_threshold] <- NA
    screened$pacisl_raw[screened$num_pacisl < hous_threshold] <- NA
    screened$pacisl_rate[screened$num_pacisl < hous_threshold] <- NA
    screened$nh_other_raw[screened$num_nh_other < hous_threshold] <- NA
    screened$nh_other_rate[screened$num_nh_other < hous_threshold] <- NA
    screened$nh_twoormor_raw[screened$num_nh_twoormor < hous_threshold] <- NA
    screened$nh_twoormor_rate[screened$num_nh_twoormor < hous_threshold] <- NA

View(screened)

d <- screened
#View(d)


############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/2022_v4/RaceCounts/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

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
county_table_name <- "arei_hous_housing_quality_county_2022"
state_table_name <- "arei_hous_housing_quality_state_2022"
indicator <- "The percentage of low-quality housing units out of all housing units (lack of available kitchen, heating, or plumbing) by race of head of household.
White, Black, Asian, Other are one race alone and Latinx-exclusive. Two or More is Latinx-exclusive. 
AIAN and NHPI are Latinx-inclusive so they are also included in Latinx counts. 
AIAN and NHPI include AIAN and NHPI Alone and in Combo, so non-Latinx AIAN and NHPI in combo are also included in Two or More. This data is"
source <- "ACS PUMS 5-year estimates 2016-2020"

#send tables to postgres
to_postgres()
