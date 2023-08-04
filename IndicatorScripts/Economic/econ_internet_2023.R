## Internet Access RC v5 ##
#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr", "usethis", "sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(tidyr)
library(stringr)
library(tidycensus)
library(dplyr)
library(DBI)
library(RPostgreSQL)
library(usethis)
library(sf)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


############## UPDATE FOR SPECIFIC INDICATOR HERE ##############

table_code = "s2802"     # YOU MUST UPDATE based on most recent Indicator Methodology or most recent RC Workflow/Cnty-State Indicator Tracking
cv_threshold = 40         # YOU MUST UPDATE based on most recent Indicator Methodology
pop_threshold = 100       # YOU MUST UPDATE based on most recent Indicator Methodology or set to NA B19301
asbest = 'max'            # YOU MUST UPDATE based on indicator, set to 'min' if S2701

############## PRE-CALCULATION DATA PREP ##############

df_wide_multigeo <- st_read(con, query = "SELECT * FROM economic.acs_5yr_s2802_multigeo_2021 WHERE geolevel IN ('place', 'county', 'state')")

# renaming rules will change depending on type of census table

  old_names <- colnames(df_wide_multigeo)[-(1:3)]
  new_names <- c("total_pop", "black_pop", "aian_pop", "asian_pop", "pacisl_pop",
                 "other_pop", "twoormor_pop", "latino_pop", "nh_white_pop", "total_raw", 
                 "black_raw", "aian_raw", "asian_raw", "pacisl_raw", "other_raw", 
                 "twoormor_raw", "latino_raw", "nh_white_raw", "total_rate", 
                 "black_rate", "aian_rate", "asian_rate", "pacisl_rate", "other_rate", 
                 "twoormor_rate", "latino_rate", "nh_white_rate", "total_pop_moe", 
                 "black_pop_moe", "aian_pop_moe", "asian_pop_moe", "pacisl_pop_moe", 
                 "other_pop_moe", "twoormor_pop_moe", "latino_pop_moe", "nh_white_pop_moe", 
                 "total_raw_moe", "black_raw_moe", "aian_raw_moe", "asian_raw_moe", 
                 "pacisl_raw_moe", "other_raw_moe", "twoormor_raw_moe", "latino_raw_moe", 
                 "nh_white_raw_moe", "total_rate_moe", "black_rate_moe", "aian_rate_moe", "asian_rate_moe", 
                 "pacisl_rate_moe", "other_rate_moe", "twoormor_rate_moe", "latino_rate_moe", 
                 "nh_white_rate_moe")
  df_wide_multigeo <- df_wide_multigeo %>%
    rename_with(~ new_names[which(old_names == .x)], .cols = old_names)

# Clean geo names
df_wide_multigeo$name <- gsub(", California", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" County", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" city", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" town", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" CDP", "", df_wide_multigeo$name)

############## PRE-CALCULATION POPULATION AND/OR CV CHECKS ##############

df <- df_wide_multigeo

### Do population checks and cv checks
  ## Calculate CV values for all rates - store in columns as cv_[race]_rate
  df$total_rate_cv <- ifelse(df$total_rate==0, NA, df$total_rate_moe/1.645/df$total_rate*100)
  df$asian_rate_cv <- ifelse(df$asian_rate==0, NA, df$asian_rate_moe/1.645/df$asian_rate*100)
  df$black_rate_cv <- ifelse(df$black_rate==0, NA, df$black_rate_moe/1.645/df$black_rate*100)
  df$nh_white_rate_cv <- ifelse(df$nh_white_rate==0, NA, df$nh_white_rate_moe/1.645/df$nh_white_rate*100)
  df$latino_rate_cv <- ifelse(df$latino_rate==0, NA, df$latino_rate_moe/1.645/df$latino_rate*100)
  df$other_rate_cv <- ifelse(df$other_rate==0, NA, df$other_rate_moe/1.645/df$other_rate*100)
  df$pacisl_rate_cv <- ifelse(df$pacisl_rate==0, NA, df$pacisl_rate_moe/1.645/df$pacisl_rate*100)
  df$twoormor_rate_cv <- ifelse(df$twoormor_rate==0, NA, df$twoormor_rate_moe/1.645/df$twoormor_rate*100)
  df$aian_rate_cv <- ifelse(df$aian_rate==0, NA, df$aian_rate_moe/1.645/df$aian_rate*100)
  
  ## Screen out rates with high CVs and low populations
  df$total_rate <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_rate))
  df$asian_rate <- ifelse(df$asian_rate_cv > cv_threshold, NA, ifelse(df$asian_pop < pop_threshold, NA, df$asian_rate))
  df$black_rate <- ifelse(df$black_rate_cv > cv_threshold, NA, ifelse(df$black_pop < pop_threshold, NA, df$black_rate))
  df$nh_white_rate <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_rate))
  df$latino_rate <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_rate))
  df$other_rate <- ifelse(df$other_rate_cv > cv_threshold, NA, ifelse(df$other_pop < pop_threshold, NA, df$other_rate))
  df$pacisl_rate <- ifelse(df$pacisl_rate_cv > cv_threshold, NA, ifelse(df$pacisl_pop < pop_threshold, NA, df$pacisl_rate))
  df$twoormor_rate <- ifelse(df$twoormor_rate_cv > cv_threshold, NA, ifelse(df$twoormor_pop < pop_threshold, NA, df$twoormor_rate))
  df$aian_rate <- ifelse(df$aian_rate_cv > cv_threshold, NA, ifelse(df$aian_pop < pop_threshold, NA, df$aian_rate))
  df$total_raw <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_raw))
  df$asian_raw <- ifelse(df$asian_rate_cv > cv_threshold, NA, ifelse(df$asian_pop < pop_threshold, NA, df$asian_raw))
  df$black_raw <- ifelse(df$black_rate_cv > cv_threshold, NA, ifelse(df$black_pop < pop_threshold, NA, df$black_raw))
  df$nh_white_raw <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_raw))
  df$latino_raw <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_raw))
  df$other_raw <- ifelse(df$other_rate_cv > cv_threshold, NA, ifelse(df$other_pop < pop_threshold, NA, df$other_raw))
  df$pacisl_raw <- ifelse(df$pacisl_rate_cv > cv_threshold, NA, ifelse(df$pacisl_pop < pop_threshold, NA, df$pacisl_raw))
  df$twoormor_raw <- ifelse(df$twoormor_rate_cv > cv_threshold, NA, ifelse(df$twoormor_pop < pop_threshold, NA, df$twoormor_raw))
  df$aian_raw <- ifelse(df$aian_rate_cv > cv_threshold, NA, ifelse(df$aian_pop < pop_threshold, NA, df$aian_raw))  
  
d <- select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"))

############## CALC RACE COUNTS STATS ##############

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

 # Adds asbest value for RC Functions
  d$asbest = asbest
  
  d <- count_values(d) #calculate number of "_rate" values
  d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
  d <- calc_diff(d) # check if 393-396 work as is with updated rc functions
  d <- calc_avg_diff(d)
  d <- calc_s_var(d)
  d <- calc_id(d)
  
  ### Split into geolevel tables
  #split into STATE, COUNTY, CITY tables 
  state_table <- d[d$geolevel == 'state', ]
  county_table <- d[d$geolevel == 'county', ]
  city_table <- d[d$geolevel == 'place', ]
  
  #calculate STATE z-scores
  state_table <- calc_state_z(state_table)
  View(state_table)
  
  #calculate COUNTY z-scores
  county_table <- calc_z(county_table)
  
  ## Calc county ranks##
  county_table <- calc_ranks(county_table)
  View(county_table)
  
  
  # #calculate CITY z-scores
  city_table <- calc_z(city_table)
   
  ## Calc city ranks##
  city_table <- calc_ranks(city_table)
  View(city_table)
  
  #rename geoid to state_id, county_id, city_id
  colnames(state_table)[1:2] <- c("state_id", "state_name")
  colnames(county_table)[1:2] <- c("county_id", "county_name")
  colnames(city_table)[1:2] <- c("city_id", "city_name")
  
  
  # ############## NON-DP05 ----- SEND COUNTY, STATE, CITY CALCULATIONS TO POSTGRES ##############
  
  ###update info for postgres tables###
  county_table_name <- "arei_econ_internet_county_2023"            # See most recent RC Workflow/v3 2021 SQL Views for table name (remember to update year to 2022)
  state_table_name <- "arei_econ_internet_state_2023"              # See most recent RC Workflow/v3 2021 SQL Views for table name (remember to update year to 2022)
  city_table_name <- "arei_econ_internet_city_2023"                # See most recent RC Workflow/v3 2021 SQL Views for table name (remember to update year to 2022)
  indicator <- "Persons with Internet Access (%)"                  # See most recent Indicator Methodology for indicator description
  source <- "2017-2021 ACS 5-Year Estimates, Table S2802, https://data.census.gov/cedsci/"   # See most recent Indicator Methodology for source info
  rc_schema <- "v5"
  
####### SEND TO POSTGRES #######
#to_postgres(county_table, state_table)
#city_to_postgres()
