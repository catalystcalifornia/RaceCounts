## Employment for RC v6 ##
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
library(tigris)
library(dplyr)
library(sf)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


############## UPDATE FOR SPECIFIC INDICATOR HERE ##############
curr_yr = 2022      # you MUST UPDATE each year
rc_yr = '2024'      # you MUST UPDATE each year
rc_schema <- "v6"   # you MUST UPDATE each year
cv_threshold = 40         
pop_threshold = 150       
asbest = 'max'            
schema = 'economic'
table_code = 's2301'


df_wide_multigeo <- st_read(con, query = paste0("select * from ",schema,".acs_5yr_",table_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state')")) # import rda_shared_data table

############## Pre-RC CALCS ##############
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
df <- prep_acs(df_wide_multigeo, table_code, cv_threshold, pop_threshold)

df_screened <- dplyr::select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"), -ends_with("_cv"))

d <- df_screened

############## CALC RACE COUNTS STATS ##############

#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

# Adds asbest value for RC Functions
d$asbest = asbest

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure asbest is correct before running this function.
d <- calc_diff(d) 
d <- calc_avg_diff(d)
d <- calc_s_var(d)
d <- calc_id(d)

### Split into geolevel tables
#split into STATE, COUNTY, CITY tables 
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table) %>% dplyr::select(-c(geolevel))
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table) 

## Calc county ranks##
county_table <- calc_ranks(county_table) %>% dplyr::select(-c(geolevel))
View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
city_table <- calc_ranks(city_table) %>% dplyr::select(-c(geolevel))
View(city_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")


############### COUNTY, STATE, CITY METADATA  ##############

### info for postgres tables will automatically update ###
county_table_name <- paste0("arei_econ_employment_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0("arei_econ_employment_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0("arei_econ_employment_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
indicator <- "Employment to Population Rate (%)"                 # See most recent Indicator Methodology for indicator description
start_yr <- curr_yr-4
source <- paste0(start_yr,"-",curr_yr,"ACS 5-Year Estimates, Table S2301, https://data.census.gov/cedsci/")   # See most recent Indicator Methodology for source info


####### SEND TO POSTGRES #######
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)

#dbDisconnect(con)
