## Employment for RC v7 ##
#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgres","tidycensus", "rvest", "tidyverse", "stringr", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(tidyr)
library(stringr)
library(tidycensus)
library(dplyr)
library(DBI)
library(RPostgres)
library(usethis)
library(here)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


############## UPDATE FOR SPECIFIC INDICATOR HERE ##############
curr_yr = 2023          # you MUST UPDATE each year with the last year from the 5-year ACS you're using
rc_yr = '2025'          # you MUST UPDATE each year
rc_schema <- "v7"       # you MUST UPDATE each year
cv_threshold = 40       # You may need to update     
pop_threshold = 150     # You may need to update      
asbest = 'max'          # Do not update      
schema = 'economic'     # Do not update
table_code = 's2301'    # Do not update

df_wide_multigeo <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",table_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
df_wide_multigeo$name <- str_remove(df_wide_multigeo$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
df_wide_multigeo$name <- gsub("; California", "", df_wide_multigeo$name)


############## Pre-RC CALCS ##############
#source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
source(".\\Functions\\rdashared_functions.R")
df <- prep_acs(df_wide_multigeo, table_code, cv_threshold, pop_threshold)

df_screened <- dplyr::select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"), -ends_with("_cv"))

d <- df_screened

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
#source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")
source(".\\Functions\\RC_Functions.R")

# Adds asbest value for RC Functions
d$asbest = asbest

# Calculate number of non-NA "_rate" values
d <- count_values(d) 

# Calculate best rate: confirm asbest is correct before running this function.
d <- calc_best(d) 

d <- calc_diff(d) 
d <- calc_avg_diff(d)
d <- calc_s_var(d)
d <- calc_id(d)

### Split into geolevel tables
#split into STATE, COUNTY, CITY, SLDU, SLDL tables 
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table) %>% dplyr::select(-c(geolevel))
View(state_table)

#calculate COUNTY z-scores and ranks
county_table <- calc_z(county_table) 

county_table <- calc_ranks(county_table) %>% dplyr::select(-c(geolevel))
View(county_table)

#calculate CITY z-scores and ranks
city_table <- calc_z(city_table)

city_table <- calc_ranks(city_table) %>% dplyr::select(-c(geolevel))
View(city_table)

#calculate SLDU z-scores and ranks
upper_table <- calc_z(upper_table)

upper_table <- calc_ranks(upper_table)
View(upper_table)

#calculate SLDL z-scores and ranks
lower_table <- calc_z(lower_table)

lower_table <- calc_ranks(lower_table)
View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")


############### COUNTY, STATE, CITY, SLDU, SLDL METADATA  ##############

### info for postgres tables will automatically update ###
county_table_name <- paste0("arei_econ_employment_county_", rc_yr)      
state_table_name <- paste0("arei_econ_employment_state_", rc_yr)        
city_table_name <- paste0("arei_econ_employment_city_", rc_yr)          
leg_table_name <- paste0("arei_econ_employment_leg_", rc_yr)            

indicator <- "Employment to Population Rate (%)"                 # See most recent Indicator Methodology for indicator description
start_yr <- curr_yr-4
source <- paste0(start_yr,"-",curr_yr," ACS 5-Year Estimates, Table S2301, https://data.census.gov/cedsci/. Script: ")   # See most recent Indicator Methodology for source info

####### SEND TO POSTGRES #######
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)
#leg_to_postgres(leg_table)

#dbDisconnect(con)
