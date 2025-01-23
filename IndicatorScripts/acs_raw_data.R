### Create updated ACS raw data tables (in postgres db) to be used in RC v7 ###

##### Source external functions/values #####
# connect to rda_shared_data database, get census api key
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("rda_shared_data") 
census_api_key(census_key1)
readRenviron("~/.Renviron")

rc_schema <- 'v7'
rc_yr <- '2025'

# Set source for ACS 5-Yr table update fx
source("W:\\RDA Team\\R\\GitHub\\RDA Functions\\main\\RDA-Functions\\acs_rda_shared_tables.R") # This fx also creates or imports the correct vintage CA CBF ZCTA list

# Script file path, for postgres table comment
filepath <- paste("W:/Project/RACE COUNTS/", rc_yr, "_", rc_schema, "/RC_Github/RaceCounts/IndicatorScripts/acs_raw_data.R")

# Define arguments for ACS table update fx
yr <- 2023 # update for the ACS data/ZCTA vintage needed
      
### If you add a new table, you must also update table_vars below
rc_acs_indicators <- list(
        "DP05",   # population
        "B04006", # ancestry
        "S2701",  # health insurance
        "S2802",  # internet access
        "S2301",  # employment
        "B19301", # per capita income
        "B25003", # homeownership
        "B25014", # overcrowded housing
        "B01001"  # pop by age
) 

## Run fx to get updates ACS table(s)
update_acs(yr=yr, acs_tables=rc_acs_indicators,filepath)

