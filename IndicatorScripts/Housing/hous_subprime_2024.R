## Subprime Mortgage Loans for RC v5 ##

## Set up ----------------------------------------------------------------
#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse","RPostgreSQL", "tidycensus", "readxl", "sf", "janitor", "stringr", "data.table", "usethis", "rvest", "tigris")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

## packages
library(tidyverse)
library(readxl)
library(RPostgreSQL)
library(sf)
library(tidycensus)
library(DBI)
library(janitor)
library(stringr)
library(data.table) # %like% operator
library(usethis)
library(rvest)
library(tigris)
library(jsonlite)
library(rlist)
library(httr)
library(curl)
options(scipen = 999) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")
setwd("W:/Data/Housing/HMDA/Subprime/2013-2017")


# update each year: variables used throughout script
acs_yr <- 2017         # last yr of acs 5y span that matches hmda yrs
hmda_yr <- '2014-2017' # hmda data yrs
rc_schema <- 'v6'
rc_yr <- '2024'


#### ALL APPLICATIONS DATA #### -------------------------------------------------------------------------
# Create rda_shared_data all applications table
# df_2013 <- read_csv("hmda_2013_ca_all-records_codes.csv")
# df_2014 <- read_csv("hmda_2014_ca_all-records_codes.csv")
# df_2015 <- read_csv("hmda_2015_ca_all-records_codes.csv")
# df_2016 <- read_csv("hmda_2016_ca_all-records_codes.csv")
# df_2017 <- read_csv("hmda_2017_ca_all-records_codes.csv")
# df_applications <- rbind(df_2013, df_2014,df_2015, df_2016, df_2017)

# export all applications to rda_shared_table ------------------------------------------------------------
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "housing"
# table_name <- "hmda_2017_5yr_all_applications"
# table_comment_source <- "ALL Home Mortgage Disclosure Act (HMDA) records including applications, denials, originations, institution purchases"
# table_source <- "HMDA historic Data: https://www.consumerfinance.gov/data-research/hmda/historic-data/. Raw data: W:/Data/Housing/HMDA/Subprime/2013-2017/hmda_xxxx_ca_all-records_codes.csv."
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# dbWriteTable(con2, c(table_schema, table_name), df_applications, overwrite = FALSE, row.names = FALSE)

# send table comment to database
# dbSendQuery(conn = con2, table_comment)  

df_applications <- dbGetQuery(con2, "SELECT * FROM housing.hmda_2017_5yr_all_applications")  # comment out above after table created, instead import from postgres


# Filter for home purchases, first lien, one-to-four family home, Owner Occupancy, Loan originated --------
##### Metadata: https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields/
df_applications <- df_applications %>% filter(lien_status == "1" & property_type == "1" & loan_purpose == "1" & owner_occupancy == "1" & action_taken %in% c("1"))


# Clean county GEOID Column ------------------------------------------- 
## paste leading zeros to county_code
df_applications <- df_applications %>% mutate(length = str_count(county_code, "[0-9]"),
                                                                        county_code = case_when(
                                                                          length == 1 ~ paste0("0600", county_code),
                                                                          length == 2 ~ paste0("060", county_code),
                                                                          length == 3 ~ paste0("06", county_code),
                                                                        )) 

## create ct_geoid field
# ct_nchar <- as.data.frame(nchar(df_applications$census_tract_number))  # check if ct numbers are all same length or if some need leading/trailing zeros
df_applications$ct_geoid <- paste0(df_applications$county_code, df_applications$census_tract_number) 
df_applications$ct_geoid = gsub("\\.", "", df_applications$ct_geoid)


###### SUBPRIME DATA ##### -------------------------------------------------------------------------
# # Import subprime data
# subprime_mortgages <- dbGetQuery(con, "SELECT * FROM data.hmda_2017_5yr_subprime_mortgages")
#
# # Add Census Tract GEOID Column -------------------------------------------
# ### first explored subprime tract numbers ### For ex: check if ct numbers are all same length or if some need leading/trailing zeros, some cts have decimals etc.
# ### figured out trailing zeroes were missing bc when i ran the code below adding leading zeroes instead, there were no matches with the ct-place xwalk.
# # tracts <- tracts(state = 'CA', year = 2017, cb = TRUE) %>% select(-c(STATEFP, TRACTCE, AFFGEOID, NAME, LSAD, ALAND, AWATER)) %>% st_drop_geometry()
#
# # relocate ct column and calc length of ct column
# subprime_clean <- subprime_mortgages %>% relocate(census_tract_number, .after = last_col()) %>% mutate(ct_nchar = str_count(census_tract_number, "[0-9]"))
#
# # split census_tract_number: digits before "." and digits after "."
# subprime_clean[c('ct_1', 'ct_2')] <- str_split_fixed(subprime_clean$census_tract_number, '\\.', 2)
# subprime_clean["ct_2"][subprime_clean["ct_2"] == ''] <- NA # replace blanks in ct_2 with NA

#
# # add trailing zeroes to cts WITHOUT decimals
# subprime_clean <- subprime_clean %>% mutate(ct_geoid = case_when(is.na(ct_2) & ct_nchar == 1 ~ paste0(census_tract_number, "0000"),
#                                                                    is.na(ct_2) & ct_nchar == 2 ~ paste0(census_tract_number, "000"),
#                                                                    is.na(ct_2) & ct_nchar == 3 ~ paste0(census_tract_number, "00"),
#                                                                    is.na(ct_2) & ct_nchar == 4 ~ paste0(census_tract_number, "0"),
#                                                                    is.na(ct_2) & ct_nchar == 5 ~ census_tract_number,
#                                              )) # there are 115 rows where ct_geoid is NA where ct_2 == '', this is bc census_tract_number is NA

# # add trailing zeroes to cts WITH decimals
# subprime_clean <- subprime_clean %>% mutate(ct_geoid = case_when(is.na(ct_geoid) & nchar(ct_1) == 1 ~ paste0("000", ct_1),
#                                              is.na(ct_geoid) & nchar(ct_1) == 2 ~ paste0("00", ct_1),
#                                              is.na(ct_geoid) & nchar(ct_1) == 3 ~ paste0("0", ct_1),
#                                              is.na(ct_geoid) & nchar(ct_1) == 4 ~ ct_1,
#                                              .default = as.character(subprime_clean$ct_geoid)
#                                              ))

# # add trailing zeroes and/or digits after decimal
# subprime_clean <- subprime_clean_2 %>% mutate(ct_geoid = case_when(nchar(ct_2) == 1 ~ paste0(ct_geoid, ct_2, "0"),
#                                              nchar(ct_2) == 2 ~ paste0(ct_geoid, ct_2),
#                                              nchar(ct_geoid) == 4 ~ paste0(ct_geoid, "00"),
#                                              nchar(ct_geoid) == 5 ~ paste0(ct_geoid, "0"),
#                                              .default = as.character(subprime_clean$ct_geoid)
#                                              ))

# # add county fips prefix
# subprime_clean <- subprime_clean_3 %>% mutate(ct_geoid = case_when(nchar(ct_geoid) == 6 ~ paste0(county_fips, ct_geoid),
#                                              .default = as.character(subprime_clean$ct_geoid)
#                                       )) # there are 115 rows where ct_geoid is NA bc census_tract_number is NA


# export clean subprime rda_shared_data table
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "housing"
# table_name <- "hmda_tract_subprime_mortgages_2013_17"
# table_comment_source <- "This table is the same as racecounts.hmda_2017_5yr_subprime_mortgages, but with a newly added ct_geoid field which is cleaned up/properly formatted tract fips codes. See: W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\IndicatorScripts\\Housing\\hous_subprime_2023.R. Raw data was originally downloaded from this URL which is no longer available: https://www.consumerfinance.gov/data-research/hmda/explore#!/as_of_year=2017,2016,2015,2014,2013&state_code-1=6&action_taken=1&rate_spread!=null&section=filters. On that page just added years 2013-17, chose the state of California, left loan originated selected (under loan), and chose YES to is it a higher-priced loan? (also under loan). Saved as W:\\Project\\RACE COUNTS\\Data\\Postgres Tables and Views\\county and state views\\2017\\subprime_county_2017\\hmda_2013_17_subprime.csv"
# table_source <- "HMDA 2013-2017"
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# dbWriteTable(con2, c(table_schema, table_name), subprime_clean, overwrite = FALSE, row.names = FALSE)

# send table comment to database
# dbSendQuery(conn = con2, table_comment)

df_subprime <- dbGetQuery(con2, "SELECT * FROM housing.hmda_tract_subprime_mortgages_2013_17")  # comment out above after table created, instead import from postgres

# Filter for home purchases, first lien, one-to-four family home, owner-occupied, Loan originated --------
df_subprime <- df_subprime %>% filter(lien_status == "1" & property_type == "1" & loan_purpose == "1" & owner_occupancy == "1" & action_taken %in% c("1"))

# Function to aggregate data for total and by race  -----------------------
#### data dictionary: https://files.consumerfinance.gov/hmda-historic-data-dictionaries/lar_record_codes.pdf
calculations <- function(df,geoid,column) {
  ## total 
  total <- df %>% group_by({{geoid}}) %>% summarize(total_observations = n())
  
  ## nh white
  nh_white <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "5" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_white_observations = n())
  
  ## nh asian
  nh_asian <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "2" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_asian_observations = n())
  
  ## nh black
  nh_black <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "3" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_black_observations = n())
  
  ## all pacisl 
  pacisl <- df %>% filter(applicant_race_1 == "4") %>% group_by({{geoid}}) %>% summarize(pacisl_observations  = n())
  
  ## all aian
  aian <- df %>% filter(applicant_race_1 == "1") %>% group_by({{geoid}}) %>% summarize(aian_observations = n())
  
  ## nh two or more
  nh_twoormor <- df %>% filter(applicant_ethnicity == "2" & !is.na(applicant_race_1) & !is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_twoormor_observations = n())
  
  ## latino
  latino <- df %>% filter(applicant_ethnicity == "1") %>% group_by({{geoid}}) %>% summarize(latino_observations = n())
  
  z <- total %>% full_join(nh_white) %>% full_join(nh_asian) %>% full_join(nh_black) %>% full_join(pacisl) %>% full_join(aian) %>% full_join(nh_twoormor) %>% full_join(latino) %>% rename_all(
    funs(stringr::str_replace_all(., 'observations', column)
    ))
  
  return(z)
}


# City Calculations -------------------------------------------------------
# merge applications and subprime with crosswalk
## This is a many-to-many join because a census tract can belong in multiple places.
crosswalk_2017 <- dbGetQuery(con2, "SELECT place_geoid, place_name, ct_geoid FROM crosswalks.ct_place_2017") # there are 1,522 unique cities
df_applications_crosswalk <- df_applications %>% right_join(crosswalk_2017) # join keeping only ct's that are in xwalk
subprime_mortgages_crosswalk <- df_subprime %>% right_join(crosswalk_2017) # join keeping only ct's that are in xwalk

applications_city <- calculations(df = df_applications_crosswalk, geoid =  place_geoid, column = 'applications')
subprime_city <- calculations(df = subprime_mortgages_crosswalk, geoid = place_geoid,  column = 'subprime')

## Combine applications with subprime
df_city_merged <- applications_city %>% full_join(subprime_city)

## screen out cities with < 75 loans originated then calculate rates
threshold <- 75

            df_city <- df_city_merged %>% mutate(
            total_raw = ifelse(total_applications < threshold, NA, total_subprime), # 90 obs filtered
            nh_white_raw = ifelse(nh_white_applications < threshold, NA, nh_white_subprime), # 184 obs filtered
            nh_asian_raw = ifelse(nh_asian_applications < threshold, NA, nh_asian_subprime), # 211 obs filtered
            nh_black_raw = ifelse(nh_black_applications < threshold, NA, nh_black_subprime), # 290 obs filtered
            pacisl_raw = ifelse(pacisl_applications < threshold, NA, pacisl_subprime), # 240 obs filtered
            aian_raw = ifelse(aian_applications < threshold, NA, aian_subprime), #271 obs filtered
            nh_twoormor_raw = ifelse(nh_twoormor_applications < threshold, NA, nh_twoormor_subprime), #178 obs filtered
            latino_raw = ifelse(latino_applications < threshold, NA, latino_subprime), #328 obs filtered
            
            total_rate = (total_raw/total_applications) * 100,
            nh_white_rate = (nh_white_raw/nh_white_applications) * 100,
            nh_asian_rate = (nh_asian_raw/nh_asian_applications) * 100,
            nh_black_rate = (nh_black_raw/nh_black_applications) * 100,
            pacisl_rate = (pacisl_raw/pacisl_applications) * 100,
            aian_rate = (aian_raw/aian_applications) * 100,
            nh_twoormor_rate = (nh_twoormor_raw/nh_twoormor_applications) * 100,
            latino_rate = (latino_raw/latino_applications) * 100,
            
            geolevel = 'place') %>% rename(geoid = place_geoid)

# merge with city and name
city_list <- places(state = 'CA', year = 2017, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, LSAD, ALAND, AWATER)) %>% st_drop_geometry()
df_city <- df_city %>% left_join(city_list, by=c("geoid" = "GEOID")) %>% rename(geoname = NAME) %>% select(geoid, geoname, geolevel, everything())


# County / State Calculations -----------------------------------------------------
### not using data-frames joined to xwalks because of duplicates. Use imported postgres tables: df_applications / df_subprime
### County ###
applications_county <- calculations(df = df_applications, geoid = county_code, column = 'applications') 
subprime_county <- calculations(df = df_subprime, geoid = county_fips,  column = 'subprime') %>% rename(county_code = county_fips)

## Combine applications with subprime
df_county <- applications_county %>% full_join(subprime_county) %>% rename(geoid = county_code) %>% drop_na(geoid)


### State ###
applications_state <- calculations(df = df_applications, geoid = state_code, column = 'applications') %>% mutate(state_code = as.character(paste("0",state_code, sep= "")))
subprime_state <- calculations(df = df_subprime, geoid = state_fips,  column = 'subprime') %>% rename(state_code = state_fips)

## Combine applications with subprime
df_state <- applications_state %>% full_join(subprime_state) %>% rename(geoid = state_code)

# Combine county and state
df <- rbind(df_county, df_state)

### County / State raw and rate calculations ### Use same threshold as city data (< 75 loans originated)
            df_calcs <- df %>% mutate(
            total_raw = ifelse(total_applications < threshold, NA, total_subprime),
            nh_white_raw = ifelse(nh_white_applications < threshold, NA, nh_white_subprime),
            nh_asian_raw = ifelse(nh_asian_applications < threshold, NA, nh_asian_subprime),
            nh_black_raw = ifelse(nh_black_applications < threshold, NA, nh_black_subprime),
            pacisl_raw = ifelse(pacisl_applications < threshold, NA, pacisl_subprime),
            aian_raw = ifelse(aian_applications < threshold, NA, aian_subprime),
            nh_twoormor_raw = ifelse(nh_twoormor_applications < threshold, NA, nh_twoormor_subprime),
            latino_raw = ifelse(latino_applications < threshold, NA, latino_subprime),
            
            total_rate = (total_raw/total_applications) * 100,
            nh_white_rate = (nh_white_raw/nh_white_applications) * 100,
            nh_asian_rate = (nh_asian_raw/nh_asian_applications) * 100,
            nh_black_rate = (nh_black_raw/nh_black_applications) * 100,
            pacisl_rate = (pacisl_raw/pacisl_applications) * 100,
            aian_rate = (aian_raw/aian_applications) * 100,
            nh_twoormor_rate = (nh_twoormor_raw/nh_twoormor_applications) * 100,
            latino_rate = (latino_raw/latino_applications) * 100,
            geolevel = ifelse(geoid == "06", "state", "county")
            ) %>% select(geoid, geolevel, everything())


## get census geoids ------------------------------------------------------
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2017)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

#add county geonames
df_county_state <- merge(x=ca, y=df_calcs, by="geoid", all=T)

#add state geoname
df_county_state <- within(df_county_state, geoname[geoid == '06'] <- 'California')

############ JOIN CITY, COUNTY & STATE TABLES ##################
df_final <- rbind(df_county_state, df_city)

# create d for functions
d <- df_final


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
#source("W:/Project/RACE COUNTS/Functions/archive/RC_Functions.R")
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/refs/heads/main/Functions/RC_Functions.R")
d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d %>% filter(geolevel == "state") %>% select(-c(geolevel)) 

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)


#split COUNTY into separate table and format id, name columns
county_table <- d %>% filter(geolevel == "county") %>% select(-c(geolevel)) 

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)


#split CITY into separate table and format id, name columns
city_table <- d %>% filter(geolevel == "place") %>% select(-c(geolevel)) 

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


###update info for postgres tables###
county_table_name <- paste0("arei_hous_subprime_county_", rc_yr)
state_table_name <- paste0("arei_hous_subprime_state_", rc_yr)
city_table_name <- paste0("arei_hous_subprime_city_", rc_yr)
indicator <- paste0("Number of higher priced Loans Per 100 Loans Originated from ", hmda_yr, ". Subgroups with fewer than ", threshold, " loans originated are excluded")
source <- paste0("HMDA historic Data: https://www.consumerfinance.gov/data-research/hmda/historic-data/, however Subprime data is not available here. Created ", Sys.Date())


#send to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)

dbDisconnect()
