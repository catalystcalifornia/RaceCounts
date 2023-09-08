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


# Create rda_shared_data all applications table -------------------------------------------------------------
# df_2013 <- read_csv("hmda_2013_ca_all-records_codes.csv")
# df_2014 <- read_csv("hmda_2014_ca_all-records_codes.csv")
# df_2015 <- read_csv("hmda_2015_ca_all-records_codes.csv")
# df_2016 <- read_csv("hmda_2016_ca_all-records_codes.csv")
# df_2017 <- read_csv("hmda_2017_ca_all-records_codes.csv")
# df_applications <- rbind(df_2013, df_2014,df_2015, df_2016, df_2017)

# export all applications to rda shared table ------------------------------------------------------------
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


# Add Census Tract GEOID Column ------------------------------------------- 
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


# merge with cross-walk
## There are some duplicates but that's okay because a census tract can belong in multiple places.
crosswalk_2017 <- dbGetQuery(con2, "SELECT * FROM crosswalks.ct_place_2017")
df_applications_crosswalk <- df_applications %>% left_join(crosswalk_2017)

###### subprime data ### -------------------------------------------------------------------------
# Import subprime data
subprime_mortgages <- dbGetQuery(con, "SELECT * FROM data.hmda_2017_5yr_subprime_mortgages")

# Filter for home purchases, first lien, one-to-four family homea, Owner-Occupancy, Loan originated --------
subprime_mortgages <- subprime_mortgages %>% filter(lien_status == "1" & property_type == "1" & loan_purpose == "1" & owner_occupancy == "1" & action_taken %in% c("1"))

# Add Census Tract GEOID Column ------------------------------------------- 
# ct_nchar <- as.data.frame(nchar(subprime_mortgages$census_tract_number))  # check if ct numbers are all same length or if some need leading/trailing zeros
subprime_mortgages$ct_nchar <- nchar(subprime_mortgages$census_tract_number)
temp <- select(subprime_mortgages, census_tract_number, ct_geoid, ct_nchar)
subprime_mortgages <- subprime_mortgages %>% mutate(length = str_count(census_tract_number, "[0-9]"),
                                              county_code = case_when(
                                                length == 2 ~ paste0("00", census_tract_number, "00"),
                                                length == 2 ~ paste0("060", census_tract_number),
                                                length == 3 ~ paste0("06", census_tract_number),
                                              )) 

subprime_mortgages$ct_geoid <- paste0(subprime_mortgages$county_fips, subprime_mortgages$census_tract_number) 
subprime_mortgages$ct_geoid = gsub("\\.", "", subprime_mortgages$ct_geoid)

# merge with cross-walk
## There are some duplicates but that's okay because a census tract can belong in multiple places.
subprime_mortgages_crosswalk <- subprime_mortgages %>% left_join(crosswalk_2017)

# Function to calculate total and race calculations -----------------------
#### data dictionary: https://files.consumerfinance.gov/hmda-historic-data-dictionaries/lar_record_codes.pdf
calculations <- function(df,geolevel,column) {
## total 
total <- df %>% group_by(applicant_ethnicity) %>% 
   filter(if(any(applicant_ethnicity == 2)) applicant_race_1 != 6 else TRUE) %>% group_by({{geolevel}}) %>% summarize(total_observations = n())
    
## nh white
nh_white <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "5" & is.na(applicant_race_2)) %>% group_by({{geolevel}}) %>% summarize(nh_white_observations = n())

## nh asian
nh_asian <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "2" & is.na(applicant_race_2)) %>% group_by({{geolevel}}) %>% summarize(nh_asian_observations = n())

## nh black
nh_black <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "3" & is.na(applicant_race_2)) %>% group_by({{geolevel}}) %>% summarize(nh_black_observations = n())

## all pacisl 
pacisl <- df %>% filter(applicant_race_1 == "4") %>% group_by({{geolevel}}) %>% summarize(pacisl_observations  = n())

## all aian
aian <- df %>% filter(applicant_race_1 == "1") %>% group_by({{geolevel}}) %>% summarize(aian_observations = n())

## nh two or more
nh_twoormor <- df %>% filter(applicant_ethnicity == "2" & !is.na(applicant_race_1) & !is.na(applicant_race_2)) %>% group_by({{geolevel}}) %>% summarize(nh_twoormor_observations = n())

## latino
latino <- df %>% filter(applicant_ethnicity == "1") %>% group_by({{geolevel}}) %>% summarize(latino_observations = n())

z <- total %>% full_join(nh_white) %>% full_join(nh_asian) %>% full_join(nh_black) %>% full_join(pacisl) %>% full_join(aian) %>% full_join(nh_twoormor) %>% full_join(latino) %>% rename_all(
      funs(stringr::str_replace_all(., 'observations', column)
      ))

return(z)
}


# City Calculations -------------------------------------------------------
applications_city <- calculations(df = df_applications_crosswalk, geolevel =  place_geoid, column = 'applications')
subprime_city <- calculations(df = subprime_mortgages_crosswalk, geolevel = place_geoid,  column = 'subprime')

## Combine applications with subprime
df_city_merged <- applications_city %>% full_join(subprime_city)

## screen for <75 applications then calculate rates
threshold <- 75

df_city <- df_city_merged %>% mutate(
total_raw = ifelse(total_applications <threshold, NA, total_subprime), # 90 obs filtered
nh_white_raw = ifelse(nh_white_applications <threshold , NA, nh_white_subprime), # 184 obs filtered
nh_asian_raw = ifelse(nh_asian_applications <threshold , NA, nh_asian_subprime), # 211 obs filtered
nh_black_raw = ifelse(nh_black_applications <threshold , NA, nh_black_subprime), # 290 obs filtered
pacisl_raw = ifelse(pacisl_applications <threshold , NA, pacisl_subprime), # 240 obs filtered
aian_raw = ifelse(aian_applications <threshold , NA, aian_subprime), #271 obs filtered
nh_twoormor_raw = ifelse(nh_twoormor_applications <threshold , NA, nh_twoormor_subprime), #178 obs filtered
latino_raw = ifelse(latino_applications <threshold , NA, latino_subprime), #328 obs filtered

total_rate = (total_raw/total_applications) * 100,
nh_white_rate = (nh_white_raw/nh_white_applications) * 100,
nh_asian_rate = (nh_asian_raw/nh_asian_applications) * 100,
nh_black_rate = (nh_black_raw/nh_black_applications) * 100,
pacisl_rate = (pacisl_raw/pacisl_applications) * 100,
aian_rate = (aian_raw/aian_applications) * 100,
nh_twoormor_rate = (nh_twoormor_raw/nh_twoormor_applications) * 100,
latino_rate = (latino_raw/latino_applications) * 100
) %>% rename(
geoid = place_geoid
)

# merge with city and name
city_list <- places(state = 'CA', year = 2017, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, LSAD, ALAND, AWATER)) %>% st_drop_geometry()
crosswalk_2017 <- dbGetQuery(con2, "SELECT * FROM crosswalks.ct_place_2017")
df_city<- df_city %>% left_join(city_list) %>% rename(geoname = name) %>% select(geoid,geoname, geolevel, everything())

# filter geoids where city did not match to census tract geoid
df_city <- df_city %>% filter(!is.na(geoid))

# County Calculations -----------------------------------------------------


## County ##
### not using cross-walk data-frame because of potential duplicates. Using original which already has county code
applications_county <- calculations(df = df_applications, geolevel = county_code, column = 'applications') 

subprime_county <- calculations(df = subprime_mortgages, geolevel = county_fips,  column = 'subprime') %>% rename(county_code = county_fips)

## Combine applications with subprime

df_county <- applications_county %>% full_join(subprime_county) %>% rename(geoid = county_code)


### State ###

applications_state <- calculations(df = df_applications, geolevel = state_code, column = 'applications') %>% mutate(state_code = as.character(paste("0",state_code, sep= "")))

subprime_state <- calculations(df = subprime_mortgages, geolevel = state_fips,  column = 'subprime') %>% rename(state_code = state_fips)

df_state <- applications_state %>% full_join(subprime_state) %>% rename(geoid = state_code)


# combine county and state

df <- rbind(df_county, df_state)

# filter NA obs
df <- df %>% filter(!is.na(geoid))

# calculations for county and state

df_calcs <- df %>% mutate(
total_raw = ifelse(total_applications <threshold, NA, total_subprime),
nh_white_raw = ifelse(nh_white_applications <threshold , NA, nh_white_subprime),
nh_asian_raw = ifelse(nh_asian_applications <threshold , NA, nh_asian_subprime),
nh_black_raw = ifelse(nh_black_applications <threshold , NA, nh_black_subprime),
pacisl_raw = ifelse(pacisl_applications <threshold , NA, pacisl_subprime),
aian_raw = ifelse(aian_applications <threshold , NA, aian_subprime),
nh_twoormor_raw = ifelse(nh_twoormor_applications <threshold , NA, nh_twoormor_subprime),
latino_raw = ifelse(latino_applications <threshold , NA, latino_subprime),

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


# merge with county

## get census geoids ------------------------------------------------------
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2019)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")


#add county geonames
df_county_state <- merge(x=ca, y=df_calcs, by="geoid", all=T)


#add state geoname
df_county_state<- within(df_county_state, geoname[geoid == '06'] <- 'California')

############ JOIN CITY, COUNTY & STATE TABLES, REMOVE NA observations for GEOID ##################
df_final <- rbind(df_county_state, df_city)

# create d for functions
d <- df_final




############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns

state_table <- d %>% filter(geoname == "California") %>%  select(-c(geolevel)) 

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)


#split COUNTY into separate table and format id, name columns
county_table <- d %>% filter(geolevel == "county") %>%  select(-c(geolevel)) 

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)


#split CITY into separate table and format id, name columns
city_table <- d %>% filter(geolevel == "place") %>%  select(-c(geolevel)) 
#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


city_table_name <- "arei_hous_subprime_city_2023"
rc_schema <- 'v5'

indicator <- "Number of higher priced Loans Per 100 Mortgage Applications from 2013-2017"
source <- "HMDA historic Data: https://www.consumerfinance.gov/data-research/hmda/historic-data/. Created 8-17-23"


#send to postgres
###city_to_postgres(city_table)

### look at V2

#county_v2 <- dbGetQuery(con, "SELECT * FROM data.arei_subprime_county_2017") %>% arrange(county_id)

#county_v2 %>% select(county_name, quadrant) %>% filter(is.na(quadrant))




#state_v2 <- dbGetQuery(con, "SELECT * FROM data.arei_subprime_state_2017")

#state_v2 %>% select(ends_with("rate"))





