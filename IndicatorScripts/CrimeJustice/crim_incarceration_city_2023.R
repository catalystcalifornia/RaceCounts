## Incarceration 2020 (City-Level) for RC v5

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

options(scipen = 999) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")

#set source for weighted averages script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")

# pull in data ------------------------------------------------------------

#https://stackoverflow.com/questions/68873466/read-html-cannot-read-a-webpage-despite-specifying-different-encodings
url <- read_html("https://www.prisonpolicy.org/origin/ca/2020/tract.html")

datatable <- url %>% html_table(fill = TRUE)

dt <- datatable [[1]] # keep first

df  <- dt %>% clean_names() %>% as.data.frame() # clean up names

df$fips_code_2020 <- paste0("0", df$fips_code_2020) # add leading zeroes here to match the crosswalk fips codes
df$census_population_2020 = as.integer(gsub("\\,", "", df$census_population_2020)) # change text fields to integer, remove commas
df$total_population_2020 = as.integer(gsub("\\,", "", df$total_population_2020))
df$imprisonment_rate_per_100_000 = as.integer(gsub("\\,", "", df$imprisonment_rate_per_100_000))

# export incarceration  to rda shared table ------------------------------------------------------------
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "crime_and_justice"
table_name <- "prison_policy_incarceration_tract_2020"
table_comment_source <- "Number of people in prison in 2020 from each California Census tract"
table_source <- "Incarceration data downloaded 7/11/2023
from https://www.prisonpolicy.org/origin/ca/2020/tract.html"

#dbWriteTable(con2, c(table_schema, table_name), df, overwrite = FALSE, row.names = FALSE)

# write comment to table, and the first three fields that won't change.
table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")

# send table comment to database
#dbSendQuery(conn = con2, table_comment)  

#rename columns for weighted average functions to work ------------------------------------------------------------
ind_df <- df %>% rename(sub_id = fips_code_2020, indicator = imprisonment_rate_per_100_000) %>% as.data.frame() 

# pull in ct-city crosswalk ------------------------------------------------------------
crosswalk <- dbGetQuery(con2, "SELECT * FROM crosswalks.ct_place_2020")

# pull in place shapes ------------------------------------------------------------
places <- places(state = 'CA', year = 2020, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))

# Weighted Averages Functions  ------------------------------------------------------------

##### GET SUB GEOLEVEL POP DATA ######

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')      # define your target geolevel: place
survey <- "census"                # define which Census survey you want
pop_threshold = 250               # define population threshold for screening
census_api_key(census_key1)       # reload census API key

##### GET SUB GEOLEVEL POP DATA ######
vars_list <- "vars_list_p2"
pop <- update_detailed_table_census(vars = vars_list_p2, yr = year, srvy = survey)  # subgeolevel total, nh alone pop
vars_list <- "vars_list_dp"
pop2 <- update_detailed_table_census(vars = vars_list_dp, yr = year, srvy = survey)  # all aian, all nhpi subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)
pop2_wide <- pop2 %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value) %>% select(GEOID, "DP1_0088C", "DP1_0090C")
pop_wide <- pop_wide %>% left_join(pop2_wide, by = 'GEOID')
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions


# rename columns to appropriate name ------------------------------------

# Census Labels
#p2_001n # Total:
#p2_005n # nh_white: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone
#p2_006n # nh_black: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone
#p1_005n # aian: !!Total:!!Population of one race:!!American Indian and Alaska Native alone
#p2_008n # nh_asian: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone
#p1_007n # pacisl: !!Total:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone
#p2_010n # nh_other: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone
#p2_011n # nh_twoormor: !!Total:!!Not Hispanic or Latino:!!Population of two or more races
#p2_002n # latinx: !!Total:!!Hispanic or Latino

#dp1_0088c # all aian
#dp1_0090c # all nhpi

pop_wide <- pop_wide %>% rename(
  total_pop = P2_001N,
  nh_white_pop = P2_005N,
  nh_black_pop = P2_006N,
  nh_asian_pop = P2_008N,
  nh_other_pop = P2_010N,
  nh_twoormor_pop = P2_011N,
  latino_pop = P2_002N,
  aian_pop = DP1_0088C,
  pacisl_pop = DP1_0090C) %>% 
  select(sub_id, target_id, NAME, geolevel, ends_with("pop"))

#  CITY WEIGHTED AVG CALCS ------------------------------------------------

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide)
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens

city_wa <- city_wa %>% left_join(select(places, c(GEOID, NAME)), by = c("target_id" = "GEOID")) %>%
  rename(geoname = NAME, geoid = target_id) %>% select(-c(geometry)) %>% mutate(geolevel = 'city') %>% dplyr::relocate(geoname, .after = geoid)   # rename columns for RC functions

# aggregate total raw -----------------------------------------------------
raw_df <- df %>% rename(ct_geoid =  fips_code_2020, total_raw = number_of_people_in_state_prison_from_each_census_tract_2020) %>% select(ct_geoid, total_raw) %>% right_join(select(crosswalk, c(ct_geoid, place_geoid))) %>% group_by(place_geoid) %>%  summarize(total_raw = sum(total_raw)) 


## merge raw with city weighted averages
city_wa <- city_wa %>% left_join(raw_df, by = c("geoid" = "place_geoid")) %>% dplyr::relocate(total_rate, .after = geoname) %>% select(-c(total_raw))  # remove total_raw bc should not appear on website

# final df
d <- city_wa

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
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))


#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)

city_table_name <- "arei_crim_incarceration_city_2023"
rc_schema <- 'v5'

indicator <- paste0("Created on ", Sys.Date(), ". Number of people in prison in 2020 - weighted average by race")
source <- "NOTE: This is a different source than the county/state incarceration indicator. Prison Policy Org https://www.prisonpolicy.org/origin/ca/2020/tract.html."

#send to postgres
#city_to_postgres(city_table)

dbDisconnect(con)
dbDisconnect(con2)



