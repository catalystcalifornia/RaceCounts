# Lack of Greenspace Weighted Avg for RC 2023
##install packages if not already installed ------------------------------
list.of.packages <- c("dplyr","data.table","sf","tigris","readr","tidyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)
library(dplyr)
library(data.table)
library(tidycensus)
library(sf)
library(RPostgreSQL)
library(stringr)
library(tidyr)
library(tigris)
options(scipen=999)

###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")
con2<-connect_to_db("racecounts")

#set source for RC Functions script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")


##### GET INDICATOR DATA ######
# You MUST load indicator data using these table/column names (ind_df / subid / indicator) in order for functions to work
ind_df <- st_read(con, query = "select ct_geoid AS geoid, median from built_environment.nlcd_tract_impervious_land_2019")
ind_df <- dplyr::rename(ind_df, sub_id = geoid, indicator = median)       # rename columns for functions


############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')     # define your target geolevel
survey <- "acs5"                  # define which Census survey you want
pop_threshold <- 250              # define minimum pop threshold for pop screen

### Load CT-Place Crosswalk & Places ### ---------------------------------------------------------------------
xwalk_filter <- st_read(con, query = "select * from crosswalks.ct_place_2020")
places <- places(state = 'CA', year = 2020, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))

##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list_acs, yr = year, srvy = survey)  # subgeolevel pop

# transform pop data to wide format 
pop_wide <- lapply(pop, to_wide)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(xwalk_filter, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### CITY WEIGHTED AVG CALCS ######
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
city_wa <- city_wa %>% left_join(select(places, c(GEOID, NAME)), by = c("target_id" = "GEOID"))  # add in target geolevel names
city_wa <- city_wa %>% rename(target_name = NAME) %>% select(-c(geometry)) %>% mutate(geolevel = 'city')  # change NAME to target_name, drop geometry, add geolevel


############# COUNTY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold <- 250              # define minimum pop threshold for pop screen

##### CREATE COUNTY GEOID & NAMES TABLE ######  You will NOT need this chunk if your indicator data table has target geolevel names already
targetgeo_names <- county_names(vars = vars_list_acs, yr = year, srvy = survey)
targetgeo_names <- select(as.data.frame(targetgeo_names), target_id = GEOID, target_name = NAME) %>%   # get targetgeolevel names
  mutate(target_name = sub(" County, California", "", target_name))           # rename columns
targetgeo_names <- distinct(targetgeo_names, .keep_all = FALSE)                                        # keep only unique rows, 1 per target geo
#####


##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list_acs, yr = year, srvy = survey)  # subgeolevel pop

# transform pop data to wide format 
pop_wide <- lapply(pop, to_wide)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              # rename to generic column name for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide)


##### COUNTY WEIGHTED AVG CALCS ######
pct_df <- pop_pct(pop_df)   # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')    # add in target geolevel names


############# STATE CALCS ##################
# get and prep state pop
ca_pop_wide <- state_pop(vars = vars_list_acs, yr = year, srvy = survey)

# calc state wa
ca_pct_df <- ca_pop_pct(ca_pop_wide)
ca_wa <- ca_wt_avg(ca_pct_df) %>% mutate(geolevel = 'state')


############ JOIN CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% union(city_wa) 
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid)        # move geoname column


############ ADDITIONAL SCREENING FOR GREENSPACE ####################
# This screen converts any rate = 0 into NA, bc it's virtually impossible for there to be no roads, parking lots, or roofs
wa_all[, 3:11][wa_all[, 3:11] == 0] <- NA  # screens only cols 3:11 which are the _rate columns

d <- wa_all
View(d)

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)


#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


###update info for postgres tables###
county_table_name <- "arei_hben_lack_of_greenspace_county_2023"
state_table_name <- "arei_hben_lack_of_greenspace_state_2023"
city_table_name <- "arei_hben_lack_of_greenspace_city_2023"
rc_schema <- 'v5'

indicator <- "Impervious Landcover (%) is the weighted average of percentage of impervious land cover out of all land cover by race. Impervious land cover includes roads, roof tops, and parking lots"
source <- "Multi-Resolution Land Characteristics Consortium, National Land Cover Database (2019), ACS DP05 (2016-2020)"


#send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
dbDisconnect(con)
dbDisconnect(con2)
