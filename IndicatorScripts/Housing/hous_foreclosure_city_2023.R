## Foreclosures per 10k Owner-Occupied Households (WA) for RC v4
######## Due to using a different population basis for WA (owner households), this is not a good script to use as a WA template. ##############


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
library(readxl)
library(janitor)
library(tigris)
options(scipen = 100) # disable scientific notation

###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

#set source for RC Functions script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")

# setwd("W:/Project/RACE COUNTS/2022_v4/Housing/Data/")

#load data and clean
foreclosure <- read_excel("W:/Data/Housing/Foreclosure/Foreclosure - Dataquick/Original Data/AdvanceProj 081122.xlsx", sheet = 2, skip = 5)

num_qtrs = 20   # update depending on how many data yrs you are working with
foreclosure <- foreclosure %>% select(-matches('2010|2011|2012|2013|2014|2015|2016|2022')) %>%
  mutate(sum_foreclosure = rowSums(.[3:22], na.rm = TRUE)) %>%  # total number of foreclosures over all data quarters
  mutate(avg_foreclosure = sum_foreclosure / num_qtrs) %>%  # avg quarterly number of foreclosures
  select(-matches('Q'))   # remove quarterly foreclosure columns

foreclosure$County = str_to_title(foreclosure$County)
foreclosure <- clean_names(foreclosure)     # remove spaces from col names
View(foreclosure)


# get census county geoids to paste to the front of the tracts from the foreclosure data
census_api_key(census_key1, overwrite=TRUE) # In practice, may need to include install=TRUE if switching between census api keys
Sys.getenv("CENSUS_API_KEY")
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)
ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
View(ca)

#merge dfs by geoname then paste the county id to the front of the tract IDs
ind_df <- left_join(ca, foreclosure, by = c("geoname" = "county")) %>% 
  mutate(sub_id = paste0(geoid, census_tract)) %>% 
  rename(c("target_id" = "geoid")) %>%
  select(target_id, sub_id, geoname, sum_foreclosure, avg_foreclosure)%>% 
  as.data.frame()
View(ind_df)
############# CITY ##################
# get census place geoids to paste to the front of the tracts from the foreclosure data



###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 250               # define population threshold for screening

### CT-Place Crosswalk ### ---------------------------------------------------------------------
## pull in 2020 CBF Places ##
places <- places(state = 'CA', year = 2020, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
tracts <- tracts(state = 'CA', year = 2020, cb = TRUE) %>% select(-c(STATEFP, TRACTCE, AFFGEOID, NAME, NAMELSAD, STATE_NAME, LSAD, ALAND, AWATER))

## spatial join ##
places_3310 <- st_transform(places, 3310) # change projection to 3310
tracts_3310 <- st_transform(tracts, 3310) # change projection to 3310
# calculate area of tracts and places
tracts_3310$area <- st_area(tracts_3310)
places_3310$pl_area <- st_area(places_3310)
# rename geoid fields
tracts_3310 <- tracts_3310%>% 
  rename("ct_geoid" = "GEOID", "county_geoid" = "COUNTYFP", "county_name" = "NAMELSADCO")
places_3310 <- places_3310%>% 
  rename("place_geoid" = "GEOID", "place_name" = "NAME")
# run intersect
tracts_places <- st_intersection(tracts_3310, places_3310) 
# create ct_place combo geoid field
tracts_places$ct_place_geoid <- paste(tracts_places$place_geoid, tracts_places$ct_geoid, sep = "_")
# calculate area of intersect
tracts_places$intersect_area <- st_area(tracts_places)
# calculate percent of intersect out of total place area
places_tracts <- tracts_places %>% mutate(prc_pl_area = as.numeric(tracts_places$intersect_area/tracts_places$pl_area))
# calculate percent of intersect out of total tract area
tracts_places$prc_area <- as.numeric(tracts_places$intersect_area/tracts_places$area)
# convert to df
tracts_places <- as.data.frame(tracts_places)
places_tracts <- as.data.frame(places_tracts)
# xwalk N = 16,342
xwalk <- full_join(places_tracts, select(tracts_places, c(ct_place_geoid, prc_area)), by = 'ct_place_geoid')
# filter xwalk where intersect between tracts and places is equal or greater than X% of tract area OR place area. xwalk_filter N = 9,675
threshold <- .25
xwalk_filter <- xwalk %>% filter(prc_area >= threshold | prc_pl_area >= threshold)
names(xwalk_filter) <- tolower(names(xwalk_filter)) # make col names lowercase
xwalk_filter <- select(xwalk_filter, ct_place_geoid, ct_geoid, place_geoid, county_geoid, place_name, namelsad, county_name, area, pl_area, intersect_area, prc_area, prc_pl_area)

# export xwalk table
table_name <- "ct_place_2020"
table_schema <- "crosswalks"
table_comment_source <- "Created with W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\IndicatorScripts\\Environment\\hben_drinking_water_2023.R and based on 2020 ACS TIGER non-CBF shapefiles.
    CTs with 25% or more of their area within a city or that cover 25% or more of a city''s area are assigned to those cities.
    As a result, a CT can be assigned to more than one city"

# make character vector for field types in postgresql db
charvect = rep('numeric', dim(xwalk_filter)[2])

# change data type for first three columns
charvect[1:7] <- "varchar" # first 7 are character for the geoid and names etc

# add names to the character vector
names(charvect) <- colnames(xwalk_filter)

#dbWriteTable(con, c(table_schema, table_name), xwalk_filter, 
#             overwrite = FALSE, row.names = FALSE,
#             field.types = charvect)

# write comment to table, and the first three fields that won't change.
table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

# send table comment to database
#dbSendQuery(conn = con, table_comment)      			


# ##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list, yr = year, srvy = survey)  # subgeolevel pop

# transform pop data to wide format
pop_wide <- lapply(pop, to_wide)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(xwalk_filter, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
# pop_df <- targetgeo_pop(pop_wide) # function doesn't work for this one so you do it custom for foreclosures and evictions

############### CUSTOMIZED VERSION OF TARGETGEO_POP FUNCTION HERE THAT WORKS WITH OWNER HOUSEHOLDS AS POP BASIS #######

# select pop estimate columns and rename to RC names
b <- select(pop_wide, sub_id, target_id, ends_with("e"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- c('target_id', 'total_target_pop', 'black_target_pop', 'aian_target_pop', 'asian_target_pop', 'pacisl_target_pop', 'other_target_pop', 'twoormor_target_pop', 'nh_white_target_pop', 'latino_target_pop')

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop_wide, sub_id, target_id, geolevel, ends_with("e"), -NAME) 
names(e) <- c('sub_id', 'target_id', 'geolevel', 'total_sub_pop', 'black_sub_pop', 'aian_sub_pop', 'asian_sub_pop', 'pacisl_sub_pop', 'other_sub_pop', 'twoormor_sub_pop', 'nh_white_sub_pop', 'latino_sub_pop')
pop_df <- e %>% left_join(c, by = "target_id")

###################################


##### EXTRA STEP: Calc avg quarterly foreclosures per 10k pop by tract bc WA avg should be calc'd using this, not avg # of foreclosures
ind_df <- ind_df %>% left_join(pop_df %>% select(sub_id, total_sub_pop), by = "sub_id") %>% 
  mutate(indicator = (avg_foreclosure / total_sub_pop) * 10000)

##### CITY WEIGHTED AVG CALCS ######
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
city_wa <- city_wa %>% left_join(select(places, c(GEOID, NAME)), by = c("target_id" = "GEOID"))  # add in target geolevel names
city_wa <- city_wa %>% rename(target_name = NAME) %>% select(-c(geometry)) %>% mutate(geolevel = 'city')  # change NAME to target_name, drop geometry, add geolevel


############# COUNTY CALCS ##################
#merge dfs by geoname then paste the county id to the front of the tract IDs
ind_df <- left_join(ca, foreclosure, by = c("geoname" = "county")) %>% 
  mutate(sub_id = paste0(geoid, census_tract)) %>% 
  rename(c("target_id" = "geoid")) %>%
  select(target_id, sub_id, geoname, sum_foreclosure, avg_foreclosure)%>% 
  as.data.frame()
View(ind_df)

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 30                # define population threshold for screening

##### CREATE COUNTY GEOID & NAMES TABLE ######  
# EXTRA STEP: Must define different vars_list than what's in WA functions, bc basis here is owner households by race, not population by race.
# select race/eth owner household variables: total, black, aian, asian, pacisl, other, twoormor, nh_white, latinx (All except Two+ and Latinx are 1 race alone)
vars_list_custom <- c("B25003_002", "B25003B_002", "B25003C_002", "B25003D_002", "B25003E_002", "B25003F_002", "B25003G_002", "B25003H_002", "B25003I_002")

targetgeo_names <- county_names(vars = vars_list_custom, yr = year, srvy = survey)  # NOTE: using custom 
targetgeo_names <- select(as.data.frame(targetgeo_names), target_id = GEOID, target_name = NAME) %>%   # get targetgeolevel names
  mutate(target_name = sub(" County, California", "", target_name))           # rename columns
targetgeo_names <- distinct(targetgeo_names, .keep_all = FALSE)                                        # keep only unique rows, 1 per target geo
#####


##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list_custom, yr = year, srvy = survey)  # subgeolevel pop. NOTE: This indicator uses a custom variable list (vars_list_custom)

# transform pop data to wide format 
pop_wide <- lapply(pop, to_wide)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              # rename to generic column name for WA functions

############### CUSTOMIZED VERSION OF TARGETGEO_POP FUNCTION HERE THAT WORKS WITH OWNER HOUSEHOLDS AS POP BASIS #######

# select pop estimate columns and rename to RC names
b <- select(pop_wide, sub_id, target_id, ends_with("e"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- c('target_id', 'total_target_pop', 'black_target_pop', 'aian_target_pop', 'asian_target_pop', 'pacisl_target_pop', 'other_target_pop', 'twoormor_target_pop', 'nh_white_target_pop', 'latino_target_pop')

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop_wide, sub_id, target_id, geolevel, ends_with("e"), -NAME) 
names(e) <- c('sub_id', 'target_id', 'geolevel', 'total_sub_pop', 'black_sub_pop', 'aian_sub_pop', 'asian_sub_pop', 'pacisl_sub_pop', 'other_sub_pop', 'twoormor_sub_pop', 'nh_white_sub_pop', 'latino_sub_pop')
pop_df <- e %>% left_join(c, by = "target_id")

###################################


##### EXTRA STEP: Calc avg quarterly foreclosures per 10k pop by tract bc WA avg should be calc'd using this, not avg # of foreclosures
ind_df <- ind_df %>% left_join(pop_df %>% select(sub_id, total_sub_pop), by = "sub_id") %>% 
  mutate(indicator = (avg_foreclosure / total_sub_pop) * 10000)


##### COUNTY WEIGHTED AVG CALCS ######
pct_df <- pop_pct(pop_df)   # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')     # add in target geolevel names


############# STATE CALCS ##################

############### CUSTOMIZED VERSION OF CA_POP_WIDE and CA_POP_PCT FUNCTION HERE THAT WORKS WITH OWNER HOUSEHOLD AS POP BASIS #######
# custom ca_pop_wide calcs
ca_pop <- do.call(rbind.data.frame, list(
  get_acs(geography = "state", state = "CA", variables = vars_list_custom, year = year, survey = survey, cache_table = TRUE)))
ca_pop <- ca_pop %>%  
  mutate(geolevel = "state")
ca_pop_wide <- select(ca_pop, GEOID, NAME, variable, estimate) %>% pivot_wider(names_from=variable, values_from=estimate)
colnames(ca_pop_wide) <- c('target_id', 'target_name', 'total_target_pop', 'black_target_pop', 'aian_target_pop', 'asian_target_pop', 'pacisl_target_pop', 'other_target_pop', 'twoormor_target_pop', 'nh_white_target_pop', 'latino_target_pop')

# custom ca_pop_pct calcs
subpop <- select(pop_wide, sub_id, ends_with("e"), target_id, -NAME)
subpop$target_id <- '06'                                        # replace county target_id values w/ state-level target_id value
colnames(subpop) <- c('sub_id', 'total_sub_pop', 'black_sub_pop', 'aian_sub_pop', 'asian_sub_pop', 'pacisl_sub_pop', 'other_sub_pop', 'twoormor_sub_pop', 'nh_white_sub_pop', 'latino_sub_pop', 'target_id')
subpop <- subpop %>% relocate(target_id, .after = sub_id)     # move sub_id column
subpop_long <- pivot_longer(subpop, 3:ncol(subpop), names_to="raceeth", values_to="sub_pop")
subpop_long$raceeth <- gsub("_sub_pop","-",as.character(subpop_long$raceeth))              # update to generic raceeth names

ca_pop_long <- pivot_longer(ca_pop_wide, 3:ncol(ca_pop_wide), names_to="raceeth", values_to="target_pop")
ca_pop_long$raceeth <- gsub("_target_pop","-",as.character(ca_pop_long$raceeth))           # update to generic raceeth names

subpop_long <- subpop_long %>% left_join(ca_pop_long, by=c("target_id" = "target_id", "raceeth" = "raceeth"))  # join target and sub pops in long form
ca_pcts_long <- subpop_long %>% mutate(pct = ifelse(target_pop < pop_threshold, NA, (sub_pop / target_pop)),   # calc pcts of each target geolevel pop per sub geolevel pop
                                       measure_pct=sub("-", "_pct_target_pop", raceeth))           # create new column names
ca_pct_df <- ca_pcts_long %>% select(sub_id, target_id, measure_pct, pct) %>%              # pivot long table back to wide keeping only new columns
  pivot_wider(names_from=measure_pct, values_from=pct)

##########################

# calc state WA
ca_wa <- ca_wt_avg(ca_pct_df) %>% mutate(geolevel = 'state')   # add geolevel type


############ JOIN CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% union(city_wa)
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid)# move geoname column

d <- wa_all
View(d)

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


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

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
county_table_name <- "arei_hous_foreclosure_county_2023"
state_table_name <- "arei_hous_foreclosure_state_2023"
city_table_name <- "arei_hous_foreclosure_city_2023"
indicator <- "Foreclosures per 10k owner households by race (WA). The data is"
source <- "DataQuick (2017-2021), purchased from DQNews and raced via weighted average using ACS 2016-20 data"
rc_schema <- 'v5'

#send tables to postgres
#to_postgres(county_table, state_table)
city_to_postgres(city_table)
