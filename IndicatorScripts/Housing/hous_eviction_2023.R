######### Eviction Filings for RC v5 #########

##install packages if not already installed ------------------------------
list.of.packages <- c("dplyr","data.table","sf","tigris","readr","tidyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr","usethis")

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
library(usethis)


###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

#set source for RC Functions script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")

# setwd("W:/Project/RACE COUNTS/2022_v4/Housing/Data/")

# Load the data & prep data for weighted average function
#### data in the 'valid' file has already been screened for large year-on-year fluctuations by Evictions Lab
df_orig <- fread("W:/Data/Housing/Eviction Lab/2000-2018/tract_proprietary_valid_2000_2018.csv", header = TRUE, data.table = FALSE)  

df <- df_orig %>% dplyr::filter((state == "California") & grepl("2014|2015|2016|2017|2018", year)) %>% 
  mutate(county_id = paste0("0", cofips), county_name = gsub(" County", "", county), fips = paste0("0", fips)) %>% 
  select(fips, county_id, county_name, year, filings) 
#View(df)


######### Screening / Data Exploration ##########
# get count of ct's per county for context
census_api_key(census_key1, overwrite=TRUE) # In practice, may need to include install=TRUE if switching between census api keys
Sys.getenv("CENSUS_API_KEY")
cts <- get_acs(geography = "tract", 
               variables = c("B19013_001"), 
               state = "CA", 
               year = 2017)  # set data yr for tracts
cts <- cts[,1:2]
cts <- cts %>% mutate(county_id = str_extract(GEOID, "^.{5}")) %>% 
  group_by(county_id) %>% mutate(n_ct = n()) %>% 
  distinct(county_id, .keep_all = TRUE) %>% 
  select(-c(GEOID, NAME))
# View(cts)

# get median # of non-na filing values grouped by county/year. then calc diff from median #. then calc % diff from median.
med <- na.omit(df) %>% group_by(county_id, county_name) %>% count(county_id, year) %>% 
  rename(non_na_count = n) %>% 
  mutate(med_non_na_count = median(non_na_count)) %>% 
  mutate(diff_from_med = non_na_count - med_non_na_count) %>% 
  mutate(pct_diff_from_med = diff_from_med / med_non_na_count * 100)
# round numeric values. join total # of cts per county.
med <- med %>% mutate_if(is.numeric, ~round(., 1)) %>% left_join(cts, by = "county_id")
# View(med)

# get count of data yrs with non-na filing_rate by county.    
data_yrs <- filter(med, !year == 2018)  # remove 2018 data since they only report it for 1 county (SF)
data_yrs <- data_yrs %>% group_by(county_id, county_name) %>% 
  mutate(num_yrs = n()) %>% 
  distinct(county_id, county_name, .keep_all = TRUE) %>% 
  select(county_id, county_name, num_yrs)
# View(data_yrs)
# summary(data_yrs)

df_join <- df %>% inner_join(data_yrs, by = c("county_id", "county_name"))

screened <- filter(df_join, num_yrs > 2) # suppress data for counties with fewer than 3 years of data
View(screened)
# unique(screened$county_name)   # 39 counties pass num_yrs screening

###############################

df_wide <- screened %>% group_by(fips, county_name) %>%
  mutate(sum_eviction = sum(filings, na.rm = TRUE)) %>%  # total number of evictions over all data yrs available
  mutate(avg_eviction = sum_eviction / num_yrs) %>%  # avg annual number of evictions
  distinct(fips, county_id, county_name, sum_eviction, avg_eviction, .keep_all = FALSE)

df_wide <- filter(df_wide, sum_eviction != 0) # screen out tracts (n = 341) where all filings for all data years = NA, since these should be NA not 0's. there are no 0's in orig. data.

df_wide <- df_wide %>% dplyr::rename(c("sub_id"= "fips", "target_id" = "county_id", "geoname" = "county_name"))

ind_df <- df_wide  # create ind_df for WA functions script
View(ind_df)

############# COUNTY CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2017)                   # define your pop data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold <- 200              # define minimum pop threshold for pop screen

########################################
##### CREATE COUNTY GEOID & NAMES TABLE ######  You will NOT need this chunk if your indicator data table has target geolevel names already

# EXTRA STEP: Must define different vars_list than what's in WA functions, bc basis here is renter households by race, not population by race.
# select race/eth owner household variables: total, black, aian, asian, pacisl, other, twoormor, nh_white, latinx (All except Two+ and Latinx are 1 race alone)
vars_list_custom <- c("B25003_003", "B25003B_003", "B25003C_003", "B25003D_003", "B25003E_003", "B25003F_003", "B25003G_003", "B25003H_003", "B25003I_003")

targetgeo_names <- county_names(vars = vars_list_custom, yr = year, srvy = survey)
targetgeo_names <- select(as.data.frame(targetgeo_names), target_id = GEOID, target_name = NAME) %>%   # get targetgeolevel names
  mutate(target_name = sub(" County, California", "", target_name))           # rename columns
targetgeo_names <- distinct(targetgeo_names, .keep_all = FALSE)                                        # keep only unique rows, 1 per target geo
#####


##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list_custom, yr = year, srvy = survey)  # subgeolevel pop

# transform pop data to wide format 
pop_wide <- lapply(pop, to_wide)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              # rename to generic column name for WA functions


############### CUSTOMIZED VERSION OF TARGETGEO_POP FUNCTION HERE THAT WORKS WITH RENTER HOUSEHOLDS AS POP BASIS #######

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

##### EXTRA STEP: Calc avg annual evictions per 100 renter hh's (rate) by tract bc WA avg should be calc'd using this, not avg # of evictions (raw)
ind_df <- ind_df %>% left_join(pop_df %>% select(sub_id, total_sub_pop), by = "sub_id") %>% 
  mutate(indicator = (avg_eviction / total_sub_pop) * 100)
ind_df <- ind_df %>% ungroup() %>% select(sub_id, indicator) 


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

############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2017)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 200               # define population threshold for screening

### CT-Place Crosswalk ### ---------------------------------------------------------------------
## pull in TIGER Places ## -- Commenting out the xwalk calcs after xwalk was exported to postgres
# places <- places(state = 'CA', year = 2017) %>% select(-c(STATEFP, PLACEFP, PLACENS, LSAD, CLASSFP, PCICBSA, PCINECTA, MTFCC, FUNCSTAT, ALAND, AWATER))
# tracts <- tracts(state = 'CA', year = 2017) %>% mutate(county_geoid = paste0(STATEFP, COUNTYFP)) %>%
#   select(-c(STATEFP, COUNTYFP, TRACTCE, NAME, NAMELSAD, MTFCC, FUNCSTAT, ALAND, AWATER)) 
# 
# ## spatial join ##
# places_3310 <- st_transform(places, 3310) # change projection to 3310
# tracts_3310 <- st_transform(tracts, 3310) # change projection to 3310
# # calculate area of tracts and places
# tracts_3310$area <- st_area(tracts_3310)
# places_3310$pl_area <- st_area(places_3310)
# # rename geoid fields
# tracts_3310 <- tracts_3310%>% 
#   rename("ct_geoid" = "GEOID")
# places_3310 <- places_3310%>% 
#   rename("place_geoid" = "GEOID", "place_name" = "NAME")
# # run intersect
# tracts_places <- st_intersection(tracts_3310, places_3310) 
# # create ct_place combo geoid field
# tracts_places$ct_place_geoid <- paste(tracts_places$place_geoid, tracts_places$ct_geoid, sep = "_")
# # calculate area of intersect
# tracts_places$intersect_area <- st_area(tracts_places)
# # calculate percent of intersect out of total place area
# places_tracts <- tracts_places %>% mutate(prc_pl_area = as.numeric(tracts_places$intersect_area/tracts_places$pl_area))
# # calculate percent of intersect out of total tract area
# tracts_places$prc_area <- as.numeric(tracts_places$intersect_area/tracts_places$area)
# # convert to df
# tracts_places <- as.data.frame(tracts_places)
# places_tracts <- as.data.frame(places_tracts)
# # xwalk N = 14,793
# xwalk <- full_join(places_tracts, select(tracts_places, c(ct_place_geoid, prc_area)), by = 'ct_place_geoid')
# # filter xwalk where intersect between tracts and places is equal or greater than X% of tract area OR place area. xwalk_filter N = 9,675
# threshold <- .25
# xwalk_filter <- xwalk %>% filter(prc_area >= threshold | prc_pl_area >= threshold)
# names(xwalk_filter) <- tolower(names(xwalk_filter)) # make col names lowercase
# xwalk_filter <- select(xwalk_filter, ct_place_geoid, ct_geoid, place_geoid, county_geoid, place_name, namelsad, area, pl_area, intersect_area, prc_area, prc_pl_area)

# export xwalk table
# table_name <- "ct_place_2017"
# table_schema <- "crosswalks"
# table_comment_source <- "Created with W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\IndicatorScripts\\Housing\\hous_eviction_2023.R and based on 2017 ACS TIGER non-CBF shapefiles.
#     CTs with 25% or more of their area within a city or that cover 25% or more of a city''s area are assigned to those cities.
#     As a result, a CT can be assigned to more than one city"

# make character vector for field types in postgresql db
# charvect = rep('numeric', dim(xwalk_filter)[2])

# change data type for first three columns
# charvect[1:6] <- "varchar" # first 6 are character for the geoid and names etc

# add names to the character vector
# names(charvect) <- colnames(xwalk_filter)

# dbWriteTable(con, c(table_schema, table_name), xwalk_filter,
#             overwrite = FALSE, row.names = FALSE,
#             field.types = charvect)

# write comment to table, and the first three fields that won't change.
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

# send table comment to database
#dbSendQuery(conn = con, table_comment)      			

# crosswalk <- xwalk_filter

crosswalk <- st_read(con, query = "SELECT * FROM crosswalks.ct_place_2017") # comment out code above after xwalk is created and pull in postgres table instead.


##### GET SUB GEOLEVEL POP DATA ######
census_api_key(census_key1)       # reload census API key
# EXTRA STEP: Must define different vars_list than what's in WA functions, bc basis here is renter households by race, not population by race.
# select race/eth owner household variables: total, black, aian, asian, pacisl, other, twoormor, nh_white, latinx (All except Two+ and Latinx are 1 race alone)
vars_list_custom <- c("B25003_003", "B25003B_003", "B25003C_003", "B25003D_003", "B25003E_003", "B25003F_003", "B25003G_003", "B25003H_003", "B25003I_003")

pop <- update_detailed_table(vars = vars_list_custom, yr = year, srvy = survey)  # subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = estimate) # n=8,057

pop_wide_city <- as.data.frame(pop_wide) %>% right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names, length(unique(pop_wide_city$GEOID)) = 7,753 bc some cts aren't matched to cities in xwalk
pop_wide_city <- dplyr::rename(pop_wide_city, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions

############### CUSTOMIZED VERSION OF TARGETGEO_POP FUNCTION HERE THAT WORKS WITH RENTER HOUSEHOLDS AS POP BASIS #######

# select pop estimate columns and rename to RC names
b <- select(pop_wide_city, sub_id, target_id, ends_with("003"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- c('target_id', 'total_target_pop', 'black_target_pop', 'aian_target_pop', 'asian_target_pop', 'pacisl_target_pop', 'other_target_pop', 'twoormor_target_pop', 'nh_white_target_pop', 'latino_target_pop')

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop_wide_city, sub_id, target_id, geolevel, ends_with("003"), -NAME) 
names(e) <- c('sub_id', 'target_id', 'geolevel', 'total_sub_pop', 'black_sub_pop', 'aian_sub_pop', 'asian_sub_pop', 'pacisl_sub_pop', 'other_sub_pop', 'twoormor_sub_pop', 'nh_white_sub_pop', 'latino_sub_pop')
pop_df_city <- e %>% left_join(c, by = "target_id")

##### CITY WEIGHTED AVG CALCS ######
pct_df <- pop_pct_multi(pop_df_city)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
city_wa <- city_wa %>% left_join(select(crosswalk, c(place_geoid, place_name)), by = c("target_id" = "place_geoid"))  # add in target geolevel names
city_wa <- city_wa %>% rename(target_name = place_name) %>% mutate(geolevel = 'city')  # change NAME to target_name, drop geometry, add geolevel

city_wa<- city_wa %>% unique()
############ JOIN CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% union(city_wa)
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid) %>% 
  dplyr::relocate(total_rate, .after = twoormor_rate) %>% 
  dplyr::relocate(total_pop, .after = twoormor_pop)# move geoname column


#### EXTRA SCREENING BC NA'S SHOULD NOT BE TREATED AS ZEROES IN THIS DATASET ####
library(naniar)
wa_all <- wa_all %>% 
  replace_with_na_at(.vars = c("total_rate","black_rate", "asian_rate", "aian_rate", "pacisl_rate", "other_rate", "twoormor_rate", "nh_white_rate", "latino_rate"),
                     condition = ~.x == 0.00000000) %>% relocate(total_rate, .after = twoormor_rate) %>% relocate(total_pop, .after = twoormor_pop)
d <- wa_all
# View(d)

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
state_table <- d[d$geoname == 'California', ] %>% select(-c(geolevel))

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
county_table_name <- "arei_hous_eviction_filing_rate_county_2023"
state_table_name <- "arei_hous_eviction_filing_rate_state_2023"
city_table_name <- "arei_hous_eviction_filing_rate_city_2023"
indicator <- "Rate of eviction filings per 100 renter households (weighted average) - annual average from 2014-2017. The data is"
source <- "the (2000-2017) valid proprietary tract-level data downloaded from the Eviction Lab with ACS 2013-2017. https://data-downloads.evictionlab.org/#data-for-analysis/"
rc_schema <- 'v5'

#send tables to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)

