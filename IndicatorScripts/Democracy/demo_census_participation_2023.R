### Census Participation (Weighted Avg) RC v5 ###
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

#set source for RC Functions script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")


##### GET INDICATOR DATA ######
# load indicator data
root <- "W:/Project/RDA Team/Census2020/data/self_response/"
ind_df <- fread(paste0(root, "selfresp_tract_2020-10-28.csv"), header = TRUE, data.table = FALSE,  colClasses = list(character = c("state", "county", "tract", "GEO_ID", "level")))
# clean indicator data: drop and rename cols, get clean ct geoid's, convert response rate to decimal format
ind_df <- ind_df %>% select(c(GEO_ID, CRRALL)) %>% rename(sub_id = GEO_ID, indicator = CRRALL) %>% mutate(sub_id = substr(sub_id, 10,20), indicator = indicator/100)

############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')      # define your target geolevel: place
survey <- "census"                # define which Census survey you want
pop_threshold = 150               # define household threshold for screening

### Load CT-Place Crosswalk & Places ### ---------------------------------------------------------------------
crosswalk <- st_read(con, query = "select * from crosswalks.ct_place_2020")
places <- st_read(con, query = "select * from geographies_ca.cb_2020_06_place_500k") %>% select(c(geoid, name)) %>% st_drop_geometry()

##### GET SUB GEOLEVEL POP DATA ######
census_api_key(census_key1)       # reload census API key
vars_list <- "vars_list_p16"
pop <- update_detailed_table_census(vars = vars_list_p16, yr = year, srvy = survey)  # subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)

pop_wide_city <- as.data.frame(pop_wide) %>% right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide_city <- dplyr::rename(pop_wide_city, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions

# rename columns to appropriate name ------------------------------------
# Census Labels -- HOUSEHOLDS  https://api.census.gov/data/2020/dec/dhc/variables.html
#p16_001n # Total:
#p16i_001n # nh_white: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone
#p16j_001n # nh_black: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone
#p16l_001n # nh_asian: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone
#p16n_001n # nh_other: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone
#p16o_001n # nh_twoormor: !!Total:!!Not Hispanic or Latino:!!Population of two or more races
#p16h_001n # latinx: !!Total:!!Hispanic or Latino
#p16c_001n # latinx-incl. aian alone
#p16e_001n # latinx-incl. nhpi alone
race_names <- function(x) {
  x <- x %>% rename(
    total_pop = P16_001N,
    nh_white_pop = P16I_001N,
    nh_black_pop = P16J_001N,
    nh_asian_pop = P16L_001N,
    nh_other_pop = P16N_001N,
    nh_twoormor_pop = P16O_001N,
    latino_pop = P16H_001N,
    aian_pop = P16C_001N,
    pacisl_pop = P16E_001N) %>%
    select(sub_id, target_id, NAME, geolevel, ends_with("pop"))
    return(x)
    }

pop_wide_city <- race_names(pop_wide_city)

##### CITY WEIGHTED AVG CALCS ######
pop_df <- targetgeo_pop(pop_wide_city) # calc target geolevel pop and number of sub geolevels per target geolevel
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
city_wa <- city_wa %>% left_join(select(places, c(geoid, name)), by = c("target_id" = "geoid"))  # add in target geolevel names
city_wa <- city_wa %>% rename(target_name = name) %>% mutate(geolevel = 'city')  # change NAME to target_name, add geolevel

# screen rates
city_wa$total_rate <- ifelse(city_wa$total_pop < pop_threshold, NA, city_wa$total_rate)
city_wa$nh_asian_rate <- ifelse(city_wa$nh_asian_pop < pop_threshold, NA, city_wa$nh_asian_rate)
city_wa$nh_black_rate <- ifelse(city_wa$nh_black_pop < pop_threshold, NA, city_wa$nh_black_rate)
city_wa$nh_white_rate <- ifelse(city_wa$nh_white_pop < pop_threshold, NA, city_wa$nh_white_rate)
city_wa$latino_rate <- ifelse(city_wa$latino_pop < pop_threshold, NA, city_wa$latino_rate)
city_wa$nh_other_rate <- ifelse(city_wa$nh_other_pop < pop_threshold, NA, city_wa$nh_other_rate)
city_wa$nh_twoormor_rate <- ifelse(city_wa$nh_twoormor_pop < pop_threshold, NA, city_wa$nh_twoormor_rate)
city_wa$aian_rate <- ifelse(city_wa$aian_pop < pop_threshold, NA, city_wa$aian_rate)
city_wa$pacisl_rate <- ifelse(city_wa$pacisl_pop < pop_threshold, NA, city_wa$pacisl_rate)

############# COUNTY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county (state is handled separately)
pop_threshold = 150               # define household threshold for screening

##### CREATE COUNTY GEOID & NAMES TABLE ######  You will NOT need this chunk if your indicator data table has target geolevel names already
survey <- "acs5"                  # define which Census survey you want
vars_list <- "vars_list_acs"
targetgeo_names <- county_names(vars = vars_list, yr = year, srvy = survey)
targetgeo_names <- select(as.data.frame(targetgeo_names), target_id = GEOID, target_name = NAME) %>%   # get targetgeolevel names
  mutate(target_name = sub(" County, California", "", target_name))           # rename columns        
targetgeo_names <- distinct(targetgeo_names, .keep_all = FALSE)                                        # keep only unique rows, 1 per target geo
#####


##### GET SUB GEOLEVEL POP DATA ######
survey <- "census"               # define which Census survey you want
vars_list <- "vars_list_p16"
pop <- update_detailed_table_census(vars = vars_list_p16, yr = year, srvy = survey)  # subgeolevel pop
pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)
pop_wide <- as.data.frame(pop_wide) %>% mutate(target_id = substr(GEOID, 1, 5)) # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID) # rename to generic column names for WA functions

# rename columns to appropriate name ------------------------------------
pop_wide <- race_names(pop_wide)

##### COUNTY WEIGHTED AVG CALCS ######
pop_df <- targetgeo_pop(pop_wide)    # calc target geolevel pop and number of sub geolevels per target geolevel
pct_df <- pop_pct(pop_df)   # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')    # add in target geolevel names and geolevel type

# screen rates
wa$total_rate <- ifelse(wa$total_pop < pop_threshold, NA, wa$total_rate)
wa$nh_asian_rate <- ifelse(wa$nh_asian_pop < pop_threshold, NA, wa$nh_asian_rate)
wa$nh_black_rate <- ifelse(wa$nh_black_pop < pop_threshold, NA, wa$nh_black_rate)
wa$nh_white_rate <- ifelse(wa$nh_white_pop < pop_threshold, NA, wa$nh_white_rate)
wa$latino_rate <- ifelse(wa$latino_pop < pop_threshold, NA, wa$latino_rate)
wa$nh_other_rate <- ifelse(wa$nh_other_pop < pop_threshold, NA, wa$nh_other_rate)
wa$nh_twoormor_rate <- ifelse(wa$nh_twoormor_pop < pop_threshold, NA, wa$nh_twoormor_rate)
wa$aian_rate <- ifelse(wa$aian_pop < pop_threshold, NA, wa$aian_rate)
wa$pacisl_rate <- ifelse(wa$pacisl_pop < pop_threshold, NA, wa$pacisl_rate)

############# STATE CALCS ##################
# get and prep state pop
ca_pop_wide <- state_pop(vars = vars_list, yr = year, srvy = survey)

# calc state wa
ca_pct_df <- ca_pop_pct(ca_pop_wide)
ca_wa <- ca_wt_avg(ca_pct_df) %>% mutate(geolevel = 'state')   # add geolevel type


############ JOIN CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% union(city_wa)
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid)# move geoname column
wa_all <- wa_all %>% mutate(across(3:11, list(~.*100))) %>% select(-c(ends_with("_rate")))  # multiply WA rates by 100 to display as percentages, per previous methodology
colnames(wa_all) <- gsub("_1", "", colnames(wa_all))  # rename new _rate columns
wa_all <- wa_all %>% select(c(geoid, geoname, ends_with("_rate"), everything())) 

d <- wa_all
View(d)

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

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


#split COUNTY into separate table and format id, name columns
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
county_table_name <- "arei_demo_census_participation_county_2023_new"
state_table_name <- "arei_demo_census_participation_state_2023_new"
city_table_name <- "arei_demo_census_participation_city_2023"
rc_schema <- 'v5'

indicator <- paste0("Created on ", Sys.Date(), ". The number of households that filled out their 2020 Census questionnaire per 100 households (weighted average total and raced rates). NOTE: _pop fields represent householders not people")
source <- "U.S. Census Bureau (2020) Census response rates and householders by race"


#send tables to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)
dbDisconnect(con)
