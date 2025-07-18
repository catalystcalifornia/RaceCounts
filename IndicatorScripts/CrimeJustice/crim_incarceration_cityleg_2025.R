## Incarceration 2020 (City-Level) for RC v7

## Set up ----------------------------------------------------------------
packages <- c("DBI", "tidyverse","RPostgreSQL", "tidycensus", "readxl", "sf", "janitor", "stringr", "data.table", "usethis", "rvest", "tigris")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen=999)

# define variables used in several places that must be updated each year
curr_yr <- 2020  # must keep same format
acs_yr <- 2020
rc_yr <- '2020'
rc_schema <- 'v7'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Crime and Justice\\QA_Sheet_Incarceration.docx" # update QA doc filepath

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("rda_shared_data")

#set source for weighted averages script
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")

# Check that variables in vars_list_dp05 used in WA fx haven't changed --------
#### Check that P2 variables and RC race names still match each year and update if needed --------
## Population: All AIAN/PacIsl Latinx incl, NH Alone White/Black/Asian/Other, NH Two+, Latinx of any race

# Load 2020 Decennial Census variables
vars_2020 <- load_variables(year = 2020, dataset = "dhc", cache = TRUE)
View(vars_2020)

sum_file <- "dhc"   # select specific Census file
vars_list_ <- c("total_"="P2_001N","urban_"="P2_005N", "rural"="P2_006N")
                #="P2_006N", ""="P2_008N", ""="P2_010N", ""="P2_011N", ""="P2_002N")#, # total and nh alone white/black/asian/other, nh two+, and latinx all races

race_mapping <- data.frame(
  name = unlist(vars_list_),
  race = names(vars_list_),
  stringsAsFactors = FALSE
)

P2_curr <- load_variables(acs_yr, sum_file, cache = TRUE) %>% 
  filter(name %in% vars_list_) %>%
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop")) %>%
  select(-race)

vars_list_P2 <- vars_list_  # vars used in update_detailed_table_census{}


# CHECK THIS TABLE TO MAKE SURE THE LABEL AND RC_RACES COLUMNS MATCH UP
print(P2_curr) 


# select acs race/eth pop variables: All AIAN/PacIsl, NH Alone White/Black/Asian/Other, NH Two+, Latinx of any race
## the variables MUST BE in this order:
vars_list_dp05 <-      c('total_'= "DP05_0001",
                         'aian_' = "DP05_0071",
                         'pacisl_' = "DP05_0073",
                         'latino_' = "DP05_0076",
                         'nh_white_' = "DP05_0082",
                         'nh_black_' = "DP05_0083",
                         'nh_asian_' = "DP05_0085",
                         'nh_other_' = "DP05_0087",
                         'nh_twoormor_' = "DP05_0088")

race_mapping <- data.frame(
  name = unlist(vars_list_dp05),
  race = names(vars_list_dp05),
  stringsAsFactors = FALSE
)

dp05_curr <- load_variables(curr_yr, "acs5/profile", cache = TRUE) %>% 
  select(-c(concept)) %>% 
  filter(name %in% vars_list_dp05) %>%
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop"), 
         name = tolower(name),            # get all DP05 vars
         label <- gsub("Estimate!!|HISPANIC OR LATINO AND RACE!!", "", label))              

# CHECK THIS TABLE TO MAKE SURE THE LABEL AND RC_RACES COLUMNS MATCH UP
print(dp05_curr) 


# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table

### GET INDICATOR DATA ###
#https://stackoverflow.com/questions/68873466/read-html-cannot-read-a-webpage-despite-specifying-different-encodings
#url <- read_html("https://www.prisonpolicy.org/origin/ca/2020/assembly.html") #CHECK THE TRACT TO SLDL TO THE SLDL COUNTS HERE
ind_df <- dbGetQuery(conn, paste0("SELECT fips_code_2020 AS sub_id, imprisonment_rate_per_100_000 AS indicator FROM crime_and_justice.prison_policy_incarceration_tract_", curr_yr)) %>% as.data.frame()

##############  ASSEMBLY DISTRICTS  #############

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldl')      # define your target geolevel: place
survey <- "census"                # define which Census survey you want
pop_threshold = 250               # define population threshold for screening
census_api_key(census_key1)       # reload census API key

##### GET SUB GEOLEVEL POP DATA ######
vars_list <- "vars_list_p2"
pop <- update_detailed_table_census(vars = vars_list_p2, yr = acs_yr, srvy = survey, subgeo = subgeo)  # subgeolevel total, nh alone pop
vars_list <- "vars_list_dp"
pop2 <- update_detailed_table_census(vars = vars_list_dp, yr = year, srvy = survey, subgeo = subgeo)  # all aian, all nhpi subgeolevel pop

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

###############  SENATE DISTRICTS ##############
# url <- read_html("https://www.prisonpolicy.org/origin/ca/2020/senate.html") #CHECK THE TRACT TO SLDU TO THE COUNTS HERE
url <- read_html("https://www.prisonpolicy.org/origin/ca/2020/tract.html")

# ############### CITY #######################
# # pull in data ------------------------------------------------------------
# #https://stackoverflow.com/questions/68873466/read-html-cannot-read-a-webpage-despite-specifying-different-encodings
# url <- read_html("https://www.prisonpolicy.org/origin/ca/2020/tract.html")
# 
# datatable <- url %>% html_table(fill = TRUE)
# 
# dt <- datatable [[1]] # keep first
# 
# df  <- dt %>% clean_names() %>% as.data.frame() # clean up names
# 
# df$fips_code_2020 <- paste0("0", df$fips_code_2020) # add leading zeroes here to match the crosswalk fips codes
# df$census_population_2020 = as.integer(gsub("\\,", "", df$census_population_2020)) # change text fields to integer, remove commas
# df$total_population_2020 = as.integer(gsub("\\,", "", df$total_population_2020))
# df$imprisonment_rate_per_100_000 = as.integer(gsub("\\,", "", df$imprisonment_rate_per_100_000))
# 
# # export incarceration  to rda shared table ------------------------------------------------------------
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "crime_and_justice"
# table_name <- "prison_policy_incarceration_tract_2020"
# table_comment_source <- "Number of people in prison in 2020 from each California Census tract"
# table_source <- "Incarceration data downloaded 7/11/2023
# from https://www.prisonpolicy.org/origin/ca/2020/tract.html"
# 
# #dbWriteTable(con2, c(table_schema, table_name), df, overwrite = FALSE, row.names = FALSE)
# 
# # write comment to table, and the first three fields that won't change.
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# 
# # send table comment to database
# #dbSendQuery(conn = con2, table_comment)  
# 
# #rename columns for weighted average functions to work ------------------------------------------------------------
# ind_df <- df %>% rename(sub_id = fips_code_2020, indicator = imprisonment_rate_per_100_000) %>% as.data.frame() 
# 
# # pull in ct-city crosswalk ------------------------------------------------------------
# crosswalk <- dbGetQuery(con2, "SELECT * FROM crosswalks.ct_place_2020")
# 
# # pull in place shapes ------------------------------------------------------------
# places <- places(state = 'CA', year = 2020, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
# 
# # Weighted Averages Functions  ------------------------------------------------------------
# 
# ##### GET SUB GEOLEVEL POP DATA ######
# 
# # set values for weighted average functions - You may need to update these
# year <- c(2020)                   # define your data vintage
# subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
# targetgeolevel <- c('place')      # define your target geolevel: place
# survey <- "census"                # define which Census survey you want
# pop_threshold = 250               # define population threshold for screening
# census_api_key(census_key1)       # reload census API key
# 
# ##### GET SUB GEOLEVEL POP DATA ######
# vars_list <- "vars_list_p2"
# pop <- update_detailed_table_census(vars = vars_list_p2, yr = year, srvy = survey)  # subgeolevel total, nh alone pop
# vars_list <- "vars_list_dp"
# pop2 <- update_detailed_table_census(vars = vars_list_dp, yr = year, srvy = survey)  # all aian, all nhpi subgeolevel pop
# 
# pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)
# pop2_wide <- pop2 %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value) %>% select(GEOID, "DP1_0088C", "DP1_0090C")
# pop_wide <- pop_wide %>% left_join(pop2_wide, by = 'GEOID')
# pop_wide <- as.data.frame(pop_wide) %>% right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
# pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions
# 
# 
# # rename columns to appropriate name ------------------------------------
# 
# # Census Labels
# #p2_001n # Total:
# #p2_005n # nh_white: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone
# #p2_006n # nh_black: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone
# #p1_005n # aian: !!Total:!!Population of one race:!!American Indian and Alaska Native alone
# #p2_008n # nh_asian: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone
# #p1_007n # pacisl: !!Total:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone
# #p2_010n # nh_other: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone
# #p2_011n # nh_twoormor: !!Total:!!Not Hispanic or Latino:!!Population of two or more races
# #p2_002n # latinx: !!Total:!!Hispanic or Latino
# 
# #dp1_0088c # all aian
# #dp1_0090c # all nhpi
# 
# pop_wide <- pop_wide %>% rename(
#   total_pop = P2_001N,
#   nh_white_pop = P2_005N,
#   nh_black_pop = P2_006N,
#   nh_asian_pop = P2_008N,
#   nh_other_pop = P2_010N,
#   nh_twoormor_pop = P2_011N,
#   latino_pop = P2_002N,
#   aian_pop = DP1_0088C,
#   pacisl_pop = DP1_0090C) %>% 
#   select(sub_id, target_id, NAME, geolevel, ends_with("pop"))
# 
# #  CITY WEIGHTED AVG CALCS ------------------------------------------------
# 
# # calc target geolevel pop and number of sub geolevels per target geolevel
# pop_df <- targetgeo_pop(pop_wide)
# pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
# city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
# 
# city_wa <- city_wa %>% left_join(select(places, c(GEOID, NAME)), by = c("target_id" = "GEOID")) %>%
#   rename(geoname = NAME, geoid = target_id) %>% select(-c(geometry)) %>% mutate(geolevel = 'city') %>% dplyr::relocate(geoname, .after = geoid)   # rename columns for RC functions
# 
# # aggregate total raw -----------------------------------------------------
# raw_df <- df %>% rename(ct_geoid =  fips_code_2020, total_raw = number_of_people_in_state_prison_from_each_census_tract_2020) %>% select(ct_geoid, total_raw) %>% right_join(select(crosswalk, c(ct_geoid, place_geoid))) %>% group_by(place_geoid) %>%  summarize(total_raw = sum(total_raw)) 
# 
# 
# ## merge raw with city weighted averages
# city_wa <- city_wa %>% left_join(raw_df, by = c("geoid" = "place_geoid")) %>% dplyr::relocate(total_rate, .after = geoname) %>% select(-c(total_raw))  # remove total_raw bc should not appear on website
# 
# # final df
# d <- city_wa
#
############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("./Functions/RC_Functions.R")

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
View(city_table)

#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to city_id, leg_id
city_table <- rename(city_table, city_id = geoid, city_name = geoname)
leg_table <- rename(leg_table, leg_id = geoid, leg_name = geoname)

### update info for postgres tables - automatically updates based on variables at top of script ##
city_table_name <- paste0("arei_crim_incarceration_city_", rc_yr)
leg_table_name <- paste0("arei_crim_incarceration_leg_", rc_yr)


indicator <- paste0("Created on ", Sys.Date(), ". Number of people in prison in 2020 - weighted average by race")
source <- "NOTE: This is a different source than the county/state incarceration indicator. Prison Policy Org https://www.prisonpolicy.org/origin/ca/2020/tract.html."

#send to postgres
#city_to_postgres(city_table)

#disconnect
dbDisconnect(conn)




