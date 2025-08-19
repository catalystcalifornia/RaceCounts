## Incarceration 2020 (City & Leg-Level) for RC v7

## Set up ----------------------------------------------------------------
packages <- c("DBI", "tidyverse", "RPostgres", "tidycensus", "readxl", "tigris", "sf", "janitor", "stringr", "data.table", "usethis", "rvest", "tigris")  

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

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("rda_shared_data")

#set source for weighted averages fx
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")
census_api_key(census_key1)    # reload census API key

# define variables used in several places that must be updated each year
curr_yr <- 2020  # must keep same format
acs_yr <- 2020
rc_yr <- '2025'
rc_schema <- 'v7'
pop_threshold = 250               # define population threshold for screening
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Crime and Justice\\QA_Sheet_Incarceration.docx" # update QA doc filepath

# may need to update each year: variables for state assm and senate calcs ------
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table

### Check that P9 variables and RC race names still match each year and update if needed --------
# Population: All AIAN/PacIsl Latinx incl, NH Alone White/Black/Asian/Other, NH Two+, Latinx of any race

# Load Decennial Census variables
vars_ <- load_variables(year = acs_yr, dataset = "dhc", cache = TRUE)
View(vars_)

sum_file <- "dhc"   # select specific Census file
vars_list_ <- c('total_' = "P9_001N",       # Total:
                'nh_white_' = "P9_005N",    # nh_white: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone
                'nh_black_' = "P9_006N",    # nh_black: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone
                'aian_' = "P6_004N",        # aian: !!Total:!!Population of one race:!!American Indian and Alaska Native alone or in combo
                'nh_asian_' = "P9_008N",    # nh_asian: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone
                'pacisl_' = "P6_006N",      # pacisl: !!Total:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone or in combo
                'nh_other_' = "P9_010N",    # nh_other: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone
                'nh_twoormor_' = "P9_011N", # nh_twoormor: !!Total:!!Not Hispanic or Latino:!!Population of two or more races
                'latino_' = "P9_002N")      # latinx: !!Total:!!Hispanic or Latino

race_mapping <- data.frame(
  name = unlist(vars_list_),
  race = names(vars_list_),
  stringsAsFactors = FALSE
)

p9_curr <- load_variables(acs_yr, sum_file, cache = TRUE) %>%
  filter(name %in% vars_list_) %>%
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop")) %>%
  select(-race)

vars_list_p9 <- p9_curr$name  # vars used in update_detailed_table_census{}

# CHECK THIS TABLE TO MAKE SURE THE LABEL AND RC_RACES COLUMNS MATCH UP
View(p9_curr)



### GET INDICATOR DATA ###
#https://stackoverflow.com/questions/68873466/read-html-cannot-read-a-webpage-despite-specifying-different-encodings
ind_df <- dbGetQuery(conn, paste0("SELECT fips_code_2020 AS sub_id, imprisonment_rate_per_100_000 AS indicator FROM crime_and_justice.prison_policy_incarceration_tract_", curr_yr))# %>% as.data.frame()

##############  ASSEMBLY DISTRICTS  #############
#url <- read_html("https://www.prisonpolicy.org/origin/ca/2020/assembly.html") # CHECK THE TOTAL RATES HERE AGAINST OUR TOTAL RATES
###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
year <- curr_yr                # define your data vintage
subgeo <- 'tract'              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- 'sldl'       # define your target geolevel: Assm
survey <- "census"             # define which Census survey you want

### Load CT-Assm Crosswalk ### 
crosswalk <- dbGetQuery(conn, paste0("SELECT geo_id, ", assm_geoid, ", afact, afact2 FROM crosswalks.", assm_xwalk)) %>%
  filter(afact >= .25 | afact2 >= .25)  # screen xwalk based on pct of ct pop in dist OR pct of dist pop in ct

##### GET SUB GEOLEVEL POP DATA ######
vars_list <- "vars_list_p9"
pop <- update_detailed_table_census(vars = vars_list_p9, yr = curr_yr, srvy = survey, subgeo = subgeo, sumfile = sum_file)  # subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)

# rename P9/P6 vars to rc_races
pop_wide <- pop_wide %>%
  rename_at(vars(starts_with("P")), 
            ~ str_replace_all(., setNames(p9_curr$rc_races, p9_curr$name)))

pop_wide <- pop_wide %>% right_join(select(crosswalk, c(geo_id, assm_geoid, afact)), by = c("GEOID" = "geo_id"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = {{assm_geoid}})            # rename to generic column names for WA functions

##### ASSEMBLY WEIGHTED AVG CALCS ###
# Get Targetgeo Pop
pop_wide_ <- targetgeo_pop(pop_wide) 

# Calc % of each targetgeo pop that each leg dist pop
pop_wide <- pop_wide %>% left_join(pop_wide_, by = c("target_id","sub_id"))       #join subpop data to targetpop data to get pct_leg
pct_df <- pop_pct_multi(pop_wide)        # NOTE: use this function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel

assm_wa <- wt_avg(pct_df, ind_df)              # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% mutate(geolevel = 'sldl')                  # add geolevel

## Add census geonames
assm_name <- get_acs(geography = "State Legislative District (Lower Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = acs_yr)

assm_name <- assm_name[,1:2]
assm_name$NAME <- str_remove(assm_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
assm_name$NAME <- gsub(", California", "", assm_name$NAME)
names(assm_name) <- c("target_id", "target_name")
# View(assm_name)

#add geonames to WA
assm_wa <- merge(x=assm_name,y=assm_wa,by="target_id", all=T)
#View(assm_wa)

###############  SENATE DISTRICTS ##############
# url <- read_html("https://www.prisonpolicy.org/origin/ca/2020/senate.html") # CHECK THE TOTAL RATES HERE AGAINST OUR TOTAL RATES
###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
year <- curr_yr                   # define your data vintage
subgeo <- 'tract'                 # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- 'sldu'          # define your target geolevel: senate
survey <- "census"                # define which Census survey you want

### Load CT-sen Crosswalk ### 
crosswalk <- dbGetQuery(conn, paste0("SELECT geo_id, ", sen_geoid, ", afact, afact2 FROM crosswalks.", sen_xwalk)) %>%
  filter(afact >= .25 | afact2 >= .25)  # screen xwalk based on pct of ct pop in dist OR pct of dist pop in ct

##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table_census(vars = vars_list_p9, yr = curr_yr, srvy = survey, subgeo = subgeo, sumfile = sum_file)  # subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)

# rename P9/P6 vars to rc_races
pop_wide <- pop_wide %>%
  rename_at(vars(starts_with("P")), 
            ~ str_replace_all(., setNames(p9_curr$rc_races, p9_curr$name)))

pop_wide <- pop_wide %>% right_join(select(crosswalk, c(geo_id, sen_geoid, afact)), by = c("GEOID" = "geo_id"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = {{sen_geoid}})            # rename to generic column names for WA functions

##### SENATE WEIGHTED AVG CALCS ###
# Get Targetgeo Pop
pop_wide_ <- targetgeo_pop(pop_wide) 

# Calc % of each targetgeo pop that each leg dist pop
pop_wide <- pop_wide %>% left_join(pop_wide_, by = c("target_id","sub_id"))       #join subpop data to targetpop data to get pct_leg
pct_df <- pop_pct_multi(pop_wide)        # NOTE: use this function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel

sen_wa <- wt_avg(pct_df, ind_df)              # calc weighted average and apply reliability screens
sen_wa <- sen_wa %>% mutate(geolevel = 'sldu')                  # add geolevel

## Add census geonames
sen_name <- get_acs(geography = "State Legislative District (upper Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = acs_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldu/sldu
sen_name$NAME <- gsub(", California", "", sen_name$NAME)
names(sen_name) <- c("target_id", "target_name")
# View(sen_name)

#add geonames to WA
sen_wa <- merge(x=sen_name,y=sen_wa,by="target_id", all=T)
#View(sen_wa)

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
# crosswalk <- dbGetQuery(con2, paste0("SELECT * FROM crosswalks.ct_place_", curr_yr))
# 
# # pull in place shapes ------------------------------------------------------------
# places <- places(state = 'CA', year = curr_yr, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
# 
# # Weighted Averages Functions  ------------------------------------------------------------
# 
# ##### GET SUB GEOLEVEL POP DATA ######
# 
# # set values for weighted average functions - You may need to update these
# year <- curr_yr              # define your data vintage
# subgeo <- 'tract'            # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
# targetgeolevel <- 'place'    # define your target geolevel: place
# survey <- "census"           # define which Census survey you want
# 
# ##### GET SUB GEOLEVEL POP DATA ######
# vars_list <- "vars_list_p9"
# pop <- update_detailed_table_census(vars = vars_list_p9, yr = year, srvy = survey)  # subgeolevel total, nh alone pop
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
# #P9_001n # Total:
# #P9_005n # nh_white: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone
# #P9_006n # nh_black: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone
# #P8_005n # aian: !!Total:!!Population of one race:!!American Indian and Alaska Native alone
# #P9_008n # nh_asian: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone
# #P8_007n # pacisl: !!Total:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone
# #P9_010n # nh_other: !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone
# #P9_011n # nh_twoormor: !!Total:!!Not Hispanic or Latino:!!Population of two or more races
# #P9_002n # latinx: !!Total:!!Hispanic or Latino
# 
# #dp1_0088c # all aian
# #dp1_0090c # all nhpi
# 
# pop_wide <- pop_wide %>% rename(
#   total_pop = P9_001N,
#   nh_white_pop = P9_005N,
#   nh_black_pop = P9_006N,
#   nh_asian_pop = P9_008N,
#   nh_other_pop = P9_010N,
#   nh_twoormor_pop = P9_011N,
#   latino_pop = P9_002N,
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

###### Join assembly, senate, and city WA tables  ##################
wa_all <- union(assm_wa, sen_wa)
# wa_all <- union(wa_all, city_wa)  # add city data when there is a data update
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid)        # move geoname column
names(wa_all) <- gsub("__", "_", names(wa_all)) # the weighted average function has 2 underscores but RC colnames should only have 1

d <- wa_all

# View(d)

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

# #split CITY into separate table and format id, name columns
# city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))
# 
# #calculate DISTRICT z-scores
# city_table <- calc_z(city_table)
# city_table <- calc_ranks(city_table)
# View(city_table)

#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
#View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
#View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to city_id, leg_id
# city_table <- rename(city_table, city_id = geoid, city_name = geoname)
leg_table <- rename(leg_table, leg_id = geoid, leg_name = geoname)

### update info for postgres tables - automatically updates based on variables at top of script ##
# city_table_name <- paste0("arei_crim_incarceration_city_", rc_yr)
leg_table_name <- paste0("arei_crim_incarceration_leg_", rc_yr)

indicator <- paste0("Number of people in prison in ", curr_yr, " - weighted average by race.NOTE: This is a different source than the county/state incarceration indicator.")
source <- paste0("Prison Policy Org https://www.prisonpolicy.org/origin/ca/2020/tract.html. Leg pop data from Census ", acs_yr, " Table P9. City pop data from Census ", acs_yr, " Tables P2 and DP1. QA doc: ", qa_filepath)

#send to postgres
#city_to_postgres(city_table)
#leg_to_postgres(leg_table)
#disconnect
dbDisconnect(conn)