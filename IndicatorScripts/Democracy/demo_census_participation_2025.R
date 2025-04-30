### Census Participation (Weighted Avg) RC v7 ###

##install packages if not already installed ------------------------------
packages <- c("dplyr","data.table","sf","tigris","readr","tidyr","DBI","RPostgres","tidycensus", "rvest", "tidyverse", "stringr")
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

###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

#set source for Weighted Average Functions
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")

# update variables used throughout each year
curr_yr <- 2020 # yr of Census data release
rc_yr <- '2025'
rc_schema <- 'v7'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Democracy\\QA_Census_Participation.docx"

# Check that variables in vars_list_p16 used in WA fx haven't changed --------
## Householders by Race: AIAN Alone incl Latinx, PacIsl Alone incl Latinx, NH Alone White/Black/Asian/Other, NH Two+, Latinx of any race
## Could not identify any Census table for householder race that had latinx incl. aian/nhpi alone or in combo. # https://api.census.gov/data/2020/dec/dhc/variables.html
sum_file <- "dhc"   # select specific Census file
vars_list_ <- list("total_" = "P16_001N",
                    "aian_" = "P16C_001N",
                    "pacisl_" = "P16E_001N",
                    "latino_" = "P16H_001N", 
                    "nh_white_" = "P16I_001N", 
                    "nh_black_" = "P16J_001N", 
                    "nh_asian_"= "P16L_001N", 
                    "nh_other_" = "P16N_001N", 
                    "nh_twoormor_" = "P16O_001N")

race_mapping <- data.frame(
  name = unlist(vars_list_),
  race = names(vars_list_),
  stringsAsFactors = FALSE
)

p16_curr <- load_variables(curr_yr, sum_file, cache = TRUE) %>% 
  filter(name %in% vars_list_) %>%
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop")) %>%
  select(-race)

vars_list_p16 <- p16_curr$name  # vars used in update_detailed_table_census{}
  
# CHECK THIS TABLE TO MAKE SURE THE LABEL AND RC_RACES COLUMNS MATCH UP
print(p16_curr) 


# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table


##### GET INDICATOR DATA ######
# load indicator data
root <- "W:/Project/RDA Team/Census2020/data/self_response/"
ind_df <- fread(paste0(root, "selfresp_tract_2020-10-28.csv"), header = TRUE, data.table = FALSE,  colClasses = list(character = c("state", "county", "tract", "GEO_ID", "level")))
# clean indicator data: drop and rename cols, get clean ct geoid's, convert response rate to decimal format
ind_df <- ind_df %>% select(c(GEO_ID, CRRALL)) %>% rename(sub_id = GEO_ID, indicator = CRRALL) %>% mutate(sub_id = substr(sub_id, 10,20), indicator = indicator/100)


############# ASSEMBLY DISTRICTS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldl')       # define your target geolevel: state assembly
survey <- "census"                # define which Census survey you want
pop_threshold = 150               # define household threshold for screening

### Load CT-Assm Crosswalk ### 
crosswalk <- dbGetQuery(con, paste0("SELECT geo_id AS ct_geoid, ", assm_geoid, " AS assm_geoid FROM crosswalks.", assm_xwalk))

##### GET SUB GEOLEVEL POP DATA ###
census_api_key(census_key1)       # reload census API key
pop <- update_detailed_table_census(vars = vars_list_p16, yr = curr_yr, srvy = survey, subgeo = subgeo, sumfile = sum_file)  # subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)

# rename P16 vars to rc_races
pop_wide <- pop_wide %>%
  rename_at(vars(starts_with("P16")), 
            ~ str_replace_all(., setNames(p16_curr$rc_races, p16_curr$name)))

pop_wide_assm <- pop_wide %>% right_join(select(crosswalk, c(ct_geoid, assm_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide_assm <- dplyr::rename(pop_wide_assm, sub_id = GEOID, target_id = assm_geoid)                               # rename to generic column names for WA functions

##### ASSEMBLY WEIGHTED AVG CALCS ###
pop_df <- targetgeo_pop(pop_wide_assm) # calc target geolevel pop and number of sub geolevels per target geolevel
pct_df <- pop_pct_multi(pop_df)        # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
assm_wa <- wt_avg(pct_df)              # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% mutate(geolevel = 'sldl')                  # add geolevel

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
assm_name <- get_decennial(geography = "State Legislative District (Lower Chamber)", 
                     variables = c("P16_001N"), 
                     sumfile = "dhc",
                     state = "CA", 
                     year = curr_yr)

assm_name <- assm_name[,1:2]
assm_name$NAME <- str_remove(assm_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
assm_name$NAME <- gsub(", California", "", assm_name$NAME)
names(assm_name) <- c("target_id", "target_name")
# View(assm_name)

# add geonames to WA
assm_wa <- merge(x=assm_name,y=assm_wa,by="target_id", all=T)
# View(assm_wa)


############# SENATE DISTRICT ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldu')       # define your target geolevel: senate
survey <- "census"                # define which Census survey you want
pop_threshold = 150               # define household threshold for screening

### Load CT-Sen Crosswalk ### 
crosswalk <- dbGetQuery(con, paste0("SELECT geo_id AS ct_geoid, ", sen_geoid, " AS sen_geoid FROM crosswalks.", sen_xwalk))

##### GET SUB GEOLEVEL POP DATA ###
census_api_key(census_key1)       # reload census API key
pop <- update_detailed_table_census(vars = vars_list_p16, yr = curr_yr, srvy = survey, subgeo = subgeo, sumfile = sum_file)  # subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)

# rename P16 vars to rc_races
pop_wide <- pop_wide %>%
  rename_at(vars(starts_with("P16")), 
            ~ str_replace_all(., setNames(p16_curr$rc_races, p16_curr$name)))

pop_wide_sen <- pop_wide %>% right_join(select(crosswalk, c(ct_geoid, sen_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide_sen <- dplyr::rename(pop_wide_sen, sub_id = GEOID, target_id = sen_geoid)                                # rename to generic column names for WA functions

##### SENATE WEIGHTED AVG CALCS ###
pop_df <- targetgeo_pop(pop_wide_sen) # calc target geolevel pop and number of sub geolevels per target geolevel
pct_df <- pop_pct_multi(pop_df)       # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
sen_wa <- wt_avg(pct_df)              # calc weighted average and apply reliability screens
sen_wa <- sen_wa %>% mutate(geolevel = 'sldu')                  # add geolevel

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_decennial(geography = "State Legislative District (Upper Chamber)", 
                          variables = c("P16_001N"), 
                          sumfile = "dhc",
                          state = "CA", 
                          year = curr_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldu/sldu
sen_name$NAME <- gsub(", California", "", sen_name$NAME)
names(sen_name) <- c("target_id", "target_name")
# View(sen_name)

# add geonames to WA
sen_wa <- merge(x=sen_name,y=sen_wa,by="target_id", all=T)
# View(sen_wa)


############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')      # define your target geolevel: place
survey <- "census"                # define which Census survey you want
pop_threshold = 150               # define household threshold for screening

### Load CT-Place Crosswalk & Places ### 
crosswalk <- st_read(con, query = "select * from crosswalks.ct_place_2020")
places <- dbGetQuery(con, paste0("select geoid, name from geographies_ca.cb_", curr_yr, "_06_place_500k"))

##### GET SUB GEOLEVEL POP DATA ###
census_api_key(census_key1)       # reload census API key
pop <- update_detailed_table_census(vars = vars_list_p16, yr = curr_yr, srvy = survey, subgeo = subgeo, sumfile = sum_file)  # subgeolevel pop

pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)

# rename P16 vars to rc_races
pop_wide <- pop_wide %>%
  rename_at(vars(starts_with("P16")), 
            ~ str_replace_all(., setNames(p16_curr$rc_races, p16_curr$name)))

pop_wide_city <- pop_wide %>% right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide_city <- dplyr::rename(pop_wide_city, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions

##### CITY WEIGHTED AVG CALCS ###
pop_df <- targetgeo_pop(pop_wide_city) # calc target geolevel pop and number of sub geolevels per target geolevel
pct_df <- pop_pct_multi(pop_df)        # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)              # calc weighted average and apply reliability screens
city_wa <- city_wa %>% left_join(select(places, c(geoid, name)), by = c("target_id" = "geoid"))  # add in target geolevel names
city_wa <- city_wa %>% rename(target_name = name) %>% mutate(geolevel = 'city')                  # change NAME to target_name, add geolevel


############# COUNTY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county (state is handled separately)
survey <- "census"                # define which Census survey you want
pop_threshold = 150               # define household threshold for screening

##### CREATE COUNTY GEOID & NAMES TABLE ######  You will NOT need this chunk if your indicator data table has target geolevel names already
counties <- dbGetQuery(con, paste0("select county_geoid AS target_id, name AS target_name from geographies_ca.cb_", curr_yr, "_06_county_500k"))

##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table_census(vars = vars_list_p16, yr = curr_yr, srvy = survey, subgeo = subgeo, sumfile = sum_file)  # subgeolevel pop
pop_wide <- pop %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = value)
pop_wide <- pop_wide %>% mutate(target_id = substr(GEOID, 1, 5))    # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                 # rename to generic column names for WA functions

# rename P16 vars to rc_races
pop_wide <- pop_wide %>%
  rename_at(vars(starts_with("P16")), 
            ~ str_replace_all(., setNames(p16_curr$rc_races, p16_curr$name)))

##### COUNTY WEIGHTED AVG CALCS ######
pop_df <- targetgeo_pop(pop_wide)    # calc target geolevel pop and number of sub geolevels per target geolevel
pct_df <- pop_pct(pop_df)            # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df)                 # calc weighted average and apply reliability screens
wa <- wa %>% left_join(counties, by = "target_id") %>% mutate(geolevel = 'county')    # add in target geolevel names and geolevel type


############# STATE CALCS ##################
# get and prep state pop
ca_pop_wide <- state_pop(vars = vars_list_p16, yr = curr_yr, srvy = survey)

# calc state wa
ca_pct_df <- ca_pop_pct(ca_pop_wide)
ca_wa <- ca_wt_avg(ca_pct_df) %>% mutate(geolevel = 'state')   # add geolevel type


############ JOIN CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% union(city_wa) %>% union(assm_wa) %>% union(sen_wa)
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
source("./Functions/RC_Functions.R")

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
View(state_table)


#split COUNTY into separate table and format id, name columns
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
View(county_table)


#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))

#calculate CITY z-scores
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

#rename geoid to state_id, county_id, city_id, leg_id
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
city_table <- rename(city_table, city_id = geoid, city_name = geoname)
leg_table <- rename(leg_table, leg_id = geoid, leg_name = geoname)


###update info for postgres tables###
county_table_name <- paste0("arei_demo_census_participation_county_", rc_yr)
state_table_name <- paste0("arei_demo_census_participation_state_", rc_yr)
city_table_name <- paste0("arei_demo_census_participation_city_", rc_yr)
leg_table_name <- paste0("arei_demo_census_participation_leg_", rc_yr)

indicator <- paste0("The number of households that filled out their ", curr_yr, " Census questionnaire per 100 households (weighted average total and raced rates)")
source <- paste0("U.S. Census Bureau (", curr_yr, ") Census response rates and householders by race (P16). NOTE: _pop fields represent householders not people. QA doc: ", qa_filepath)


#send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)

dbDisconnect(con)
