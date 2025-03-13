### Drinking Water Contamination (Weighted Avg) RC v6 ### 

##install packages if not already installed ------------------------------
list.of.packages <- c("dplyr","data.table","sf","tigris","readr","tidyr","DBI","RPostgreSQL","tidycensus","rvest","tidyverse","stringr","usethis")
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
options(scipen=999)

###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

#set source for Weighted-Average Functions script
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/SWANA_Ancestry_List.R")

# update variables used throughout each year
curr_yr <- 2021 # yr of CES data release
ces_v <- '4.0'  # CES version
acs_yr <- 2020
rc_yr <- '2024'
rc_schema <- 'v6'


##### GET INDICATOR DATA ######
# load indicator data
ind_df <- st_read(con, query = paste0("select ct_geoid AS geoid, drinkwat from built_environment.oehha_ces4_tract_", curr_yr))
ind_df <- dplyr::rename(ind_df, sub_id = geoid, indicator = drinkwat)       # rename columns for functions
ind_df <- filter(ind_df, indicator >= 0) # screen out NA values of -999


############# ASSEMBLY DISTRICTS ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldl')      # define your target geolevel: county (state is handled separately)
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 250              # define population threshold for screening
assm_geoid <- 'sldl24'			 # define column with Assm geoid

### CT-Assm Crosswalk ### ---------------------------------------------------------------------
#set source for CT-Assm Crosswalk fx
xwalk_filter <- dbGetQuery(con, paste0("SELECT geo_id AS ct_geoid, ", assm_geoid, " AS assm_geoid FROM crosswalks.tract_2020_state_assembly_2024"))
assm <- dbGetQuery(con, paste0("SELECT ", assm_geoid, " AS assm_geoid FROM crosswalks.tract_2020_state_assembly_2024")) %>% unique()

##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list_acs, yr = acs_yr, srvy = survey)  # subgeolevel pop
pop <- as.data.frame(pop)

# get SWANA pop
vars_list_acs_swana <- get_swana_var(acs_yr, survey)
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = acs_yr, srvy = survey) %>% as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>% rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- pop_ %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = c(e, m), names_glue = "{variable}{.value}")


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(xwalk_filter, c(ct_geoid, assm_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = assm_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### ASSM WEIGHTED AVG CALCS ######
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
assm_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% mutate(geolevel = 'lower')  # change drop geometry, add geolevel


############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 250              # define population threshold for screening

### CT-Place Crosswalk ### ---------------------------------------------------------------------
#set source for CT-Place Crosswalk fx
source("W:/Project/RACE COUNTS/Functions/RC_CT_Place_Xwalk.R")
xwalk_filter <- make_ct_place_xwalk(acs_yr) %>% select(ct_geoid, place_geoid, place_name)
places <- xwalk_filter %>% select(c(place_geoid, place_name)) %>% unique()

##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list_acs, yr = acs_yr, srvy = survey)  # subgeolevel pop
pop <- as.data.frame(pop)

# get SWANA pop
vars_list_acs_swana <- get_swana_var(acs_yr, survey)
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = acs_yr, srvy = survey) %>% as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>% rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- pop_ %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = c(e, m), names_glue = "{variable}{.value}")


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(xwalk_filter, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### CITY WEIGHTED AVG CALCS ######
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
city_wa <- city_wa %>% left_join(select(places, c(place_geoid, place_name)), by = c("target_id" = "place_geoid")) # add in target geolevel names
city_wa <- city_wa %>% rename(target_name = place_name) %>% mutate(geolevel = 'city')  # change NAME to target_name, drop geometry, add geolevel


############# COUNTY ##################

###### DEFINE VALUES FOR FUNCTIONS ######

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: place
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 250               # define population threshold for screening

##### CREATE COUNTY GEOID & NAMES TABLE ######  You will NOT need this chunk if your indicator data table has target geolevel names already
targetgeo_names <- county_names(vars = vars_list_acs, yr = acs_yr, srvy = survey)

#####


##### GET SUB GEOLEVEL POP DATA ######
pop <- update_detailed_table(vars = vars_list_acs, yr = acs_yr, srvy = survey)  # subgeolevel pop

pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = acs_yr, srvy = survey) %>% as.data.frame() %>%
  group_by(GEOID, NAME, geolevel)%>%
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana") # subgeolevel pop

# combine DP05 groups with swana estimates 
pop <- rbind(pop, pop_swana)

# transform pop data to wide format 
pop_wide <- lapply(pop, to_wide)

# convert to df
pop_wide <- pop_wide$GEOID %>% as.data.frame()

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id

pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              # rename to generic column name for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide)


##### COUNTY WEIGHTED AVG CALCS ######
pct_df <- pop_pct(pop_df)   # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')    # add in target geolevel names and geolevel type


############# STATE CALCS ##################
# get and prep state pop
ca_pop_wide <- state_pop(vars = vars_list_acs, vars2 = vars_list_acs_swana, yr = acs_yr, srvy = survey)

# calc state wa
ca_pct_df <- ca_pop_pct(ca_pop_wide)
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
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

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

#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'upper', ]
lower_table <- d[d$geolevel == 'lower', ]

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
colnames(leg_table)[geoid] <- c("leg_id")


### info for postgres tables will auto update ###
county_table_name <- paste0("arei_hben_drinking_water_county_", rc_yr)
state_table_name <- paste0("arei_hben_drinking_water_state_", rc_yr)
city_table_name <- paste0("arei_hben_drinking_water_city_", rc_yr)
leg_table_name <- paste0("arei_hben_drinking_water_leg_", rc_yr)

start_yr <- acs_yr - 4

indicator <- paste0("Created on ", Sys.Date(), ". Exposure to Contaminated Drinking Water Score")
source <- paste0("CalEnviroScreen ", ces_v, " (", curr_yr, ") https://oehha.ca.gov/calenviroscreen/report/calenviroscreen-40, ACS DP05 (", start_yr, "-", acs_yr, ").")


#send tables to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)
#leg_to_postgres(leg_table)