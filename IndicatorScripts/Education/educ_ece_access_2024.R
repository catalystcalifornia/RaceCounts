### ECE Access RC v6 ### 

##install packages if not already installed ------------------------------
list.of.packages <- c("dplyr","tidyr","tidycensus","tigris","readxl","sf","tidyverse","usethis","RPostgeSQL")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)
library(dplyr)
library(tidyr)
library(tidycensus)
library(tigris)
library(readxl)
library(sf)
library(tidyverse)
library(usethis)
library(RPostgreSQL)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


#### 1. SET UP: DEFINE VARIABLES, FX SOURCE, GET COUNTY NAMES ####

# define variables used in several places that must be updated each year
curr_yr <- "2020-2021"  # must keep same format
rc_schema <- "v6"
yr <- "2024"

# set values for weighted average functions - You may need to update these
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")     #set source for WA Functions script
year <- c(2020)                   # define your data vintage
subgeo <- c('zcta')              # define your sub geolevel: can be tract or zcta (zcta may require some additions to the fx since they are mostly for tract)
targetgeolevel <- c('county')     # define your target geolevel
survey <- "census"                # define which Census survey you want
pop_threshold = 50                # define population threshold for screening
census_api_key(census_key1)       # reload census API key
vars_list <- "vars_list_p12"      # pop under 5 by race/eth, the list of variables is in the WA fx script

county_yr <- 2021
targetgeo_names <- county_names(vars = vars_list_acs, yr = county_yr, srvy = "acs5")              # use fx to get county names


#### 2. AIR TK ENR DATA ####

#get tk data
air_tk <- read_xlsx("W:/Data/Education/American Institute for Research/2020/tk.xlsx", range = "A4:F2566", na = "*")
# air_tk_meta <- read_xlsx("W:/Data/Education/American Institute for Research/2020/tk.xlsx", range = "A2:F5", na = "*")
# air_tk_meta <- air_tk_meta[-which(air_tk_meta[1]=='California'),] # check against subset below
# View(air_tk_meta)

names(air_tk) <- c("geoname", "pct_zip", "three", "four", "five", "tk") 

#join county ids
air_tk <- left_join(air_tk, targetgeo_names, by = c("geoname" = "target_name")) %>% 
  
  #fill in the blanks for county ids, 
  fill(target_id) %>% 
  
  #filter out county totals
  filter(pct_zip != "Percent of Zip Code Allocation")  %>%
  
  #create county-zip id
  mutate(zip_county_id = paste0(geoname, "-", target_id)) %>% 
  
  #remove unnecessary fields
  select(-"three", -"four", -"five")


#### 3. CCCRRN ENR DATA ####

#get CCCRRN data
cccrrn <- read_xlsx("W:/Data/Education/CCCRRN/2021/CatalystCA2021Data_rev.xlsx") %>% rename(geoname = ZIPCODE)

#format columns for join
cccrrn$geoname <- as.character(cccrrn$geoname) 


#### 4. Join AIR TK and CCCRRN then calculate enrollment counts ####

# calculated as we did for education.ece_zip_code_enrollment_rate_2018 used in RC v3
## which assumes ccrrn capacity = full enrollment. 
df <- full_join(air_tk, cccrrn, by = "geoname") %>% rename("sub_id" = "geoname")  # join AIR TK and CCCRRN enr data
df$enrollment <- rowSums(df[,c("INFCAP", "PRECAP", "FCCCAP", "tk")], na.rm=TRUE) # unweighted enrollment
df <- filter(df, enrollment >= 0)

# import ZCTA-County Relationship File from Census. https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2020.html#zcta
## Read more: https://www2.census.gov/geo/pdfs/maps-data/data/rel2020/zcta520/explanation_tab20_zcta520_county20_natl.pdf and
         ##   https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/2020-zcta-record-layout.html#county
filepath <- "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"
rel_file <- read_delim(file = filepath, delim = "|", na = c("*", ""))
xwalk <- filter(rel_file, (substr(GEOID_COUNTY_20,1,2) == '06' & !is.na(GEOID_ZCTA5_20))) %>%
                rename(zcta_id = GEOID_ZCTA5_20, county_id = GEOID_COUNTY_20, county_name = NAMELSAD_COUNTY_20) %>%
                mutate(pct_zcta = AREALAND_PART / AREALAND_ZCTA5_20) %>%      # calc pct of zcta within each county it overlaps with
                select(c(zcta_id, county_id, county_name, pct_zcta))

#### 5. Get ZCTA under 5 pop by race ####
pop <- update_detailed_table_census(vars = vars_list, yr = year, srvy = survey)  # subgeolevel pop
pop <- lapply(pop, function(x) cbind(x, table = str_extract(x$variable, "[^_]+"))) # create table field used in next step
pop_ <- as.data.frame(pop) %>% select(c(GEOID, table, geolevel, value)) %>% group_by(GEOID, table, geolevel) %>% summarise(value_sum=sum(value))
pop_ <- pop_ %>% left_join(p12_meta, by = c("table" = "p12_table")) # add race field
pop_wide <- pop_ %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, geolevel), names_from = p12_race, values_from = value_sum)
pop_wide <- pop_wide %>% right_join(select(xwalk, c(zcta_id, county_id, county_name)), by = c("GEOID" = "zcta_id"))  # join target geoids/names
pop_wide <- pop_wide %>% relocate(county_id, .after = "GEOID") %>% relocate(county_name, .after = "county_id")
colnames(pop_wide)[5:ncol(pop_wide)] <- paste(colnames(pop_wide)[5:ncol(pop_wide)], "unw_sub_pop", sep = "_") # rename sub_pop columns
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = county_id, target_name = county_name) # rename to generic column names for WA functions

# Weight ZCTA pop by % of ZCTA within each county it overlaps with
pop_wide <- pop_wide %>% left_join(xwalk %>% select(zcta_id, county_id, pct_zcta), by = c("sub_id" = "zcta_id", "target_id" = "county_id")) #join pop data to xwalk to get pct_zcta
pop_wt <- pop_wide %>% select(c(sub_id, target_id, pct_zcta, ends_with("sub_pop"))) %>% mutate(across(where(~is.numeric(.)), ~.x*pct_zcta)) #calc wt zcta pop
colnames(pop_wt) <- gsub('unw_', '', colnames(pop_wt)) # drop unw (unweighted) part of colnames
n_df <- pop_wt %>% select(target_id, sub_id) %>% group_by(target_id) %>% summarise(n = n()) # count of sub_geos in each target_geo
pop_wt <- pop_wt %>% left_join(n_df, by = "target_id")


#### 6. Get county and state under 5 pop by race ####

###### County Pop ##
# Get target pop directly from API, rather than use targetgeo_pop{}, bc ZCTAs don't cover all of counties and don't fully nest into counties as CTs do.
### This is total county pop, not the total of county pop that resides within a zcta.
pop_target <- list(get_decennial(geography = "county", state = "CA", variables = vars_list_p12, year = year, sumfile = "dhc") %>% 
                    mutate(geolevel = "county"))
pop_target <- lapply(pop_target, function(x) cbind(x, table = str_extract(x$variable, "[^_]+"))) # create table field used in next step
pop_target_ <- as.data.frame(pop_target) %>% select(c(GEOID, table, value)) %>% group_by(GEOID, table) %>% summarise(value_sum=sum(value))
pop_target_ <- pop_target_ %>% left_join(p12_meta, by = c("table" = "p12_table")) # add race field
pop_target_wide <- pop_target_ %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID), names_from = p12_race, values_from = value_sum)
pop_target_wide <- pop_target_wide %>% rename(target_id = GEOID)
colnames(pop_target_wide)[2:ncol(pop_target_wide)] <- paste(colnames(pop_target_wide)[2:ncol(pop_target_wide)], "target_pop", sep = "_") # rename target_pop columns

#join county data to zcta data and format
pop_df <- left_join(pop_wt, pop_target_wide, by="target_id")


###### State Pop ##
### This is total state pop, not the total of state pop that resides within a zcta.
pop_state <- list(get_decennial(geography = "state", state = "CA", variables = vars_list_p12, year = year, sumfile = "dhc") %>% 
                     mutate(geolevel = "state"))
ca_pop <- lapply(pop_state, function(x) cbind(x, table = str_extract(x$variable, "[^_]+"))) # create table field used in next step
ca_pop <- as.data.frame(ca_pop) %>% select(c(GEOID, table, value)) %>% group_by(GEOID, table) %>% summarise(value_sum=sum(value))
ca_pop <- ca_pop %>% left_join(p12_meta, by = c("table" = "p12_table")) %>% select(-c(table)) # add race field
ca_pop <- ca_pop %>% rename(target_id = GEOID, target_pop = value_sum, raceeth = p12_race)
ca_pop_wide <- pivot_wider(ca_pop, id_cols = target_id, names_from = raceeth, names_glue = "{raceeth}_{.value}", values_from = target_pop)


#### 7. Calc indicator by zcta ################
# get indicator for county (weighted total enr rate for zctas)
ind_df <- df %>% select(sub_id, enrollment) %>% left_join(pop_df %>% select(sub_id, target_id, total_sub_pop, pct_zcta), by = c("sub_id"), relationship = "many-to-many") 
ind_df <- ind_df %>% mutate(wt_enr = ind_df$enrollment * ind_df$pct_zcta) %>% unique() # calc weighted enrollment and keep only unique rows (zcta-county combos)

ind_df$indicator <- ind_df$wt_enr / ind_df$total_sub_pop * 100 # calc indicator (weighted total enr rate by zcta)
ind_df$indicator[ind_df$indicator == "Inf"] <- 100
ind_df_cnty <- ind_df # NOTE: ind_df will be overwritten later during state WA calcs, so storing as another df for later


#### 8. Calc weighted averages ################
pop_threshold = 50 # same threshold as used in previous calcs

##### COUNTY WEIGHTED AVG CALCS ###
    pct_df <- pop_pct_multi(pop_df) # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
    wa <- wt_avg(pct_df)            # calc weighted average and apply reliability screens
    
    wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')    # add in target geolevel names and geolevel type


##### STATE WEIGHTED AVG CALCS ###
# This code comes from/replaces ca_pop_pct{} which works with tracts but not zctas
    subpop <- pop_wide %>% select(-c(pct_zcta, target_name, geolevel))
    subpop$target_id <- '06'                                           # replace county target_id values w/ state-level target_id value
    subpop_long <- pivot_longer(subpop, 3:ncol(subpop), names_to="raceeth", values_to="sub_pop") %>% unique()
    subpop_long$raceeth <- gsub("_unw_sub_pop","-",as.character(subpop_long$raceeth))              # update to generic raceeth names
    
    ca_pop_long <- pivot_longer(ca_pop_wide, 2:ncol(ca_pop_wide), names_to="raceeth", values_to="target_pop")
    ca_pop_long$raceeth <- gsub("_target_pop","-",as.character(ca_pop_long$raceeth))               # update to generic raceeth names
    
    subpop_long <- subpop_long %>% left_join(ca_pop_long, by=c("target_id" = "target_id", "raceeth" = "raceeth"))  # join target and sub pops in long form
    ca_pcts_long <- subpop_long %>% mutate(pct = ifelse(target_pop < pop_threshold, NA, (sub_pop / target_pop)),   # calc pcts of each target geolevel pop per sub geolevel pop
                                           measure_pct=sub("-", "_pct_target_pop", raceeth))           # create new column names
    ca_pct_df <- ca_pcts_long %>% select(sub_id, target_id, measure_pct, pct) %>%              # pivot long table back to wide keeping only new columns
      pivot_wider(names_from=measure_pct, values_from=pct)
    
    # get indicator for state (unweighted total enr rate for zctas)
    ### unweighted bc for state-level, it doesn't matter if a zcta is split btwn 1 or more counties
    ind_df_st <- ind_df %>% select(c(sub_id, enrollment))
    ind_df_st <- ind_df_st %>% unique() %>% left_join(pop_wide %>% select(sub_id, total_unw_sub_pop) %>% unique(), by = "sub_id")
    ind_df_st$indicator <- ind_df_st$enrollment / ind_df_st$total_unw_sub_pop * 100
    ind_df <- ind_df_st  # note: this overwrites the weighted indicator ind_df
    
    ca_wa <- ca_wt_avg(ca_pct_df) %>% mutate(geolevel = 'state')   # add geolevel type
    n_st <- length(unique(pop_wide$sub_id))  # get count of zctas statewide
    ca_wa$n <- n_st # add unique count of zctas


#### 9. Join county & state WA tables  ##################
wa_all <- union(wa, ca_wa)
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid)# move geoname column

d <- wa_all
View(d)


#### 10. Calc RACE COUNTS stats ##############
###### To use the following RC Functions, 'd' will need the following columns at minimum: 
###### county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
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


### update info for postgres tables - automatically updates based on variables at top of script ###
county_table_name <- paste0("arei_educ_ece_access_county_",yr)
state_table_name <- paste0("arei_educ_ece_access_state_",yr)
indicator <- "ECE Access"
source <- paste0("CCCRRN https://rrnetwork.org/ and AIR ELNAT https://elneedsassessment.org/ (", curr_yr, ")")

#send tables to postgres
#to_postgres(county_table, state_table)


#disconnect
dbDisconnect(con)






