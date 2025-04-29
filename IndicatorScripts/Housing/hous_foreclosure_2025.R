## Foreclosures per 10k Owner-Occupied Households (WA) for RC v7

######## Due to using a different population basis for WA (owner households), this is not a good script to use as a WA template. ##############
##install packages if not already installed ------------------------------
packages <- c("dplyr", "data.table", "readxl", "tidycensus", "sf", "DBI", "RPostgres", "stringr", "tidyr", "tigris", "usethis", "readr", "rvest", "tidyverse", "stringr")  

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

#set source for RC Functions script
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")

#set QA filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Sheet_Foreclosure.docx"

# update each year: variables used throughout script
acs_yr <- 2021         # last yr of acs 5y span
curr_yr <- '2017-2021' # acs 5yr span
rc_schema <- 'v7'
rc_yr <- '2025'

# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # This may need to be updated. Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # This may need to be updated.
sen_geoid <- 'sldu24'			                      # This may need to be updated. Define column with Senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # This may need to be updated.


# Check that B25003 variable names each year and update if needed ####
## We define different vars_list than what's in WA functions, bc basis here is owner households by race, not population by race.
## b25003_curr must represent the following data in this same order:
## Owner households: total, black, aian, asian, pacisl, other, twoormor, nh_white, latinx (All except Two+ and Latinx are 1 race alone, all except Latinx are non-Latinx.)
## the variables MUST BE in this order:
vars_list_b25003<- list("total"="B25003_002", 
                        "black"="B25003B_002", 
                        "aian"="B25003C_002", 
                        "asian"="B25003D_002", 
                        "pacisl"="B25003E_002", 
                        "other"="B25003F_002", 
                        "twoormor"="B25003G_002", 
                        "nh_white"="B25003H_002", 
                        "latino"="B25003I_002")

race_mapping <- data.frame(
  name = unlist(vars_list_b25003),
  race = names(vars_list_b25003),
  stringsAsFactors = FALSE
)

b25003_curr <- load_variables(acs_yr, "acs5", cache = TRUE) %>% 
  filter(name %in% vars_list_b25003) %>%
  select(-c(geography)) %>% 
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "_pop")) %>%
  select(-race)

# CHECK THIS TABLE TO MAKE SURE THE CONCEPT AND RC_RACES COLUMNS MATCH UP
print(b25003_curr) 


# Export foreclosure to rda shared table ------------------------------------------------------------
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# foreclosure <- read_excel("W:/Data/Housing/Foreclosure/Foreclosure - Dataquick/Original Data/AdvanceProj 081122.xlsx", sheet = 2, skip = 5)

# table_schema <- "housing"
# table_name <- "dataquick_tract_2010_22_foreclosures"
# table_comment_source <- "Foreclosures per 10k owner households by race (WA)."
# table_source <- "The data is from DataQuick (2010-2022), purchased from DQNews."
# dbWriteTable(con, c(table_schema, table_name), foreclosure, overwrite = FALSE, row.names = FALSE)

# Load data and clean-----
foreclosure <- dbGetQuery(con, "SELECT * FROM housing.dataquick_tract_2010_22_foreclosures") # comment out table creation above after it's done, and instead import data from pgadmin

num_qtrs = 20   # update depending on how many data yrs you are working with
foreclosure <- foreclosure %>% select(-matches('2010|2011|2012|2013|2014|2015|2016|2022')) %>%
  mutate(sum_foreclosure = rowSums(.[3:22], na.rm = TRUE)) %>%  # total number of foreclosures over all data quarters
  mutate(avg_foreclosure = sum_foreclosure / num_qtrs) %>%  # avg quarterly number of foreclosures
  select(-matches('Q'))   # remove quarterly foreclosure columns

foreclosure$County = str_to_title(foreclosure$County)
 foreclosure$County <- str_remove(foreclosure$County,  "\\s*\\(.*\\)\\s*")     # remove spaces from col names
 foreclosure$County <- gsub("; California", "", foreclosure$County)
 foreclosure <- rename(foreclosure, "county"="County", "census_tract"="Census Tract")

# There are 3 tracts in the data that appear in 2020 tract list, but not in 2010 tract list.
## We pull those foreclosures out here, and assign them to the 2020 tracts after the 2010-2020 tract conversion process.
foreclosures_20 <- foreclosure %>% filter(census_tract %in% c('000902', '001003') & county == 'Siskiyou') %>%
  mutate(GEOID_TRACT_20 = paste0('06093', census_tract)) %>%   # Siskiyou
  rename(foreclosure_20 = sum_foreclosure) %>%
  select(c(GEOID_TRACT_20, foreclosure_20))
 
View(foreclosure)

# merge dfs by geoname then paste the county id to the front of the tract IDs to make full CT FIPS codes
ind_2010 <- left_join(targetgeo_names, foreclosure, by = c("target_name" = "county")) %>% 
  mutate(sub_id = paste0(target_id, census_tract)) %>% 
  #rename(c("target_id" = "geoid")) %>%
  select(target_id, sub_id, target_name, sum_foreclosure, avg_foreclosure) %>% 
  as.data.frame()
# View(ind_2010)

# note: Foreclosure uses 2010 tract vintage, population estimates use 2020 tract vintage
cb_tract_2010_2020 <- fread("W:\\Data\\Geographies\\Relationships\\cb_tract2020_tract2010_st06.txt", sep="|", colClasses = 'character', data.table = FALSE) %>%
  select(GEOID_TRACT_10, NAMELSAD_TRACT_10, AREALAND_TRACT_10, GEOID_TRACT_20, NAMELSAD_TRACT_20, AREALAND_TRACT_20, AREALAND_PART) %>%
  mutate_at(vars(contains("AREALAND")), function(x) as.numeric(x)) %>%
  # calculate overlapping land area of 2010 and 2020 tracts (AREALAND_PART) as a percent of 2020 tract land area (AREALAND_TRACT_20)
  mutate(prc_overlap=AREALAND_PART/AREALAND_TRACT_10) # pct of 2010 tract that is in 2020 tract to ensure 100% of 2010 foreclosures are assigned to 2020 tracts

# Because Foreclosure uses 2010 vintage tracts - need to convert to 2020 vintage and allocate score accordingly
ind_2010_2020 <- cb_tract_2010_2020 %>%
  right_join(ind_2010, by=c('GEOID_TRACT_10'='sub_id')) %>%
  select(GEOID_TRACT_10, sum_foreclosure, AREALAND_TRACT_10, AREALAND_PART, GEOID_TRACT_20, AREALAND_TRACT_20, prc_overlap) %>%
  # Allocate CES scores from 2010 tracts to 2020 using prc_overlap
  mutate(foreclosure_20=sum_foreclosure*prc_overlap)

## check that prc_overlap sums to 1 for 2010 tracts
# check_prc_is_1 <- ind_2010_2020 %>%
#   select(GEOID_TRACT_10, prc_overlap) %>%
#   group_by(GEOID_TRACT_10) %>%
#   summarize(total_prc = sum(prc_overlap))

# add back foreclosure data for 2020-only tracts
ind_2010_2020 <- ind_2010_2020 %>% plyr::rbind.fill(foreclosures_20)

# create indicator df (2020 tracts) to be used in WA calcs
ind_2020 <- ind_2010_2020 %>%
  # sum weighted foreclosures by 2020 tract
  group_by(GEOID_TRACT_20) %>%
  summarize(sum_foreclosure = sum(foreclosure_20)) %>%
  # clean up names
  rename(sub_id = GEOID_TRACT_20) %>% 
  # calc 5-yr avg foreclosure rate (2017-2021)
  mutate(avg_foreclosure = sum_foreclosure / num_qtrs) 

ind_df <- ind_2020 # rename to ind_df for WA fx


############# COUNTY CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 30                # define population threshold for screening


##### CREATE COUNTY GEOID & NAMES TABLE ###  
census_api_key(census_key1, overwrite=TRUE)
targetgeo_names <- get_acs(geography = "county", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = acs_yr)

targetgeo_names <- targetgeo_names[,1:2]
targetgeo_names$NAME <- str_remove(targetgeo_names$NAME,  "\\s*\\(.*\\)\\s*")  # clean geonames
targetgeo_names$NAME <- gsub(" County, California", "", targetgeo_names$NAME)
names(targetgeo_names) <- c("target_id", "target_name")


##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop. NOTE: This indicator uses a custom variable list (vars_list_b25003)

# transform pop data to wide format 
pop_wide <- to_wide(pop)

# rename B25003 vars to rc_races
rc_races_ <- paste0(rc_races, "_")
names(rc_races_) <- vars_list_b25003
names(pop_wide) <- str_replace_all(names(pop_wide), rc_races_)

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              # rename to generic column name for WA functions

##### CUSTOMIZED VERSION OF TARGETGEO_POP FUNCTION HERE THAT WORKS WITH OWNER HOUSEHOLDS AS POP BASIS ###

# select pop estimate columns and rename to RC names
b <- select(pop_wide, sub_id, target_id, ends_with("e"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- gsub("_e", "_target_pop", colnames(c))

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop_wide, sub_id, target_id, geolevel, ends_with("e"), -NAME) 
names(e) <- gsub("_e", "_sub_pop", colnames(e))
pop_df <- e %>% left_join(c, by = "target_id")

##### EXTRA STEP: Calc avg quarterly foreclosures per 10k pop by tract bc WA avg should be calc'd using this, not avg # of foreclosures
ind_df <- ind_2020 %>% left_join(pop_df %>% select(sub_id, total_sub_pop), by = "sub_id") %>% 
  mutate(indicator = (avg_foreclosure / total_sub_pop) * 10000)

##### COUNTY WEIGHTED AVG CALCS ###
pct_df <- pop_pct(pop_df)   # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')     # add in target geolevel names

############# STATE CALCS ##################
############### CUSTOMIZED VERSION OF CA_POP_WIDE and CA_POP_PCT FUNCTION HERE THAT WORKS WITH OWNER HOUSEHOLD AS POP BASIS ###
# custom ca_pop_wide calcs
ca_pop <- do.call(rbind.data.frame, list(
  get_acs(geography = "state", state = "CA", variables = vars_list_b25003, year = year, survey = survey, cache_table = TRUE)))
ca_pop <- ca_pop %>%  
  mutate(geolevel = "state")
ca_pop_wide <- select(ca_pop, GEOID, NAME, variable, estimate) %>% pivot_wider(names_from=variable, values_from=estimate)
ca_pop_wide <- ca_pop_wide %>% rename("target_id" = "GEOID", "target_name" = "NAME")

# rename B25003 vars to rc_races
rc_races_ <- paste0(rc_races, "_target_pop")  # note: this is slightly different than code for counties
names(rc_races_) <- vars_list_b25003
names(ca_pop_wide) <- str_replace_all(names(ca_pop_wide), rc_races_)

# custom ca_pop_pct calcs
subpop <- select(pop_wide, sub_id, ends_with("e"), target_id, -NAME)
subpop$target_id <- '06'                                        # replace county target_id values w/ state-level target_id value
colnames(subpop) <- gsub("_e", "_sub_pop", colnames(subpop))
subpop_long <- pivot_longer(subpop, ends_with("_sub_pop"), names_to="raceeth", values_to="sub_pop")
subpop_long$raceeth <- gsub("_sub_pop","-",as.character(subpop_long$raceeth))              # update to generic raceeth names

ca_pop_long <- pivot_longer(ca_pop_wide, ends_with("_target_pop"), names_to="raceeth", values_to="target_pop")
ca_pop_long$raceeth <- gsub("_target_pop","-",as.character(ca_pop_long$raceeth))           # update to generic raceeth names

subpop_long <- subpop_long %>% left_join(ca_pop_long, by=c("target_id" = "target_id", "raceeth" = "raceeth"))  # join target and sub pops in long form
ca_pcts_long <- subpop_long %>% mutate(pct = ifelse(target_pop < pop_threshold, NA, (sub_pop / target_pop)),   # calc pcts of each target geolevel pop per sub geolevel pop
                                       measure_pct=sub("-", "_pct_target_pop", raceeth))                       # create new column names
ca_pct_df <- ca_pcts_long %>% select(sub_id, target_id, measure_pct, pct) %>%                                  # pivot long table back to wide keeping only new columns
  pivot_wider(names_from=measure_pct, values_from=pct)

# calc state WA
ca_wa <- ca_wt_avg(ca_pct_df) %>% mutate(geolevel = 'state')   # add geolevel type

############# CITY CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')      # define your target geolevel: place
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 30                # define population threshold for screening #its 250 for county and state

# pull in crosswalk
xwalk_filter <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.ct_place_", acs_yr))

####### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop

# transform pop data to wide format
pop_wide <- to_wide(pop)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- pop_wide %>% right_join(select(xwalk_filter, c(ct_geoid, place_geoid, place_name)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid, target_name = place_name)                         # rename to generic column names for WA functions

# rename B25003 vars to rc_races
rc_races_ <- paste0(rc_races, "_")
names(rc_races_) <- vars_list_b25003
names(pop_wide) <- str_replace_all(names(pop_wide), rc_races_)

############### CUSTOMIZED VERSION OF TARGETGEO_POP FUNCTION HERE THAT WORKS WITH OWNER HOUSEHOLDS AS POP BASIS ###
# calc target geolevel pop and number of sub geolevels per target geolevel

# select pop estimate columns
b <- select(pop_wide, sub_id, target_id, ends_with("e"), -NAME)

# aggregate sub geolevel pop to target geolevel and rename to RC names
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- gsub("_e", "_target_pop", colnames(c))

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop_wide, sub_id, target_id, geolevel, ends_with("e"), -NAME) 
colnames(e) <- gsub("_e", "_sub_pop", colnames(e))
pop_df_city <- e %>% left_join(c, by = "target_id")


##### CITY WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df_city)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)             # calc weighted average and apply reliability screens
city_wa <- city_wa %>%                # add place names (target names)
  left_join(xwalk_filter %>% select(c(place_name, place_geoid)), by = c("target_id" = "place_geoid"), relationship = "many-to-many") %>%
  rename(target_name = place_name) %>%
  relocate(target_name, .after=target_id) %>%
  unique() # take unique rows bc xwalk can have >1 row per place
city_wa <- city_wa %>% mutate(geolevel = 'city')  # add geolevel

############# ASSEMBLY CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldl')      # define your target geolevel: state assembly
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 250              # define population threshold for screening

### CT-Assm Crosswalk ##
# Import CT-Assm Crosswalk
xwalk_filter <- dbGetQuery(con, paste0("SELECT geo_id AS ct_geoid, ", assm_geoid, " AS assm_geoid FROM crosswalks.", assm_xwalk))

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop

# transform pop data to wide format
pop_wide <- to_wide(pop)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(xwalk_filter, c(ct_geoid, assm_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = assm_geoid) # rename to generic column names for WA functions

# rename B25003 vars to rc_races
rc_races_ <- paste0(rc_races, "_")
names(rc_races_) <- vars_list_b25003
names(pop_wide) <- str_replace_all(names(pop_wide), rc_races_)

##### CUSTOMIZED VERSION OF TAR# calc target geolevel pop and number of sub geolevels per target geolevel
# calc target geolevel pop and number of sub geolevels per target geolevel

# select pop estimate columns and rename to RC names
b <- select(pop_wide, sub_id, target_id, ends_with("e"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- gsub("_e", "_target_pop", colnames(c))

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop_wide, sub_id, target_id, geolevel, ends_with("e"), -NAME) 
names(e) <- gsub("_e", "_sub_pop", colnames(e))
pop_df <- e %>% left_join(c, by = "target_id")

##### ASSM WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
assm_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% mutate(geolevel = 'sldl')  # add geolevel

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
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

############# SENATE CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldu')      # define your target geolevel: county (state is handled separately)
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 250              # define population threshold for screening

### CT-Sen Crosswalk ##
# Import CT-Sen Crosswalk
xwalk_filter <- dbGetQuery(con, paste0("SELECT geo_id AS ct_geoid, ", sen_geoid, " AS sen_geoid FROM crosswalks.", sen_xwalk))

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop

# transform pop data to wide format
pop_wide <- to_wide(pop)
#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(xwalk_filter, c(ct_geoid, sen_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = sen_geoid) # rename to generic column names for WA functions

# rename B25003 vars to rc_races
rc_races_ <- paste0(rc_races, "_")
names(rc_races_) <- vars_list_b25003
names(pop_wide) <- str_replace_all(names(pop_wide), rc_races_)

##### CUSTOMIZED VERSION OF TAR# calc target geolevel pop and number of sub geolevels per target geolevel
# calc target geolevel pop and number of sub geolevels per target geolevel

# select pop estimate columns and rename to RC names
b <- select(pop_wide, sub_id, target_id, ends_with("e"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- gsub("_e", "_target_pop", colnames(c))

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop_wide, sub_id, target_id, geolevel, ends_with("e"), -NAME) 
names(e) <- gsub("_e", "_sub_pop", colnames(e))
pop_df <- e %>% left_join(c, by = "target_id")

##### SEN WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
sen_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
sen_wa <- sen_wa %>% mutate(geolevel = 'sldu')  # add geolevel

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_acs(geography = "State Legislative District (Upper Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = acs_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
sen_name$NAME <- gsub(", California", "", sen_name$NAME)
names(sen_name) <- c("target_id", "target_name")
# View(sen_name)

#add geonames to WA
sen_wa <- merge(x=sen_name,y=sen_wa,by="target_id", all=T)
#View(sen_wa)

############ JOIN CITY, ASSM, SEN, COUNTY, & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% union(city_wa) %>% union(assm_wa) %>% union(sen_wa) 
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid)        # move geoname column

d <- wa_all
View(d)

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

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
View(state_table)

#remove state from county table
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

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")

###update info for postgres tables###
county_table_name <- paste0("arei_hous_foreclosure_county_", rc_yr)
state_table_name <- paste0("arei_hous_foreclosure_state_", rc_yr)
city_table_name <- paste0("arei_hous_foreclosure_city_", rc_yr)
leg_table_name <- paste0("arei_hous_foreclosure_leg_", rc_yr)

indicator <- paste0("Foreclosures per 10k owner households by race (WA). The data is")
source <- paste0("DataQuick (", curr_yr, "), purchased from DQNews and raced via weighted average using ACS ", curr_yr, " data. QA doc: ", qa_filepath)

#send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)
# dbDisconnect(con)
