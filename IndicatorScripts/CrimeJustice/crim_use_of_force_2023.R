## Use of Force 2016-2021 RC v5

## Set up ----------------------------------------------------------------
#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse","RPostgreSQL", "tidycensus", "readxl", "sf", "janitor", "stringr", "data.table", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

## packages
library(tidyverse)
library(readxl)
library(RPostgreSQL)
library(sf)
library(tidycensus)
library(DBI)
library(janitor)
library(stringr)
library(data.table) # %like% operator
library(usethis)

options(scipen = 999) # disable scientific notation


# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")


############### PREP 2020 URSUS RDA_SHARED_DATA TABLES ########################
# Data Dictionary: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/Use%20of%20Force%20Incident%20Reporting%202020%20Context_073021.pdf
##### civilian-officer
filepath = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/URSUS_Civilian-Officer_2020.csv" 
fieldtype = 1:36  # confirm using metadata link

## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "crime_and_justice"
table_name <- "ursus_civilian_officer_2020"
table_comment_source <- "NOTE: race/eth column values have inconsistencies, for example ''''asian indian'''' and ''''asian_indian''''."
table_source <- "Use of force data downloaded 6/6/2023
from https://openjustice.doj.ca.gov/data. Metadata saved here: W:/Data/Crime and Justice/Police Violence/Open Justice/2020/"

## Run function to prep and export rda_shared_data table. 
### NOTE: con2 must be rda_shared_data for function to work.
source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
df <- get_ursus_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db
ursus_civilian_officer_2020 <- df %>% mutate(year=str_sub(table_name,-4,-1))

##### incident
filepath = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/URSUS_Incident_2020.csv" 
fieldtype = 1:14  # confirm using metadata link

## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "crime_and_justice"
table_name <- "ursus_incident_2020"
table_comment_source <- "NOTE: This table has 1 row per incident with total # of civilians involved in Use of Force incident. Tables like ursus_civilian_officer_2016, have 1 row per civilian involved in an incident. So if you join the tables, then sum the num_involved_civilians field, you will be double-counting people."
table_source <- "Use of force data downloaded 6/6/2023
from https://openjustice.doj.ca.gov/data. Metadata saved here: W:/Data/Crime and Justice/Police Violence/Open Justice/2020/"

## Run function to prep and export rda_shared_data table. 
### NOTE: con2 must be rda_shared_data for function to work.
source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
#df <- get_ursus_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db

############### PREP 2021 URSUS RDA_SHARED_DATA TABLES ########################
# Data Dictionary: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/UseofForce_Context_2021.pdf
##### civilian-officer
filepath = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/UseofForce_Civilian-Officer_2021f.csv" 
fieldtype = 1:36  # confirm using metadata link

## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "crime_and_justice"
table_name <- "ursus_civilian_officer_2021"
table_comment_source <- "NOTE: race/eth column values have inconsistencies, for example ''''asian indian'''' and ''''asian_indian''''."
table_source <- "Use of force data downloaded 6/6/2023
from https://openjustice.doj.ca.gov/data. Metadata saved here: W:/Data/Crime and Justice/Police Violence/Open Justice/2021/"

## Run function to prep and export rda_shared_data table. 
### NOTE: con2 must be rda_shared_data for function to work.
source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
#df <- get_ursus_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db
ursus_civilian_officer_2021 <- df %>% mutate(year=str_sub(table_name,-4,-1))

##### incident
filepath = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/UseofForce_Incident_2021.csv" 
fieldtype = 1:14  # confirm using metadata link

## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "crime_and_justice"
table_name <- "ursus_incident_2021"
table_comment_source <- "NOTE: This table has 1 row per incident with total # of civilians involved in Use of Force incident. Tables like ursus_civilian_officer_2016, have 1 row per civilian involved in an incident. So if you join the tables, then sum the num_involved_civilians field, you will be double-counting people."
table_source <- "Use of force data downloaded 6/6/2023
from https://openjustice.doj.ca.gov/data. Metadata saved here: W:/Data/Crime and Justice/Police Violence/Open Justice/2021/"

## Run function to prep and export rda_shared_data table. 
### NOTE: con2 must be rda_shared_data for function to work.
source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
#df <- get_ursus_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db


############### PREP ALL URSUS --------------------------------------------------------------
### Pull in URSUS data from rda_shared_data.crime_and_justice ---------------------------------------------------------------
sql_query <- "SELECT table_name FROM information_schema.tables
                   WHERE table_schema='crime_and_justice' AND table_type='BASE TABLE'
                   ORDER BY table_name"
table_list <- dbGetQuery(con2, sql_query)
# filter for only URSUS tables
ursus_list <- filter(table_list, grepl("ursus_",table_name)) 
ursus_list <- ursus_list[order(ursus_list$table_name), ]  # alphabetize list of URSUS tables, needed to format list correctly for next step
# import all tables on ursus_list
ursus_tables <- lapply(setNames(paste0("select * from crime_and_justice.", ursus_list), ursus_list), DBI::dbGetQuery, conn = con2)
ursus_tables <- Map(cbind, ursus_tables, year = names(ursus_tables)) # add year column, populated by table names
ursus_tables <- lapply(ursus_tables, transform, year=str_sub(year,-4,-1)) # update year column values to year only
list2env(ursus_tables, envir = .GlobalEnv) # convert URSUS list to separate df's

# Combine all data years --------------------------------------------------
year_list <- c('2016','2017','2018','2019','2020','2021')  # update based on which years of data you are including

prep_ursus <- function(old.x) {
  table1 = eval(parse(text=paste0("ursus_civilian_officer_", old.x)))
  table2 = eval(parse(text=paste0("ursus_incident_", old.x)))
  if(!'state_name' %in% names(table2)) table2 <- table2 %>% add_column(state_name = 'CA') # add state_name col if doesn't exist bc in some tables col is called 'state' not 'state_name'
  new.x <- left_join(table1, table2) %>% select(incident_id, civilian_officer, race_ethnic_group, received_force, county, state_name, city, zip_code, num_involved_civilians, year) %>% filter(
    received_force == "TRUE", civilian_officer == "Civilian" )
  # clean data
  new.x$county <- gsub(" County", "", new.x$county)  # remove ' County' from county column values
  new.x$county <- str_to_title(new.x$county) # make first letter capital for county
  new.x$city <- str_to_title(new.x$city) # make first letter capital for city
  new.x$race_ethnic_group <- sub("_", " ", new.x$race_ethnic_group)   # replace _ with space
  new.x$race_ethnic_group <- sub(" /", ",", new.x$race_ethnic_group)  # replace / with comma
  new.x$received_force <- as.logical(new.x$received_force)
  new.x$zip_code <- as.character(new.x$received_force)
  
return(new.x)  
}

joined <- list() # create empty list for loop below

# create new list of joined dataframes
for (i in year_list) {
 temp <- prep_ursus(i)
 joined[[i]] <- temp   
  }

df_all_years <- as.data.frame(bind_rows(joined, .id = "incident_id")) 

# Calculate Raw: Number of incidents across all data years (total and by race) included NOT average per year ---------------------------------------------------------
#### NOTE: There is 1 row per civilian involved in each incident, but each of those rows reports the total # involved and has the same incident ID.
##### So if you sum the num_involved_civilians (which is the total # per incident) column, you will be double/triple-counting folks etc.
######### NOTE: Due to using our custom race groups, some people are double-counted. Ex: Someone who id's as more than 1 group will be counted in both groupings.

# create new race columns for our custom groups, you may need to update this code for race_reclass df based on 'races' df created below
      # races <- unique(df_all_years$race_ethnic_group)  # check which race groups are present in data #
      # sort(races) # get list of unique race/ethnic groups
race_reclass <- df_all_years %>% mutate(nh_white = ifelse(race_ethnic_group == 'white', 'nh_white', 'not nh_white'), 
                                     black = ifelse(grepl('black', race_ethnic_group), 'black', 'not black'), 
                                     aian = ifelse(grepl('american indian', race_ethnic_group), 'aian', 'not aian'), 
                                     api = ifelse(grepl('asian|islander', race_ethnic_group), 'api', 'not api'), 
                                     latino = ifelse(grepl('hispanic', race_ethnic_group), 'latino', 'not latino'),
                                     city = trimws(city)) # remove leading/trailing spaces for match on geoname with ACS later

calc_counts <- function(race_eth, geolevel) {
  counts <- race_reclass %>% filter(race_reclass[[race_eth]] == race_eth) %>% group_by(city, county) %>% mutate(involved = n()) %>% summarise(involved = min(involved))
  new_name <- paste0(race_eth, '_involved')  # generate race-specific col name
  counts <- counts %>% rename(!!new_name := involved)  # rename to race-specific col name
  return(counts)  
}

#### Fix USOF names that should match to ACS later on in script if possible #### Manually looked up unmatched USOF places
######## THIS MANUAL RESEARCH & CLEANING PROCESS WILL NEED TO BE REVIEWED/UPDATED EACH TIME WE PREP THIS DATA #######
la_nhood <- race_reclass$city %in% c("Canoga Park", "Chatsworth", "North Hills", "Pacoima", "Panorama City", "Playa Del Rey", "San Pedro", "Sylmar", "Tujunga", "Van Nuys", "West Hills", "Woodland Hills")
race_reclass$city[la_nhood] <- "Los Angeles"
race_reclass <- race_reclass %>% mutate(city = ifelse(city=='City Of Industry', 'Industry', city)) %>%
  mutate(city = ifelse(city=='Ventura', 'San Buenaventura (Ventura)', city)) %>%
  mutate(city = ifelse(city=='Rivererside', 'Riverside', city)) %>%
  mutate(city = ifelse(city=='Saint Helena', 'St. Helena', city)) %>%
  mutate(city = ifelse(city=='White Water', 'Whitewater', city)) %>%
  mutate(city = ifelse(city=='Cottonwood' & county == 'Shasta', 'Cottonwood CDP (Shasta County)', city))

# City: calc counts by race
total_ <- race_reclass %>% group_by(city, county) %>% mutate(total_involved = n()) %>%  summarise(total_involved = min(total_involved))
black_ <- calc_counts('black')
aian_ <- calc_counts('aian')
api_ <- calc_counts('api')
nh_white_ <- calc_counts('nh_white')
latino_ <- calc_counts('latino')

# join calcs by race together
df_city <- total_ %>% left_join(black_) %>% left_join(aian_) %>% left_join(api_) %>% left_join(nh_white_) %>% left_join(latino_) %>% rename("geoname" = "city") %>% mutate(geolevel='city')


# County: calc counts by race
df_county <- df_city %>% group_by(county) %>% summarise(across(ends_with("involved"), sum, na.rm=TRUE)) %>% # here we can sum not count bc df_city has unduplicated counts
                         rename("geoname" = "county")

# State: calc counts by race
df_county <- df_county %>% adorn_totals("row") %>% as.data.frame(df_county)
df_county$geoname[df_county$geoname == 'Total'] <- 'California'
df_county$geolevel <- ifelse(df_county$geoname == 'California', 'state', 'county')

# join city, county, state together
df_all <- rbind(df_city, df_county) 

# make NA = 0 for cities/counties that appear in USOF data. places that are not in the USOF data still receive NA.
df_all <- df_all %>% mutate(total_involved = coalesce(total_involved, 0), black_involved = coalesce(black_involved, 0), aian_involved = coalesce(aian_involved, 0), api_involved = coalesce(api_involved, 0),
                            nh_white_involved = coalesce(nh_white_involved, 0), latino_involved = coalesce(latino_involved, 0))


  
# Get Total Population: adapted from race_multigeo_2023.R ----------------------------------------------------
# API Call Info  # variables list -- https://api.census.gov/data/2021/acs/acs5/profile/groups/DP05.html
yr = 2021 # change as needed
srvy = "acs5"
table_code = "DP05" # pop by race
dataset = "acs5/profile"      
possible_vars <- list("DP05" = c("DP05_0033", "DP05_0065", "DP05_0066", "DP05_0067", "DP05_0068", "DP05_0071", "DP05_0077")) # NH White, All Black, All AIAN, All API
vars_list <- possible_vars[table_code][[1]]

  # Confirm variables are correct
  #acs_vars <- load_variables(year = yr, dataset = dataset, cache = TRUE)
  #df_metadata <- subset(acs_vars, acs_vars$name %in% vars_list)
  #df_metadata

# county and state pop data
pop_cs <- do.call(rbind.data.frame, list(
  get_acs(geography = "state", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE)
  %>% mutate(geolevel = "state"),
  get_acs(geography = "county", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE)
  %>% mutate(geolevel = "county")
))

# Rename estimate and moe columns to e and m, respectively
pop_cs <- pop_cs %>% rename(e = estimate, m = moe)

# place data/shape and county shape
pop_place <- get_acs(geography = "place", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE, geometry = TRUE) %>%
             mutate(geolevel = "city")
county <- get_acs(geography = "county", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE, geometry = TRUE) %>%
             mutate(geolevel = "county", NAME = gsub(" County, California", "", NAME)) %>% rename(county = NAME)
pop_place_ <- st_join(pop_place, left = FALSE, county["county"]) # join county names to places. https://r-spatial.github.io/sf/reference/st_join.html
pop_place_ <- pop_place_ %>% mutate(pl_co = paste(NAME,county, sep="_"))
pop_place_ <- unique(pop_place_) %>% st_drop_geometry()
pop_place_join <- pop_place %>% left_join(select(pop_place_, NAME, county, pl_co), by = "NAME") %>% unique() %>% st_drop_geometry()

# Rename estimate and moe columns to e and m, respectively
pop_place_join <- pop_place_join %>% rename(e = estimate, m = moe)

# Spread! Switch to wide format.
df_cs_wide <- pop_cs %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

df_place_wide <- pop_place_join %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

df_wide_multigeo <- bind_rows(df_cs_wide, df_place_wide) # append county/state/place dfs

# update col names, calc API pop
df_wide_multigeo$total_pop <- df_wide_multigeo$DP05_0033e
df_wide_multigeo$black_pop <- df_wide_multigeo$DP05_0065e
df_wide_multigeo$aian_pop <- df_wide_multigeo$DP05_0066e
df_wide_multigeo$api_pop <- df_wide_multigeo$DP05_0068e + df_wide_multigeo$DP05_0067e
df_wide_multigeo$latino_pop <- df_wide_multigeo$DP05_0071e
df_wide_multigeo$nh_white_pop <- df_wide_multigeo$DP05_0077e

# drop moe's and orig columns
df_wide_multigeo <- select(df_wide_multigeo, -ends_with("m"), -starts_with("DP05"))

# remove "County, California"
df_wide_multigeo$NAME <- gsub(" County, California", "", df_wide_multigeo$NAME)
df_wide_multigeo$NAME <- gsub(" CDP, California", "", df_wide_multigeo$NAME)
df_wide_multigeo$NAME <- gsub(" town, California", "", df_wide_multigeo$NAME)
df_wide_multigeo$NAME <- gsub(" City city, California", " City", df_wide_multigeo$NAME)
df_wide_multigeo$NAME <- gsub(" city, California", "", df_wide_multigeo$NAME)
df_wide_multigeo$NAME <- gsub(", California", "", df_wide_multigeo$NAME)
#df_wide_multigeo$NAME <- ifelse(df_wide_multigeo$GEOID == '0608968', "Burbank (Santa Clara)", df_wide_multigeo$NAME) # update name to distinguish from Burbank city in LAC
df_wide_multigeo <- df_wide_multigeo %>% rename(geoname = NAME)
names(df_wide_multigeo) <- tolower(names(df_wide_multigeo))
# note: CDP names have not been modified and will not match, in next section we check to see if any of them actually have USOF data and make appropriate fixes


# Join USOF and Pop Data --------------------------------------------------
#df_all_ <- full_join(df_all, df_wide_multigeo, by = c("geoname", "geolevel")) %>% arrange(geoname) %>% select(GEOID, geoname, everything())
  # check if there are places with USOF data that did not match to ACS place, but should
  # usof_nomatch <- filter(df_all_, is.na(GEOID)) # this df should have 49 unmatched
  # View(usof_nomatch)
  # acs_nomatch <- filter(df_all_, (is.na(total_involved) & geolevel == 'place'))
  # View(acs_nomatch)

# Re-join usof and pop data to check if fixes worked
df_calcs <- full_join(df_all, df_wide_multigeo, by = c("geoname", "geolevel", "county")) %>% arrange(geoname) %>% select(geoid, geoname, everything())
#usof_nomatch_final <- filter(df_all_, is.na(GEOID)) # this df should have 31 unmatched


# Screening / calc rates ----------------------------------------------------------
pop_threshold = 100
data_yrs = 6  # update based on number of data yrs included. you must multiply pop by this number to get accurate annual avg rate. raw is the sum of incidents across yrs, not annual avg.
incident_threshold = 5  # update appropriately each year to ensure counties with few incidents and small pops do not result in outlier rates

df_screened <- df_calcs %>% 
  mutate(
    # screen
    total_raw = ifelse(total_pop < pop_threshold | total_involved < incident_threshold, NA, total_involved),
    
    nh_white_raw = ifelse(nh_white_pop < pop_threshold | total_involved < incident_threshold, NA, nh_white_involved),
    
    black_raw = ifelse(black_pop < pop_threshold | total_involved < incident_threshold, NA, black_involved),
    
    aian_raw = ifelse(aian_pop < pop_threshold | total_involved < incident_threshold, NA, aian_involved),
    
    api_raw = ifelse(api_pop < pop_threshold | total_involved < incident_threshold, NA, api_involved),
    
    latino_raw = ifelse(latino_pop < pop_threshold | total_involved < incident_threshold, NA, latino_involved),
    
    # Flipping the rate calc to get "NOT subject to USOF rate" to solve disparity calc issues, lots of zero rates, etc. - used only for disp calcs
    total_rate = ifelse(total_pop < pop_threshold | total_involved < incident_threshold, NA, ((total_pop * data_yrs) - total_raw) / (total_pop * data_yrs) * 100000),
    
    nh_white_rate = ifelse(nh_white_pop < pop_threshold | total_involved < incident_threshold, NA, ((nh_white_pop * data_yrs) - nh_white_raw) / (nh_white_pop * data_yrs) * 100000),
    
    black_rate = ifelse(black_pop < pop_threshold | total_involved < incident_threshold, NA, ((black_pop * data_yrs) - black_raw) / (black_pop * data_yrs) * 100000),
    
    aian_rate = ifelse(aian_pop < pop_threshold | total_involved < incident_threshold, NA, ((aian_pop * data_yrs) - aian_raw) / (aian_pop * data_yrs) * 100000),
    
    api_rate = ifelse(api_pop < pop_threshold | total_involved < incident_threshold, NA, ((api_pop * data_yrs) - api_raw) / (api_pop * data_yrs) * 100000),
    
    latino_rate = ifelse(latino_pop < pop_threshold | total_involved < incident_threshold, NA, ((latino_pop * data_yrs) - latino_raw) / (latino_pop * data_yrs) * 100000),
    
    # Use of Force Rates - will be displayed on RC.org
    total_rate_usof = ifelse(total_pop < pop_threshold | total_involved < incident_threshold, NA, round((total_raw / (total_pop * data_yrs)) * 100000,1)),
    
    nh_white_rate_usof = ifelse(nh_white_pop < pop_threshold | total_involved < incident_threshold, NA, round((nh_white_raw / (nh_white_pop * data_yrs)) * 100000,1)),
    
    black_rate_usof = ifelse(black_pop < pop_threshold | total_involved < incident_threshold, NA, round((black_raw / (black_pop * data_yrs)) * 100000,1)),
    
    aian_rate_usof = ifelse(aian_pop < pop_threshold | total_involved < incident_threshold, NA, round((aian_raw / (aian_pop * data_yrs)) * 100000,1)),
    
    api_rate_usof = ifelse(api_pop < pop_threshold | total_involved < incident_threshold, NA, round((api_raw / (api_pop * data_yrs)) * 100000,1)),
    
    latino_rate_usof = ifelse(latino_pop < pop_threshold | total_involved < incident_threshold, NA, round((latino_raw / (latino_pop * data_yrs)) * 100000,1)),
    
  ) 


# keep only selected columns
# filters out Census places that have no USOF data AND bad joins. Ex: SF city joins to both SF County and San Mateo County and this filter removes the San Mateo joined record.
d <- ungroup(df_screened) %>% select(!ends_with("involved")) %>% filter(!is.na(total_raw))  


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'. 
          #For USOF: For calcs, set to 'max' bc we use non-force rate. After calcs, we update 'max' to 'min' bc we display force rate where min is best.

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns. Drop unneeded cols.
state_table <- d[d$geoname == 'California', ] %>% select(-c(geolevel, county, pl_co))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)

state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#split COUNTY into separate table and format id, name columns. Drop unneeded cols.
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel, county, pl_co))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel, pl_co))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


#### EXTRA STEP RENAMING COLUMNS AND CHANGING ABBEST FROM 'MAX' TO 'MIN' DUE TO USING FLIPPED RATES IN DISPARITY CALCS, BUT DISPLAYING REGULAR RATES ON RC.ORG ####
    state_table <- state_table %>% rename_with(~paste0(., "_flipped"), ends_with("_rate")) %>% mutate(asbest = 'min')
    names(state_table) <- gsub("_rate_usof", "_rate", colnames(state_table))
    
    county_table <- county_table %>% rename_with(~paste0(., "_flipped"), ends_with("_rate")) %>% mutate(asbest = 'min')
    names(county_table) <- gsub("_rate_usof", "_rate", colnames(county_table))
    
    city_table <- city_table %>% rename_with(~paste0(., "_flipped"), ends_with("_rate")) %>% mutate(asbest = 'min')
    names(city_table) <- gsub("_rate_usof", "_rate", colnames(city_table))

###update info for postgres tables###
county_table_name <- "arei_crim_use_of_force_county_2023"
state_table_name <- "arei_crim_use_of_force_state_2023"
city_table_name <- "arei_crim_use_of_force_city_2023"
rc_schema <- 'v5'

indicator <- paste0("Created on ", Sys.Date(), ". Annual average number of people injured in Law Enforcement Use of force Incidents per 100,000 People over 6 years. Raw is total number of people injured over 6 years. Note, we use the flipped rates for disparity calcs, but display the regular rates on RC.org. This data is")
source <- "CADOJ 2016-2021 https://openjustice.doj.ca.gov/data"

#send tables to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)



