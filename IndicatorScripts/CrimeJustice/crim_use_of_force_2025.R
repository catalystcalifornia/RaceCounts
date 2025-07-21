## Use of Force 2016-2022 RC v7

## Set up ----------------------------------------------------------------
#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse", "RPostgreSQL", "tidycensus", "readxl", "sf", "janitor", "stringr", "data.table", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

## packages
library(tidyverse)
library(readxl)
library(RPostgres) 
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

# Update each year
curr_yr <- '2016-2023'
rc_yr <- '2025'
rc_schema <- 'v7'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Crime and Justice\\QA_Sheet_Use_of_Force.docx"

############### PREP LATEST URSUS RDA_SHARED_DATA TABLES ########################
metadata <- "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-07/use-of-force-readme-06202024f.pdf" # update each year
##### civilian-officer
filepath = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-07/UseofForce_Civilian-Officer_2023.csv" # update each year
fieldtype = 1:26  # confirm using metadata link
 
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "crime_and_justice"
table_name <- paste0("ursus_civilian_officer_", substr(curr_yr, 6,9))
table_comment_source <- "NOTE: race/eth column values have inconsistencies, for example ''''asian indian'''' and ''''asian_indian''''."
table_source <- paste0("Use of force data downloaded ", Sys.Date(), " from https://openjustice.doj.ca.gov/data. Metadata here: ", metadata, 
                        " and saved here: W:/Data/Crime and Justice/Police Violence/Open Justice/", substr(curr_yr, 6,9))

#function no longer works, unable to fix it
#source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")

df <- read_csv(file = filepath, na = c("*", ""))

#format column names
names(df) <- tolower(names(df)) # make col names lowercase
df <- df %>% mutate_all(as.character) # make all data characters

  ##  WRITE TABLE TO POSTGRES DB ##               NOTE: con2 must be rda_shared_data for function to work.
  # make character vector for field types in postgres table
  charvect = rep('numeric', dim(df)[2]) 
  charvect[fieldtype] <- "varchar" # specify which cols are varchar, the rest will be numeric
  
  # add names to the character vector
  names(charvect) <- colnames(df)
  
  dbWriteTable(con2, Id(table_schema, table_name), df,
               overwrite = FALSE, row.names = FALSE,
               field.types = charvect)

  # comment on table
  indicator <- "Use of Force Civilians and Officers 2023"
  column_names <- colnames(df)
  column_comments <- ""
  
  # add comment on table and columns using add_table_comments() (accessed via credentials script) 
  add_table_comments(con2, table_schema, table_name, indicator, table_source, qa_filepath, column_names, column_comments) 
  
  
  

# ##### incident
# filepath = "https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2023-06/UseofForce_Incident_2022.csv" # update each year
# fieldtype = 1:14  # confirm using metadata link
# 
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "crime_and_justice"
# table_name <- paste0("ursus_incident_", substr(curr_yr, 6,9))
# table_comment_source <- "NOTE: This table has 1 row per incident with total # of civilians involved in Use of Force incident. Tables like ursus_civilian_officer_2016, have 1 row per civilian involved in an incident. So if you join the tables, then sum the num_involved_civilians field, you will double-count people."
# table_source <- paste0("Use of force data downloaded ", Sys.Date(), " from https://openjustice.doj.ca.gov/data. Metadata saved here: W:/Data/Crime and Justice/Police Violence/Open Justice/", substr(curr_yr, 6,9))
# 
# ## Run function to prep and export rda_shared_data table. 
# ### NOTE: con2 must be rda_shared_data for function to work.
# source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
# df <- get_ursus_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db


############### Gather all URSUS data years --------------------------------------------------------------
### Pull in URSUS data from rda_shared_data.crime_and_justice 
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

# Combine all data years 
year_list <- lapply(ursus_tables, function(x) x %>% select(year)) %>% rbindlist() %>% unique() # will autoupdate based on which years of data you have in ursus_tables list
year_list <- year_list[['year']] # convert to vector

prep_ursus <- function(old.x) {
  table1 = eval(parse(text=paste0("ursus_civilian_officer_", old.x)))
  table2 = eval(parse(text=paste0("ursus_incident_", old.x)))
  if(!'state_name' %in% names(table2)) table2 <- table2 %>% add_column(state_name = 'CA') # add state_name col if doesn't exist bc in some tables col is called 'state' not 'state_name'
  if(!'incident_id' %in% names(table1)) table1 <- table1 %>% rename(incident_id = incident_code) # rename incident_code col to incident_id bc in some tables col is called 'incident_code' not 'incident_id'
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
# check row counts to see if loop/fx worked correctly
#join_check <- df_all_years %>% group_by(year) %>% mutate(involved = n()) %>% summarise(total_involved = min(involved))

# Calculate counts by race: Number of incidents across all data years (total and by race) included NOT average per year ---------------------------------------------------------
#### NOTE: There is 1 row per civilian involved in each incident, but each of those rows reports the total # involved and has the same incident ID.
##### So if you sum the num_involved_civilians (which is the total # per incident) column, you double/triple-count folks etc.
######### NOTE: Due to using our custom race groups, some people are double-counted. Ex: Someone who id's as more than 1 group will be counted in both groups.

# create new race columns for our custom groups, you may need to update this code for race_reclass df based on 'races' df created below
# races <- unique(df_all_years$race_ethnic_group)  # check which race groups are present in data #
# sort(races) # get list of unique race/ethnic groups
race_reclass <- df_all_years %>% mutate(nh_white = ifelse(race_ethnic_group == 'white', 'nh_white', 'not nh_white'), 
                                        nh_black = ifelse(race_ethnic_group == 'black', 'nh_black', 'not_black'),
                                        nh_aian = ifelse(grepl('american indian', race_ethnic_group), 'nh_aian', 'not nh_aian'), 
                                        nh_pacisl = ifelse(grepl('islander', race_ethnic_group), 'nh_pacisl', 'not nh_pacisl'),
                                        nh_asian = ifelse(race_ethnic_group == 'asian' | race_ethnic_group == 'asian indian', 'nh_asian', 'not nh_asian'),
                                        latino = ifelse(grepl('hispanic', race_ethnic_group), 'latino', 'not latino'),
                                        nh_twoormor = ifelse(grepl(',', race_ethnic_group), 'nh_twoormor', 'not twoormor'),
                                        city = trimws(city)) # remove leading/trailing spaces for match on geoname with ACS later 
race_reclass$nh_twoormor = ifelse(grepl('hispanic', race_reclass$race_ethnic_group), 'not twoormor', race_reclass$nh_twoormor)

#### Fix city names that should match to ACS later on in script where possible #### Manually look up places with USOF data that do not match to ACS pop data ####
######## THIS MANUAL RESEARCH & CLEANING PROCESS MUST BE REVIEWED/UPDATED EACH TIME WE PREP THIS DATA 
la_nhood <- race_reclass$city %in% c("Canoga Park", "Chatsworth", "North Hills", "Pacoima", "Panorama City", "Playa Del Rey", "San Pedro", "Sherman Oaks", "Studio City", "Sylmar", "Tujunga", "Van Nuys", "West Hills", "Woodland Hills")
race_reclass$city[la_nhood] <- "Los Angeles"
race_reclass <- race_reclass %>% mutate(city = ifelse(city=='City Of Industry', 'Industry', city)) %>%
  mutate(city = ifelse(city=='Ventura', 'San Buenaventura (Ventura)', city)) %>%
  mutate(city = ifelse(city=='Rivererside', 'Riverside', city)) %>%
  mutate(city = ifelse(city=='Saint Helena', 'St. Helena', city)) %>%
  mutate(city = ifelse(city=='White Water', 'Whitewater', city)) %>%
  mutate(city = ifelse(city=='Cottonwood' & county == 'Shasta', 'Cottonwood CDP (Shasta County)', city)) %>%
  mutate(city = ifelse(city=='Lakeside' & county == 'San Diego', 'Lakeside CDP (San Diego County)', city)) %>%
  mutate(city = ifelse(city=='Spring Valley' & county == 'San Diego', 'Spring Valley CDP (San Diego County)', city)) %>%
  mutate(city = ifelse(city=='Tahoe City', 'Sunnyside-Tahoe City', city)) %>%
  mutate(city = ifelse(city=='Paso Robles', 'El Paso de Robles (Paso Robles)', city)) %>%
  mutate(city = ifelse(city=='Brownsville', 'Challenge-Brownsville', city)) %>%
  mutate(city = ifelse(city=='Mcfarland', 'McFarland', city)) %>%
  mutate(city = ifelse(city=='Mcarthur', 'McArthur', city)) %>%
  mutate(city = ifelse(city=='Mckinleyville', 'McKinleyville', city)) %>%
  mutate(city = ifelse(city=='Marina Del Rey', 'Marina del Rey', city)) %>%
  mutate(city = ifelse(city=='El Sobrante' & county == 'Contra Costa', 'El Sobrante CDP (Contra Costa County)', city)) %>%
  mutate(city = ifelse(city=='View Park', 'View Park-Windsor Hills', city))       

#### Calc counts by race ####
calc_counts <- function(race_eth) {
  counts <- race_reclass %>% filter(race_reclass[[race_eth]] == race_eth) %>% group_by(city, county) %>% mutate(involved = n()) %>% summarise(involved = min(involved))
  new_name <- paste0(race_eth, '_involved')  # generate race-specific col name
  counts <- counts %>% rename(!!new_name := involved)  # rename to race-specific col name
  return(counts)  
}

# City: calc counts by race
total_ <- race_reclass %>% group_by(city, county) %>% mutate(total_involved = n()) %>% summarise(total_involved = min(total_involved))
nh_black_ <- calc_counts('nh_black')
nh_aian_ <- calc_counts('nh_aian')
nh_pacisl_ <- calc_counts('nh_pacisl')
nh_asian_ <- calc_counts('nh_asian')
nh_white_ <- calc_counts('nh_white')
latino_ <- calc_counts('latino')
nh_twoormor_ <- calc_counts('nh_twoormor')

## join city calcs by race together
df_city <- total_ %>% left_join(nh_pacisl_) %>% left_join(nh_asian_) %>% left_join(nh_black_) %>% left_join(nh_aian_) %>% left_join(nh_white_) %>% left_join(latino_) %>% left_join(nh_twoormor_) %>% rename("geoname" = "city") %>% mutate(geolevel='city')


# County: calc counts by race
df_county <- df_city %>% group_by(county) %>% summarise(across(ends_with("involved"), sum, na.rm=TRUE)) %>% # here we can sum not count bc df_city has unduplicated counts
  rename("geoname" = "county")

# State: calc counts by race
df_county <- df_county %>% adorn_totals("row") %>% as.data.frame(df_county)
df_county$geoname[df_county$geoname == 'Total'] <- 'California'
df_county$geolevel <- ifelse(df_county$geoname == 'California', 'state', 'county')

# join city, county, state together
df_all <- rbind(df_city, df_county) %>% relocate(geolevel, .after = county)

# make NA = 0 for cities/counties that appear in USOF data. Places that are not in the USOF data still receive NA.
df_all <- df_all %>% mutate(total_involved = coalesce(total_involved, 0), nh_pacisl_involved = coalesce(nh_pacisl_involved, 0), nh_black_involved = coalesce(nh_black_involved, 0), nh_aian_involved = coalesce(nh_aian_involved, 0), nh_asian_involved = coalesce(nh_asian_involved, 0),
                            nh_white_involved = coalesce(nh_white_involved, 0), latino_involved = coalesce(latino_involved, 0), nh_twoormor_involved = coalesce(nh_twoormor_involved, 0))

# Summarize Pomona bc it throws an error in d <- calc_diff(d) later. Pomona is listed in 2 different counties LA and SB Counties in the data, but is actually in LAC.
df_all <- df_all %>% filter(geoname != "Pomona") %>% bind_rows(
  df_all %>% filter(geoname == "Pomona") %>% summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE))) %>%
    mutate(county = "Los Angeles", geolevel = "city") %>%
    select(geoname, county, everything())
) %>% arrange(geoname)

# Summarize American Canyon bc it throws an error in d <- calc_diff(d) later. American Canyon has 2 listings, 1 with a typo.
df_all <- df_all %>% mutate(geoname = ifelse(geoname == 'American Cyn', 'American Canyon', geoname)) 
df_all <- df_all %>% filter(geoname != "American Canyon") %>% bind_rows(
  df_all %>% filter(geoname == "American Canyon") %>% summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE))) %>%
    mutate(county = "Napa", geolevel = "city") %>%
    select(geoname, county, everything())
) %>% arrange(geoname)

# Get ACS population data ----------------------------------------------------
dp05 <- dbGetQuery(con, "SELECT * FROM v6.arei_race_multigeo") %>% mutate(name = gsub(" County, California", "", name),
                                                                          name =  gsub(" CDP, California", "", name),
                                                                          name =  gsub(" city, California", "", name),
                                                                          name =  gsub(" town, California", "", name),
                                                                          name =  gsub(", California", "", name)) %>% 
  select(-c(contains(c("swana", "nh_other_", "aian", "pacisl", "pct_"))))
nh_aian_pacisl <- dbGetQuery(con2, "SELECT geoid, dp05_0081e AS nh_aian_pop, dp05_0083e as nh_pacisl_pop FROM demographics.acs_5yr_dp05_multigeo_2022 WHERE geolevel IN ('state', 'county', 'place')")
dp05 <- dp05 %>% full_join(nh_aian_pacisl)


city_county <- st_read(con2, query = "SELECT place_geoid, county_name FROM crosswalks.county_place_2020")

# clean up name col
dp05$name <- gsub(" County, California", "", dp05$name)
dp05$name <- gsub(" CDP, California", "", dp05$name)
dp05$name <- gsub(" town, California", "", dp05$name)
dp05$name <- gsub(" City city, California", " City", dp05$name)
dp05$name <- gsub(" city, California", "", dp05$name)
dp05$name <- gsub(", California", "", dp05$name)
names(dp05) <- tolower(names(dp05))
# note: CDP names have not been modified and will not match, in next section we check to see if any of them actually have USOF data and make appropriate fixes ~line 138

dp05_ <- dp05 %>% left_join(city_county, by = c("geoid" = "place_geoid")) %>% mutate(county_name = gsub(" County", "", county_name), geolevel = gsub("place", "city", geolevel)) %>%
  rename(geoname = name, county = county_name) # Foresta is the only city that doesn't match to a county, it is in a nat'l park so we can exclude


# Join USOF and Pop Data to check for unmatched cities --------------------------------------------------
# df_all_ <- full_join(df_all, dp05_, by = c("geoname", "geolevel")) %>% arrange(geoname) %>% select(geoid, geoname, everything())
# check if there are places with USOF data that did not match to ACS place, but should
# usof_nomatch <- filter(df_all_, is.na(geoid)) # this df had 40 rows before manual edits ~line 138
# View(usof_nomatch)
# acs_nomatch <- filter(df_all_, (is.na(total_involved) & geolevel == 'place')) # this df should have 0 rows
# View(acs_nomatch)

# Re-join USOF and pop data after manual fixes
df_calcs <- full_join(df_all, dp05_, by = c("geoname", "geolevel", "county")) %>% arrange(geoname) %>% select(geoid, geoname, everything())
df_calcs$geoname <- gsub(" CDP", "", df_calcs$geoname)
# usof_nomatch_final <- filter(df_all_, is.na(geoid)) # check if manual fixes worked: this df should have 21 unmatched
# View(usof_nomatch_final)

# Data screening / calc rates ----------------------------------------------------------
pop_threshold = 100
incident_threshold = 5  # update appropriately each year to ensure counties with few incidents and small pops do not result in outlier rates
data_yrs = length(unique(year_list))  # auto updates based on number of data yrs in ursus_tables. you must multiply pop by this number to get accurate annual avg rate. raw is the sum of incidents across yrs, not annual avg.

df_screened <- df_calcs %>% 
  mutate(
    # screen raw counts
    total_raw = ifelse(total_pop < pop_threshold | total_involved < incident_threshold, NA, total_involved),
    
    nh_white_raw = ifelse(nh_white_pop < pop_threshold | total_involved < incident_threshold, NA, nh_white_involved),
    
    nh_black_raw = ifelse(nh_black_pop < pop_threshold | total_involved < incident_threshold, NA, nh_black_involved),
    
    nh_aian_raw = ifelse(nh_aian_pop < pop_threshold | total_involved < incident_threshold, NA, nh_aian_involved),
    
    nh_pacisl_raw = ifelse(nh_pacisl_pop < pop_threshold | total_involved < incident_threshold, NA, nh_pacisl_involved),
    
    nh_asian_raw = ifelse(nh_asian_pop < pop_threshold | total_involved < incident_threshold, NA, nh_asian_involved),
    
    latino_raw = ifelse(latino_pop < pop_threshold | total_involved < incident_threshold, NA, latino_involved),
    
    nh_twoormor_raw = ifelse(nh_twoormor_pop < pop_threshold | total_involved < incident_threshold, NA, nh_twoormor_involved),
    
    # Screen and flip the rate calc to get "NOT subject to USOF rate" to solve disparity calc issues, lots of zero rates, etc. - used only for disp calcs
    total_rate = ifelse(total_pop < pop_threshold | total_involved < incident_threshold, NA, ((total_pop * data_yrs) - total_raw) / (total_pop * data_yrs) * 100000),
    
    nh_white_rate = ifelse(nh_white_pop < pop_threshold | total_involved < incident_threshold, NA, ((nh_white_pop * data_yrs) - nh_white_raw) / (nh_white_pop * data_yrs) * 100000),
    
    nh_black_rate = ifelse(nh_black_pop < pop_threshold | total_involved < incident_threshold, NA, ((nh_black_pop * data_yrs) - nh_black_raw) / (nh_black_pop * data_yrs) * 100000),
    
    nh_aian_rate = ifelse(nh_aian_pop < pop_threshold | total_involved < incident_threshold, NA, ((nh_aian_pop * data_yrs) - nh_aian_raw) / (nh_aian_pop * data_yrs) * 100000),
    
    nh_pacisl_rate = ifelse(nh_pacisl_pop < pop_threshold | total_involved < incident_threshold, NA, ((nh_pacisl_pop * data_yrs) - nh_pacisl_raw) / (nh_pacisl_pop * data_yrs) * 100000),
    
    nh_asian_rate = ifelse(nh_asian_pop < pop_threshold | total_involved < incident_threshold, NA, ((nh_asian_pop * data_yrs) - nh_asian_raw) / (nh_asian_pop * data_yrs) * 100000),
    
    latino_rate = ifelse(latino_pop < pop_threshold | total_involved < incident_threshold, NA, ((latino_pop * data_yrs) - latino_raw) / (latino_pop * data_yrs) * 100000),
    
    nh_twoormor_rate = ifelse(nh_twoormor_pop < pop_threshold | total_involved < incident_threshold, NA, ((nh_twoormor_pop * data_yrs) - nh_twoormor_raw) / (nh_twoormor_pop * data_yrs) * 100000),
    
    # Screen and calc Use of Force Rates - will be displayed on RC.org
    total_rate_usof = ifelse(total_pop < pop_threshold | total_involved < incident_threshold, NA, round((total_raw / (total_pop * data_yrs)) * 100000,1)),
    
    nh_white_rate_usof = ifelse(nh_white_pop < pop_threshold | total_involved < incident_threshold, NA, round((nh_white_raw / (nh_white_pop * data_yrs)) * 100000,1)),
    
    nh_black_rate_usof = ifelse(nh_black_pop < pop_threshold | total_involved < incident_threshold, NA, round((nh_black_raw / (nh_black_pop * data_yrs)) * 100000,1)),
    
    nh_aian_rate_usof = ifelse(nh_aian_pop < pop_threshold | total_involved < incident_threshold, NA, round((nh_aian_raw / (nh_aian_pop * data_yrs)) * 100000,1)),
    
    nh_pacisl_rate_usof = ifelse(nh_pacisl_pop < pop_threshold | total_involved < incident_threshold, NA, round((nh_pacisl_raw / (nh_pacisl_pop * data_yrs)) * 100000,1)),
    
    nh_asian_rate_usof = ifelse(nh_asian_pop < pop_threshold | total_involved < incident_threshold, NA, round((nh_asian_raw / (nh_asian_pop * data_yrs)) * 100000,1)),
    
    latino_rate_usof = ifelse(latino_pop < pop_threshold | total_involved < incident_threshold, NA, round((latino_raw / (latino_pop * data_yrs)) * 100000,1)),
    
    nh_twoormor_rate_usof = ifelse(nh_twoormor_pop < pop_threshold | total_involved < incident_threshold, NA, round((nh_twoormor_raw / (nh_twoormor_pop * data_yrs)) * 100000,1))
    
  )

# keep only selected columns and places with non-NA _raw values
d <- ungroup(df_screened) %>% select(!ends_with("involved")) %>% select(!starts_with("nh_twoormor")) %>% filter(!is.na(total_raw))


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'. 
#For USOF: For calcs, set to 'max' bc we use non-force rate. After calcs, we update 'max' to 'min' bc we display force rate where min is best.

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns. Drop unneeded cols.
state_table <- d[d$geoname == 'California', ] %>% select(-c(geolevel, county))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#split COUNTY into separate table and format id, name columns. Drop unneeded cols.
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel, county))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel, county))

#calculate city z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


#### EXTRA STEP RENAMING COLUMNS AND CHANGING ABBEST FROM 'MAX' TO 'MIN' DUE TO USING FLIPPED RATES IN DISPARITY CALCS, BUT DISPLAYING REGULAR RATES ON RC.ORG ####
usof_flip <- function(table) {
  table <- table %>% rename_with(~paste0(., "_flipped"), ends_with("_rate")) %>% mutate(asbest = 'min')
  names(table) <- gsub("_rate_usof", "_rate", colnames(table))
  return(table)
}

state_table  <- usof_flip(state_table)
county_table <- usof_flip(county_table)
city_table   <- usof_flip(city_table)


###update info for postgres tables will automatically update###
county_table_name <- paste0("arei_crim_use_of_force_county_", rc_yr)
state_table_name <- paste0("arei_crim_use_of_force_state_", rc_yr)
city_table_name <- paste0("arei_crim_use_of_force_city_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Annual average number of people injured in Law Enforcement Use of force Incidents per 100,000 People over ", data_yrs," years. Raw is total number of people injured over ", data_yrs," years. Note, we use the flipped rates for disparity calcs, but display the regular rates on RC.org. This data is")
source <- paste0("CADOJ ",curr_yr, " https://openjustice.doj.ca.gov/data")

#send tables to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)

dbDisconnect(con)
dbDisconnect(con2)

