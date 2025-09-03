#### Composite Index (z-score) for RC v7 ####
###### This script produces arei_composite_index_city_[year]_draft and arei_composite_index_city_[year] tables

#install packages if not already installed
packages <- c("tidyverse","RPostgres","sf","usethis")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen = 100) 

# Load PostgreSQL driver and databases & set sources --------------------------------------------------
source("W:\\RDA Team\\R\\credentials_source.R")
source("./Functions/RC_Index_Functions.R")

con <- connect_to_db("racecounts")


############### SPECIFY WHICH CITY INDEX TO RUN ##################
#index_type <- 'arei_'   # DRAFT city index (all cities)
index_type <- 'api_'     # FINAL city index (screened cities)


# update each yr
rc_yr <- '2025'
rc_schema <- 'v7'
qa_filepath <- 'W:\\Project\\RACE COUNTS\\2025_v7\\Composite Index\\QA_Sheet_City_Index.docx'

## Set issue and indicator thresholds
crim_threshold <- 1
demo_threshold <- 1
econ_threshold <- 2 # could be 3
educ_threshold <- 2
hlth_threshold <- 1
hben_threshold <- 2
hous_threshold <- 3

issue_area_threshold <- 4 # city must have at least 3 issue perf/disp z-scores or it will be suppressed
indicator_threshold <- 13 # city must have data for at least 13 indicators or it will be suppressed

# pull in list of all tables in current racecounts schema
table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", rc_schema, "' AND (table_name LIKE '%city_", rc_yr, "' OR table_name LIKE '%district_", rc_yr, "') AND table_name NOT LIKE '%index%';")
rc_list <- dbGetQuery(con, table_list) %>% rename('table' = 'table_name')

######################### get SCREENED indicator table names ######################### ------------------------------------
### Filter so we can pull z-scores from only the specified type (api or arei) of city/dist indicator tables
rc_list <- filter(rc_list, grepl(index_type,table))
print(rc_list) # check indicator table list is correct and complete

######################### CITY INDEX CALCS ########################### ---------------------------
# pull in cross-walk to go from district to city
crosswalk <- dbGetQuery(con, paste0("SELECT  city_id, city_name, dist_id, total_enroll FROM ", rc_schema, ".arei_city_county_district_table"))

# pull in city_id and city_names
arei_race_multigeo <- dbGetQuery(con, paste0("SELECT geoid, name, geolevel, total_pop FROM ", rc_schema, ".arei_race_multigeo")) %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)


# pull and run calcs from separate script
source(paste0("./IndexScripts/composite_index_city_calcs_", rc_yr, ".R"))  # filepath will auto-update each year
# city_index df is the final result that should be exported to postgres

# Export SCREENED index to postgres ------------------------------------------------------
table_name <- case_when (index_type == 'arei_' ~ paste0("arei_composite_index_city_", rc_yr, "_draft"),
                         index_type == 'api_' ~ paste0("arei_composite_index_city_", rc_yr))
table_comment_source <- case_when (index_type == 'arei_' ~ paste0("This is the UNSCREENED city index table including screening only for representation across all issue areas and indicators.
                                      R script: W://Project//RACE COUNTS//", rc_yr, "_", rc_schema, "//RC_Github//RaceCounts//IndexScripts//composite_index_city_", rc_yr, ".R 
                                      QA document: ", qa_filepath),
                                   index_type == 'api_' ~ paste0("This is the SCREENED city index table including screening for population and for representation across all issue areas and indicators. 
                                      R script: W://Project//RACE COUNTS//", rc_yr, "_", rc_schema, "//RC_Github//RaceCounts//IndexScripts//composite_index_city_", rc_yr, ".R 
                                      QA document: ", qa_filepath))


# send city index and comment to postgres
#city_index_to_postgres(city_index)


dbDisconnect(con)  