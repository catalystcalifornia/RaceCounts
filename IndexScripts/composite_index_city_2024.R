#### SCREENED Composite Index (z-score) for RC v6 ####
#### Same script as W:\Project\RACE COUNTS\2024_v6\Composite Index\composite_index_city_2024_draft.R EXCEPT this script uses "api_" tables and exports the unscreened index table

#install packages if not already installed
packages <- c("tidyverse","RPostgreSQL","sf","here","usethis")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

# Load PostgreSQL driver and databases --------------------------------------------------
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# Set Source for Index Functions script -----------------------------------
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Index_Functions.R")

# remove exponentiation
options(scipen = 100) 

# update each yr
rc_yr <- '2024'
rc_schema <- 'v6'


# pull in list of all tables in current racecounts schema
table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", rc_schema, "' AND (table_name LIKE '%city_", rc_yr, "' OR table_name LIKE '%district_", rc_yr, "') AND table_name NOT LIKE '%index%';")
rc_list <- dbGetQuery(con, table_list) %>% rename('table' = 'table_name')

######################### get SCREENED indicator table names ######################### ------------------------------------
### Filter so we can pull z-scores from the "api_" city/dist indicator tables that contain calcs only for the cities that pass the total_pop threshold screen.
rc_list <- filter(rc_list, grepl("api_",table))
print(rc_list) # check indicator table list is correct and complete

######################### CITY INDEX CALCS ########################### ---------------------------
# pull in cross-walk to go from district to city
crosswalk <- dbGetQuery(con, paste0("SELECT  city_id, city_name, dist_id, total_enroll FROM ", rc_schema, ".arei_city_county_district_table"))

# pull in city_id and city_names
arei_race_multigeo <- dbGetQuery(con, paste0("SELECT geoid, name, geolevel, total_pop FROM ", rc_schema, ".arei_race_multigeo")) %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)


# pull and run calcs from separate script
# filepath will auto-update each year
source(here("IndexScripts", paste0("composite_index_city_calcs_", rc_yr, ".R")))
# city_index df is the final result that should be exported to postgres


# Export SCREENED index to postgres ------------------------------------------------------
table_name <- paste0("arei_composite_index_city_", rc_yr)
table_comment_source <- paste0("Created ", Sys.Date(), ". This is the SCREENED city index table including pop screen and threshold for representation across all issue areas and indicators.
The UNSCREENED index has same name with _draft at the end. 
R script: W://Project//RACE COUNTS//", rc_yr, "_", rc_schema, "//RC_Github//RaceCounts//IndexScripts//composite_index_city_", rc_yr, ".R 
QA document: W://Project//RACE COUNTS//", rc_yr, "_", rc_schema, "//Composite Index//Documentation//QA_sheet_Composite_Index_City.docx") 


# send city index and comment to postgres
#city_index_to_postgres(city_index)


dbDisconnect(con)  
