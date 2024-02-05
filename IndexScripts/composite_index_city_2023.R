#### SCREENED Composite Index (z-score) for RC v5 ####
#### Same script as W:\Project\RACE COUNTS\2023_v5\Composite Index\composite_index_city_2023_draft.R EXCEPT this script uses "api_" tables and exports the unscreened index table

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgreSQL","sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# Packages ----------------------------------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(sf)
library(usethis)

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# Set source for index functions
source("W:/Project/RACE COUNTS/Functions/RC_Index_Functions.R")

# pull in cross-walk to go from district to city
crosswalk <- dbGetQuery(con, "SELECT  city_id, city_name, dist_id, total_enroll FROM v5.arei_city_county_district_table")

# pull in city_id and city_names
arei_race_multigeo <- dbGetQuery(con, "SELECT geoid, name, geolevel, total_pop FROM v5.arei_race_multigeo") %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

# pull in list of all tables in current racecounts schema
curr_schema <- "v5"  # update each year
curr_yr <- "2023"
table_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = curr_schema))$table, function(x) slot(x, 'name'))))

######################### get SCREENED indicator table names ######################### ------------------------------------
### Filter so we can pull z-scores from the "api_" city/dist indicator tables that contain calcs only for the cities that pass the total_pop threshold screen.
rc_list <- filter(table_list, grepl("api_",table))



######################### CITY INDEX CALCS ########################### ---------------------------
# pull and run calcs from separate script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/IndexScripts/composite_index_city_calcs_2023.R")



# Export SCREENED index to postgres ------------------------------------------------------
table_name <- "arei_composite_index_city_2023"
table_schema <- "v5"
table_comment_source <- "This is the SCREENED city index table including pop screen and threshold for representation across all issue areas and indicators.
The UNSCREENED index is: v5.arei_composite_index_city_2023_draft
R script: W://Project//RACE COUNTS//2023_v5//RC_Github/RaceCounts/IndexScripts//composite_index_city_2023.R 
QA document: W://Project/RACE COUNTS//2023_v5//Composite Index//Documentation//QA_sheet_Composite_Index_City.docx" 

# send city index and comment to postgres
#city_index_to_postgres(city_index)


dbDisconnect(con)  
