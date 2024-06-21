#### Crime and Justice (z-score) for RC v6 ####

#install packages if not already installed
packages <- c("tidyverse","RPostgreSQL","sf","usethis")  

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

# udpate each yr
rc_yr <- '2024'
rc_schema <- 'v6'
source <- "California Health Interview Survey (CHIS) (2011-2022), Vera Institute of Justice (2018), California Department of Justice Open Justice Data (CADOJ) (2016-2022 USOF), (2011-2022 Status Offenses), and (2022 RIPA stops) and American Community Survey (ACS) 5-Year Estimates, Tables B01001B/H/I and DP05 (2018-2022)"

issue <- 'crime_and_justice'

# Add indicators and arei_multigeo_list ------------------------------------------------------
####################### ADD COUNTY DATA #####################################
# you must update this section if we add or remove any indicators in an issue #

c_1 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_crim_incarceration_county_", rc_yr))
c_2 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_crim_perception_of_safety_county_", rc_yr))
c_3 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_crim_status_offenses_county_", rc_yr))
c_4 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_crim_use_of_force_county_", rc_yr))
c_5 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_crim_officer_initiated_stops_county_", rc_yr))

region_urban_type <- st_read(con, query = paste0("SELECT geoid AS county_id, region, urban_type FROM ", rc_schema, ".arei_multigeo_list"))

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname1 <- 'incarceration'
varname2 <- 'safety'
varname3 <- 'offenses'
varname4 <- 'force'
varname4 <- 'stops'

# Clean data --------
## use function to select cols we want, cap z-scores, and rename z-score cols

### c1 
c_1 <- clean_data_z(c_1, varname1)

### c2
c_2 <- clean_data_z(c_2, varname2)

### c3
c_3 <- clean_data_z(c_3, varname3)

## c4
c_4 <- clean_data_z(c_4, varname4)

## c5
c_5 <- clean_data_z(c_5, varname5)

# Join Data Together ------------------------------------------------------
c_index <- full_join(c_1, c_2) 
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
c_index <- full_join(c_index, c_5)

colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names

# calculate z-scores. Will need to add threshold option to the calculate_z function
ind_threshold <- 2  # update depending on the number of indicators in the issue area
c_index <- calculate_z(c_index)

# merge region and urban type from current arei_multigeo_list
c_index <- left_join(c_index, region_urban_type)

# rename columns
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("_rank"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("performance_z"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("disparity_z"))

# select/reorder final columns for index table
index_table <- c_index %>% select(county_id, county_name, region, urban_type, ends_with("_rank"), quadrant, disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("performance_z"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 
index_table_name <- paste0("arei_crim_index_", rc_yr)
index <- "Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is"

#index_to_postgres(index_table, rc_schema)
#dbDisconnect(con)
	
