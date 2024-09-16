#### Housing Index (z-score) for RC v6 ####

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
source <- "CA Dept of Education (2022-23), US Department of Housing and Urban Development (HUD) Comprehensive Housing Affordability Strategy (CHAS) data (2016-2020), Home Mortgage Disclosure Act (HMDA) (Denied Mortgage 2019-2022) (Subprime Mortgage 2013-2017), The Eviction Lab at Princeton University (2014-2017), DataQuick (2017-2021), and AMERICAN COMMUNITY SURVEY 5-YEAR ESTIMATES, TABLES B25003B-I (2018-2022), B25014B-I, DP05, and PUMS (2018-2022) "

issue <- 'housing'

# Add indicators and arei_county_region_urban_type ------------------------------------------------------
####################### ADD COUNTY DATA #####################################
# you must update this section if we add or remove any indicators in an issue #

c_1 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_cost_burden_owner_county_", rc_yr))
c_2 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_cost_burden_renter_county_", rc_yr))
c_3 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_denied_mortgages_county_", rc_yr))
c_4 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_eviction_filing_rate_county_", rc_yr)) 
c_5 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_foreclosure_county_", rc_yr))
c_6 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_homeownership_county_", rc_yr))
c_7 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_overcrowded_county_", rc_yr))
c_8 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_housing_quality_county_", rc_yr))
c_9 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_student_homelessness_county_", rc_yr))
c_10 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_subprime_county_", rc_yr))

## define variable names for clean_data_z function. you MUST UPDATE for each issue area. Copy from v3 index view.
varname1 <- 'burden_own'
varname2 <- 'burden_rent'
varname3 <- 'denied'
varname4 <- 'eviction'
varname5 <- 'forecl'
varname6 <- 'homeown'
varname7 <- 'overcrowded'
varname8 <- 'quality'
varname9 <- 'homeless'
varname10 <- 'subprime'


region_urban_type <- st_read(con, query = paste0("SELECT geoid AS county_id, region, urban_type FROM ", rc_schema, ".arei_county_region_urban_type"))


# Clean data --------
# use function to select cols we want, cap z-scores, and rename z-score cols

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

## c6 
c_6 <- clean_data_z(c_6, varname6)

### c7
c_7 <- clean_data_z(c_7, varname7)

## c8
c_8 <- clean_data_z(c_8, varname8)

## c9
c_9 <- clean_data_z(c_9, varname9)

## c10 
c_10 <- clean_data_z(c_10, varname10)


# Join Data Together ------------------------------------------------------
c_index <- full_join(c_1, c_2) 
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
c_index <- full_join(c_index, c_5)
c_index <- full_join(c_index, c_6)
c_index <- full_join(c_index, c_7)
c_index <- full_join(c_index, c_8)
c_index <- full_join(c_index, c_9)
c_index <- full_join(c_index, c_10)
colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names

# calculate z-scores. Will need to add threshold option to the calculate_z function
ind_threshold <- 5  # update depending on the number of indicators in the issue area
c_index <- calculate_z(c_index)

# merge region and urban type from current arei_county_region_urban_type
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
index_table_name <- paste0("arei_hous_index_", rc_yr)
index <- "Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is"

index_to_postgres(index_table, rc_schema)
dbDisconnect(con)









