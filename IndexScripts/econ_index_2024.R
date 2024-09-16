#### Economic Opportunity (z-score) for RC v6 ####

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
source <- "American Community Survey (ACS) PUMS 2018-2022, American Community Survey (ACS) 2018-2022 Tables S2301 / S2802 / B19301B-I, and United Ways of California 2023"

issue <- 'economic_opportunity'

# Add indicators and arei_county_region_urban_type ------------------------------------------------------
####################### ADD COUNTY DATA #####################################
# you MUST update this section if we add or remove any indicators in an issue #

c_1 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_connected_youth_county_", rc_yr))
c_2 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_employment_county_", rc_yr))
c_3 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_internet_county_", rc_yr))
c_4 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_officials_county_", rc_yr))
c_5 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_per_capita_income_county_", rc_yr))
c_6 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_real_cost_measure_county_", rc_yr))
c_7 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_living_wage_county_", rc_yr))

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname1 <- 'connected'
varname2 <- 'employ'
varname3 <- 'internet'
varname4 <- 'officials'
varname5 <- 'percap'
varname6 <- 'realcost'
varname7 <- 'living wage'


region_urban_type <- st_read(con, query = paste0("SELECT geoid AS county_id, region, urban_type FROM ", rc_schema, ".arei_county_region_urban_type"))


# Clean data --------

### c1 
# use function to select cols we want, cap z-scores, and rename z-score cols
c_1 <- clean_data_z(c_1, varname1)

### c2
# use function to select cols we want and cap z-scores
c_2 <- clean_data_z(c_2, varname2)

### c3
# use function to select cols we want and cap z-scores
c_3 <- clean_data_z(c_3, varname3)

## c4
# use function to select cols we want and cap z-scores
c_4 <- clean_data_z(c_4, varname4)

## c5
# use function to select cols we want and cap z-scores
c_5 <- clean_data_z(c_5, varname5)

## c6 
# use function to select cols we want and cap z-scores
c_6 <- clean_data_z(c_6, varname6)

## c7 
# use function to select cols we want and cap z-scores
c_7 <- clean_data_z(c_7, varname7)


# Join Data Together ------------------------------------------------------
c_index <- full_join(c_1, c_2) 
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
c_index <- full_join(c_index, c_5)
c_index <- full_join(c_index, c_6)
c_index <- full_join(c_index, c_7)
colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names


# calculate z-scores. Will need to add threshold option to the calculate_z function
ind_threshold <- 3  # update depending on the number of indicators in the issue area
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
index_table_name <- paste0("arei_econ_index_", rc_yr)
index <- "Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is"

index_to_postgres(index_table, rc_schema)
dbDisconnect(con)
