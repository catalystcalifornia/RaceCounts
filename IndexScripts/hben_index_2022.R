#### Healthy Built Environment Index (z-score) for RC v4 ####

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgreSQL","sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)


# Packages ----------------------------------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(sf)


# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")


# Add HBEN indicators and arei_multigeo_list ------------------------------------------------------
####################### ADD COUNTY DATA #####################################
c_1 <- st_read(con, query = "SELECT * FROM v4.arei_hben_drinking_water_county_2022")
c_2 <- st_read(con, query = "SELECT * FROM v4.arei_hben_food_access_county_2022")
c_3 <- st_read(con, query = "SELECT * FROM v4.arei_hben_haz_weighted_avg_county_2022")
c_4 <- st_read(con, query = "SELECT * FROM v4.arei_hben_toxic_release_county_2022")
c_5 <- st_read(con, query = "SELECT * FROM v4.arei_hben_asthma_county_2022")
c_6 <- st_read(con, query = "SELECT * FROM v4.arei_hben_lack_of_greenspace_county_2022")

region_urban_type <- st_read(con, query = "SELECT geoid AS county_id, region, urban_type FROM v4.arei_multigeo_list")

## define variable names for clean_data_z function. you MUST UPDATE for each issue area. Copy from v3 index view.
varname1 <- 'water'
varname2 <- 'food'
varname3 <- 'hazard'
varname4 <- 'toxic'
varname5 <- 'asthma'
varname6 <- 'green'

# Set Source for Index Functions script -----------------------------------
source("W:/Project/RACE COUNTS/Functions/RC_Index_Functions.R")

# remove exponentiation
options(scipen = 100) 

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


# Join Data Together ------------------------------------------------------
c_index_v4 <- full_join(c_1, c_2) 
c_index_v4 <- full_join(c_index_v4, c_3)
c_index_v4 <- full_join(c_index_v4, c_4)
c_index_v4 <- full_join(c_index_v4, c_5)
c_index_v4 <- full_join(c_index_v4, c_6)

colnames(c_index_v4) <- gsub("performance", "perf", names(c_index_v4))  # shorten col names
colnames(c_index_v4) <- gsub("disparity", "disp", names(c_index_v4))    # shorten col names

# calculate z-scores. Will need to add threshold option to the calculate_z function
ind_threshold <- 4  # update depending on the number of indicators in the issue area
c_index_v4 <- calculate_z(c_index_v4)

# merge region and urban type from current arei_multigeo_list
c_index_v4<- left_join(c_index_v4, region_urban_type)

# rename columns -- YOU MUST UPDATE ISSUE (COPY FROM v3 INDEX VIEW) --
issue <- 'healthy_built_environment'
c_index_v4 <- c_index_v4 %>% rename_with(~ paste0(issue, "_", .x), ends_with("_rank"))
c_index_v4 <- c_index_v4 %>% rename_with(~ paste0(issue, "_", .x), ends_with("performance_z"))
c_index_v4 <- c_index_v4 %>% rename_with(~ paste0(issue, "_", .x), ends_with("disparity_z"))

# select/reorder final columns for index table
index_table <- c_index_v4 %>% select(county_id, county_name, region, urban_type, ends_with("_rank"), quadrant, disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("performance_z"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 
index_table_name <- "arei_hben_index_2023"
index <- "Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is"
source <- "California Health Interview Survey (CHIS), CalEnviroScreen 4.0, National Land Cover Database (NLCD) 2019, and American Community Survey (ACS) 2016-2020 Table DP05"
rc_schema <- "v5"

#index_to_postgres()


