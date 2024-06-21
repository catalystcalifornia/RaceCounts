#### Housing Index (z-score) for RC v5 ####

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
  
# Add indicators and arei_multigeo_list ------------------------------------------------------
####################### ADD COUNTY DATA #####################################
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_hous_cost_burden_owner_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_hous_cost_burden_renter_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_hous_denied_mortgages_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_hous_eviction_filing_rate_county_2023") 
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_hous_foreclosure_county_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_hous_homeownership_county_2023")
c_7 <- st_read(con, query = "SELECT * FROM v5.arei_hous_overcrowded_county_2023")
c_8 <- st_read(con, query = "SELECT * FROM v5.arei_hous_housing_quality_county_2023")
c_9 <- st_read(con, query = "SELECT * FROM v5.arei_hous_student_homelessness_county_2023")
c_10 <- st_read(con, query = "SELECT * FROM v5.arei_hous_subprime_county_2023")

region_urban_type <- st_read(con, query = "SELECT geoid AS county_id, region, urban_type FROM v5.arei_race_multigeo_2023")


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

# Set Source for Index Functions script -----------------------------------
source("W:/Project/RACE COUNTS/Functions/RC_Index_Functions.R")

# remove exponentiation
options(scipen = 100) 

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
c_index_v5 <- full_join(c_1, c_2) 
c_index_v5 <- full_join(c_index_v5, c_3)
c_index_v5 <- full_join(c_index_v5, c_4)
c_index_v5 <- full_join(c_index_v5, c_5)
c_index_v5 <- full_join(c_index_v5, c_6)
c_index_v5 <- full_join(c_index_v5, c_7)
c_index_v5 <- full_join(c_index_v5, c_8)
c_index_v5 <- full_join(c_index_v5, c_9)
c_index_v5 <- full_join(c_index_v5, c_10)
colnames(c_index_v5) <- gsub("performance", "perf", names(c_index_v5))  # shorten col names
colnames(c_index_v5) <- gsub("disparity", "disp", names(c_index_v5))    # shorten col names

# calculate z-scores. Will need to add threshold option to the calculate_z function
ind_threshold <- 5  # update depending on the number of indicators in the issue area
c_index_v5 <- calculate_z(c_index_v5)

# merge region and urban type from current arei_multigeo_list
c_index_v5<- left_join(c_index_v5, region_urban_type)

# rename columns -- YOU MUST UPDATE ISSUE (COPY FROM V3 INDEX VIEW) --
issue <- 'housing'
c_index_v5 <- c_index_v5 %>% rename_with(~ paste0(issue, "_", .x), ends_with("_rank"))
c_index_v5 <- c_index_v5 %>% rename_with(~ paste0(issue, "_", .x), ends_with("performance_z"))
c_index_v5 <- c_index_v5 %>% rename_with(~ paste0(issue, "_", .x), ends_with("disparity_z"))

# select/reorder final columns for index table
index_table <- c_index_v5 %>% select(county_id, county_name, region, urban_type, ends_with("_rank"), quadrant, disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("performance_z"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 
index_table_name <- "arei_hous_index_2023"
index <- "Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is"
source <- "CUS Department of Housing and Urban Development (HUD) Comprehensive Housing Affordability Strategy (CHAS) data (2014-2018), Home Mortgage Disclosure Act (HMDA) (2019-2020), The Eviction Lab at Princeton University (2014-2017), DataQuick (2017-2021), and AMERICAN COMMUNITY SURVEY 5-YEAR ESTIMATES, TABLES B25003B-I (2016-2020), B25014B-I, DP05, and PUMS (2016-2020)"

index_to_postgres()













