#### Composite Index Index (z-score) for RC v5 ####

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


## Add indexes and arei_multigeo_list ------------------------------------------------------
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_crim_index_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_demo_index_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_econ_index_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_educ_index_2023")
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_hben_index_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_index_2023")
c_7 <- st_read(con, query = "SELECT * FROM v5.arei_hous_index_2023")

region_urban_type <- st_read(con, query = "SELECT geoid AS county_id, region, urban_type FROM v4.arei_multigeo_list")


## Define variable names for clean_index_data_z function. you MUST UPDATE for each issue area. Used prefixes from arei_multigeo_list table.
varname1 <- 'crime_and_justice'
varname2 <- 'democracy'
varname3 <- 'economic_opportunity'
varname4 <- 'education'
varname5 <- 'healthy_built_environment'
varname6 <- 'health_care_access'
varname7 <- 'housing'


# Set Source for Index Functions script -----------------------------------
source("W:/Project/RACE COUNTS/Functions/RC_Index_Functions.R")

# remove exponentiation
options(scipen = 100) 

# Clean data --------
# use function to select cols we want, cap z-scores, and rename z-score cols

### c1 
c_1 <- clean_index_data_z(c_1, varname1)

### c2
c_2 <- clean_index_data_z(c_2, varname2)

### c3
c_3 <- clean_index_data_z(c_3, varname3)

## c4
c_4 <- clean_index_data_z(c_4, varname4)

## c5
c_5 <- clean_index_data_z(c_5, varname5)

## c6 
c_6 <- clean_index_data_z(c_6, varname6)

### c7
c_7 <- clean_index_data_z(c_7, varname7)



# Join Data Together ------------------------------------------------------
c_index_v5 <- full_join(c_1, c_2) 
c_index_v5 <- full_join(c_index_v5, c_3)
c_index_v5 <- full_join(c_index_v5, c_4)
c_index_v5 <- full_join(c_index_v5, c_5)
c_index_v5 <- full_join(c_index_v5, c_6)
c_index_v5 <- full_join(c_index_v5, c_7)


colnames(c_index_v5) <- gsub("performance", "perf", names(c_index_v5))  # shorten col names
colnames(c_index_v5) <- gsub("disparity", "disp", names(c_index_v5))    # shorten col names

# calculate z-scores. Will need to add threshold option to the calculate_z function
ind_threshold <- 4  # update depending on the number of indexes a county has
c_index_v5 <- calculate_index_z(c_index_v5)

# merge region and urban type from current arei_multigeo_list
c_index_v5<- left_join(c_index_v5, region_urban_type)

# # rename columns -- YOU MUST UPDATE ISSUE (COPY FROM V3 INDEX VIEW) --
# issue <- 'education'
# c_index_v5 <- c_index_v5 %>% rename_with(~ paste0(issue, "_", .x), ends_with("_rank"))
# c_index_v5 <- c_index_v5 %>% rename_with(~ paste0(issue, "_", .x), ends_with("performance_z"))
# c_index_v5 <- c_index_v5 %>% rename_with(~ paste0(issue, "_", .x), ends_with("disparity_z"))

# select/reorder final columns for index table
index_table <- c_index_v5 %>% select(county_id, county_name, region, urban_type, ends_with("_rank"), quadrant, disparity_z, performance_z, disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("_performance_z"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 

#####UPDATE#####
rc_schema <- "v5"
index_table_name <- "arei_composite_index_2023"
index <- "QA doc: W:\\Project\\RACE COUNTS\\2023_v5\\Composite Index\\QA_Sheet_Composite_Index.docx Includes all Issue area indexes. Composite index z-scores are the average z-scores for performance and disparity across all issue indexes. This data is"
source <- "various sources"

#index_to_postgres()









