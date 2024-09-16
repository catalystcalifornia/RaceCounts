#### Composite Index Index (z-score) for RC v6 ####

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
index <- "QA doc: W:\\Project\\RACE COUNTS\\2024_v6\\Composite Index\\QA_Sheet_Composite_Index.docx Includes all Issue area indexes. Composite index z-scores are the average z-scores for performance and disparity across all issue indexes. This data is"


## Add indexes and arei_county_region_urban_type ------------------------------------------------------
c_1 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_crim_index_", rc_yr))
c_2 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_demo_index_", rc_yr))
c_3 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_econ_index_", rc_yr))
c_4 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_educ_index_", rc_yr))
c_5 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hben_index_", rc_yr))
c_6 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hlth_index_", rc_yr))
c_7 <- st_read(con, query = paste0("SELECT * FROM ", rc_schema, ".arei_hous_index_", rc_yr))

## Define variable names for clean_index_data_z function. you MUST UPDATE for each issue area. Used prefixes from curr_schema arei_issue_list$api_name_v2
varname1 <- 'crime_and_justice'
varname2 <- 'democracy'
varname3 <- 'economic_opportunity'
varname4 <- 'education'
varname5 <- 'healthy_built_environment'
varname6 <- 'health_care_access'
varname7 <- 'housing'


region_urban_type <- st_read(con, query = paste0("SELECT county_id, region, urban_type FROM ", rc_schema, ".arei_county_region_urban_type"))


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
c_index <- full_join(c_1, c_2) 
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
c_index <- full_join(c_index, c_5)
c_index <- full_join(c_index, c_6)
c_index <- full_join(c_index, c_7)


colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names

# calculate z-scores. Will need to add threshold option to the calculate_z function
ind_threshold <- 4  # update depending on the number of indexes a county has
c_index <- calculate_index_z(c_index)

# merge region and urban type from current arei_county_region_urban_type
c_index <- left_join(c_index, region_urban_type)

# select/reorder final columns for index table
index_table <- c_index %>% select(county_id, county_name, region, urban_type, ends_with("_rank"), quadrant, disparity_z, performance_z, disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("_performance_z"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 

#####UPDATE#####
index_table_name <- paste0("arei_composite_index_", rc_yr)
source <- "various sources"

index_to_postgres(index_table, rc_schema)
dbDisconnect(con)




