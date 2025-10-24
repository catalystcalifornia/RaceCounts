#### Leg District Composite Index Index (z-score) for RC v7 ####

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

# Load PostgreSQL driver and databases --------------------------------------------------
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# Set Source for Index Functions script -----------------------------------
source("./Functions/RC_Index_Functions.R")

# remove exponentiation
options(scipen = 100) 

# udpate each yr
rc_yr <- '2025'
rc_schema <- 'v7'
ind_threshold <- 4  # update depending on the number of indexes a county has

# You will need to update if we add more Democracy or Health indicators for Leg Districts
c_2 <- dbGetQuery(con, paste0("SELECT leg_id, leg_name, geolevel, disparity_z AS democracy_disparity_z, performance_z AS democracy_performance_z FROM ", rc_schema, ".arei_demo_census_participation_leg_", rc_yr))
c_6 <- dbGetQuery(con, paste0("SELECT leg_id, leg_name, geolevel, disparity_z AS health_care_access_disparity_z, performance_z AS health_care_access_performance_z FROM ", rc_schema, ".arei_hlth_health_insurance_leg_", rc_yr))

qa_filepath <- 'W:\\Project\\RACE COUNTS\\2025_v7\\Composite Index\\QA_Sheet_Leg_Indexes.docx'

## Add indexes and arei_county_region_urban_type ------------------------------------------------------
c_1 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_crim_index_leg_", rc_yr))
c_3 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_econ_index_leg_", rc_yr))
c_4 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_index_leg_", rc_yr))
c_5 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hben_index_leg_", rc_yr))
c_7 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_index_leg_", rc_yr))

## Define variable names for clean_index_data_z function. you MUST UPDATE for each issue area. Used prefixes from curr_schema arei_issue_list$api_name_v2
varname1 <- 'crime_and_justice'
varname2 <- 'democracy'
varname3 <- 'economic_opportunity'
varname4 <- 'education'
varname5 <- 'healthy_built_environment'
varname6 <- 'health_care_access'
varname7 <- 'housing'


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
index_list <- list(c_1, c_2, c_3, c_4, c_5, c_6, c_7)
index_list <- lapply(index_list, function(x) x %>% mutate(leg_name = gsub("State ", "", leg_name)))

c_index <- index_list %>% reduce(full_join, by=c('leg_id', 'leg_name', 'geolevel'))

colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names


# ASSEMBLY CALCS ------------------------------------------------------
assm_index <- filter(c_index, geolevel == 'sldl')

# calculate z-scores.
assm_index <- calculate_index_z(assm_index, ind_threshold)

# SENATE CALCS ------------------------------------------------------
sen_index <- filter(c_index, geolevel == 'sldu')

# calculate z-scores.
sen_index <- calculate_index_z(sen_index, ind_threshold)


# JOIN LEG INDEX TOGETHER ------------------------------------------------------
c_index <- rbind(assm_index, sen_index)

# select/reorder final columns for index table
index_table <- c_index %>% select(leg_id, leg_name, geolevel, ends_with("_rank"), quadrant, disparity_z, performance_z, disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("_performance_z"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 

#####UPDATE#####
index_table_name <- paste0("arei_composite_index_leg_", rc_yr)
index <- paste0("QA doc: ", qa_filepath, ". Includes all indicators. Index z-scores are the average z-scores for performance and disparity across all indicators. This data is") 
source <- "various sources"

index_to_postgres(index_table, rc_schema)
dbDisconnect(con)

