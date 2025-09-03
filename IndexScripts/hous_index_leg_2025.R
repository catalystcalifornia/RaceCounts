#### Housing Index (z-score) for RC v7 ####

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
source <- "US Department of Housing and Urban Development (HUD) 
Comprehensive Housing Affordability Strategy (CHAS) data (2017-2021), 
Home Mortgage Disclosure Act (HMDA) (Denied Mortgage 2019-2023) (Subprime Mortgage 2013-2017), 
The Eviction Lab at Princeton University (2014-2017), DataQuick (2017-2021), and 
AMERICAN COMMUNITY SURVEY 5-YEAR ESTIMATES, TABLES B25003B-I (2019-2023), B25014B-I, DP05, and 
PUMS (2019-2023)"
ind_threshold <- 5  # geos with < threshold # of indicator values are excluded from index. depends on the number of indicators in the issue area

qa_filepath <- 'W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Hous_Index.docx'

issue <- 'housing'

####################### ADD DATA #####################################
# you must update this section if we add or remove any indicators in an issue #

c_1 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_cost_burden_renter_leg_", rc_yr))
c_2 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_denied_mortgages_leg_", rc_yr))
c_3 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_eviction_filing_rate_leg_", rc_yr)) 
c_4 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_foreclosure_leg_", rc_yr))
c_5 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_homeownership_leg_", rc_yr))
c_6 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_overcrowded_leg_", rc_yr))
c_7 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_housing_quality_leg_", rc_yr))
c_8 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_hous_subprime_leg_", rc_yr))

## define variable names for clean_data_z function. you MUST UPDATE for each issue area. Copy from v6 index view.
varname1 <- 'burden_rent'
varname2 <- 'denied'
varname3 <- 'eviction'
varname4 <- 'forecl'
varname5 <- 'homeown'
varname6 <- 'overcrowded'
varname7 <- 'quality'
varname8 <- 'subprime'


region_urban_type <- dbGetQuery(con, paste0("select county_id, region, urban_type from ", rc_schema, ".arei_county_region_urban_type")) # get region, urban_type


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


# Join Data Together ------------------------------------------------------
c_index <- mutate(c_1,c_2)
c_index <- mutate(c_index,c_3)
c_index <- mutate(c_index,c_4)
c_index <- mutate(c_index,c_5)
c_index <- mutate(c_index,c_6)
c_index <- mutate(c_index,c_7)
c_index <- mutate(c_index,c_8)

colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names


# ASSEMBLY CALCS ------------------------------------------------------
assm_index <- filter(c_index, geolevel == 'sldl')

# calculate z-scores.
assm_index <- calculate_z(assm_index, ind_threshold)

# SENATE CALCS ------------------------------------------------------
sen_index <- filter(c_index, geolevel == 'sldu')

# calculate z-scores.
sen_index <- calculate_z(sen_index, ind_threshold)


# JOIN LEG INDEX TOGETHER ------------------------------------------------------
c_index <- rbind(assm_index, sen_index)

# rename columns
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("_rank"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("performance_z"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("disparity_z"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("quartile"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("quadrant"))

# select/reorder final columns for index table
index_table <- c_index %>% select(leg_id, leg_name, geolevel, ends_with("_rank"), ends_with("quadrant"), disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("performance_z"), ends_with("disparity_z_quartile"), ends_with("performance_z_quartile"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 
index_table_name <- paste0("arei_hous_index_leg_", rc_yr)
index <- paste0("QA doc: ", qa_filepath, ". Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is")

index_to_postgres(index_table, rc_schema)
dbDisconnect(con)


