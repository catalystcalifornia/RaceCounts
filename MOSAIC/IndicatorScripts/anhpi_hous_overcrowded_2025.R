## MOSAIC: Disaggregated Asian/NHPI Overcrowded Housing B25014 ###

#install packages if not already installed
packages <- c("readr", "tidyr", "dplyr", "DBI", "RPostgres", "tidycensus", "tidyverse", "stringr", "usethis", "httr", "jsonlite", "rlang")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen=999)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
source(".\\MOSAIC\\Functions\\acs_fx.R")
con <- connect_to_db("mosaic")


############## UPDATE VARIABLES ##############
curr_yr = 2021      # Always 2021 for MOSAIC 2026 project
rc_yr = '2025'      # you MUST UPDATE each year
rc_schema ="v7"     # you MUST UPDATE each year
schema = 'v7'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Overcrowded_Housing - MOSAIC.docx"

# set these thresholds to match methodology for internet access for RC: https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_CountyState.html#Internet_Access

cv_threshold = 40         # YOU MUST UPDATE based on most recent Indicator Methodology
pop_threshold = 100       # YOU MUST UPDATE based on most recent Indicator Methodology or set to NA B19301
asbest = 'min'  
schema = 'housing'
table_code = "b25014"     # YOU MUST UPDATE based on most recent Indicator Methodology or most recent RC Workflow/Cnty-State Indicator Tracking

# CREATE RAW DATA TABLES -------------------------------------------------------------------------
## Only run this section if the raw data tables have not been created yet ##
race <- "asian"
asian_list <- get_detailed_race(table_code, race, curr_yr)
# check race col names which are created in fx
# unique(asian_list[[2]]$new_label) 

race <- "nhpi"
nhpi_list <- get_detailed_race(table_code, race, curr_yr)
# check race col names which are created in fx
# unique(nhpi_list[[2]]$new_label)

# These lists asian_list and nhpi_list have too many rows, so need to explore the columns and drop what isn't needed

# pull out metadata for each list as a df
asian_meta <- asian_list[[2]]
nhpi_meta <- nhpi_list[[2]]

# further filter down what the actual different internet indicators there are in the metadata
# 
# asian_meta_filter<-asian_meta %>%
#   mutate(after_third = str_split_i(new_label, "!!", 4)) %>%
#   count(after_third, sort = TRUE)
# 
# # scrolling through the different internet sub-variables and consulting with the internet methodology (https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_CountyState.html#Internet_Access)
# # for RC I am going to select the variable 'Broadband of any type' and push to postgres.
# # Also need to filter for all the population total estimate values
# #
# # Identify which variables to keep: after talking to LF we are just using 'broadband of any type'
# asian_list_keep <- str_detect(
#   asian_list[[2]]$new_label,
#   "Broadband of any type"
# ) |
# str_detect(
#   asian_list[[2]]$new_label,
#   "^(Estimate|MOE)!!Total:[^!]*$"
# )|
#   str_detect(
#     asian_list[[2]]$new_var,
#     "geoid"
#   )|
#   str_detect(
#     asian_list[[2]]$new_var,
#     "geolevel"
#   )|
#   str_detect(
#     asian_list[[2]]$new_var,
#     "name"
#   )
# 
# 
# # Filter both parts of the list
# asian_list_filtered <- list(
#   asian_list[[1]][, asian_list_keep, drop = FALSE],
#   asian_list[[2]][asian_list_keep, ]
# )
# 
# # Preserve the original names
# names(asian_list_filtered) <- names(asian_list)
# 
# # Check that worked:
# asian_filtered_meta <- asian_list_filtered[[2]] # scrolled through this and looks good
# 
# # Repeat steps for nhpi_list
# 
# # Identify which variables to keep
# nhpi_list_keep <- str_detect(
#   nhpi_list[[2]]$new_label,
#   "Broadband of any type"
# ) |
#   str_detect(
#     nhpi_list[[2]]$new_label,
#     "^(Estimate|MOE)!!Total:[^!]*$"
#   )|
#   str_detect(
#     nhpi_list[[2]]$new_var,
#     "geoid"
#   )|
#   str_detect(
#     nhpi_list[[2]]$new_var,
#     "geolevel"
#   )|
#   str_detect(
#     nhpi_list[[2]]$new_var,
#     "name"
#   )
# 
# 
# # Filter both parts of the list
# nhpi_list_filtered <- list(
#   nhpi_list[[1]][, nhpi_list_keep, drop = FALSE],
#   nhpi_list[[2]][nhpi_list_keep, ]
# )
# 
# # Preserve the original names
# names(nhpi_list_filtered) <- names(nhpi_list)
# 
# # Check that worked:
# nhpi_filtered_meta <- nhpi_list_filtered[[2]] # scrolled through this and looks good
# 
# # reassign filtered list name to just list_name for function syntax
# asian_list<-asian_list_filtered
# nhpi_list<-nhpi_list_filtered

# Send revised tables only with necessary columns to postgres
send_to_mosaic(table_code, asian_list, rc_schema)
send_to_mosaic(table_code, nhpi_list, rc_schema)


# IMPORT RAW DATA FROM POSTGRES -------------------------------------------
asian_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.asian_acs_5yr_%s_multigeo_%s",
                                      rc_schema, tolower(table_code), curr_yr))

nhpi_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.nhpi_acs_5yr_%s_multigeo_%s",
                                     rc_schema, tolower(table_code), curr_yr))

#### ASIAN: Pre-RC CALCS ##############

# NOTE: Moving forward with the 004 sub-internet variable: Broadband of any kind

asian_df <- prep_acs(asian_data, 'asian', table_code, cv_threshold, pop_threshold)

asian_df_screened <- dplyr::select(asian_df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_cv"))

d <- asian_df_screened

race_name <- 'asian'  # this var is used to create the RC table name

######## ASIAN: CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = asbest    # Adds asbest value for RC Functions

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity

### Split into geolevel tables
#split into STATE, COUNTY, CITY, SLDU, SLDL tables 
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table) %>% dplyr::select(-c(geolevel, total_rate))
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table) 

## Calc county ranks## These fx don't work bc total_rate is NA
# county_table <- calc_ranks(county_table) 
county_table <- county_table %>% dplyr::select(-c(geolevel, total_rate))
# View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
# city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::select(-c(geolevel, total_rate))
#View(city_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")


############## ASIAN: COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0(tolower(race_name), "_econ_internet_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_econ_internet_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_econ_internet_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("Internet access (Any kind of broadband) ", str_to_title(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") 5-Year Estimates, SPT Table ", toupper(table_code), ", https://data.census.gov/cedsci/ . QA doc: ", qa_filepath)   # See most recent Indicator Methodology for source info

############## ASIAN: SEND TO POSTGRES #######
to_postgres(county_table,state_table, 'mosaic')
city_to_postgres(city_table, 'mosaic')

dbDisconnect(con)


#### NHPI: Pre-RC CALCS ##############
nhpi_df <- prep_acs(nhpi_data, 'nhpi', table_code, cv_threshold, pop_threshold)

nhpi_df_screened <- dplyr::select(nhpi_df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_cv"))

d <- nhpi_df_screened

race_name <- 'nhpi'  # this var is used to create the RC table name

######## NHPI: CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = asbest    # Adds asbest value for RC Functions

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity

### Split into geolevel tables
#split into STATE, COUNTY, CITY, SLDU, SLDL tables 
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table) %>% dplyr::select(-c(geolevel))
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table) 

## Calc county ranks## These fx don't work bc total_rate is NA
# county_table <- calc_ranks(county_table) 
county_table <- county_table %>% dplyr::select(-c(geolevel, total_rate))
# View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
# city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::select(-c(geolevel, total_rate))
#View(city_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")


############## NHPI: COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0(tolower(race_name), "_econ_internet_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_econ_internet_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_econ_internet_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("Internet access (Broadband of any kind) ", toupper(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") 5-Year Estimates, SPT Table ", toupper(table_code), ", https://data.census.gov/cedsci/ . QA doc: ", qa_filepath)   # See most recent Indicator Methodology for source info

############## NHPI: SEND TO POSTGRES #######
to_postgres(county_table,state_table, 'mosaic')
city_to_postgres(city_table, 'mosaic')

dbDisconnect(con)



