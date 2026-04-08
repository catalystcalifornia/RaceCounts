## MOSAIC: Disaggregated Asian/NHPI Mortgage Status by Selected Monthly Owner Costs as a Percentage of Household Income in the Past 12 Months for OWNER B25091 ###

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
curr_yr = 2021      # Data year
rc_yr = '2025'      # you MUST UPDATE each year
rc_schema ="v7"     # you MUST UPDATE each year
schema = 'v7'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_HousingBurden_Owner-MOSAIC.docx"

cv_threshold = 40         
pop_threshold = 100      # in this case, a screen on number of housing units, not population       
asbest = 'min'            
schema = 'housing'
table_code = 'b25091'          # Select relevant indicator table name


# CREATE RAW DATA TABLES -------------------------------------------------------------------------
# Only run this section if the raw data tables have not been created yet ##
 race <- "asian"
 asian_list <- get_detailed_race(table_code, race, curr_yr)
 # check race col names which are created in fx
 View(asian_list[[2]])

 race <- "nhpi"
 nhpi_list <- get_detailed_race(table_code, race, curr_yr)
 # check race col names which are created in fx
 View(nhpi_list[[2]])

######  Transform the data for the raw data table  ###
# This variable is broken up by geo, by tenure, by cost burden %, and by detailed race.
# We need to collapse the subcategories so that we just have it broken down by geo + detailed race + cost burden under or over 30%.
#
# # Step 1: pivot to long format (it's too much data to work with wide format until we aggregate it down)
#  nhpi_long <- nhpi_list$nhpi_df %>%
#    pivot_longer(
#      cols = -c(name, geoid, geolevel),
#      names_to = "variable",
#      values_to = "value"
#    )
# 
#  asian_long <- asian_list$asian_df %>%
#    pivot_longer(
#      cols = -c(name, geoid, geolevel),
#      names_to = "variable",
#      values_to = "value"
#    )
# 
#  # Step 2: separate estimate vs MOE and extract table number / detailed race number
#  nhpi_long <- nhpi_long %>%
#    mutate(
#      type      = if_else(str_ends(variable, "e"), "e", "m"),
#      table_num = str_extract(variable, "(?<=b25091_)[a-z0-9]+(?=_)"),
#      var_num   = str_extract(variable, "\\d{3}(?=[em])")
#    )
# 
#  asian_long <- asian_long %>%
#    mutate(
#      type      = if_else(str_ends(variable, "e"), "e", "m"),
#      table_num = str_extract(variable, "(?<=b25091_)[a-z0-9]+(?=_)"),
#      var_num   = str_extract(variable, "\\d{3}(?=[em])")
#    )
# 
#  # check that the extract worked
#  nhpi_long %>% filter(is.na(table_num)) %>% View()
#  nhpi_long %>% filter(is.na(var_num)) %>% View()
#  table(nhpi_long$var_num)
# # # it looks good so keep going
# 
#  # Step 3: get totals (var_num == "001")
#  nhpi_total <- nhpi_long %>%
#    filter(var_num == "001") %>%
#    mutate(var_base = substr(variable, 1, nchar(variable) - 5)) %>%
#    select(name, geoid, geolevel, var_base, type, value, var_num)
# 
#  asian_total <- asian_long %>%
#    filter(var_num == "001") %>%
#    mutate(var_base = substr(variable, 1, nchar(variable) - 5)) %>%
#    select(name, geoid, geolevel, var_base, type, value, var_num)
# 
# # # Step 4: filter for cost burden variables
#  nhpi_burden_vars <- as.data.frame(nhpi_list$metadata %>% filter(grepl("percent", new_label)))
#  nhpi_burden <- nhpi_long %>%
#    filter(
#      variable %in% nhpi_burden_vars$new_var)
#   # check we got all the cost burden e/m values, # unique cost burden variables should be the same as in metadata
#  anti_join(nhpi_burden_vars %>% select(new_var), nhpi_burden %>% select(variable) %>% unique(), by = c("new_var" = "variable"))
# 
#  asian_burden_vars <- as.data.frame(asian_list$metadata %>% filter(grepl("percent", new_label)))
#  asian_burden <- asian_long %>%
#    filter(
#      variable %in% asian_burden_vars$new_var)
# 
# # # Step 5: aggregate by cost burden status
#  nhpi_burden_summary <- nhpi_burden %>%
#    mutate(var_base = substr(variable, 1, nchar(variable) - 5),
#           burden = case_when(
#             var_num %in% c("008", "009", "010", "011", "019", "020", "021", "022") ~ "burdened",
#             var_num %in% c("003", "004", "005", "006", "007", "014", "015", "016", "017", "018") ~ "not_burdened",
#             TRUE ~ NA_character_  # 001, 002, 013 are totals - exclude
#           )
#    ) %>%
#    filter(!is.na(burden)) %>%
#    select(-variable) %>%
#    group_by(name, geoid, geolevel, var_base, burden) %>%
#    summarise(
#      e = safe_sum(value[type == "e"]),
#      m = moe_sum(
#        moe      = value[type == "m"],
#        estimate = value[type == "e"]
#      ),
#      .groups = "drop"
#    ) %>%
#    mutate(var_num = if_else(burden == 'burdened', "002", "003")) %>%  # create new var_num for our calc'd cost burden variable
#    pivot_longer(cols = -c(name, geoid, geolevel, var_base, burden, var_num),
#                 names_to = "type",
#                 values_to = "value",
#    )
# 
# # # check Carson - Polynesian Alone
#  nhpi_burden %>%
#    filter(geoid == '0611530' & table_num == '051' & type == 'e') %>%
#    group_by(geoid, table_num, type) %>%
#    summarise(e=sum(value, na.rm=TRUE))
# 
# 
#  asian_burden_summary <- asian_burden %>%
#    mutate(var_base = substr(variable, 1, nchar(variable) - 5),
#           burden = case_when(
#             var_num %in% c("008", "009", "010", "011", "019", "020", "021", "022") ~ "burdened",
#             var_num %in% c("003", "004", "005", "006", "007", "014", "015", "016", "017", "018") ~ "not_burdened",
#             TRUE ~ NA_character_  # 001, 002, 013 are totals - exclude
#           )
#    ) %>%
#    filter(!is.na(burden)) %>%
#    select(-variable) %>%
#    group_by(name, geoid, geolevel, var_base, burden) %>%
#    summarise(
#      e = safe_sum(value[type == "e"]),
#      m = moe_sum(
#        moe      = value[type == "m"],
#        estimate = value[type == "e"]
#      ),
#      .groups = "drop"
#    ) %>%
#    mutate(var_num = if_else(burden == 'burdened', "002", "003")) %>% #  create new var_num for our calc'd cost burden variable
#    pivot_longer(cols = -c(name, geoid, geolevel, var_base, burden, var_num),
#                 names_to = "type",
#                 values_to = "value",
#    )
# 
# 
# # # Step 6: combine and pivot to final wide format
#  nhpi_final <- bind_rows(nhpi_total, nhpi_burden_summary) %>%
#    pivot_wider(names_from = c(var_base, var_num, burden, type),
#                names_glue = "{var_base}_{var_num}{type}",
#                values_from = value)
# 
#  asian_final <- bind_rows(asian_total, asian_burden_summary) %>%
#    pivot_wider(names_from = c(var_base, var_num, burden, type),
#                names_glue = "{var_base}_{var_num}{type}",
#                values_from = value)
# 
# #
# # # Step 7: Build the metadata
#  prep_metadata <- function(meta) {
#    metadata_final <- meta %>%
#      filter(grepl('_001', new_var))
# 
#    burden_metadata <- metadata_final %>%
#      mutate(
#        new_var = str_replace(new_var, "_001", "_002"),
#        new_label = str_replace(new_label, "Total:", "Total: Cost Burdened")
#      )
# 
#    not_burden_metadata <- metadata_final %>%
#      mutate(
#        new_var = str_replace(new_var, "_001", "_003"),
#        new_label = str_replace(new_label, "Total:", "Total: Not Cost Burdened")
#      )
# 
#    metadata_final <- bind_rows(metadata_final, burden_metadata, not_burden_metadata)
# 
#    new_rows <- data.frame(new_var = c("name", "geoid", "geolevel"), new_label = c("geography name", "fips code", "City, County, State"))
#    metadata_final <- rbind(new_rows, metadata_final)
# 
#    return(metadata_final)
#  }
# 
#  # nhpi metadata
#  nhpi_metadata_final <- prep_metadata(nhpi_list$metadata)
# 
# # # reassemble list to match expected structure
#  nhpi_list <- list(
#    nhpi_df  = nhpi_final,
#    metadata = nhpi_metadata_final
#  )
# 
# # # asian metadata
#  asian_metadata_final <- prep_metadata(asian_list$metadata)
# 
# # # reassemble list to match expected structure
#  asian_list <- list(
#    asian_df  = asian_final,
#    metadata = asian_metadata_final
#  )
# 
# # # Send table to postgres
#  send_to_mosaic(table_code, asian_list, rc_schema)
#  send_to_mosaic(table_code, nhpi_list, rc_schema)


# IMPORT RAW DATA FROM POSTGRES -------------------------------------------
asian_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.asian_acs_5yr_%s_multigeo_%s",
                                      rc_schema, tolower(table_code), curr_yr))

nhpi_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.nhpi_acs_5yr_%s_multigeo_%s",
                                     rc_schema, tolower(table_code), curr_yr))

#### ASIAN: Pre-RC CALCS ##############
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
county_table <- county_table %>% dplyr::select(-c(geolevel, total_rate)) %>%
  select(where(~!all(is.na(.))))      # drop cols where all values are NA
# View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
# city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::select(-c(geolevel, total_rate)) %>%
  select(where(~!all(is.na(.))))      # drop cols where all values are NA
#View(city_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")


############## ASIAN: COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0(tolower(race_name), "_hous_cost_burden_renter_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_hous_cost_burden_renter_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_hous_cost_burden_renter_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("The percentage of rented housing units experiencing cost burden (Monthly housing costs, including utilities, exceeding 30% of monthly income ", toupper(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
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
county_table <- county_table %>% dplyr::select(-c(geolevel, total_rate)) %>%
  select(where(~!all(is.na(.))))      # drop cols where all values are NA
# View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
# city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::select(-c(geolevel, total_rate)) %>%
  select(where(~!all(is.na(.))))      # drop cols where all values are NA
#View(city_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")


############## NHPI: COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0(tolower(race_name), "_hous_cost_burden_renter_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_hous_cost_burden_renter_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_hous_cost_burden_renter_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("The percentage of rented housing units experiencing cost burden (Monthly housing costs, including utilities, exceeding 30% of monthly income ", toupper(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") 5-Year Estimates, SPT Table ", toupper(table_code), ", https://data.census.gov/cedsci/ . QA doc: ", qa_filepath)   # See most recent Indicator Methodology for source info

############## ASIAN: SEND TO POSTGRES #######
to_postgres(county_table,state_table, 'mosaic')
city_to_postgres(city_table, 'mosaic')

dbDisconnect(con)