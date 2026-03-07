## MOSAIC: Disaggregated Asian/NHPI health_insurance B27001 instead of S2701###

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
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Health_Insurance - MOSAIC.docx"

cv_threshold = 40         
pop_threshold = 130      
pop_threshold = 0    
asbest = 'min'            
schema = 'health'
table_code = 'b27001'    # Select relevant indicator table name


# # CREATE RAW DATA TABLES -------------------------------------------------------------------------
# # Only run this section if the raw data tables have not been created yet ##
# race <- "asian"
# asian_list <- get_detailed_race(table_code, race, curr_yr)
# # check race col names which are created in fx
# # unique(asian_list[[2]]$new_label)
# 
# # race = for MOSAIC, either 'asian' or 'nhpi', case-insensitive
# # table_code = ACS table_code name, eg: "B25003" or "b25003", case-insensitive
# # year = ACS data year, defaults to 2021 if none specified
# #
# race <- "nhpi"
# nhpi_list <- get_detailed_race(table_code, race, curr_yr)
# # # check race col names which are created in fx
# # #unique(nhpi_list[[2]]$new_label)
# # #
# # #check the metadata to see if 001 is actually the total row
# # nhpi_list$metadata %>%
# #   filter(str_extract(new_var, "\\d{3}(?=[em]$)") == "001") %>%
# #   select(new_var, new_label)
# # # it is the total row so we need to exclude it when trying to get uninsured raw values and use is for total values (pop values)
# 
# #######   Transform the data for the raw data table  ############
# ### Instructions: This variable is broken up into insured/uninsured, by age group, by sex, by geolevel, and by detailed race.
# # We need to collapse the subcategories so that we just have it broken down by geolevel, detailed race, and uninsured and total of uninsured plus insured.
# 
#   # Step 1: pivot to long format (its too much data to work with wide format until we aggregate it down)
# nhpi_long <- nhpi_list$nhpi_df %>%
#   pivot_longer(
#     cols = -c(name, geoid, geolevel),
#     names_to = "variable",
#     values_to = "value"
#   )
# 
# asian_long <- asian_list$asian_df %>%
#   pivot_longer(
#     cols = -c(name, geoid, geolevel),
#     names_to = "variable",
#     values_to = "value"
#   )
# 
# # Step 2: separate estimate vs MOE and extract table number / detailed race number
# nhpi_long <- nhpi_long %>%
#   mutate(
#     type      = if_else(str_ends(variable, "e"), "e", "m"),
#     table_num = str_extract(variable, "(?<=b27001_)[a-z0-9]+(?=_)"),
#     var_num   = str_extract(variable, "\\d{3}(?=[em]$)")
#   )
# 
# asian_long <- asian_long %>%
#   mutate(
#     type      = if_else(str_ends(variable, "e"), "e", "m"),
#     table_num = str_extract(variable, "(?<=b27001_)[a-z0-9]+(?=_)"),
#     var_num   = str_extract(variable, "\\d{3}(?=[em]$)")
#   )
# 
# # #check that the extract worked
# # nhpi_long %>% filter(is.na(table_num)) %>% View()
# # nhpi_long %>% filter(is.na(var_num)) %>% View()
# # table(nhpi_long$var_num)
# #it looks good so keep going
# 
# # Step 3: get totals (var_num == "001")
# nhpi_total <- nhpi_long %>%
#   filter(var_num == "001") %>%
#   mutate(var_base = str_remove(variable, "[em]$")) %>%
#   select(name, geoid, geolevel, table_num, type, value, var_base) %>%
#   pivot_wider(names_from = type, values_from = value) %>%
#   group_by(name, geoid, geolevel, table_num) %>%
#   summarise(
#     m = moe_sum(moe = m, estimate = e),
#     e = if_else(all(is.na(e)), NA_real_, sum(e, na.rm = TRUE)),
#     .groups = "drop"
#   ) %>%
#   mutate(measure = "pop")
# 
# asian_total <- asian_long %>%
#   filter(var_num == "001") %>% #total is 001 so you don't need this column for this section
#   mutate(var_base = str_remove(variable, "[em]$")) %>%
#   select(name, geoid, geolevel, table_num, type, value, var_base) %>%
#   pivot_wider(names_from = type, values_from = value) %>%
#   group_by(name, geoid, geolevel, table_num) %>%
#   summarise(
#     m      = moe_sum(moe = m, estimate = e),
#     e = if_else(all(is.na(e)), NA_real_, sum(e, na.rm = TRUE)),
#     .groups  = "drop"
#   ) %>%
#   mutate(measure = "pop")
# 
# # Step 4: filter to uninsured variables
# 
# nhpi_uninsured <- nhpi_long %>%
#   filter(
#     str_detect(
#       variable,
#       paste(
#         nhpi_list$metadata %>%
#           filter(str_detect(new_label, "No health insurance coverage")) %>%
#           mutate(var = str_remove(new_var, "[em]$")) %>%
#           pull(var) %>%
#           unique(),
#         collapse = "|"
#       )
#     ),
#     var_num != "001" #exclude the total row
#   )
# 
# asian_uninsured <- asian_long %>%
#   filter(
#     str_detect(
#       variable,
#       paste(
#         asian_list$metadata %>%
#           filter(str_detect(new_label, "No health insurance coverage")) %>%
#           mutate(var = str_remove(new_var, "[em]$")) %>%
#           pull(var) %>%
#           unique(),
#         collapse = "|"
#       )
#     ),
#     var_num != "001" #exclude the total row
#   )
# 
# # Step 5: pivot wide and aggregate uninsured
# nhpi_uninsured_summary <- nhpi_uninsured %>%
#   mutate(var_base = str_remove(variable, "[em]$")) %>%
#   select(-variable) %>%
#   pivot_wider(names_from = type, values_from = value) %>%
#   group_by(name, geoid, geolevel, table_num) %>%
#   summarise(
#     m      = moe_sum(moe = m, estimate = e),
#     e = if_else(all(is.na(e)), NA_real_, sum(e, na.rm = TRUE)),
#     .groups  = "drop"
#   ) %>%
#   mutate(measure = "raw")
# 
# asian_uninsured_summary <- asian_uninsured %>%
#   mutate(var_base = str_remove(variable, "[em]$")) %>%
#   select(-variable) %>%
#   pivot_wider(names_from = type, values_from = value) %>%
#   group_by(name, geoid, geolevel, table_num) %>%
#   summarise(
#     m      = moe_sum(moe = m, estimate = e),
#     e = if_else(all(is.na(e)), NA_real_, sum(e, na.rm = TRUE)),
#     .groups  = "drop"
#   ) %>%
#   mutate(measure = "raw")
# 
# # Step 6: combine and pivot to final wide format
# nhpi_final <- bind_rows(nhpi_total, nhpi_uninsured_summary) %>%
#   pivot_wider(
#     names_from  = c(table_num, measure),
#     values_from = c(e, m),
#     names_glue  = "b27001_{table_num}_{ifelse(measure == 'pop', ifelse(.value == 'e', 'pop', 'pop_moe'), ifelse(.value == 'e', 'raw', 'raw_moe'))}"
# 
#   )
# 
# asian_final <- bind_rows(asian_total, asian_uninsured_summary) %>%
#   pivot_wider(
#     names_from  = c(table_num, measure),
#     values_from = c(e, m),
#     names_glue  = "b27001_{table_num}_{ifelse(measure == 'pop', ifelse(.value == 'e', 'pop', 'pop_moe'), ifelse(.value == 'e', 'raw', 'raw_moe'))}"
#   )
# 
# # Step 7: Add the metadata back
# # build metadata bridge while still in long format
# nhpi_metadata_final <- bind_rows(
#   # total variables
#   nhpi_long %>%
#     filter(var_num == "001") %>%
#     distinct(table_num) %>%
#     mutate(
#       estimate_var = paste0("b27001_", table_num, "_pop"),
#       moe_var      = paste0("b27001_", table_num, "_pop_moe")
#     ),
#   # uninsured variables
#   nhpi_uninsured %>%
#     distinct(table_num) %>%
#     mutate(
#       estimate_var = paste0("b27001_", table_num, "_raw"),
#       moe_var      = paste0("b27001_", table_num, "_raw_moe")
#     )
# ) %>%
#   pivot_longer(cols = c(estimate_var, moe_var),
#                names_to = NULL, values_to = "new_var") %>%
#   left_join(
#     nhpi_list$metadata %>%
#       filter(str_detect(new_var, "001e$")) %>%  # only estimate rows, not MOE
#       mutate(
#         table_num = str_extract(new_var, "(?<=b27001_)[a-z0-9]+(?=_)"),
#         new_label = str_remove(new_label, "^Estimate!!Total: ")
#       ) %>%
#       select(table_num, new_label),
#     by = "table_num"
#   ) %>%
# mutate(
#   new_label = case_when(
#     str_ends(new_var, "_raw")     ~ paste0("Estimate!!No health insurance coverage: ", new_label),
#     str_ends(new_var, "_raw_moe") ~ paste0("MOE!!No health insurance coverage: ", new_label),
#     str_ends(new_var, "_pop")     ~ paste0("Estimate!!Total: ", new_label),
#     str_ends(new_var, "_pop_moe") ~ paste0("MOE!!Total: ", new_label)
#   )
# )%>%
#   select(new_var, new_label) %>%
#   bind_rows(
#     tibble(new_var   = c("name", "geoid", "geolevel"),
#            new_label = c("", "fips code", "city, county, state"))
#   )
# 
# # reassemble list to match expected structure
# nhpi_list <- list(
#   nhpi_df  = nhpi_final,
#   metadata = nhpi_metadata_final
# )
# 
# 
# asian_metadata_final <- bind_rows(
#   # total variables
#   asian_long %>%
#     filter(var_num == "001") %>%
#     distinct(table_num) %>%
#     mutate(
#       estimate_var = paste0("b27001_", table_num, "_pop"),
#       moe_var      = paste0("b27001_", table_num, "_pop_moe")
#     ),
#   # uninsured variables
#   asian_uninsured %>%
#     distinct(table_num) %>%
#     mutate(
#       estimate_var = paste0("b27001_", table_num, "_raw"),
#       moe_var      = paste0("b27001_", table_num, "_raw_moe")
#     )
# ) %>%
#   pivot_longer(cols = c(estimate_var, moe_var),
#                names_to = NULL, values_to = "new_var") %>%
#   left_join(
#     asian_list$metadata %>%
#       filter(str_detect(new_var, "001e$")) %>%  # only estimate rows, not MOE
#       mutate(
#         table_num = str_extract(new_var, "(?<=b27001_)[a-z0-9]+(?=_)"),
#         new_label = str_remove(new_label, "^Estimate!!Total: ")
#       ) %>%
#       select(table_num, new_label),
#     by = "table_num"
#   ) %>%
# mutate(
#   new_label = case_when(
#     str_ends(new_var, "_raw")     ~ paste0("Estimate!!No health insurance coverage: ", new_label),
#     str_ends(new_var, "_raw_moe") ~ paste0("MOE!!No health insurance coverage: ", new_label),
#     str_ends(new_var, "_pop")     ~ paste0("Estimate!!Total: ", new_label),
#     str_ends(new_var, "_pop_moe") ~ paste0("MOE!!Total: ", new_label)
#   )
# ) %>%
#   select(new_var, new_label) %>%
#   bind_rows(
#     tibble(new_var   = c("name", "geoid", "geolevel"),
#            new_label = c("", "fips code", "city, county, state"))
#   )
# 
# # reassemble list to match expected structure
# asian_list <- list(
#   asian_df  = asian_final,
#   metadata = asian_metadata_final
# )

# # Send table to postgres
# send_to_mosaic(table_code, asian_list, rc_schema)
# send_to_mosaic(table_code, nhpi_list, rc_schema)


# # IMPORT RAW DATA FROM POSTGRES -------------------------------------------
asian_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.asian_acs_5yr_%s_multigeo_%s",
                                     rc_schema, tolower(table_code), curr_yr))

nhpi_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.nhpi_acs_5yr_%s_multigeo_%s",
                                     rc_schema, tolower(table_code), curr_yr))

#### ASIAN: Pre-RC CALCS ##############
asian_df <- prep_acs(asian_data, 'asian', table_code, cv_threshold, pop_threshold)


# #check if things make sense
# #indian raw is  na but I would expect that to be a bigger group if pop is 1599 for Alameda City
# # raw should be 42 according to online table
# asian_df %>%
#   filter(geoid %in% c("0600562", "06001")) %>%
#   select(name, geoid, geolevel, indian_pop, indian_raw, indian_pop_moe, indian_raw_moe)
# # # A tibble: 2 x 7
# # name    geoid   geolevel indian_pop indian_raw indian_pop_moe indian_raw_moe    ####raw is getting suppresed so maybe its a cv threshold issue?
# # <chr>   <chr>   <chr>         <dbl>      <dbl>          <dbl>          <dbl>
# #   1 Alameda 0600562 place          1599         NA            504           43.7
# # 2 Alameda 06001   county       142253       2372           3701          392.
# 
# ###### lets check the rate_cv too. Confirmed cv of 60.3 explains why the raw is getting suppressed even though it has data.
# asian_df %>%
# filter(geoid == "0600562") %>%
#   select(indian_rate, indian_rate_moe, indian_rate_cv, indian_raw)
# # A tibble: 1 x 4
# # indian_rate indian_rate_moe indian_rate_cv indian_raw
# # <dbl>           <dbl>          <dbl>      <dbl>
# #   1          NA            2.61           60.3         NA
# 
# #it makes sense that rates are getting suppressed but maybe raw values shouldn't be suppressed because uninsured rates are lower for asian subgroups and we still want to show the differences. 
# #Let's bring this up w/ the team for a methodology discussion.
# 
# # # some very high cvs like 640 for Aliso viejo but this is a dataset with pretty large MOEs so its actually correct
# # rate_cv = rate_moe / 1.645 / rate * 100
# # rate = 42 / 1599 * 100 = 2.63%
# # rate_moe = 2.61
# # racte_cv = 2.61 / 1.645 / 2.63 * 100 = 60.3 
# #
# # bhutanese is missing is there a 072 for b27001?
#   # I checked against the dataset online (https://data.census.gov/table?q=B27001:+Health+Insurance+Coverage+Status+by+Sex+by+Age&t=-04&g=050XX00US06001&y=2021&d=ACS+5-Year+Estimates+Selected+Population+Detailed+Tables) 
#   #and Bhutanese really is missing from this analysis

######################## resume ##########
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
county_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("Uninsured Population (%) ", str_to_title(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") ACS 5-Year Estimates, Table B27001, https://data.census.gov/cedsci/. QA doc: ", qa_filepath)   # See most recent Indicator Methodology for source info

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
county_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("Uninsured Population (%) ", toupper(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") 5-Year Estimates, SPT Table B27001, https://data.census.gov/cedsci/ . QA doc: ", qa_filepath)   # See most recent Indicator Methodology for source info

############## NHPI: SEND TO POSTGRES #######
to_postgres(county_table,state_table, 'mosaic')
city_to_postgres(city_table, 'mosaic')

dbDisconnect(con)