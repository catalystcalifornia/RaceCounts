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
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Health_Insurance - MOSAIC.docx. Please note that the variables ending in _002 is an internally created code to represent the raw values of uninsured by race to work with our prep_acs function and is not the Census ACS total male insured category." #going to run this variable again after raw data prep b/c it contains a note specific to the raw data tables

cv_threshold = 40         
pop_threshold = 130      
asbest = 'min'            
schema = 'health'
table_code = 'b27001'    # Select relevant indicator table name



county_api_call <- sprintf(
  "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&ucgid=pseudo(0400000US06$0500000)",
  curr_yr, toupper(table_code))

state_api_call <- sprintf(
  "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&ucgid=0400000US06",
  curr_yr, toupper(table_code))

api_call_list <- c(county_api_call, state_api_call)  

# Loop through API calls
parsed_list <- list()    # create empty list for loop results

for (i in api_call_list) {
  
  response <- GET(i)
  status_code(response)
  
  # Check if the request was successful
  if (status_code(response) == 200) {
    print("API request successful.")
  } else {
    stop("API request failed with status code: ", status_code(response))
  }
  
  raw_content <- content(response, "text")   # returns the response body as text
  parsed_data <- fromJSON(raw_content)
  
  parsed_list[[i]] <- parsed_data               # put loop results into a list
  
}

# clean up data
parsed_data <- do.call(rbind, parsed_list) %>% as.data.frame()
clean_data <- parsed_data
colnames(clean_data) <- clean_data[1, ]  # replace col names with first row values
clean_data <- clean_data[, !duplicated(names(clean_data))] # drop any duplicate cols, eg: POPGROUP
clean_data <- clean_data %>%
  mutate(across(contains(table_name), as.numeric))         # assign numeric cols to numeric type
clean_data <- clean_data %>%
  filter(!if_all(where(is.numeric), is.na))                # drop rows where all numeric values are NA, this also removes the extra 'header' rows
clean_data$geoid <- str_replace(clean_data$GEO_ID, ".*US", "")   # clean geoids
clean_data$geolevel <- case_when(                                # add geolevel bc it's a multigeo table
  nchar(clean_data$geoid) == 2 ~ 'state',
  nchar(clean_data$geoid) == 5 ~ 'county',
  .default = NA
)

clean_data <- clean_data %>%
  select(where(~!all(is.na(.))))         # drop cols where all vals are NA, eg: X_EA and X_MA the annotation cols






# CREATE RAW DATA TABLES -------------------------------------------------------------------------
# Only run this section if the raw data tables have not been created yet ##
# race <- "asian"
# asian_list <- get_detailed_race(table_code, race, curr_yr)
# #check race col names which are created in fx
# unique(asian_list[[2]]$new_label)
# 
# race <- "nhpi"
# nhpi_list <- get_detailed_race(table_code, race, curr_yr)
# #check race col names which are created in fx
# unique(nhpi_list[[2]]$new_label)
# 
# #check the metadata
# View(nhpi_list$metadata)
# 
# 
# ######  Transform the data for the raw data table  ############
# This variable is broken up by age group, by sex, by geo, insurance status, and by detailed race.
# We need to collapse the subcategories so that we just have it broken down by geo + detailed race + insurance status.
# 
# Step 1: pivot to long format (its too much data to work with wide format until we aggregate it down)
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
# check that the extract worked
# nhpi_long %>% filter(is.na(table_num)) %>% View()
# nhpi_long %>% filter(is.na(var_num)) %>% View()
# table(nhpi_long$var_num)
# it looks good so keep going
# 
# # Step 3: get totals (var_num == "001")
# nhpi_total <- nhpi_long %>%
#   filter(var_num == "001") %>%
#   mutate(var_base = substr(variable, 1, nchar(variable) - 5)) %>%
#   select(name, geoid, geolevel, var_base, type, value, var_num)
# 
# asian_total <- asian_long %>%
#   filter(var_num == "001") %>%
#   mutate(var_base = substr(variable, 1, nchar(variable) - 5)) %>%
#   select(name, geoid, geolevel, var_base, type, value, var_num)
# 
# # Step 4: filter to uninsured variables
# nhpi_uninsured_vars <- as.data.frame(nhpi_list$metadata %>% filter(grepl("No health insurance", new_label)))
# nhpi_uninsured <- nhpi_long %>%
#   filter(
#     variable %in% nhpi_uninsured_vars$new_var)
# # check we got all the uninsured e/m values, # unique uninsured variables should be the same as in metadata
# anti_join(nhpi_uninsured_vars %>% select(new_var), nhpi_uninsured %>% select(variable) %>% unique(), by = c("new_var" = "variable"))

# asian_uninsured_vars <- as.data.frame(asian_list$metadata %>% filter(grepl("No health insurance", new_label)))
# asian_uninsured <- asian_long %>%
#   filter(
#     variable %in% asian_uninsured_vars$new_var)
# 
# # Step 5: pivot wide and aggregate uninsured
# nhpi_uninsured_summary <- nhpi_uninsured %>%
#   mutate(var_base = substr(variable, 1, nchar(variable) - 5)) %>%
#   select(-variable) %>%
#   pivot_wider(names_from = type, values_from = value) %>%
#   group_by(name, geoid, geolevel, var_base) %>%
#   summarise(
#     #I was looking at Fresno county Pakistani to check b/c CR and LF got 199 but I got 174 for the raw_moe. Because of Census recommendations the moe_sum() formula will take the max moe if there are multiple zero estimates and uses that one only. This one had 11 zero estimates so it just takes the max zero moe (31) and doesn't use the other ones so the calculation is actually: Square root(81^2 + 121^2 + 11^2 + 58^2 +49^2 + 13^2 + 46^2 +31^2) = 174.17 that's why moe_sum() should be calculated before e is collapsed on the next line.
#     m = moe_sum(moe = m, estimate = e),
#     e = if_else(all(is.na(e)), NA_real_, sum(e, na.rm = TRUE)),
#     .groups = "drop"
#   ) %>%
#   pivot_longer(cols = c(e, m), names_to = "type", values_to = "value") %>%
#   mutate(var_num = "002")   # create new var_num for our calc'd uninsured variable
# # check Carson - Polynesian Alone
# # nhpi_uninsured %>%
# # filter(geoid == '0611530' & table_num == '051' & type == 'e') %>%
# # group_by(geoid, table_num, type) %>%
# # summarise(e=sum(value, na.rm=TRUE))
# 
# asian_uninsured_summary <- asian_uninsured %>%
#   mutate(var_base = substr(variable, 1, nchar(variable) - 5)) %>%
#   select(-variable) %>%
#   pivot_wider(names_from = type, values_from = value) %>%
#   group_by(name, geoid, geolevel, var_base) %>%
#   summarise(
#     m = moe_sum(moe = m, estimate = e),
#     e = if_else(all(is.na(e)), NA_real_, sum(e, na.rm = TRUE)),
#     .groups = "drop"
#   ) %>%
#   pivot_longer(cols = c(e, m), names_to = "type", values_to = "value") %>%
#   mutate(var_num = "002")   # create new var_num for our calc'd uninsured variable

# # check Fresno County - Pakistani Alone
# # Documentation: W:\Cold Data Migration\W Drive\Data\Demographics\ACS\Documentation\Using ACS Data for Researchers 2009.pdf 
# # Or https://www.census.gov/content/dam/Census/library/publications/2009/acs/ACSResearch.pdf  See Page A-14.
# fresno_check <- asian_uninsured %>%
# select(-variable) %>%
# filter(name == 'Fresno County' & table_num == '026') %>%
# pivot_wider(names_from = type, values_from = value) %>%
# group_by(name, geoid, geolevel, table_num) %>%
# summarise(m_update = moe_sum(moe = m, estimate = e), # updated moe calc
#           e=sum(e, na.rm=TRUE),
#           m_orig=moe_sum(moe=m, estimate=e))         # original moe calc
# fresno_check2 <- asian_uninsured %>%
#   filter(name == 'Fresno County' & table_num == '026' & type == 'm') %>%
#   mutate(m_sq = value^2)
# sqrt(sum(fresno_check2$m_sq))   # manually calc'd MOE per ACS Documentation cited above



# # Step 6: combine and pivot to final wide format
# nhpi_final <- bind_rows(nhpi_total, nhpi_uninsured_summary) %>%
#   pivot_wider(names_from = c(var_base, var_num, type),
#               names_glue = "{var_base}_{var_num}{type}",
#               values_from = value)
# 
# asian_final <- bind_rows(asian_total, asian_uninsured_summary) %>%
#   pivot_wider(names_from = c(var_base, var_num, type),
#               names_glue = "{var_base}_{var_num}{type}",
#               values_from = value)
# 
# ## check that moe issue was resolved. Looks good reordering moe_sum() inputs worked. Its now the expected moe again. moe should be before estimate in the function so that it doesn't use the collapsed scalar values of estimates as opposed to the vector of estimated to aggregate the moes.
# asian_final %>%
#   filter(geoid == "06019") %>%
#   select(b27001_026_001e, b27001_026_002e, b27001_026_001m, b27001_026_002m)
# 
# # Step 7: Build the metadata
# prep_metadata <- function(meta) {
#   #meta = metadata
# 
#   metadata_final <- meta %>%
#     filter(grepl('_001', new_var))
# 
#   raw_metadata <- metadata_final %>%
#     mutate(
#       new_var = str_replace(new_var, "_001", "_002"),
#       new_label = str_replace(new_label, "Total:", "Total: Uninsured")
#     )
# 
#   metadata_final <- bind_rows(metadata_final, raw_metadata) %>%
#     mutate(geoid = 'fips code',
#            geoname = 'geography name',
#            geolevel = 'City, County, State')
# 
#   return(metadata_final)
# }
# 
# 
# 
# # nhpi metadata
# nhpi_metadata_final <- prep_metadata(nhpi_list$metadata)
# 
# # reassemble list to match expected structure
# nhpi_list <- list(
#   nhpi_df  = nhpi_final,
#   metadata = nhpi_metadata_final
# )
# 
# # asian metadata
# asian_metadata_final <- prep_metadata(asian_list$metadata)
# 
# # reassemble list to match expected structure
# asian_list <- list(
#   asian_df  = asian_final,
#   metadata = asian_metadata_final
# )
# 
# # Send table to postgres
# send_to_mosaic(table_code, asian_list, rc_schema)
# send_to_mosaic(table_code, nhpi_list, rc_schema)

# Need to run qa filepath again becuase the previous included a comment that was only for the raw data tables
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Health_Insurance - MOSAIC.docx"
## IMPORT RAW DATA FROM POSTGRES -------------------------------------------
asian_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.asian_acs_5yr_%s_multigeo_%s",
                                      rc_schema, tolower(table_code), curr_yr))

nhpi_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.nhpi_acs_5yr_%s_multigeo_%s",
                                     rc_schema, tolower(table_code), curr_yr))
asian_data_qa <- dbGetQuery(con, ("SELECT * FROM v7.asian_acs_5yr_b27001_multigeo_2021_todelete_v1"))
asian_data_qa %>%
  filter(geoid == "06019") %>%
  select(b27001_026_pop, b27001_026_pop_moe, b27001_026_raw, b27001_026_raw_moe)

asian_data_qa2 <- dbGetQuery(con, ("SELECT * FROM v7.asian_acs_5yr_b27001_multigeo_2021_todelete_v2"))
asian_data_qa2 %>%
  filter(geoid == "06019") %>%
  select(b27001_026_001e, b27001_026_002e, b27001_026_001m, b27001_026_002m)
#### ASIAN: Pre-RC CALCS ##############
asian_df <- prep_acs(asian_data, 'asian', table_code, cv_threshold, pop_threshold)


# #check if things make sense
# #indian raw is na but I would expect that to be a bigger group if pop is 1599 for Alameda City
# # raw should be 42 according to online table
# asian_df %>%
#   filter(geoid %in% c("0600562", "06001")) %>%
#   select(name, geoid, geolevel, indian_pop, indian_raw, indian_pop_moe, indian_raw_moe)
# # name    geoid   geolevel indian_pop indian_raw indian_pop_moe indian_raw_moe    ####raw is getting suppressed so maybe it's a cv threshold issue?
# # 1 Alameda 0600562 place          1599         NA            504           43.7
# # 2 Alameda 06001   county       142253       2372           3701          392.
# 
# ###### lets check the rate_cv too. Confirmed cv of 60.3 explains why the raw is getting suppressed even though it has data.
# asian_df %>%
# filter(geoid %in% c("0600562", "06001") %>%
#   select(indian_rate, indian_rate_moe, indian_rate_cv, indian_raw)
# #   name    geoid   geolevel indian_pop indian_raw indian_pop_moe indian_raw_moe
# # 1 Alameda 0600562 place          1599         42            504           116.
# # 2 Alameda 06001   county       142253       2372           3701           392.
# 
# #it makes sense that rates are getting suppressed but maybe raw values shouldn't be suppressed because uninsured rates are lower for asian subgroups and we still want to show the differences. 
# #Let's bring this up w/ the team for a methodology discussion.
# 
# # # some very high cvs like 640 for Aliso Viejo but this is a dataset with pretty large MOEs so it's correct
# # rate_cv = rate_moe / 1.645 / rate * 100
# # rate = 42 / 1599 * 100 = 2.63%
# # rate_moe = 2.61
# # racte_cv = 2.61 / 1.645 / 2.63 * 100 = 60.3 
# #
# # bhutanese is missing is there a 072 for b27001?
#   # I checked against the dataset online (https://data.census.gov/table?q=B27001:+Health+Insurance+Coverage+Status+by+Sex+by+Age&t=-04&g=050XX00US06001&y=2021&d=ACS+5-Year+Estimates+Selected+Population+Detailed+Tables) 
#   #and Bhutanese really is missing from the original data

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

# # STATE: check how many groups have non-na rates
# sum(!is.na(state_table[, grep("_rate$", names(state_table))]))
# # check which groups have na rates
# names(which(colSums(is.na(state_table[, grep("_rate$", names(state_table))])) > 0))

# # COUNTY: check how many groups have non-na rates
# sum(!is.na(county_table[, grep("_rate$", names(county_table))]))
# # check how many counties have non-na rates by group
# colSums(!is.na(county_table[, grep("_rate$", names(county_table))]))


############## ASIAN: COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("Uninsured Population (%) ", str_to_title(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") ACS 5-Year Estimates, SPT Table ", toupper(table_code), " https://data.census.gov/cedsci/. QA doc: ", qa_filepath)   # See most recent Indicator Methodology for source info

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

# # STATE: check how many groups have non-na rates
# sum(!is.na(state_table[, grep("_rate$", names(state_table))]))
# # check which groups have na rates
# names(which(colSums(is.na(state_table[, grep("_rate$", names(state_table))])) > 0))
# 
# # COUNTY: check how many groups have non-na rates
# sum(!is.na(county_table[, grep("_rate$", names(county_table))]))
# # check how many counties have non-na rates by group
# colSums(!is.na(county_table[, grep("_rate$", names(county_table))]))


############## NHPI: COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0(tolower(race_name), "_hlth_health_insurance_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- paste0("Uninsured Population (%) ", toupper(race_name), " Detailed Groups ONLY")  # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") 5-Year Estimates, SPT Table ", toupper(table_code), " https://data.census.gov/cedsci/ . QA doc: ", qa_filepath)   # See most recent Indicator Methodology for source info

############## NHPI: SEND TO POSTGRES #######
to_postgres(county_table,state_table, 'mosaic')
city_to_postgres(city_table, 'mosaic')

dbDisconnect(con)
