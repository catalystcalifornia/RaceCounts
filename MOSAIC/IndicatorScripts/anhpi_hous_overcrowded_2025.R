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

# set these thresholds to match methodology for overcrowded housing for RC: https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_CountyState.html#Overcrowded_Housing
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


# # scrolling through the different overcrowded housing sub-variables and consulting with the RC methodology (https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_CountyState.html#Overcrowded_Housing
# # I am going to select: 001, 005, 006, 007, 011, 012, 013 subvariables to push to postgres
# #
# # Identify which variables to keep: 
asian_list_keep <- str_detect(
    asian_list[[2]]$new_var,
    "^.*_[^_]+_0(01|05|06|07|11|12|13)"
  )|
str_detect(
  asian_list[[2]]$new_label,
  "^(Estimate|MOE)!!Total:[^!]*$"
)|
  str_detect(
    asian_list[[2]]$new_var,
    "geoid"
  )|
  str_detect(
    asian_list[[2]]$new_var,
    "geolevel"
  )|
  str_detect(
    asian_list[[2]]$new_var,
    "name"
  )


# Filter both parts of the list
asian_list_filtered <- list(
  asian_list[[1]][, asian_list_keep, drop = FALSE],
  asian_list[[2]][asian_list_keep, ]
)

# Preserve the original names
names(asian_list_filtered) <- names(asian_list)

# Check that worked:
asian_filtered_meta <- asian_list_filtered[[2]] # scrolled through this and looks good

#  Repeat steps for nhpi_list

# Identify which variables to keep
nhpi_list_keep <- str_detect(
 nhpi_list[[2]]$new_var,
  "^.*_[^_]+_0(01|05|06|07|11|12|13)"
)|
  str_detect(
    nhpi_list[[2]]$new_label,
    "^(Estimate|MOE)!!Total:[^!]*$"
  )|
  str_detect(
    nhpi_list[[2]]$new_var,
    "geoid"
  )|
  str_detect(
    nhpi_list[[2]]$new_var,
    "geolevel"
  )|
  str_detect(
    nhpi_list[[2]]$new_var,
    "name"
  )


# Filter both parts of the list
nhpi_list_filtered <- list(
  nhpi_list[[1]][, nhpi_list_keep, drop = FALSE],
  nhpi_list[[2]][nhpi_list_keep, ]
)

# Preserve the original names
names(nhpi_list_filtered) <- names(nhpi_list)

# Check that worked:
nhpi_filtered_meta <- nhpi_list_filtered[[2]] # scrolled through this and looks good

# reassign filtered list name to just list_name for function syntax
asian_list<-asian_list_filtered
nhpi_list<-nhpi_list_filtered

# Send revised tables only with necessary columns to postgres
send_to_mosaic(table_code, asian_list, rc_schema)
send_to_mosaic(table_code, nhpi_list, rc_schema)


# IMPORT RAW DATA FROM POSTGRES -------------------------------------------
asian_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.asian_acs_5yr_%s_multigeo_%s",
                                      rc_schema, tolower(table_code), curr_yr))

nhpi_data <- dbGetQuery(con, sprintf("SELECT * FROM %s.nhpi_acs_5yr_%s_multigeo_%s",
                                     rc_schema, tolower(table_code), curr_yr))

#### ASIAN: Pre-RC CALCS ##############

asian_df <- prep_acs(asian_data, 'asian', table_code, cv_threshold, pop_threshold)

#########ACS PREP FX TEST############


# Overcrowded Housing #
## Occupants per Room
names(asian_data) <- gsub("001e", "_pop", names(asian_data))
names(asian_data) <- gsub("001m", "_pop_moe", names(asian_data))

## total data (more disaggregated than raced values so different prep needed)

### Extract total values to perform the various calculations needed
totals <- asian_data %>%
  select(geoid, geolevel, starts_with("total"))

totals <- totals %>% pivot_longer(total005e:total013e, names_to="var_name", values_to = "estimate")
totals <- totals %>% pivot_longer(total005m:total013m, names_to="var_name2", values_to = "moe")
totals$var_name <- substr(totals$var_name, 1, nchar(totals$var_name)-1)
totals$var_name2 <- substr(totals$var_name2, 1, nchar(totals$var_name2)-1)
totals <- totals[totals$var_name == totals$var_name2, ]
totals <- select(totals, -c(var_name, var_name2))

### sum the numerator columns 005e-013e (total_raw):
total_raw_values <- totals %>%
  select(geoid, geolevel, estimate) %>%
  group_by(geoid, geolevel) %>%
  summarise(total_raw = sum(estimate))

#### join these calculations back to x
x <- left_join(x, total_raw_values, by = c("geoid", "geolevel"))

### calculate the total_raw_moe using moe_sum (need to sort MOE values first to make sure highest MOE is used in case of multiple zero estimates)
### methodology source is text under table on slide 52 here: https://www.census.gov/content/dam/Census/programs-surveys/acs/guidance/training-presentations/20180418_MOE.pdf
total_raw_moes <- totals %>%
  select(geoid, geolevel, estimate, moe) %>%
  group_by(geoid, geolevel) %>%
  arrange(desc(moe), .by_group = TRUE) %>%
  summarise(total_raw_moe = moe_sum(moe, estimate, na.rm=TRUE))   # https://walker-data.com/tidycensus/reference/moe_sum.html

#### join these calculations back to x
x <- left_join(x, total_raw_moes, by = c("geoid", "geolevel"))

### calculate total_rate
total_rates <- left_join(total_raw_values, totals[, 1:3])
total_rates$total_rate <- total_rates$total_raw/total_rates$total_pop*100
total_rates <- total_rates %>%
  select(geoid, geolevel, total_rate) %>%
  distinct()

#### join these calculations back to x
x <- left_join(x, total_rates, by = c("geoid", "geolevel"))

### calculate the moe for total_rate
total_pop_data <- totals %>%
  select(geoid, geolevel, total_pop, total_pop_moe) %>%
  distinct()
total_rate_moes <- left_join(total_raw_values, total_raw_moes, by = c("geoid", "geolevel")) %>%
  left_join(., total_pop_data, by = c("geoid", "geolevel"))
total_rate_moes$total_rate_moe <- moe_prop(total_rate_moes$total_raw,    # https://walker-data.com/tidycensus/reference/moe_prop.html
                                           total_rate_moes$total_pop, 
                                           total_rate_moes$total_raw_moe, 
                                           total_rate_moes$total_pop_moe)*100
total_rate_moes <- total_rate_moes %>%
  select(geoid, geolevel, total_rate_moe)

#### join these calculations back to x
x <- left_join(x, total_rate_moes, by = c("geoid", "geolevel"))

## raced data (raw values don't need aggregation like total values do)

### calculate raced rates
x$asian_rate <- ifelse(x$asian_pop <= 0, NA, x$asian_raw/x$asian_pop*100)
x$black_rate <- ifelse(x$black_pop <= 0, NA, x$black_raw/x$black_pop*100)
x$nh_white_rate <- ifelse(x$nh_white_pop <= 0, NA, x$nh_white_raw/x$nh_white_pop*100)
x$latino_rate <- ifelse(x$latino_pop <= 0, NA, x$latino_raw/x$latino_pop*100)
x$other_rate <- ifelse(x$other_pop <= 0, NA, x$other_raw/x$other_pop*100)
x$pacisl_rate <- ifelse(x$pacisl_pop <= 0, NA, x$pacisl_raw/x$pacisl_pop*100)
x$twoormor_rate <- ifelse(x$twoormor_pop <= 0, NA, x$twoormor_raw/x$twoormor_pop*100)
x$aian_rate <- ifelse(x$aian_pop <= 0, NA, x$aian_raw/x$aian_pop*100)


### calculate moes for raced rates
x$asian_rate_moe <- moe_prop(x$asian_raw,
                             x$asian_pop,
                             x$asian_raw_moe,
                             x$asian_pop_moe)*100

x$black_rate_moe <- moe_prop(x$black_raw,
                             x$black_pop,
                             x$black_raw_moe,
                             x$black_pop_moe)*100

x$nh_white_rate_moe <- moe_prop(x$nh_white_raw,
                                x$nh_white_pop,
                                x$nh_white_raw_moe,
                                x$nh_white_pop_moe)*100

x$latino_rate_moe <- moe_prop(x$latino_raw,
                              x$latino_pop,
                              x$latino_raw_moe,
                              x$latino_pop_moe)*100

x$other_rate_moe <- moe_prop(x$other_raw,
                             x$other_pop,
                             x$other_raw_moe,
                             x$other_pop_moe)*100

x$pacisl_rate_moe <- moe_prop(x$pacisl_raw,
                              x$pacisl_pop,
                              x$pacisl_raw_moe,
                              x$pacisl_pop_moe)*100

x$twoormor_rate_moe <- moe_prop(x$twoormor_raw,
                                x$twoormor_pop,
                                x$twoormor_raw_moe,
                                x$twoormor_pop_moe)*100

x$aian_rate_moe <- moe_prop(x$aian_raw,
                            x$aian_pop,
                            x$aian_raw_moe,
                            x$aian_pop_moe)*100


### Convert any NaN to NA
x <- x %>% 
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

### drop the total006-013 e and m columns and pop_moe cols
x <- x %>%
  select(-starts_with("total0"), -ends_with("_pop_moe"))

















###############

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



