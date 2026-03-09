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
# 
# # set these thresholds to match methodology for overcrowded housing for RC: https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_CountyState.html#Overcrowded_Housing
cv_threshold = 40         # YOU MUST UPDATE based on most recent Indicator Methodology
pop_threshold = 100       # YOU MUST UPDATE based on most recent Indicator Methodology or set to NA B19301
asbest = 'min'
schema = 'housing'
table_code = "b25014"     # YOU MUST UPDATE based on most recent Indicator Methodology or most recent RC Workflow/Cnty-State Indicator Tracking

# # CREATE RAW DATA TABLES -------------------------------------------------------------------------
# ## Only run this section if the raw data tables have not been created yet ##
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

#########ACS PREP FX TEST ASIAN ONLY############

# Overcrowded Housing #
# ## Occupants per Room


### Extract total values:  

totals <- asian_data %>%
  select(name, geoid, geolevel,   matches("_(001|005|006|007|011|012|013)e$"))

totals <- totals %>% pivot_longer(4:290, names_to="var_name", values_to = "estimate")

# repeat this step for MOEs
moe <- asian_data %>%
  select(name, geoid, geolevel,   matches("_(001|005|006|007|011|012|013)m$"))

moe <- moe %>% pivot_longer(4:290, names_to="var_name2", values_to = "moe")

totals$var_name <- substr(totals$var_name, 1, nchar(totals$var_name)-1) # remove the e in the variable name

moe$var_name2 <- substr(moe$var_name2, 1, nchar(moe$var_name2)-1) # remove the m in the variable name

# join the total and moe tables together

totals<-totals%>%left_join(moe, by=c("var_name"="var_name2",
                                   "geoid" = "geoid",
                                   "name" = "name",
                                   "geolevel" = "geolevel"))%>%
  select(name, geoid, geolevel, var_name, estimate, moe)

# Quick QA: Go back to asian_data df and filter for geoid == 0600562 and geolevel== 'place' and look at estimate/moe values for b25014_013_001, b25014_013_005
# etc. and make sure values match what is in the totals df

# reformat table even more so that the races are its own columns

# start with a race code table: I took this by copy pasting lines 320-361 into chatgpt
# from the MOSAIC/Functions/acs_fx.R script and asking chatgpt to format that into the race_lookup tribble so I would not have to manually
# retype all the asian variable coding myself

library(tibble)

race_lookup <- tribble(
  ~race_code, ~ethnic_group,
  "013", "indian",
  "014", "bangladeshi",
  "015", "cambodian",
  "016", "chinese",
  "017", "chinese_no_taiwan",
  "018", "taiwanese",
  "019", "filipino",
  "020", "hmong",
  "021", "indonesian",
  "022", "japanese",
  "023", "korean",
  "024", "laotian",
  "025", "malaysian",
  "026", "pakistani",
  "027", "sri_lankan",
  "028", "thai",
  "029", "vietnamese",
  "032", "indian_aoic",
  "033", "bangladeshi_aoic",
  "034", "cambodian_aoic",
  "035", "chinese_aoic",
  "036", "chinese_no_taiwan_aoic",
  "037", "taiwanese_aoic",
  "038", "filipino_aoic",
  "039", "hmong_aoic",
  "040", "indonesian_aoic",
  "041", "japanese_aoic",
  "042", "korean_aoic",
  "043", "laotian_aoic",
  "044", "malaysian_aoic",
  "045", "pakistani_aoic",
  "046", "sri_lankan_aoic",
  "047", "thai_aoic",
  "048", "vietnamese_aoic",
  "072", "bhutanese",
  "073", "burmese",
  "075", "mongolian",
  "076", "nepalese",
  "081", "burmese_aoic",
  "083", "mongolian_aoic",
  "084", "nepalese_aoic",
  "085", "okinawan_aoic"
)


# Join the race lookup to my totals df by extracting and creating a race_code column from var_name

totals_re<-totals%>%
  mutate(
    race_code = str_split(var_name, "_", simplify = TRUE)[,2]) %>%
  left_join(race_lookup, by = "race_code")

# Quick QA: Look up metadata in rda dictionary https://www.healthycity.org/rda-dev/pgdatadictionary/ and just 
# do control+F for race codes lke _017_ or _014_ and see what the metadata in data dictionary is and compare
# to how it got recoded in totals_re I think looks good. 

### sum the numerator columns 005e-013e for each race group and geolevel

total_num_values <- totals_re %>%
  filter(!str_ends(var_name, "_001"))%>% # filter out the population total value this will be our denominator
  group_by(name, geoid, geolevel, race) %>%
  summarise(raw = sum(estimate))

# Quick QA: 
# I just looked at the totals_re df and filtered geoid=="0600562" & geolevel=='place' and did a quick sum of estimate where race==indian excluding b25014_013_001 and I got 26 which is what I see in total_num_values

# join tables together so we end up with a column for the numerator and denominator

total_num_values<-total_num_values%>%
  left_join(totals_re%>%filter(str_ends(var_name, "_001")), by=c( "name" = "name",
                                                                  "geoid" = "geoid",
                                                        "geolevel" = "geolevel",
                                                        "race" = "race"))%>%
  select(name, geoid, geolevel, race, var_name,  estimate, raw, moe)%>%
  rename("pop"="estimate",
         "pop_moe"="moe")

### calculate the total_raw_moe using moe_sum (need to sort MOE values first to make sure highest MOE is used in case of multiple zero estimates)
### methodology source is text under table on slide 52 here: https://www.census.gov/content/dam/Census/programs-surveys/acs/guidance/training-presentations/20180418_MOE.pdf

total_num_moes <- totals_re %>%
  filter(!str_ends(var_name, "_001"))%>% # filter out the population total moes to calculate only aggregated MOEs
  group_by(name, geoid, geolevel, race) %>%
  arrange(desc(moe), .by_group = TRUE) %>%
  summarise(raw_moe = moe_sum(moe, estimate, na.rm=TRUE))   # https://walker-data.com/tidycensus/reference/moe_sum.html

#### join numerator totals and numerator MOEs together to one df

df <- total_num_values%>%
  left_join(total_num_moes,  by=c( "name" = "name",
                                   "geoid" = "geoid",
                                   "geolevel" = "geolevel",
                                   "race" = "race"))

### calculate rates

df<-df%>%
   group_by(name, geoid, geolevel, race) %>%
  mutate(rate = ifelse(raw <= 0, NA, raw/pop*100)) # set rate == NA if the raw estimate is <=0

### calculate the moe for rates

df <- df %>%
  group_by(name, geoid, geolevel,race)%>%
  mutate(rate_moe=moe_prop(raw, pop, raw_moe, pop_moe)*100)  # https://walker-data.com/tidycensus/reference/moe_prop.html
  

# Now pivot the table back to wider for RC formatting in order to use subsequent RC functions

df_wide<-df%>%
  select(-var_name) %>%              # drop var_name since I don't need it
  pivot_wider(
    id_cols = c(name, geoid, geolevel),    # keep these as identifiers
    names_from = race,               # pivot based on race
    values_from = c(pop, raw, pop_moe, raw_moe, rate, rate_moe),
    names_glue = "{race}_{.value}"   # format column names so that the race value is attached
  )%>%
  mutate(total_rate = NA_real_) # for other RC functions to work we need a total_rate column even though for MOSAIC these values will just all be NA

### Convert any NaN to NA
df_wide <- df_wide %>% 
  mutate_all(function(x) ifelse(is.nan(x), NA, x))%>%
  ungroup()

# assign as asian_df for subsequent functions

asian_df<-df_wide

############### MOVE ON TO SCREENING 

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



