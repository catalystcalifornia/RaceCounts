## Subprime Mortgage Loans for RC v7 ##

### install packages if not already installed ####
packages <- c("openxlsx","tidyr","dplyr","stringr", "DBI", "RPostgres","data.table", "openxlsx", "tidycensus", "tidyverse", 
              "janitor","httr", "readxl","sf","DBI","usethis","rvest","tigris","jsonlite","rlist","curl")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
}

options(scipen = 999) # disable scientific notation


### Set up workspace ####
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")

qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Sheet_Subprime.docx"

# update each year: variables used throughou
xwalk_yr <- 2020       # vintage of final shapes for analysis, eg: places, leg districts NOT vintage of the data
hmda_yr <- '2013-2017' # hmda data yrs
rc_schema <- 'v7'
rc_yr <- '2025'
threshold <- 75        # data for geos+race combos with < threshold are suppressed


# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table    

### ALL APPLICATIONS DATA #### 
# Create rda_shared_data all applications table
# df_2013 <- read_csv("hmda_2013_ca_all-records_codes.csv")
# df_2014 <- read_csv("hmda_2014_ca_all-records_codes.csv")
# df_2015 <- read_csv("hmda_2015_ca_all-records_codes.csv")
# df_2016 <- read_csv("hmda_2016_ca_all-records_codes.csv")
# df_2017 <- read_csv("hmda_2017_ca_all-records_codes.csv")
# df_applications <- rbind(df_2013, df_2014,df_2015, df_2016, df_2017)

# export all applications to rda_shared_table 
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "housing"
# table_name <- "hmda_2017_5yr_all_applications"
# table_comment_source <- "ALL Home Mortgage Disclosure Act (HMDA) records including applications, denials, originations, institution purchases"
# table_source <- "HMDA historic Data: https://www.consumerfinance.gov/data-research/hmda/historic-data/. Raw data: W:/Data/Housing/HMDA/Subprime/2013-2017/hmda_xxxx_ca_all-records_codes.csv."
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# dbWriteTable(con2, c(table_schema, table_name), df_applications, overwrite = FALSE, row.names = FALSE)

# send table comment to database
# dbSendQuery(conn = con2, table_comment)  

df_applications <- dbGetQuery(con2, "SELECT * FROM housing.hmda_2017_5yr_all_applications")  # comment out above after table created, instead import from postgres


# Filter for home purchases, first lien, one-to-four family home, Owner Occupancy, Loan originated 
##### Metadata: https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields/
df_applications <- df_applications %>% filter(lien_status == "1" & property_type == "1" & loan_purpose == "1" & owner_occupancy == "1" & action_taken %in% c("1"))


# Clean county GEOID Column 
## paste leading zeros to county_code
df_applications <- df_applications %>% mutate(length = str_count(county_code, "[0-9]"),
                                                                        county_code = case_when(
                                                                          length == 1 ~ paste0("0600", county_code),
                                                                          length == 2 ~ paste0("060", county_code),
                                                                          length == 3 ~ paste0("06", county_code),
                                                                        )) 

## create ct_geoid field
# ct_nchar <- as.data.frame(nchar(df_applications$census_tract_number))  # check if ct numbers are all same length or if some need leading/trailing zeros
df_applications$ct_geoid <- paste0(df_applications$county_code, df_applications$census_tract_number) 
df_applications$ct_geoid = gsub("\\.", "", df_applications$ct_geoid)


### SUBPRIME DATA ##### 
# # Import subprime data
# subprime_mortgages <- dbGetQuery(con, "SELECT * FROM data.hmda_2017_5yr_subprime_mortgages")

# # Add Census Tract GEOID Column 
# ### first explored subprime tract numbers ### For ex: check if ct numbers are all same length or if some need leading/trailing zeros, some cts have decimals etc.
# ### figured out trailing zeroes were missing bc when i ran the code below adding leading zeroes instead, there were no matches with the ct-place xwalk.
# # tracts <- tracts(state = 'CA', year = 2017, cb = TRUE) %>% select(-c(STATEFP, TRACTCE, AFFGEOID, NAME, LSAD, ALAND, AWATER)) %>% st_drop_geometry()
#
# # relocate ct column and calc length of ct column
# subprime_clean <- subprime_mortgages %>% relocate(census_tract_number, .after = last_col()) %>% mutate(ct_nchar = str_count(census_tract_number, "[0-9]"))
#
# # split census_tract_number: digits before "." and digits after "."
# subprime_clean[c('ct_1', 'ct_2')] <- str_split_fixed(subprime_clean$census_tract_number, '\\.', 2)
# subprime_clean["ct_2"][subprime_clean["ct_2"] == ''] <- NA # replace blanks in ct_2 with NA

#
# # add trailing zeroes to cts WITHOUT decimals
# subprime_clean <- subprime_clean %>% mutate(ct_geoid = case_when(is.na(ct_2) & ct_nchar == 1 ~ paste0(census_tract_number, "0000"),
#                                                                    is.na(ct_2) & ct_nchar == 2 ~ paste0(census_tract_number, "000"),
#                                                                    is.na(ct_2) & ct_nchar == 3 ~ paste0(census_tract_number, "00"),
#                                                                    is.na(ct_2) & ct_nchar == 4 ~ paste0(census_tract_number, "0"),
#                                                                    is.na(ct_2) & ct_nchar == 5 ~ census_tract_number,
#                                              )) # there are 115 rows where ct_geoid is NA where ct_2 == '', this is bc census_tract_number is NA

# # add trailing zeroes to cts WITH decimals
# subprime_clean <- subprime_clean %>% mutate(ct_geoid = case_when(is.na(ct_geoid) & nchar(ct_1) == 1 ~ paste0("000", ct_1),
#                                              is.na(ct_geoid) & nchar(ct_1) == 2 ~ paste0("00", ct_1),
#                                              is.na(ct_geoid) & nchar(ct_1) == 3 ~ paste0("0", ct_1),
#                                              is.na(ct_geoid) & nchar(ct_1) == 4 ~ ct_1,
#                                              .default = as.character(subprime_clean$ct_geoid)
#                                              ))

# # add trailing zeroes and/or digits after decimal
# subprime_clean <- subprime_clean_2 %>% mutate(ct_geoid = case_when(nchar(ct_2) == 1 ~ paste0(ct_geoid, ct_2, "0"),
#                                              nchar(ct_2) == 2 ~ paste0(ct_geoid, ct_2),
#                                              nchar(ct_geoid) == 4 ~ paste0(ct_geoid, "00"),
#                                              nchar(ct_geoid) == 5 ~ paste0(ct_geoid, "0"),
#                                              .default = as.character(subprime_clean$ct_geoid)
#                                              ))

# # add county fips prefix
# subprime_clean <- subprime_clean_3 %>% mutate(ct_geoid = case_when(nchar(ct_geoid) == 6 ~ paste0(county_fips, ct_geoid),
#                                              .default = as.character(subprime_clean$ct_geoid)
#                                       )) # there are 115 rows where ct_geoid is NA bc census_tract_number is NA


# export clean subprime rda_shared_data table
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "housing"
# table_name <- "hmda_tract_subprime_mortgages_2013_17"
# table_comment_source <- "This table is the same as racecounts.hmda_2017_5yr_subprime_mortgages, but with a newly added ct_geoid field which is cleaned up/properly formatted tract fips codes. See: W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\IndicatorScripts\\Housing\\hous_subprime_2023.R. Raw data was originally downloaded from this URL which is no longer available: https://www.consumerfinance.gov/data-research/hmda/explore#!/as_of_year=2017,2016,2015,2014,2013&state_code-1=6&action_taken=1&rate_spread!=null&section=filters. On that page just added years 2013-17, chose the state of California, left loan originated selected (under loan), and chose YES to is it a higher-priced loan? (also under loan). Saved as W:\\Project\\RACE COUNTS\\Data\\Postgres Tables and Views\\county and state views\\2017\\subprime_county_2017\\hmda_2013_17_subprime.csv"
# table_source <- "HMDA 2013-2017"
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# dbWriteTable(con2, c(table_schema, table_name), subprime_clean, overwrite = FALSE, row.names = FALSE)

# send table comment to database
# dbSendQuery(conn = con2, table_comment)

df_subprime <- dbGetQuery(con2, "SELECT * FROM housing.hmda_tract_subprime_mortgages_2013_17")  # comment out above after table created, instead import from postgres

# Filter for home purchases, first lien, one-to-four family home, owner-occupied, Loan originated 
df_subprime <- df_subprime %>% filter(lien_status == "1" & property_type == "1" & loan_purpose == "1" & owner_occupancy == "1" & action_taken %in% c("1"))


### Convert data from 2010 CT's to 2020 CT's ####
cb_tract_2010_2020 <- fread("W:\\Data\\Geographies\\Relationships\\cb_tract2020_tract2010_st06.txt", sep="|", colClasses = 'character', data.table = FALSE) %>%
  select(GEOID_TRACT_10, NAMELSAD_TRACT_10, AREALAND_TRACT_10, GEOID_TRACT_20, NAMELSAD_TRACT_20, AREALAND_TRACT_20, AREALAND_PART) %>%
  mutate_at(vars(contains("AREALAND")), function(x) as.numeric(x)) %>%
  # calculate overlapping land area of 2010 and 2020 tracts (AREALAND_PART) as a percent of 2020 tract land area (AREALAND_TRACT_20)
  mutate(prc_overlap=AREALAND_PART/AREALAND_TRACT_10, # pct of 2010 tract that is in 2020 tract to ensure 100% of 2010 loans are assigned to 2020 tracts  
         county_id = substr(GEOID_TRACT_20,1,5))

df_applications20 <- df_applications %>%
  left_join(cb_tract_2010_2020, by = c("ct_geoid"="GEOID_TRACT_10"), relationship = "many-to-many") %>%
  # Allocate data from 2010 tracts to 2020 using prc_overlap
  rename("wt_val"="prc_overlap")		# 2020 data value set to pct_overlap bc data was converted from 2010 tracts

## There is 1 tract that does not match (06037137000 which is a 2020 tract) with about 39 rows. There are also rows where ct_geoid is NA.
## Added manual fix above to assign GEOID_TRACT_20 06037137000 for rows with census_tract 06037137000.
app_nomatch <- anti_join(df_applications, cb_tract_2010_2020, by = c("ct_geoid"="GEOID_TRACT_10")) 
df_applications20 <- df_applications20 %>% 
  mutate(GEOID_TRACT_20 = ifelse(ct_geoid == '06037137000', '06037137000', GEOID_TRACT_20),
         county_id = ifelse(ct_geoid == '06037137000', '06037', county_id),
         wt_val = ifelse(ct_geoid == '06037137000', 1, wt_val))

df_subprime20 <- df_subprime %>%
  left_join(cb_tract_2010_2020, by = c("ct_geoid"="GEOID_TRACT_10"), relationship = "many-to-many") %>%
  # Allocate data from 2010 tracts to 2020 using prc_overlap
  rename("wt_val"="prc_overlap")		# 2020 data value set to pct_overlap bc data was converted from 2010 tracts

## There is 1 tract that does not match (06037137000 which is a 2020 tract) with about 6 rows. There are also rows where ct_geoid doesn't match 2010 or 2020 or is NA.
## Added manual fix above to assign GEOID_TRACT_20 06037137000 for rows with census_tract 06037137000.
subprime_nomatch <- anti_join(df_subprime, cb_tract_2010_2020, by = c("ct_geoid"="GEOID_TRACT_10")) 
subprime_nomatch_valid <- subprime_nomatch %>% select(ct_geoid) %>% filter(!is.na(ct_geoid)) %>% #unique() 
  left_join(cb_tract_2010_2020, by = c("ct_geoid" = "GEOID_TRACT_20"), keep = TRUE)

df_subprime20 <- df_subprime20 %>% 
  mutate(GEOID_TRACT_20 = ifelse(ct_geoid == '06037137000', '06037137000', GEOID_TRACT_20),
         county_id = ifelse(ct_geoid == '06037137000', '06037', county_id),
         wt_val = ifelse(ct_geoid == '06037137000', 1, wt_val))

### Function to aggregate data for total and by race: county, city, assm, sen  -----------------------
# data dictionary: https://files.consumerfinance.gov/hmda-historic-data-dictionaries/lar_record_codes.pdf
calculations <- function(df,geoid,column) {
  ## total 
  total <- df %>% group_by({{geoid}}) %>% summarize(total_observations = sum(wt_val, na.rm=TRUE))
  
  ## nh white
  nh_white <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "5" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_white_observations = sum(wt_val, na.rm=TRUE))
  
  ## nh asian
  nh_asian <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "2" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_asian_observations = sum(wt_val, na.rm=TRUE))
  
  ## nh black
  nh_black <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "3" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_black_observations = sum(wt_val, na.rm=TRUE))
  
  ## all pacisl 
  pacisl <- df %>% filter(applicant_race_1 == "4") %>% group_by({{geoid}}) %>% summarize(pacisl_observations = sum(wt_val, na.rm=TRUE))
  
  ## all aian
  aian <- df %>% filter(applicant_race_1 == "1") %>% group_by({{geoid}}) %>% summarize(aian_observations = sum(wt_val, na.rm=TRUE))
  
  ## nh two or more
  nh_twoormor <- df %>% filter(applicant_ethnicity == "2" & !is.na(applicant_race_1) & !is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_twoormor_observations = sum(wt_val, na.rm=TRUE))
  
  ## latino
  latino <- df %>% filter(applicant_ethnicity == "1") %>% group_by({{geoid}}) %>% summarize(latino_observations = sum(wt_val, na.rm=TRUE))
  
  z <- total %>% full_join(nh_white) %>% full_join(nh_asian) %>% full_join(nh_black) %>% full_join(pacisl) %>% full_join(aian) %>% full_join(nh_twoormor) %>% full_join(latino) %>% rename_all(
    funs(stringr::str_replace_all(., 'observations', column)
    ))
  
  return(z)
}

### City Calcs ####
# merge applications and subprime with crosswalk
## This is a many-to-many join because a census tract can be assigned to multiple places.
# set source for Crosswalk Function script
source("./Functions/RC_CT_Place_Xwalk.R")
xwalk_city <- make_ct_place_xwalk(xwalk_yr) # must specify which data year

applications_crosswalk <- df_applications20 %>% 
  right_join(xwalk_city, relationship = "many-to-many")    # join keeping only ct's that are in xwalk
subprime_crosswalk <- df_subprime20 %>% 
  right_join(xwalk_city, relationship = "many-to-many")    # join keeping only ct's that are in xwalk

applications_city <- calculations(df = applications_crosswalk, geoid = place_geoid, column = 'applications')
subprime_city <- calculations(df = subprime_crosswalk, geoid = place_geoid,  column = 'subprime')

## Combine applications with subprime
df_city_merged <- applications_city %>% full_join(subprime_city)

# merge with city and name
city_list <- places(state = 'CA', year = xwalk_yr, cb = TRUE) %>% 
  select(c(GEOID, NAME)) %>% st_drop_geometry()
df_city <- df_city_merged %>% 
  left_join(city_list, by=c("place_geoid" = "GEOID")) %>% 
  rename(geoname = NAME, geoid = place_geoid) %>% 
  mutate(geolevel = 'place') %>%
  select(geoid, geoname, geolevel, everything())


### County Calcs #####
applications_county <- calculations(df = df_applications20, geoid = county_id, column = 'applications') 
subprime_county <- calculations(df = df_subprime20, geoid = county_id, column = 'subprime')

## Combine applications with subprime
df_county <- applications_county %>% full_join(subprime_county) %>% rename(geoid = county_id) %>% drop_na(geoid)


### State Calcs #####
# Function to aggregate data for total and by race: state ####
## State-level calcs based on 2010 tract data so that we don't lose records where census_tract is NA.
#### data dictionary: https://files.consumerfinance.gov/hmda-historic-data-dictionaries/lar_record_codes.pdf
calculations_st <- function(df,geoid,column) {
  ## total 
  total <- df %>% group_by({{geoid}}) %>% summarize(total_observations = n())
  
  ## nh white
  nh_white <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "5" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_white_observations = n())
  
  ## nh asian
  nh_asian <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "2" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_asian_observations = n())
  
  ## nh black
  nh_black <- df %>% filter(applicant_ethnicity == "2" & applicant_race_1 == "3" & is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_black_observations = n())
  
  ## all pacisl 
  pacisl <- df %>% filter(applicant_race_1 == "4") %>% group_by({{geoid}}) %>% summarize(pacisl_observations = n())
  
  ## all aian
  aian <- df %>% filter(applicant_race_1 == "1") %>% group_by({{geoid}}) %>% summarize(aian_observations = n())
  
  ## nh two or more
  nh_twoormor <- df %>% filter(applicant_ethnicity == "2" & !is.na(applicant_race_1) & !is.na(applicant_race_2)) %>% group_by({{geoid}}) %>% summarize(nh_twoormor_observations = n())
  
  ## latino
  latino <- df %>% filter(applicant_ethnicity == "1") %>% group_by({{geoid}}) %>% summarize(latino_observations = n())
  
  z <- total %>% full_join(nh_white) %>% full_join(nh_asian) %>% full_join(nh_black) %>% full_join(pacisl) %>% full_join(aian) %>% full_join(nh_twoormor) %>% full_join(latino) %>% rename_all(
    funs(stringr::str_replace_all(., 'observations', column)
    ))
  
  return(z)
}

applications_state <- calculations_st(df = df_applications, geoid = state_code, column = 'applications') %>% mutate(state_code = as.character(paste("0",state_code, sep= "")))
subprime_state <- calculations_st(df = df_subprime, geoid = state_fips,  column = 'subprime') %>% rename(state_code = state_fips)

## Combine applications with subprime
df_state <- applications_state %>% full_join(subprime_state) %>% rename(geoid = state_code)

# Combine county and state
df <- rbind(df_county, df_state)

## get census geoids
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = xwalk_yr)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

#add county geonames
df_county_state <- merge(x=ca, y=df, by="geoid", all=T)

#add state geoname
df_county_state <- within(df_county_state, geoname[geoid == '06'] <- 'California')
df_county_state$geolevel = ifelse(df_county_state$geoid == "06", "state", "county")

### Assembly Calcs ####
### CT-Assm Crosswalk ###
xwalk_assm <- dbGetQuery(con2, paste0("SELECT * FROM crosswalks.", assm_xwalk)) %>%
  rename("assm_geoid" = {assm_geoid}, "geoid" = "geo_id")

# keep ct-dist matches where % of tract pop in district OR % of dist pop in tract >= 25% (same as % area threshold used for tract-city xwalk)
xwalk_assm <- filter(xwalk_assm, (afact >= .25 | afact2 >= .25))       

applications_crosswalk <- df_applications20 %>%
  right_join(xwalk_assm, by = c("ct_geoid" = "geoid"), relationship = "many-to-many")   # join keeping only ct's that are in xwalk

subprime_crosswalk <- df_subprime20 %>%
  right_join(xwalk_assm, by = c("ct_geoid" = "geoid"), relationship = "many-to-many")   # join keeping only ct's that are in xwalk

applications_assm <- calculations(df = applications_crosswalk, geoid = assm_geoid, column = 'applications')

subprime_assm <- calculations(df = subprime_crosswalk, geoid = assm_geoid, column = 'subprime')

## Combine applications with subprime
df_assm_merged <- applications_assm %>% full_join(subprime_assm)

# merge with assm and name
## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
assm_name <- get_acs(geography = "State Legislative District (Lower Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = xwalk_yr)

assm_name <- assm_name[,1:2]
assm_name$NAME <- str_remove(assm_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
assm_name$NAME <- gsub(", California", "", assm_name$NAME)
# View(assm_name)


#add geonames and geolevel
df_assm <- df_assm_merged %>% 
  left_join(assm_name, by=c("assm_geoid" = "GEOID")) %>% 
  rename(geoname = NAME, geoid = assm_geoid) %>% 
  mutate(geolevel = 'sldl') %>%
  select(geoid, geoname, geolevel, everything())


### Senate Calcs ####
### CT-sen Crosswalk ###
xwalk_sen <- dbGetQuery(con2, paste0("SELECT * FROM crosswalks.", sen_xwalk)) %>%
  rename("sen_geoid" = {sen_geoid}, "geoid" = "geo_id")

# keep ct-dist matches where % of tract pop in district OR % of dist pop in tract >= 25% (same as % area threshold used for tract-city xwalk)
xwalk_sen <- filter(xwalk_sen, (afact >= .25 | afact2 >= .25))       

applications_crosswalk <- df_applications20 %>%
  right_join(xwalk_sen, by = c("ct_geoid" = "geoid"), relationship = "many-to-many")    # join keeping only ct's that are in xwalk

subprime_crosswalk <- df_subprime20 %>%
  right_join(xwalk_sen, by = c("ct_geoid" = "geoid"), relationship = "many-to-many")    # join keeping only ct's that are in xwalk

applications_sen <- calculations(df = applications_crosswalk, geoid = sen_geoid, column = 'applications')

subprime_sen <- calculations(df = subprime_crosswalk, geoid = sen_geoid, column = 'subprime')

## Combine applications with subprime
df_sen_merged <- applications_sen %>% full_join(subprime_sen)

# merge with sen and name
## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_acs(geography = "State Legislative District (Upper Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = xwalk_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
sen_name$NAME <- gsub(", California", "", sen_name$NAME)
# View(sen_name)


#add geonames
df_sen <- df_sen_merged %>% 
  left_join(sen_name, by=c("sen_geoid" = "GEOID")) %>% 
  rename(geoname = NAME, geoid = sen_geoid) %>% 
  mutate(geolevel = 'sldu') %>%
  select(geoid, geoname, geolevel, everything())


### JOIN & SCREEN LEGISLATIVE, CITY, COUNTY & STATE TABLES ####
df_final <- rbind(df_city, df_county_state, df_assm, df_sen)

## screen out cities with < threshold loans originated then calculate rates
df_final <- df_final %>% mutate(
  total_raw = ifelse(total_applications < threshold, NA, total_subprime),
  nh_white_raw = ifelse(nh_white_applications < threshold, NA, nh_white_subprime),
  nh_asian_raw = ifelse(nh_asian_applications < threshold, NA, nh_asian_subprime),
  nh_black_raw = ifelse(nh_black_applications < threshold, NA, nh_black_subprime),
  pacisl_raw = ifelse(pacisl_applications < threshold, NA, pacisl_subprime),
  aian_raw = ifelse(aian_applications < threshold, NA, aian_subprime),
  nh_twoormor_raw = ifelse(nh_twoormor_applications < threshold, NA, nh_twoormor_subprime),
  latino_raw = ifelse(latino_applications < threshold, NA, latino_subprime),

  total_rate = (total_raw/total_applications) * 100,
  nh_white_rate = (nh_white_raw/nh_white_applications) * 100,
  nh_asian_rate = (nh_asian_raw/nh_asian_applications) * 100,
  nh_black_rate = (nh_black_raw/nh_black_applications) * 100,
  pacisl_rate = (pacisl_raw/pacisl_applications) * 100,
  aian_rate = (aian_raw/aian_applications) * 100,
  nh_twoormor_rate = (nh_twoormor_raw/nh_twoormor_applications) * 100,
  latino_rate = (latino_raw/latino_applications) * 100)

# create d for functions
d <- df_final


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity

### Split into geolevel tables
#split into STATE, COUNTY, CITY, SLDU, SLDL tables 
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table) %>% dplyr::select(-c(geolevel))
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table) 

## Calc county ranks##
county_table <- calc_ranks(county_table) %>% dplyr::select(-c(geolevel))
View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
city_table <- calc_ranks(city_table) %>% dplyr::select(-c(geolevel))
View(city_table)

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to state_id, county_id, city_id, leg_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")

############### COUNTY, STATE, CITY, LEG METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0("arei_hous_subprime_county_", rc_yr)
state_table_name <- paste0("arei_hous_subprime_state_", rc_yr)
city_table_name <- paste0("arei_hous_subprime_city_", rc_yr)
leg_table_name <- paste0("arei_hous_subprime_leg_", rc_yr)          

indicator <- paste0(" Number of higher priced Loans Per 100 Loans Originated. Subgroups with fewer than ", threshold, " loans originated are excluded")                   
source <- paste0("HMDA historic Data (", hmda_yr, "): https://www.consumerfinance.gov/data-research/hmda/historic-data/, however Subprime data is not available here.")   

#send to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)
# 
# dbDisconnect(con)
# dbDisconnect(con2)
