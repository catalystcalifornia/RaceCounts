## Denied Mortgages Applications for RC v5 ##

list.of.packages <- c("openxlsx","tidyr","dplyr","stringr", "DBI", "RPostgreSQL","data.table", "openxlsx", "tidycensus", "tidyverse", "janitor")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#library(plyr)
library(dplyr)
library(DBI)
library(RPostgreSQL)
library(data.table)
library(stringr)
library(openxlsx)
library(tidyr)
library(tidycensus)                                
library(tidyverse)
library(tigris)
library(sf)
library(janitor)
# library(janitor)
options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")
setwd("W:/Data/Housing/HMDA/Denied Mortgages/2019_20/")
con2 <- connect_to_db("rda_shared_data")

# export foreclosure to rda shared table ------------------------------------------------------------
# Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# source("W:\\RDA Team\\R\\credentials_source.R")
# denied_2019 <- read.csv("actions_taken_3_state_CA_2019.csv", colClasses = c(county_code = "character", census_tract = "character"))
# denied_2020 <- read.csv("actions_taken_3_state_CA_2020.csv", colClasses = c(county_code = "character", census_tract = "character"))
# denied_2019_20 <- bind_rows(denied_2019, denied_2020) %>% plyr::rename(c("county_code" = "geoid"))
# denied_2019_20 <- denied_2019_20 %>% mutate(geoid = ifelse(nchar(geoid) == 4, paste0("0",geoid), geoid)) %>%  # add leading zeros to county/ct fips where missing
#                                      mutate(census_tract = ifelse(nchar(census_tract) == 10, paste0("0",census_tract), census_tract))
# table_schema <- "housing"
# table_name <- "hmda_tract_denied_mortgages_2019_20"
# table_comment_source <- "Denied Mortgages out of all Loan Applications with cleaned geoid/census_tract fields"
# table_source <- "HMDA (2019-2020) https://ffiec.cfpb.gov/data-browser/"
# dbWriteTable(con2, c(table_schema, table_name), denied_2019_20, overwrite = FALSE, row.names = FALSE)
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")

# send table comment to database
#dbSendQuery(conn = con2, table_comment)  

# loans_2019 <- read.csv("actions_taken_1_state_CA_2019.csv", colClasses = c(county_code = "character", census_tract = "character"))
# loans_2020 <- read.csv("actions_taken_1_state_CA_2020.csv", colClasses = c(county_code = "character", census_tract = "character"))
# loans_2019_20 <- bind_rows(loans_2019, loans_2020) %>% plyr::rename(c("county_code" = "geoid"))
# loans_2019_20 <- loans_2019_20 %>% mutate(geoid = ifelse(nchar(geoid) == 4, paste0("0",geoid), geoid)) %>%  # add leading zeros to county/ct fips where missing
#                                      mutate(census_tract = ifelse(nchar(census_tract) == 10, paste0("0",census_tract), census_tract))
# table_name <- "hmda_tract_loans_originated_2019_20"
# table_comment_source <- "Loans Originated out of all Loan Applications with cleaned geoid/census_tract fields"
# table_source <- "HMDA (2019-2020) https://ffiec.cfpb.gov/data-browser/"
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# dbWriteTable(con2, c(table_schema, table_name), loans_2019_20, overwrite = FALSE, row.names = FALSE)

# send table comment to database
#dbSendQuery(conn = con2, table_comment)  

# ### DENIED MORTGAGE APPLICATIONS ----------------------------------------
#Get data then filter out multifamily housing and subordinate liens. Keep only single family housing and first liens. Replace state_code with FIPS Code.
denied_2019_20 <- dbGetQuery(con2, "SELECT * FROM housing.hmda_tract_denied_mortgages_2019_20") %>%
           select(activity_year, state_code, geoid, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, derived_ethnicity, derived_race) %>%
  filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
           derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
           derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  & 
           occupancy_type == "1" & str_detect(geoid, "^06")) %>%      # select records where geoid begins with '06'
           mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%  # replace state_code in those records with '06'
           select(state_code, geoid, census_tract, derived_ethnicity, derived_race) 

# ### LOANS ORIGINATED (Applications) -------------------------------------
#Get downloaded data and subset
loans_2019_20 <- dbGetQuery(con2, "SELECT * FROM housing.hmda_tract_loans_originated_2019_20") %>%
          select(activity_year, state_code, geoid, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, derived_ethnicity, derived_race) %>%
  filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
           derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
           derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  &
           occupancy_type == "1" & str_detect(geoid, "^06")) %>%      # select records where geoid begins with '06'
          mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%
          select(state_code, geoid, census_tract, derived_ethnicity, derived_race) 
# head(loans_2019_20)

############## County / State #############
# ######### Loan originations (applications) by race and total ------------
latino <- filter(loans_2019_20, derived_ethnicity == "Hispanic or Latino") %>% dplyr::group_by(geoid) %>%
  dplyr::summarise(latino_originated = n())

nh_black <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_black_originated = n())

nh_asian <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_asian_originated = n())

nh_white <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_white_originated = n())

nh_twoormor <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_twoormor_originated = n())

#aian alone, latinx inclusive
aian <- filter(loans_2019_20, derived_race == "American Indian or Alaska Native") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(aian_originated = n())

#pacisl alone, latinx inclusive
pacisl <- filter(loans_2019_20, derived_race == "Native Hawaiian or Other Pacific Islander") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(pacisl_originated = n())

total <- loans_2019_20 %>% group_by(geoid) %>% summarise(total_originated = n())

# merge all loan county tables
loans <- left_join(total, nh_black, by = c("geoid")) %>% 
  left_join(nh_asian, by = c("geoid")) %>% 
  left_join(latino, by = c("geoid")) %>% 
  left_join(nh_white, by = c("geoid")) %>% 
  left_join(aian, by = c("geoid")) %>% 
  left_join(pacisl, by = c("geoid")) %>% 
  left_join(nh_twoormor, by = c("geoid"))
# View(loans)


# ########### Denied mortgages by race and total --------------------------
latino <- filter(denied_2019_20, derived_ethnicity== "Hispanic or Latino") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(latino_denied = n())

nh_black <- filter(denied_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_black_denied = n())

nh_asian <- filter(denied_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_asian_denied = n())

nh_white <- filter(denied_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_white_denied = n())

nh_twoormor <- filter(denied_2019_20,derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(nh_twoormor_denied = n())

#aian alone, latinx inclusive
aian <- filter(denied_2019_20, derived_race == "American Indian or Alaska Native") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(aian_denied = n())

#pacisl alone, latinx inclusive
pacisl <- filter(denied_2019_20, derived_race == "Native Hawaiian or Other Pacific Islander") %>%
  dplyr::group_by(geoid) %>% dplyr::summarise(pacisl_denied = n())

total <- denied_2019_20  %>% group_by(geoid) %>% summarise(total_denied = n())

# merge all denied county tables
denied <- left_join(total, nh_black, by = c("geoid")) %>% 
  left_join(nh_asian, by = c("geoid")) %>% 
  left_join(latino, by = c("geoid")) %>% 
  left_join(nh_white, by = c("geoid")) %>% 
  left_join(aian, by = c("geoid")) %>% 
  left_join(pacisl, by = c("geoid")) %>% 
  left_join(nh_twoormor, by = c("geoid"))
#View(denied)

#merge loan and denied dfs
df_join <- left_join(loans, denied, by = c("geoid")) %>% mutate(geolevel = "county")
# View(df_join)


# #get census geonames ----------------------------------------------------
census_api_key(census_key1, overwrite=TRUE)
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
# View(ca)

#add geonames and state row
df_wide <- merge(x=ca,y=df_join,by="geoid", all=T) %>% adorn_totals("row")
df_wide$geoid[df_wide$geoid == 'Total'] <- '06'
df_wide$geoname[df_wide$geoid == '06'] <- 'California'
df_wide$geolevel[df_wide$geoid == '06'] <- 'state'
#View(df_wide)

### CT-Place Crosswalk ### ---------------------------------------------------------------------
# pull in crosswalk
xwalk_filter <- dbGetQuery(con2, "SELECT * FROM crosswalks.ct_place_2020")
#View(xwalk_filter)

############## City #############

#join xwalk filter to loans
loans_2019_20 <- loans_2019_20 %>% right_join(select(xwalk_filter, c(ct_geoid, place_geoid)), by = c("census_tract" = "ct_geoid"), relationship = "many-to-many")  # join target geoids/names

# races----
latino <- filter(loans_2019_20, derived_ethnicity == "Hispanic or Latino") %>% dplyr::group_by(place_geoid) %>%
  dplyr::summarise(latino_originated = n())

nh_black <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_black_originated = n())

nh_asian <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_asian_originated = n())

nh_white <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_white_originated = n())

nh_twoormor <- filter(loans_2019_20, derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_twoormor_originated = n())

#aian alone, latinx inclusive
aian <- filter(loans_2019_20, derived_race == "American Indian or Alaska Native") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(aian_originated = n())

#pacisl alone, latinx inclusive
pacisl <- filter(loans_2019_20, derived_race == "Native Hawaiian or Other Pacific Islander") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(pacisl_originated = n())

total <- loans_2019_20 %>% group_by(place_geoid) %>% summarise(total_originated = n())

# merge all loan county tables
loans <- left_join(total, nh_black, by = c("place_geoid")) %>% 
  left_join(nh_asian, by = c("place_geoid")) %>% 
  left_join(latino, by = c("place_geoid")) %>% 
  left_join(nh_white, by = c("place_geoid")) %>% 
  left_join(aian, by = c("place_geoid")) %>% 
  left_join(pacisl, by = c("place_geoid")) %>% 
  left_join(nh_twoormor, by = c("place_geoid"))
# View(loans)


# ########### Denied mortgages by race and total --------------------------

#join xwalk filter to loans
denied_2019_20 <- denied_2019_20 %>% right_join(select(xwalk_filter, c(ct_geoid, place_geoid)), by = c("census_tract" = "ct_geoid"), relationship = "many-to-many")  # join target geoids/names

# races -----
latino <- filter(denied_2019_20, derived_ethnicity== "Hispanic or Latino") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(latino_denied = n())

nh_black <- filter(denied_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_black_denied = n())

nh_asian <- filter(denied_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_asian_denied = n())

nh_white <- filter(denied_2019_20, derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_white_denied = n())

nh_twoormor <- filter(denied_2019_20,derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(nh_twoormor_denied = n())

#aian alone, latinx inclusive
aian <- filter(denied_2019_20, derived_race == "American Indian or Alaska Native") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(aian_denied = n())

#pacisl alone, latinx inclusive
pacisl <- filter(denied_2019_20, derived_race == "Native Hawaiian or Other Pacific Islander") %>%
  dplyr::group_by(place_geoid) %>% dplyr::summarise(pacisl_denied = n())

total <- denied_2019_20 %>% group_by(place_geoid) %>% summarise(total_denied = n())

# merge all denied county tables
denied <- left_join(total, nh_black, by = c("place_geoid")) %>% 
  left_join(nh_asian, by = c("place_geoid")) %>% 
  left_join(latino, by = c("place_geoid")) %>% 
  left_join(nh_white, by = c("place_geoid")) %>% 
  left_join(aian, by = c("place_geoid")) %>% 
  left_join(pacisl, by = c("place_geoid")) %>% 
  left_join(nh_twoormor, by = c("place_geoid"))
#View(denied)

#merge loan and denied dfs
city_df <- left_join(loans, denied, by = c("place_geoid")) %>% 
  left_join(select(xwalk_filter, c(place_name, place_geoid, county_name)), by = "place_geoid") %>% 
  # left_join(select(loans_2019_20, c(place_geoid, geoid)), by = "place_geoid") %>% 
  mutate(geolevel = "city") %>% 
  dplyr::rename("geoname"="place_name", "geoid"="place_geoid") %>% unique()
# city_df$county_name <- gsub(" County", "", city_df$county_name)
# View(city_df)


city_df <- city_df %>% select(-c(county_name))
combined_df <- rbind(df_wide,city_df) %>% unique()


# # SCREEN DATA: set originated loan (application) threshold --------------
threshold = 15

df_pct <- combined_df %>% 
  mutate( total_pct_denied = ifelse((total_originated) < threshold, NA, (total_denied / total_originated)*100),  
          nh_black_pct_denied = ifelse(is.na(nh_black_originated), NA, (nh_black_denied / nh_black_originated)*100),
          aian_pct_denied = ifelse(is.na(aian_originated), NA, (aian_denied / aian_originated)*100),
          nh_asian_pct_denied = ifelse(is.na(nh_asian_originated), NA, (nh_asian_denied / nh_asian_originated)*100),
          latino_pct_denied = ifelse(is.na(latino_originated), NA, (latino_denied / latino_originated)*100),
          pacisl_pct_denied = ifelse(is.na(pacisl_originated), NA, (pacisl_denied / pacisl_originated)*100),
          nh_white_pct_denied = ifelse(is.na(nh_white_originated), NA, (nh_white_denied / nh_white_originated)*100),
          nh_twoormor_pct_denied = ifelse(is.na(nh_twoormor_originated), NA, (nh_twoormor_denied / nh_twoormor_originated)*100),

          # calculate _raw column if _originated column is not less than threshold
          total_raw = ifelse(total_originated < threshold, NA, total_denied),
          nh_black_raw = ifelse(nh_black_originated < threshold, NA, nh_black_denied),
          aian_raw = ifelse(aian_originated < threshold, NA, aian_denied),
          nh_asian_raw = ifelse(nh_asian_originated < threshold, NA, nh_asian_denied),
          latino_raw = ifelse(latino_originated < threshold, NA, latino_denied),
          pacisl_raw = ifelse(pacisl_originated < threshold, NA, pacisl_denied),
          nh_white_raw = ifelse(nh_white_originated < threshold, NA, nh_white_denied),
          nh_twoormor_raw = ifelse(nh_twoormor_originated < threshold, NA, nh_twoormor_denied),
              
          # calculate _rate column if _rate column is not less than threshold
          total_rate = ifelse(total_originated < threshold, NA, total_pct_denied),
          nh_black_rate = ifelse(nh_black_originated < threshold, NA, nh_black_pct_denied),
          aian_rate = ifelse(aian_originated < threshold, NA, aian_pct_denied),
          nh_asian_rate = ifelse(nh_asian_originated < threshold, NA, nh_asian_pct_denied),
          latino_rate = ifelse(latino_originated < threshold, NA, latino_pct_denied),
          pacisl_rate = ifelse(pacisl_originated < threshold, NA, pacisl_pct_denied),
          nh_white_rate = ifelse(nh_white_originated < threshold, NA, nh_white_pct_denied),
          nh_twoormor_rate = ifelse(nh_twoormor_originated < threshold, NA, nh_twoormor_pct_denied)
    )

View(df_pct)

d <- select(df_pct, geoid, geoname, ends_with("_originated"), ends_with("_rate"), ends_with("_raw"), geolevel) %>% as.data.frame()  

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid") %>% as.data.frame() 
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>% as.data.frame() 
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel)) %>% as.data.frame() 

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)

###update info for postgres tables###
county_table_name <- "arei_hous_denied_mortgages_county_2023"
state_table_name <- "arei_hous_denied_mortgages_state_2023"
city_table_name <- "arei_hous_denied_mortgages_city_2023"
indicator <- "Denied Mortgages out of all Loan Applications (%). Subgroups with fewer than 15 loans originated are excluded. County/state tables use v4 methodology, city table uses v5 methodology (owner-occupied only). This data is"
source <- "HMDA (2019-2020) https://ffiec.cfpb.gov/data-browser/"
rc_schema <- 'v5'

#send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)