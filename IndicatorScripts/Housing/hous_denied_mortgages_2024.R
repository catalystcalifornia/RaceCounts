## Denied Mortgages Applications for RC v6 ##

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
library(httr) # connect to HMDA API
options(scipen=999)

source("W:\\RDA Team\\R\\credentials_source.R")
#setwd("W:/Data/Housing/HMDA/Denied Mortgages/2019_20/")
con2 <- connect_to_db("rda_shared_data")

##### You must manually update hmda_yrs each year ###
    hmda_yrs = c('2021','2022') 
    table_schema <- "housing"

#### PULL DATA FROM HMDA API -------------------------------------------
### If the most current denied mortgage and loans originated tables are already in postgres, then skip to PREP DENIED MORTGAGES code chunk.
####### Info on HMDA API here: https://ffiec.cfpb.gov/documentation/api/data-browser/

# ## NOTE: This loop may take up to 10 minutes to run...
# hmda_list <- list()
#  for (i in hmda_yrs) {  ### Denied Mortgages
# 
#    hmda_list[[paste0("hmda_tract_denied_mortgages_",i)]] <- GET(url=paste0("https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=CA&actions_taken=3&years=",i)) %>% content("parsed")
# 
#  }
# 
# 
# ## NOTE: This loop may take up to 10 minutes to run...
# hmda_list2 <- list()
# for (i in hmda_yrs) {   ### Loans Originated (aka applications)
# 
#   hmda_list2[[paste0("hmda_tract_loans_originated_",i)]] <- GET(url=paste0("https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=CA&actions_taken=1&years=",i)) %>% content("parsed")
# 
# }


#### CREATE RDA_SHARED_DATA TABLES -------------------------------------------
# Check if census tract, county code and state are the correct length bc in the past there have been some geoid errors.
## If not, then leading zeroes may be missing or there may be other errors to check out.

      # list_elem_num <- 1 # update this to 1 to check the 1st list element, then switch it to 2 to check the 2nd list element etc.
      #       nrow(filter(hmda_list[[list_elem_num]], nchar(hmda_list[[list_elem_num]]$census_tract) != 11))
      #       nrow(filter(hmda_list[[list_elem_num]], nchar(hmda_list[[list_elem_num]]$county_code) != 5))
      #       nrow(filter(hmda_list[[list_elem_num]], hmda_list[[list_elem_num]]$state_code != "CA"))
      #       nrow(filter(hmda_list2[[list_elem_num]], nchar(hmda_list2[[list_elem_num]]$census_tract) != 11))
      #       nrow(filter(hmda_list2[[list_elem_num]], nchar(hmda_list2[[list_elem_num]]$county_code) != 5))
      #       nrow(filter(hmda_list2[[list_elem_num]], hmda_list2[[list_elem_num]]$state_code != "CA"))

      
# Denied Mortgages
      # for(i in 1:length(hmda_list)){
      #   hmda_names <- names(hmda_list)
      #   table_name <- hmda_names[i]
      #   table_comment_source <- "Denied Mortgages out of all Loan Applications"
      #   table_source <- "HMDA https://ffiec.cfpb.gov/data-browser/"
      #   table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
      # 
      #   # send table and comment to postgres
      #   dbWriteTable(con2, c(table_schema,hmda_names[i]), hmda_list[[i]], overwrite = FALSE, row.names = FALSE)
      #   dbSendQuery(conn = con2, table_comment)
      # 
      #   # index the table
      #   dbSendQuery(conn = con2, paste0("create index ", hmda_names[i], "_census_tract on ",
      #                      table_schema, ".", hmda_names[i], " (census_tract);"))
      # 
      # }

# Loans Originated
      # for(i in 1:length(hmda_list2)){
      #   hmda_names <- names(hmda_list2)
      #   table_name <- hmda_names[i]
      #   table_comment_source <- "All Loans Originated (aka loan applications)"
      #   table_source <- "HMDA https://ffiec.cfpb.gov/data-browser/"
      #   table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
      # 
      #   # send table and comment to postgres
      #   dbWriteTable(con2, c(table_schema,hmda_names[i]), hmda_list2[[i]], overwrite = FALSE, row.names = FALSE)
      #   dbSendQuery(conn = con2, table_comment)
      # 
      #   # index the table
      #   dbSendQuery(conn = con2, paste0("create index ", hmda_names[i], "_census_tract on ",
      #                      table_schema, ".", hmda_names[i], " (census_tract);"))
      # 
      # }


#### PREP DENIED MORTGAGE RATES ----------------------------------------
# pull in list tables needed for calcs
data_yrs <- c("2019", "2020", "2021", "2022") ### You must update the data_yrs each year ###

table_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con2, DBI::Id(schema = curr_schema))$table, function(x) slot(x, 'name'))))
mtg_tbl_list <- filter(table_list, grepl("hmda_tract_denied_mortgages",table))   # filter for denied mortgage tables
mtg_tbl_list <- mtg_tbl_list[grepl(paste(data_yrs, collapse="|"), mtg_tbl_list$table), ]  # filter for specified data_yrs tables
loan_tbl_list <- filter(table_list, grepl("hmda_tract_loans_originated",table))  # filter for loans originated tables
loan_tbl_list <- loan_tbl_list[grepl(paste(data_yrs, collapse="|"), loan_tbl_list$table), ] # filter for specified data_yrs tables

# Check your lists contain all the required tables
      #mtg_tbl_list
      #loan_tbl_list

# Import data then filter out multifamily housing and subordinate liens. Keep only single family housing and first liens. Replace state_code with FIPS Code.
mtg_tables <- lapply(setNames(paste0("select * from ", table_schema, ".", mtg_tbl_list), mtg_tbl_list), DBI::dbGetQuery, conn = con2)
loan_tables <- lapply(setNames(paste0("select * from ", table_schema, ".", loan_tbl_list), loan_tbl_list), DBI::dbGetQuery, conn = con2)

# Rename 2019_20 tables geoid col to county_code, so it matches orig data (and 2021 tables and newer)
mtg_tables[["hmda_tract_denied_mortgages_2019_20"]] <- mtg_tables[["hmda_tract_denied_mortgages_2019_20"]] %>% rename(county_code = geoid)
loan_tables[["hmda_tract_loans_originated_2019_20"]] <- loan_tables[["hmda_tract_loans_originated_2019_20"]] %>% rename(county_code = geoid)

# keep only needed columns and filter
mtg_tables <- lapply(mtg_tables, function(x) {x <- x %>% select(activity_year, state_code, county_code, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, derived_ethnicity, derived_race)})
denied_data <- lapply(mtg_tables, function(x) {x <- x %>% filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
                                                                 derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
                                                                 derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  & 
                                                                 occupancy_type == "1" & str_detect(county_code, "^06")) %>%      # select records where county_code begins with '06'
                                                                 mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%  # replace state_code in those records with '06'
                                                                  select(state_code, county_code, census_tract, derived_ethnicity, derived_race)})


loan_tables <- lapply(loan_tables, function(x) {x <- x %>% select(activity_year, state_code, county_code, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, derived_ethnicity, derived_race)})
loans_data <- lapply(loan_tables, function(x) {x <- x %>% filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
                                                                   derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
                                                                   derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  & 
                                                                   occupancy_type == "1" & str_detect(county_code, "^06")) %>%      # select records where county_code begins with '06'
                                                                   mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%  # replace state_code in those records with '06'
                                                                    select(state_code, county_code, census_tract, derived_ethnicity, derived_race)})


############## County / State #############
# fx to get raced and total counts
get_raced_hmda <- function(z, suffix) {
  
  latino <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Hispanic or Latino") %>% dplyr::group_by(county_code) %>%
    dplyr::summarise(latino = n())})
  
    latino <- latino %>% reduce(full_join) %>% group_by(county_code) %>% summarise(latino = sum(latino)) %>% as.data.frame()
  
  #aian alone, latinx inclusive
  aian <- lapply(z, function (x) {x <- x %>% filter(derived_race == "American Indian or Alaska Native") %>%
    dplyr::group_by(county_code) %>% dplyr::summarise(aian = n())})
  
    aian <- aian %>% reduce(full_join) %>% group_by(county_code) %>% summarise(aian = sum(aian)) 
  
  #pacisl alone, latinx inclusive
  pacisl <- lapply(z, function (x) {x <- x %>% filter(derived_race == "Native Hawaiian or Other Pacific Islander") %>%
    dplyr::group_by(county_code) %>% dplyr::summarise(pacisl = n())})
  
    pacisl <- pacisl %>% reduce(full_join) %>% group_by(county_code) %>% summarise(pacisl = sum(pacisl)) 
  
  nh_black <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
    dplyr::group_by(county_code) %>% dplyr::summarise(nh_black = n())})
  
    nh_black <- nh_black %>% reduce(full_join) %>% group_by(county_code) %>% summarise(nh_black = sum(nh_black)) 
  
  nh_asian <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
    dplyr::group_by(county_code) %>% dplyr::summarise(nh_asian = n())})
  
    nh_asian <- nh_asian %>% reduce(full_join) %>% group_by(county_code) %>% summarise(nh_asian = sum(nh_asian)) 
  
  nh_white <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
    dplyr::group_by(county_code) %>% dplyr::summarise(nh_white = n())})
  
    nh_white <- nh_white %>% reduce(full_join) %>% group_by(county_code) %>% summarise(nh_white = sum(nh_white)) 
  
  nh_twoormor <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
    dplyr::group_by(county_code) %>% dplyr::summarise(nh_twoormor = n())})
  
    nh_twoormor <- nh_twoormor %>% reduce(full_join) %>% group_by(county_code) %>% summarise(nh_twoormor = sum(nh_twoormor)) 
  
  total <- lapply(z, function (x) {x <- x %>% dplyr::group_by(county_code) %>% dplyr::summarise(total = n())})
  
    total <- total %>% reduce(full_join) %>% group_by(county_code) %>% summarise(total = sum(total)) 
  
  # merge all loan county lists
  joined <- total %>% full_join(latino) %>% full_join(aian) %>% full_join(pacisl) %>% full_join(nh_black) %>% full_join(nh_asian) %>% full_join(nh_white) %>% full_join(nh_twoormor)

  # add specified suffix to colnames except county_code
  joined <- joined %>% rename_at(vars(-county_code), ~paste0(., suffix)) %>% mutate(geolevel = 'county')
  
  return(joined)
}
get_raced_hmda_st <- function(z, suffix) {
  
  latino <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Hispanic or Latino") %>% dplyr::group_by(state_code) %>%
    dplyr::summarise(latino = n())})
  
  latino <- latino %>% reduce(full_join) %>% group_by(state_code) %>% summarise(latino = sum(latino)) %>% as.data.frame()
  
  #aian alone, latinx inclusive
  aian <- lapply(z, function (x) {x <- x %>% filter(derived_race == "American Indian or Alaska Native") %>%
    dplyr::group_by(state_code) %>% dplyr::summarise(aian = n())})
  
  aian <- aian %>% reduce(full_join) %>% group_by(state_code) %>% summarise(aian = sum(aian)) 
  
  #pacisl alone, latinx inclusive
  pacisl <- lapply(z, function (x) {x <- x %>% filter(derived_race == "Native Hawaiian or Other Pacific Islander") %>%
    dplyr::group_by(state_code) %>% dplyr::summarise(pacisl = n())})
  
  pacisl <- pacisl %>% reduce(full_join) %>% group_by(state_code) %>% summarise(pacisl = sum(pacisl)) 
  
  nh_black <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
    dplyr::group_by(state_code) %>% dplyr::summarise(nh_black = n())})
  
  nh_black <- nh_black %>% reduce(full_join) %>% group_by(state_code) %>% summarise(nh_black = sum(nh_black)) 
  
  nh_asian <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
    dplyr::group_by(state_code) %>% dplyr::summarise(nh_asian = n())})
  
  nh_asian <- nh_asian %>% reduce(full_join) %>% group_by(state_code) %>% summarise(nh_asian = sum(nh_asian)) 
  
  nh_white <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
    dplyr::group_by(state_code) %>% dplyr::summarise(nh_white = n())})
  
  nh_white <- nh_white %>% reduce(full_join) %>% group_by(state_code) %>% summarise(nh_white = sum(nh_white)) 
  
  nh_twoormor <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
    dplyr::group_by(state_code) %>% dplyr::summarise(nh_twoormor = n())})
  
  nh_twoormor <- nh_twoormor %>% reduce(full_join) %>% group_by(state_code) %>% summarise(nh_twoormor = sum(nh_twoormor)) 
  
  total <- lapply(z, function (x) {x <- x %>% dplyr::group_by(state_code) %>% dplyr::summarise(total = n())})
  
  total <- total %>% reduce(full_join) %>% group_by(state_code) %>% summarise(total = sum(total)) 
  
  # merge all loan county lists
  joined <- total %>% full_join(latino) %>% full_join(aian) %>% full_join(pacisl) %>% full_join(nh_black) %>% full_join(nh_asian) %>% full_join(nh_white) %>% full_join(nh_twoormor)
  
  # add specified suffix to colnames except state_code
  joined <- joined %>% rename_at(vars(-state_code), ~paste0(., suffix)) %>% mutate(geolevel = 'state')
  
  return(joined)
}
# get raced data and add column suffixes
loans <- get_raced_hmda(loans_data, "_originated")
loans_st <- get_raced_hmda_st(loans_data, "_originated")

denied <- get_raced_hmda(denied_data, "_denied")
denied_st <- get_raced_hmda_st(denied_data, "_denied")

#merge loan and denied dfs
county_join <- left_join(loans, denied, by = c("county_code", "geolevel")) %>% rename(geoid = county_code)
state_join <- left_join(loans_st, denied_st, by = c("state_code", "geolevel")) %>% rename(geoid = state_code)
df_join <- rbind(county_join, state_join)


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
df_wide <- merge(x=ca,y=df_join,by="geoid", all=T)
df_wide$geoname[df_wide$geoid == '06'] <- 'California'
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