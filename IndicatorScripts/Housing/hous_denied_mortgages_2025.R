
## Denied Mortgages Applications for RC v7 ##


list.of.packages <- c("openxlsx","tidyr","dplyr","stringr", "DBI", "RPostgreSQL","data.table", "openxlsx", "tidycensus", "tidyverse", "janitor")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)


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
con2 <- connect_to_db("rda_shared_data")

##### You must manually update variables each year ###

    new_hmda_yrs = c('2023') # yrs of data that need to be downloaded
    data_yrs <- c("2019", "2020", "2021", "2022", "2023") # all data yrs included in analysis
    table_schema <- "housing"
    rc_yr <- 2025
    rc_schema <- "v7"

#### City Counts section also requires update ####    
        

#### PULL DATA FROM HMDA API -------------------------------------------
### If the most current denied mortgage and loans originated tables are already in postgres, then skip to PREP DENIED MORTGAGES code chunk.
####### Info on HMDA API here: https://ffiec.cfpb.gov/documentation/api/data-browser/


# # NOTE: This loop may take up to 10 minutes to run...
# hmda_list <- list()
#  for (i in new_hmda_yrs) {  ### Denied Mortgages
# 
#    hmda_list[[paste0("hmda_tract_denied_mortgages_",i)]] <- GET(url=paste0("https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=CA&actions_taken=3&years=",i)) %>% content("parsed")
# 
#  }
# 
# 
# ## NOTE: This loop may take up to 10 minutes to run...
# hmda_list2 <- list()
# for (i in new_hmda_yrs) {   ### Loans Originated (aka applications)
# 
#   hmda_list2[[paste0("hmda_tract_loans_originated_",i)]] <- GET(url=paste0("https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=CA&actions_taken=1&years=",i)) %>% content("parsed")
# 
# }



#### CREATE RDA_SHARED_DATA TABLES -------------------------------------------
# Check if census tract, county code and state are the correct length bc in the past there have been some geoid errors.
## If not, then leading zeroes may be missing or there may be other errors to check out.


    # for (i in 1:length(hmda_list)) {
    #   print(paste0(names(hmda_list[i]), " has ", (nrow(filter(hmda_list[[i]], nchar(hmda_list[[i]]$census_tract) != 11))), " census_tract errors"))
    #   print(paste0(names(hmda_list[i]), " has ", (nrow(filter(hmda_list[[i]], nchar(hmda_list[[i]]$county_code) != 5))), " county_code errors"))
    #   print(paste0(names(hmda_list[i]), " has ", (nrow(filter(hmda_list[[i]], hmda_list[[i]]$state_code != "CA"))), " state_code errors"))
    # }
    # 
    # for (i in 1:length(hmda_list2)) {
    #   print(paste0(names(hmda_list2[i]), " has ", (nrow(filter(hmda_list2[[i]], nchar(hmda_list2[[i]]$census_tract) != 11))), " census_tract errors"))
    #   print(paste0(names(hmda_list2[i]), " has ", (nrow(filter(hmda_list2[[i]], nchar(hmda_list2[[i]]$county_code) != 5))), " county_code errors"))
    #   print(paste0(names(hmda_list2[i]), " has ", (nrow(filter(hmda_list2[[i]], hmda_list2[[i]]$state_code != "CA"))), " state_code errors"))
    # }

      
# Denied Mortgages
      # for(i in 1:length(hmda_list)){
      #   hmda_names <- names(hmda_list)
      #   table_name <- hmda_names[i]
      #   table_comment_source <- "Denied Mortgages out of all Loan Applications"
      #   table_source <- paste0("HMDA https://ffiec.cfpb.gov/data-browser/ downloaded on ", Sys.Date())
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
      #   table_source <- paste0("HMDA https://ffiec.cfpb.gov/data-browser/ downloaded on ", Sys.Date())
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
table_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con2, DBI::Id(schema = curr_schema))$table, function(x) slot(x, 'name'))))
mtg_tbl_list <- filter(table_list, grepl("hmda_tract_denied_mortgages", table))   # filter for denied mortgage tables
mtg_tbl_list <- mtg_tbl_list[grepl(paste(data_yrs, collapse="|"), mtg_tbl_list$table), ]  # filter for specified data_yrs tables
mtg_tbl_list <- mtg_tbl_list[!grepl("2019_20", mtg_tbl_list)] # filter out old multi-yr tables
loan_tbl_list <- filter(table_list, grepl("hmda_tract_loans_originated", table))  # filter for loans originated tables
loan_tbl_list <- loan_tbl_list[grepl(paste(data_yrs, collapse="|"), loan_tbl_list$table), ] # filter for specified data_yrs tables
loan_tbl_list <- loan_tbl_list[!grepl("2019_20", loan_tbl_list)] # filter out old multi-yr tables

# Check your lists contain all the required tables

      #mtg_tbl_list
      #loan_tbl_list


# Import data then filter out multifamily housing and subordinate liens. Keep only single family housing and first liens. Replace state_code with FIPS Code.
mtg_tables <- lapply(setNames(paste0("select * from ", table_schema, ".", mtg_tbl_list), mtg_tbl_list), DBI::dbGetQuery, conn = con2)
loan_tables <- lapply(setNames(paste0("select * from ", table_schema, ".", loan_tbl_list), loan_tbl_list), DBI::dbGetQuery, conn = con2)


# keep only needed columns and filter
mtg_data <- lapply(mtg_tables, function(x) {x <- x %>% select(activity_year, state_code, county_code, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, derived_ethnicity, derived_race)})
denied_data <- lapply(mtg_data, function(x) {
                      x <- x %>% filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
                                        derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
                                        derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  & 
                                        occupancy_type == "1" & str_detect(county_code, "^06")) %>%      # select records where county_code begins with '06'
                                        mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%  # replace state_code in those records with '06'
                                        select(state_code, county_code, census_tract, derived_ethnicity, derived_race)})

loan_data <- lapply(loan_tables, function(x) {x <- x %>% select(activity_year, state_code, county_code, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, derived_ethnicity, derived_race)})
loans_data <- lapply(loan_data, function(x) {x <- x %>% filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
                                                                   derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
                                                                   derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  & 
                                                                   occupancy_type == "1" & str_detect(county_code, "^06")) %>%      # select records where county_code begins with '06'
                                                                   mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%  # replace state_code in those records with '06'
                                                                   select(state_code, county_code, census_tract, derived_ethnicity, derived_race)})

# add data yr to each list element: this will be used for city-level data
denied_data <- map2(denied_data, names(denied_data), ~ mutate(.x, year = .y)) # create column with dataset name
denied_data <- lapply(denied_data, function(x) {x %>% mutate(year = str_sub(x$year, start = -4))}) # clean $year

loans_data <- map2(loans_data, names(loans_data), ~ mutate(.x, year = .y)) # create column with dataset name
loans_data <- lapply(loans_data, function(x) {x %>% mutate(year = str_sub(x$year, start = -4))}) # clean $year


############## County / State Counts #############
# fx to get raced and total counts
get_raced_hmda <- function(z, suffix) { # get raced and total loan or denied mtg counts at county level
  
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

get_raced_hmda_st <- function(z, suffix) { # exact same as get_raced_hmda just for state level
  
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

# merge loan and denied dfs
county_join <- left_join(loans, denied, by = c("county_code", "geolevel")) %>% rename(geoid = county_code)
state_join <- left_join(loans_st, denied_st, by = c("state_code", "geolevel")) %>% rename(geoid = state_code)
df_join <- rbind(county_join, state_join)


## Add census geonames
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

############## City Counts #############
### CT-Place Crosswalk ###
# pull in crosswalks - 2019-21 HMDA data uses 2011-15 geos, 2022 HMDA data uses 2020 geos
# set source for Crosswalk Function script
source("W:/Project/RACE COUNTS/Functions/RC_CT_Place_Xwalk.R")
xwalk_filter_15 <- make_ct_place_xwalk("2015") # must specify which data year
xwalk_filter_20 <- make_ct_place_xwalk("2020") # must specify which data year

# segment 2019-21 and 2022 data bc the former uses 2015 CT's while the latter uses 2020 CT's
# 2019-21 Data
loans_2019_21 <- loans_data[grepl(("2019|2020|2021"), names(loans_data))]
denied_2019_21 <- denied_data[grepl(("2019|2020|2021"), names(denied_data))]

# 2022 Data
loans_2022 <- loans_data[grepl(("2022"), names(loans_data))]
denied_2022 <- denied_data[grepl(("2022"), names(denied_data))]

#join xwalk filter to data -- join city geoids/names
join_xwalk <- function(df, xwalk) {
  lapply(df, function(x) x <- x %>% right_join(select(xwalk, c(ct_geoid, place_geoid, place_name)), by = c("census_tract" = "ct_geoid"), relationship = "many-to-many"))
  }

loans_2019_22 <- c(join_xwalk(loans_2019_21, xwalk_filter_15), join_xwalk(loans_2022, xwalk_filter_20))
denied_2019_22 <- c(join_xwalk(denied_2019_21, xwalk_filter_15), join_xwalk(denied_2022, xwalk_filter_20))


# fx to get raced and total counts
get_raced_hmda_city <- function(z, suffix) { # get raced and total loan or denied mtg counts at city level
  
  latino <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Hispanic or Latino") %>% dplyr::group_by(place_geoid, place_name) %>%
    dplyr::summarise(latino = n())})
  
  latino <- latino %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(latino = sum(latino)) %>% as.data.frame()
  
  #aian alone, latinx inclusive
  aian <- lapply(z, function (x) {x <- x %>% filter(derived_race == "American Indian or Alaska Native") %>%
    dplyr::group_by(place_geoid, place_name) %>% dplyr::summarise(aian = n())})
  
  aian <- aian %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(aian = sum(aian)) 
  
  #pacisl alone, latinx inclusive
  pacisl <- lapply(z, function (x) {x <- x %>% filter(derived_race == "Native Hawaiian or Other Pacific Islander") %>%
    dplyr::group_by(place_geoid, place_name) %>% dplyr::summarise(pacisl = n())})
  
  pacisl <- pacisl %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(pacisl = sum(pacisl)) 
  
  nh_black <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
    dplyr::group_by(place_geoid, place_name) %>% dplyr::summarise(nh_black = n())})
  
  nh_black <- nh_black %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(nh_black = sum(nh_black)) 
  
  nh_asian <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
    dplyr::group_by(place_geoid, place_name) %>% dplyr::summarise(nh_asian = n())})
  
  nh_asian <- nh_asian %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(nh_asian = sum(nh_asian)) 
  
  nh_white <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
    dplyr::group_by(place_geoid, place_name) %>% dplyr::summarise(nh_white = n())})
  
  nh_white <- nh_white %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(nh_white = sum(nh_white)) 
  
  nh_twoormor <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
    dplyr::group_by(place_geoid, place_name) %>% dplyr::summarise(nh_twoormor = n())})
  
  nh_twoormor <- nh_twoormor %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(nh_twoormor = sum(nh_twoormor)) 
  
  total <- lapply(z, function (x) {x <- x %>% dplyr::group_by(place_geoid, place_name) %>% dplyr::summarise(total = n())})
  
  total <- total %>% reduce(full_join) %>% group_by(place_geoid, place_name) %>% summarise(total = sum(total)) 
  
  # merge all loan city lists
  joined <- total %>% full_join(latino) %>% full_join(aian) %>% full_join(pacisl) %>% full_join(nh_black) %>% full_join(nh_asian) %>% full_join(nh_white) %>% full_join(nh_twoormor)
  
  # add specified suffix to colnames except place_geoid
  joined <- joined %>% rename_at(vars(-c(place_geoid, place_name)), ~paste0(., suffix)) %>% mutate(geolevel = 'city')
  
  return(joined)
}

# get raced data and add column suffixes
loans_city_19_22 <- get_raced_hmda_city(loans_2019_22, "_originated")
denied_city_19_22 <- get_raced_hmda_city(denied_2019_22, "_denied")


# combine city loan and city denied tables
city_join <- full_join(loans_city_19_22, denied_city_19_22, by = c("place_geoid", "place_name", "geolevel")) %>% rename(geoid = place_geoid, geoname = place_name)

# combine city with county/state data
df_combined <- rbind(df_wide, city_join)


## SCREEN DATA: set originated loan (application) threshold --------------
threshold = 15

df_pct <- df_combined %>% 
  mutate(total_pct_denied = ifelse((total_originated) < threshold, NA, (total_denied / total_originated)*100),  
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

#View(df_pct)

d <- select(df_pct, geoid, geoname, geolevel, ends_with("_originated"), ends_with("_rate"), ends_with("_raw")) %>% as.data.frame()  

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
#View(d)

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
county_table_name <- paste0("arei_hous_denied_mortgages_county_", rc_yr)
state_table_name <- paste0("arei_hous_denied_mortgages_state_", rc_yr)
city_table_name <- paste0("arei_hous_denied_mortgages_city_", rc_yr)
indicator <- paste0("Created on ", Sys.Date(), ". Denied Mortgages out of all Loan Applications (%). Subgroups with fewer than ", threshold, " loans originated are excluded. This data is")
source <- paste0("HMDA (", paste(data_yrs, collapse = ", "), ") https://ffiec.cfpb.gov/data-browser/")


#send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)