## Denied Mortgages Applications for RC v7 ##

## install packages if not already installed ------------------------------
packages <- c("openxlsx","tidyr","dplyr","stringr", "DBI", "RPostgres","data.table", "openxlsx", "tidycensus", "tidyverse", "janitor","httr")
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

###### SET UP WORKSPACE #######
source("W:\\RDA Team\\R\\credentials_source.R")
con2 <- connect_to_db("rda_shared_data")

##### You must manually update variables each year ###
new_hmda_yrs = c('2023') # yr(s) of data that need to be downloaded
data_yrs <- c("2019", "2020", "2021", "2022", "2023") # all data yrs included in analysis
tract20_yrs <- c("2022|2023")                         # data yrs based on 2020 tracts = any data_yrs after 2021. must use "|" as separator.
table_schema <- "housing"
acs_yr <- 2020   # ACS yr for pop and final geos
rc_yr <- 2025
rc_schema <- "v7"
table_schema <- 'housing'

# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table    

threshold = 15  # originated loan minimum threshold per city/leg/county/state by race

    

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
# pull in list of data tables needed for calcs
table_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con2, DBI::Id(schema = table_schema))$table, function(x) slot(x, 'name'))))
mtg_tbl_list <- filter(table_list, grepl("hmda_tract_denied_mortgages", table))           # filter for denied mortgage tables
mtg_tbl_list <- mtg_tbl_list[grepl(paste(data_yrs, collapse="|"), mtg_tbl_list$table), ]  # filter for specified data_yrs tables
mtg_tbl_list <- as.list(mtg_tbl_list[!grepl("2019_20", mtg_tbl_list$table), ])            # filter out old multi-yr tables
loan_tbl_list <- filter(table_list, grepl("hmda_tract_loans_originated", table))          # filter for loans originated tables
loan_tbl_list <- loan_tbl_list[grepl(paste(data_yrs, collapse="|"), loan_tbl_list$table), ] # filter for specified data_yrs tables
loan_tbl_list <- as.list(loan_tbl_list[!grepl("2019_20", loan_tbl_list$table), ])         # filter out old multi-yr tables

# Check your lists contain all the required tables
  #mtg_tbl_list
  #loan_tbl_list


# Import data then filter out multifamily housing and subordinate liens. Keep only single family housing and first liens. Replace state_code with FIPS Code.
mtg_tables <- lapply(setNames(paste0("select * from ", table_schema, ".", mtg_tbl_list$table), mtg_tbl_list$table), DBI::dbGetQuery, conn = con2)
loan_tables <- lapply(setNames(paste0("select * from ", table_schema, ".", loan_tbl_list$table), loan_tbl_list$table), DBI::dbGetQuery, conn = con2)


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
denied_data <- map2(denied_data, names(denied_data), ~ mutate(.x, year = .y))                       # create column with dataset name
denied_data <- lapply(denied_data, function(x) {x %>% mutate(year = str_sub(x$year, start = -4))})  # clean $year

loans_data <- map2(loans_data, names(loans_data), ~ mutate(.x, year = .y))                          # create column with dataset name
loans_data <- lapply(loans_data, function(x) {x %>% mutate(year = str_sub(x$year, start = -4))})    # clean $year


# segment 2019-21 and 2022-23 data bc the former uses 2010 CT's while the latter uses 2020 CT's
# 2010 Tract Data
loans_1 <- loans_data[grepl(("2019|2020|2021"), names(loans_data))]
denied_1 <- denied_data[grepl(("2019|2020|2021"), names(denied_data))]

# 2020 Tract Data
loans_2 <- loans_data[grepl((tract20_yrs), names(loans_data))]
denied_2 <- denied_data[grepl((tract20_yrs), names(denied_data))]

# Convert 2019-21 data from 2010 CT's to 2020 CT's
cb_tract_2010_2020 <- fread("W:\\Data\\Geographies\\Relationships\\cb_tract2020_tract2010_st06.txt", sep="|", colClasses = 'character', data.table = FALSE) %>%
  select(GEOID_TRACT_10, NAMELSAD_TRACT_10, AREALAND_TRACT_10, GEOID_TRACT_20, NAMELSAD_TRACT_20, AREALAND_TRACT_20, AREALAND_PART) %>%
  mutate_at(vars(contains("AREALAND")), function(x) as.numeric(x)) %>%
  # calculate overlapping land area of 2010 and 2020 tracts (AREALAND_PART) as a percent of 2020 tract land area (AREALAND_TRACT_20)
  mutate(prc_overlap=AREALAND_PART/AREALAND_TRACT_10, # pct of 2010 tract that is in 2020 tract to ensure 100% of 2010 loans are assigned to 2020 tracts  
         county_id = substr(GEOID_TRACT_20,1,5))
         
loans_1 <- lapply(loans_1, function(x) 
  x %>% left_join(cb_tract_2010_2020, by = c("census_tract"="GEOID_TRACT_10"), relationship = "many-to-many") %>%
    # Allocate data from 2010 tracts to 2020 using prc_overlap
    rename("wt_val"="prc_overlap"))		# 2020 data value set to pct_overlap bc data was converted from 2010 tracts

# check for loans that do not match to 2020 tracts
loans_nomatch <- lapply(loans_1, function(x) x %>% filter(is.na(county_id)) %>% group_by(census_tract) %>% summarise(count = n()))
## There is 1 tract that does not match (06037137000 which is a 2020 tract) with about 140 rows across 2019-21. There are also rows where census_tract is NA.
## Added manual fix above to assign GEOID_TRACT_20 06037137000 for rows with census_tract 06037137000.

loans_1 <- lapply(loans_1, function(x) x %>% 
                     mutate(GEOID_TRACT_20 = ifelse(census_tract == '06037137000', '06037137000', GEOID_TRACT_20),
                            county_id = ifelse(census_tract == '06037137000', '06037', county_id),
                            wt_val = ifelse(census_tract == '06037137000', 1, wt_val)))

loans_2 <- lapply(loans_2, function(x) 
  x %>% mutate(wt_val = 1, county_id = county_code, GEOID_TRACT_20 = census_tract))   # 2020 data value set to 1 bc data is already based on 2020 tracts

# combine all data years
loans_all <- c(loans_1, loans_2)

denied_1 <- lapply(denied_1, function(x) 
  x %>% left_join(cb_tract_2010_2020, by = c("census_tract"="GEOID_TRACT_10"), relationship = "many-to-many") %>%
    # Allocate data from 2010 tracts to 2020 using prc_overlap
    rename("wt_val"="prc_overlap"))		                          # 2020 data value set to pct_overlap bc data was converted from 2010 tracts

# check for denied mtg that do not match to 2020 tracts
denied_nomatch <- lapply(denied_1, function(x) x %>% filter(is.na(county_id)) %>% group_by(census_tract) %>% summarise(count = n()))
## There is 1 tract that does not match (06037137000 a 2020 tract) with about 140 rows across 2019-21. There are also rows where census_tract is NA.
## Added manual fix above to assign GEOID_TRACT_20 06037137000 for rows with census_tract 06037137000.

## manual fix due to the 2019-21 rows that are assigned to 2020 tracts
denied_1 <- lapply(denied_1, function(x) x %>% 
                       mutate(GEOID_TRACT_20 = ifelse(census_tract == '06037137000', '06037137000', GEOID_TRACT_20),
                              county_id = ifelse(census_tract == '06037137000', '06037', county_id),
                              wt_val = ifelse(census_tract == '06037137000', 1, wt_val)))

denied_2 <- lapply(denied_2, function(x) 
  x %>% mutate(wt_val = 1, county_id = county_code, GEOID_TRACT_20 = census_tract))		# 2020 data value set to 1 bc data is already based on 2020 tracts

# combine all data years
denied_all <- c(denied_1, denied_2)


#### FX TO GET RACED AND TOTAL COUNTS ----------------------------------------
get_raced_hmda <- function(z, geoid, geolevel, suffix) { # get raced and total loan or denied mtg counts at county level
  
  latino <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Hispanic or Latino") %>% dplyr::group_by({{geoid}}) %>%
    dplyr::summarise(latino = sum(wt_val, na.rm=TRUE))})
  
  latino <- latino %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(latino = sum(latino, na.rm=TRUE)) %>% as.data.frame()
  
  #aian alone, latinx inclusive
  aian <- lapply(z, function (x) {x <- x %>% filter(derived_race == "American Indian or Alaska Native") %>%
    dplyr::group_by({{geoid}}) %>% dplyr::summarise(aian = sum(wt_val, na.rm=TRUE))})
  
  aian <- aian %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(aian = sum(aian, na.rm=TRUE)) 
  
  #pacisl alone, latinx inclusive
  pacisl <- lapply(z, function (x) {x <- x %>% filter(derived_race == "Native Hawaiian or Other Pacific Islander") %>%
    dplyr::group_by({{geoid}}) %>% dplyr::summarise(pacisl = sum(wt_val, na.rm=TRUE))})
  
  pacisl <- pacisl %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(pacisl = sum(pacisl, na.rm=TRUE)) 
  
  nh_black <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Black or African American") %>%
    dplyr::group_by({{geoid}}) %>% dplyr::summarise(nh_black = sum(wt_val, na.rm=TRUE))})
  
  nh_black <- nh_black %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(nh_black = sum(nh_black, na.rm=TRUE)) 
  
  nh_asian <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "Asian") %>%
    dplyr::group_by({{geoid}}) %>% dplyr::summarise(nh_asian = sum(wt_val, na.rm=TRUE))})
  
  nh_asian <- nh_asian %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(nh_asian = sum(nh_asian, na.rm=TRUE)) 
  
  nh_white <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", derived_race == "White") %>%
    dplyr::group_by({{geoid}}) %>% dplyr::summarise(nh_white = sum(wt_val, na.rm=TRUE))})
  
  nh_white <- nh_white %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(nh_white = sum(nh_white, na.rm=TRUE)) 
  
  nh_twoormor <- lapply(z, function (x) {x <- x %>% filter(derived_ethnicity == "Not Hispanic or Latino", (derived_race == "Joint" | derived_race == "2 or more minority races")) %>%
    dplyr::group_by({{geoid}}) %>% dplyr::summarise(nh_twoormor = sum(wt_val, na.rm=TRUE))})
  
  nh_twoormor <- nh_twoormor %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(nh_twoormor = sum(nh_twoormor, na.rm=TRUE)) 
  
  total <- lapply(z, function (x) {x <- x %>% dplyr::group_by({{geoid}}) %>% dplyr::summarise(total = sum(wt_val, na.rm=TRUE))})
  
  total <- total %>% reduce(full_join) %>% group_by({{geoid}}) %>% summarise(total = sum(total, na.rm=TRUE)) 
  
  # merge all loan city lists
  joined <- total %>% full_join(latino) %>% full_join(aian) %>% full_join(pacisl) %>% full_join(nh_black) %>% full_join(nh_asian) %>% full_join(nh_white) %>% full_join(nh_twoormor)
  
  # add specified suffix to colnames except place_geoid
  joined <- joined %>% rename_at(vars(-c({{geoid}})), ~paste0(., suffix)) %>% mutate(geolevel = {{geolevel}})

  return(joined)
}


############## County / State Counts #############
# get raced data and add column suffixes
loans <- get_raced_hmda(loans_all, county_id, "county", "_originated")
loans_st <- get_raced_hmda(loans_all, state_code, "state", "_originated")

denied <- get_raced_hmda(denied_all, county_id, "county", "_denied")
denied_st <- get_raced_hmda(denied_all, state_code, "state", "_denied")

# merge loan and denied dfs
county_join <- left_join(loans, denied, by = c("county_id", "geolevel")) %>% 
  rename(geoid = county_id)
state_join <- left_join(loans_st, denied_st, by = c("state_code", "geolevel")) %>% 
  rename(geoid = state_code)

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = acs_yr)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
# View(ca)


#add geonames and state row
df_join <- rbind(county_join, state_join)
df_wide <- merge(x=ca,y=df_join,by="geoid", all=T)
df_wide$geoname[df_wide$geoid == '06'] <- 'California'
df_wide <- df_wide %>% filter(!is.na(geoid))
#View(df_wide)

############## City Counts #############
### CT-Place Crosswalk ###
# set source for Crosswalk Function script
source("./Functions/RC_CT_Place_Xwalk.R")
xwalk_city <- make_ct_place_xwalk(acs_yr) # must specify which data year

# Join 2020 tract level data to City-Tract Xwalk. Note: many tracts will not match bc Census Places do NOT cover the entire state.
loans_all_city <- lapply(loans_all, function(x) x %>% right_join(select(xwalk_city, c(ct_geoid, place_geoid)), by = c("GEOID_TRACT_20" = "ct_geoid"), relationship = "many-to-many"))
denied_all_city <- lapply(denied_all, function(x) x %>% right_join(select(xwalk_city, c(ct_geoid, place_geoid)), by = c("GEOID_TRACT_20" = "ct_geoid"), relationship = "many-to-many"))

# get raced data and add column suffixes
loans_city <- get_raced_hmda(loans_all_city, place_geoid, "city", "_originated")
denied_city <- get_raced_hmda(denied_all_city, place_geoid, "city", "_denied")

# merge loan and denied dfs
city_join <- left_join(loans_city, denied_city, by = c("place_geoid", "geolevel")) %>% 
  rename(geoid = place_geoid)


## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
ca <- get_acs(geography = "place", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = acs_yr)

ca <- ca[,1:2]
ca$NAME <- gsub(", California", "", ca$NAME)
ca$NAME <- gsub(" city", "", ca$NAME)
ca$NAME <- gsub(" CDP", "", ca$NAME)
ca$NAME <- gsub(" town", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
# View(ca)


#add geonames
city_join <- merge(x=ca,y=city_join,by="geoid", all=T)


############## Assembly Counts #############
### CT-Assm Crosswalk ###
xwalk_assm <- dbGetQuery(con2, paste0("SELECT * FROM crosswalks.", assm_xwalk)) %>%
  rename("assm_geoid" = {assm_geoid}, "geoid" = "geo_id")

# Join 2020 tract level data to Assm-Tract Xwalk.
loans_all_assm <- lapply(loans_all, function(x) x %>% right_join(select(xwalk_assm, c(assm_geoid, geoid)), by = c("GEOID_TRACT_20" = "geoid"), relationship = "many-to-many"))
denied_all_assm <- lapply(denied_all, function(x) x %>% right_join(select(xwalk_assm, c(assm_geoid, geoid)), by = c("GEOID_TRACT_20" = "geoid"), relationship = "many-to-many"))

# check for loans that do not match to Leg Dist. All records matched to Leg Dist.
#loans_nomatch_assm <- lapply(loans_all_assm, function(x) x %>% filter(is.na({assm_geoid})) %>% group_by(GEOID_TRACT_20) %>% summarise(count = n()))

# get raced data and add column suffixes
loans_assm <- get_raced_hmda(loans_all_assm, assm_geoid, "sldl", "_originated")
denied_assm <- get_raced_hmda(denied_all_assm, assm_geoid, "sldl", "_denied")

# merge loan and denied dfs
assm_join <- left_join(loans_assm, denied_assm, by = c("assm_geoid", "geolevel")) %>%
  rename("geoid" = "assm_geoid")


## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
assm_name <- get_acs(geography = "State Legislative District (Lower Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = acs_yr)

assm_name <- assm_name[,1:2]
assm_name$NAME <- str_remove(assm_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
assm_name$NAME <- gsub(", California", "", assm_name$NAME)
names(assm_name) <- c("geoid", "geoname")
# View(assm_name)


#add geonames
assm_join <- merge(x=assm_name,y=assm_join,by="geoid", all=T)


############## State Senate Counts #############
### CT-Senate Crosswalk ###
xwalk_sen <- dbGetQuery(con2, paste0("SELECT * FROM crosswalks.", sen_xwalk)) %>%
  rename("sen_geoid" = {sen_geoid}, "geoid" = "geo_id")

# Join 2020 tract level data to Assm-Tract Xwalk.
loans_all_sen <- lapply(loans_all, function(x) x %>% right_join(select(xwalk_sen, c(sen_geoid, geoid)), by = c("GEOID_TRACT_20" = "geoid"), relationship = "many-to-many"))
denied_all_sen <- lapply(denied_all, function(x) x %>% right_join(select(xwalk_sen, c(sen_geoid, geoid)), by = c("GEOID_TRACT_20" = "geoid"), relationship = "many-to-many"))

# check for loans that do not match to Leg Dist. All records matched to Leg Dist.
#loans_nomatch_sen <- lapply(loans_all_sen, function(x) x %>% filter(is.na({sen_geoid})) %>% group_by(GEOID_TRACT_20) %>% summarise(count = n()))

# get raced data and add column suffixes
loans_sen <- get_raced_hmda(loans_all_sen, sen_geoid, "sldu", "_originated")
denied_sen <- get_raced_hmda(denied_all_sen, sen_geoid, "sldu", "_denied")

# merge loan and denied dfs
sen_join <- left_join(loans_sen, denied_sen, by = c("sen_geoid", "geolevel")) %>%
  rename("geoid" = "sen_geoid")


## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_acs(geography = "State Legislative District (Upper Chamber)", 
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = acs_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
sen_name$NAME <- gsub(", California", "", sen_name$NAME)
names(sen_name) <- c("geoid", "geoname")
# View(sen_name)


#add geonames
sen_join <- merge(x=sen_name,y=sen_join,by="geoid", all=T)



# Combine all geolevel counts --------------------------------------------------
df_combined <- union(df_wide, city_join) %>% union(assm_join) %>% union(sen_join)


## SCREEN DATA: using originated loan (application) threshold defined at top of script --------------

df_pct <- df_combined %>% 
  mutate(total_pct_denied = ifelse(is.na(total_originated), NA, (total_denied / total_originated)*100),  
          nh_black_pct_denied = ifelse(is.na(nh_black_originated), NA, (nh_black_denied / nh_black_originated)*100),
          aian_pct_denied = ifelse(is.na(aian_originated), NA, (aian_denied / aian_originated)*100),
          nh_asian_pct_denied = ifelse(is.na(nh_asian_originated), NA, (nh_asian_denied / nh_asian_originated)*100),
          latino_pct_denied = ifelse(is.na(latino_originated), NA, (latino_denied / latino_originated)*100),
          pacisl_pct_denied = ifelse(is.na(pacisl_originated), NA, (pacisl_denied / pacisl_originated)*100),
          nh_white_pct_denied = ifelse(is.na(nh_white_originated), NA, (nh_white_denied / nh_white_originated)*100),
          nh_twoormor_pct_denied = ifelse(is.na(nh_twoormor_originated), NA, (nh_twoormor_denied / nh_twoormor_originated)*100),

          # calculate _raw column if _originated column is > than threshold
          total_raw = ifelse(total_originated < threshold, NA, total_denied),
          nh_black_raw = ifelse(nh_black_originated < threshold, NA, nh_black_denied),
          aian_raw = ifelse(aian_originated < threshold, NA, aian_denied),
          nh_asian_raw = ifelse(nh_asian_originated < threshold, NA, nh_asian_denied),
          latino_raw = ifelse(latino_originated < threshold, NA, latino_denied),
          pacisl_raw = ifelse(pacisl_originated < threshold, NA, pacisl_denied),
          nh_white_raw = ifelse(nh_white_originated < threshold, NA, nh_white_denied),
          nh_twoormor_raw = ifelse(nh_twoormor_originated < threshold, NA, nh_twoormor_denied),
              
          # calculate _rate column if _rate column is > than threshold
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

d <- select(df_pct, geoid, geoname, geolevel, ends_with("_originated"), ends_with("_rate"), ends_with("_raw")) %>%
  select(-c(starts_with("nh_twoormor_"))) %>%      # drop two+ bc HMDA is two+ "minority" races and does not match with ACS denominator
  as.data.frame()  

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

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
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel)) %>% as.data.frame() 

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
View(city_table)


#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

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
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
city_table <- rename(city_table, city_id = geoid, city_name = geoname)
leg_table <- rename(leg_table, leg_id = geoid, leg_name = geoname)


###update info for postgres tables###
county_table_name <- paste0("arei_hous_denied_mortgages_county_", rc_yr)
state_table_name <- paste0("arei_hous_denied_mortgages_state_", rc_yr)
city_table_name <- paste0("arei_hous_denied_mortgages_city_", rc_yr)
leg_table_name <- paste0("arei_hous_denied_mortgages_leg_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Denied Mortgages out of all Loan Applications (%). Subgroups with fewer than ", threshold, " loans originated are excluded. This data is")
source <- paste0("HMDA (", paste(data_yrs, collapse = ", "), ") https://ffiec.cfpb.gov/data-browser/")


#send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)

dbDisconnect(con2)