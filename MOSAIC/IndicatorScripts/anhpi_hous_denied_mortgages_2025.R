## MOSAIC: Disaggregated Asian/NHPIDenied Mortgages Applications ###

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
con <- connect_to_db("mosaic")
con2 <- connect_to_db("rda_shared_data")

##### You must manually update variables each year ###
data_yrs <- c("2019", "2020", "2021", "2022", "2023") # all data yrs included in analysis
tract20_yrs <- c("2022|2023")                         # data yrs based on 2020 tracts = any data_yrs after 2021. must use "|" as separator.
table_schema <- "housing"
acs_yr <- 2020   # ACS yr for pop and final geos
rc_yr <- 2025
rc_schema <- "v7"

threshold = 15  # originated loan minimum threshold per city/leg/county/state by race

#### PREP APPLICANT RACE LIST --------------------
# Source: https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields#applicant_race-1
subgroup_code <- c(1, 2, 21, 22, 23, 24, 25, 26, 27, 3 , 4, 41, 42, 43, 44, 5, 6, 7)
subgroup_label <- c("aian_aoic", "asian_aoic", "indian_aoic", "chinese_aoic", "filipino_aoic", "japanese_aoic", "korean_aoic", "vietnamese_aoic", "other_asian", "black_aoic", "nhpi_aoic", "nat_hawaii_aoic", "guam_chamorro_aoic", "samoan_aoic", "other_pacific_aoic", "white_aoic", "unknown", "na")
subgroup_list <- tibble(subgroup_label, subgroup_code) %>%
  # add binary asian/nhpi race cols
  mutate(asian = case_when(   
           subgroup_code %in% (c(2,21,22,23,24,25,26,27)) ~ 1,
           TRUE ~ 0),
         nhpi = case_when(    
           subgroup_code %in% (c(4,41,42,43,44)) ~ 1,
           TRUE ~ 0))

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
mtg_data <- lapply(mtg_tables, function(x) {x <- x %>% select(activity_year, state_code, county_code, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, "applicant_race-1", "applicant_race-2", "applicant_race-3", "applicant_race-4", "applicant_race-5")})
denied_data <- lapply(mtg_data, function(x) {
                      x <- x %>% filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
                                        derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
                                        derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  & 
                                        occupancy_type == "1" & str_detect(county_code, "^06")) %>%      # select records where county_code begins with '06'
                                        mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%  # replace state_code in those records with '06'
                                        select(state_code, county_code, census_tract, , "applicant_race-1", "applicant_race-2", "applicant_race-3", "applicant_race-4", "applicant_race-5")})

loan_data <- lapply(loan_tables, function(x) {x <- x %>% select(activity_year, state_code, county_code, census_tract, loan_purpose, occupancy_type, derived_loan_product_type, derived_dwelling_category, "applicant_race-1", "applicant_race-2", "applicant_race-3", "applicant_race-4", "applicant_race-5")})
loans_data <- lapply(loan_data, function(x) {x <- x %>% filter(derived_dwelling_category != 'Multifamily:Site-Built' & derived_dwelling_category != 'Multifamily:Manufactured' &
                                                                   derived_loan_product_type != 'Conventional:Subordinate Lien' & derived_loan_product_type != 'FHA:Subordinate Lien' &
                                                                   derived_loan_product_type != 'FSA/RHS:Subordinate Lien' & derived_loan_product_type != 'VA:Subordinate Lien'  & 
                                                                   occupancy_type == "1" & str_detect(county_code, "^06")) %>%      # select records where county_code begins with '06'
                                                                   mutate(state_code = replace(state_code, str_detect(state_code, "CA"), "06")) %>%  # replace state_code in those records with '06'
                                                                   select(state_code, county_code, census_tract, , "applicant_race-1", "applicant_race-2", "applicant_race-3", "applicant_race-4", "applicant_race-5")})

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
cb_tract_2010_2020 <- read.table("https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_st06.txt", header = TRUE, colClasses = c(GEOID_TRACT_10 = "character", GEOID_TRACT_20 = "character"), sep = "|") %>%
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
## There is 1 tract that does not match (06037137000 which is a 2020 tract) with about 140 rows across 2019-21. There are also rows where census_tract is NA.
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


#### RECLASS RACE ----------------------------------------
# Fx to add subgroup_labels and new binary cols for each applicant_race-x value
anhpi_reclass <- function(x, subgroup_list) {  
  # x = hmda dataframe
  # subgroup_list = dataframe containing all applicant_race-x codes and labels, plus 1 binary column for asian and 1 for nhpi
  
  x <- x %>% left_join(subgroup_list %>% select(subgroup_code, subgroup_label), by = c("applicant_race-1" = "subgroup_code"))
  x <- x %>% left_join(subgroup_list %>% select(subgroup_code, subgroup_label), by = c("applicant_race-2" = "subgroup_code"))
  x <- x %>% left_join(subgroup_list %>% select(subgroup_code, subgroup_label), by = c("applicant_race-3" = "subgroup_code"))
  x <- x %>% left_join(subgroup_list %>% select(subgroup_code, subgroup_label), by = c("applicant_race-4" = "subgroup_code"))
  x <- x %>% left_join(subgroup_list %>% select(subgroup_code, subgroup_label), by = c("applicant_race-5" = "subgroup_code"))
  
  # get all unique subgroup labels to use as new column names
  subgroup_vals <- unique(subgroup_list$subgroup_label)
  
  for (sg in subgroup_vals) {
    x <- x %>%
      mutate("{sg}" := if_else(
        if_any(starts_with("subgroup_label"), ~ . == sg), 1L, 0L
      ))
  }
  
return(x)
}

# apply race reclass fx
reclass_denied <- map(denied_all, ~ anhpi_reclass(.x, subgroup_list))
reclass_loans <- map(loans_all, ~ anhpi_reclass(.x, subgroup_list))


# Check reclass
nrow(denied_all[[5]] %>% filter(if_any(starts_with("applicant_race-"), ~ . == 24)))
nrow(filter(reclass_denied[[5]], japanese_aoic == 1))

#### FX TO GET SUBGROUP AND TOTAL COUNTS ----------------------------------------
# Fx to calc counts by subgroup for each geo for each year using weighted values
calc_counts <- function(x, subgroup_list, geo, type) {
  x %>%
    group_by(across(all_of(geo))) %>%
    summarise(
      across(
        all_of(subgroup_list$subgroup_label),
        \(.) if (all(is.na(.))) NA_real_ else sum(. * wt_val, na.rm = TRUE)
      )  
    ) %>%
    rename_with(~ paste0(., "_", type), all_of(subgroup_list$subgroup_label)) %>%
    ungroup() %>%
    rename(geoid = all_of(geo))
}


# apply calc_counts fx
denied_c <- map(reclass_denied, ~ calc_counts(.x, subgroup_list, "county_code", "denied"))
denied_st <- map(reclass_denied, ~ calc_counts(.x, subgroup_list, "state_code", "denied"))
loans_c <- map(reclass_loans, ~ calc_counts(.x, subgroup_list, "county_code", "originated"))
loans_st <- map(reclass_loans, ~ calc_counts(.x, subgroup_list, "state_code", "originated"))

# Check calcs - avg wt * count of rows should should match the related value in denied_c view
reclass_denied[[1]] %>% filter(samoan_aoic == 1 & county_code == '06037') %>% summarise(mean(wt_val, na.rm=TRUE)) # avg wt - 0.8
View(reclass_denied[[1]] %>% filter(samoan_aoic == 1 & county_code == '06037'))  # count of rows - 30
View(denied_c[[1]] %>% filter(geoid == '06037'))  # 24

reclass_loans[[1]] %>% filter(vietnamese_aoic == 1) %>% summarise(mean(wt_val, na.rm=TRUE)) # avg wt - 0.5721871
View(reclass_loans[[1]] %>% filter(vietnamese_aoic == 1))  # count of rows - 14,879
View(loans_st[[1]])  # 8,513


# Fx to aggregate counts by subgroup for each geo for all years
agg_counts <- function(counts_list1, counts_list2, geo) {
  c(counts_list1, counts_list2) %>%
    bind_rows() %>%
    group_by(across(all_of(geo))) %>%
    summarise(
      across(
        everything(),
        \(.) if (all(is.na(.))) NA_integer_ else sum(., na.rm = TRUE)
      )
    ) %>%
    ungroup()
}

denied <- agg_counts(denied_c, denied_st, "geoid")
loans <- agg_counts(loans_c, loans_st, "geoid")

# check state aggregation is sum of county counts
filter(denied %>% filter(geoid == '06')) %>% select(asian_aoic_denied)  # 97,944
sum(denied$asian_aoic_denied, na.rm=TRUE) - 97944  # should equal 97,944

sum(denied$asian_aoic_denied[denied$geoid != '06'], na.rm=TRUE)  # sum of counties # 97,944
denied$asian_aoic_denied[denied$geoid == '06']                    # state row - should match # should equal 97,944

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
df_join <- loans %>% full_join(denied, by = "geoid")
df_wide <- merge(x=ca,y=df_join,by="geoid", all=T)
df_wide$geoname[df_wide$geoid == '06'] <- 'California'
df_wide <- df_wide %>% filter(!is.na(geoid))
df_wide$geolevel <- ifelse(df_wide$geoid == '06', 'state', 'county')


## CALC RATES & SCREEN DATA: using originated loan (application) threshold defined at top of script --------------
# Fx to calc subgroup rates
calc_rates <- function(x, subgroup_list) {
  for (sg in subgroup_list$subgroup_label) {
    denied_col     <- paste0(sg, "_denied")
    originated_col <- paste0(sg, "_originated")
    rate_col       <- paste0(sg, "_rate")
    
    x <- x %>%
      mutate("{rate_col}" := if_else(
        is.na(.data[[denied_col]]) | is.na(.data[[originated_col]]),
        NA_real_,
        .data[[denied_col]] / .data[[originated_col]] * 100
      ))
  }
  return(x)
}

df_combined <- calc_rates(df_wide, subgroup_list)

# check rate calcs
df_wide %>% filter(geoid == '06') %>% summarise(rate = korean_aoic_denied / korean_aoic_originated * 100) # result below should match
df_combined %>% filter(geoid == '06') %>% select(korean_aoic_rate)
df_combined %>% filter(geoid == '06105') %>% select(starts_with("japanese")) # check NA rate is returned when _denied is NA

# Fx to screen data
screen_calc <- function(x, subgroup_list, threshold) {
  for (sg in subgroup_list$subgroup_label) {
    denied_col     <- paste0(sg, "_denied")
    originated_col <- paste0(sg, "_originated")
    raw_col        <- paste0(sg, "_raw")
    rate_col       <- paste0(sg, "_rate")
    
    x <- x %>%
      mutate(
        "{raw_col}"  := if_else(.data[[originated_col]] > threshold, .data[[denied_col]], NA_real_),
        "{rate_col}" := if_else(.data[[originated_col]] > threshold, .data[[rate_col]],  NA_real_)
      )
  }
  return(x)
}

df_screen <- screen_calc(df_combined, subgroup_list, threshold)

# Fx to add total cols
calc_totals <- function(x, subgroup_list) {
  x %>%
    mutate(
      total_originated = if_else(
        if_all(ends_with("_originated"), ~ is.na(.)),
        NA_real_,
        rowSums(across(ends_with("_originated")), na.rm = TRUE)
      ),
      total_raw = if_else(  # unscreened
        if_all(ends_with("_denied"), ~ is.na(.)),
        NA_real_,
        rowSums(across(ends_with("_denied")), na.rm = TRUE)
      ),
      total_denied = if_else(
        if_all(ends_with("_denied"), ~ is.na(.)),
        NA_real_,
        rowSums(across(ends_with("_denied")), na.rm = TRUE)
      )
    ) %>%
    # screened total_raw / total_rate
    mutate(
      total_raw = if_else(total_originated > threshold, total_raw, NA_real_)
    ) %>%
    mutate(
      total_rate = if_else(
        (is.na(total_originated) | is.na(total_denied) | total_originated == 0), NA_real_,
        total_raw / total_originated * 100
      )
    )
}

df_screen <- calc_totals(df_screen, subgroup_list)

# Check screen worked
df_combined %>% filter(geoid == '06011') %>% select(filipino_aoic_originated, filipino_aoic_rate)
df_screen %>% filter(geoid == '06011') %>% select(filipino_aoic_originated, filipino_aoic_raw, filipino_aoic_rate)

df_screen <- select(df_screen, geoid, geoname, geolevel, ends_with("_originated"), ends_with("_rate"), ends_with("_raw")) 


############## SPLIT INTO ASIAN AND NHPI TABLES ##############
# Fx to split into two tables
split_tables <- function(x, subgroup_list) {
  
  suffixes <- c("_originated", "_rate", "_raw")
  
  # get subgroup labels for each category
  asian_subgroups <- subgroup_list %>% filter(asian == 1) %>% pull(subgroup_label)
  nhpi_subgroups  <- subgroup_list %>% filter(nhpi == 1)  %>% pull(subgroup_label)
  other_subgroups <- subgroup_list %>% filter(asian == 0, nhpi == 0) %>% pull(subgroup_label)
  total_subgroups <- colnames(x %>% select(starts_with("total")))
  
  # build col names for all suffixes
  asian_cols <- as.vector(outer(asian_subgroups, suffixes, paste0))
  nhpi_cols  <- as.vector(outer(nhpi_subgroups,  suffixes, paste0))
  other_cols <- as.vector(outer(other_subgroups, suffixes, paste0))
  total_cols <- as.vector(total_subgroups)
    
  base_cols <- c("geoid", "geoname", "geolevel")
  
  asian_table <- x %>% select(all_of(c(base_cols, asian_cols, other_cols, total_cols)))
  nhpi_table  <- x %>% select(all_of(c(base_cols, nhpi_cols,  other_cols, total_cols)))
  
  list(asian = asian_table, nhpi = nhpi_table)
}

tables <- split_tables(df_screen, subgroup_list)
asian_df <- tables$asian %>% 
  select(-matches("^na_|^unknown_|^white_|^black_|^aian_")) # remove groups not in Asian or NHPI
nhpi_df  <- tables$nhpi %>% 
  select(-matches("^na_|^unknown_|^white_|^black_|^aian_")) # remove groups not in Asian or NHPI

############## ASIAN: CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

d <- asian_df
race <- 'asian'

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
state_table <- calc_state_z(state_table) %>% select(-geolevel)
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table) %>% select(-geolevel)
View(county_table)


#rename geoid to state_id, county_id, city_id, leg_id
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
county_table <- rename(county_table, county_id = geoid, county_name = geoname) %>%
  select(where(~!all(is.na(.))))    # drop cols where all values are NA

###update info for postgres tables###
county_table_name <- paste0(race,"_hous_denied_mortgages_county_", rc_yr)
state_table_name <- paste0(race, "_hous_denied_mortgages_state_", rc_yr)

indicator <- paste0("Denied Mortgages out of all Loan Applications (%). Includes ", str_to_title(race), " subgroups and other groups from applicant_race-x cols. Subgroups with fewer than ", threshold, " loans originated are excluded. This data is")
source <- paste0("HMDA (", paste(data_yrs, collapse = ", "), ") https://ffiec.cfpb.gov/data-browser/")


#send tables to postgres
# to_postgres(county_table, state_table, "mosaic")



############## NHPI: CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

d <- nhpi_df
race <- 'nhpi'

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
state_table <- calc_state_z(state_table) %>% select(-geolevel)
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table) %>% select(-geolevel)
View(county_table)


#rename geoid to state_id, county_id, city_id, leg_id
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
county_table <- rename(county_table, county_id = geoid, county_name = geoname) %>%
  select(where(~!all(is.na(.))))    # drop cols where all values are NA

###update info for postgres tables###
county_table_name <- paste0(race,"_hous_denied_mortgages_county_", rc_yr)
state_table_name <- paste0(race, "_hous_denied_mortgages_state_", rc_yr)

indicator <- paste0("Denied Mortgages out of all Loan Applications (%). Includes ", str_to_title(race), " subgroups and other groups from applicant_race-x cols. Subgroups with fewer than ", threshold, " loans originated are excluded. This data is")
source <- paste0("HMDA (", paste(data_yrs, collapse = ", "), ") https://ffiec.cfpb.gov/data-browser/")


#send tables to postgres
# to_postgres(county_table, state_table, "mosaic")

dbDisconnect(con)
dbDisconnect(con2)