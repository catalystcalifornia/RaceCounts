### Key Takeaways RC v6 ###
####### Produces arei_findings_races_multigeo, arei_findings_places_multigeo, and arei_findings_issues tables

# Packages ----------------------------------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(xfun)

options(scipen=999) # disable scientific notation

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
source("KeyTakeaways/key_findings_functions.R")
con <- connect_to_db("racecounts")
curr_schema <- 'v6' # update each year, this field populates most table and file names automatically
curr_yr <- '2024'   # update each year, this field populates most table and file names automatically

# pull in geo level ids with name. I don't do this directly in the data in case names differ and we have issues merging later
arei_race_multigeo_city <- dbGetQuery(con, paste0("SELECT geoid, name, geolevel FROM ", curr_schema, ".arei_race_multigeo")) %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

arei_race_multigeo_county <- dbGetQuery(con, paste0("SELECT geoid, name, geolevel FROM ", curr_schema, ".arei_race_multigeo")) %>% filter(geolevel == "county") %>% rename(county_id = geoid, county_name = name) %>% select(-geolevel)

arei_race_multigeo_state <- dbGetQuery(con, paste0("SELECT geoid, name, geolevel FROM ", curr_schema, ".arei_race_multigeo")) %>% filter(geolevel == "state") %>% rename(state_id = geoid, state_name = name) %>% select(-geolevel)

# pull in crosswalk to go from district to city
crosswalk <- dbGetQuery(con, paste0("SELECT city_id, dist_id, total_enroll FROM ", curr_schema, ".arei_city_county_district_table"))

########## TO LOAD ALL DATA FROM RDATA FILE AND NOT RE-RUN ALL THE TABLE IMPORT/PREP UNLESS UNDERLYING DATA HAS CHANGED #########
################## SKIP TO LINE ~312 ###

rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_", curr_yr, "';")

rc_list <- dbGetQuery(con, rc_list_query)

# City Data Tables --------------------------------------------------------------------

# filter for final ("api_") city tables, excluding index tables, and alphabetize table list
# note: city education tables are handled separately (district tables)
city_list <- rc_list %>%
  filter(grepl(paste0("^api_.*_city_", curr_yr, "$"), table_name)) %>%
  arrange(table_name) %>% # alphabetize
  pull(table_name) # converts from df object to list; important for next steps using lapply

# import all tables on city_list
city_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", city_list), city_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
city_tables <- map2(city_tables, names(city_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

# create a long df of race disparity values from every api_ city table
city_tables_disparity <- lapply(city_tables, function(x) x %>% select(city_id, asbest, ends_with("disparity_z"), indicator, values_count))

disparity <- imap_dfr(city_tables_disparity, ~ .x %>%
                        pivot_longer(cols = ends_with("disparity_z"),
                                     names_to = "race",
                                     values_to = "disparity_z_score")) %>% 
  mutate(race = (ifelse(race == 'disparity_z', 'total', race)),
         race = gsub('_disparity_z', '', race))

# create a long df of race performance/outcome values from every api_ city table
city_tables_performance <- lapply(city_tables, function(x) x %>% select(city_id, asbest, ends_with("performance_z"), indicator, values_count))


performance <- imap_dfr(city_tables_performance, ~ .x %>%
                          pivot_longer(cols = ends_with("performance_z"),
                                       names_to = "race",
                                       values_to = "performance_z_score")) %>% 
  mutate(race = (ifelse(race == 'performance_z', 'total', race)),
         race = gsub('_performance_z', '', race))

# create a long df of race rates from every api_ city table
city_tables_rate <- lapply(city_tables, function(x) x %>% select(city_id, asbest, ends_with("_rate"), indicator, values_count))

rate <- imap_dfr(city_tables_rate , ~ .x %>%
                   pivot_longer(cols = ends_with("_rate"),
                                names_to = "race",
                                values_to = "rate")) %>% 
  mutate(race = (ifelse(race == 'rate', 'total', race)),
         race = gsub('_rate', '', race))

# merge all 3 (disparity, performance, rate) long dfs
df_merged <- disparity %>% 
  full_join(performance) %>% 
  full_join(rate)

# create issue, indicator, geo_level, race generic columns for issue tables except for education
df_city <- df_merged %>% 
  mutate(
    issue = substring(indicator, 5, 8),
    indicator = substring(indicator, 10),
    indicator = gsub(paste0("_city_",curr_yr), '', indicator),
    geo_level = "city",
    race_generic = gsub('nh_', '', race)) %>%  # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
  left_join(arei_race_multigeo_city) %>% 
  rename(geoid = city_id, geo_name = city_name)

# City (District) Education Tables: must be handled separately bc they are school district not city-level ----------------------------------------
education_list <- rc_list %>%
  filter(grepl(paste0("^api_.*_district_", curr_yr, "$"), table_name)) %>%
  arrange(table_name) %>% # alphabetize
  pull(table_name) # converts from df object to list; important for next steps using lapply

# import all tables on education_list
education_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables <- map2(education_tables, names(education_tables), ~ mutate(.x, indicator = .y))

# create a long df of race disparity values from every api_ district (edu) table
education_tables_disparity <- lapply(education_tables, function(x) x %>% select(dist_id, district_name, asbest, ends_with("disparity_z"), indicator, values_count))

education_disparity <- imap_dfr(education_tables_disparity, ~ .x %>%
                                  pivot_longer(cols = ends_with("disparity_z"),
                                               names_to = "race",
                                               values_to = "disparity_z_score")) %>% 
  mutate(race = (ifelse(race == 'disparity_z', 'total', race)),
         race = gsub('_disparity_z', '', race),
         district_name = str_trim(district_name, "right"))

# create a long df of race performance/outcome values from every api_ district (edu) table
education_tables_performance <- lapply(education_tables, function(x) x %>% select(dist_id, district_name, asbest, ends_with("performance_z"), indicator, values_count))


education_performance <- imap_dfr(education_tables_performance, ~ .x %>%
                                    pivot_longer(cols = ends_with("performance_z"),
                                                 names_to = "race",
                                                 values_to = "performance_z_score")) %>% 
  mutate(race = (ifelse(race == 'performance_z', 'total', race)),
         race = gsub('_performance_z', '', race),
         district_name = str_trim(district_name, "right"))

# create a long df of race rate values from every api_ district (edu) table
education_tables_rate <- lapply(education_tables, function(x) x %>% select(dist_id, district_name, asbest, ends_with("_rate"), indicator, values_count))

education_rate <- imap_dfr(education_tables_rate , ~ .x %>%
                             pivot_longer(cols = ends_with("_rate"),
                                          names_to = "race",
                                          values_to = "rate")) %>% 
  mutate(race = (ifelse(race == 'rate', 'total', race)),
         race = gsub('_rate', '', race),
         district_name = str_trim(district_name, "right"))

# merge all 3 (disparity, performance, rate) long dfs
df_merged_education <- education_disparity %>% 
  full_join(education_performance) %>% 
  full_join(education_rate)

# create issue, indicator, geo_level, race generic columns for education table
df_education_district <- df_merged_education %>% 
  mutate(
    issue = substring(indicator, 5, 8),
    indicator = substring(indicator, 10),
    indicator = gsub(paste0('_district_', curr_yr), '', indicator),
    geo_level = "city",
    race_generic = gsub('nh_', '', race)) %>% # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
  left_join(crosswalk, by = "dist_id", relationship = 'many-to-many') %>% 
  left_join(arei_race_multigeo_city, by = "city_id") %>%
  filter(!is.na(race) & !grepl('University', city_name)) %>% 
  rename(geoid = city_id, geo_name = city_name) %>%
  select(geoid, geo_name, dist_id, district_name, everything())

# County Data Tables ------------------------------------------------------

# filter for only county level indicator tables, drop all others including api_*_county_ current year tables
county_list <- rc_list %>%
  filter(grepl(paste0("^arei_.*_county_", curr_yr, "$"), table_name)) %>%
  arrange(table_name) %>% # alphabetize
  pull(table_name) # converts from df object to list; important for next steps using lapply

# import all tables on county_list
county_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", county_list), county_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
county_tables <- map2(county_tables, names(county_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

# create a long df of race disparity values from every arei_ county table
county_tables_disparity <- lapply(county_tables, function(x) x %>% select(county_id, asbest, ends_with("disparity_z"), indicator, values_count))


county_disparity <- imap_dfr(county_tables_disparity, ~
                               .x %>%
                               pivot_longer(cols = ends_with("disparity_z"),
                                            names_to = "race",
                                            values_to = "disparity_z_score")) %>% 
  mutate(race = (ifelse(race == 'disparity_z', 'total', race)),
         race = gsub('_disparity_z', '', race))

# create a long df of race performance/outcome values from every arei_ county table
county_tables_performance <- lapply(county_tables, function(x) x %>% select(county_id, asbest, ends_with("performance_z"), indicator, values_count))


county_performance <- imap_dfr(county_tables_performance, ~ .x %>%
                                 pivot_longer(cols = ends_with("performance_z"),
                                              names_to = "race",
                                              values_to = "performance_z_score")) %>% 
  mutate(race = (ifelse(race == 'performance_z', 'total', race)),
         race = gsub('_performance_z', '', race))

# create a long df of race rates from every arei_ county table
county_tables_rate <- lapply(county_tables, function(x) x %>% select(county_id, asbest, ends_with("_rate"), indicator, values_count))

county_rate <- imap_dfr(county_tables_rate , ~ .x %>%
                          pivot_longer(cols = ends_with("_rate"),
                                       names_to = "race",
                                       values_to = "rate")) %>% 
  mutate(race = (ifelse(race == 'rate', 'total', race)),
         race = gsub('_rate', '', race))

# merge all 3 (disparity, performance, rate) long dfs
df_merged_county <- county_disparity %>% 
  full_join(county_performance) %>% 
  full_join(county_rate)

# create issue, indicator, geo_level, race generic columns for issue tables
df_county <- df_merged_county %>% 
  mutate(
    issue = substring(indicator, 6,9),
    indicator = substring(indicator, 11),
    indicator = gsub(paste0('_county_', curr_yr), '', indicator),
    geo_level = "county",
    race_generic = gsub('nh_', '', race)) %>% # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
  left_join(arei_race_multigeo_county) %>%
  rename(geoid = county_id, geo_name = county_name)

# State Data Tables -------------------------------------------------------------------
# Note state tables do not have performance z scores
# filter for only state level indicator tables
state_list <- rc_list %>%
  filter(grepl(paste0("^arei_.*_state_", curr_yr, "$"), table_name)) %>%
  arrange(table_name) %>% # alphabetize
  pull(table_name) # converts from df object to list; important for next steps using lapply

# import all tables on state_list
state_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", state_list), state_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
state_tables <- map2(state_tables, names(state_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

# create a long df of race disparity values from every arei_ state table
state_tables_disparity <- lapply(state_tables, function(x) x %>% select(state_id, asbest,ends_with("disparity_z"), indicator, values_count))


state_disparity <- imap_dfr(state_tables_disparity, ~ .x %>%
                              pivot_longer(cols = ends_with("disparity_z"),
                                           names_to = "race",
                                           values_to = "disparity_z_score")) %>%
  mutate(race = (ifelse(race == 'disparity_z', 'total', race)),
         race = gsub('_disparity_z', '', race))

# create a long df of race rates from every arei_ state table
state_tables_rate <- lapply(state_tables, function(x) x %>% select(state_id, asbest, ends_with("_rate"), indicator, values_count))

state_rate <- imap_dfr(state_tables_rate , ~
                         .x %>%
                         pivot_longer(cols = ends_with("_rate"),
                                      names_to = "race",
                                      values_to = "rate")) %>% 
  mutate(race = (ifelse(race == 'rate', 'total', race)),
         race = gsub('_rate', '', race))

# merge all 2 (disparity, rate) long dfs - note: state tables do not have performance z scores
df_merged_state <- state_disparity %>% 
  full_join(state_rate)

# create issue, indicator, geo_level, race generic columns for issue tables
df_state <- df_merged_state %>% 
  mutate(
    issue = substring(indicator, 6,9),
    indicator = substring(indicator, 11),
    indicator = gsub(paste0('_state_', curr_yr), '', indicator),
    geo_level = "state",
    race_generic = gsub('nh_', '', race), # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
    performance_z_score = NA) %>% 
  left_join(arei_race_multigeo_state) %>% 
  rename(geoid = state_id, geo_name = state_name)

# merge city, county, state data (educ tables are excluded)
df <- bind_rows(df_city, df_county, df_state) %>% 
  select(geoid, geo_name, issue, indicator, race, asbest, 
         rate, disparity_z_score, performance_z_score, 
         values_count, geo_level, race_generic)

# remove records where city name is actually a university
# v6: there is 1 place like this (University of California-Santa Barbara CDP, California) with 200 rows
university_check <- df %>%
  filter(grepl('University', geo_name)) %>%
  select(geo_name) %>%
  distinct() %>%
  pull(geo_name)

final_df <- df %>% 
  filter(!(geo_name %in% university_check))

######## NOTE: You MUST re-run the whole script and update the RData file if underlying data changes ###########
# save df as .RData file, so don't have to re-run each time we update findings text, logic etc.
#saveRDS(final_df, file = paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/final_df_", Sys.Date(), ".RData")) 
#saveRDS(df_education_district, file = paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/df_education_district_", Sys.Date(), ".RData")) 

######## LOAD ALL DATA FROM RDATA FILE ######
#### You may need to manually substitute the most recent date for Sys.Date() in the lines below.
# final_df <- readRDS(paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/final_df_", Sys.Date(), ".RData"))
# df_education_district <- readRDS(paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/df_education_district_", Sys.Date(), ".RData"))

# NOTE: when you call final_df in your code chunk(s), rename it before running code on it bc it takes a LONG time to run again...

# Clean geo names
final_df <- clean_geo_names(final_df)

# Get long form race names for findings ------------------------------------------------
race_generic <- unique(final_df$race_generic)
long_name <- c("Total", "American Indian / Alaska Native", "Latinx", "Asian", "Black", "Another Race", "Multiracial", "White", "Native Hawaiian / Pacific Islander", "Southwest Asian / North African / South Asian", "Asian / Pacific Islander", "Southwest Asian / North African", "Filipinx")
race_names <- data.frame(race_generic, long_name)

# Create indicator long name df -------------------------------------------
### NOTE: This list may need to be updated or re-ordered. ###
indicator <- dbGetQuery(con, paste0("SELECT arei_indicator AS indicator, api_name AS indicator_short, arei_issue_area FROM ", curr_schema, ".arei_indicator_list_cntyst"))

# unique education indicators at school district level, for city key takeaways analysis later
educ_indicators <- filter(indicator, arei_issue_area == 'Education')

## Use San Jose to compare: filter(geoid == "0668000")
# Finding 1: Worst  and best rates by geoid and race -------------------------------------------------------------

## Part 1: Worst rates

### This section creates findings like: -----------------------------------------------------
## Race page: "Kern's Latinx residents have the worst rates for 7 of the 42 RACE COUNTS indicators." 
## and Place pages: "Across indicators, Contra Costa County Black residents are most impacted by racial disparity."
### Step 1: Get worst raced rate for each indicator and pull in race name grouped by geo + indicator
### Step 2: Count number of times each race has the worst rate grouped by geo
### Step 3: Generate sentences with # indicators with worst rates each race has out of all indicators grouped by geo + race
### Step 4: Generate sentences with # indicators with best rates each race has out of all indicators grouped by geo + race
### Step 5: Generate sentences identifying which race(s) faces the most racial disparity grouped by geo
### Step 6: Decide if we need to suppress/screen out findings for counties with few ID's like Alpine

### EXTRA STEP: find the most disparate school district for city+education indicator combo, then merge this with the long df later
# rank overall district disparity z-scores for each city+indicator combo
df_education_district_disparate <- df_education_district %>% 
  filter(!is.na(geoid)) %>% 
  group_by(geoid, indicator, race) %>% 
  mutate(rk = ifelse(race == 'total', dense_rank(-disparity_z_score), NA))

# clean geo_names
df_education_district_disparate_final <- clean_geo_names(df_education_district_disparate)

# checked for districts tied for rk 1, but there are none. if there are ties, will need to add tiebreaker code similar to what's in the best outcomes code
#temp <- filter(df_education_district_disparate, rk=='1')
#temp <- temp %>% select(geoid, geo_name, indicator, rate) %>% group_by(geoid, indicator) %>% count(rate)

# keep indicator data for the most disparate district for each city+indicator combo only
df_education_district_disparate_final <- df_education_district_disparate_final %>% 
  group_by(geoid, dist_id, indicator) %>% 
  fill(rk, .direction = "downup") %>%
  filter(rk == 1) %>% 
  select (-c(rk))

# bind most disparate district with main df
df_lf <- bind_rows(final_df, df_education_district_disparate_final) 
df_lf <- filter(df_lf, race != 'total')   # remove total rates bc all findings in this section are raced

# data where race_generic == api is duplicated and race_generic is renamed as "asian" and "pacisl"
df_lf <- api_split(df_lf) 

# rename SWANASA as SWANA for findings purposes
df_lf <- df_lf %>% 
  mutate(race_generic=replace(race_generic, race_generic=='swanasa', 'swana'))  


### Table counting number of non-NA rates per race+geo combo, used for screening worst counts later ### 
bestworst_screen <- df_lf %>% 
  group_by(geoid, race_generic) %>% 
  summarise(rate_count = sum(!is.na(rate)))


### Worst rates - RACE PAGE ###
worst_table <- df_lf %>% 
  group_by(geoid, geo_level, indicator) %>% 
  top_n(1, disparity_z_score) %>% # get worst raced disparity z-score by geo+indicator combo
  rename(worst_rate = race_generic) %>% 
  filter(values_count > 1) # filter out geo+indicator combos with only 1 raced rate

# Flagging this code returns many-to-many warnings, should use "relationship = "many-to-many"" to QA clarification
worst_table2 <- df_lf %>% 
  left_join(
    select(worst_table, geoid, indicator, worst_rate, geo_level), by = c("geoid", "indicator", "geo_level")) %>%
  mutate(worst = ifelse((race_generic == worst_rate), 1, 0)) %>% # worst = binary indicating whether the race+geo combo is the worst rate             
  group_by(geoid, geo_name, geo_level, race_generic) %>% 
  summarise(count = sum(worst, na.rm = TRUE)) %>% # count = num of worst rates for race+geo combo
  left_join(race_names, by = "race_generic") %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic")) 

# NOTE: This df does include findings for non-RC race pg grps, however they won't appear on the site
wb_rate_threshold <- 5  # suppress findings for race+geo combos with data for fewer than 6 indicators
worst_rate_count <- worst_table2 %>%
  filter(!is.na(rate_count)) %>%
  mutate(finding_type = 'worst count', 
         findings_pos = 2,
         finding = ifelse(rate_count > wb_rate_threshold, 
                          paste0(geo_name, "'s ", long_name, " residents have the worst rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), 
                          paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."))) 


# Part 2: Best rates ---------------------------------------------------

## Note: Code differs from Worst rates to account for when min is best and there is raced rate = 0, so we cannot use disparity_z for min asbest indicators
##### It is also different because there are ties for rank 1

## First, find school district with the best total_rate (overall outcome) per city+indicator combo
df_education_district_best_outcome <- df_education_district %>% 
  filter(values_count > 1 & !is.na(geoid)) %>% 
  group_by(geoid, indicator, race) %>% 
  mutate(rk = case_when((asbest == 'min' & race == 'total') ~ dense_rank(rate),
                        (asbest == 'max' & race == 'total') ~ dense_rank(-rate),
                        .default = NA)) # using dense_rank means there can be ties, use enr as tie-breaker

# clean geo_names
df_education_district_best_outcome_final <- clean_geo_names(df_education_district_best_outcome)

# tie-breaker when 2+ districts tie for best overall outcome for a city+indicator combo
tiebreaker <- df_education_district_best_outcome_final %>% 
  group_by(geoid, indicator, rk) %>% 
  mutate(ties = ifelse(rk == '1', sum(rk), NA)) %>%
  ungroup() %>%
  filter(ties > 1) %>% # if ties is >1 then there is a tie
  group_by(geoid, indicator) %>% 
  mutate(rk2 = rank(-total_enroll)) # break tie based on largest total_enrollment

# update df_education_district_best_outcome_final with revised ranks
df_education_district_best_outcome_final <- df_education_district_best_outcome_final %>% 
  mutate(old_rk = rk) %>% # preserve original ranks with ties
  left_join(select(tiebreaker, geoid, indicator, dist_id, race, rk2), by = c("geoid", "indicator", "dist_id", "race")) %>%
  mutate(rk = ifelse(!is.na(rk2), rk2, rk)) # update rk to reflect tiebreaker

# keep indicator data for the best overall outcome district for each city+indicator combo only
df_education_district_best_outcome_final <- df_education_district_best_outcome_final %>% 
  group_by(geoid, dist_id, indicator) %>% 
  fill(rk, .direction = "downup") %>%
  filter(rk == 1) %>% 
  select (-c(rk, old_rk, rk2)) # clean up columns

## Now, bind this back with the df
df_lf2 <- bind_rows(final_df, df_education_district_best_outcome_final) 
df_lf2 <- filter(df_lf2, race != 'total')   # remove total rates bc all findings in this section are raced

# data where race_generic == api is duplicated and race_generic is renamed as "asian" and "pacisl"
df_lf2 <- api_split(df_lf2) # duplicate api rates as asian and pacisl

# rename SWANASA as SWANA for findings purposes
df_lf2 <- df_lf2 %>% 
  mutate(race_generic=replace(race_generic, race_generic=='swanasa', 'swana'))  

#### Note: Code differs from Worst rates to account for when min is best and there is raced rate = 0, so we cannot use disparity_z for min asbest indicators ####
best_table <- df_lf2 %>% 
  group_by(geoid, geo_level, indicator) %>% 
  mutate(
    # rank based on which race has best outcome for given indicator (i.e., min or max)
    rk = case_when(asbest == 'min' ~dense_rank(rate),
                        asbest == 'max' ~dense_rank(-rate),
                        .default = NA),
    # identify race with best rate using rk (ties are ok)
    best_rate = ifelse(rk == 1, race_generic, ""))  

# Flagging this code chunk returns many-to-many warning - should add "relationship="many-to-many"" for QA clarification
best_table2 <- df_lf2 %>%
  filter(values_count > 1) %>%  # filter out indicators with only 1 raced rate 
  left_join(
    select(best_table, geoid, indicator, best_rate, geo_level), by = c("geoid", "indicator", "geo_level")) %>%
  mutate(best = ifelse((race_generic == best_rate), 1, 0)) %>%             
  group_by(geoid, geo_name, geo_level, race_generic) %>% 
  summarise(count = sum(best, na.rm = TRUE)) %>%
  left_join(race_names, by = c("race_generic")) %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic"))

best_rate_count <- best_table2 %>%
  filter(!is.na(rate_count)) %>% 
  mutate(finding_type = 'best count', 
         findings_pos = 1,
         finding = ifelse(rate_count > wb_rate_threshold, 
                          paste0(geo_name, "'s ", long_name, " residents have the best rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), 
                          paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis.")))


## Bind worst and best tables - RACE PAGE ## ----------------------------------------------
worst_best_counts <- bind_rows(worst_rate_count, best_rate_count)

worst_best_counts <- rename(worst_best_counts, race = race_generic) %>% 
  select(-long_name, -rate_count, -count)


# Finding 2: Most Impacted Group - PLACE PAGE ---------------------------------------

### Table counting number of indicators with ID's (multiple raced disp_z scores) per geo, used for screening most impacted later ### 
impact_screen <- df_lf %>% 
  group_by(geoid, geo_name, indicator) %>% 
  summarise(rate_count = sum(!is.na(disparity_z_score))) %>%
  ungroup() %>%
  filter(rate_count > 1) %>% 
  group_by(geoid, geo_name) %>% 
  summarise(id_count = n())

impact_table <- worst_table2 %>% 
  select(-rate_count) %>% 
  group_by(geoid, geo_name) %>% 
  top_n(1, count) %>% # get race most impacted by racial disparity by geo
  left_join(
    select(impact_screen, geoid, id_count), by = "geoid")

# Check for geos with ties for group with the most worst rates: Amador, Imperial, San Mateo
ties_worst_rate <- impact_table %>%
  group_by(geoid, geo_name, geo_level, count) %>%
  filter(n() > 1)

## the next few lines concatenate the names of the tied groups to prep for findings
impact_table2 <- impact_table %>%
  filter(!is.na(id_count)) %>% 
  group_by(geoid, geo_name, count) %>%
  mutate(race_count = n()) %>% # count the number of most impacted groups
  arrange(long_name) %>% # order long race name alphabetically
  mutate(group_order = paste0("group_", rank(long_name, ties.method = "first"))) %>% # number the most impacted groups grouped by geo
  ungroup()

# Create long_name2 that combines race groups tied for most impacted (capped at 3 race groups, anything more is marked '99999')
# long_name2 is used later to build the most impacted findings statement
impact_table_wide <- impact_table2 %>% 
  select(geoid, geo_name, geo_level, id_count, race_count, group_order, long_name) %>%      #pivot long table back to wide
  pivot_wider(names_from=group_order, values_from=long_name) %>%
  group_by(geoid) %>% 
  mutate(long_name2 = 
           case_when(
             race_count == 1 ~ group_1,
             race_count == 2 ~ paste0(group_1, " and ", group_2),
             race_count == 3 ~ paste0(group_1, ", ", group_2, ", and ", group_3),
             .default = '99999'))  %>%
  select(geoid, geo_name, geo_level, id_count, race_count, long_name2)

# Find max race count for findings below
max_race_count <- max(impact_table_wide$race_count)

# Create most impacted findings
# Findings are generated when a geo has more ids than the max race_count

most_impacted <- impact_table_wide %>% 
  mutate(finding_type = 'most impacted',
         finding = 
           case_when(
             (id_count > max_race_count & long_name2 != '99999') ~ paste0("Across indicators, ", geo_name, " ", long_name2, " residents are most impacted by racial disparity."), 
             (id_count > max_race_count & long_name2 == '99999') ~ paste0('There are more than three groups tied for most impacted in ', geo_name, "."), # added finding where 4+ groups tie for 'most impacted' bc finding becomes less meaningful
             .default = paste0("Data for residents of ", geo_name, " is too limited for this analysis.")),
         findings_pos = 1) %>%
  select(c(geoid, geo_name, geo_level, finding_type, finding, findings_pos)) 

most_impacted$geo_name <- gsub(" County", "", most_impacted$geo_name) 
most_impacted$geo_name <- gsub(" City", "", most_impacted$geo_name) 


# Finding 3: Most disparate indicator by race & place - RACE PAGE ---------------------

## This section creates findings for Race pages - most disparate indicator by race & place. 
## Example:"Denied Mortgages is the most disparate indicator for American Indian / Alaska Native residents of San Francisco."


## Extra step: find most disparate indicator by geo_name and indicator

# we already pulled the most disparate school district for each school in the previous analysis.
df_ds <- bind_rows(final_df, df_education_district_disparate_final)
# df_ds %>% filter(is.na(geo_name)) # why do some geo_names in housing don't have a geo_name? Some of them belong to census designated places with very low pop counts-- we'll filter this out later

df_ds <- df_ds %>%
  filter(race != 'total')    # remove total rates bc all findings in this section are raced

# data where race_generic == api is duplicated and race_generic is renamed as "asian" and "pacisl"
df_ds <- api_split(df_ds) 

# rename SWANASA as SWANA for findings purposes
df_ds <- df_ds %>% 
  mutate(race_generic=replace(race_generic, race_generic=='swanasa', 'swana'))  

# Using the most_disp_by_race fn sourced from key_findings_functions.R
aian_ <- most_disp_by_race(x=df_ds, y='aian')

asian_ <- most_disp_by_race(x=df_ds, y='asian')

black_ <- most_disp_by_race(x=df_ds, y='black')

latinx_ <- most_disp_by_race(x=df_ds, y='latino')

pacisl_ <- most_disp_by_race(x=df_ds, y='pacisl')

white_ <- most_disp_by_race(x=df_ds, y='white')

swana_ <- most_disp_by_race(x=df_ds, y='swana')

most_disp <- bind_rows(aian_, asian_, black_, latinx_, pacisl_, white_, swana_) %>%
  select(geoid, geo_name, dist_id, district_name, total_enroll, race, long_name, indicator_count, ends_with("_ind"), everything())


# create findings
n = 5 # indicator_count threshold

most_disp_final <- most_disp %>% 
  mutate(
    finding = 
      case_when(
        ## Suppress finding if race+geo combo has 5 or fewer indicator disparity_z scores
        indicator_count <= n ~ paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."),   
        ## Finding when a geo's most disparate indicator is in education (specified district)
        (indicator_count > n & arei_issue_area == 'Education' & !is.na(district_name)) ~ paste0(long_name, " residents face the most disparity with ", indicator, " (", district_name, ") in ", geo_name, "."),
        .default = paste0(long_name, " residents face the most disparity with ", indicator, " in ", geo_name, ".")),
    finding_type = 'most disparate', findings_pos = 3) %>%
  select(geoid, geo_name, geo_level, dist_id, district_name, total_enroll, long_name, race, indicator, indicator_count, finding_type, findings_pos, finding) %>% 
  # some geoids don't have geo_names. all of them belong in housing
  # for example: Camp Pendleton North. They don't pass the indicator count threshold either way to be included in the finding, so we will remove these. 
  filter(!is.na(geo_name)) 


# Save most_disp, best_rate_counts, worst_rate_counts as 1 df
rda_race_findings <- bind_rows(most_disp_final, worst_best_counts)

rda_race_findings <- rda_race_findings %>% 
  relocate(geo_level, .after = geo_name) %>% 
  relocate(finding_type, .after = race) %>% 
  mutate(src = 'rda', 
         citations = '',
         race = ifelse(race == 'pacisl', 'nhpi', race))  # rename pacisl to nhpi to feed API - In all other tables we use 'pacisl'

## Export postgres table
dbWriteTable(con, c(curr_schema, "arei_findings_races_multigeo"), rda_race_findings, overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE ", curr_schema, ".arei_findings_races_multigeo IS ' Created ", Sys.Date(), ". Findings for Race pages (API) created using W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\KeyTakeaways\\key_findings_", curr_yr, ".R.';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_races_multigeo.finding_type
                         IS 'Categorizes findings: count of best and worst rates by race/geo combo, most disparate indicator by race/geo combo';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_races_multigeo.src
                         IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_races_multigeo.citations
                         IS 'External ", curr_schema, ".citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_races_multigeo.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
#print(comment)
dbSendQuery(con, comment)


# Finding 4: the most disparate and worst outcome indicators - PLACE PAGE ----------

## Extra step: first merge the most disparate district for education with df for cities
df_3 <- bind_rows(final_df, df_education_district_disparate_final) 

### This section creates findings for Place page - the most disparate and worst outcome indicators #####
disp_long <- df_3 %>% 
  filter(race == "total" & geo_level %in% c("county", "city")) %>% 
  select(geoid, geo_name, dist_id, district_name, total_enroll, indicator, disparity_z_score, geo_level) %>% 
  rename(variable = indicator, value = disparity_z_score)

### Worst Disparity - PLACE PAGE ----

#### Rank indicators by disp_z with worst/highest disp_z = 1
disp_final <- disp_long %>%
  group_by(geoid, geo_name) %>%
  mutate(rk = min_rank(-value))

##### Select only worst/highest disparity indicator per county
disp_final <- disp_final %>% 
  filter(rk == 1) %>% 
  arrange(geoid) 

##### Rename variable and value fields
names(disp_final)[names(disp_final) == 'value'] <- 'worst_disp_z'
names(disp_final)[names(disp_final) == 'variable'] <- 'worst_disp_indicator'

# join to get long indicator names for findings
worst_disp <- select(disp_final, -c(rk, worst_disp_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_disp_indicator" = "indicator_short")) %>% 
  rename(long_disp_indicator = indicator)

# Adjust for geos with tied indicators
worst_disp2 <- worst_disp %>% 
  group_by(geoid,geo_name) %>% 
  mutate(disp_ties = n()) %>%
  mutate(long_disp_indicator = paste0(long_disp_indicator, collapse = " and ")) %>% 
  select(-c(worst_disp_indicator)) %>% 
  unique() # RC v6: no ties


# Write findings using ifelse statements
worst_disp3 <- subset(worst_disp2, !is.na(geo_name)) %>%
  mutate(finding_type = 'worst disparity', finding = paste0(long_disp_indicator, " is the most disparate indicator in ", geo_name, "."), 
         findings_pos = 4) %>% mutate(
           finding = ifelse(
             (arei_issue_area == 'Education' & geo_level == "city"),
             paste0(long_disp_indicator, " (", district_name, ") is the most disparate indicator in ", geo_name, "."),
             finding)) %>% 
  select(geoid, geo_name, dist_id, district_name, total_enroll, geo_level, finding_type,finding, findings_pos)


### Worst Outcome - PLACE PAGE ----

## Extra step: first identify the worst outcome district per city for each ed indicator, then merge the that district with df for cities
df_education_district_worst_outcome <- df_education_district %>% 
  filter(!is.na(geoid)) %>% 
  group_by(geoid, race_generic) %>%
  mutate(rk = dense_rank(performance_z_score)) %>% 
  filter(rk == "1") %>% 
  select(-rk) # RC v6 no ties

# clean geo_name
df_education_district_worst_outcome_final <- clean_geo_names(df_education_district_worst_outcome)

df_4 <- bind_rows(final_df, df_education_district_worst_outcome_final) 

outc_long <- df_4 %>% 
  filter(race == "total" & geo_level %in% c("county", "city")) %>% 
  select(geoid, geo_name, dist_id, district_name, total_enroll, indicator, performance_z_score, geo_level) %>%
  rename(variable = indicator, value = performance_z_score)

#### Rank indicators by perf_z with worst/lowest perf_z = 1
outc_final <- outc_long %>%
  group_by(geoid, geo_name) %>%
  mutate(rk = min_rank(value))

##### Select only worst/lowest outcome indicator per county
outc_final <- outc_final %>% 
  filter(rk == 1) %>% 
  arrange(geoid)

##### Rename variable and value fields
names(outc_final)[names(outc_final) == 'value'] <- 'worst_perf_z'
names(outc_final)[names(outc_final) == 'variable'] <- 'worst_perf_indicator'    

# join to get long indicator names for findings
worst_outc <- select(outc_final, -c(rk, worst_perf_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_perf_indicator" = "indicator_short")) %>% 
  rename(long_perf_indicator = indicator)

# Adjust for counties with tied indicators
worst_outc2 <- worst_outc %>% 
  group_by(geoid, geo_name) %>% 
  mutate(perf_ties = n()) %>%
  mutate(long_perf_indicator = paste0(long_perf_indicator, collapse = " and ")) %>% 
  select(-c(worst_perf_indicator)) %>% 
  unique() # RC v6: no ties

# Write Findings using ifelse statements
worst_outc3 <- worst_outc2 %>%
  filter(!is.na(geo_name)) %>%
  mutate(finding_type = 'worst overall outcome', 
         finding = 
           case_when(
             (arei_issue_area == 'Education' & geo_level == "city") ~ paste0(long_perf_indicator, " (", district_name, ") has the worst overall outcome in ", geo_name, "."),
             .default = paste0(long_perf_indicator, " has the worst overall outcome in ", geo_name, ".")),
         findings_pos = 5) %>% 
  select(geoid, geo_name, dist_id, district_name, total_enroll, geo_level, finding_type, finding, findings_pos)


# Combine findings into one final df
worst_disp_outc <- union(worst_disp3, worst_outc3)


# Finding 5: Above/Below Average Disparity/Outcome Findings - PLACE PAGE --------------------------------------
### This section creates findings for Place page - the summary statements above/below avg disparity/outcome across counties ####
## pull composite disparity/outcome z-scores for city and county; add urban type = NA for cities.
index_county <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".arei_composite_index_", curr_yr)) %>% 
  select(county_id, county_name, urban_type, disparity_z, performance_z) %>% 
  rename(geoid = county_id, geo_name = county_name) %>% 
  mutate(geo_level = "county")

index_city <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".arei_composite_index_city_", curr_yr)) %>% 
  select(city_id, city_name, disparity_z, performance_z) %>% 
  rename(geoid = city_id, geo_name = city_name) %>% 
  mutate(urban_type = NA, geo_level = "city")

index_county_city <- rbind(index_county, index_city) 

index_county_city$geo_name <- ifelse(index_county_city$geo_level == 'county', paste0(index_county_city$geo_name, ' County'), index_county_city$geo_name)

# Above/Below Avg Disp/Outcome
avg_statement_df <- index_county_city %>% 
  mutate(pop_type = ifelse(urban_type == 'Urban', 'more', 'less'), # used for more/less populous finding
         outc_type = ifelse(performance_z < 0, 'below', 'above'),
         disp_type = ifelse(disparity_z < 0, 'below', 'worse than')) %>%
         clean_geo_names()

disp_avg_statement <- avg_statement_df %>%
  mutate(finding_type = 'disparity', 
         finding = ifelse(geo_level == "county",
                          paste0(geo_name, "'s racial disparity across indicators is ", disp_type, " average for California counties."), 
                          paste0(geo_name, "'s racial disparity across indicators is ", disp_type, " average for California cities.")),
         finding = ifelse(is.na(disp_type), 
                          paste0("Data for ", geo_name, " is too limited for this analysis."), 
                          finding),
         findings_pos = 2) %>% 
  select(geoid, geo_name, geo_level, finding_type, finding, findings_pos)

outc_avg_statement <- avg_statement_df %>% 
  mutate(finding_type = 'outcomes', 
         finding = ifelse(geo_level == "county", 
                          paste0(geo_name, "'s overall outcomes across indicators are ", outc_type, " average for California counties."),
                          paste0(geo_name, "'s overall outcomes across indicators are ", outc_type, " average for California cities.")),
         finding = ifelse(is.na(outc_type), 
                          paste0("Data for ", geo_name, " is too limited for this analysis."), 
                          finding),
         findings_pos = 3) %>% 
  select(geoid, geo_name, geo_level, finding_type, finding, findings_pos) 

## bind everything together
worst_disp_outc_ <- worst_disp_outc %>% 
  select(-c(dist_id, district_name, total_enroll))

rda_places_findings <- rbind(most_impacted, disp_avg_statement, outc_avg_statement, worst_disp_outc_) %>%
  mutate(src = 'rda', citations = '') %>%
  relocate(geo_level, .after = geo_name)


#### HK: (manual) issue area findings (used on issue areas pages and the state places page) ####

issue_area_findings <- read.csv(paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/RaceCounts/KeyTakeaways/manual_findings_", curr_schema, "_", curr_yr, ".csv"), encoding = "UTF-8", check.names = FALSE)
colnames(issue_area_findings) <- c("issue_area", "finding", "findings_pos")

issue_area_findings_type_dict <- list(economy = "Economic Opportunity",
                                      education = "Education",
                                      housing = "Housing",
                                      health = "Health Care Access",
                                      democracy = "Democracy",
                                      crime = "Crime and Justice",
                                      hbe = "Healthy Built Environment")

issue_area_findings$finding_type <- case_when(issue_area_findings$issue_area == 'economy' ~ "Economic Opportunity",
                                              issue_area_findings$issue_area == 'health' ~ "Health Care Access",
                                              issue_area_findings$issue_area == 'crime' ~ "Crime and Justice",
                                              issue_area_findings$issue_area == 'hbe' ~ "Healthy Built Environment",
                                              .default = str_to_title(issue_area_findings$issue_area))

issue_area_findings$src <- "rda"

issue_area_findings$citations <- ""

dbWriteTable(con, c(curr_schema, "arei_findings_issues"), issue_area_findings, overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE ", curr_schema, ".arei_findings_issues IS 'findings for Issue Area pages (API) created using W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\KeyTakeaways\\key_findings_", curr_yr, ".R. Created ", Sys.Date(), ".';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_issues.finding_type
                       IS 'Categorizes findings: race most impacted by inequities in a geo, above/below avg disp, above/below perf, most disp indicator, worst perf indicator';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_issues.src
                       IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_issues.citations
                       IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_issues.findings_pos
                       IS 'Used to determine the order a set of findings should appear in on RC.org';")
# print(comment)
dbSendQuery(con, comment)



###### Combine Issues table and places_findings_table #####
# prep issues table for addition to places_findings_table
state_issue_area_findings <- issue_area_findings

state_issue_area_findings <- state_issue_area_findings %>% select(-issue_area)

state_issue_area_findings$geoid <- "06"
state_issue_area_findings$geo_name <- "California"
state_issue_area_findings$geo_level <- "state"

# reorder findings position for places positions
state_issue_area_findings <- state_issue_area_findings %>%
  mutate(findings_pos = row_number() + 1) 

findings_places_multigeo <- rbind(rda_places_findings, state_issue_area_findings)


## Create postgres table
dbWriteTable(con, c(curr_schema, "arei_findings_places_multigeo"), findings_places_multigeo, overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE ", curr_schema, ".arei_findings_places_multigeo IS ' Created ", Sys.Date(), ". Findings for Place pages (API) created using W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\KeyTakeaways\\key_findings_", curr_yr, ".R.';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_places_multigeo.finding_type
                        IS 'Categorizes findings: race most impacted by inequities in a geo, above/below avg disp, above/below outcome, most disp indicator, worst outcome indicator';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_places_multigeo.src
                        IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_places_multigeo.citations
                        IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN ", curr_schema, ".arei_findings_places_multigeo.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
#print(comment)
dbSendQuery(con, comment)

dbDisconnect(con)  

