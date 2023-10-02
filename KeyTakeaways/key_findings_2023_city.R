# Packages ----------------------------------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(sf)


# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")


# pull in geo level ids with name. I don't do this directly in the data in case names differ and we have issues merging later
arei_race_multigeo_city <- dbGetQuery(con, "SELECT geoid, name, geolevel  FROM v5.arei_race_multigeo") %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

arei_race_multigeo_county <- dbGetQuery(con, "SELECT geoid, name, geolevel  FROM v5.arei_race_multigeo") %>% filter(geolevel == "county") %>% rename(county_id = geoid, county_name = name) %>% select(-geolevel)

arei_race_multigeo_state <- dbGetQuery(con, "SELECT geoid, name, geolevel  FROM v5.arei_race_multigeo") %>% filter(geolevel == "state") %>% rename(state_id = geoid, state_name = name) %>% select(-geolevel)

# pull in cross-walk to go from district to city
crosswalk <- dbGetQuery(con, "SELECT  city_id, city_name, dist_id, total_enroll FROM v5.arei_city_county_district_table")

# City --------------------------------------------------------------------

# pull in list of tables in racecounts.v5
rc_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v5"))$table, function(x) slot(x, 'name'))))



# pull in list of tables in racecounts.v5

# filter for only city level indicator tables
city_list <- filter(rc_list, grepl("_city_2023",table)) %>% filter(table!= "arei_composite_index_city_2023")
city_list <- city_list[order(city_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on city_list
city_tables <- lapply(setNames(paste0("select * from v5.", city_list), city_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
city_tables <- map2(city_tables, names(city_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name


# call columns we want



# you need to pivot wider
city_tables_disparity <- lapply(city_tables, function(x) x%>% select(city_id, asbest,ends_with("disparity_z"), indicator, values_count))


disparity <- imap_dfr(city_tables_disparity, ~
                        .x %>% 
                        pivot_longer(cols = ends_with("disparity_z"),
                                     names_to = "race",
                                     values_to = "disparity_z_score")) %>% mutate(
                                       race = (ifelse(race == 'disparity_z', 'total', race)),
                                       race = gsub('_disparity_z', '', race))



city_tables_performance <- lapply(city_tables, function(x) x%>% select(city_id, asbest, ends_with("performance_z"), indicator, values_count))


performance <- imap_dfr(city_tables_performance, ~
                          .x %>% 
                          pivot_longer(cols = ends_with("performance_z"),
                                       names_to = "race",
                                       values_to = "performance_z_score")) %>% mutate(
                                         race = (ifelse(race == 'performance_z', 'total', race)),
                                         race = gsub('_performance_z', '', race))




city_tables_rate <- lapply(city_tables, function(x) x%>% select(city_id, asbest, ends_with("_rate"), indicator, values_count))

rate <- imap_dfr(city_tables_rate , ~
                   .x %>% 
                   pivot_longer(cols = ends_with("_rate"),
                                names_to = "race",
                                values_to = "rate")) %>% mutate(
                                  race = (ifelse(race == 'rate', 'total', race)),
                                  race = gsub('_rate', '', race))

# merge all 3 

df_merged <- disparity %>% full_join(performance) %>% full_join(rate)

# create issue, indicator, geolevel, race generic columns for issue tables except for education
df_city <- df_merged %>% mutate(
  issue = case_when(startsWith(indicator, "arei_crim") ~ "crim",
                    startsWith(indicator, "arei_demo") ~ "demo",
                    startsWith(indicator, "arei_econ") ~ "econ",
                    startsWith(indicator, "arei_hben") ~ "hben",
                    startsWith(indicator, "arei_hlth") ~ "hlth",
                    startsWith(indicator, "arei_hous") ~ "hous"
  ),
  indicator = substring(indicator, 11),
  indicator = gsub('_city_2023', '', indicator),
  geolevel = "city",
  race_generic = gsub('nh_', '', race) # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
  
) %>% left_join(arei_race_multigeo_city) %>% rename(geoid = city_id, geo_name = city_name) %>%  mutate(geo_name = paste0(geo_name, " City"))



# education tables 
education_list <- filter(rc_list, grepl("_district_2023",table))
education_list <- education_list[order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on city_list
education_tables <- lapply(setNames(paste0("select * from v5.", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables <- map2(education_tables, names(education_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

# call columns we want

# you need to pivot wider
education_tables_disparity <- lapply(education_tables, function(x) x%>% select(dist_id,district_name, asbest,ends_with("disparity_z"), indicator, values_count))


education_disparity <- imap_dfr(education_tables_disparity, ~
                                  .x %>% 
                                  pivot_longer(cols = ends_with("disparity_z"),
                                               names_to = "race",
                                               values_to = "disparity_z_score_unweighted")) %>% mutate(
                                                 race = (ifelse(race == 'disparity_z', 'total', race)),
                                                 race = gsub('_disparity_z', '', race))


education_tables_performance <- lapply(education_tables, function(x) x%>% select(dist_id,district_name, asbest, ends_with("performance_z"), indicator, values_count))


education_performance <- imap_dfr(education_tables_performance, ~
                                    .x %>% 
                                    pivot_longer(cols = ends_with("performance_z"),
                                                 names_to = "race",
                                                 values_to = "performance_z_score_unweighted")) %>% mutate(
                                                   race = (ifelse(race == 'performance_z', 'total', race)),
                                                   race = gsub('_performance_z', '', race))

education_tables_rate <- lapply(education_tables, function(x) x%>% select(dist_id,district_name, asbest, ends_with("_rate"), indicator, values_count))

education_rate <- imap_dfr(education_tables_rate , ~
                             .x %>% 
                             pivot_longer(cols = ends_with("_rate"),
                                          names_to = "race",
                                          values_to = "rate_unweighted")) %>% mutate(
                                            race = (ifelse(race == 'rate', 'total', race)),
                                            race = gsub('_rate', '', race))

df_merged_education <- education_disparity %>% full_join(education_performance) %>% full_join(education_rate)

dist_name <- df_merged_education %>% group_by(dist_id, district_name) %>% summarize(count = n()) %>% select(-count) # unique dist id and district name to use later




# pull in cross-walk
# crosswalk <- dbGetQuery(con, "SELECT * FROM v5.arei_city_county_district_table")


# df_education_crosswalk <-  crosswalk %>% select(city_id,dist_id, district_name) %>% left_join(df_merged_education) 


# create issue, indicator, geolevel, race generic columns for education table
df_education_district <- df_merged_education %>% mutate(
  issue = case_when(startsWith(indicator, "arei_educ") ~ "educ"
  ),
  indicator = substring(indicator, 11),
  indicator = gsub('_district_2023', '', indicator),
  geolevel = "city",
  race_generic = gsub('nh_', '', race) # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
) %>% left_join(crosswalk, by = "dist_id") %>% rename (geoid = city_id, geo_name = city_name) %>% filter(!is.na(race))


## extra step: weighted averages by enrollment size per indicator ( some cities have multiple districts but not every indicator will have multiple districts per city).

## df_education_district %>% filter(is.na(geoid)) %>% group_by(district_name) %>% summarize(count = n()) 180 school council districts won't match because it might cover a rural area


enrollment_percentages <- df_education_district %>% select(geoid,  dist_id, total_enroll, indicator) %>% unique() %>% group_by(geoid,  indicator) %>% mutate(sum_total_enroll = sum(total_enroll, na.rm = T), 
                                                                                                                                                             percent_total_enroll = (total_enroll/sum_total_enroll)) %>% ungroup() %>% select(geoid, dist_id, indicator, total_enroll, sum_total_enroll, percent_total_enroll)


# enrollment_percentages %>% filter(geoid == "0606070")

df_education_district_weighted <- df_education_district %>% left_join(enrollment_percentages, by = c("geoid","dist_id","indicator", "total_enroll")) %>% mutate(
  disparity_z_score = disparity_z_score_unweighted * percent_total_enroll,
  performance_z_score = performance_z_score_unweighted * percent_total_enroll,
  rate = rate_unweighted * percent_total_enroll
) 

df_education_city <- df_education_district_weighted %>% group_by(geoid, asbest, indicator,  race, issue, geolevel, race_generic) %>% summarize(disparity_z_score = sum(disparity_z_score, na.rm = T), performance_z_score = sum(performance_z_score, na.rm = T), rate = sum(rate, na.rm = T)) %>% left_join(arei_race_multigeo_city, by = c("geoid" = "city_id")) %>% rename(geo_name = city_name) %>%  mutate(geo_name = paste0(geo_name, " City"))


## count values ----------------- we need this since we are aggregating                                                                                                                                                     
count_values <- df_education_city %>% ungroup() %>% select(geoid, indicator, race, rate) %>% mutate(race = paste0(race, "_rate")) %>%                                                       pivot_wider(
  names_from = race, 
  values_from = rate
) %>% select(-total_rate)


rates <- dplyr::select(count_values, ends_with("_rate"))
rates$values_count <- rowSums(!is.na(rates))


count_values$values_count <- rates$values_count

count_values <- count_values %>% select(geoid, indicator, values_count)

### 
df_education_city <- df_education_city %>% left_join(count_values, by = c("geoid", "indicator"))

df_education_city <- df_education_city %>% filter(!is.na(geoid)) # eliminate missing cities without z-score, rates, or geoid value


# County

# pull in list of tables in racecounts.v5

# filter for only county level indicator tables

county_list <- filter(rc_list, grepl("_county_2023",table))
county_list <- county_list[order(county_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on county_list
county_tables <- lapply(setNames(paste0("select * from v5.", county_list), county_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
county_tables <- map2(county_tables, names(county_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name


# call columns we want

# you need to pivot wider
county_tables_disparity <- lapply(county_tables, function(x) x%>% select(county_id, asbest,ends_with("disparity_z"), indicator, values_count))


county_disparity <- imap_dfr(county_tables_disparity, ~
                               .x %>% 
                               pivot_longer(cols = ends_with("disparity_z"),
                                            names_to = "race",
                                            values_to = "disparity_z_score")) %>% mutate(
                                              race = (ifelse(race == 'disparity_z', 'total', race)),
                                              race = gsub('_disparity_z', '', race))



county_tables_performance <- lapply(county_tables, function(x) x%>% select(county_id, asbest, ends_with("performance_z"), indicator, values_count))


county_performance <- imap_dfr(county_tables_performance, ~
                                 .x %>% 
                                 pivot_longer(cols = ends_with("performance_z"),
                                              names_to = "race",
                                              values_to = "performance_z_score")) %>% mutate(
                                                race = (ifelse(race == 'performance_z', 'total', race)),
                                                race = gsub('_performance_z', '', race))




county_tables_rate <- lapply(county_tables, function(x) x%>% select(county_id, asbest, ends_with("_rate"), indicator, values_count))

county_rate <- imap_dfr(county_tables_rate , ~
                          .x %>% 
                          pivot_longer(cols = ends_with("_rate"),
                                       names_to = "race",
                                       values_to = "rate")) %>% mutate(
                                         race = (ifelse(race == 'rate', 'total', race)),
                                         race = gsub('_rate', '', race))

# merge all 3 

df_merged_county <- county_disparity %>% full_join(county_performance) %>% full_join(county_rate)

# create issue, indicator, geolevel, race generic columns for issue tables except for education
df_county <- df_merged_county %>% mutate(
  issue = case_when(startsWith(indicator, "arei_crim") ~ "crim",
                    startsWith(indicator, "arei_demo") ~ "demo",
                    startsWith(indicator, "arei_educ") ~ "educ",
                    startsWith(indicator, "arei_econ") ~ "econ",
                    startsWith(indicator, "arei_hben") ~ "hben",
                    startsWith(indicator, "arei_hlth") ~ "hlth",
                    startsWith(indicator, "arei_hous") ~ "hous"
  ),
  indicator = substring(indicator, 11),
  indicator = gsub('_county_2023', '', indicator),
  geolevel = "county",
  race_generic = gsub('nh_', '', race) # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
) %>% left_join(arei_race_multigeo_county) %>% rename(geoid = county_id, geo_name = county_name)  %>%  mutate(geo_name = paste0(geo_name, " County"))


# state -------------------------------------------------------------------
# pull in list of tables in racecounts.v5

# filter for only state level indicator tables
state_list <- filter(rc_list, grepl("_state_2023",table))
state_list <- state_list[order(state_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on state_list
state_tables <- lapply(setNames(paste0("select * from v5.", state_list), state_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
state_tables <- map2(state_tables, names(state_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name


# call columns we want

# you need to pivot wider
state_tables_disparity <- lapply(state_tables, function(x) x%>% select(state_id, asbest,ends_with("disparity_z"), indicator, values_count))


state_disparity <- imap_dfr(state_tables_disparity, ~
                              .x %>% 
                              pivot_longer(cols = ends_with("disparity_z"),
                                           names_to = "race",
                                           values_to = "disparity_z_score")) %>% mutate(
                                             race = (ifelse(race == 'disparity_z', 'total', race)),
                                             race = gsub('_disparity_z', '', race))


state_tables_rate <- lapply(state_tables, function(x) x%>% select(state_id, asbest, ends_with("_rate"), indicator, values_count))

state_rate <- imap_dfr(state_tables_rate , ~
                         .x %>% 
                         pivot_longer(cols = ends_with("_rate"),
                                      names_to = "race",
                                      values_to = "rate")) %>% mutate(
                                        race = (ifelse(race == 'rate', 'total', race)),
                                        race = gsub('_rate', '', race))

# merge all 2

df_merged_state <- state_disparity %>% full_join(state_rate)

# create issue, indicator, geolevel, race generic columns for issue tables except for education
df_state <- df_merged_state %>% mutate(
  issue = case_when(startsWith(indicator, "arei_crim") ~ "crim",
                    startsWith(indicator, "arei_demo") ~ "demo",
                    startsWith(indicator, "arei_educ") ~ "educ",
                    startsWith(indicator, "arei_econ") ~ "econ",
                    startsWith(indicator, "arei_hben") ~ "hben",
                    startsWith(indicator, "arei_hlth") ~ "hlth",
                    startsWith(indicator, "arei_hous") ~ "hous"
  ),
  indicator = substring(indicator, 11),
  indicator = gsub('_state_2023', '', indicator),
  geolevel = "state",
  race_generic = gsub('nh_', '', race), # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
  performance_z_score = NA
) %>% left_join(arei_race_multigeo_state) %>% rename(geoid = state_id, geo_name = state_name)



df <- bind_rows(df_city, df_education_city, df_county, df_state) %>% select(
  geoid, geo_name,issue, indicator, race, asbest, rate, disparity_z_score, performance_z_score, values_count,geolevel, race_generic) %>% rename(geo_level = geolevel)


# remove universities: there are 6 of them
df <- df %>% filter(!grepl('University', geo_name))




# NOTE: when you call this df in your code chunk(s), rename it before running code on it bc it takes a LONG time to run again...

# Get long form race names for findings ------------------------------------------------
race_generic <- unique(df$race_generic)
long_name <- c("Total", "American Indian / Alaska Native","Latinx", "Asian", "Black", "Other Race","Two or More Races", "White", "Native Hawaiian / Pacific Islander","API", "Filipinx")
race_names <- data.frame(race_generic, long_name)


# Create indicator long name df -------------------------------------------
indicator <- c("Incarceration", "Use of Force", "Census Participation", "Diversity of Electeds", "Employment","Internet Access", "Officials and Managers", "Per Capita Income", "Drinking Water Contaminants", "Proximity to Hazards", "Lack of Greenspace", "Toxic Releases from Facilities", "Health Insurance", "Homeowner Cost Burden", "Renter Cost Burden", "Denied Mortgages","Evictions","Foreclosures","Homeownership","Housing Quality","Overcrowded Housing","Subprime Mortgages", "3rd Grade English Proficiency","3rd Grade Math Proficiency","High School Graduation","Teacher & Staff Diversity", "Chronic Absenteeism", "Suspensions","Perception of Safety","Arrests for Status Offenses","Diversity of Candidates","Voter Registration", "Voting in Midterm Elections","Voting in Presidential Elections", "Connected Youth","Living Wage","Cost-of-Living Adjusted Poverty","Early Childhood Education Access","Asthma", "Food Access", "Got Help", "Life Expectancy","Low Birthweight", "Preventable Hospitalizations", "Usual Source of Care", "Student Homelessness")


indicator_short  <- unique(df$indicator)


indicator <- data.frame(indicator, indicator_short)

### This section creates findings like: -----------------------------------------------------
## Race page: "Kern's Latinx residents have the worst rates for 7 of the 42 RACE COUNTS indicators." 
## and Place pages: "Across indicators, Contra Costa County Black residents are most impacted by racial disparity."
### Step 1: Get worst raced rate for each indicator and pull in race name grouped by geo + indicator
### Step 2: Count number of times each race has the worst rate grouped by geo
### Step 3: Generate sentences with # indicators with worst rates each race has out of all indicators grouped by geo + race
### Step 4: Generate sentences with # indicators with best rates each race has out of all indicators grouped by geo + race
### Step 5: Generate sentences identifying which race(s) faces the most racial disparity grouped by geo
### Step 6: Decide if we need to suppress/screen out findings for counties with few ID's like Alpine

# copy df before running any code
df_lf <- filter(df, race != 'total')   # remove total rates bc all findings in this section are raced

# duplicate API rows, assigning one set race_generic Asian and the other set PacIsl
api_asian <- filter(df_lf, race_generic == 'api') %>% mutate(race_generic = 'asian')
api_pacisl <- filter(df_lf, race_generic == 'api') %>% mutate(race_generic = 'pacisl')
df_lf <- filter(df_lf, race_generic != 'api')       # remove api rows
df_lf <- bind_rows(df_lf, api_asian, api_pacisl)    # add back api rows as asian AND pacisl rows



### Table counting number of non-NA rates per race+geo combo, used for screening best/worst counts later ### 
bestworst_screen <- #subset(df_lf, !race_generic %in% c('filipino', 'other', 'twoormor')) %>%           # keep only races we have RACE pages for on RC.org
  df_lf %>% group_by(geoid, race_generic) %>% summarise(rate_count = sum(!is.na(rate)))


### Table counting number of indicators with ID's (multiple raced disp_z scores) per geo, used for screening most impacted later ### 
impact_screen <- df_lf %>% group_by(geoid, geo_name, indicator) %>% summarise(count = sum(!is.na(disparity_z_score)))
impact_screen <- filter(impact_screen, count > 1) %>% group_by(geoid, geo_name) %>% summarise(id_count = n())    

### Worst rates - RACE PAGE ###
filter_nonRC <- #df_lf %>% filter(race_generic %in% c('filipino', 'other', 'twoormor') & values_count == "2" & !is.na(rate)) %>%
  df_lf %>% filter(values_count == "2" & !is.na(rate)) %>%
  select(geoid, geo_name, issue, indicator) %>% mutate(remove = 1) ## create df2 of observations with non-RC groups as one of the only two rates. There are 2 observations

worst_table  <- df_lf %>% left_join(filter_nonRC) %>%   
  #filter(is.na(remove) & !race_generic %in% c('filipino', 'other', 'twoormor') & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate AND where 1 of 2 raced rates is a non-RC Race page group
  filter(is.na(remove) & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate AND where 1 of 2 raced rates is a non-RC Race page group
  group_by(geoid, geo_level, indicator) %>% top_n(1, disparity_z_score) %>% # get worst raced rate by geo+indicator
  rename(worst_rate = race_generic) %>% select(-remove)


worst_table2 <- #subset(df_lf, (!race_generic %in% c('filipino', 'other', 'twoormor')) & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate
  subset(df_lf, values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate
  left_join(select(worst_table, geoid, indicator, worst_rate, geo_level), by = c("geoid", "indicator", "geo_level")) %>%
  mutate(worst = ifelse((race_generic == worst_rate), 1, 0)) %>%             
  group_by(geoid, geo_name, geo_level, race_generic) %>% summarise(count = sum(worst, na.rm = TRUE)) %>%
  left_join(race_names, by = "race_generic") %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic")) 
worst_table2 <- worst_table2 %>% mutate(count = ifelse(is.na(count) & rate_count > 0, 0, count)) 


worst_rate_count <- filter(worst_table2, !is.na(rate_count)) %>% mutate(geo_name = gsub(' County', '', geo_name), finding_type = 'worst count', findings_pos = 2) %>% 
  mutate(finding = ifelse(rate_count > 5, paste0(geo_name, "'s ", long_name, " residents have the worst rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."))) %>%   mutate(geo_name = gsub(' City', '', geo_name))


### Best rates - RACE PAGE ###
#### Note: Code differs from Worst rates to account for when min is best and there is raced rate = 0, so we cannot use disparity_z for min asbest indicators ####
best_table <- #subset(df_lf, (!race_generic %in% c('filipino', 'other', 'twoormor')) & values_count > 1 & !is.na(rate)) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate
  subset(df_lf, values_count > 1 & !is.na(rate)) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate              
  select(c(geoid, issue, indicator, values_count, geo_level, asbest, rate, race_generic)) %>% 
  group_by(geoid, issue, indicator, values_count, geo_level, asbest) %>% 
  mutate(best_rank = ifelse(asbest == 'min', dense_rank(rate), dense_rank(-rate)))  %>% # use dense_rank to give ties the same rank, and all integer ranks
  mutate(best_rate = ifelse(best_rank == 1, race_generic, ""))    # identify race with best rate using best_rank
best_table <- best_table %>% left_join(filter_nonRC) %>% filter(is.na(remove)) %>% select(-geo_name, -remove) # remove non-RC best group rates. Total of 2 obs



best_table <- # subset(df_lf, (!race_generic %in% c('filipino', 'other', 'twoormor')) & values_count > 1 & !is.na(rate)) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate
  subset(df_lf, values_count > 1 & !is.na(rate)) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate              
  select(c(geoid, issue, indicator, values_count, geo_level, asbest, rate, race_generic)) %>% 
  group_by(geoid, issue, indicator, values_count, geo_level, asbest) %>% 
  mutate(best_rank = ifelse(asbest == 'min', dense_rank(rate), dense_rank(-rate)))  %>% # use dense_rank to give ties the same rank, and all integer ranks
  mutate(best_rate = ifelse(best_rank == 1, race_generic, ""))    # identify race with best rate using best_rank
best_table <- best_table %>% left_join(filter_nonRC) %>% filter(is.na(remove)) %>% select(-geo_name, -remove) # remove non-RC best group rates. Total of 2 obs





best_table2 <- #subset(df_lf, (!race_generic %in% c('filipino', 'other', 'twoormor')) & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate 
  subset(df_lf, values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate 
  left_join(select(best_table, geoid, indicator, best_rate, geo_level), by = c("geoid", "indicator", "geo_level")) %>%
  mutate(best = ifelse((race_generic == best_rate), 1, 0)) %>%             
  group_by(geoid, geo_name, geo_level, race_generic) %>% summarise(count = sum(best, na.rm = TRUE)) %>%
  left_join(race_names, by = c("race_generic")) %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic"))
best_table2 <- best_table2 %>% mutate(count = ifelse(is.na(count) & rate_count > 0, 0, count))



best_rate_count <- filter(best_table2, !is.na(rate_count)) %>% mutate(geo_name = gsub(' County', '', geo_name), finding_type = 'best count', findings_pos = 1) %>%
  mutate(finding = ifelse(rate_count > 5, paste0(geo_name, "'s ", long_name, " residents have the best rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."))) %>%   mutate(geo_name = gsub(' City', '', geo_name))


### Bind worst and best tables - RACE PAGE ### ----------------------------------------------
worst_best_counts <- bind_rows(worst_rate_count, best_rate_count)
worst_best_counts <- rename(worst_best_counts, race = race_generic) %>% select(-long_name, -rate_count, -count)
worst_best_counts <- worst_best_counts  %>%
  # mutate(geo_level = ifelse(geo_name == 'California', 'state', 'county')) 
  filter(!race %in% c('filipino', 'other', 'twoormor')) # filter out races that don't have RC Race Pages

### Most impacted - PLACE PAGE ### ---------------------------------------------------

impact_table <- worst_table2 %>% select(-rate_count) %>% group_by(geoid, geo_name) %>% top_n(1, count) %>% # get race most impacted by racial disparity by geo
  left_join(select(impact_screen, geoid, id_count), by = "geoid")
# 5 counties have ties for group with the most worst rates: Amador, Madera, Mono, San Mateo, Tulare
## the next few lines concatenate the names of the tied groups to prep for findings

impact_table2 <- impact_table %>% 
  group_by(geoid, geo_name, count) %>% 
  mutate(long_name2 = paste0(long_name, collapse = " and ")) %>%  select(-c(long_name, race_generic)) %>% unique()
most_impacted <- impact_table2 %>% mutate(finding_type = 'most impacted', finding = ifelse(id_count > 4, paste0("Across indicators, ", geo_name, " ", long_name2, " residents are most impacted by racial disparity."), paste0("Data for residents of ", geo_name, " is too limited for this analysis.")),
                                          findings_pos = 1)


most_impacted <- most_impacted %>% select(c(geoid, geo_name, geo_level, finding_type, finding, findings_pos))
most_impacted$geo_name <- gsub(" County", "", most_impacted$geo_name) 
most_impacted$geo_name <- gsub(" City", "", most_impacted$geo_name) 
most_impacted <- most_impacted[-c(1)]

### This section creates findings for Race pages - most disparate indicator by race & place. 
##Example:"Denied Mortgages is the most disparate indicator for American Indian/Alaska Native residents of San Francisco." ------------------------------------------------------------------

# Function to prep raced most_disparate tables 

most_disp_by_race <- function(x, y, d) {
  # Nested function to pull the column with the maximum value ----------------------
  find_first_max_index_na <- function(row) {
    
    head(which(row== max(row, na.rm=TRUE)), 1)[1]
  }
  
  if(is.null(d)) {       ## For races excluding Asian and PacIsl
    # filter by race, pivot_wider, select the columns we want, get race long_name
    z <- x %>% filter(race_generic == y) %>% mutate(indicator = paste0(indicator, "_ind")) %>% pivot_wider(names_from = indicator, values_from = disparity_z_score) %>% group_by(geoid, geo_name) %>% 
      fill(ends_with("ind"), .direction = 'updown')  %>% 
      filter (!duplicated(geo_name)) %>% select(-race) %>% rename(race = race_generic) %>% select(geoid, geo_name, race, ends_with("ind"))
    z <- z %>% inner_join(race_names, by = c('race' = 'race_generic')) %>% select(geoid, geo_name, race, long_name, everything()) 
    
    # count indicators
    indicator_count <- z %>% ungroup %>% select(-geoid:-long_name)
    indicator_count$indicator_count <- rowSums(!is.na(indicator_count))
    z$indicator_count <- indicator_count$indicator_count 
    
    # select columns we need
    z <- z %>% select(geoid, geo_name, race, long_name, indicator_count, everything())
    
    # remove "ind" in columns
    colnames(z) <- gsub("_ind", "", colnames(z))
    
    
    
    # unique indicators that apply to race
    indicator_col <- z %>% ungroup %>% select(6:ncol(z))
    indicator_col <- names(indicator_col)
    
    # pull the column name with the maximum value
    z$max_col <- colnames(z[indicator_col]) [
      apply(
        z[indicator_col],
        MARGIN = 1,
        find_first_max_index_na )
    ]
    
    ## merge with indicator
    z <- left_join(z, indicator, by = c("max_col"="indicator_short"))
    
    ## add finding
    z <- z %>% mutate(
      finding = ifelse(indicator_count <= n,     ## Suppress finding if race+geo combo has 5 or fewer indicator disparity_z scores
                       paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."),
                       paste0(long_name, " residents face the most disparity with ", indicator, " in ", geo_name, "."))
    ) %>% select(
      geoid, geo_name, race, long_name, indicator, indicator_count, finding) %>% arrange(geo_name)
    
    return(z)
  }
  
  else {       ## For Asian and PacIsl only bc we count Asian+API and PacIsl+API
    # filter by race, pivot_wider, select the columns we want, get race long_name
    z <- x %>% filter(race_generic == y | race_generic == d) %>% mutate(indicator = paste0(indicator, "_ind")) %>% pivot_wider(names_from = indicator, values_from = disparity_z_score) %>% group_by(geoid, geo_name) %>% 
      fill(ends_with("ind"), .direction = 'updown')  %>% 
      filter (!duplicated(geo_name)) %>% select(-race_generic) %>% mutate(race = y) %>% select(geoid, geo_name, race, ends_with("ind"))
    z <- z %>% inner_join(race_names, by = c('race' = 'race_generic')) %>% select(geoid, geo_name, race, long_name, everything()) 
    
    indicator_count <- z %>% ungroup %>% select(-geoid:-long_name)
    indicator_count$indicator_count <- rowSums(!is.na(indicator_count))
    z$indicator_count <- indicator_count$indicator_count 
    
    # select columns we need
    z <- z %>% select(geoid, geo_name, race, long_name, indicator_count, everything())
    
    # remove "ind" in columns
    colnames(z) <- gsub("_ind", "", colnames(z))
    
    # unique indicators that apply to race
    indicator_col <- z %>% ungroup %>% select(6:ncol(z))
    indicator_col <- names(indicator_col)
    
    # pull the column name with the maximum value
    z$max_col <- colnames(z[indicator_col]) [
      apply(
        z[indicator_col],
        MARGIN = 1,
        find_first_max_index_na )
    ]
    
    ## merge with indicator
    z <- left_join(z, indicator, by = c("max_col"="indicator_short"))
    
    ## add finding
    z <- z %>% mutate(
      finding = ifelse(indicator_count<=n, ## threshold of equal or less than 5
                       paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."),
                       paste0(long_name, " residents face the most disparity with ", indicator, " in ", geo_name, "."))
    ) %>% select(
      geoid, geo_name, race, long_name, indicator, indicator_count, finding) %>% arrange(geo_name)
    
    return(z)
  }
}

# copy df before running any code
# Most Disparate Indicator by Race - RACE PAGE
df_ds <- filter(df, race != 'total')    # remove total rates bc all findings in this section are raced
n = 5 # threshold-- manually update this

aian_ <- most_disp_by_race(df_ds, 'aian', d = NULL)

asian_ <- most_disp_by_race(df_ds, 'asian', 'api')

black_ <- most_disp_by_race(df_ds, 'black', d = NULL)

latinx_ <- most_disp_by_race(df_ds, 'latino', d = NULL)

pacisl_ <- most_disp_by_race(df_ds, 'pacisl', 'api')

white_ <- most_disp_by_race(df_ds, 'white', d = NULL)

final_findings <- bind_rows(aian_, asian_, black_, latinx_, pacisl_, white_) 


##  most_disp <- final_findings %>% mutate(geo_level = ifelse(geo_name == 'California', 'state', 'county'),
##                                         finding_type = 'most disparate',
##                                        findings_pos = 3, geo_name = gsub(' County', '', geo_name))

most_disp <- final_findings %>% mutate(geo_level = case_when(
  grepl('City', geo_name) ~ "city",
  grepl('County', geo_name) ~ "county",
  grepl('California', geo_name) ~ "state"
),finding_type = 'most disparate', findings_pos = 3)



## Extra step: add city council district finding to findings related to education

educ_indicators <- unique(df_education_city$indicator)
educ_indicators <- indicator %>% filter(
  indicator_short %in% educ_indicators
)
educ_indicators <- unique(educ_indicators$indicator)

## most disparate district by race and city. This should have 1 district and 1 indicator per race and geo for education though there may be ties.
top_disparate_district_race <- df_education_district_weighted %>% filter(!race_generic %in% c("total", "filipino", "twoormor") & issue == "educ" & !is.na(disparity_z_score)) %>% select(geoid, geo_name, dist_id, district_name, race_generic, total_enroll, indicator, disparity_z_score) %>% group_by(geoid, race_generic) %>%  mutate(rk = min_rank(-disparity_z_score)) %>%  filter(rk == "1") %>% select(geoid, dist_id, district_name, race_generic, total_enroll) %>% rename(race = race_generic) %>% group_by(geoid, race) %>% 
  mutate(perf_ties = n())  %>% 
  mutate(district_name = paste0(district_name, collapse = " and "),
         total_enroll = paste0(total_enroll, collapse = " and "),
         dist_id = paste0(dist_id, collapse = " and ")) %>% 
  ungroup() %>%  select(geoid, dist_id, district_name, race, total_enroll) %>% unique()

## merge this back with most disparate
most_disp2 <- most_disp %>% left_join(
  top_disparate_district_race, by = c("geoid", "race")  
) %>% mutate(finding = 
               ifelse(
                 indicator %in% educ_indicators & geo_level == "city" & !grepl('too limited', finding), ## add city council district findings to education automated findings
                 paste0(long_name, " residents face the most disparity with ", indicator, " in ", geo_name, " (", district_name, ")."),
                 finding)) %>% relocate(any_of(c("dist_id", "district_name", "total_enroll")), .after = geo_name)

most_disp3 <- most_disp2 %>% mutate(
  dist_id = 
    ifelse(
      !indicator %in% educ_indicators & geo_level == "city", NA, dist_id ## remove district ids for non education indicators
    ),
  district_name = 
    ifelse(
      !indicator %in% educ_indicators & geo_level == "city", NA, district_name ## remove district names for non education indicators
    ),
  
  total_enroll = 
    ifelse(
      !indicator %in% educ_indicators & geo_level == "city", NA, total_enroll ## remove district names for non education indicators
    ),
  
  
  finding = 
    ifelse(
      indicator %in% educ_indicators & geo_level == "city" & is.na(dist_id), ## remove findings where there is no district id for education observations. 
      paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."), 
      finding)
)

## Look at education findings
#education_findings <- most_disp3 %>% filter(
#indicator %in% educ_indicators & geo_level == "city" & !grepl('too limited', finding))  %>% arrange(geoid)

# Save most_disp, best_rate_counts, worst_rate_counts as 1 csv
rda_race_door_findings <- bind_rows(most_disp3, worst_best_counts)
rda_race_door_findings <- rda_race_door_findings %>% relocate(geo_level, .after = geo_name) %>% relocate(finding_type, .after = race) %>% mutate( src = 'rda', citations = '') %>%
  mutate(race = ifelse(race == 'latino', 'latinx', ifelse(race == 'pacisl', 'nhpi', race)))  # rename latino to latinx, and pacisl to nhpi to feed API - will change API later so we can use RC standard latino/pacisl


## these findings were wrong in V4 and need to be re-QA'ed
findings_changed <- rda_race_door_findings %>% filter(geo_level %in% c("county", "state") & indicator == "Incarceration" & !grepl('limited', finding)) %>% select(geoid, geo_name, race, finding)


#### Compare rda_race_door_findings with arei_findings_races_multigeo
# arei_findings_races_multigeo <- dbGetQuery(con, "SELECT * FROM v5.arei_findings_races_multigeo")


## Create postgres table
#dbWriteTable(con, c("v5", "arei_racedoor_findings_multigeo_test"), rda_race_door_findings,
#            overwrite = FALSE, row.names = FALSE)



# comment on table and columns
comment <- paste0("COMMENT ON TABLE v5.arei_racedoor_findings_multigeo_test IS 'findings for Race pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaway\\key_findings_2023_city.R.';",
                  "COMMENT ON COLUMN v5.arei_racedoor_findings_multigeo_test.finding_type
                         IS 'Categorizes findings: count of best and worst rates by race/geo combo, most disparate indicator by race/geo combo';",
                  "COMMENT ON COLUMN v5.arei_racedoor_findings_multigeo_test.src
                         IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN v5.arei_racedoor_findings_multigeo_test.citations
                         IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN v5.arei_racedoor_findings_multigeo_test.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
print(comment)
#dbSendQuery(con, comment)



### This section creates findings for Place page - the most disparate and worst performance indicators across counties #####
disp_long <- df %>% filter(race == "total" & geo_level %in% c("county", "city") & !is.na(disparity_z_score) & !disparity_z_score == "0") %>% select(geoid, geo_name, indicator, disparity_z_score, geo_level) %>% rename(variable = indicator, value = disparity_z_score) %>% mutate(geo_name = gsub('County', '', geo_name),
                                                                                                                                                                                                                                                                                     geo_name = gsub('City', '', geo_name))

## Worst Disparity - PLACE PAGE ----

#### Rank indicators by disp_z with worst/highest disp_z = 1
disp_final <- disp_long %>%
  group_by(geoid, geo_name) %>%
  mutate(rk = min_rank(-value))


##### Select only worst/highest disparity indicator per county
disp_final <- disp_final %>% filter(rk == 1) %>% arrange(geoid) 

##### Rename variable and value fields
names(disp_final)[names(disp_final) == 'value'] <- 'worst_disp_z'
names(disp_final)[names(disp_final) == 'variable'] <- 'worst_disp_indicator'

# clean names for later merging
disp_final$worst_disp_indicator <- gsub('_disp_z', '', disp_final$worst_disp_indicator)

# join to get long indicator names for findings
worst_disp <- select(disp_final, -c(rk, worst_disp_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_disp_indicator" = "indicator_short")) %>% rename(long_disp_indicator = indicator)

# Adjust for counties with tied indicators
worst_disp2 <- worst_disp %>% 
  group_by(geoid,geo_name) %>% 
  mutate(disp_ties = n()) %>%
  mutate(long_disp_indicator = paste0(long_disp_indicator, collapse = " and ")) %>% select(-c(worst_disp_indicator)) %>% unique()



# Write Findings using ifelse statements
worst_disp2 <- worst_disp2 %>% 
  mutate(finding_type = 'worst disparity', finding = ifelse(geo_level == "county", 
                                                            paste0(geo_name, " County's high racial disparity in ", long_disp_indicator," stands out most compared to other counties."),
                                                            paste0(geo_name, " City's high racial disparity in ", long_disp_indicator," stands out most compared to other cities.")), 
         findings_pos = 4) %>% 
  select(geoid, geo_name, geo_level, long_disp_indicator, finding_type,finding, findings_pos) 


### Extra step: add finding related to education

# First, find highest disparity for total by district.
top_disparate_district_total <- df_education_district_weighted %>% filter(race_generic == "total" & issue == "educ" & !is.na(disparity_z_score) & !disparity_z_score == "0") %>% select(geoid, geo_name, dist_id, district_name, total_enroll, indicator, disparity_z_score) %>% group_by(geoid) %>%  mutate(rk = min_rank(-disparity_z_score)) %>%  filter(rk == "1") %>% select(geoid, dist_id, district_name,  total_enroll, indicator) %>% group_by(geoid) %>% 
  mutate(perf_ties = n())  %>% 
  mutate(district_name = paste0(district_name, collapse = " and "),
         total_enroll = paste0(total_enroll, collapse = " and "),
         dist_id = paste0(dist_id, collapse = " and ")) %>% 
  ungroup() %>% select(geoid, dist_id, district_name, total_enroll)  %>% select(geoid, dist_id, district_name, total_enroll) %>% unique()


# Now, merge this 
worst_disp3 <- worst_disp2 %>% left_join(
  top_disparate_district_total, by = c("geoid")
) %>%  mutate(finding = 
                ifelse(
                  long_disp_indicator %in% educ_indicators & geo_level == "city",
                  paste0(geo_name, " City's ", "(", district_name, ")", " high racial disparity in ", long_disp_indicator," stands out most compared to other cities."),
                  finding
                )
) %>% select(geoid, geo_name, dist_id, district_name, total_enroll, long_disp_indicator, geo_level, finding_type, finding, findings_pos)

## remove district ids and names for non education indicators
worst_disp4 <- worst_disp3 %>% mutate(
  dist_id = 
    ifelse(
      !long_disp_indicator %in% educ_indicators & geo_level == "city", NA, dist_id ## remove district ids for non education indicators
    ),
  district_name = 
    ifelse(
      !long_disp_indicator %in% educ_indicators & geo_level == "city", NA, district_name ## remove district names for non education indicators
    ),
  total_enroll = 
    ifelse(
      !long_disp_indicator %in% educ_indicators & geo_level == "city", NA, total_enroll ## remove district names for non education indicators
    ),
  
  finding = 
    ifelse(
      long_disp_indicator %in% educ_indicators & geo_level == "city" & is.na(dist_id), ## remove findings where there is no district id for education observations. 
      paste0("Data for ", long_name, " is too limited for this analysis."), 
      finding)
) %>% select(-long_disp_indicator)

------    
  ## Worst Performance - PLACE PAGE ----

# Keep only perf_z columns
#select_cols <- lapply(data_list, select, county_id,
#                      county_name,
#                      ends_with(c("perf_z")))

# Merge into 1 matrix, removing duplicative county_id columns
#merged_perf <- reduce(select_cols, inner_join, by = c("county_name", "county_id"))

### Convert table from wide to long format
#perf_long <- reshape2::melt(merged_perf, id.vars=c("county_id", "county_name"))
#perf_long <- rename(perf_long, geo_name = county_name, geoid = county_id) %>% mutate(geo_level = "county")

perf_long <- df %>% filter(race == "total" & geo_level %in% c("county", "city") & !is.na(performance_z_score) & !performance_z_score == "0") %>% select(geoid, geo_name, indicator, performance_z_score, geo_level) %>% rename(variable = indicator, value = performance_z_score) %>% mutate(geo_name = gsub('County', '', geo_name),
                                                                                                                                                                                                                                                                                             geo_name = gsub('City', '', geo_name))

#### Rank indicators by perf_z with worst/lowest perf_z = 1

perf_final <- perf_long %>%
  group_by(geoid, geo_name) %>%
  mutate(rk = min_rank(value))


##### Select only worst/lowest performance indicator per county
perf_final <- perf_final %>% filter(rk == 1) %>% arrange(geoid)

##### Rename variable and value fields
names(perf_final)[names(perf_final) == 'value'] <- 'worst_perf_z'
names(perf_final)[names(perf_final) == 'variable'] <- 'worst_perf_indicator'    

# clean names for later merging
perf_final$worst_perf_indicator <- gsub('_perf_z', '', perf_final$worst_perf_indicator)

# join to get long indicator names for findings

worst_perf <- select(perf_final, -c(rk, worst_perf_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_perf_indicator" = "indicator_short")) %>% rename(long_perf_indicator = indicator)

# Adjust for counties with tied indicators
worst_perf2 <- worst_perf %>% 
  group_by(geoid, geo_name) %>% 
  mutate(perf_ties = n()) %>%
  mutate(long_perf_indicator = paste0(long_perf_indicator, collapse = " and ")) %>% select(-c(worst_perf_indicator)) %>% unique() 


# Write Findings using ifelse statements
worst_perf2 <- worst_perf2 %>% 
  mutate(finding_type = 'worst overall outcome', finding = ifelse(geo_level == "county", 
                                                                  paste0(geo_name, " County's low overall outcome in ", long_perf_indicator, " stands out most compared to other counties."),
                                                                  paste0(geo_name, " City's low overall outcome in ", long_perf_indicator," stands out most compared to other cities.")),  
         findings_pos = 5) %>% 
  select(geoid, geo_name, geo_level, long_perf_indicator,  finding_type, finding, findings_pos)

### Extra step: add finding related to education


# First, find lowest performance for total by district.
lowest_performance_district_total <- df_education_district_weighted %>% filter(race_generic == "total" & issue == "educ" & !is.na(performance_z_score) & !performance_z_score == "0") %>% select(geoid, geo_name, dist_id, district_name, total_enroll, indicator, performance_z_score) %>% group_by(geoid) %>%  mutate(rk = min_rank(performance_z_score)) %>%  filter(rk == "1") %>% select(geoid, dist_id, district_name,  total_enroll, indicator) %>% group_by(geoid) %>% 
  mutate(perf_ties = n())  %>% 
  mutate(district_name = paste0(district_name, collapse = " and "),
         total_enroll = paste0(total_enroll, collapse = " and "),
         dist_id = paste0(dist_id, collapse = " and ")) %>% 
  ungroup() %>% select(geoid, dist_id, district_name, total_enroll)  %>% select(geoid, dist_id, district_name, total_enroll) %>% unique()

# Now, merge this 
worst_perf3 <- worst_perf2 %>% left_join(
  lowest_performance_district_total, by = c("geoid")
) %>%  mutate(finding = 
                ifelse(
                  long_perf_indicator %in% educ_indicators & geo_level == "city",
                  paste0(geo_name, " City's ", "(", district_name, ")", " low overall outcome in ", long_perf_indicator," stands out most compared to other cities."),
                  finding
                )
) %>% select(geoid, geo_name, dist_id, district_name, total_enroll, long_perf_indicator, geo_level, finding_type, finding, findings_pos)



## remove district ids and names for non education indicators
worst_perf4 <- worst_perf3 %>% mutate(
  dist_id = 
    ifelse(
      !long_perf_indicator %in% educ_indicators & geo_level == "city", NA, dist_id ## remove district ids for non education indicators
    ),
  district_name = 
    ifelse(
      !long_perf_indicator %in% educ_indicators & geo_level == "city", NA, district_name ## remove district names for non education indicators
    ),
  total_enroll = 
    ifelse(
      !long_perf_indicator %in% educ_indicators & geo_level == "city", NA, total_enroll ## remove district names for non education indicators
    ),
  
  finding = 
    ifelse(
      long_perf_indicator %in% educ_indicators & geo_level == "city" & is.na(dist_id), ## remove findings where there is no district id for education observations. 
      paste0("Data for ", long_name, " is too limited for this analysis."), 
      finding)
) %>% select(-long_perf_indicator)



# Combine findings into one final df
worst_disp_perf <- union(worst_disp4, worst_perf4)


### This section creates findings for Place page- the summary statements above/below avg disparity/performance across counties ####
# Indicators

## pull composite disparity/performance z-scores for city and county; create urban type NA for cities.
index_county <- st_read(con, query = "SELECT * FROM v5.arei_composite_index_2023") %>% select(county_id, county_name, urban_type, disparity_z, performance_z) %>% rename(geoid = county_id, geo_name = county_name) %>% mutate(geo_level = "county")

index_city <- st_read(con, query = "SELECT * FROM v5.arei_composite_index_city_2023") %>% select(city_id, city_name, disparity_z, performance_z) %>% rename(geoid = city_id, geo_name = city_name) %>% mutate(urban_type = NA, geo_level = "city")


# Above/Below Avg Disp/Perf - PLACE PAGE
sum_statement_df_county <- index_county  %>% 
  mutate(perf_type = ifelse(performance_z < 0, 'below', 'above'),
         pop_type = ifelse(urban_type == 'Urban', 'more', 'less'),
         disp_type = ifelse(disparity_z < 0, 'below', 'above')) 

sum_statement_df_city <- index_city  %>% 
  mutate(perf_type = ifelse(performance_z < 0, 'below', 'above'),
         pop_type = NA,
         disp_type = ifelse(disparity_z < 0, 'below', 'above')) 

sum_statement_df <- rbind(sum_statement_df_county, sum_statement_df_city)       


disp_avg_statement <- sum_statement_df  %>%
  mutate(finding_type = 'disparity', finding = ifelse(geo_level == "county", paste0(geo_name, " County's racial disparity across indicators is ", disp_type, " average for California counties."),paste0(geo_name, " City's racial disparity across indicators is ", disp_type, " average for California cities.")),
         finding = ifelse(is.na(disp_type), NA, finding),
         findings_pos = 2) %>% 
  select(geoid, geo_name, geo_level, finding_type, finding, findings_pos)

### replace null statements with "finding is too limited for geo_name"

disp_avg_statement <- disp_avg_statement %>% mutate(finding = 
                                                      ifelse(is.na(finding), paste0("Data for ", geo_name, " ", geo_level, " is too limited for this analysis."), finding))

perf_avg_statement <- sum_statement_df  %>% 
  mutate(finding_type = 'outcomes', finding = ifelse(geo_level == "county", paste0(geo_name, " County's overall outcomes across indicators are ", perf_type, " average for California counties."), paste0(geo_name, " County's overall outcomes across indicators are ", perf_type, " average for California counties.")),
         
         finding = ifelse(is.na(perf_type), NA, finding),
         findings_pos = 3) %>% 
  select(geoid, geo_name, geo_level, finding_type, finding, findings_pos) 

### replace null statements with "finding is too limited for geo_name"
perf_avg_statement <- perf_avg_statement %>% mutate(finding = 
                                                      ifelse(is.na(finding), paste0("Data for ", geo_name, " ", geo_level, " is too limited for this analysis."), finding))

## bind everything together
rda_places_findings <- rbind(most_impacted, disp_avg_statement, perf_avg_statement, worst_disp_perf) %>%
  mutate(src = 'rda', citations = '') %>%
  relocate(geo_level, .after = geo_name)


## compare with v4 findings 
arei_findings_places_multigeo <- dbGetQuery(con, "SELECT * FROM v5.arei_findings_places_multigeo")


## Create postgres table
#dbWriteTable(con, c("v5", "arei_findings_places_multigeo_test"), rda_places_findings,
#             overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE v5.arei_findings_places_multigeo_test IS 'findings for Race pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaway\\key_findings_2023_city.R.';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_test.finding_type
                        IS 'Categorizes findings: race most impacted by inequities in a geo, above/below avg disp, above/below perf, most disp indicator, worst perf indicator';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_test.src
                        IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_test.citations
                        IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_test.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
print(comment)
#dbSendQuery(con, comment)






