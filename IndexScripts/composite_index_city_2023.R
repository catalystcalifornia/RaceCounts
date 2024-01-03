#### Composite Index (z-score) for RC v5: Draft and Screened ####

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgreSQL","sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# Packages ----------------------------------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(sf)
library(usethis)

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# Set source for index functions
source("W:/Project/RACE COUNTS/Functions/RC_Index_Functions.R")

# pull in cross-walk to go from district to city
crosswalk <- dbGetQuery(con, "SELECT  city_id, city_name, dist_id, total_enroll FROM v5.arei_city_county_district_table")


######################### DRAFT/UNSCREENED CITY INDEX ########################### ---------------------------
# Education Section -------------------------------------------------------
## These indicators require extra prep bc they are at school district not city level
# pull in list of tables in racecounts.v5
rc_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v5"))$table, function(x) slot(x, 'name'))))

## Pull in city (or district) level education indicators
education_list <- filter(rc_list, grepl("_district_2023",table)) # pull only district level indicator tables
education_list <- filter(education_list, grepl("arei_",table)) # get only unscreened indicator tables
education_list <- education_list [order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on education_list
education_tables_list <- lapply(setNames(paste0("select * from v5.", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables_list <- map2(education_tables_list, names(education_tables_list), ~ mutate(.x, indicator = .y)) # create column with indicator name

education_tables_list <- lapply(education_tables_list, function(x) x %>% select(dist_id, district_name, disparity_z, performance_z, indicator))

education_tables <- imap_dfr(education_tables_list, ~
               .x %>% 
                   pivot_longer(cols = disparity_z,
                 names_to = "measure",
                 values_to = "disparity_z_score_unweighted")) %>% select(-measure)  

education_tables <- education_tables %>% mutate(indicator =  gsub('arei_', '', indicator)) %>% mutate(indicator =  gsub('_district_2023', '', indicator)) %>%
                                         rename(performance_z_score_unweighted = performance_z) %>% relocate(indicator, .after = disparity_z_score_unweighted)

df_education_district <- education_tables %>% left_join(crosswalk, by = "dist_id") %>% filter(!is.na(city_id))


## extra step: weighted averages by enrollment size per indicator (some cities have multiple districts but not every indicator will have multiple districts per city).

enrollment_percentages <- df_education_district %>% select(city_id, dist_id, total_enroll, indicator) %>% unique() %>% group_by(city_id, indicator) %>% 
                                                    mutate(sum_total_enroll = sum(total_enroll, na.rm = T), 
                                                           percent_total_enroll = (total_enroll/sum_total_enroll)) %>% ungroup() %>% 
                                                    select(city_id, dist_id, indicator, total_enroll, sum_total_enroll, percent_total_enroll)

df_education_district_weighted <- df_education_district %>% left_join(enrollment_percentages, by = c("city_id","dist_id","indicator","total_enroll")) %>%
                                                            mutate(disparity_z_score = disparity_z_score_unweighted * percent_total_enroll,
                                                                   performance_z_score = performance_z_score_unweighted * percent_total_enroll)

df_education_city <- df_education_district_weighted %>% group_by(city_id, city_name, indicator) %>% summarize(disp_z = sum(disparity_z_score), perf_z = sum(performance_z_score))


education_tables_agg <- df_education_city %>% pivot_wider(
  names_from = indicator,
  values_from = c(disp_z, perf_z),
  names_glue = "{indicator}_{.value}",
)


## Add the rest of the indicators  ------------------------------------------------------

# pull in list of tables in racecounts.v5

# filter for only city level indicator tables
city_list <- filter(rc_list, grepl("_city_2023",table))
city_list <- filter(city_list, grepl("arei_",table)) # get only unscreened indicator tables
city_list <- filter(city_list, !grepl("index", table)) # filter out index in case there is a prev version in postgres

city_list <- city_list[order(city_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step


# import all tables on city_list
city_tables <- lapply(setNames(paste0("select * from v5.", city_list), city_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
city_tables <- map2(city_tables, names(city_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name


# rename column 
city_tables_short <- lapply(city_tables, function(x) x %>% select(city_id, city_name, disparity_z, performance_z, indicator))

city_tables_updated <-
  lapply(names(city_tables_short), function(i){
    x <- city_tables_short[[ i ]]
    # set 2nd column to a new name
    names(x)[3] <- paste0(i, "_disp_z")
    names(x)[4] <- paste0(i, "_perf_z")
    # return
    x
  })

# only select columns we want
city_tables_updated <- lapply(city_tables_updated, function(x) x%>% select(city_id, ends_with("disp_z"), ends_with("perf_z")))
city_tables_updated <- city_tables_updated %>% reduce(full_join) # convert list to df
names(city_tables_updated)[-1] <- substring(names(city_tables_updated)[-1],6) # clean colnames
names(city_tables_updated)[-1] <- gsub(x = names(city_tables_updated)[-1], pattern = "city_2023_", replacement = "", names) # clean colnames

# make into df
arei_race_multigeo <- dbGetQuery(con, "SELECT geoid, name, geolevel, total_pop FROM v5.arei_race_multigeo") %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

# merge to get city names and education table
city_tables_df <- city_tables_updated %>% arrange(city_id) %>% distinct(city_id, .keep_all = TRUE) %>% left_join(education_tables_agg) %>% left_join(arei_race_multigeo) %>% select(city_id, city_name, everything())

# remove cities that are actually universities/colleges: RC v5 there are 6 of them
city_tables_df <- city_tables_df %>% filter(!grepl('University', city_name))

# cap perf_z and disp_z values  at |3.5|. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
indicator_cap <- 3.5
city_tables_capped <- city_tables_df %>% mutate(across(ends_with("disp_z"), 
             ~ case_when(. > indicator_cap ~ indicator_cap, 
                       . < -indicator_cap ~ -indicator_cap,
                       TRUE ~ .))) %>% 
  
  mutate(across(ends_with("perf_z"), 
             ~ case_when(. > indicator_cap ~ indicator_cap, 
                       . < -indicator_cap ~ -indicator_cap,
                       TRUE ~ .)))


# Count the number of valid disp_z and perf_z per city. We will use this later
all_indicators_perf_count <- city_tables_df %>% select(city_id, ends_with("perf_z")) %>%
  mutate(all_indicators_perf_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_perf_count)

all_indicators_disp_count <- city_tables_df %>% select(city_id, ends_with("disp_z")) %>%
  mutate(all_indicators_disp_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_disp_count)


# count number of indicators per issue area
indicators <- names(city_tables) %>% as.data.frame()
educ_indicators <- unique(education_tables$indicator) %>% as.data.frame()


crim_count <- length(grep("crim", indicators$.))
demo_count <- length(grep("demo", indicators$.))
econ_count <- length(grep("econ", indicators$.))
hben_count <- length(grep("hben", indicators$.))
hlth_count <- length(grep("hlth", indicators$.))
hous_count <- length(grep("hous", indicators$.))
educ_count <- length(grep("educ", educ_indicators$.))



# make separate data-frames for all
crim <- city_tables_capped %>% select(city_id, city_name, starts_with("crim")) %>% mutate(crim_count = crim_count) 
demo <- city_tables_capped %>% select(city_id, city_name, starts_with("demo")) %>% mutate(demo_count = demo_count) 
econ <- city_tables_capped %>% select(city_id, city_name, starts_with("econ")) %>% mutate(econ_count = econ_count) 
hben <- city_tables_capped %>% select(city_id, city_name, starts_with("hben")) %>% mutate(hben_count = hben_count) 
hlth <- city_tables_capped %>% select(city_id, city_name, starts_with("hlth")) %>% mutate(hlth_count = hlth_count) 
hous <- city_tables_capped %>% select(city_id, city_name, starts_with("hous")) %>% mutate(hous_count = hous_count) 
educ <- city_tables_capped %>% select(city_id, city_name, starts_with("educ")) %>% mutate(educ_count = educ_count) 


## Set issue area thresholds
crim_threshold <- 1
demo_threshold <- 1
econ_threshold <- 2 # could be 3
educ_threshold <- 2
hlth_threshold <- 1
hben_threshold <- 2
hous_threshold <- 3


# Calc issue area z-scores (avg of issue area's indicator z-scores)
crim_index <-  calculate_city_issue(crim, crim_count, crim_threshold) %>% rename_with(~ paste0('crime_and_justice', "_", .x), ends_with("z"))
demo_index <-  calculate_city_issue(demo, demo_count, demo_threshold) %>% rename_with(~ paste0('democracy', "_", .x), ends_with("z"))
econ_index <-  calculate_city_issue(econ, econ_count, econ_threshold) %>% rename_with(~ paste0('economic_opportunity', "_", .x), ends_with("z"))
hben_index <-  calculate_city_issue(hben, hben_count, hben_threshold) %>% rename_with(~ paste0('healthy_built_environment', "_", .x), ends_with("z"))
hlth_index <-  calculate_city_issue(hlth, hlth_count, hlth_threshold) %>% rename_with(~ paste0('health_care_access', "_", .x), ends_with("z"))
hous_index <-  calculate_city_issue(hous,hous_count, hous_threshold) %>% rename_with(~ paste0('housing', "_", .x), ends_with("z"))
educ_index <-  calculate_city_issue(educ,educ_count, educ_threshold) %>% rename_with(~ paste0('education', "_", .x), ends_with("z"))


# merge all issue tables together
all_index <- crim_index %>% select(city_id, ends_with("z")) %>% left_join(
                                      demo_index %>% select(city_id, ends_with("z"))   
                                    )   %>% left_join(
                                      econ_index %>% select(city_id, ends_with("z"))   
                                    )   %>% left_join(
                                      hben_index %>% select(city_id, ends_with("z"))   
                                    )   %>% left_join(
                                      hlth_index %>% select(city_id, ends_with("z"))   
                                    )   %>% left_join(
                                      hous_index %>% select(city_id, ends_with("z"))   
                                    )   %>% left_join(
                                      educ_index %>% select(city_id, ends_with("z"))   
                                    )  %>% left_join(arei_race_multigeo) %>% mutate(
                                      all_indicators_disp_count = all_indicators_disp_count$all_indicators_disp_count,
                                      all_indicators_perf_count = all_indicators_perf_count$all_indicators_perf_count,
                                    ) %>% select(
                                      city_id, city_name, ends_with("count"), everything()
                                    )  


# Calc composite index
issue_area_threshold <- 4 # city must have at least 3 issue perf/disp z-scores or it will be suppressed
indicator_threshold <- 13 # city must have data for at least 13 indicators or it will be suppressed

city_index_draft <- calculate_city_index(all_index, issue_area_threshold, indicator_threshold)
city_index_draft <- city_index_draft %>% select(city_id, city_name, all_indicators_disp_count, all_indicators_perf_count, disp_values_count, perf_values_count,
                                                disparity_rank, performance_rank, quadrant, disparity_z, performance_z, everything()) %>% arrange(-disparity_z)

# Export to postgres ------------------------------------------------------
table_name <- "arei_composite_index_city_2023_draft"
table_schema <- "v5"
table_comment_source <- "This is the UNSCREENED city index table including threshold for representation across all issue areas and indicators.
This draft has been saved as materialized view: v5.arei_composite_index_city_2023_draft_m
R script used to calculate rates and import table: W:/Project/RACE COUNTS/2023_v5/RC_Github/RaceCounts/IndexScripts/composite_index_city_2023.R 
QA document: W:/Project/RACE COUNTS/2023_v5/Composite Index/Documentation/QA_sheet_Composite_Index_City.docx;" 

# make character vector for field types in postgresql db
charvect = rep('numeric', dim(city_index_draft)[2])

# change data type for columns
charvect[c(1:2)] <- "varchar" # Define which cols are character for the geoid and names etc

# add names to the character vector
names(charvect) <- colnames(city_index_draft)

# dbWriteTable(con, c(table_schema, table_name), city_index_draft, overwrite = FALSE, row.names = FALSE, field.types = charvect)

# write comment to table, and the first three fields that won't change.
table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

## send table comment to database
#dbSendQuery(conn = con, table_comment)      	


######################### SCREENED CITY INDEX ########################### ---------------------------
### Pull z-scores from the "api_" city/dist indicator tables that contain calcs only for the cities that pass the total_pop threshold screen.
###### Dataframes etc below have "_s" suffix to denote they contain screened data

# Education Section -------------------------------------------------------
## These indicators require extra prep bc they are at school district not city level
# pull in list of tables in racecounts.v5
rc_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v5"))$table, function(x) slot(x, 'name'))))

## Pull in city (or district) level education indicators
education_list_s <- filter(rc_list, grepl("_district_2023",table)) # pull only district level indicator tables
education_list_s <- filter(education_list_s, grepl("api_",table)) # pull only "api_" (screened) indicator tables
education_list_s <- education_list_s [order(education_list_s$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on education_list_s
education_tables_list_s <- lapply(setNames(paste0("select * from v5.", education_list_s), education_list_s), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables_list_s  <- map2(education_tables_list_s, names(education_tables_list_s), ~ mutate(.x, indicator = .y)) # create column with indicator name

education_tables_list_s <- lapply(education_tables_list_s, function(x) x %>% select(dist_id, district_name, disparity_z, performance_z, indicator))

education_tables_s <- imap_dfr(education_tables_list_s, ~
                               .x %>% 
                               pivot_longer(cols = disparity_z,
                                            names_to = "measure",
                                            values_to = "disparity_z_score_unweighted")) %>% select(-measure)  

education_tables_s <- education_tables_s %>% mutate(indicator =  gsub('api_', '', indicator)) %>% mutate(indicator =  gsub('_district_2023', '', indicator)) %>%
  rename(performance_z_score_unweighted = performance_z) %>% relocate(indicator, .after = disparity_z_score_unweighted)

df_education_district_s <- education_tables_s %>% left_join(crosswalk, by = "dist_id") %>% filter(!is.na(city_id))


## extra step: weighted averages by enrollment size per indicator (some cities have multiple districts but not every indicator will have multiple districts per city).

enrollment_percentage_s <- df_education_district_s %>% select(city_id, dist_id, total_enroll, indicator) %>% unique() %>% group_by(city_id, indicator) %>% 
  mutate(sum_total_enroll = sum(total_enroll, na.rm = T), 
         percent_total_enroll = (total_enroll/sum_total_enroll)) %>% ungroup() %>% 
  select(city_id, dist_id, indicator, total_enroll, sum_total_enroll, percent_total_enroll)

df_education_district_weighted_s <- df_education_district_s %>% left_join(enrollment_percentage_s, by = c("city_id","dist_id","indicator","total_enroll")) %>%
  mutate(disparity_z_score = disparity_z_score_unweighted * percent_total_enroll,
         performance_z_score = performance_z_score_unweighted * percent_total_enroll)

df_education_city_s <- df_education_district_weighted_s %>% group_by(city_id, city_name, indicator) %>% summarize(disp_z = sum(disparity_z_score), perf_z = sum(performance_z_score))


education_tables_agg_s <- df_education_city_s %>% pivot_wider(
  names_from = indicator,
  values_from = c(disp_z, perf_z),
  names_glue = "{indicator}_{.value}",
)


## Add the rest of the indicators  ------------------------------------------------------

# pull in list of tables in racecounts.v5

# filter for only city level indicator tables
city_list_s <- filter(rc_list, grepl("_city_2023",table)) # pull only city level indicator tables
city_list_s <- filter(city_list_s, grepl("api_",table)) # pull only screened indicator tables
city_list_s <- filter(city_list_s, !grepl("index", table)) # filter out index in case there is a prev version in postgres

city_list_s <- city_list_s[order(city_list_s$table), ] # alphabetize list of tables, changes df to list the needed format for next step


# import all tables on city_list_s
city_tables_s <- lapply(setNames(paste0("select * from v5.", city_list_s), city_list_s), DBI::dbGetQuery, conn = con)

# create column with indicator name
city_tables_s <- map2(city_tables_s, names(city_tables_s), ~ mutate(.x, indicator = .y)) # create column with indicator name

# rename column 
city_tables_short_s <- lapply(city_tables_s, function(x) x %>% select(city_id, city_name, disparity_z, performance_z, indicator))

city_tables_updated_s <-
  lapply(names(city_tables_short_s), function(i){
    x <- city_tables_short_s[[ i ]]
    # set 2nd column to a new name
    names(x)[3] <- paste0(i, "_disp_z")
    names(x)[4] <- paste0(i, "_perf_z")
    # return
    x
  })

# only select columns we want
city_tables_updated_s <- lapply(city_tables_updated_s, function(x) x%>% select(city_id, ends_with("disp_z"), ends_with("perf_z")))
city_tables_updated_s <- city_tables_updated_s %>% reduce(full_join) # convert list to df
names(city_tables_updated_s)[-1] <- substring(names(city_tables_updated_s)[-1],5) # clean colnames
names(city_tables_updated_s)[-1] <- gsub(x = names(city_tables_updated_s)[-1], pattern = "city_2023_", replacement = "", names) # clean colnames

# make into df
arei_race_multigeo <- dbGetQuery(con, "SELECT geoid, name, geolevel, total_pop FROM v5.arei_race_multigeo") %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

# merge to get city names and education table
city_tables_df_s <- city_tables_updated_s %>% arrange(city_id) %>% distinct(city_id, .keep_all = TRUE) %>% left_join(education_tables_agg_s) %>% left_join(arei_race_multigeo) %>% select(city_id, city_name, everything())

# remove cities that are actually universities/colleges: RC v5 there are 6 of them
city_tables_df_s <- city_tables_df_s %>% filter(!grepl('University', city_name))

# cap perf_z and disp_z values  at |3.5|. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
indicator_cap <- 3.5
city_tables_capped_s <- city_tables_df_s %>% mutate(across(ends_with("disp_z"), 
                                                       ~ case_when(. > indicator_cap ~ indicator_cap, 
                                                                   . < -indicator_cap ~ -indicator_cap,
                                                                   TRUE ~ .))) %>% 
  
                                         mutate(across(ends_with("perf_z"), 
                                                       ~ case_when(. > indicator_cap ~ indicator_cap, 
                                                                   . < -indicator_cap ~ -indicator_cap,
                                                                   TRUE ~ .)))

# Count the number of valid disp_z and perf_z per city. We will need this later
all_indicators_perf_count_s <- city_tables_df_s %>% select(city_id, ends_with("perf_z")) %>%
  mutate(all_indicators_perf_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_perf_count)

all_indicators_disp_count_s <- city_tables_df_s %>% select(city_id, ends_with("disp_z")) %>%
  mutate(all_indicators_disp_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_disp_count)


# Count number of indicators per issue area
indicators_s <- names(city_tables_s) %>% as.data.frame()
educ_indicators_s <- unique(education_tables_s$indicator) %>% as.data.frame()


crim_count_s <- length(grep("crim", indicators_s$.))
demo_count_s <- length(grep("demo", indicators_s$.))
econ_count_s <- length(grep("econ", indicators_s$.))
hben_count_s <- length(grep("hben", indicators_s$.))
hlth_count_s <- length(grep("hlth", indicators_s$.))
hous_count_s <- length(grep("hous", indicators_s$.))
educ_count_s <- length(grep("educ", educ_indicators_s$.))


# make separate dataframes for all
crim_s <- city_tables_capped_s %>% select(city_id, city_name, starts_with("crim")) %>% mutate(crim_count = crim_count) 
demo_s <- city_tables_capped_s %>% select(city_id, city_name, starts_with("demo")) %>% mutate(demo_count = demo_count) 
econ_s <- city_tables_capped_s %>% select(city_id, city_name, starts_with("econ")) %>% mutate(econ_count = econ_count) 
hben_s <- city_tables_capped_s %>% select(city_id, city_name, starts_with("hben")) %>% mutate(hben_count = hben_count) 
hlth_s <- city_tables_capped_s %>% select(city_id, city_name, starts_with("hlth")) %>% mutate(hlth_count = hlth_count) 
hous_s <- city_tables_capped_s %>% select(city_id, city_name, starts_with("hous")) %>% mutate(hous_count = hous_count) 
educ_s <- city_tables_capped_s %>% select(city_id, city_name, starts_with("educ")) %>% mutate(educ_count = educ_count) 


## issue area threshold
crim_threshold_s <- 1
demo_threshold_s <- 1
econ_threshold_s <- 2 # could be 3
educ_threshold_s <- 2
hlth_threshold_s <- 1
hben_threshold_s <- 2
hous_threshold_s <- 3


crim_index_s <-  calculate_city_issue(crim_s, crim_count_s, crim_threshold_s) %>% rename_with(~ paste0('crime_and_justice', "_", .x), ends_with("z"))
demo_index_s <-  calculate_city_issue(demo_s, demo_count_s, demo_threshold_s) %>% rename_with(~ paste0('democracy', "_", .x), ends_with("z"))
econ_index_s <-  calculate_city_issue(econ_s, econ_count_s, econ_threshold_s) %>% rename_with(~ paste0('economic_opportunity', "_", .x), ends_with("z"))
hben_index_s <-  calculate_city_issue(hben_s, hben_count_s, hben_threshold_s) %>% rename_with(~ paste0('healthy_built_environment', "_", .x), ends_with("z"))
hlth_index_s <-  calculate_city_issue(hlth_s, hlth_count_s, hlth_threshold_s) %>% rename_with(~ paste0('health_care_access', "_", .x), ends_with("z"))
hous_index_s <-  calculate_city_issue(hous_s, hous_count_s, hous_threshold_s) %>% rename_with(~ paste0('housing', "_", .x), ends_with("z"))
educ_index_s <-  calculate_city_issue(educ_s, educ_count_s, educ_threshold_s) %>% rename_with(~ paste0('education', "_", .x), ends_with("z"))


# merge all issue tables together
all_index_s <- crim_index_s %>% select(city_id, ends_with("z"), ends_with("z")) %>% left_join(
  demo_index_s %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  econ_index_s %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  hben_index_s %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  hlth_index_s %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  hous_index_s %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  educ_index_s %>% select(city_id, ends_with("z"))   
)  %>% left_join(arei_race_multigeo) %>% mutate(
  all_indicators_disp_count = all_indicators_disp_count_s$all_indicators_disp_count,
  all_indicators_perf_count = all_indicators_perf_count_s$all_indicators_perf_count,
) %>% select(
  city_id, city_name, ends_with("count"), ends_with("disparity_z"), ends_with("performance_z")
)

names(all_index_s) <- gsub(x = names(all_index_s), pattern = "disparity_z", replacement = "disp_z")  
names(all_index_s) <- gsub(x = names(all_index_s), pattern = "performance_z", replacement = "perf_z")  


# Calc composite index
issue_area_threshold_s <- 4 # place must have at least 3 issue perf/disp z-scores or it will be suppressed
indicator_threshold_s <- 13 # a place must have data for at least 13 indicators or it will be suppressed

city_index_screen <- calculate_city_index(all_index_s, issue_area_threshold_s, indicator_threshold_s)
city_index_screen <- city_index_screen %>% select(city_id, city_name, all_indicators_disp_count, all_indicators_perf_count, disp_values_count, perf_values_count,
                                              disparity_rank, performance_rank, quadrant, disparity_z, performance_z, crime_and_justice_disp_z, crime_and_justice_perf_z,
                                              democracy_disp_z, democracy_perf_z, economic_opportunity_disp_z, economic_opportunity_perf_z, education_disp_z, education_perf_z,
                                              healthy_built_environment_disp_z, healthy_built_environment_perf_z, health_care_access_disp_z, health_care_access_perf_z, 
                                              housing_disp_z, housing_perf_z) %>% arrange(-disparity_z)

# Export screened index to postgres ------------------------------------------------------
table_name <- "arei_composite_index_city_2023"
table_schema <- "v5"
table_comment_source <- "This is the SCREENED city index table including threshold for representation across all issue areas and indicators.
The UNSCREENED index is here: v5.arei_composite_index_city_2023_draft
R script used to calculate rates and import table: W://Project//RACE COUNTS//2023_v5//RC_Github/RaceCounts/IndexScripts//composite_index_city_2023.R 
QA document: W://Project/RACE COUNTS//2023_v5//Composite Index//Documentation//QA_sheet_Composite_Index_City.docx;" 

# make character vector for field types in postgresql db
charvect = rep('numeric', dim(city_index_screen)[2])

# change data type for columns
charvect[c(1:2)] <- "varchar" # Define which cols are character for the geoid and names etc

# add names to the character vector
names(charvect) <- colnames(city_index_screen)

# dbWriteTable(con, c(table_schema, table_name), city_index_screen, overwrite = FALSE, row.names = FALSE, field.types = charvect)

# write comment to table, and the first three fields that won't change.
table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

## send table comment to database
#dbSendQuery(conn = con, table_comment)     

