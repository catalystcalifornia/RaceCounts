#### SCREENED Composite Index (z-score) for RC v5 ####
#### Same script as W:\Project\RACE COUNTS\2023_v5\Composite Index\composite_index_city_2023_draft.R EXCEPT this script uses "arei_" tables and exports the unscreened index table

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

# pull in city_id and city_names
arei_race_multigeo <- dbGetQuery(con, "SELECT geoid, name, geolevel, total_pop FROM v5.arei_race_multigeo") %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

# pull in list of all tables in current racecounts schema
curr_schema <- "v5"  # update each year
table_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = curr_schema))$table, function(x) slot(x, 'name'))))

######################### get SCREENED indicator table names ######################### ------------------------------------
### Filter so we can pull z-scores from the "api_" city/dist indicator tables that contain calcs only for the cities that pass the total_pop threshold screen.
rc_list <- filter(table_list, grepl("api_",table))



######################### CITY INDEX CALCS ########################### ---------------------------
# Education Section -------------------------------------------------------
## These indicators require extra prep bc they are at school district not city level

## Pull in city (or district) level education indicators
education_list <- filter(rc_list, grepl("_district_2023",table)) # pull only district level indicator tables
education_list <- education_list[order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on education_list
education_tables_list<- lapply(setNames(paste0("select * from v5.", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables_list <- map2(education_tables_list, names(education_tables_list), ~ mutate(.x, indicator = .y)) # create column with indicator name

education_tables_list<- lapply(education_tables_list, function(x) x %>% select(dist_id, district_name, disparity_z, performance_z, indicator))

education_tables<- imap_dfr(education_tables_list, ~
                               .x %>% 
                               pivot_longer(cols = disparity_z,
                                            names_to = "measure",
                                            values_to = "disparity_z_score_unweighted")) %>% select(-measure)  

education_tables<- education_tables%>% mutate(indicator =  gsub('api_', '', indicator)) %>% mutate(indicator =  gsub('_district_2023', '', indicator)) %>%
  rename(performance_z_score_unweighted = performance_z) %>% relocate(indicator, .after = disparity_z_score_unweighted)

df_education_district<- education_tables%>% left_join(crosswalk, by = "dist_id") %>% filter(!is.na(city_id))

## Aggregate District data to Cities: weighted averages by enrollment size per indicator (some cities have multiple districts but not every indicator will have multiple districts per city).
education_tables_agg<- dist_data_to_city(df_education_district)


## Add the rest of the indicators  ------------------------------------------------------

# pull in list of tables in racecounts.v5

# filter for only city level indicator tables
city_list<- filter(rc_list, grepl("_city_2023",table)) # pull only city level indicator tables
city_list<- filter(city_list, !grepl("index", table)) # filter out index in case there is a prev version in postgres
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
city_tables_updated <- lapply(city_tables_updated, function(x) x %>% select(city_id, ends_with("disp_z"), ends_with("perf_z")))
city_tables_updated <- city_tables_updated %>% reduce(full_join) # convert list to df
names(city_tables_updated)[-1] <- substring(names(city_tables_updated)[-1],5) # clean colnames
names(city_tables_updated)[-1] <- gsub(x = names(city_tables_updated)[-1], pattern = "city_2023_", replacement = "", names) # clean colnames

# merge to get city names and education table
city_tables_df <- city_tables_updated %>% arrange(city_id) %>% distinct(city_id, .keep_all = TRUE) %>% left_join(education_tables_agg) %>% left_join(arei_race_multigeo) %>% select(city_id, city_name, everything())

# remove cities that are actually universities/colleges: RC v5 there are 6 of them
# remove cities with no city_name: RC v5 there are 6 of them
city_tables_df <- city_tables_df %>% filter(!grepl('University', city_name)) %>% filter(!is.na(city_name)) %>% filter(!is.na(city_name))

# cap perf_z and disp_z values  at |3.5|. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
city_tables_capped <- clean_city_indicator_data_z(city_tables_df)

# Count the number of valid disp_z and perf_z per city. We will need this later
all_indicators_perf_count <- city_tables_df %>% select(city_id, ends_with("perf_z")) %>%
  mutate(all_indicators_perf_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_perf_count)

all_indicators_disp_count <- city_tables_df %>% select(city_id, ends_with("disp_z")) %>%
  mutate(all_indicators_disp_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_disp_count)

# Count number of indicators per issue area
indicators <- names(city_tables) %>% as.data.frame()
educ_indicators <- unique(education_tables$indicator) %>% as.data.frame()

# Calc SCREENED Index ---------------------------------------------------
crim_count <- length(grep("crim", indicators$.))
demo_count<- length(grep("demo", indicators$.))
econ_count<- length(grep("econ", indicators$.))
hben_count<- length(grep("hben", indicators$.))
hlth_count<- length(grep("hlth", indicators$.))
hous_count<- length(grep("hous", indicators$.))
educ_count<- length(grep("educ", educ_indicators$.))


# make separate dataframes for all
crim<- city_tables_capped %>% select(city_id, city_name, starts_with("crim")) %>% mutate(crim_count = crim_count) 
demo<- city_tables_capped %>% select(city_id, city_name, starts_with("demo")) %>% mutate(demo_count = demo_count) 
econ<- city_tables_capped %>% select(city_id, city_name, starts_with("econ")) %>% mutate(econ_count = econ_count) 
hben<- city_tables_capped %>% select(city_id, city_name, starts_with("hben")) %>% mutate(hben_count = hben_count) 
hlth<- city_tables_capped %>% select(city_id, city_name, starts_with("hlth")) %>% mutate(hlth_count = hlth_count) 
hous<- city_tables_capped %>% select(city_id, city_name, starts_with("hous")) %>% mutate(hous_count = hous_count) 
educ<- city_tables_capped %>% select(city_id, city_name, starts_with("educ")) %>% mutate(educ_count = educ_count) 


## issue area threshold
crim_threshold <- 1
demo_threshold <- 1
econ_threshold <- 2 # could be 3
educ_threshold <- 2
hlth_threshold <- 1
hben_threshold <- 2
hous_threshold <- 3


crim_index <-  calculate_city_issue(crim, crim_count, crim_threshold) %>% rename_with(~ paste0('crime_and_justice', "_", .x), ends_with("z"))
demo_index <-  calculate_city_issue(demo, demo_count, demo_threshold) %>% rename_with(~ paste0('democracy', "_", .x), ends_with("z"))
econ_index <-  calculate_city_issue(econ, econ_count, econ_threshold) %>% rename_with(~ paste0('economic_opportunity', "_", .x), ends_with("z"))
hben_index <-  calculate_city_issue(hben, hben_count, hben_threshold) %>% rename_with(~ paste0('healthy_built_environment', "_", .x), ends_with("z"))
hlth_index <-  calculate_city_issue(hlth, hlth_count, hlth_threshold) %>% rename_with(~ paste0('health_care_access', "_", .x), ends_with("z"))
hous_index <-  calculate_city_issue(hous, hous_count, hous_threshold) %>% rename_with(~ paste0('housing', "_", .x), ends_with("z"))
educ_index <-  calculate_city_issue(educ, educ_count, educ_threshold) %>% rename_with(~ paste0('education', "_", .x), ends_with("z"))


# merge all issue tables together
  all_index <- crim_index %>% select(city_id, ends_with("z"), ends_with("z")) %>% left_join(
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
    city_id, city_name, ends_with("count"), ends_with("disparity_z"), ends_with("performance_z")
  )



# Calc composite index
issue_area_threshold <- 4 # place must have at least 3 issue perf/disp z-scores or it will be suppressed
indicator_threshold <- 13 # a place must have data for at least 13 indicators or it will be suppressed

city_index <- calculate_city_index(all_index, issue_area_threshold, indicator_threshold)
city_index <- city_index %>% select(city_id, city_name, all_indicators_disp_count, all_indicators_perf_count, issue_disp_count, issue_perf_count,
                                    disparity_rank, performance_rank, quadrant, disparity_z, performance_z, crime_and_justice_disp_z, crime_and_justice_perf_z,
                                    democracy_disp_z, democracy_perf_z, economic_opportunity_disp_z, economic_opportunity_perf_z, education_disp_z, education_perf_z,
                                    healthy_built_environment_disp_z, healthy_built_environment_perf_z, health_care_access_disp_z, health_care_access_perf_z, 
                                    housing_disp_z, housing_perf_z) %>% arrange(-disparity_z)








# Export SCREENED index to postgres ------------------------------------------------------
table_name <- "arei_composite_index_city_2023"
table_schema <- "v5"
table_comment_source <- "This is the SCREENED city index table including pop screen and threshold for representation across all issue areas and indicators.
The UNSCREENED index is: v5.arei_composite_index_city_2023_draft
R script used to calculate rates and import table: W://Project//RACE COUNTS//2023_v5//RC_Github/RaceCounts/IndexScripts//composite_index_city_2023.R 
QA document: W://Project/RACE COUNTS//2023_v5//Composite Index//Documentation//QA_sheet_Composite_Index_City.docx" 

# send city index and comment to postgres
#city_index_to_postgres(city_index)


dbDisconnect(con)  
