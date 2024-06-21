# Reference/run this script within the draft and final city index scripts #

######################### CITY INDEX CALCS ########################### ---------------------------
# Education Section -------------------------------------------------------
## These indicators require extra prep bc they are at school district not city level

## Pull in city (or district) level education indicators
education_list <- filter(rc_list, grepl(paste0("_district_",rc_yr),table)) # pull only district level indicator tables
education_list <- education_list [order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on education_list
education_tables_list <- lapply(setNames(paste0("select * from ", rc_schema, ".", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables_list <- map2(education_tables_list, names(education_tables_list), ~ mutate(.x, indicator = .y)) # create column with indicator name

education_tables_list <- lapply(education_tables_list, function(x) x %>% select(dist_id, district_name, disparity_z, performance_z, indicator))

education_tables <- imap_dfr(education_tables_list, ~
                               .x %>% 
                               pivot_longer(cols = disparity_z,
                                            names_to = "measure",
                                            values_to = "disparity_z_score_unweighted")) %>% select(-measure)  

education_tables <- education_tables %>% mutate(indicator = gsub('arei_|api_', '', indicator)) %>% mutate(indicator =  gsub(paste0('_district_',rc_yr), '', indicator)) %>%
  rename(performance_z_score_unweighted = performance_z) %>% relocate(indicator, .after = disparity_z_score_unweighted)

df_education_district <- education_tables %>% left_join(crosswalk, by = "dist_id") %>% filter(!is.na(city_id))

## Aggregate District data to Cities: weighted averages by enrollment size per indicator (some cities have multiple districts but not every indicator will have multiple districts per city).
education_tables_agg <- dist_data_to_city(df_education_district)


## Add the rest of the indicators  ------------------------------------------------------

# pull in list of tables in current racecounts schema

# filter for only city level indicator tables
city_list <- filter(rc_list, grepl(paste0("_city_",rc_yr),table))
city_list <- filter(city_list, !grepl("index", table)) # filter out index in case there is a prev version in postgres
city_list <- city_list[order(city_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step


# import all tables on city_list
city_tables <- lapply(setNames(paste0("select * from ", rc_schema, ".", city_list), city_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
city_tables <- map2(city_tables, names(city_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name


# rename column 
city_tables_short <- lapply(city_tables, function(x) x %>% select(city_id, city_name, disparity_z, performance_z, indicator))

city_tables_updated <-
                      lapply(names(city_tables_short), function(i){
                        x <- city_tables_short[[ i ]]
                        # set new name for 2nd column
                        names(x)[3] <- paste0(i, "_disp_z")
                        names(x)[4] <- paste0(i, "_perf_z")
                        # return
                        x
                      })

# only select columns we want
city_tables_updated <- lapply(city_tables_updated, function(x) x %>% select(city_id, ends_with("disp_z"), ends_with("perf_z")))
city_tables_updated <- city_tables_updated %>% reduce(full_join) # convert list to df
names(city_tables_updated)[-1] <- gsub(x = names(city_tables_updated)[-1], pattern = paste0("_city_",rc_yr), replacement = "", names) # clean colnames

# merge to get city names and education table
city_tables_df <- city_tables_updated %>% arrange(city_id) %>% distinct(city_id, .keep_all = TRUE) %>% left_join(education_tables_agg) %>% left_join(arei_race_multigeo) %>% select(city_id, city_name, everything())

# clean col names
names(city_tables_df) <- sub('^arei_|^api_', '', names(city_tables_df))

# remove cities that are actually universities/colleges
# remove cities with no city_name
city_tables_df <- city_tables_df %>% filter(!grepl('University', city_name)) %>% filter(!is.na(city_name)) %>% filter(!is.na(city_name))

# cap perf_z and disp_z values  at |3.5|. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
city_tables_capped <- clean_city_indicator_data_z(city_tables_df)

# Count the number of valid disp_z and perf_z per city. We will use this later
all_indicators_perf_count <- city_tables_df %>% select(city_id, ends_with("perf_z")) 
all_indicators_perf_count <- all_indicators_perf_count %>% mutate(all_indicators_perf_count = rowSums(!is.na(all_indicators_perf_count[,c(-1)]))) %>%
                                    select(city_id, all_indicators_perf_count)

all_indicators_disp_count <- city_tables_df %>% select(city_id, ends_with("disp_z"))
all_indicators_disp_count <- all_indicators_disp_count %>% mutate(all_indicators_disp_count = rowSums(!is.na(all_indicators_disp_count[,c(-1)]))) %>% 
                                    select(city_id, all_indicators_disp_count)

# Count number of indicators per issue area
indicators <- names(city_tables) %>% as.data.frame()
educ_indicators <- unique(education_tables$indicator) %>% as.data.frame()


# Calc UNSCREENED Index ---------------------------------------------------
# Count number of indicators per issue
crim_count <- length(grep("crim", indicators$.))
demo_count <- length(grep("demo", indicators$.))
econ_count <- length(grep("econ", indicators$.))
hben_count <- length(grep("hben", indicators$.))
hlth_count <- length(grep("hlth", indicators$.))
hous_count <- length(grep("hous", indicators$.))
educ_count <- length(grep("educ", educ_indicators$.))


# make separate data-frames for all issues
crim <- city_tables_capped %>% select(city_id, city_name, starts_with("crim")) %>% mutate(crim_count = crim_count) 
demo <- city_tables_capped %>% select(city_id, city_name, starts_with("demo")) %>% mutate(demo_count = demo_count) 
econ <- city_tables_capped %>% select(city_id, city_name, starts_with("econ")) %>% mutate(econ_count = econ_count) 
hben <- city_tables_capped %>% select(city_id, city_name, starts_with("hben")) %>% mutate(hben_count = hben_count) 
hlth <- city_tables_capped %>% select(city_id, city_name, starts_with("hlth")) %>% mutate(hlth_count = hlth_count) 
hous <- city_tables_capped %>% select(city_id, city_name, starts_with("hous")) %>% mutate(hous_count = hous_count) 
educ <- city_tables_capped %>% select(city_id, city_name, starts_with("educ")) %>% mutate(educ_count = educ_count) 


## Set issue thresholds
crim_threshold <- 1
demo_threshold <- 1
econ_threshold <- 2 # could be 3
educ_threshold <- 2
hlth_threshold <- 1
hben_threshold <- 2
hous_threshold <- 3


# Calc issue z-scores (avg of issue's indicator z-scores)
crim_index <- calculate_city_issue(crim, crim_count, crim_threshold) %>% rename_with(~ paste0('crime_and_justice', "_", .x), ends_with("z"))
demo_index <- calculate_city_issue(demo, demo_count, demo_threshold) %>% rename_with(~ paste0('democracy', "_", .x), ends_with("z"))
econ_index <- calculate_city_issue(econ, econ_count, econ_threshold) %>% rename_with(~ paste0('economic_opportunity', "_", .x), ends_with("z"))
hben_index <- calculate_city_issue(hben, hben_count, hben_threshold) %>% rename_with(~ paste0('healthy_built_environment', "_", .x), ends_with("z"))
hlth_index <- calculate_city_issue(hlth, hlth_count, hlth_threshold) %>% rename_with(~ paste0('health_care_access', "_", .x), ends_with("z"))
hous_index <- calculate_city_issue(hous, hous_count, hous_threshold) %>% rename_with(~ paste0('housing', "_", .x), ends_with("z"))
educ_index <- calculate_city_issue(educ, educ_count, educ_threshold) %>% rename_with(~ paste0('education', "_", .x), ends_with("z"))


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

city_index <- calculate_city_index(all_index, issue_area_threshold, indicator_threshold)

# add county, region data
region_pop <- dbGetQuery(con, "SELECT  city_id, county_id, county_name, region FROM v5.arei_city_county_district_table")
city_index <- city_index %>% left_join(region_pop, by = 'city_id')
city_index <- unique(city_index) %>% select(city_id, city_name, all_indicators_disp_count, all_indicators_perf_count, issue_disp_count, issue_perf_count,
                                    disparity_rank, performance_rank, quadrant, disparity_z, performance_z, crime_and_justice_disp_z, crime_and_justice_perf_z,
                                    democracy_disp_z, democracy_perf_z, economic_opportunity_disp_z, economic_opportunity_perf_z, education_disp_z, education_perf_z,
                                    healthy_built_environment_disp_z, healthy_built_environment_perf_z, health_care_access_disp_z, health_care_access_perf_z, 
                                    housing_disp_z, housing_perf_z, region, county_id, county_name, total_pop, disparity_z_quartile, performance_z_quartile) %>% arrange(-disparity_z)



