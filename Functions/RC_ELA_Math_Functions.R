### 3rd Grade English Language Arts & Math Functions for RC ### 
######## These fx are used to prep the 3rd Grade ELA and Math RC tables

##install packages if not already installed ------------------------------
list.of.packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)
library(dplyr)
library(tidyr)
library(tidycensus)
library(sf)
library(tidyverse) # to scrape metadata table from cde website
library(usethis) # connect to github

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")



clean_ela_math <- function(x, y) {
  
  df_subset <- rename(x, rate = percentage_standard_met_and_above, pop = total_students_tested_with_scores, race = student_group_id)
  
  # Filter for 3rd grade, ELA test, race/ethnicity subgroups, county/state/district/ level 
  df_subset <- df_subset %>% filter(grade == "03" & test_id == y & race %in% c("001","074","075","076","077","078","079","080","144")
                                    & type_id %in% c("04", "05", "06", "07", "09", "10"))   
  
  ## calc raw/rate and screen ---------------------------------------------------------
  #calculate raw
  df_subset$raw = round(df_subset$pop * df_subset$rate / 100, 0)
  
  #pop screen
  df_subset <- df_subset %>% mutate(raw = ifelse(pop < threshold, NA, raw), rate = ifelse(pop < threshold, NA, rate))
  
  #select just fields we need
  df_subset <- df_subset %>% select(geoname, cdscode, geolevel, type_id, race, rate, raw, pop) 
  
  #rename race/eth categories
  df_subset$race <- gsub("001", "total", df_subset$race)
  df_subset$race <- gsub("074", "nh_black", df_subset$race)
  df_subset$race <- gsub("075", "nh_aian", df_subset$race)
  df_subset$race <- gsub("076", "nh_asian", df_subset$race)
  df_subset$race <- gsub("077", "nh_filipino", df_subset$race)
  df_subset$race <- gsub("078", "latino", df_subset$race)
  df_subset$race <- gsub("079", "nh_pacisl", df_subset$race)
  df_subset$race <- gsub("080", "nh_white", df_subset$race)
  df_subset$race <- gsub("144", "nh_twoormor", df_subset$race)
  
  #clean geoname columns
  df_subset$geoname <- gsub("State of ", "", df_subset$geoname)
  
  county_match <- filter(df_subset,type_id=="05") %>% right_join(counties,by='geoname') %>% mutate(district='')
  
  # get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
  schools <- paste0('cde_public_schools_',curr_yr) # Pull in public schools table matching CAASPP data year.
  districts <- dbGetQuery(con, paste0("SELECT cdscode, district, ncesdist AS geoid FROM education.",schools," WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'"))
  district_match <- filter(df_subset,type_id=="06") %>% right_join(districts,by='cdscode')
  
  matched <- union(county_match, district_match) %>% select(c(cdscode, geoid, district)) %>% distinct() # combine distinct county and district geoid matched df's
  df_final <- df_subset %>% full_join(matched, by='cdscode')
  df_final <- df_final %>% relocate(geoid) %>% mutate(geoname = ifelse(type_id == "04", "California", geoname), # add geoname and geoid for state
                                                      geoid = ifelse(type_id == "04", "06", geoid),
                                                      geoname = ifelse(type_id == "06", district, geoname),
                                                      geoid = ifelse(type_id %in% c("07","09","10"), cdscode, geoid)) %>% select(-c(district)) # sub district name into geoname for district rows
  
  # Check that it doesn't match any non-number
  df_final <- df_final %>% filter(str_detect(df_final$geoid, "^[:digit:]+$") == TRUE) %>% filter(!is.na(geoid)) # remove records without fips codes
  
  return(df_final)
  
}


# clean_leg_elamath <- function(x) {
#   # clean school data for leg dist
#   # x = caaspp dataframe, type = desired CDE type_id
#   leg_df <- rename(x, rate = percentage_standard_met_and_above, pop = total_students_tested_with_scores, race = student_group_id)
#   
#   # Filter for 3rd grade, ELA test, race/ethnicity subgroups, county/state/district/ level 
#   leg_df <- leg_df %>% filter(grade == "03" & race %in% c("001","074","075","076","077","078","079","080","144")
#                               & type_id == '07')   
#   
#   #calculate raw
#   leg_df <- leg_df %>% 
#     select(cdscode, geoname, type_id, school_name, test_id, race, pop, rate) %>%
#     group_by(cdscode, geoname, type_id, school_name, test_id, race) %>%
#     mutate(raw = round(pop * rate / 100, 0))
#   
#   #rename race/eth categories
#   leg_df$race <- gsub("001", "total", leg_df$race)
#   leg_df$race <- gsub("074", "nh_black", leg_df$race)
#   leg_df$race <- gsub("075", "nh_aian", leg_df$race)
#   leg_df$race <- gsub("076", "nh_asian", leg_df$race)
#   leg_df$race <- gsub("077", "nh_filipino", leg_df$race)
#   leg_df$race <- gsub("078", "latino", leg_df$race)
#   leg_df$race <- gsub("079", "nh_pacisl", leg_df$race)
#   leg_df$race <- gsub("080", "nh_white", leg_df$race)
#   leg_df$race <- gsub("144", "nh_twoormor", leg_df$race)
#   
#   # get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
#   #schools <- paste0('cde_public_schools_',curr_yr) # Pull in public schools table matching CAASPP data year.
#   #districts <- dbGetQuery(con, paste0("SELECT cdscode, district, ncesdist AS geoid FROM education.",schools," WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'"))
#   #district_match <- leg_df %>% right_join(districts,by='cdscode')
#   
#   #matched <- district_match %>% ungroup() %>% select(c(cdscode, geoid, district)) %>% distinct() # combine distinct county and district geoid matched df's
#   #leg_df <- leg_df %>% full_join(matched, by='cdscode')
#   leg_df <- leg_df %>% ungroup() %>% relocate(geoid) %>% #mutate(geoname = district) %>%
#     select(c(geoid, cdscode, geoname, school_name, test_id, race, pop, raw)) # drop unneeded cols
#   
#   return(leg_df)
# }



clean_leg_elamath <- function(x) { # Used for testing sch dist to leg dist aggregation
  # clean school dist data for leg dist
  # x = caaspp dataframe, type = desired CDE type_id
  leg_df <- rename(x, rate = percentage_standard_met_and_above, pop = total_students_tested_with_scores, race = student_group_id)
  
  # Filter for 3rd grade, ELA test, race/ethnicity subgroups, county/state/district/ level 
  leg_df <- leg_df %>% filter(grade == "03" & race %in% c("001","074","075","076","077","078","079","080","144")
                              & type_id == '06')   
  
  #calculate raw
  leg_df <- leg_df %>% 
    select(cdscode, geoname, type_id, district_code, district_name, test_id, race, pop, rate) %>%
    group_by(cdscode, geoname, type_id, district_code, district_name, test_id, race) %>%
    mutate(raw = round(pop * rate / 100, 0))

  #rename race/eth categories
  leg_df$race <- gsub("001", "total", leg_df$race)
  leg_df$race <- gsub("074", "nh_black", leg_df$race)
  leg_df$race <- gsub("075", "nh_aian", leg_df$race)
  leg_df$race <- gsub("076", "nh_asian", leg_df$race)
  leg_df$race <- gsub("077", "nh_filipino", leg_df$race)
  leg_df$race <- gsub("078", "latino", leg_df$race)
  leg_df$race <- gsub("079", "nh_pacisl", leg_df$race)
  leg_df$race <- gsub("080", "nh_white", leg_df$race)
  leg_df$race <- gsub("144", "nh_twoormor", leg_df$race)
  
  # get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
  schools <- paste0('cde_public_schools_',curr_yr) # Pull in public schools table matching CAASPP data year.
  districts <- dbGetQuery(con, paste0("SELECT cdscode, district, ncesdist AS geoid FROM education.",schools," WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'"))
  district_match <- leg_df %>% right_join(districts,by='cdscode')
  
  matched <- district_match %>% ungroup() %>% select(c(cdscode, geoid, district)) %>% distinct() # combine distinct county and district geoid matched df's
  leg_df <- leg_df %>% full_join(matched, by='cdscode')
  leg_df <- leg_df %>% ungroup() %>% relocate(geoid) %>% mutate(geoname = district) %>%
    select(c(geoid, cdscode, geoname, district_code, test_id, race, pop, raw)) # drop unneeded cols

return(leg_df)
}


calc_leg_elamath_sd <- function(d, xwalk, threshold) { # Used for testing sch dist to leg dist aggregation
  ##Calc weighted Leg Dist stats from school dist data
  
  calcs <- d %>% 
    left_join(xwalk, by = "geoid") %>%
    select(geoid, cdscode, leg_id, geolevel, afact, test_id, race, pop, raw) %>%
    mutate(across(where(is.numeric), ~.x * afact)) %>% #calc wt sch dist data
    select(-afact) %>%
    group_by(leg_id, geolevel, test_id, race) %>%  
    summarise(across(where(is.numeric), sum, na.rm = TRUE), .groups = "drop") %>%
    rename(geoid = leg_id) %>%
    mutate(rate = raw / pop * 100) %>%
    relocate(geolevel, .before = 3) %>%
    drop_na()
  
  ##Screen weighted Leg Dist data from school dist data
  calcs_screened <- calcs %>%
    mutate(raw = ifelse(pop < threshold, NA, raw),
           rate = ifelse(pop < threshold, NA, rate))
  
  calcs_wide <- calcs_screened %>%
    pivot_wider(
      names_from = race,
      names_glue = "{race}_{.value}",
      values_from = c(pop, raw, rate)
    )
  
return(calcs_wide)
}


calc_leg_elamath <- function(d, xwalk, threshold) {
  ##Calc Leg Dist stats from school data
  
  calcs <- d %>% 
    select(-geolevel) %>%
    left_join(xwalk, by = "geoid") %>%
    select(geoid, cdscode, leg_id, geolevel, ends_with("_pop"), ends_with("_raw")) #%>%
  
  pop_long <- calcs %>%
    select(leg_id, cdscode, geolevel, ends_with("_pop")) %>%
    pivot_longer(
      cols = ends_with("_pop"),
      names_to = "race",
      values_to = "pop") %>%
    mutate(race = gsub("_pop", "", race))
  
  raw_long <- calcs %>%
    select(leg_id, cdscode, geolevel, ends_with("_raw")) %>%
    pivot_longer(
      cols = ends_with("_raw"),
      names_to = "race",
      values_to = "raw") %>%
    mutate(race = gsub("_raw", "", race))
  
  calcs_long <- pop_long %>% 
    left_join(raw_long, by = c('cdscode', 'leg_id', 'race', 'geolevel')) %>%
    mutate(agg_pop = ifelse(!is.na(raw), pop, NA)) %>%  # exclude pop where raw is NA, so we only include pop in denominators where raw is not NA
    group_by(leg_id, geolevel, race) %>%  
    summarise(pop = sum(agg_pop, na.rm=TRUE),           # sum agg_pop to calc pop denominator
              raw = sum(raw, na.rm=TRUE)) %>%
    mutate(rate = raw / pop * 100) %>%
    rename(geoid = leg_id) %>%
    drop_na()
  
  ##Screen Leg Dist data from school data
  calcs_screened <- calcs_long %>%
    mutate(raw = ifelse(pop < threshold, NA, raw),
           rate = ifelse(pop < threshold, NA, rate))
  
  calcs_wide <- calcs_screened %>%
    pivot_wider(
      names_from = race,
      names_glue = "{race}_{.value}",
      values_from = c(pop, raw, rate)
    )
  
  return(calcs_wide)
}
