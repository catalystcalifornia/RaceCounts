### 3rd Grade English Language Arts & Math Functions for RC ### 
######## These fx are used to prep the rda_shared_data tables for 3rd Grade ELA and Math RC calcs

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
                                    & type_id %in% c("04", "05", "06", "07"))   
  
  ## calc raw/rate and screen ---------------------------------------------------------
  #calculate raw
  df_subset$raw = round(df_subset$pop * df_subset$rate / 100, 0)
  
  #pop screen
  threshold = 20
  df_subset <- df_subset %>% mutate(raw = ifelse(pop < threshold, NA, raw), rate = ifelse(pop < threshold, NA, rate))
  
  #select just fields we need
  df_subset <- df_subset %>% select(geoname, cdscode, type_id, race, rate, raw, pop) 
  
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
  districts <- st_read(con, query = paste0("SELECT cdscode, district, ncesdist AS geoid FROM education.",schools," WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'"))
  district_match <- filter(df_subset,type_id=="06") %>% right_join(districts,by='cdscode')
  
  matched <- union(county_match, district_match) %>% select(c(cdscode, geoid, district)) %>% distinct() # combine distinct county and district geoid matched df's
  df_final <- df_subset %>% full_join(matched, by='cdscode')
  df_final <- df_final %>% relocate(geoid) %>% mutate(geoname = ifelse(type_id == "04", "California", geoname), # add geoname and geoid for state
                                                      geoid = ifelse(type_id == "04", "06", geoid),
                                                      geoname = ifelse(type_id == "06", district, geoname),
                                                      geoid = ifelse(type_id == "07", cdscode, geoid)) %>% select(-c(district)) # sub district name into geoname for district rows
  
  # Check that it doesn't match any non-number
  df_final <- df_final %>% filter(str_detect(df_final$geoid, "^[:digit:]+$") == TRUE) %>% filter(!is.na(geoid)) # remove records without fips codes
  
  return(df_final)
  
}