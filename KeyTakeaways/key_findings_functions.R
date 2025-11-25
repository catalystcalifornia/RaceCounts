# Functions used to generate of Key Findings
library(stringr)
library(dplyr)

# Clean geo names by removing type of geo and state name from name fields
clean_geo_names <- function(x){
  # clean place names
  x$geo_name <- str_remove(x$geo_name, ", California")
  x$geo_name <- str_remove(x$geo_name, " city")
  x$geo_name <- str_remove(x$geo_name, " CDP")
  x$geo_name <- str_remove(x$geo_name, " town")
  x$geo_name <- gsub(" County)", ")", x$geo_name)
  
  return(x)
}

# Duplicate API (Asian Pacific Islander) rows, assigning one set race_generic Asian and the other set PacIsl
api_split <- function(x) {
  api_asian <- filter(x, race_generic == 'api') %>% 
    mutate(race_generic = 'asian')
  
  api_pacisl <- filter(x, race_generic == 'api') %>% 
    mutate(race_generic = 'pacisl')
  
  # remove api rows
  temp <- filter(x, race_generic != 'api') 
  
  # add back api rows as asian AND pacisl rows
  x <- bind_rows(temp, api_asian, api_pacisl)    
  
  return(x)
}

# Get and remove records where city name is actually a university
university_check <- function(x) {
  univ_count <- x %>%
    filter(grepl('University', geo_name)) %>%
    select(geo_name) %>%
    group_by(geo_name) %>% summarize(univ_count = n())
  View(univ_count)
  print("See univ_count for list of cities that are universities.")
  
  x <- x %>% 
    filter(!(geo_name %in% univ_count$geo_name))

  print("Data for cities that are actually universities have been removed.")
    
return(x)
}

# Function to prep raced most_disparate tables 
most_disp_by_race <- function(x, y) {
  
  # filter by race, pivot_wider, select the columns we want, get race long_name, reorder cols
  z <- x %>% 
    filter(race_generic == y) %>% 
    mutate(indicator = paste0(indicator, "_ind")) %>% 
    pivot_wider(names_from = indicator, values_from = disparity_z_score) %>% 
    group_by(geoid, geo_name, geo_level) %>%  
    fill(ends_with("ind"), dist_id, district_name, total_enroll, .direction = 'updown') %>% 
    filter(!duplicated(geo_name)) %>% 
    select(-race) %>% 
    rename(race = race_generic) %>% 
    select(geoid, geo_name, race, dist_id, district_name, total_enroll, ends_with("ind"), geo_level) %>% 
    inner_join(race_names, by = c('race' = 'race_generic')) %>%   # add race long names
    select(geoid, geo_name, geo_level, race, dist_id, district_name, total_enroll, long_name, everything())
  
  # count non-null indicators by race/place combo
  indicator_count_ <- z %>% 
    ungroup() %>% 
    select(geoid, geo_level, long_name, ends_with("_ind"), geo_level) %>% 
    mutate(indicator_count = rowSums(!is.na(select(., ends_with("_ind")))))
  
  # select columns to join back to z
  indicator_count <- indicator_count_ %>%
    select(geoid, geo_level, long_name, indicator_count)
  
  z <- z %>%
    left_join(indicator_count, by=c("geoid", "long_name", "geo_level")) # add indicator counts by race/place combo to original df
  
  # reorder columns
  z <- z %>% 
    select(geoid, geo_name, geo_level, dist_id, district_name, total_enroll, race, long_name, indicator_count, everything()) 
  
  # get list of _ind cols that apply to race
  indicator_col <- z %>% 
    ungroup() %>% 
    select(ends_with("_ind")) %>%
    names()
  
  # Get name of indicator with highest disp z-score
  z <- z %>%
    ungroup() %>%
    rowwise() %>%
    mutate(max_col = list(names(z[indicator_col])[which.max(c_across(indicator_col))])) 
  
  z$max_col <- gsub("_ind", "", z$max_col)
  
  ## merge with indicator
  # Note script using this function must have already created the indicator df
  z <- left_join(z, indicator, by = c("max_col"="indicator_short"))
  
  return(z)
}

