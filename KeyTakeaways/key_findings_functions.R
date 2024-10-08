library(stringr)
library(dplyr)


clean_geo_names <- function(x){
  # clean place names
  x$geo_name <- str_remove(x$geo_name, ", California")
  x$geo_name <- str_remove(x$geo_name, " city")
  x$geo_name <- str_remove(x$geo_name, " CDP")
  x$geo_name <- str_remove(x$geo_name, " town")
  x$geo_name <- gsub(" County)", ")", x$geo_name)
  
  return(x)
}


api_split <- function(x) {
  # duplicate API (Asian Pacific Islander) rows, assigning one set race_generic Asian and the other set PacIsl
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


most_disp_by_race <- function(x, y) {
  # Function to prep raced most_disparate tables 
  
  # filter by race, pivot_wider, select the columns we want, get race long_name, reorder cols
  z <- x %>% 
    filter(race_generic == y) %>% 
    mutate(indicator = paste0(indicator, "_ind")) %>% 
    pivot_wider(names_from = indicator, values_from = disparity_z_score) %>% 
    group_by(geoid, geo_name) %>%  
    fill(ends_with("ind"), dist_id, district_name, total_enroll, .direction = 'updown')  %>% 
    filter(!duplicated(geo_name)) %>% 
    select(-race) %>% 
    rename(race = race_generic) %>% 
    select(geoid, geo_name, race, dist_id, district_name, total_enroll, ends_with("ind"), geo_level) %>% 
    inner_join(race_names, by = c('race' = 'race_generic')) %>% 
    select(geoid, geo_name, race,  dist_id, district_name, total_enroll, long_name, everything()) # add race long names
  
  # count non-null indicators by race/place
  indicator_count_ <- z %>% 
    ungroup() %>% 
    select(geoid, long_name, ends_with("_ind"), geo_level) %>% 
    mutate(indicator_count = rowSums(!is.na(select(., ends_with("_ind")))))
  
  # select columns to join back to z
  indicator_count <- indicator_count_ %>%
    select(geoid, long_name, indicator_count)
  
  z <- z %>%
    left_join(indicator_count, by=c("geoid", "long_name")) # add indicator counts by race/place to original df
  
  # reorder columns
  z <- z %>% 
    select(geoid, geo_name, dist_id, district_name, total_enroll, race, long_name, indicator_count, everything()) 
  
  # get list of _ind cols that apply to race
  indicator_col <- z %>% 
    ungroup() %>% 
    select(ends_with("_ind")) %>%
    names()
  
  # Get name of indicator with highest z-score
  z <- z %>%
    ungroup() %>%
    rowwise() %>%
    mutate(max_col = list(names(z[indicator_col])[which.max(c_across(indicator_col))])) 
  
  z$max_col <- gsub("_ind", "", z$max_col)
  
  ## merge with indicator
  # Note script using this function must have already created the indicator table
  z <- left_join(z, indicator, by = c("max_col"="indicator_short"))
  
  return(z)
}

