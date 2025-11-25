# Function to create raw data exports for leg dist, county or state RC data
## See example script where this fx is used: W:/Project/RACE COUNTS/2025_v7/RC_Github/LF/RaceCounts/Data/export_rc_data.R


## BEFORE RUNNING THE FUNCTION: ##############

### User must create the table_list
#### Eg: rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_", curr_yr, "';")
####     rc_list <- dbGetQuery(con, rc_list_query)

### User must define geotype 
#### Eg: geotype <- 'county'  (or 'state' or 'leg')


export_RCdata <- function(table_list, geotype) {
  
  geotype_list <- table_list %>%
    filter(grepl(paste0("^arei_.*_", geotype, "_", curr_yr, "$"), table_name)) %>%
    arrange(table_name) %>% # alphabetize
    pull(table_name) # converts from df object to list; important for next steps using lapply
  
  # import all tables on geotype_list
  geotype_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", geotype_list), geotype_list), DBI::dbGetQuery, conn = con)
  
  # create column with indicator name
  geotype_tables <- map2(geotype_tables, names(geotype_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name
  
  # create generic geoid col
  existing_col <- paste0(geotype, "_id")
  new_col <- 'geoid'
  geotype_tables <- lapply(geotype_tables, function(x) x %>%
                             mutate(!!sym(new_col) := !!sym(existing_col)))
  
  # Check if 'geolevel' column exists, and if not, create it with specific values
  geotype_tables <- lapply(geotype_tables, function(x) {
    # Check if 'geolevel' exists in the list element
    if (!"geolevel" %in% names(x)) {
      # If it doesn't exist, create it and assign NA values
      x$geolevel <- geotype
    }
    return(x)
  })
  
  # create a long df of rate values from every arei_ table
  geotype_tables_rate <- lapply(geotype_tables, function(x) x %>% select(ends_with("id"), geolevel, ends_with("_rate"), indicator))
  
  geotype_tables_rate <- imap_dfr(geotype_tables_rate, ~ .x %>%
                                    
                                    pivot_longer(cols = ends_with("_rate"),
                                                 names_to = "race",
                                                 values_to = "rate")) %>% 
    mutate(race = (ifelse(race == 'rate', 'total', race)),
           race = gsub('_rate', '', race))
  
  
  # create a long df of race raw values from every arei_ table
  geotype_tables_raw <- lapply(geotype_tables, function(x) x %>% select(ends_with("id"), geolevel, ends_with("_raw"), indicator))
  
  # remove tables with no raws
  geotype_tables_raw <- geotype_tables_raw[sapply(geotype_tables_raw, function(x) "total_raw" %in% names(x))]
  
  #format
  geotype_tables_raw <- imap_dfr(geotype_tables_raw, ~ .x %>%
                                   pivot_longer(cols = ends_with("_raw"),
                                                names_to = "race",
                                                values_to = "raw")) %>% 
    mutate(race = (ifelse(race == 'raw', 'total', race)),
           race = gsub('_raw', '', race))
  
  
  # merge rate and raw long dfs
  df_merged_geotype <- geotype_tables_rate %>% 
    left_join(geotype_tables_raw)
  
  
  
  # format
  df_geotype <- df_merged_geotype %>% 
    mutate(indicator = substring(indicator, 11),
           indicator = gsub(paste0('_', geotype, '_', curr_yr), '', indicator))
  
  # join geonames
  arei_race_geo <- filter(arei_race_multigeo, geolevel %in% df_geotype$geolevel)
  
  df_geotype <- df_geotype %>%
    left_join(arei_race_geo, by=c('geoid', 'geolevel')) %>%
    rename(geo_name = name) %>%
    select(ends_with("_id"), ends_with("name"), geolevel, indicator, race, rate, raw)
  
  return(df_geotype)
}
