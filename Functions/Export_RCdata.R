# Function to create raw data exports for county or state RC data
## See example script where this fx is used: W:\Project\RACE COUNTS\2024_v6\RaceCounts\HPI_Ask\export_hpi_data.R


## BEFORE RUNNING THE FUNCTION: ##############

### User must create the table_list
#### Eg: rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_", curr_yr, "';")
####     rc_list <- dbGetQuery(con, rc_list_query)

### User must define geolevel 
#### Eg: geolevel <- 'county'


export_RCdata <- function(table_list, geolevel) {
  
  geolevel_list <- table_list %>%
    filter(grepl(paste0("^arei_.*_", geolevel, "_", curr_yr, "$"), table_name)) %>%
    arrange(table_name) %>% # alphabetize
    pull(table_name) # converts from df object to list; important for next steps using lapply
  
  # import all tables on geolevel_list
  geolevel_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", geolevel_list), geolevel_list), DBI::dbGetQuery, conn = con)
  
  # create column with indicator name
  geolevel_tables <- map2(geolevel_tables, names(geolevel_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name
  
  # create a long df of rate values from every arei_ table
  geolevel_tables_rate <- lapply(geolevel_tables, function(x) x %>% select(paste0(geolevel, "_id"), ends_with("_rate"), indicator))
  
  geolevel_tables_rate <- imap_dfr(geolevel_tables_rate, ~
                                     .x %>%
                                     pivot_longer(cols = ends_with("_rate"),
                                                  names_to = "race",
                                                  values_to = "rate")) %>% 
    mutate(race = (ifelse(race == 'rate', 'total', race)),
           race = gsub('_rate', '', race))
  
  
  # create a long df of race raw values from every arei_ table
  geolevel_tables_raw <- lapply(geolevel_tables, function(x) x %>% select(paste0(geolevel, "_id"), ends_with("_raw"), indicator))
  
  # remove tables with no raws
  geolevel_tables_raw <- geolevel_tables_raw[sapply(geolevel_tables_raw, function(x) "total_raw" %in% names(x))]
  
  #format
  geolevel_tables_raw <- imap_dfr(geolevel_tables_raw, ~ .x %>%
                                    pivot_longer(cols = ends_with("_raw"),
                                                 names_to = "race",
                                                 values_to = "raw")) %>% 
    mutate(race = (ifelse(race == 'raw', 'total', race)),
           race = gsub('_raw', '', race))
  
  
  # merge rate and raw long dfs
  df_merged_geolevel <- geolevel_tables_rate %>% 
    full_join(geolevel_tables_raw) %>%
    rename(geoid = paste0(geolevel, "_id"))
  
  
  # format
  df_geolevel <- df_merged_geolevel %>% 
    mutate(indicator = substring(indicator, 11),
           indicator = gsub(paste0('_', geolevel, '_', curr_yr), '', indicator),
           geo_level = geolevel) %>%
    left_join(arei_race_multigeo) %>%
    rename(geo_name = name)
  
  return(df_geolevel)
}

