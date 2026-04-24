# Export MOSAIC RC data and metadata
# Adapted from: ./RaceCounts/Data/export_rc_data.R

# Set up workspace ----------------------------------------------------------------
packages <- c("tidyverse", "RPostgres", "xfun", "usethis", "writexl", "openxlsx", "tidycensus") 

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(install_packages) > 0) {
  install.packages(install_packages)
} else {
  print("All required packages are already installed.")
}

for(pkg in packages){
  library(pkg, character.only = TRUE)
}

options(scipen=999) # disable scientific notation

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("mosaic")

# Update schema and year variables --------------------------------------------------
curr_schema <- 'v7' # update each year, this field populates most table and file names automatically
curr_yr <- '2025'   # update each year, this field populates most table and file names automatically
acs_yr <- '2024'    # update this year, this field used to pull ACS vars


# Fx to update race names ----------------------------------------------------
## Updates include:
#### In new_race - change str_detect for Asian, add str_to_title and gsub for "TRUE", added alone_incombo col
update_race <- function(x) {
  # x is the dataframe which must contain a column with standard RC postgres race names
  x <- x %>%
  # update race names
    mutate(new_race = case_when (
      str_detect(race, "api") ~ "Asian / Native Hawaiian / Pacific Islander",
      str_detect(race, "aian") ~ "American Indian / Alaska Native",
      str_detect(race, "^asian") ~ "Asian",
      str_detect(race, "nh_asian") ~ "Asian",
      str_detect(race, "black") ~ "Black",
      str_detect(race, "filipino") ~ "Filipinx",
      str_detect(race, "latino") ~ "Latinx",
      str_detect(race, "other_r") ~ "Another Race",
      str_detect(race, "pacisl") ~ "Native Hawaiian / Pacific Islander",
      race == "swana" ~ "Southwest Asian / North African",
      race == "swanasa" ~ "Southwest Asian / North African / South Asian",
      str_detect(race, "twoormor") ~ "Multiracial",
      str_detect(race, "white") ~ "White",
      str_detect(race, "total") ~ "Total",
      TRUE ~ str_to_title(gsub("_", " ", race))
    )
    ) %>%
    # add Latinx-inclusive/Non-Latinx col
    mutate(latinx_inclusive = case_when (
      str_detect(race, "nh_") ~ "Non-Latinx",
      str_detect(race, "total") ~ "Latinx-inclusive",
      TRUE ~ "Latinx-inclusive"
    )
    ) %>%
    # add One Race Alone/Alone or in Combo/In Combination col
    mutate(alone_incombo = case_when (
      str_detect(race, "aoic") ~ "Alone or in Combination",
      str_detect(race, "twoormor") ~ "In Combination",
      str_detect(race, "nh_") ~ "One Race Alone",
      TRUE ~ NA_character_
    )
    ) %>%
    # Refine alone_incombo col for indicators with aoic AND one race alone version of subgroups
    group_by(indicator) %>%  
    mutate(
      alone_incombo = case_when(
        !str_ends(race, "_aoic") & paste0(race, "_aoic") %in% race ~ "One Race Alone",
        is.na(alone_incombo) ~ "Alone or in Combination",  # for all indicators with NA, bc they are also aoic
        .default = alone_incombo
      )
    ) %>%
    ungroup() %>%
    
    relocate(new_race, .after = race) %>%
    relocate(latinx_inclusive, .after = new_race) %>%
    relocate(alone_incombo, .after = latinx_inclusive)
  
  
return(x)
}


# Load pop tables ----------------------------------------------------
asian_pop <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".aa_pop_b02018")) %>% filter(geolevel != 'place')
nhpi_pop <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".nhpi_pop_b02019")) %>% filter(geolevel != 'place')
  
acs_var <- load_variables(acs_yr, 'acs5')
pop_vars <- acs_var %>%
  filter(grepl(("B02018|B02019"), name)) %>%
  select(-geography) %>%
  mutate(name = paste0(tolower(name), "e")) %>%
  # add dupe rows to become percent metadata
  { bind_rows(
    mutate(., type = "orig"),
    mutate(., type = "dupe")
  )} %>%
  # update name/label for percent rows
  mutate(., name = ifelse(type == "dupe", gsub("e$", "_pct", name), name)) %>%
  mutate(., label = ifelse(type == "dupe", gsub("Estimate", "Percent", label), label)) %>%
  # update label for Total rows
  mutate(., label = ifelse(grepl("b02018_001", name), paste0(label, "!!Asian Alone or in Any Combination"), label)) %>%
  mutate(., label = ifelse(grepl("b02019_001", name), paste0(label, "!!Native Hawaiian and Other Pacific Islander Alone or in Any Combination"), label)) 

# Update pop dataframe col names
asian_pop_ <- asian_pop %>%
  rename_with(~deframe(pop_vars[, c("name", "label")])[.x], 
              .cols = intersect(pop_vars$name, names(.))) %>%
  select(-ends_with("m"))

nhpi_pop_ <- nhpi_pop %>%
  rename_with(~deframe(pop_vars[, c("name", "label")])[.x], 
              .cols = intersect(pop_vars$name, names(.))) %>%
  select(-ends_with("m"))

# Load indicator tables ----------------------------------------------------
rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_", curr_yr, "';")

rc_list <- dbGetQuery(con, rc_list_query)

# County & State Indicator Tables
## Function based on (.\\Functions\\Export_RCdata.R") 
#### Updates include: 
####### Remove 'arei' from geotype_list filter
####### Remove ends_with("id") and geolevel from geotype_tables_rate 'select'
####### Update geotype_tables_raw filter to keep only list elements with at least one _raw col
####### Updated to keep county_name/state_name cols and remove the join to add them back at the end
export_mosaicdata <- function(table_list, geotype) {
  
  geotype_list <- table_list %>%
    filter(grepl(paste0("^.*_", geotype, "_", curr_yr, "$"), table_name)) %>%
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
  
  # Rename county_name/state_name to geoname
  geotype_tables <- lapply(geotype_tables, function(x) {
    if ("county_name" %in% names(x)) {
      x <- rename(x, geoname = county_name)
    } else if ("state_name" %in% names(x)) {
      x <- rename(x, geoname = state_name)
    }
    return(x)
  })
  
  
  # create a long df of rate values from every arei_ table
  geotype_tables_rate <- lapply(geotype_tables, function(x) x %>% select(ends_with("name"), ends_with("id"), ends_with("_rate"), indicator, -ends_with("_id")))
  
  geotype_tables_rate <- imap_dfr(geotype_tables_rate, ~ .x %>%
                                    
                                    pivot_longer(cols = ends_with("_rate"),
                                                 names_to = "race",
                                                 values_to = "rate")) %>% 
    mutate(race = (ifelse(race == 'rate', 'total', race)),
           race = gsub('_rate', '', race))
  
  
  # create a long df of race raw values from every arei_ table
  geotype_tables_raw <- lapply(geotype_tables, function(x) x %>% select(ends_with("name"), ends_with("id"), ends_with("_raw"), indicator, -ends_with("_id")))
  
  # remove tables with no raws
  geotype_tables_raw <- Filter(function(df) any(grepl("_raw$", names(df))), geotype_tables_raw)
  
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
    mutate(indicator_old = indicator,
           indicator = substring(indicator, 11),
           indicator = gsub(paste0('_', geotype, '_', curr_yr), '', indicator),
           indicator = ifelse(startsWith(indicator, "_"), substring(indicator, 2), indicator))
  
  # delete after checking prev. step
  df_geotype <- df_geotype %>% select(-indicator_old) 
  
  # drop rows where values are NA
  df_geotype <- df_geotype %>% drop_na(rate)
  
  # drop rows where race is na, unknown, etc.
  df_geotype <- df_geotype %>% filter(!race %in% c('na', 'unknown'))
  
return(df_geotype)
}



# create county df for export
geolevel <- 'county'
df_county <- export_mosaicdata(rc_list, geolevel)

# create state df for export
geolevel <- 'state'
df_state <- export_mosaicdata(rc_list, geolevel)

# format race
df_county <- update_race(df_county) %>%
  select(-race) %>%
  rename(race = new_race)

df_state <- update_race(df_state) %>%
  select(-race) %>%
  rename(race = new_race)


# Pull in Indicator metadata table ------------------------------------------------------
metadata <- read.xlsx("W:\\Project\\RACE COUNTS\\2025_v7\\MOSAIC\\indicator_list.xlsx", sheet = "metadata")
 
metadata <- metadata %>%
  dplyr::rename("indicator" = "arei_indicator",
                "issue" = "arei_issue_area",
                "best value" = "arei_best",
                "indicator_short" = "api_name") %>%
  select(issue, indicator, everything()) %>%
  select(-c(type, raw_rounding, rate_rounding, arei_indicator_id, prefix, y_label, site_year, link2))


############### ADD IN POP PUMS (AND MAYBE REGULAR ACS POP) AS A TAB ################
# Put county data, state data and metadata into a list
list_data <- list("County" = df_county, "State" = df_state, "Asian Pop" = asian_pop_, "NHPI Pop" = nhpi_pop_,"Metadata" = metadata)

# Export to Excel
write_xlsx(list_data, ".\\MOSAIC\\mosaic_county_state_data.xlsx")


#close connection
dbDisconnect(con)


