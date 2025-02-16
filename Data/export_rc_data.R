# Export RC data and metadata


# Set up workspace ----------------------------------------------------------------
packages <- c("tidyverse", "RPostgreSQL", "xfun", "usethis", "writexl") 

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
con <- connect_to_db("racecounts")

# Update schema and year variables --------------------------------------------------
curr_schema <- 'v6' # update each year, this field populates most table and file names automatically
curr_yr <- '2024'   # update each year, this field populates most table and file names automatically


# Load metadata tables and create RC table list ----------------------------------------------------
# pull in geo level ids with name. I don't do this directly in the data in case names differ and we have issues merging later
arei_race_multigeo_city <- dbGetQuery(con, paste0("SELECT geoid, name, geolevel FROM ", curr_schema, ".arei_race_multigeo")) %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

arei_race_multigeo <- dbGetQuery(con, paste0("SELECT geoid, name, geolevel FROM ", curr_schema, ".arei_race_multigeo")) %>% select(-geolevel) # used for county and state

# pull in crosswalk to go from district to city
crosswalk <- dbGetQuery(con, paste0("SELECT city_id, dist_id, total_enroll FROM ", curr_schema, ".arei_city_county_district_table"))

rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_", curr_yr, "';")

rc_list <- dbGetQuery(con, rc_list_query)



# City Data Tables --------------------------------------------------------------------

# filter for final ("api_") city tables, excluding index tables, and alphabetize table list
# note: city education tables are handled separately (district tables)
city_list <- rc_list %>%
  filter(grepl(paste0("^api_.*_city_", curr_yr, "$"), table_name)) %>%
  arrange(table_name) %>% # alphabetize
  pull(table_name) # converts from df object to list; important for next steps using lapply

# import all tables on city_list
city_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", city_list), city_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
city_tables <- map2(city_tables, names(city_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

# create a long df of _rate values

city_tables_rate <- lapply(city_tables, function(x) x %>% select(city_id, ends_with("_rate"), indicator))

city_tables_rate <- imap_dfr(city_tables_rate, ~ .x %>%
                        pivot_longer(cols = ends_with("_rate"),
                                     names_to = "race",
                                     values_to = "rate")) %>% 
  mutate(race = (ifelse(race == 'rate', 'total', race)),
         race = gsub('_rate', '', race)) 


# create a long df of _raw values

city_tables_raw <- lapply(city_tables, function(x) x %>% select(city_id, ends_with("_raw"), indicator))


# remove tables with no raws
# incarceration, census participation, per capita income, drinking water, hazards, greenspace, toxic releases, eviction, foreclosure, housing quality
city_tables_raw <- city_tables_raw[sapply(city_tables_raw, function(x) "total_raw" %in% names(x))]

city_tables_raw <- imap_dfr(city_tables_raw, ~ .x %>%
                      pivot_longer(cols = ends_with("_raw"),
                                   names_to = "race",
                                   values_to = "raw")) %>% 
  mutate(race = (ifelse(race == 'raw', 'total', race)),
         race = gsub('_raw', '', race)) 



# merge city_tables_rate, city_tables_raw
df_merged <- city_tables_rate %>% 
  full_join(city_tables_raw)


# format indicator and add city name through join
df_city <- df_merged %>% 
  mutate(
    indicator = substring(indicator, 10),
    indicator = gsub(paste0("_city_",curr_yr), '', indicator)
    ) %>%
  left_join(arei_race_multigeo_city) %>%
  select(city_id, city_name, everything())



# City (District) Education Tables: must be handled separately bc they are school district not city-level ----------------------------------------
education_list <- rc_list %>%
  filter(grepl(paste0("^api_.*_district_", curr_yr, "$"), table_name)) %>%
  arrange(table_name) %>% # alphabetize
  pull(table_name) # converts from df object to list; important for next steps using lapply

# import all tables on education_list
education_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables <- map2(education_tables, names(education_tables), ~ mutate(.x, indicator = .y))

# create a long df of _rate values
education_tables_rate <- lapply(education_tables, function(x) x %>% select(dist_id, district_name, ends_with("_rate"), indicator))

education_tables_rate <- imap_dfr(education_tables_rate, ~ .x %>%
                                  pivot_longer(cols = ends_with("_rate"),
                                               names_to = "race",
                                               values_to = "rate")) %>% 
  mutate(race = (ifelse(race == 'rate', 'total', race)),
         race = gsub('_rate', '', race),
         district_name = str_trim(district_name, "right")) 


# create a long df of _raw values
education_tables_raw <- lapply(education_tables, function(x) x %>% select(dist_id, district_name, ends_with("_raw"), indicator))

education_tables_raw <- imap_dfr(education_tables_raw, ~ .x %>%
                          pivot_longer(cols = ends_with("_raw"),
                                       names_to = "race",
                                       values_to = "raw")) %>% 
  mutate(race = (ifelse(race == 'raw', 'total', race)),
         race = gsub('_raw', '', race),
         district_name = str_trim(district_name, "right")) 


# merge education_tables_rate, education_tables_raw
df_merged_education <- education_tables_rate %>% 
  full_join(education_tables_raw)


# format indicator
df_education_district <- df_merged_education %>% 
  mutate(indicator = substring(indicator, 10),
         indicator = gsub(paste0('_district_', curr_yr), '', indicator))



# get curr_schema.arei_indicator_list_city

metadata <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".arei_indicator_list_city"))

metadata <- metadata %>% select(-c(arei_indicator, arei_city_view,  arei_issue_area_id, arei_indicator_id, race_type, ind_order, oid, site_year, data_year, raw_rounding, rate_rounding, ind_show_on_dev, ind_show_on_site))

metadata <- metadata %>% rename("indicator" = "api_name",
                                "issue" = "arei_issue_area",
                                "best" = "arei_best") %>%
  select(indicator, everything())


write_xlsx(df_city, paste0("W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\Data\\rc_city_data.xlsx"))
write_xlsx(df_education_district, paste0("W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\Data\\rc_school_district_data.xlsx"))
write_xlsx(metadata, paste0("W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\Data\\rc_city_school_district_metadata.xlsx"))


# County & State Data Tables ------------------------------------------------------
source("W:\\Project\\RACE COUNTS\\Functions\\Export_RCdata.R")

# create county df for export
geolevel <- 'county'
df_county <- export_RCdata(rc_list, geolevel)

# export county data Excel file
write_xlsx(df_county, paste0("W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\Data\\rc_county_data.xlsx"))

# create state df for export
geolevel <- 'state'
df_state <- export_RCdata(rc_list, geolevel)

# export state data Excel file
write_xlsx(df_state, paste0("W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\Data\\rc_state_data.xlsx"))


# Indicator metadata table from curr_schema.arei_indicator_list_cntyst ------------------------------------------------------

metadata <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".arei_indicator_list_cntyst"))

metadata <- metadata %>% select(-c(arei_indicator,  arei_issue_area_id, arei_indicator_id, race_type, ind_order, site_year, data_year, raw_rounding, rate_rounding, ind_show_on_dev, ind_show_on_site, newoid))

metadata <- metadata %>% rename("indicator" = "api_name",
                                "issue" = "arei_issue_area",
                                "best" = "arei_best") %>%
  select(indicator, everything())

# export metadata Excel file
write_xlsx(metadata, paste0("W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\Data\\rc_county_state_metadata.xlsx"))


#close connection
dbDisconnect(con)


