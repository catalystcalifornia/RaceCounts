#### arei_multigeo_list for RC v6 ####

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgreSQL","sf","usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

for(pkg in list.of.packages){ 
  library(pkg, character.only = TRUE) 
} 


# remove exponentiation
options(scipen = 100) 

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")


# Update each year --------------------------------------------------------
curr_schema <- 'v6'
prev_schema <- 'v5'
rc_yr <- '2024'


# pull in RC county_ids from previous schema, then race and region/urban type from current schema
county_ids <- st_read(con, query = paste0("select geoid, county_id from ", prev_schema, ".arei_multigeo_list where geolevel <> 'place'")) # get RC-specific county_id's
race <- st_read(con, query = paste0("select * from ", curr_schema, ".arei_race_multigeo where geolevel <> 'place'")) # import county & state records only
region_urban <- st_read(con, query = paste0("select county_id AS geoid, region, urban_type from ", curr_schema, ".arei_county_region_urban_type")) # get region, urban_type

## get RC county index tables ##
  # import county index tables
  table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name NOT LIKE '%_city_%' AND table_name LIKE '%index%';")
  rc_list <- dbGetQuery(con, table_list) %>% rename('table' = 'table_name')
  
  index_list <- rc_list[order(rc_list$table), ] # alphabetize list of index tables which transforms into character from list, needed to format list correctly for next steps
  index_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", index_list), index_list), DBI::dbGetQuery, conn = con) # import tables from postgres
  
  # format and clean tables
  index_tables_sort <- lapply(index_tables, function(i) i[order(i$county_id),]) # sort all list elements by county_id so they are all in the same order
  index_tables_clean <- lapply(index_tables_sort, function(x) x%>% select(county_id, ends_with(c("disparity_z", "disparity_rank", "performance_z", "performance_rank", "quartile", "quadrant"))))
  index_df <- as.data.frame(do.call(cbind, index_tables_clean))                     # convert list to df
  names(index_df) <- gsub(x = names(index_df), pattern = ".*\\.", replacement = "") # clean up column names
  index_df = index_df[,!duplicated(names(index_df))]                                # drop duplicated county_id columns

# join tables together
multigeo_list <- left_join(race, county_ids) %>% left_join(region_urban) %>% relocate(county_id, .after = geoid) %>% relocate(region, .after = name) %>% relocate(urban_type, .after = total_pop) %>%
                    dplyr::rename(geo_name = name)
multigeo_list <- left_join(multigeo_list, index_df, by = c("geoid" = "county_id"))


# City Data ---------------------------------------------------------------

# pull in city race and RC city_id tables from curr_schema
city_race <- st_read(con, query = paste0("select * from ", curr_schema, ".arei_race_multigeo where geolevel = 'place'")) # import city records only
city_ids <- st_read(con, query = paste0("select city_id AS geoid, region from ", curr_schema, ".arei_city_county_district_table")) %>% unique() # get unique city_ids, regions. postgres table has multiple listings per city depending on how many school dist it has.

## get RC city index table ##
# import city index table
city_index <- dbGetQuery(con, paste0("SELECT city_id, disparity_z, disparity_rank, performance_z, performance_rank FROM ", curr_schema, ".arei_composite_index_city_", rc_yr))

# join city tables together
city_multigeo_list <- left_join(city_race, city_ids) %>% rename(geo_name = name)
city_multigeo_list <- right_join(city_multigeo_list, city_index, by = c("geoid" = "city_id"))

# bind city table to county/state table
library(plyr)
final_multigeo_list <- rbind.fill(multigeo_list, city_multigeo_list) # use rbind.fill so cols missing in city table autofill with NA.
unloadNamespace("plyr") # unload plyr bc conflicts with dplyr used elsewhere

# clean geo_name column
clean_geo_names <- function(x){
  
  x$geo_name <- str_remove(x$geo_name, ", California")
  x$geo_name <- str_remove(x$geo_name, " city")
  x$geo_name <- str_remove(x$geo_name, " CDP")
  x$geo_name <- str_remove(x$geo_name, " town")
  x$geo_name <- gsub(" County", "", x$geo_name)
  
  return(x)
}

final_multigeo_list <- final_multigeo_list %>%
  clean_geo_names


# Export to Postgres ------------------------------------------------------

table_name <- "arei_multigeo_list"
table_comment_source <- paste0("Created ", Sys.Date(), ". Based on arei_race_multigeo, arei_county_region_urban_type, composite index and all issue area index tables for cities and counties. Feeds RC.org scatterplots and map. Source: W:\\Project\\RACE COUNTS\\", rc_yr, "_", curr_schema, "\\Composite Index\\arei_multigeo_list.R")
table_comment <- paste0("COMMENT ON TABLE ", curr_schema, ".", table_name, " IS '", table_comment_source, ".';")
column_comment <- paste0("COMMENT ON COLUMN ", curr_schema, ".", table_name, ".county_id IS 'This is the RACE COUNTS-specific county id, not county FIPS code.';")

# get list of multigeo col names
cols <- colnames(multigeo_list)
text_type <- grep("^geoid|^geo_name|^region|^urban_type|^geolevel|quartile$|quadrant$", cols) # specify which cols should be text (geoid, geo_name, region, urban_type, geolevel)
cols[text_type] # confirm correct cols are there
dblprecision_type <- grep("^pct|_z$", cols) # specify which cols should be double precision (pct_, _z)
cols[dblprecision_type] # confirm correct cols are there

charvect = rep('integer', dim(multigeo_list)[2]) 
charvect[text_type] <- "text" # specify which cols are text
charvect[dblprecision_type] <- "double precision" # specify which cols are double precision

# add names to the character vector
names(charvect) <- colnames(multigeo_list)
charvect # check col types before exporting table to database

# dbWriteTable(con, c(curr_schema, table_name), final_multigeo_list, overwrite = FALSE, row.names = FALSE, field.types = charvect)

# send table and column comments to database
# dbSendQuery(conn = con, table_comment)
# dbSendQuery(conn = con, column_comment)

# dbDisconnect(con)

