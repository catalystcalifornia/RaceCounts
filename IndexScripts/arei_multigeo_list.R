#### arei_multigeo_list for RC v6 ####

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgreSQL","sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)


# Packages ----------------------------------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(sf)

# remove exponentiation
options(scipen = 100) 

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# pull in county race and RC county_id tables from racecounts db - Chris updated county_ids to come from v5
race <- st_read(con, query = "select * from v5.arei_race_multigeo where geolevel <> 'place'") # import county & state records only
county_ids <- st_read(con, query = "select geoid, county_id, region, urban_type from v5.arei_multigeo_list") # get RC-specific county_id's, region, urban_type

## get RC county index tables ##
  # import county index tables - Chris updated rc_list schema to v6
  rc_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v6"))$table, function(x) slot(x, 'name')))) # create list of tables in racecounts.v6
  index_list <- filter(rc_list, grepl("_index_2023",table)) # filter for only index tables
  index_list <- index_list[order(index_list$table), ] # alphabetize list of index tables which transforms into character from list, needed to format list correctly for next steps
  index_tables <- lapply(setNames(paste0("select * from v5.", index_list), index_list), DBI::dbGetQuery, conn = con) # import tables from postgres
  
  # format and clean tables
  index_tables_sort <- lapply(index_tables, function(i) i[order(i$county_id),]) # sort all list elements by county_id so they are all in the same order
  index_tables_clean <- lapply(index_tables_sort, function(x) x%>% select(county_id, ends_with(c("disparity_z", "disparity_rank", "performance_z", "performance_rank"))))
  index_df <- as.data.frame(do.call(cbind, index_tables_clean))                     # convert list to df
  names(index_df) <- gsub(x = names(index_df), pattern = ".*\\.", replacement = "") # clean up column names
  index_df = index_df[,!duplicated(names(index_df))]                                # drop duplicated county_id columns

# join tables together
multigeo_list <- left_join(race, county_ids) %>% relocate(county_id, .after = geoid) %>% relocate(region, .after = name) %>% relocate(urban_type, .after = total_pop) %>%
                    dplyr::rename(geo_name = name)
multigeo_list <- left_join(multigeo_list, index_df, by = c("geoid" = "county_id"))


# City Data ---------------------------------------------------------------

# pull in city race and RC city_id tables from racecounts db - chris updated city_race to pull from v6
city_race <- st_read(con, query = "select * from v6.arei_race_multigeo where geolevel = 'place'") # import city records only
city_ids <- st_read(con, query = "select city_id AS geoid, region from v5.arei_city_county_district_table") %>% unique() # get unique city_ids, regions. postgres table has multiple listings per city depending on how many school dist it has.

## get RC city index table ##
# import city index table - Chris updated to pull from v6 city index
city_index <- dbGetQuery(con, "SELECT city_id, disparity_z, disparity_rank, performance_z, performance_rank FROM v6.arei_composite_index_city_2023")

# join city tables together
city_multigeo_list <- left_join(city_race, city_ids) %>% rename(geo_name = name)
city_multigeo_list <- right_join(city_multigeo_list, city_index, by = c("geoid" = "city_id"))

# bind city table to county/state table
library(plyr)
final_multigeo_list <- rbind.fill(multigeo_list, city_multigeo_list) # use rbind.fill so cols missing in city table autofill with NA.
unloadNamespace("plyr") # unload plyr bc conflicts with dplyr used elsewhere


# Export to Postgres ------------------------------------------------------

table_schema <- "v6" #chris updated to v6
table_name <- "arei_multigeo_list"
table_comment_source <- "Based on arei_race_multigeo, composite index and all issue area index tables for cities and counties. Feeds RC.org scatterplots and map. Source: W:\\Project\\RACE COUNTS\\2023_v5\\Composite Index\\arei_multigeo_list.R"

# get list of multigeo col names
cols <- colnames(multigeo_list)
text_type <- grep("^geoid|^geo_name|^region|^urban_type|^geolevel", cols) # specify which cols should be text (geoid, geo_name, region, urban_type, geolevel)
cols[text_type] # confirm correct cols are there
dblprecision_type <- grep("^pct|_z$", cols) # specify which cols should be double precision (pct_, _z)
cols[dblprecision_type] # confirm correct cols are there

charvect = rep('integer', dim(multigeo_list)[2]) 
charvect[text_type] <- "text" # specify which cols are text
charvect[dblprecision_type] <- "double precision" # specify which cols are double precision

# add names to the character vector
names(charvect) <- colnames(multigeo_list)

# dbWriteTable(con, c(table_schema, table_name), final_multigeo_list, overwrite = FALSE, row.names = FALSE, field.types = charvect)

table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".';")

# send table comment to database
# dbSendQuery(conn = con, table_comment)

#dbDisconnect()







