#### arei_multigeo_list for RC v7 ####

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgres","sf", "usethis", "plyr", "dplyr")
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
curr_schema <- 'v7'
prev_schema <- 'v6'
rc_yr <- '2025'

qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Composite Index\\QA_arei_multigeo_list.docx"

# pull in RC county_ids from previous schema, then race and region/urban type from current schema
county_ids <- dbGetQuery(con, paste0("select geoid, county_id from ", prev_schema, ".arei_multigeo_list where geolevel <> 'place'")) # get RC-specific county_id's
race <- dbGetQuery(con, paste0("select * from ", curr_schema, ".arei_race_multigeo where geolevel IN ('county','state')")) # import county & state records only
region_urban <- dbGetQuery(con, paste0("select county_id AS geoid, region, urban_type from ", curr_schema, ".arei_county_region_urban_type")) # get region, urban_type

## get RC county index tables ##
  # import county index tables
  table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name NOT LIKE '%_city_%' AND table_name NOT LIKE '%_leg_%'AND table_name LIKE '%index%';")
  rc_list <- dbGetQuery(con, table_list) %>% dplyr::rename('table' = 'table_name')
  
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
city_race <- dbGetQuery(con, paste0("select * from ", curr_schema, ".arei_race_multigeo where geolevel = 'place'")) # import city records only
city_ids <- dbGetQuery(con, paste0("select city_id AS geoid, region from ", curr_schema, ".arei_city_county_district_table")) %>% unique() # get unique city_ids, regions. postgres table has multiple listings per city depending on how many school dist it has.

## get RC city index table ##
# import city index table
city_index <- dbGetQuery(con, paste0("SELECT city_id, disparity_z, disparity_rank, performance_z, performance_rank, disparity_z_quartile, performance_z_quartile, quadrant FROM ", curr_schema, ".arei_composite_index_city_", rc_yr))

# join city tables together
city_multigeo_list <- left_join(city_race, city_ids) %>% dplyr::rename(geo_name = name)
city_multigeo_list <- right_join(city_multigeo_list, city_index, by = c("geoid" = "city_id"))


# Legislative District Data ---------------------------------------------------------------
# pull in RC county_ids from previous schema, then race and region/urban type from current schema
con2 <- connect_to_db("rda_shared_data")

sldl_crosswalk <- dbGetQuery(con2, "SELECT * FROM crosswalks.state_assembly_2024_region_urban")
sldu_crosswalk <- dbGetQuery(con2, "SELECT * FROM crosswalks.state_senate_2024_region_urban")
leg_ids <- rbind(sldl_crosswalk, sldu_crosswalk) %>% select(-c(ct_total_pop, total_pop, pct_total_pop, rank))

race <- dbGetQuery(con, paste0("select * from ", curr_schema, ".arei_race_multigeo where geolevel IN ('sldl','sldu')")) # import county & state records only

## get RC leg index tables ##
# import leg index tables
leg_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_index_leg_%' AND table_name NOT LIKE '%_city_%'AND table_name NOT LIKE '%_county_%';")
rc_leg_list <- dbGetQuery(con, leg_list) %>% dplyr::rename('table' = 'table_name')

leg_list <- rc_leg_list[order(rc_leg_list$table), ] # alphabetize list of index tables which transforms into character from list, needed to format list correctly for next steps
leg_index_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", leg_list), leg_list), DBI::dbGetQuery, conn = con) # import tables from postgres

# format and clean tables
leg_index_tables_sort <- lapply(leg_index_tables, function(i) i[order(i$leg_id),]) # sort all list elements by leg_id so they are all in the same order
leg_index_tables_clean <- lapply(leg_index_tables_sort, function(x) x%>% select(leg_id, geolevel, ends_with(c("disparity_z", "disparity_rank", "performance_z", "performance_rank", "quartile", "quadrant"))))
leg_index_df <- as.data.frame(do.call(cbind, leg_index_tables_clean))                     # convert list to df
names(leg_index_df) <- gsub(x = names(leg_index_df), pattern = ".*\\.", replacement = "") # clean up column names
leg_index_df = leg_index_df[,!duplicated(names(leg_index_df))]                                # drop duplicated leg_id columns

# join tables together
leg_multigeo_list <- left_join(race, leg_ids, by=c("geoid"="leg_id", "geolevel")) %>% relocate(geolevel, .after = geoid) %>% relocate(region, .after = name) %>% 
  relocate(urban_type, .after = total_pop) %>%
  dplyr::rename(geo_name = name)
leg_multigeo_list <- left_join(leg_multigeo_list, leg_index_df, by = c("geoid" = "leg_id", "geolevel"))


# bind leg and city table to county/state table

final_multigeo_list <- rbind.fill(multigeo_list, city_multigeo_list, leg_multigeo_list) # use rbind.fill so cols missing in city table autofill with NA.
#unloadNamespace("plyr") # unload plyr bc conflicts with dplyr used elsewhere

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

final_leg_list <- final_multigeo_list %>% filter(geolevel=='sldl' | geolevel=='sldu' )
final_multigeo_list <- final_multigeo_list %>% filter(geolevel!='sldl' | geolevel!='sldu' )

# Export to Postgres ------------------------------------------------------

table_name <- "arei_multigeo_list"
table_comment_source <- paste0("Created ", Sys.Date(), ". Based on arei_race_multigeo, arei_county_region_urban_type, composite index and all issue area index tables for cities and counties. Feeds RC.org scatterplots and map. Source: W:\\Project\\RACE COUNTS\\", rc_yr, "_", curr_schema, "\\RC_Github\\LF\\RaceCounts\\IndexScripts\\arei_multigeo_list.R. QA doc: ", qa_filepath)
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

# dbWriteTable(con,
#               Id(schema = curr_schema, table = table_name), final_multigeo_list,
#               overwrite = FALSE, row.names = FALSE, field.types = charvect)
# 
# # send table and column comments to database
# # Start a transaction
# dbBegin(con)
# dbExecute(con, table_comment)
# dbExecute(con, column_comment)
# 
# # Commit the transaction if everything succeeded
# dbCommit(con)

# Export leg list to postgres

table_name <- "arei_leg_list"
table_comment_source <- paste0("Created ", Sys.Date(), ". Based on arei_race_multigeo, arei_county_region_urban_type, composite index and all issue area index tables for cities and counties. Feeds RC.org scatterplots and map. Source: W:\\Project\\RACE COUNTS\\", rc_yr, "_", curr_schema, "\\RC_Github\\LF\\RaceCounts\\IndexScripts\\arei_multigeo_list.R. QA doc: ", qa_filepath)
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

dbWriteTable(con,
             Id(schema = curr_schema, table = table_name), final_leg_list,
             overwrite = FALSE, row.names = FALSE, field.types = charvect)

# send table and column comments to database
# Start a transaction
dbBegin(con)
dbExecute(con, table_comment)
dbExecute(con, column_comment)

# Commit the transaction if everything succeeded
dbCommit(con)

dbDisconnect(con)

