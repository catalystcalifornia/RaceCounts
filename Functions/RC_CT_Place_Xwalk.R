### Census Tract to Place (City) Crosswalk ### 

##install packages if not already installed ------------------------------
list.of.packages <- c("dplyr","data.table","sf","tigris","readr","tidyr","DBI","RPostgres","tidycensus","rvest","tidyverse","stringr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)
library(dplyr)
library(data.table)
library(tidycensus)
library(sf)
library(RPostgres)
library(stringr)
library(tidyr)
library(tigris)
options(scipen=999)

make_ct_place_xwalk <- function(curr_yr) {
  
###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con2 <- connect_to_db("rda_shared_data")

threshold <- .25 # 25% - this is our standard of pct of area for RC
table_name <- paste0("ct_place_",curr_yr) 
table_schema <- "crosswalks"              

# check if xwalk already exists in postgres
check_tables_sql <- paste0("SELECT * FROM information_schema.tables WHERE table_schema = '",
                           table_schema, "' AND table_name ='", table_name, "';")
check_tables <- dbGetQuery(con2, check_tables_sql)
View(check_tables)

if (nrow(check_tables)==1) {
  # the data already exists
  print("The crosswalk already exists - pulling from db")
  xwalk_sql <- paste0("SELECT * FROM ", table_schema, ".", table_name, ";")
  ct_place_xwalk <- dbGetQuery(con2, xwalk_sql)
  return(ct_place_xwalk)

} else if (nrow(check_tables)==0) {
  # the data does not exist in db
  print("The data is not in pg - creating xwalk now...")
  
  ### Create & Export Crosswalk ### ---------------------------------------------------------------------
  ## pull in curr_yr CBF Places and CTs ##
  places <- places(state = 'CA', year = curr_yr, cb = TRUE) %>% select(c(GEOID, starts_with("NAME"), geometry))
  tracts <- tracts(state = 'CA', year = curr_yr, cb = TRUE) %>% select(c(COUNTYFP, GEOID, STATEFP, geometry))
  
  ## spatial join ##
  places_3310 <- st_transform(places, 3310) # change projection to 3310
  tracts_3310 <- st_transform(tracts, 3310) # change projection to 3310
  # calculate area of tracts and places
  tracts_3310$area <- st_area(tracts_3310)
  places_3310$pl_area <- st_area(places_3310)
  # rename geoid fields
  tracts_3310 <- tracts_3310 %>% 
    rename("ct_geoid" = "GEOID", "county_geoid" = "COUNTYFP")
  places_3310 <- places_3310 %>% 
    rename("place_geoid" = "GEOID", "place_name" = "NAME")
  # run intersect
  print("Intersecting ct's and places...")
  tracts_places <- st_intersection(tracts_3310, places_3310) 
  # create ct_place combo geoid field
  tracts_places$ct_place_geoid <- paste(tracts_places$place_geoid, tracts_places$ct_geoid, sep = "_")
  # calculate area of intersect
  tracts_places$intersect_area <- st_area(tracts_places)
  # calculate percent of intersect out of total place area
  places_tracts <- tracts_places %>% mutate(prc_pl_area = as.numeric(tracts_places$intersect_area/tracts_places$pl_area))
  # calculate percent of intersect out of total tract area
  tracts_places$prc_area <- as.numeric(tracts_places$intersect_area/tracts_places$area)
  # convert to df
  tracts_places <- as.data.frame(tracts_places)
  places_tracts <- as.data.frame(places_tracts)
  
  xwalk <- full_join(places_tracts, select(tracts_places, c(ct_place_geoid, prc_area)), by = 'ct_place_geoid')
  
  # filter xwalk where intersect between tracts and places is equal or greater than X% of tract area OR place area.
  print(paste0("Filtering crosswalk using minimum intersection area threshold of ",threshold,"..."))
  xwalk_filter <- xwalk %>% filter(prc_area >= threshold | prc_pl_area >= threshold)
  names(xwalk_filter) <- tolower(names(xwalk_filter)) # make col names lowercase
  xwalk_filter <- select(xwalk_filter, ct_place_geoid, ct_geoid, place_geoid, county_geoid, place_name, starts_with("namelsad"), area, pl_area, intersect_area, prc_area, prc_pl_area)
  
  # export xwalk table
  table_comment_source <- paste0("Created with W:\\Project\\RACE COUNTS\\Functions\\RC_CT_Place_Xwalk.R and based on ",curr_yr," ACS TIGER non-CBF shapefiles.
    CTs with ",threshold," or more of their area within a city or that cover ",threshold," or more of a city''s area are assigned to those cities.
    As a result, a CT can be assigned to more than one city")
  
  # make character vector for field types in postgresql db
  charvect = rep('numeric', dim(xwalk_filter)[2])
  
  # change data type for first three columns
  charvect[1:7] <- "varchar" # first 7 are character for the geoid and names etc
  
  # add names to the character vector
  names(charvect) <- colnames(xwalk_filter)
  
 
  dbWriteTable(con2,
               Id(schema = table_schema, table = table_name),
               xwalk_filter, 
               overwrite = FALSE, row.names = FALSE,
               field.types = charvect)
  
  # Start a transaction
  dbBegin(con2)
  
  #comment on table and columns
  table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")
  print(comment)
  
  dbExecute(con2, table_comment)
  
  # Commit the transaction if everything succeeded
  dbCommit(con2)
  return("Table and columns comments added to table!")
  
  print("Pulling in the new crosswalk table from postgres...")
  ct_place_xwalk <- paste0("SELECT * FROM ", table_schema, ".", table_name, ";")
  
  dbDisconnect(con2)
  
  return(ct_place_xwalk)
  
} else {
  return("something is really wrong - duplicate pg tables are impossible")
}
}  
  

