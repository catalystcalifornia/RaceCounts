## City-School District Xwalk, Add in City, District, County Pop for v7 ##

#install packages if not already installed
packages <- c("readr", "dplyr","data.table","sf","tigris","readr","tidyr","DBI","RPostgres","tidycensus", "rvest", "tidyverse", "stringr", "jsonlite")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

#rvest # to scrape data table from cde website
#jsonlite # to scrape map layer ids from healthycity API
#tidyr # to reformat nested df of json response

options(scipen=999)


###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("racecounts")

# UPDATE EACH YEAR --------------------------------------------------------
cde_yr <- '2023_24' # cde enrollment data yr
filepath = "https://www3.cde.ca.gov/researchfiles/cadashboard/censusenrollratesdownload2024.txt"  # get url here: https://www.cde.ca.gov/ta/ac/cm/censenrolldatafiles.asp
fieldtype = 1:6 # specify which cols should be varchar, the rest will be assigned numeric. check layout here: https://www.cde.ca.gov/ta/ac/cm/censusenroll23.asp
acs_yr <- '2019-2023' # acs pop data vintage
curr_yr <- '2023' # census geography vintage
rc_yr <- '2025'
rc_schema <- 'v7'
qa_filepath <- 'W:\\Project\\RACE COUNTS\\2025_v7\\API\\QA_Sheet_arei_city_county_district.docx'

# may or may not need to be updated
threshold <- .30   # districts with > threshold % of area within a city are assigned to that city, cities with > threshold % of area within a district are assigned to that district


### Prep district_place_[year] Crosswalk ### ---------------------------------------------------------------------
## pull in CBF Places and Districts ##
places <- places(state = 'CA', year = curr_yr, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
unified <- school_districts(state = 'CA', type = "unified", year = curr_yr, cb = TRUE) %>% select(-c(STATEFP, UNSDLEA, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
elementary <- school_districts(state = 'CA', type = "elementary", year = curr_yr, cb = TRUE) %>% select(-c(STATEFP, ELSDLEA, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
secondary <- school_districts(state = 'CA', type = "secondary", year = curr_yr, cb = TRUE) %>% select(-c(STATEFP, SCSDLEA, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))

crosswalk <- function(x, y, threshold) {
  
            x_3310 <- st_transform(x, 3310) # change projection to 3310
            y_3310 <- st_transform(y, 3310) # change projection to 3310
            # calculate area of tracts and places
            x_3310$area <- st_area(x_3310)
            y_3310$y_area <- st_area(y_3310)
            # rename geoid fields
            x_3310 <- x_3310%>% 
              rename("x_geoid" = "GEOID", "x_name" = "NAME")
            y_3310 <- y_3310%>% 
              rename("y_geoid" = "GEOID", "y_name" = "NAME")
            # run intersect
            x_y <- st_intersection(x_3310, y_3310) 
            # create combo geoid field
            x_y$x_y_geoid <- paste(x_y$y_geoid, x_y$x_geoid, sep = "_")
            # calculate area of intersect
            x_y$intersect_area <- st_area(x_y)
            # calculate percent of intersect out of total place area
            y_x <- x_y %>% mutate(prc_y_area = as.numeric(x_y$intersect_area/x_y$y_area))
            # calculate percent of intersect out of total tract area
            x_y$prc_area <- as.numeric(x_y$intersect_area/x_y$area)
            # convert to df
            x_y <- as.data.frame(x_y)
            y_x <- as.data.frame(y_x)

            xwalk <- full_join(y_x, select(x_y, c(x_y_geoid, prc_area)), by = 'x_y_geoid')
            # filter xwalk where intersect between is equal or greater than Z% of x area OR y area. 
            xwalk_filter <- xwalk %>% filter(prc_area >= threshold | prc_y_area >= threshold)
            names(xwalk_filter) <- tolower(names(xwalk_filter)) # make col names lowercase
            xwalk_filter <- select(xwalk_filter, x_y_geoid, x_geoid, y_geoid, y_name, x_name, namelsad, area, y_area, intersect_area, prc_area, prc_y_area)

            colnames(xwalk_filter) <- gsub("x", "district", colnames(xwalk_filter))  # rename fields to specific geographies involved
            colnames(xwalk_filter) <- gsub("y", "place", colnames(xwalk_filter))  
            xwalk_filter <- data.frame(xwalk_filter)
            
}

# run xwalk function on all 3 district types
unified_places <- crosswalk(unified, places, threshold) %>% mutate(district_type = 'Unified')
elementary_places <- crosswalk(elementary, places, threshold) %>% mutate(district_type = 'Elementary')
secondary_places <- crosswalk(secondary, places, threshold) %>% mutate(district_type = 'Secondary')

xwalk_filter <- rbind(unified_places, elementary_places, secondary_places) %>% relocate(district_type, .before = area)

# pull in cds codes
cds <- dbGetQuery(con, paste0("SELECT cdscode, ncesdist AS district_geoid FROM education.cde_public_schools_", cde_yr, " WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'"))

xwalk_filter <- xwalk_filter %>% left_join(cds, by = "district_geoid") %>% relocate(cdscode, .before = place_geoid)
xwalk_filter <- xwalk_filter %>% mutate(area = as.numeric(area), place_area = as.numeric(place_area), intersect_area = as.numeric(intersect_area))

# export xwalk table 

table_name <- paste0("district_place_", curr_yr)
table_schema <- "crosswalks"
table_comment_source <- paste0("Created on ", Sys.Date(), " with W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\Functions\\arei_city_county_district_table.R and based on ", curr_yr, " ACS TIGER CBF shapefiles.
    Districts with ", threshold * 100, "% or more of their area within a city or that cover ", threshold * 100, "% or more of a city''s area are assigned to those cities.
    As a result, a district can be assigned to more than one city. QA doc: ", qa_filepath)

# make character vector for field types in postgresql db
charvect = rep('numeric', dim(xwalk_filter)[2])

# change data type for first three columns
charvect[1:8] <- "varchar" # Cols 1-7 are character for the geoid and names etc

# add names to the character vector
names(charvect) <- colnames(xwalk_filter)

# dbWriteTable(con, 
#              Id(schema = table_schema, table = table_name), 
#              xwalk_filter, overwrite = FALSE, row.names = FALSE,
#              field.types = charvect)

# write comment to table, and the first three fields that won't change.
table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

# send table comment to database
# dbBegin(con)
# dbExecute(con, table_comment)
# dbCommit(con)

# Prep arei_city_county_district_table for export -------------------------------
# Add counties, enrollment, pop, and region data
counties <- dbGetQuery(con, paste0("SELECT place_geoid, county_geoid, county_name FROM crosswalks.county_place_", curr_yr))
city_pop <- dbGetQuery(con2, paste0("SELECT geoid AS place_geoid, total_pop AS city_pop FROM ", rc_schema, ".arei_race_multigeo WHERE geolevel = 'place'"))
county_pop <- dbGetQuery(con2, paste0("SELECT geoid AS county_geoid, total_pop AS county_pop FROM ", rc_schema, ".arei_race_multigeo WHERE geolevel = 'county'"))
regions <- dbGetQuery(con2, paste0("SELECT county_id AS county_geoid, region FROM ", rc_schema, ".arei_county_region_urban_type"))


# get cde_yr census enr by district
dist_enr <- read_delim(file = filepath, delim = "\t", na = c("*", "")) %>% filter(rtype == 'D') %>% select(c(cds, totalenrollment)) %>% distinct()

# get ids from city map layer
city_map_geojson <- fromJSON("https://www.healthycity.org/maps/geojson/shape/12/?topojson&cached")
geometries <- city_map_geojson$objects$geo$geometries

city_map_ids <- geometries %>%
  select(id, properties) %>%
  unnest_wider(properties) %>%
  select(-shape_id) %>%
  dplyr::rename(city_map_id = id,
                city_name = name,
                city_geoid = geoid)
  
city_map_ids$city_map_id <- as.character(city_map_ids$city_map_id)
city_map_ids$city_name <- gsub(" County", "", city_map_ids$city_name)  # keep County for dupe city names, eg: El Sobrante

rc_table <- xwalk_filter %>% left_join(counties, by = "place_geoid") %>% mutate(county_name = gsub(" County", "", county_name))
rc_table <- rc_table %>% left_join(city_pop, by = "place_geoid")
rc_table <- rc_table %>% left_join(county_pop, by = "county_geoid")
rc_table <- rc_table %>% left_join(dist_enr, by = c("cdscode"="cds"))
rc_table <- rc_table %>% left_join(regions, by = "county_geoid")
rc_table <- rc_table %>% left_join(city_map_ids, by=c("place_geoid"="city_geoid"))
rc_table <- rc_table %>% 
  mutate(place_name = ifelse(!is.na(city_name), city_name, place_name)) %>%  # replace city name with name from city_map_ids (if not null)
  mutate(place_name = gsub(" CDP", "", place_name)) %>%  # drop "CDP" from place_name
  select(-city_name)  # drop city_map_ids name
  
rc_table <- rc_table %>% dplyr::rename(dist_id = district_geoid,
                                       city_id = place_geoid,
                                       city_name = place_name,
                                       county_id = county_geoid,
                                       total_enroll = totalenrollment      ) %>% 
              select(c(city_id, city_map_id, city_name, city_pop, dist_id, district_name, cdscode, total_enroll, district_type, county_id, county_name, region, county_pop))

# Export to postgres
table_name <- "arei_city_county_district_table"
table_comment_source <- paste0("Created on ", Sys.Date(), " with W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\RaceCounts\\Functions\\arei_city_county_district_table.R and based on ", curr_yr, " ACS TIGER CBF shapefiles and CDE data, ", acs_yr, " ACS pop data and ", cde_yr, " enrollment data.
    Districts with ", threshold * 100, "% or more of their area within a city or that cover ", threshold * 100, "% or more of a city''s area are assigned to those cities.
    As a result, a district can be assigned to more than one city. QA doc: ", qa_filepath)

# make character vector for field types in postgresql db
charvect = rep('numeric', dim(rc_table)[2])

# change data type for columns
charvect[c(1:3,5:7,9:12)] <- "varchar" # Define which cols are character for the geoid and names etc

# add names to the character vector
names(charvect) <- colnames(rc_table)

# dbWriteTable(con2,
#              Id(schema = rc_schema, table = table_name),
#              rc_table, overwrite = FALSE, row.names = FALSE,
#              field.types = charvect)

# write comment to table, and the first three fields that won't change.
table_comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

# send table comment to database
# dbBegin(con2)
# dbExecute(con2, table_comment)
# dbCommit(con2)


#dbDisconnect(con, con2)