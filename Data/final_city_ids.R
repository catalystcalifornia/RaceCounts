## Create a csv of city names, ids, and map ids to upload to WordPress. This list creates place pages for the list of cities we included in our final index.

library(jsonlite)
library(here)

source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("racecounts")

# Use current working directory to get current rc_version automatically
rc_version <- sub(".*/(\\d+_v\\d+)/.*", "\\1", getwd())
current_yr <- strsplit(rc_version, "_")[[1]][1]
schema <- strsplit(rc_version, "_")[[1]][2]
## just in case, confirm current_yr is correct:

if (current_yr != format(Sys.time(), "%Y")) {
  message("! current_yr is an unexpected value, please confirm it is correct")
} else {
  city_map_geojson <- fromJSON("https://www.healthycity.org/maps/geojson/shape/12/?topojson&cached")
  
  final_cities <- dbGetQuery(conn, 
                             statement=paste0("select geoid as city_id, geo_name as city_name from ", 
                                              schema, ".arei_multigeo_list where geolevel='place';"))
  
  dbDisconnect(conn = conn)
  
  geometries <- city_map_geojson$objects$geo$geometries
  
  city_map_ids <- geometries %>%
    select(id, properties) %>%
    unnest_wider(properties) %>%
    select(id, geoid) %>%
    rename(api_id=geoid,
           map_id = id)
  
  city_map_ids$map_id <- as.character(city_map_ids$map_id)
  
  final_csv <- left_join(final_cities, city_map_ids, by = c("city_id"="api_id")) %>% 
    rename(api_id = city_id) %>%
    select(city_name, api_id, map_id)
  
  missing_map_shapes <- final_csv %>% filter(is.na(map_id)) %>% select(city_name) %>% pull()
  
  date_script_ran <- Sys.Date() %>%
    gsub("-", "", ., fixed=TRUE)
  
  file_name <- paste0("city_map_ids_", date_script_ran, ".csv")
  
  full_filepath <- paste0('W:\\Project\\RACE COUNTS\\', current_yr, '_', schema, '\\API\\', file_name)
  
  write.csv(final_csv, full_filepath, row.names=FALSE)
  
  message(paste("Success! :", file_name, "exported to", full_filepath))
  message(paste("Warning: The following cities do not have map shapes:", paste(missing_map_shapes, collapse = ", ")))
}



