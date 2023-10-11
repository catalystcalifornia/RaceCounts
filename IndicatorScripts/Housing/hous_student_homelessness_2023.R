## Student Homelessness for RC v5 ##

#install packages if not already installed
list.of.packages <- c("openxlsx","tidyr","dplyr","stringr","RPostgreSQL","data.table","tidycensus","sf","tigris")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(openxlsx)
library(stringr)
library(tidyr)
library(dplyr)
library(RPostgreSQL)
library(data.table)
library(tidycensus)
library(tigris)
library(sf)
options(scipen = 999) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")

############### PREP COUNTY/STATE DATA ########################

#get data for student homelessness by race/ethnicity and county and state
county_enrollment_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/enrollment_total_by_county_2021_22.xlsx", sheet=1, startRow=1, rows=c(1:59), cols=c(1:11))
# head(county_enrollment_df)
# colnames(county_enrollment_df)

county_enrollment_df <- dplyr::rename(county_enrollment_df, c("geoname" = "Name",                             
                                                        "enroll_total" = "Total",                            
                                                       "enroll_nh_black" = "African.American",            
                                                       "enroll_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                       "enroll_nh_asian" = "Asian",                            
                                                       "enroll_nh_filipino" = "Filipino",                        
                                                       "enroll_latino" = "Hispanic.or.Latino",               
                                                       "enroll_nh_nhpi" = "Pacific.Islander",                 
                                                       "enroll_nh_white" = "White",                           
                                                       "enroll_nh_twoormor" = "Two.or.More.Races",                
                                                       "enroll_not_reported" = "Not.Reported"))  


state_enrollment_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/enrollment_total_by_county_2021_22.xlsx", sheet=1, startRow=61, rows=c(61:62), cols=c(1:11))
# head(state_enrollment_df)
state_enrollment_df <- dplyr::rename(state_enrollment_df, c("geoname" = "Name",                             
                                                     "enroll_total" = "Total",                            
                                                     "enroll_nh_black" = "African.American",            
                                                     "enroll_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                     "enroll_nh_asian" = "Asian",                            
                                                     "enroll_nh_filipino" = "Filipino",                        
                                                     "enroll_latino" = "Hispanic.or.Latino",               
                                                     "enroll_nh_nhpi" = "Pacific.Islander",                 
                                                     "enroll_nh_white" = "White",                           
                                                     "enroll_nh_twoormor" = "Two.or.More.Races",                
                                                     "enroll_not_reported" = "Not.Reported"))  
state_enrollment_df$geoname[state_enrollment_df$geoname =='Statewide'] <- 'California'

# head(state_enrollment_df)

county_homeless_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/homeless_enrollment_by_county_2021_22.xlsx", sheet=1, startRow=1, rows=c(1:59), cols=c(1:11))
# head(county_homeless_df)
county_homeless_df <- dplyr::rename(county_homeless_df, c("geoname" = "Name",                             
                                                   "homeless_total" = "Total",                            
                                                   "homeless_nh_black" = "African.American",            
                                                   "homeless_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                   "homeless_nh_asian" = "Asian",                            
                                                   "homeless_nh_filipino" = "Filipino",                        
                                                   "homeless_latino" = "Hispanic.or.Latino",               
                                                   "homeless_nh_nhpi" = "Pacific.Islander",                 
                                                   "homeless_nh_white" = "White",                           
                                                   "homeless_nh_twoormor" = "Two.or.More.Races",                
                                                   "homeless_not_reported" = "Not.Reported"))  

state_homeless_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/homeless_enrollment_by_county_2021_22.xlsx", sheet=1, startRow=61, rows=c(61:62), cols=c(1:11))

state_homeless_df <- dplyr::rename(state_homeless_df, c("geoname" = "Name",                             
                                                 "homeless_total" = "Total",                            
                                                 "homeless_nh_black" = "African.American",            
                                                 "homeless_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                 "homeless_nh_asian" = "Asian",                            
                                                 "homeless_nh_filipino" = "Filipino",                        
                                                 "homeless_latino" = "Hispanic.or.Latino",               
                                                 "homeless_nh_nhpi" = "Pacific.Islander",                 
                                                 "homeless_nh_white" = "White",                           
                                                 "homeless_nh_twoormor" = "Two.or.More.Races",                
                                                 "homeless_not_reported" = "Not.Reported"))  
state_homeless_df$geoname[state_homeless_df$geoname =='Statewide'] <- 'California'
# head(state_homeless_df)

######### Bind and merge dataframes to create one final df ##########

enrollment <- rbind(county_enrollment_df,state_enrollment_df)
# View(enrollment)

homelessness <- rbind(county_homeless_df,state_homeless_df)
# View(homelessness)

df <- enrollment %>% left_join(homelessness, by = "geoname")


#get census geoids

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

#add county geoids
df <- merge(x=ca,y=df,by="geoname", all=T)
#add state geoid
df <- within(df, geoid[geoname == 'California'] <- '06')


#View(df)

####### clean and transform the raw csv data #######
threshold <- 50
df <- df %>% 
  mutate( total_raw = ifelse(enroll_total < threshold, NA, homeless_total),   #pop screen for _raw columns where a group's enrollment is less than 50
          nh_black_raw = ifelse(enroll_nh_black < threshold, NA, homeless_nh_black),
          nh_aian_raw = ifelse(enroll_nh_aian < threshold, NA, homeless_nh_aian),
          nh_asian_raw = ifelse(enroll_nh_asian < threshold, NA, homeless_nh_asian),
          nh_filipino_raw = ifelse(enroll_nh_filipino < threshold, NA, homeless_nh_filipino),
          latino_raw = ifelse(enroll_latino < threshold, NA, homeless_latino),
          nh_pacisl_raw = ifelse(enroll_nh_nhpi < threshold, NA, homeless_nh_nhpi),
          nh_white_raw = ifelse(enroll_nh_white < threshold, NA, homeless_nh_white),
          nh_twoormor_raw = ifelse(enroll_nh_twoormor < threshold, NA, homeless_nh_twoormor),
          
          # calculate _rate column if _raw column does not equal NA
          total_rate = ifelse(is.na(total_raw), NA, (total_raw / enroll_total)*100),
          nh_black_rate = ifelse(is.na(nh_black_raw), NA, (nh_black_raw / enroll_nh_black)*100),
          nh_aian_rate = ifelse(is.na(nh_aian_raw), NA, (nh_aian_raw / enroll_nh_aian)*100),
          nh_asian_rate = ifelse(is.na(nh_asian_raw), NA, (nh_asian_raw / enroll_nh_asian)*100),
          nh_filipino_rate = ifelse(is.na(nh_filipino_raw), NA, (nh_filipino_raw / enroll_nh_filipino)*100),
          latino_rate = ifelse(is.na(latino_raw), NA, (latino_raw / enroll_latino)*100),
          nh_pacisl_rate = ifelse(is.na(nh_pacisl_raw), NA, (nh_pacisl_raw / enroll_nh_nhpi)*100),
          nh_white_rate = ifelse(is.na(nh_white_raw), NA, (nh_white_raw / enroll_nh_white)*100),
          nh_twoormor_rate = ifelse(is.na(nh_twoormor_raw), NA, (nh_twoormor_raw / enroll_nh_twoormor)*100),
          
          # create pop columns
          total_pop = enroll_total,
          nh_black_pop = enroll_nh_black,
          nh_aian_pop = enroll_nh_aian,
          nh_asian_pop = enroll_nh_asian,
          nh_filipino_pop = enroll_nh_filipino,
          latino_pop = enroll_latino,
          nh_pacisl_pop = enroll_nh_nhpi,
          nh_white_pop = enroll_nh_white,
          nh_twoormor_pop = enroll_nh_twoormor
          )
# View(df)   #This line is used to see a pop-out view of the df. Un-comment this line if you intend to use it
# colnames(df) This line is used to view column names so that you can select the appropriate column names when creating the df_subset. Un-comment this line if you intend to use it

########## create a df with only the newly calculated columns #######
df_subset <- select(df, geoid, geoname, ends_with("_pop"), ends_with("_raw"), ends_with("_rate")) %>% mutate(geolevel = ifelse(geoid == '06', 'state', 'county'))
  
View(df_subset)


### District-Place Crosswalk ### ---------------------------------------------------------------------
#  commented out after table is exported to postgres  #
## pull in 2021 CBF Places and Districts ##
# places <- places(state = 'CA', year = 2021, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
# unified <- school_districts(state = 'CA', type = "unified", year = 2021, cb = TRUE) %>% select(-c(STATEFP, UNSDLEA, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
# elementary <- school_districts(state = 'CA', type = "elementary", year = 2021, cb = TRUE) %>% select(-c(STATEFP, ELSDLEA, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
# secondary <- school_districts(state = 'CA', type = "secondary", year = 2021, cb = TRUE) %>% select(-c(STATEFP, SCSDLEA, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
# 
# crosswalk <- function(x, y, threshold) {
#   
#   x_3310 <- st_transform(x, 3310) # change projection to 3310
#   y_3310 <- st_transform(y, 3310) # change projection to 3310
#   # calculate area of tracts and places
#   x_3310$area <- st_area(x_3310)
#   y_3310$y_area <- st_area(y_3310)
#   # rename geoid fields
#   x_3310 <- x_3310%>% 
#     rename("x_geoid" = "GEOID", "x_name" = "NAME")
#   y_3310 <- y_3310%>% 
#     rename("y_geoid" = "GEOID", "y_name" = "NAME")
#   # run intersect
#   x_y <- st_intersection(x_3310, y_3310) 
#   # create combo geoid field
#   x_y$x_y_geoid <- paste(x_y$y_geoid, x_y$x_geoid, sep = "_")
#   # calculate area of intersect
#   x_y$intersect_area <- st_area(x_y)
#   # calculate percent of intersect out of total place area
#   y_x <- x_y %>% mutate(prc_y_area = as.numeric(x_y$intersect_area/x_y$y_area))
#   # calculate percent of intersect out of total tract area
#   x_y$prc_area <- as.numeric(x_y$intersect_area/x_y$area)
#   # convert to df
#   x_y <- as.data.frame(x_y)
#   y_x <- as.data.frame(y_x)
#   
#   xwalk <- full_join(y_x, select(x_y, c(x_y_geoid, prc_area)), by = 'x_y_geoid')
#   # filter xwalk where intersect between is equal or greater than Z% of x area OR y area. 
#   xwalk_filter <- xwalk %>% filter(prc_area >= threshold | prc_y_area >= threshold)
#   names(xwalk_filter) <- tolower(names(xwalk_filter)) # make col names lowercase
#   xwalk_filter <- select(xwalk_filter, x_y_geoid, x_geoid, y_geoid, y_name, x_name, namelsad, area, y_area, intersect_area, prc_area, prc_y_area)
#   
#   colnames(xwalk_filter) <- gsub("x", "district", colnames(xwalk_filter))  # rename fields to specific geographies involved
#   colnames(xwalk_filter) <- gsub("y", "place", colnames(xwalk_filter))  
#   xwalk_filter <- data.frame(xwalk_filter)
#   
# }

# run xwalk function on all 3 district types
# threshold <- .30
# unified_places <- crosswalk(unified, places, threshold) %>% mutate(district_type = 'Unified')
# elementary_places <- crosswalk(elementary, places, threshold) %>% mutate(district_type = 'Elementary')
# secondary_places <- crosswalk(secondary, places, threshold) %>% mutate(district_type = 'Secondary')
# 
# xwalk_filter <- rbind(unified_places, elementary_places, secondary_places) %>% relocate(district_type, .before = area)
# cds_codes <- st_read(con2, query = "SELECT DISTINCT ncesdist AS district_geoid, left(cdscode,7), district FROM education.cde_public_schools_2022_23") %>% # pull dist_ids
                  # mutate(cdscode = paste0(left,"0000000"))  # create district cdscodes
# xwalk_filter <- xwalk_filter %>% left_join(select(cds_codes, c(district_geoid, cdscode)), by = ("district_geoid")) %>% relocate(cdscode, .before = place_geoid) # n=2,373

# export xwalk table -------------------------------------------------------------------------

# table_name <- "district_place_2021"
# table_schema <- "crosswalks"
# table_comment_source <- "Created with W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\IndicatorScripts\\Housing\\hous_student_homelessness_2023.R and based on 2021 ACS TIGER CBF shapefiles.
#     Districts with 30% or more of their area within a city or that cover 30% or more of a city''s area are assigned to those cities.
#     As a result, a district can be assigned to more than one city"

# make character vector for field types in postgresql db
# charvect = rep('numeric', dim(xwalk_filter)[2])

# change data type for first three columns
# charvect[1:8] <- "varchar" # Cols 1-8 are character for the geoid and names etc

# add names to the character vector
# names(charvect) <- colnames(xwalk_filter)
# 
# dbWriteTable(con2, c(table_schema, table_name), xwalk_filter,
#             overwrite = FALSE, row.names = FALSE,
#             field.types = charvect)

# write comment to table, and the first three fields that won't change.
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

# send table comment to database
# dbSendQuery(conn = con2, table_comment)


############### PREP CITY DATA ########################
dist_df <- st_read(con2, query = "SELECT * FROM education.cde_dataquest_district_homeless_2022_23")
crosswalk_2021 <- st_read(con2, query = "SELECT place_geoid, place_name, district_geoid, cdscode, prc_area FROM crosswalks.district_place_2021") # we use 2021 shapes bc 2022 shapes are avail. yet
dist_df_xwalk <- dist_df %>% left_join(crosswalk_2021, by=("cdscode"))

## extra step: weighted averages 

# Option 1: Calculate weighted averages based on percent of district intersection in city

calculations_enr <- function(x,geoid,geoname) {
#  ## total 
  total_enroll <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(total_enroll = sum(total_enroll*prc_area, na.rm=TRUE))
  
#  ## nh white
  nh_white_enroll <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_white_enroll = sum(nh_white_enroll*prc_area, na.rm=TRUE))
  
  ## nh asian
  nh_asian_enroll <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_asian_enroll = sum(nh_asian_enroll*prc_area, na.rm=TRUE))
  
  ## nh filipino
  nh_filipino_enroll <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_filipino_enroll = sum(nh_filipino_enroll*prc_area, na.rm=TRUE))
  
  ## nh black
  nh_black_enroll <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_black_enroll = sum(nh_black_enroll*prc_area, na.rm=TRUE))
  
  ## nh pacisl 
  nh_pacisl_enroll <- x %>% group_by({{geoid}}, {{geoname}})%>% summarize(nh_pacisl_enroll = sum(nh_nhpi_enroll*prc_area, na.rm=TRUE))
  
  ## nh aian
  nh_aian_enroll <- x %>% group_by({{geoid}}, {{geoname}})%>% summarize(nh_aian_enroll = sum(nh_aian_enroll*prc_area, na.rm=TRUE))
  
  ## nh two or more
  nh_twoormor_enroll <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_twoormor_enroll = sum(nh_twoormor_enroll*prc_area, na.rm=TRUE))
  
  ## latino
  latino_enroll <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(latino_enroll = sum(latino_enroll*prc_area, na.rm=TRUE))
  
  z <- total_enroll %>% full_join(nh_white_enroll) %>% full_join(nh_asian_enroll) %>% full_join(nh_filipino_enroll) %>% full_join(nh_black_enroll) %>% full_join(nh_pacisl_enroll) %>% full_join(nh_aian_enroll) %>% full_join(nh_twoormor_enroll) %>% full_join(latino_enroll) 
  
  return(z)
}

calculations_homeless <- function(x,geoid,geoname) {
  ## total 
  total_homeless <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(total_homeless = sum(total_homeless*prc_area, na.rm=TRUE))
  
  ## nh white
  nh_white_homeless <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_white_homeless = sum(nh_white_homeless*prc_area, na.rm=TRUE))
  
  ## nh asian
  nh_asian_homeless <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_asian_homeless = sum(nh_asian_homeless*prc_area, na.rm=TRUE))
  
  ## nh filipino
  nh_filipino_homeless <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_filipino_homeless = sum(nh_filipino_homeless*prc_area, na.rm=TRUE))
    
  ## nh black
  nh_black_homeless <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_black_homeless = sum(nh_black_homeless*prc_area, na.rm=TRUE))
  
  ## nh pacisl 
  nh_pacisl_homeless <- x %>% group_by({{geoid}}, {{geoname}})%>% summarize(nh_pacisl_homeless = sum(nh_nhpi_homeless*prc_area, na.rm=TRUE))
  
  ## nh aian
  nh_aian_homeless <- x %>% group_by({{geoid}}, {{geoname}})%>% summarize(nh_aian_homeless = sum(nh_aian_homeless*prc_area, na.rm=TRUE))
  
  ## nh two or more
  nh_twoormor_homeless <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(nh_twoormor_homeless = sum(nh_twoormor_homeless*prc_area, na.rm=TRUE))
  
  ## latino
  latino_homeless <- x %>% group_by({{geoid}}, {{geoname}}) %>% summarize(latino_homeless = sum(latino_homeless*prc_area, na.rm=TRUE))
  
  z <- total_homeless %>% full_join(nh_white_homeless) %>% full_join(nh_asian_homeless) %>% full_join(nh_filipino_homeless) %>% full_join(nh_black_homeless) %>% full_join(nh_pacisl_homeless) %>% full_join(nh_aian_homeless) %>% full_join(nh_twoormor_homeless) %>% full_join(latino_homeless) 
  
  return(z)
}

city_enr <- calculations_enr(dist_df_xwalk,place_geoid,place_name)
city_homeless <- calculations_homeless(dist_df_xwalk,place_geoid,place_name)

df_city_lf <- city_enr %>% left_join(city_homeless, by=c("place_geoid", "place_name"))


# Option 2: Calculate weighted averages based on total and raced enrollment size

```{r}
dist_df_xwalk
```


enrollment_weights <- dist_df_xwalk %>% group_by(place_geoid) %>% mutate(
sum_total_enroll = sum(total_enroll, na.rm = T), 
prc_total_enroll = (total_enroll/sum_total_enroll),

sum_nh_black_enroll = sum(nh_black_enroll, na.rm = T), 
prc_nh_black_enroll = (nh_black_enroll/sum_nh_black_enroll),

sum_nh_aian_enroll = sum(nh_aian_enroll, na.rm = T), 
prc_nh_aian_enroll = (nh_aian_enroll/sum_nh_aian_enroll),

sum_nh_pacisl_enroll = sum(nh_nhpi_enroll, na.rm = T), 
prc_nh_pacisl_enroll = (nh_nhpi_enroll/sum_nh_pacisl_enroll),

sum_nh_asian_enroll = sum(nh_asian_enroll, na.rm = T), 
prc_nh_asian_enroll = (nh_asian_enroll/sum_nh_asian_enroll),
                                                                                                      
sum_nh_filipino_enroll = sum(nh_filipino_enroll, na.rm = T), 
prc_nh_filipino_enroll = (nh_filipino_enroll/sum_nh_filipino_enroll), 

sum_latino_enroll = sum(latino_enroll, na.rm = T), 
prc_latino_enroll = (latino_enroll/sum_latino_enroll),

sum_nh_white_enroll = sum(nh_white_enroll, na.rm = T), 
prc_nh_white_enroll = (nh_white_enroll/sum_nh_white_enroll),

sum_nh_twoormor_enroll = sum(nh_twoormor_enroll, na.rm = T), 
prc_nh_twoormor_enroll = (nh_twoormor_enroll/sum_nh_twoormor_enroll))


## calculate weighted enrollment and homelessness

total_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(total_enroll = sum(total_enroll*prc_total_enroll, na.rm = TRUE)) 
total_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(total_homeless = sum(total_homeless*prc_total_enroll, na.rm = TRUE))

nh_black_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_black_enroll = sum(nh_black_enroll*prc_nh_black_enroll, na.rm = TRUE))
nh_black_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_black_homeless = sum(nh_black_homeless*prc_nh_black_enroll, na.rm = TRUE))

nh_aian_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_aian_enroll = sum(nh_aian_enroll*prc_nh_aian_enroll, na.rm = TRUE))
nh_aian_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_aian_homeless = sum(nh_aian_homeless*prc_nh_aian_enroll, na.rm = TRUE))

nh_asian_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_asian_enroll = sum(nh_asian_enroll*prc_nh_asian_enroll, na.rm = TRUE)) 
nh_asian_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_asian_homeless = sum(nh_asian_homeless*prc_nh_asian_enroll, na.rm = TRUE))

nh_filipino_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_filipino_enroll = sum(nh_filipino_enroll*prc_nh_filipino_enroll, na.rm = TRUE)) 
nh_filipino_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_filipino_homeless = sum(nh_filipino_homeless*prc_nh_filipino_enroll, na.rm = TRUE))

latino_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(latino_enroll = sum(latino_enroll*prc_latino_enroll, na.rm = TRUE)) 
latino_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(latino_homeless = sum(latino_homeless*prc_latino_enroll, na.rm = TRUE))

nh_pacisl_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_pacisl_enroll = sum(nh_nhpi_enroll*prc_nh_pacisl_enroll, na.rm = TRUE)) 
nh_pacisl_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_pacisl_homeless = sum(nh_nhpi_homeless*prc_nh_pacisl_enroll, na.rm = TRUE))

nh_white_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_white_enroll = sum(nh_white_enroll*prc_nh_white_enroll, na.rm = TRUE)) 
nh_white_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_white_homeless = sum(nh_white_homeless*prc_nh_white_enroll, na.rm = TRUE))

nh_twoormor_enroll <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_twoormor_enroll = sum(nh_twoormor_enroll*prc_nh_twoormor_enroll, na.rm = TRUE)) 
nh_twoormor_homeless <- enrollment_weights %>% group_by(place_geoid,place_name) %>% summarize(nh_twoormor_homeless = sum(nh_twoormor_homeless*prc_nh_twoormor_enroll, na.rm = TRUE))

# merge enroll and homeless together
df_city <- total_enroll %>% full_join(total_homeless) %>% full_join(nh_black_enroll) %>% full_join(nh_black_homeless) %>% full_join(nh_aian_enroll) %>% full_join(nh_aian_homeless) %>% full_join(nh_asian_enroll) %>% full_join(nh_asian_homeless) %>% full_join(nh_filipino_enroll) %>% full_join(nh_filipino_homeless) %>% full_join(latino_enroll) %>% full_join(latino_homeless) %>% full_join(nh_pacisl_enroll) %>% full_join(nh_pacisl_homeless) %>% full_join(nh_white_enroll) %>% full_join(nh_white_homeless) %>% full_join(nh_twoormor_enroll) %>% full_join(nh_twoormor_homeless)


# screen data and calc rates; n = 1,612 cities. threshold is defined in county/state section.
threshold_2 <- 1 # extra screen for city data only on count of homeless students
df_city_screen <- df_city %>% mutate(
  total_raw = ifelse(total_enroll < threshold | total_homeless < threshold_2, NA, total_homeless), 
  nh_white_raw = ifelse(nh_white_enroll < threshold | nh_white_homeless < threshold_2, NA, nh_white_homeless), 
  nh_asian_raw = ifelse(nh_asian_enroll < threshold | nh_asian_homeless < threshold_2, NA, nh_asian_homeless), 
  nh_filipino_raw = ifelse(nh_filipino_enroll < threshold | nh_filipino_homeless < threshold_2, NA, nh_filipino_homeless), 
  nh_black_raw = ifelse(nh_black_enroll < threshold | nh_black_homeless < threshold_2, NA, nh_black_homeless), 
  nh_pacisl_raw = ifelse(nh_pacisl_enroll < threshold | nh_pacisl_homeless < threshold_2, NA, nh_pacisl_homeless), 
  nh_aian_raw = ifelse(nh_aian_enroll < threshold | nh_aian_homeless < threshold_2, NA, nh_aian_homeless), 
  nh_twoormor_raw = ifelse(nh_twoormor_enroll < threshold | nh_twoormor_homeless < threshold_2, NA, nh_twoormor_homeless), 
  latino_raw = ifelse(latino_enroll < threshold | latino_homeless < threshold_2, NA, latino_homeless), 
  
  total_rate = (total_raw/total_enroll) * 100,                   # 
  nh_white_rate = (nh_white_raw/nh_white_enroll) * 100,          # 
  nh_asian_rate = (nh_asian_raw/nh_asian_enroll) * 100,          # 
  nh_filipino_rate = (nh_filipino_raw/nh_filipino_enroll) * 100, #
  nh_black_rate = (nh_black_raw/nh_black_enroll) * 100,          # 
  nh_pacisl_rate = (nh_pacisl_raw/nh_pacisl_enroll) * 100,       # 
  nh_aian_rate = (nh_aian_raw/nh_aian_enroll) * 100,             #  
  nh_twoormor_rate = (nh_twoormor_raw/nh_twoormor_enroll) * 100, # 
  latino_rate = (latino_raw/latino_enroll) * 100,                # 
  # colSums(!is.na(df_city_screen)) # to check how many non-null values we get after screening
  geolevel = 'place') %>% rename(geoid = place_geoid, geoname = place_name)

# update city df to match county/state df
names(df_city_screen) <- gsub(x = names(df_city_screen), pattern = "_enroll", replacement = "_pop")  
df_city_screen <- df_city_screen %>% select(-contains("homeless"))

## Combine county/state with city data ##
final_df <- rbind(df_subset, df_city_screen)
d <- final_df

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


###update info for postgres tables###
county_table_name <- "arei_hous_student_homelessness_county_2023"
state_table_name <- "arei_hous_student_homelessness_state_2023"
city_table_name <- "arei_hous_student_homelessness_city_2023"
indicator <- "Student homelessness rates. Data for groups with enrollment under 50 are excluded from the calculations. Homelessness rate calculated as a percent of enrollment for each group"
source <- "CDE 2021-22 (county/state), CDE 2022-23 (city) https://dq.cde.ca.gov/dataquest/"
rc_schema <- 'v5'


#send tables to postgres
#to_postgres(county_table, state_table)

#city_to_postgres(city_table)

