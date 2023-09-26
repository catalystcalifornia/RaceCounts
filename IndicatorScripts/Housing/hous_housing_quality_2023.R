## Percentage of Low Quality Housing Units RC v5 City ##
# Install packages if not already installed
list.of.packages <- c("data.table", "stringr", "dplyr", "RPostgreSQL", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "here", "sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#Load libraries
library(data.table)
library(stringr)
library(dplyr)
library(RPostgreSQL)
library(dbplyr)
library(srvyr)
library(tidycensus)
library(tidyr)
library(rpostgis)
library(here)
library(sf)
library(tigris)
options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")
con2<- connect_to_db("racecounts")


#set source for weighted averages script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")

# API Call Info - Change each year

yr = 2021
survey = "acs5"
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
var =  c("B25003_001", "B25003B_001", "B25003C_001", "B25003D_001", "B25003E_001", "B25003F_001","B25003G_001", "B25003H_001", "B25003I_001", "B25040_010","B25048_003", "B25052_003")


# Data Dictionary ------------------------------------------------------
data_dict<- load_variables(year = yr, dataset = survey, cache = TRUE)  %>% filter (name %in% var)

#B25049 is in methodology but  seems like B25048_003 does the trick?
# TENURE HOUSEHOLDER, NO FUEL, NO PUMBLING, NO KITCHEN ------------------------------------------------------------------

############## UPDATE FOR SPECIFIC INDICATOR HERE ##############

# Pull data from Census API
df <- do.call(rbind.data.frame, list(
  get_acs(geography = "tract", state = "CA", variables = var, year = yr, survey = survey, cache_table = TRUE)
  %>% mutate(geolevel = "tract")
))

# Rename estimate and moe columns to e and m, respectively
df <- df %>%
  rename(e = estimate, m = moe)

# Spread! Switch to wide format.
df <- df %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

# Rename, calculate moe, pct, cv's
df_calcs <- df %>% rename(
  total_universe = B25003_001e,
  total_universe_moe = B25003_001m,
  black_universe = B25003B_001e,
  black_universe_moe = B25003B_001m,
  aian_universe = B25003C_001e,
  aian_universe_moe = B25003C_001m,
  asian_universe = B25003D_001e,
  asian_universe_moe = B25003D_001m,
  pacisl_universe = B25003E_001e,
  pacisl_universe_moe = B25003E_001m,
  other_universe = B25003F_001e,
  other_universe_moe = B25003F_001m,
  twoormor_universe = B25003G_001e,
  twoormor_moe = B25003G_001m,
  nh_white_universe = B25003H_001e,
  nh_white_universe_moe = B25003H_001m ,
  latino_universe = B25003I_001e,
  latino_universe_moe = B25003I_001m,
  
  num_incomplete_heating = B25040_010e,
  moe_num_incomplete_heating = B25040_010m,
  
  num_incomplete_plumbing = B25048_003e,
  moe_num_incomplete_plumbing = B25048_003m,
  
  
  num_incomplete_kitchen = B25052_003e,
  moe_num_incomplete_kitchen = B25052_003m) %>%
  
  mutate(
    pct_incomplete_heating = ifelse(total_universe == 0, NA, num_incomplete_heating/ total_universe * 100),
    
    pct_incomplete_plumbing= ifelse(total_universe == 0, NA, num_incomplete_plumbing/ total_universe * 100),
    
    pct_incomplete_kitchen= ifelse(total_universe == 0, NA, num_incomplete_kitchen/ total_universe * 100))
 
  
### Commented out: no longer need this because we are not calculating MOE anymore
# Calculate places tenure and low quality housing

############## PRE-CALCULATION DATA PREP ##############
# Pull data from Census API
#df_places <- do.call(rbind.data.frame, list(
#  get_acs(geography = "place", state = "CA", variables = var, year = yr, survey = survey, cache_table = TRUE)
#  %>% mutate(geolevel = "places")
#))

# Rename estimate and moe columns to e and m, respectively
#df_places <- df_places %>%
#  rename(e = estimate, m = moe) %>%
#  # Spread! Switch to wide format.
#  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

# rename columns
#df_places_calcs <- df_places %>% rename(
#total_universe = B25003_001e,
#total_universe_moe = B25003_001m,
#black_universe = B25003B_001e,
#black_universe_moe = B25003B_001m,
#aian_universe = B25003C_001e,
#aian_universe_moe = B25003C_001m,
#asian_universe = B25003D_001e,
#asian_universe_moe = B25003D_001m,
#pacisl_universe = B25003E_001e,
#pacisl_universe_moe = B25003E_001m,
#other_universe = B25003F_001e,
#other_universe_moe = B25003F_001m,
#twoormor_universe = B25003G_001e,
#twoormor_moe = B25003G_001m,
#nh_white_universe = B25003H_001e,
#nh_white_universe_moe = B25003H_001m ,
#latino_universe = B25003I_001e,
#latino_universe_moe = B25003I_001m,

#num_incomplete_heating = B25040_010e,
#moe_num_incomplete_heating = B25040_010m,

#num_incomplete_plumbing = B25048_003e,
#moe_num_incomplete_plumbing = B25048_003m,

#num_incomplete_kitchen = B25052_003e,
#moe_num_incomplete_kitchen = B25052_003m) %>%

#  mutate(
#pct_incomplete_heating = ifelse(total_universe == 0, 0, num_incomplete_heating/ total_universe * 100),

#pct_incomplete_plumbing= ifelse(total_universe == 0, 0, num_incomplete_plumbing/ total_universe * 100),
#pct_incomplete_kitchen= ifelse(total_universe == 0, 0, num_incomplete_kitchen/ total_universe * 100),

# calculate moe of pct
#moe_pct_incomplete_heating = moe_prop(num_incomplete_heating,
#                                              total_universe,
#                                              moe_num_incomplete_heating,
#                                            total_universe_moe)*100,

#moe_pct_incomplete_plumbing = moe_prop(num_incomplete_plumbing,
#                                              total_universe,
#                                             moe_num_incomplete_plumbing,
#                                             total_universe_moe)*100,

#moe_pct_incomplete_kitchen = moe_prop(num_incomplete_kitchen,
#                                             total_universe,
#                                              moe_num_incomplete_kitchen,
#                                           total_universe_moe)*100,

## calculate cv

#cv_num_incomplete_heating = ifelse(num_incomplete_heating == 0, 0, moe_num_incomplete_heating/1.645/ num_incomplete_heating*100),

#cv_pct_incomplete_heating = ifelse(pct_incomplete_heating == 0, 0,moe_pct_incomplete_heating / 1.645 / pct_incomplete_heating*100),

#cv_num_incomplete_plumbing = ifelse(num_incomplete_plumbing == 0, 0, moe_num_incomplete_plumbing / 1.645 / num_incomplete_plumbing* 100),

#cv_pct_incomplete_plumbing = ifelse(pct_incomplete_plumbing == 0, 0, moe_pct_incomplete_plumbing / 1.645/ pct_incomplete_plumbing * 100),

#cv_num_incomplete_kitchen = ifelse(num_incomplete_kitchen == 0, 0, moe_num_incomplete_kitchen / 1.645 / num_incomplete_kitchen * 100),

#cv_pct_incomplete_kitchen = ifelse(pct_incomplete_kitchen == 0, 0, moe_pct_incomplete_kitchen / 1.645 / pct_incomplete_kitchen * 100)

#)


# cross-walk: export 2021 cross-walk data ------------------------------------------------------------
### CT-Place Crosswalk ### ---------------------------------------------------------------------
## pull in 2020 CBF Places ##
places <- places(state = 'CA', year = 2021, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))
tracts <- tracts(state = 'CA', year = 2021, cb = TRUE) %>% select(-c(STATEFP, TRACTCE, AFFGEOID, NAME, NAMELSAD, STATE_NAME, LSAD, ALAND, AWATER))

## spatial join ##
# places_3310 <- st_transform(places, 3310) # change projection to 3310
# tracts_3310 <- st_transform(tracts, 3310) # change projection to 3310
# # calculate area of tracts and places
# tracts_3310$area <- st_area(tracts_3310)
# places_3310$pl_area <- st_area(places_3310)
# # rename geoid fields
# tracts_3310 <- tracts_3310%>% 
#   rename("ct_geoid" = "GEOID", "county_geoid" = "COUNTYFP", "county_name" = "NAMELSADCO")
# places_3310 <- places_3310%>% 
#   rename("place_geoid" = "GEOID", "place_name" = "NAME")
# # run intersect
# tracts_places <- st_intersection(tracts_3310, places_3310) 
# # create ct_place combo geoid field
# tracts_places$ct_place_geoid <- paste(tracts_places$place_geoid, tracts_places$ct_geoid, sep = "_")
# # calculate area of intersect
# tracts_places$intersect_area <- st_area(tracts_places)
# # calculate percent of intersect out of total place area
# places_tracts <- tracts_places %>% mutate(prc_pl_area = as.numeric(tracts_places$intersect_area/tracts_places$pl_area))
# # calculate percent of intersect out of total tract area
# tracts_places$prc_area <- as.numeric(tracts_places$intersect_area/tracts_places$area)
# # convert to df
# tracts_places <- as.data.frame(tracts_places)
# places_tracts <- as.data.frame(places_tracts)
# # xwalk N = 16,342
# xwalk <- full_join(places_tracts, select(tracts_places, c(ct_place_geoid, prc_area)), by = 'ct_place_geoid')
# # filter xwalk where intersect between tracts and places is equal or greater than X% of tract area OR place area. xwalk_filter N = 9,675
# threshold <- .25
# xwalk_filter <- xwalk %>% filter(prc_area >= threshold | prc_pl_area >= threshold)
# names(xwalk_filter) <- tolower(names(xwalk_filter)) # make col names lowercase
# xwalk_filter <- select(xwalk_filter, ct_place_geoid, ct_geoid, place_geoid, county_geoid, place_name, namelsad, county_name, area, pl_area, intersect_area, prc_area, prc_pl_area)
# 
# # export xwalk table
# table_name <- "ct_place_2021"
# table_schema <- "crosswalks"
# table_comment_source <- "Created with W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\IndicatorScripts\\Environment\\hben_drinking_water_2023.R and based on 2021 ACS TIGER non-CBF shapefiles.
#     CTs with 25% or more of their area within a city or that cover 25% or more of a city''s area are assigned to those cities.
#     As a result, a CT can be assigned to more than one city"
# 
# # make character vector for field types in postgresql db
# charvect = rep('numeric', dim(xwalk_filter)[2])
# 
# #   change data type for first three columns
# charvect[1:7] <- "varchar" # first 7 are character for the geoid and names etc
# 
# #  add names to the character vector
# names(charvect) <- colnames(xwalk_filter)
# 
# dbWriteTable(con, c(table_schema, table_name), xwalk_filter, 
#              overwrite = FALSE, row.names = FALSE,
#              field.types = charvect)
# 
# # write comment to table, and the first three fields that won't change.
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

# send table comment to database
# dbSendQuery(conn = con, table_comment)      			

crosswalk <- dbGetQuery(con, "SELECT * FROM crosswalks.ct_place_2021")

# places ------------------------------------------------------------

places <- places(state = 'CA', year = 2020, cb = TRUE) %>% select(-c(STATEFP, PLACEFP, PLACENS, AFFGEOID, STUSPS, STATE_NAME, LSAD, ALAND, AWATER))

# universe(total housing units and by race)

universe <- df_calcs %>% select(GEOID, NAME, ends_with("universe"))

# join with cross walk
universe <- universe  %>% right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid")) %>% dplyr::rename(sub_id = GEOID, target_id = place_geoid)  # join target geoids/names and rename to generic column names for WA functions 


# rename to pop
pop <- universe %>% rename_all(.funs = funs(sub("universe*", "pop", names(universe)))) %>% as.data.frame() %>%
  mutate(geolevel = "tract")

#  CITY WEIGHTED AVG CALCS ------------------------------------------------
############## UPDATE FOR SPECIFIC INDICATOR HERE ##############

#year <- c(2020)                  # define your data vintage
#subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
#targetgeolevel <- c('place')     # define your target geolevel: county (state is handled separately)
#survey <- "acs5"                 # define which Census survey you want
pop_threshold = 0                 # define population threshold for screening: note this is only for households. We need to assign pop threshold a value in order for functions to work

#pop_df <- targetgeo_pop(pop) 

## Comment out target geo pop because population here are households for total and by race, not population estimates

# select pop columns and rename to RC names
b <- select(pop, sub_id, target_id, ends_with("pop"), -NAME)


# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- gsub("_pop", "_target_pop", colnames(c))

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop, sub_id, target_id, geolevel, ends_with("pop"), -NAME) 
names(e) <-gsub("_pop", "_sub_pop", colnames(e))
pop <- e %>% left_join(c, by = "target_id")
pop_df <- as.data.frame(pop)


## indicator 1: Lack of Kitchen

# Rename so WA functions can work
ind_df <- df_calcs  %>% select(GEOID, NAME, pct_incomplete_kitchen) %>% rename(indicator = pct_incomplete_kitchen, sub_id = GEOID )

pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel

# calc weighted average and apply reliability screens

city_wa <- wt_avg(pct_df) 

# rename columns for RC functions
kitchen_city_wa <- city_wa %>% left_join(select(places, c(GEOID, NAME)), by = c("target_id" = "GEOID")) %>%
  rename(geoname = NAME, geoid = target_id) %>% select(-c(geometry)) %>% mutate(geolevel = 'city') %>% dplyr::relocate(geoname, .after = geoid)   

# rename columns
kitchen_city_wa  <- kitchen_city_wa  %>% rename_all(.funs = funs(sub("rate", "kitchen", names(kitchen_city_wa))))


## indicator 2: Lack of Heating

# Rename so WA functions can work
ind_df <- df_calcs %>% select(GEOID, NAME, pct_incomplete_heating) %>% rename(indicator = pct_incomplete_heating, sub_id = GEOID )

# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df)        

heating_city_wa <- city_wa %>% left_join(select(places, c(GEOID, NAME)), by = c("target_id" = "GEOID")) %>%
  rename(geoname = NAME, geoid = target_id) %>% select(-c(geometry)) %>% mutate(geolevel = 'city') %>% dplyr::relocate(geoname, .after = geoid)   # rename columns for RC functions

# rename columns
heating_city_wa  <- heating_city_wa  %>% rename_all(.funs = funs(sub("rate", "heating", names(heating_city_wa))))


## indicator 3: Lack of Plumbing

# Rename so WA functions can work
ind_df <- df_calcs %>% select(GEOID, NAME, pct_incomplete_plumbing) %>% rename(indicator = pct_incomplete_plumbing, sub_id = GEOID)

# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df)        

plumbing_city_wa <- city_wa %>% left_join(select(places, c(GEOID, NAME)), by = c("target_id" = "GEOID")) %>%
  rename(geoname = NAME, geoid = target_id) %>% select(-c(geometry)) %>% mutate(geolevel = 'city') %>% dplyr::relocate(geoname, .after = geoid)   # rename columns for RC functions

# rename columns
plumbing_city_wa <- plumbing_city_wa %>% rename_all(.funs = funs(sub("rate", "plumbing", names(plumbing_city_wa))))


# Merge all 3 -------------------------------------------------------------

df <- kitchen_city_wa %>% full_join(heating_city_wa) %>% full_join(plumbing_city_wa) %>% select(geoid, geoname, ends_with("kitchen"), ends_with("plumbing"), ends_with("heating"), everything())

## average out the rates, ## filter for cities that only have 1 of the 3 indicator for total/by race. 
df_average <- df %>% mutate(
  
  nh_white_rate = ifelse(!is.na(nh_white_heating) & is.na(nh_white_kitchen) & is.na(nh_white_plumbing) | is.na(nh_white_heating) & !is.na(nh_white_kitchen) & is.na(nh_white_plumbing) | is.na(nh_white_heating) & is.na(nh_white_kitchen) & !is.na(nh_white_plumbing), NA, (nh_white_heating + nh_white_kitchen + nh_white_plumbing) / 3),
  
  black_rate = ifelse(!is.na(black_heating) & is.na(black_kitchen) & is.na(black_plumbing) | is.na(black_heating) & !is.na(black_kitchen) & is.na(black_plumbing) | is.na(black_heating) & is.na(black_kitchen) & !is.na(black_plumbing), NA, (black_heating + black_kitchen + black_plumbing) / 3),
  
  aian_rate = ifelse(!is.na(aian_heating) & is.na(aian_kitchen) & is.na(aian_plumbing) | is.na(aian_heating) & !is.na(aian_kitchen) & is.na(aian_plumbing) | is.na(aian_heating) & is.na(aian_kitchen) & !is.na(aian_plumbing), NA, (aian_heating + aian_kitchen + aian_plumbing) / 3),
  
  latino_rate = ifelse(!is.na(latino_heating) & is.na(latino_kitchen) & is.na(latino_plumbing) | is.na(latino_heating) & !is.na(latino_kitchen) & is.na(latino_plumbing) | is.na(latino_heating) & is.na(latino_kitchen) & !is.na(latino_plumbing), NA, (latino_heating + latino_kitchen + latino_plumbing) / 3),
  
  
  asian_rate = ifelse(!is.na(asian_heating) & is.na(asian_kitchen) & is.na(asian_plumbing) | is.na(asian_heating) & !is.na(asian_kitchen) & is.na(asian_plumbing) | is.na(asian_heating) & is.na(asian_kitchen) & !is.na(asian_plumbing), NA, (asian_heating + asian_kitchen + asian_plumbing) / 3),
  
  
  pacisl_rate = ifelse(!is.na(pacisl_heating) & is.na(pacisl_kitchen) & is.na(pacisl_plumbing) | is.na(pacisl_heating) & !is.na(pacisl_kitchen) & is.na(pacisl_plumbing) | is.na(pacisl_heating) & is.na(pacisl_kitchen) & !is.na(pacisl_plumbing), NA, (pacisl_heating + pacisl_kitchen + pacisl_plumbing) / 3),
  
  
  other_rate = ifelse(!is.na(other_heating) & is.na(other_kitchen) & is.na(other_plumbing) | is.na(other_heating) & !is.na(other_kitchen) & is.na(other_plumbing) | is.na(other_heating) & is.na(other_kitchen) & !is.na(other_plumbing), NA, (other_heating + other_kitchen + other_plumbing) / 3),
  
  twoormor_rate = ifelse(!is.na( twoormor_heating) & is.na( twoormor_kitchen) & is.na( twoormor_plumbing) | is.na( twoormor_heating) & !is.na( twoormor_kitchen) & is.na( twoormor_plumbing) | is.na( twoormor_heating) & is.na( twoormor_kitchen) & !is.na( twoormor_plumbing), NA, ( twoormor_heating +  twoormor_kitchen +  twoormor_plumbing) / 3),
  
  
  total_rate = ifelse(!is.na(total_heating) & is.na(total_kitchen) & is.na(total_plumbing) | is.na(total_heating) & !is.na(total_kitchen) & is.na(total_plumbing) | is.na(total_heating) & is.na(total_kitchen) & !is.na(total_plumbing), NA, (total_heating + total_kitchen + total_plumbing) / 3),
  
  geolevel = "city"
) %>% select(
  
  
  geoid, geoname, ends_with("rate"), geolevel
) 



# pop screening -----------------------------------------------------------


#load_variables(year = yr, dataset = survey, cache = TRUE)  %>% filter (name %in% c("B01001_001", "B01001B_001", "B01001C_001", "B01001D_001", "B01001E_001", "B01001F_001", "B01001G_001", "B01001H_001", "B01001I_001"))

# Load population estimates that match our data: total, black, aian, asian, nhpi, other, two or more, latino, nh white ------------------------------

yr = 2021
survey = "acs5"
subgeo <- c('place')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
var2 = c("B01001_001", "B01001B_001", "B01001C_001", "B01001D_001", "B01001E_001", "B01001F_001", "B01001G_001", "B01001H_001", "B01001I_001")

# Pull data from Census API
pop2 <- do.call(rbind.data.frame, list(
  get_acs(geography = "place", state = "CA", variables = var2, year = yr, survey = survey, cache_table = TRUE)
  %>% mutate(geolevel = "place")
))

# Rename estimate and moe columns to e and m, respectively
pop2 <- pop2 %>%
  rename(e = estimate, m = moe)

# Spread! Switch to wide format.
pop2 <- pop2 %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

# rename columns to population estimates
pop2 <- pop2 %>% rename(geoid = GEOID,
                        total_count = B01001_001e,
                        black_count = B01001B_001e,
                        aian_count = B01001C_001e,
                        asian_count = B01001D_001e,
                        nhpi_count = B01001E_001e,
                        other_count = B01001F_001e,
                        twoormor_count = B01001G_001e,
                        nh_white_count = B01001H_001e,
                        latino_count = B01001I_001e) %>% select(geoid, ends_with("count"))

population_threshold = 250


df_screen <- df_average %>% left_join(
  pop2
) %>%
  mutate(
    total_rate = ifelse(total_count < population_threshold, NA, total_rate),
    nh_white_rate = ifelse(nh_white_count < population_threshold, NA, nh_white_rate),
    black_rate = ifelse(black_count < population_threshold, NA, black_rate),
    aian_rate = ifelse(aian_count < population_threshold, NA, aian_rate),
    asian_rate = ifelse(asian_count < population_threshold, NA, asian_rate),
    pacisl_rate = ifelse(nhpi_count < population_threshold, NA, pacisl_rate),
    other_rate = ifelse(other_count < population_threshold, NA, other_rate),
    twoormor_rate = ifelse(twoormor_count < population_threshold, NA, twoormor_rate),
    latino_rate = ifelse(latino_count < population_threshold, NA, latino_rate)
  )

# count number of rates
rates <- dplyr::select(df_screen, ends_with("_rate"))
rates$rate_count<- rowSums(!is.na(rates))
df_screen$rate_count <- rates$rate_count



# filter for only cities with at least 1 rate value
df_final_screen <- df_screen %>% filter(
  rate_count >0
) %>% select(geoid, geoname, ends_with("rate"), geolevel)


d <- df_final_screen



############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity



#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))


#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)

city_table_name <- "arei_hous_housing_quality_city_2023"
rc_schema <- 'v5'

indicator <- "Average percent of households that lack kitchen, plumbing, and heat in comparison to total households."

source <- "American Community Survey 5-Year Estimates (2017-21) Tables B25048, B25052, B25003B-I, B25040     Created 7-20-23
W:/Project/RACE COUNTS/2023_v5/Housing/Documentation/QA_Housing_Quality_City.docx"

# city_to_postgres(city_table)


# Previous City Views ------------------------------------------------------





v2_city <- dbGetQuery(con2, "SELECT * FROM data.arei_housing_quality_city")

#arei_heating_tract <- dbGetQuery(con2, "SELECT * FROM data.arei_heating_tract")
#arei_heating_wa_city <- dbGetQuery(con2, "SELECT * FROM data.arei_heating_wa_city") %>% arrange(city_name)


#arei_plumbing_tract <- dbGetQuery(con2, "SELECT * FROM data.arei_plumbing_tract")
#arei_plumbing_wa_city <- dbGetQuery(con2, "SELECT * FROM data.arei_plumbing_wa_city") %>% arrange(city_name)


#arei_kitchen_tract <- dbGetQuery(con2, "SELECT * FROM data.arei_kitchen_tract")
#arei_kitchen_wa_city <- dbGetQuery(con2, "SELECT * FROM data.arei_kitchen_wa_city") %>% arrange(city_name)
