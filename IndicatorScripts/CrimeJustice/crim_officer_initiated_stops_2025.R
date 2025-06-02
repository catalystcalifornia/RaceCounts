## Officer Initiated Stops RC v7
## Set up ----------------------------------------------------------------

##install packages if not already installed 
packages <- c("tidyverse", "readr", "readxl", "DBI", "RPostgres", "sf", "data.table", "tidycensus", "stringr", "openxlsx", "usethis", "naniar", "tidygeocoder", "tigris", "plyr", "stringi")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen=999)

# Update each year
curr_yr <- '2022-2023'
yrs_list <- c("2022","2023")
acs_yr <- '2023'
rc_yr <- '2025'
rc_schema <- 'v7'
sldl_crosswalk <- 'place_2020_sldl_2024'
sldu_crosswalk <- 'place_2020_sldu_2024'

pop_threshold = 100
stop_threshold = 5  # update appropriately each year to ensure counties/cities with few stops and small pops do not result in outlier rates

qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Crime and Justice\\QA_Sheet_Officer_Initiated_Stops.docx"


# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")


# Before running rest of script, check RIPA race/eth codes. See: W:\Data\Crime and Justice\CA DOJ\RIPA Stop Data\RIPA Dataset Read Me 2023 - 20241112.pdf # --------


# Import Place-SLDL/SLDU Crosswalks ####
sldl_xwalk <- dbGetQuery(con2, paste0("SELECT geoid_sldl2024_20 AS geoid, namelsad_sldl2024_20 AS geoname, geoid_place_20 FROM crosswalks.", sldl_crosswalk))
sldu_xwalk <- dbGetQuery(con2, paste0("SELECT geoid_sldu2024_20 AS geoid, namelsad_sldu2024_20 AS geoname, geoid_place_20 FROM crosswalks.", sldu_crosswalk))


# Get RIPA data --------------------------------------------------------
# create list of statewide RIPA tables based on yrs_list defined above
table_names <- data.frame()

for (i in yrs_list){
  output = paste0("cadoj_ripa_", i)
  table_names = rbind(table_names, output)
  table_names <- table_names %>% dplyr::rename(name = 1)
}

table_list <- table_names[order(table_names$name), ]  # alphabetize list of table_names tables, needed to format list correctly for next step

# import RIPA data
ripa_orig <- lapply(setNames(paste0("select county, agency_name, call_for_service, rae_hispanic_latino, rae_full, rae_native_american, 
                                    rae_pacific_islander, rae_middle_eastern_south_asian from crime_and_justice.", table_list, " WHERE call_for_service = 0;"), table_list), DBI::dbGetQuery, conn = con2)

ripa_orig <- Map(cbind, ripa_orig, year = names(ripa_orig))                      # add year column, populated by table names
ripa_orig <- lapply(ripa_orig, transform, year=str_sub(year,-4,-1))              # update year column values to year only
ripa_orig <- lapply(ripa_orig, function(i) {i[] <- lapply(i, as.character); i})  # convert all cols to char
ripa_orig_ <- rbindlist(ripa_orig)   # convert list into df

# Clean RIPA data --------------------------------------------------------
# manual cleaning so that unique agency names to match to cities later
agency_names <- as.data.frame(unique(ripa_orig_$agency_name))
colnames(agency_names)[1] = "agency_name"
agency_names_ <- agency_names %>% mutate(agency_name_new = gsub(" PD", "", agency_name))  # clean police dept names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SO", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SD", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" COUNTY SO", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SHERIFF'S DEPT", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SHERIFF", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" COUNTY SHERIFF'S OFFICE", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" SHERIFF", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPARTMENT", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPARTME", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPAR", "", agency_name_new)) 
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPT", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DE", "", agency_name_new))
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" - DOC", "", agency_name_new))
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub("-COMM", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" - COMM", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO DA", " COUNTY", agency_name_new))  # clean County Dist Atty's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CA DA", " COUNTY", agency_name_new))  # clean County Dist Atty's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" - HOJ", "", agency_name_new))         # clean County Dist Atty's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO. DA INV.", " COUNTY", agency_name_new))  # clean County DA's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" COUNTY DA", " COUNTY", agency_name_new))  # clean County DA's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO MARSHAL", " COUNTY", agency_name_new))  # clean County Marshall's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO WELFARE", " COUNTY", agency_name_new))  # clean County Welfare's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO DEPT OF HUMAN ASST", " COUNTY", agency_name_new))  # clean County DHA's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO DA", " COUNTY", agency_name_new))  # clean County DA's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO FIRE - ARSON", " COUNTY", agency_name_new))  # clean County FD's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" COUNTY DISTRICT PARKS", " COUNTY", agency_name_new))  # clean County Park's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO DEPT PARKS", " COUNTY", agency_name_new))  # clean County Park's names


agency_names_ <- agency_names_ %>% mutate(agency_name_new = str_to_title(agency_names_$agency_name_new))

agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Lapd', "Los Angeles", agency_name_new))
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Atherton #1', "Atherton", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'S. San Francisco', "South San Francisco", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Angels Camp', "Angels", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Blytheunication', "Blythe", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Carmel', "Carmel-by-the-Sea", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Lindsay Department Of Public S', "Lindsay", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Mcfarland', "McFarland", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Mt. Shasta', "Mount Shasta", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Sunnyvale Dps', "Sunnyvale", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'Sunnyvale Dps', "Sunnyvale", agency_name_new)) 
agency_names_ <- agency_names_ %>% 
  mutate(agency_name_new = ifelse(agency_name_new == 'W. Sacramento', "West Sacramento", agency_name_new)) 

ripa_orig_ <- ripa_orig_ %>% mutate(county = ifelse(agency_name %like% 'CHP-HQ', 'CA Highway Patrol', county)) # Reassign CHP rows assigned to Sacramento since they are actually statewide

ripa_final <- ripa_orig_ %>% 
  left_join(agency_names_, by = "agency_name") # join cleaned agency names to ripa data

ripa_cfs <- ripa_final %>% mutate(state_id = '06')   # add state fips col

# Count number of data years each agency has data for
count_data_yrs <- ripa_cfs %>% group_by(agency_name_new) %>% dplyr::summarise(data_yrs = length(unique(year)))

# Join data years count to RIPA data
ripa_cfs <- ripa_cfs %>% left_join(count_data_yrs)

#### Calc State, County, Agency counts by race, weighted by ripa_cfs$data_yrs ####
source(".\\Functions\\crime_justice_functions.R")
state_calcs <- stops_by_race(ripa_cfs, state_id)
county_calcs <- stops_by_race(ripa_cfs, county) %>%  # CA Hwy Patrol is in this df, but is filtered out during pop join later (county_df)
  mutate(county = gsub(" County", "", county))
agency_calcs <- stops_by_race(ripa_cfs, agency_name_new)   # this df includes agencies at all levels: state, county, city, school district, etc.


#### Get pop data by race ####
pop <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_race_multigeo")) %>%
  mutate(name = gsub(", California", "",
                gsub(" County", "",
                gsub(" CDP", "",
                gsub(" city", "",
                gsub(" town", "", name)))))) %>%
  mutate(name = case_when(name == 'El Paso de Roblescity' ~ 'Paso Robles',
                          name == 'San Buenaventuracity' ~ 'Ventura',
                          TRUE ~ name)) %>%
  select(-c(contains(c("swana_", "nh_other_", "pacisl_", "aian_", "pct_"))))     # drop unneeded cols

nh_aian_pacisl <- dbGetQuery(con2, paste0("SELECT geoid, nh_aian_count AS nh_aian_pop, nh_nhpi_count as nh_pacisl_pop FROM demographics.acs_5yr_multigeo_", acs_yr, "_race_wide WHERE geolevel IN ('state', 'county', 'place')"))
pop <- pop %>% full_join(nh_aian_pacisl)     # join nh_pacisl + nh_aian pop to main pop df


#### Create separate state, county, city df's with RIPA and pop data ####
state_df <- pop %>% filter(geolevel == 'state') %>% left_join(state_calcs, by = c("geoid" = "state_id")) %>% dplyr::rename(geoname = name)
county_df <- pop %>% filter(geolevel == 'county') %>% left_join(county_calcs, by = c("name" = "county")) %>% dplyr::rename(geoname = name)
city_df <- pop %>% filter(geolevel == 'place') %>% left_join(agency_calcs, by = c("name" = "agency_name_new")) %>% dplyr::rename(geoname = name)

# copy SF County (Sheriff) data to SF City record since SF Sheriff serves SF City/County
sf_stops <- county_df %>% 
  filter(geoname == 'San Francisco') %>% 
  select(geoname, ends_with("_stops"))
city_df <- city_df %>% rows_update(sf_stops, by = c("geoname"))

# na_cities <- city_df %>% filter(is.na(total_stops)) # n = 1274, the cities on this list are covered by County Sheriff's that serve many cities/areas.


#### Calc Leg Dist stops by race ####
## Aggregate city stops to leg dist using city-leg xwalks. Note: a city can match to more than 1 district
sldu_df <- left_join(sldu_xwalk, city_df %>% filter(!is.na(total_stops)) %>% select(-c(geoname)), by=c("geoid_place_20"="geoid")) %>% 
  group_by(geoid, geoname) %>% 
  dplyr::summarise(across(where(is.numeric), sum, na.rm = TRUE)) %>% 
  mutate(geolevel="sldu") %>% 
  select(-c(ends_with("_pop"), geolevel, geoname))

sldl_df <- left_join(sldl_xwalk, city_df %>% filter(!is.na(total_stops)) %>% select(-c(geoname)), by=c("geoid_place_20"="geoid")) %>% 
  group_by(geoid, geoname) %>% 
  dplyr::summarise(across(where(is.numeric), sum, na.rm = TRUE)) %>% 
  mutate(geolevel="sldl") %>% 
  select(-c(ends_with("_pop"), geolevel, geoname))

### Manually assign some agency stop data (that doesn't match to cities) to leg districts ####
# identify additional agencies that can be assigned to leg districts
# city_pop <- pop %>% filter(geolevel == 'place')
# no_city_stops <- anti_join(agency_calcs, city_pop %>% filter(geolevel == 'place'), by = c("agency_name_new" = "name")) 
# no_city_stops <-  no_city_stops %>% filter(!endsWith(!!no_city_data$agency_name_new, !!"County"))  # n = 103
# write.xlsx(no_city_stops, 'W:\\Data\\Crime and Justice\\CA DOJ\\RIPA Stop Data\\2023\\agency_addresses.xlsx')

# import manually-looked up addresses for colleges/univ and a few other agencies that can be assigned to counties/leg dist.
agency_addresses <- read_excel('W:\\Data\\Crime and Justice\\CA DOJ\\RIPA Stop Data\\2023\\agency_addresses.xlsx') %>%
  filter(!is.na(Address))

agency_addresses <- agency_addresses %>% geocode(address = Address, method = "osm")
# convert to sf object
agency <- st_as_sf(agency_addresses, coords = c("long", "lat"), crs = 4326)
st_crs(agency)   # WGS84: 4326
agency_proj <- st_transform(agency, 3310)
#plot(agency)

# pull in sldl, sldu geos
sldl <- st_read(con2, query = "SELECT * FROM geographies_ca.cb_2023_06_sldl_500k")
#state_legislative_districts(state = 'CA', year = 2023, cb = TRUE, house = "lower")
st_crs(sldl)    # ESPG: 3310 NAD83
#plot(sldl)

sldu <- st_read(con2, query = "SELECT * FROM geographies_ca.cb_2023_06_sldu_500k")
#state_legislative_districts(state = 'CA', year = 2023, cb = TRUE, house = "upper")
st_crs(sldu)    # ESPG: 3310 NAD83
#plot(sldl)

# Intersect unassigned agencies with leg dists, then drop geometry
sldl_agency <- st_intersection(agency_proj, sldl) %>% st_drop_geometry()
sldu_agency <- st_intersection(agency_proj, sldu) %>% st_drop_geometry()

# Join RIPA data to unassigned agencies-leg dist xwalks
ripa_sldl_agency <- sldl_agency %>% 
  left_join(ripa_cfs, by = c("agency_name_new", "agency_name"))
  
ripa_sldu_agency <- sldu_agency %>% 
  left_join(ripa_cfs, by = c("agency_name_new", "agency_name"))

# Calc stops by race for agencies manually assigned to leg dist
sldl_manual <- stops_by_race(ripa_sldl_agency, geoid)

sldu_manual <- stops_by_race(ripa_sldu_agency, geoid)

## Join Leg Dist stops aggregated from city stops with manually assigned Leg Dist stops
sldl_df_ <- sldl_df %>% rbind.fill(sldl_manual) %>%
  group_by(geoid) %>%
  dplyr::summarise(across(everything(), sum, na.rm=TRUE)) %>%
  left_join(pop %>% filter(geolevel == 'sldl')) %>%
  dplyr::rename('geoname' = 'name')

sldu_df_ <- sldu_df %>% rbind.fill(sldu_manual) %>%
  group_by(geoid) %>%
  dplyr::summarise(across(everything(), sum, na.rm=TRUE)) %>%
  left_join(pop %>% filter(geolevel == 'sldu')) %>%
  dplyr::rename('geoname' = 'name')

# Data screening  --------------------------------------------------------

# combine sldu, sldl, city, county, state data
all_df <- rbind(state_df, county_df, city_df, sldu_df_, sldl_df_)

df_screened <- all_df %>% mutate(
  # screen raw counts
  total_raw = ifelse(total_pop < pop_threshold | total_stops< stop_threshold, NA, total_stops),
  latino_raw = ifelse(latino_pop < pop_threshold | latino_stops< stop_threshold, NA, latino_stops),
  nh_white_raw = ifelse(nh_white_pop < pop_threshold | nh_white_stops < stop_threshold, NA, nh_white_stops),
  nh_black_raw = ifelse(nh_black_pop < pop_threshold | nh_black_stops < stop_threshold, NA, nh_black_stops),
  nh_asian_raw = ifelse(nh_asian_pop < pop_threshold | nh_asian_stops < stop_threshold, NA, nh_asian_stops),
  nh_twoormor_raw = ifelse(nh_twoormor_pop < pop_threshold | nh_twoormor_stops < stop_threshold, NA, nh_twoormor_stops),
  nh_aian_raw = ifelse( nh_aian_pop < pop_threshold |  nh_aian_stops < stop_threshold, NA,  nh_aian_stops),
  nh_pacisl_raw = ifelse(nh_pacisl_pop < pop_threshold | nh_pacisl_stops < stop_threshold, NA, nh_pacisl_stops),
  swanasa_raw = ifelse(swanasa_pop < pop_threshold | swanasa_stops < stop_threshold, NA, swanasa_stops),
  
  # calc rates using annual avg (weighted) stops
  total_rate = ifelse(total_pop < pop_threshold | total_stops< stop_threshold, NA, total_stops_wt/total_pop) * 1000,
  latino_rate = ifelse(latino_pop < pop_threshold | latino_stops< stop_threshold, NA, latino_stops_wt/latino_pop) * 1000,
  nh_white_rate = ifelse(nh_white_pop < pop_threshold | nh_white_stops < stop_threshold, NA, nh_white_stops_wt/nh_white_pop) * 1000,
  nh_black_rate = ifelse(nh_black_pop < pop_threshold | nh_black_stops < stop_threshold, NA, nh_black_stops_wt/nh_black_pop) * 1000,
  nh_asian_rate = ifelse(nh_asian_pop < pop_threshold | nh_asian_stops < stop_threshold, NA, nh_asian_stops_wt/nh_asian_pop) * 1000,
  nh_twoormor_rate = ifelse(nh_twoormor_pop < pop_threshold | nh_twoormor_stops < stop_threshold, NA, nh_twoormor_stops_wt/nh_twoormor_pop) * 1000,
  nh_aian_rate = ifelse( nh_aian_pop < pop_threshold |  nh_aian_stops < stop_threshold, NA,  nh_aian_stops_wt/nh_aian_pop) * 1000,
  nh_pacisl_rate = ifelse(nh_pacisl_pop < pop_threshold | nh_pacisl_stops < stop_threshold, NA, nh_pacisl_stops_wt/nh_pacisl_pop) * 1000,
  swanasa_rate = ifelse(swanasa_pop < pop_threshold | swanasa_stops < stop_threshold, NA, swanasa_stops_wt/swanasa_pop) * 1000
)

d <- df_screened %>% select(-c(contains(c("_stops")), starts_with("nh_twoormor"))) # drop multiracial bc of mismatch with pop denominator

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming con2ventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'. 

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns. Drop unneeded cols.
state_table <- d[d$geoname == 'California', ] %>% select(-c(geolevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
View(state_table)

#split COUNTY into separate table and format id, name columns. Drop unneeded cols.
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'place', ] %>% select(-c(geolevel))

#calculate city z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
View(city_table)

#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
#View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
#View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid / geoname
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")

###update info for postgres tables will automatically update###
county_table_name <- paste0("arei_crim_officer_initiated_stops_county_", rc_yr)
state_table_name <- paste0("arei_crim_officer_initiated_stops_state_", rc_yr)
city_table_name <- paste0("arei_crim_officer_initiated_stops_city_", rc_yr)
leg_table_name <- paste0("arei_crim_officer_initiated_stops_leg_", rc_yr)

indicator <- "Officer initiated stops per 1,000 people. Raw is multi-year total number of officer initiated stops. Rate is annual average rate. Note: City data is based only on the largest agency in that city. Leg data is aggregated from city data. In addition, stops are assigned to the geography where the law enforcement agency is located, not where the stop occurred. Population is NOT pop for entire Leg District, it is sum of pop for cities assigned to the district that have stop data. This data is"
source <- paste0("CADOJ RIPA ",curr_yr, " https://openjustice.doj.ca.gov/data")

# #send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)
# 
# dbDisconnect(con)
# dbDisconnect(con2)








