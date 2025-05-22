## Officer Initiated Stops RC v7
## Set up ----------------------------------------------------------------

##install packages if not already installed ------------------------------
packages <- c("tidyverse", "readr", "readxl", "DBI", "RPostgres", "sf", "data.table", "tidycensus", "stringr", "openxlsx", "usethis")  

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
curr_yr <- '2023'
rc_yr <- '2025'
rc_schema <- 'v7'

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Crime and Justice\\QA_Sheet_Officer_Initiated_Stops.docx"

# Before running rest of script, ensure that RIPA race/eth codes. See: W:\Data\Crime and Justice\CA DOJ\RIPA Stop Data\RIPA Dataset Read Me 2023 - 20241112.pdf # --------

##### Begin RACE COUNTS prep #
# Import RIPA postgres table --------------------------------------------------------
ripa_orig <- dbGetQuery(con2, "SELECT county, agency_name, call_for_service, rae_hispanic_latino, rae_full, rae_native_american, 
                                  rae_pacific_islander, rae_middle_eastern_south_asian FROM crime_and_justice.cadoj_ripa_2023 WHERE call_for_service = 0;") 

# manual cleaning so that unique agency names to match to cities later
agency_names <- as.data.frame(unique(ripa_orig$agency_name))
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
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub("-COMM", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" - COMM", "", agency_name_new))  

agency_names_ <- agency_names_ %>% mutate(agency_name_new = str_to_title(agency_names_$agency_name_new))
agency_names_ <- agency_names_  %>% mutate(agency_name_new = 
                                                ifelse(agency_name_new == 'Lapd', "Los Angeles", agency_name_new))
agency_names_ <- agency_names_  %>% mutate(agency_name_new = 
                                                ifelse(agency_name_new == 'Atherton #1', "Atherton", agency_name_new)) 

ripa_final <- ripa_orig %>% left_join(agency_names_, by = "agency_name") # join cleaned agency names to ripa data

# filter data for officer-initiated stops by race/eth --------------------------------------------------------
ripa_cfs <- ripa_final %>% mutate(state_id = '06')  

#### Calc counts by race ####
source(".\\Functions\\crime_justice_functions.R")
state_calcs <- stops_by_state(ripa_cfs)
county_calcs <- stops_by_county(ripa_cfs) %>% mutate(county = gsub(" County", "", county))
agency_calcs <- stops_by_agency(ripa_cfs) # this df includes agencies at all levels: state, county, city, school district, etc.


# get pop data by race
pop <- dbGetQuery(con, "SELECT * FROM v6.arei_race_multigeo") %>% mutate(name = gsub(" County, California", "", name),
                                                                          name =  gsub(" CDP, California", "", name),
                                                                          name =  gsub(" city, California", "", name),
                                                                          name =  gsub(" town, California", "", name),
                                                                          name =  gsub(", California", "", name)) %>% 
                                                                   select(-c(contains(c("swana_", "nh_other_", "pct_"))))

nh_aian_pacisl <- dbGetQuery(con2, "SELECT geoid, dp05_0081e AS nh_aian_pop, dp05_0083e as nh_pacisl_pop FROM demographics.acs_5yr_dp05_multigeo_2023 WHERE geolevel IN ('state', 'county', 'place')")
pop <- pop %>% full_join(nh_aian_pacisl)


state_df <- pop %>% filter(geolevel == 'state') %>% left_join(state_calcs, by = c("geoid" = "state_id"))
county_df <- pop %>% filter(geolevel == 'county') %>% left_join(county_calcs, by = c("name" = "county"))
city_df <- pop %>% filter(geolevel == 'place') %>% left_join(agency_calcs, by = c("name" = "agency_name_new"))
# na_cities <- city_df %>% filter(is.na(total_stops)) # n = 1281, the cities on this list are covered by County Sheriff's that serve many cities/areas, with exception of SF.

sldu_xwalk <- read_delim("W:\\Data\\Geographies\\Relationships\\place20_sldu24\\tab20_sldu202420_place20_st06.txt", delim = "|", trim_ws = TRUE, show_col_types = FALSE)
sldl_xwalk <- read_delim("W:\\Data\\Geographies\\Relationships\\place20_sldl24\\tab20_sldl202420_place20_st06.txt", delim = "|", trim_ws = TRUE, show_col_types = FALSE)

# ## push census crosswalk tables to rda_shared_data db
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "geographies_ca"
# table_name <- "tab20_sldu202420_place20_st06"
# indicator <- "California State level 2024 SLDU To 2020 Place Relationship Files"       
# source <- "The data is from the US Census https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2020.html#place" 
# dbWriteTable(con2, 
#              Id(schema = table_schema, table = table_name), 
#              sldu_xwalk, overwrite = FALSE)
# # comment on table and columns
# column_names <- colnames(sldu_xwalk)
# column_comments <- c("OID of SLDU", "GEOID of the SLDU", "Name with translated Legal/Statistical Area Description of SLDU", "Total land area of SLDU in square meters",
#                      "Total water area of SLDU in square meters", "MAFTIGER feature class code of SLDU", "Functional status of SLDU", "OID of 2020 place",
#                      "GEOID of the 2020 place", "Name with translated Legal/Statistical Area Description of 2020 place", "Land area of 2020 place in square meters", 
#                      "Water area of 2020 place in square meters", "MAFTIGER feature class code of 2020 place", "FIPS class code of 2020 place", 
#                      "Functional status of 2020 place", "Calculated land area of the overlapping part in square meters", "Calculated water area of the overlapping part in square meters")  
# add_table_comments(con2, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
# 
# table_name <- "tab20_sldl202420_place20_st06"
# indicator <- "California State level 2024 SLDL To 2020 Place Relationship Files"       
# source <- "The data is from the US Census https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2020.html#place" 
# dbWriteTable(con2, 
#              Id(schema = table_schema, table = table_name), 
#              sldl_xwalk, overwrite = FALSE)
# # comment on table and columns
# column_names <- colnames(sldl_xwalk)
# column_comments <- c("OID of SLDL", "GEOID of the SLDL", "Name with translated Legal/Statistical Area Description of SLDL", "Total land area of SLDL in square meters",
#                      "Total water area of SLDL in square meters", "MAFTIGER feature class code of SLDL", "Functional status of SLDL", "OID of 2020 place",
#                      "GEOID of the 2020 place", "Name with translated Legal/Statistical Area Description of 2020 place", "Land area of 2020 place in square meters", 
#                      "Water area of 2020 place in square meters", "MAFTIGER feature class code of 2020 place", "FIPS class code of 2020 place", 
#                      "Functional status of 2020 place", "Calculated land area of the overlapping part in square meters", "Calculated water area of the overlapping part in square meters")  
# 
# add_table_comments(con2, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
#########################################################

sldu_df <- left_join(sldu_xwalk, city_df, by=c("GEOID_PLACE_20"="geoid")) %>% select(-c("OID_SLDU2024_20", "AREALAND_SLDU2024_20",  "AREAWATER_SLDU2024_20", "MTFCC_SLDU2024_20", "FUNCSTAT_SLDU2024_20", 
                                                                                        "OID_PLACE_20", "GEOID_PLACE_20", "NAMELSAD_PLACE_20", "AREALAND_PLACE_20", "AREAWATER_PLACE_20", "MTFCC_PLACE_20", "CLASSFP_PLACE_20",     
                                                                                        "FUNCSTAT_PLACE_20","AREALAND_PART",  "AREAWATER_PART" )) %>%  
  group_by(GEOID_SLDU2024_20, NAMELSAD_SLDU2024_20) %>% 
  summarise(across(where(is.numeric), sum, na.rm = TRUE))%>% 
  rename("geoid"="GEOID_SLDU2024_20", "name"="NAMELSAD_SLDU2024_20") %>% mutate(geolevel="sldu")
sldl_df <- left_join(sldl_xwalk, city_df, by=c("GEOID_PLACE_20"="geoid")) %>% select(-c("OID_SLDL2024_20", "AREALAND_SLDL2024_20",  "AREAWATER_SLDL2024_20", "MTFCC_SLDL2024_20", "FUNCSTAT_SLDL2024_20", 
                                                                                        "OID_PLACE_20", "GEOID_PLACE_20", "NAMELSAD_PLACE_20", "AREALAND_PLACE_20", "AREAWATER_PLACE_20", "MTFCC_PLACE_20", "CLASSFP_PLACE_20",     
                                                                                        "FUNCSTAT_PLACE_20","AREALAND_PART",  "AREAWATER_PART" )) %>%  
  group_by(GEOID_SLDL2024_20, NAMELSAD_SLDL2024_20) %>% 
  summarise(across(where(is.numeric), sum, na.rm = TRUE)) %>% 
  rename("geoid"="GEOID_SLDL2024_20", "name"="NAMELSAD_SLDL2024_20") %>%  mutate(geolevel="sldl")
# combine sldu, sldl, city, county, state data
all_df <- rbind(state_df, county_df, city_df, sldu_df, sldl_df) %>% rename(geoname = name)

# copy SF County (Sheriff) data to SF City record since SF Sheriff primarily serves SF City
sf_stops <- all_df %>% filter(geoname == 'San Francisco' & geolevel == 'county') %>% select(geoname, ends_with("_stops"))
all_df <- all_df %>% rows_update(sf_stops, by = "geoname")

# copy 	S. San Francisco PD data to South San Francisco record
so_sf_stops <- agency_calcs %>% filter(agency_name_new=='S. San Francisco') %>% mutate(geoname="South San Francisco")%>%select(geoname, ends_with("_stops"))
all_df <- all_df %>% rows_update(so_sf_stops, by = "geoname")

# Data screening  --------------------------------------------------------
pop_threshold = 100
stop_threshold = 5  # update appropriately each year to ensure counties/cities with few stops and small pops do not result in outlier rates

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
                aian_raw = ifelse(aian_pop < pop_threshold |  aian_stops < stop_threshold, NA,  aian_stops),
                pacisl_raw = ifelse(pacisl_pop < pop_threshold | pacisl_stops < stop_threshold, NA, pacisl_stops),
                swanasa_raw = ifelse(swanasa_pop < pop_threshold | swanasa_stops < stop_threshold, NA, swanasa_stops),
                
                # calc rates
                total_rate = (total_raw/total_pop) * 1000,
                latino_rate = (latino_raw/latino_pop) * 1000,
                nh_white_rate = (nh_white_raw/nh_white_pop) * 1000,
                nh_black_rate = (nh_black_raw/nh_black_pop) * 1000,
                nh_asian_rate = (nh_asian_raw/nh_asian_pop) * 1000,
                nh_twoormor_rate = (nh_twoormor_raw/nh_twoormor_pop) * 1000,
                nh_aian_rate = (nh_aian_raw/nh_aian_pop) * 1000,
                nh_pacisl_rate = (nh_pacisl_raw/nh_pacisl_pop) * 1000,
                aian_rate = (aian_raw/aian_pop) * 1000,
                pacisl_rate = (pacisl_raw/pacisl_pop) * 1000,
                swanasa_rate = (swanasa_raw/swanasa_pop) * 1000
)

d <- df_screened %>% select(-c(contains(c("_stops")), starts_with(c("nh_twoormor","aian_","pacisl")))) # drop multiracial bc of mismatch with pop denominator and drop aian and nhpi alone or in combo

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
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#split COUNTY into separate table and format id, name columns. Drop unneeded cols.
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'place', ] %>% select(-c(geolevel))

#calculate city z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)

#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")

###update info for postgres tables will automatically update###
county_table_name <- paste0("arei_crim_officer_initiated_stops_county_", rc_yr)
state_table_name <- paste0("arei_crim_officer_initiated_stops_state_", rc_yr)
city_table_name <- paste0("arei_crim_officer_initiated_stops_city_", rc_yr)
leg_table_name <- paste0("arei_crim_officer_initiated_stops_leg_", rc_yr)

indicator <- "Officer initiated stops per 1,000 people. Raw is total number of officer initiated stops. Note: City data is based only on the largest agency in that city. In addition, stops are assigned to the geography where the law enforcement agency is located, not where the stop occurred. This data is"
source <- paste0("CADOJ RIPA ",curr_yr, " https://openjustice.doj.ca.gov/data")

# #send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)
# 
# dbDisconnect(con)
# dbDisconnect(con2)








