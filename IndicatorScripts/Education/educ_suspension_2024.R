### Suspension for RC v6 ###

#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgres","tidycensus", "rvest", "stringr", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(readr)
library(dplyr) # to scrape metadata table from cde website
library(httr2) # to scrape metadata table from cde website
library(tidyr)
library(DBI)
library(RPostgres)
library(tidycensus)
library(rvest) # to scrape metadata table from cde website
library(stringr) # cleaning up data
library(usethis) # connect to github

source("W:\\RDA Team\\R\\credentials_source.R") # connect_to_db()
source("./Functions/rdashared_functions.R") # get_cde_data(), get_cde_metadata()

# create connection for rda database
con <- connect_to_db("rda_shared_data")

##### update each year #####
curr_yr <- '2023_24' 
acs_yr <- 2023
rc_yr <- '2025'
rc_schema <- 'v7'

# To get Suspensions Data from CDE website
filepath = "https://www3.cde.ca.gov/demo-downloads/discipline/suspension24.txt"   # will need to update each year
fieldtype = 1:11 # specify which cols should be varchar, the rest will be assigned numeric
url <-  "https://www.cde.ca.gov/ds/ad/fssd.asp"   # define webpage with metadata
exclude_cols <- c("Errata Flag (Y/N)") # list of metadata columns excluded from export to postgres
html_element <- "table" # html element name where metadata is located
 
# Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "education"
table_name <- paste0("cde_multigeo_calpads_suspensions_", curr_yr)
table_comment_source <- paste("NOTE: Only use suspension data from this link: ", url, ". The Dashboard download is incomplete and lacks data for most high schools (at least within LAUSD). Wide data format, multigeo table with state, county, district, and school")
table_source <- "Wide data format, multigeo table with state, county, district, and school"

##### Prep and export indicator data rda_shared_data table with metadata #####
## function to create and export rda_shared_table to postgres db
# df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) 

## Run function to add rda_shared_data column comments
# colcomments <- get_cde_metadata(url, html_element, table_schema, table_name, exclude_cols)
# View(colcomments)

##### get county geoids-----
census_api_key(census_key1, overwrite = TRUE)
Sys.getenv("CENSUS_API_KEY") # confirms value saved to .renviron
counties <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = acs_yr) %>%
  select(GEOID, NAME) %>%
  mutate(NAME=gsub(" County, California", "", NAME, fixed=TRUE)) %>%
  rename(geoid=GEOID,
         geoname=NAME)

#### Continue prep for RC ####
# comment out code to pull data and use this once rda_shared_data table is created
df <- dbGetQuery(con, statement = paste0("SELECT * FROM ", table_schema, ".", table_name,";")) 

#filter for county and state rows, all types of schools, and racial categories only
df_subset <- df %>% 
  filter(aggregatelevel %in% c("C", "T", "D") & 
           charteryn == "All" & 
           reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%
  # select needed fields
  select(cdscode, aggregatelevel, countycode, districtcode, countyname, 
         districtname, reportingcategory, unduplicatedcountofstudentssuspendedtotal, 
         cumulativeenrollment, suspensionratetotal) %>%
  # rename for standard RC columns names
  rename(
    raw = unduplicatedcountofstudentssuspendedtotal, 
    pop = cumulativeenrollment, 
    rate = suspensionratetotal) %>%
  #rename race categories
  mutate(reportingcategory=case_when(
    reportingcategory=="TA"~"total",
    reportingcategory=="RB"~"nh_black",
    reportingcategory=="RI"~"nh_aian",
    reportingcategory=="RA"~"nh_asian",
    reportingcategory=="RF"~"nh_filipino",
    reportingcategory=="RH"~"latino",
    reportingcategory=="RP"~"nh_pacisl",
    reportingcategory=="RT"~"nh_twoormor",
    reportingcategory=="RW"~"nh_white",
    .default = reportingcategory
  ))

# pivot wider
df_wide <- df_subset %>% 
  pivot_wider(names_from = reportingcategory, 
              names_glue = "{reportingcategory}_{.value}", 
              values_from = c(raw, pop, rate)) %>% 
  rename(geoname=countyname) %>%
  # get county geoids
  left_join(counties, by="geoname") %>%
  # rename state to California and add geoid
  mutate(geoname=ifelse(geoname=="State", "California", geoname)) %>%
  within(geoid[geoname == 'California'] <- '06') 

# get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
districts <- dbGetQuery(con, statement = 
                          paste0("SELECT cdscode, ncesdist AS geoid FROM education.cde_public_schools_", 
                                 curr_yr, 
                                 " WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'")) 

df_final <- df_wide %>%
  left_join(districts, by = "cdscode") %>% 
  rename(geoid = geoid.x) %>%
  mutate(
    geoid=ifelse(aggregatelevel == "D", geoid.y, geoid), 
    geoname=ifelse(!is.na(districtname), districtname, geoname)) %>% 
  select(-c(geoid.y, districtname, countycode, districtcode)) %>% 
  # add geolevel for RC calc functions
  mutate(geolevel = case_when(
    aggregatelevel=="T"~"state",
    aggregatelevel=='C'~"county",
    aggregatelevel=="D"~"place",
    .default = aggregatelevel
  )) %>%
  relocate(geoid, geoname, cdscode, aggregatelevel, geolevel) %>%
  # remove records without fips codes
  filter(!is.na(geoid) & geoid != "No Data") %>%
  # combine distinct county and district geoid matched df's
  distinct()
 
# View(df_final)

d <- df_final

####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

# Assign Suspensions asbest to 'min' (i.e., fewer suspensions is best)
d$asbest = 'min'    

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
# View(d)

#split STATE into separate table and format id, name columns
state_table <- d %>%
  filter(geolevel=="state") %>%
  select(-c(cdscode, aggregatelevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
# View(state_table)

#remove state from county table
county_table <- d %>%
  filter(geolevel=="county") %>% 
  select(-c(cdscode, aggregatelevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
# View(county_table)

#split CITY into separate table
city_table <- d %>%
  filter(geolevel=="place") %>% 
  select(-c(aggregatelevel))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname") %>% relocate(cdscode, .after = dist_id) 
# View(city_table)

###update info for postgres tables###
county_table_name <- paste0("arei_educ_suspension_county_", rc_yr)
state_table_name <- paste0("arei_educ_suspension_state_", rc_yr)
city_table_name <- paste0("arei_educ_suspension_district_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Unduplicated students suspended, cumulative enrollment, and unduplicated suspension rate. This data is")
source <- paste("CDE", curr_yr, url)

#send tables to postgres
# to_postgres(county_table,state_table)
# city_to_postgres()
# dbDisconnect(con)
