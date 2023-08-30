### Suspension for RC v5 ###

#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(readr)
library(dplyr)
library(tidyr)
library(DBI)
library(RPostgreSQL)
library(tidycensus)
library(sf)
library(tidyverse) # to scrape metadata table from cde website
#library(rvest) # to scrape metadata table from cde website
library(stringr) # cleaning up data
library(usethis) # connect to github

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

############### PREP DATA ########################

## Get Suspensions
# filepath = "https://www3.cde.ca.gov/demo-downloads/discipline/suspension22-v2.txt"   # will need to update each year
# fieldtype = 1:11 # specify which cols should be varchar, the rest will be assigned numeric
# 
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "education"
# table_name <- "cde_multigeo_calpads_suspensions_2021_22"
# table_comment_source <- "NOTE: Only use suspension data from this link, https://www.cde.ca.gov/ds/ad/filessd.asp. The Dashboard download is incomplete and lacks data for most high schools (at least within LAUSD). Wide data format, multigeo table with state, county, district, and school"
# table_source <- "Wide data format, multigeo table with state, county, district, and school"
# 
# ## Run function to prep and export rda_shared_data table 
# source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
# df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db
# View(df)

df <- st_read(con, query = "SELECT * FROM education.cde_multigeo_calpads_suspensions_2021_22") # comment out code to pull data and use this once rda_shared_data table is created

###### NOTE: This function isn't working for Suspensions (loop part of function is the issue).
## Run function to add rda_shared_data column comments
# See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
# url <-  "https://www.cde.ca.gov/ds/ad/fssd.asp"   # define webpage with metadata
# html_nodes <- "table"
# colcomments <- get_cde_metadata(url, table_schema, table_name)
# View(colcomments)


#### Continue prep for RC ####

#filter for county and state rows, all types of schools, and racial categories only
df_subset <- df %>% filter(aggregatelevel %in% c("C", "T", "D") & charteryn == "All" & 
                             reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%

    #select just fields we need
     select(cdscode, aggregatelevel, countycode, districtcode, countyname, districtname, reportingcategory, unduplicatedcountofstudentssuspendedtotal, cumulativeenrollment, suspensionratetotal)

#format for column headers
df_subset <- rename(df_subset, 
                    raw = "unduplicatedcountofstudentssuspendedtotal",
                    pop = "cumulativeenrollment",
                    rate = "suspensionratetotal")
#rename race categories
df_subset$reportingcategory <- gsub("TA", "total", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RB", "nh_black", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RI", "nh_aian", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RA", "nh_asian", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RF", "nh_filipino", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RH", "latino", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RP", "nh_pacisl", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RT", "nh_twoormor", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RW", "nh_white", df_subset$reportingcategory)

#pivot wide
df_wide <- df_subset %>% pivot_wider(names_from = reportingcategory, names_glue = "{reportingcategory}_{.value}", 
                                     values_from = c(raw, pop, rate)) %>% rename( c("geoname" = "countyname"))
df_wide$geoname[df_wide$geoname =='State'] <- 'California'   # update state rows' geoname field values


#get county geoids
census_api_key(census_key1, overwrite = TRUE)

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2021)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
View(ca)

#add county geoids
df_wide <- merge(x=ca,y=df_wide,by="geoname", all=T)
#add state geoid
df_wide <- within(df_wide, geoid[geoname == 'California'] <- '06')
View(df_wide)

# get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
districts <- st_read(con, query = "SELECT cdscode, ncesdist AS geoid FROM education.cde_public_schools_2021_22 WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'") # district,

df_final <- 
  left_join(df_wide, districts, by = c('cdscode')) %>% 
  dplyr::rename("geoid" = "geoid.x") %>%
  mutate(geoid=ifelse(aggregatelevel == "D", geoid.y, geoid),
         geoname=ifelse(!is.na(districtname), districtname, geoname)) %>% 
  select(-c(geoid.y, districtname, countycode, districtcode)) %>% 
  distinct() %>%  # combine distinct county and district geoid matched df's
  relocate(geoid, geoname, cdscode, aggregatelevel)

df_final <- filter(df_final, !is.na(geoid)) # remove records without fips codes
View(df_final)

d <- df_final


####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ] %>% select(-c(cdscode, aggregatelevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
View(state_table)

#remove state from county table
county_table <- d[d$aggregatelevel == 'C', ] %>% select(-c(cdscode, aggregatelevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
View(county_table)

#split CITY into separate table
city_table <- d[d$aggregatelevel == 'D', ] %>% select(-c(aggregatelevel))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname") %>% relocate(cdscode, .after = dist_id) 
View(city_table)

###update info for postgres tables###
county_table_name <- "arei_educ_suspension_county_2023"
state_table_name <- "arei_educ_suspension_state_2023"
city_table_name <- "arei_educ_suspension_district_2023"
rc_schema <- "v5"

indicator <- "Unduplicated students suspended, cumulative enrollment, and unduplicated suspension rate. This data is"
source <- "CDE 2021-22 https://www.cde.ca.gov/ds/ad/filessd.asp"

#send tables to postgres
# to_postgres(county_table,state_table)
# city_to_postgres()
