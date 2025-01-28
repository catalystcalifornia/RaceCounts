### Chronic Absenteeism RC v6 ### 

#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#library(readr)
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

# update each year
curr_yr <- '2023_24' # CDE data year, must keep this format
rc_yr <- '2025'    # RC year
rc_schema <- 'v7'

############### PREP RDA_SHARED_DATA TABLE ########################

## Get Chronic Absenteeism
  filepath = "https://www3.cde.ca.gov/demo-downloads/attendance/chronicabsenteeism24.txt"   # will need to update each year
  fieldtype = 1:12 # specify which cols should be varchar, the rest will be assigned numeric in table export

## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
     table_schema <- "education"
     table_name <- "cde_multigeo_chronicabs_2023_24"
     table_comment_source <- "NOTE: Only use chronic absenteeism data from this link. The Dashboard download is incomplete and lacks data for most high schools (at least within LAUSD).
     Chronic absenteeism data downloaded from https://www.cde.ca.gov/ds/ad/filesabd.asp"
     table_source <- "Wide data format, multigeo table with state, county, district, and school"

## Run function to prep and export rda_shared_data table
  source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
  df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db
  View(df)

## Run function to add rda_shared_data column comments
# See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
url <-  "https://www.cde.ca.gov/ds/ad/fsabd.asp"   # define webpage with metadata
html_nodes <- "table"
colcomments <- get_cde_metadata(url, html_nodes, table_schema, table_name)
View(colcomments)

df <- st_read(con, query = "SELECT * FROM education.cde_multigeo_chronicabs_2023_24") # comment out code to pull data and use this once rda_shared_data table is created

#### Continue prep for RC ####

#filter for county and state rows, all types of schools, and racial categories
df_subset <- df %>% filter(aggregatelevel %in% c("C", "T", "D") & charterschool == "All" & dass == "All" &
                             reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%
  
  #select just fields we need
  select(cdscode, countyname, districtname, aggregatelevel, reportingcategory, chronicabsenteeismeligiblecumulativeenrollment, chronicabsenteeismcount, chronicabsenteeismrate)

#format for column headers
df_subset <- rename(df_subset, 
                    raw = "chronicabsenteeismcount",
                    pop = "chronicabsenteeismeligiblecumulativeenrollment",
                    rate = "chronicabsenteeismrate")

df_subset$reportingcategory <- gsub("TA", "total", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RB", "nh_black", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RI", "nh_aian", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RA", "nh_asian", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RF", "nh_filipino", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RH", "latino", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RP", "nh_pacisl", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RT", "nh_twoormor", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RW", "nh_white", df_subset$reportingcategory)

#pop screen on number of chronically absent students (raw)
threshold <- 20
df_subset <- df_subset %>% mutate(raw = ifelse(raw < threshold, NA, raw)) %>%
  mutate(rate = ifelse(raw < threshold, NA, rate))
# View(df_subset)

#pivot
df_wide <- df_subset %>% pivot_wider(names_from = reportingcategory, names_glue = "{reportingcategory}_{.value}", 
                                     values_from = c(raw, pop, rate)) 


####### GET COUNTY & SCHOOL DISTRICT GEOIDS ##### ---------------------------------------------------------------------

# county geoids
counties <- get_acs(geography = "county",
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = 2023)

counties <- counties[,1:2]
counties$NAME <- gsub(" County, California", "", counties$NAME) 
names(counties) <- c("geoid", "geoname")
county_match <- filter(df_wide,aggregatelevel=="C") %>% right_join(counties,by=c('countyname'='geoname'))

# get school district geoids - pull in active district records w/ geoids from CDE schools' list (NCES District ID)
districts <- st_read(con, query = "SELECT cdscode, ncesdist AS geoid FROM education.cde_public_schools_2023_24 WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'")
district_match <- filter(df_wide,aggregatelevel=="D") %>% right_join(districts,by='cdscode')

matched <- union(county_match, district_match) %>% select(c(cdscode, geoid)) # combine county and district geoid match df's back together
df_final <- df_wide %>% full_join(matched, by='cdscode')
df_final <- df_final %>% relocate(geoid) %>% mutate(countyname = ifelse(aggregatelevel == "T", "California", countyname), # add geoname and geoid for state
                                                    geoid = ifelse(aggregatelevel == "T", "06", geoid)) 
df_final <- filter(df_final, !is.na(geoid)) # remove records without fips codes
df_final <- rename(df_final, geoname = countyname)

d <- df_final


####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update d$asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_id" = "geoid", "state_name" = "geoname") %>% select(-c(districtname, cdscode, aggregatelevel))
View(state_table)

#remove state from county table
county_table <- d[d$aggregatelevel == 'C', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_id" = "geoid", "county_name" = "geoname") %>% select(-c(districtname, cdscode, aggregatelevel))
View(county_table)

#remove county/state from place table
city_table <- d[d$aggregatelevel == 'D', ] %>% select(-c(aggregatelevel)) 

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "districtname", "county_name" = "geoname") %>% relocate(county_name, .after = district_name)
View(city_table)

###update info for postgres tables###
county_table_name <- paste0("arei_educ_chronic_absenteeism_county_", rc_yr)
state_table_name <- paste0("arei_educ_chronic_absenteeism_state_", rc_yr)
city_table_name <- paste0("arei_educ_chronic_absenteeism_district_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Chronic Absenteeism Eligible Cumulative Enrollment, Chronic Absenteeism Count, and Chronic Absenteeism Rate. This data is")

source <- paste0("CDE ", curr_yr, " https://www.cde.ca.gov/ds/ad/filesabd.asp")

#send tables to postgres
to_postgres(county_table,state_table)
city_to_postgres()

