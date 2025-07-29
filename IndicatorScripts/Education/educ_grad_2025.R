### HS Graduation RC v7 ### 

# install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# Load Libraries
library(readr)
library(dplyr)
library(tidyr)
library(DBI)
library(RPostgreSQL)
library(tidycensus)
library(sf)
library(tidyverse) # to scrape metadata table from cde website
library(rvest) # to scrape metadata table from cde website
library(stringr) # cleaning up data
library(usethis)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# update each year
curr_yr <- '2023_24' 
rc_yr <- '2025'
rc_schema <- 'v7'

############### PREP RDA_SHARED_DATA TABLE ########################

# #Get HS Grad, handle nas, ensure DistrictCode reads in right
# # Data Dictionary: https://www.cde.ca.gov/ds/ad/fsacgr.asp
# filepath = "https://www3.cde.ca.gov/demo-downloads/acgr/acgr24.txt"
# fieldtype = 1:12 # specify which cols should be varchar, the rest will be assigned numeric
# 
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "education"
# table_name <- paste0("cde_multigeo_calpads_graduation_",curr_yr)
# table_comment_source <- "NOTE: This data is not trendable with data from before 2016-17. See more here: https://www.cde.ca.gov/ds/sd/sd/acgrinfo.asp"
# table_source <- "Downloaded from https://www.cde.ca.gov/ds/ad/filesacgr.asp. Headers were cleaned of characters like /, ., ), and (. Cells with values of * were nullified. Created cdscode by concatenating county, district, and school codes"
# 
# ## Run function to prep and export rda_shared_data table
# # source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
# source("W:/Project/RACE COUNTS/2025_v7/RC_Github/CR/Functions/RC_Functions.R")
# df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db
# # View(df)
# 
# ## Run function to add rda_shared_data column comments
# ## See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
# url <-  "https://www.cde.ca.gov/ds/ad/fsacgr.asp"   # define webpage with metadata
# html_nodes <- "table"
# colcomments <- get_cde_metadata(url, html_nodes, table_schema, table_name)
# View(colcomments)

# Get County GEOIDS --------------------------------------------------------------------
census_api_key(census_key1, install = TRUE, overwrite = TRUE)
Sys.getenv("CENSUS_API_KEY") # confirms value saved to .renviron

counties <- get_acs(geography = "county", 
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = 2023)

counties <- counties[,1:2]
counties$NAME <- gsub(" County, California", "", counties$NAME)
names(counties) <- c("geoid", "geoname")

###### High School Graduation Data: Prep for RC Functions #########
# comment out code to pull data and use this once rda_shared_data table is created
df <- st_read(con, query = paste0("SELECT * FROM education.cde_multigeo_calpads_graduation_",curr_yr))
  

#### Continue prep for RC ####

#filter for county and state rows, all types of schools, and racial categories
df_subset <- df %>% filter(aggregatelevel %in% c("C", "T", "D") & charterschool == "All" & dass == "All" & reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%
  dplyr::select(aggregatelevel, cdscode, countyname, districtname, reportingcategory, cohortstudents, regularhsdiplomagraduatescount, regularhsdiplomagraduatesrate)

#format for column headers
df_subset <- rename(df_subset, raw = "regularhsdiplomagraduatescount", pop = "cohortstudents", rate = "regularhsdiplomagraduatesrate")

df_subset$reportingcategory <- gsub("TA", "total", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RB", "nh_black", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RI", "nh_aian", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RA", "nh_asian", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RF", "nh_filipino", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RH", "latino", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RP", "nh_pacisl", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RT", "nh_twoormor", df_subset$reportingcategory)
df_subset$reportingcategory <- gsub("RW", "nh_white", df_subset$reportingcategory)
df_subset <- rename(df_subset,c("geoname" = "countyname"))

#pop screen. suppress raw/rate for groups with fewer than 20 graduating students.
threshold = 20
df_subset <- df_subset %>% mutate(rate = ifelse(raw < threshold, NA, rate), raw = ifelse(raw < threshold, NA, raw))

#pivot wider
df_wide <- df_subset %>% pivot_wider(names_from = reportingcategory, names_glue = "{reportingcategory}_{.value}", values_from = c(raw, pop, rate))
df_wide$geoname[df_wide$geoname =='State'] <- 'California'   # update state rows' geoname field values
df_wide <- merge(x=counties,y=df_wide,by="geoname", all=T) #add county geoids
df_wide <- within(df_wide, geoid[geoname == 'California'] <- '06')#add state geoid

# get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
districts <- st_read(con, query = paste0("SELECT cdscode, ncesdist AS geoid FROM education.cde_public_schools_",curr_yr," WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'")) # district,

df_final <- left_join(df_wide, districts, by = c('cdscode')) %>% dplyr::rename("geoid" = "geoid.x") %>%
  mutate(geoid=ifelse(aggregatelevel == "D", geoid.y, geoid), geoname=ifelse(!is.na(districtname), districtname, geoname)) %>% 
  select(-c(geoid.y, districtname)) %>% distinct() %>%  # combine distinct county and district geoid matched df's
  relocate(geoid, geoname, cdscode, aggregatelevel)

df_final <- filter(df_final, !is.na(geoid)) # remove records without fips codes

d <- df_final

####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
# source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")
source("W:/Project/RACE COUNTS/2025_v7/RC_Github/CR/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
# View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]%>% select(-c(cdscode, aggregatelevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
# View(state_table)

#remove state from county table
county_table <- d[d$aggregatelevel == 'C', ] %>% select(-c(cdscode, aggregatelevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
# View(county_table)

#split CITY into separate table
city_table <- d[d$aggregatelevel == 'D', ] %>% select(-c(aggregatelevel))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname") %>% relocate(cdscode, .after = dist_id)
# View(city_table)

###update info for postgres tables will automatically update###
county_table_name <- paste0("arei_educ_hs_grad_county_",rc_yr)
state_table_name <- paste0("arei_educ_hs_grad_state_",rc_yr)
city_table_name <- paste0("arei_educ_hs_grad_district_",rc_yr)
indicator <- paste0("Created on ", Sys.Date(), ". Four-year adjusted cohort graduation rate")
source <- paste0("CDE ",curr_yr," https://www.cde.ca.gov/ds/ad/filesacgr.asp")


#send tables to postgres
to_postgres(county_table,state_table)
city_to_postgres()
dbDisconnect(con)