## Student Homelessness for RC v6 ##

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
options(scipen = 999) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# update each year
curr_yr <- '2022_23' # CDE data year, must keep this format
rc_yr <- '2024'    # RC year
rc_schema <- 'v6'

#### 1) Get Student Homelessness Data from CDE website or postgres ####
# filepath = "https://www3.cde.ca.gov/demo-downloads/homeless/hse2223.txt"   # will need to update each year
# fieldtype = 1:12 # specify which cols should be varchar, the rest will be assigned numeric
# 
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "education"
# table_name <- paste0("cde_multigeo_calpads_homelessness_", curr_yr)
# table_comment_source <- "NOTE: This data is not trendable with data from before 2016-17"
# table_source <- "Downloaded from https://www.cde.ca.gov/ds/ad/fileshse.asp. Headers were cleaned of characters like /, ., ), and (. Also cells with values of * were nullified. Created cdscode by concatenating county, district, and school codes"
# 
# ## Run function to prep and export rda_shared_data table
# source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
# df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db

###### NOTE: This function isn't working for Student Homelessness (loop part of function is the issue).
## Run function to add rda_shared_data column comments
# See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
# url <-  "https://www.cde.ca.gov/ds/ad/fshse.asp"   # define webpage with metadata
# html_nodes <- "table"
# colcomments <- get_cde_metadata(url, html_nodes, table_schema, table_name)
# View(colcomments)

df <- st_read(con, query = "SELECT * FROM education.cde_multigeo_calpads_homelessness_2022_23") # comment out code above to pull data and use this once rda_shared_data table is created

#### 2) Get census geoids----

counties <- get_acs(geography = "county", 
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = 2022)

counties <- counties[,1:2]
counties$NAME <- gsub(" County, California", "", counties$NAME)
names(counties) <- c("geoid", "geoname")

#### 3) Continue prep for RC fx ####

#filter for county and state rows, all types of schools, and racial categories only
df_subset <- df %>% filter(aggregatelevel %in% c("C", "T") & charterschool == "All" & dass=="All" & reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%
  #select just fields we need
  select(countyname, aggregatelevel, reportingcategory, homelessstudentenrollment, cumulativeenrollment)

#format for column headers
df_subset <- df_subset %>% rename(raw = "homelessstudentenrollment", pop = "cumulativeenrollment") %>% mutate(rate=(raw/pop)*100)

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

#pop screen. suppress raw/rate for groups with fewer than 50 graduating students.
threshold = 50
df_subset <- df_subset %>% mutate(rate = ifelse(raw < threshold, NA, rate), raw = ifelse(raw < threshold, NA, raw))

#pivot wider
df_wide <- df_subset %>% pivot_wider(names_from = reportingcategory, names_glue = "{reportingcategory}_{.value}", values_from = c(raw, pop, rate)) %>% rename( c("geoname" = "countyname"))
df_wide$geoname[df_wide$geoname =='State'] <- 'California'   # update state rows' geoname field values
df_wide <- merge(x=counties,y=df_wide,by="geoname", all=T) # add county geoids
df_wide <- within(df_wide, geoid[geoname == 'California'] <- '06') # add state geoid

d <- df_wide

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update d$asbest line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ] %>% select(-c(aggregatelevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
# View(state_table)

#remove state from county table
county_table <- d[d$aggregatelevel == 'C', ] %>% select(-c(aggregatelevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
# View(county_table)


###update info for postgres tables###
county_table_name <- paste0("arei_hous_student_homelessness_county_", rc_yr)
state_table_name <- paste0("arei_hous_student_homelessness_state_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Student homelessness rates. Data for groups with enrollment under 50 are excluded from the calculations. Homelessness rate calculated as a percent of enrollment for each group")
source <- paste0("CDE ", curr_yr, " https://www.cde.ca.gov/ds/ad/fileshse.asp")

#send tables to postgres
#to_postgres(county_table, state_table)

# dbDisconnect(con)

