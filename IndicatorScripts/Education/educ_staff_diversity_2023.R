### Teacher & Staff Diversity RC v5 ### 

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

# ##### get enrollment data and save it in pgadmin -----
# filepath = "https://www3.cde.ca.gov/demo-downloads/ce/cenroll1819.txt" 
# fieldtype = 1:12 # specify which cols should be varchar, the rest will be assigned numeric
# 
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "education"
# table_name <- "cde_multigeo_enrollment_2018_19"
# table_comment_source <- "NOTE: This data is not trendable with data from before 2016-17. See more here: https://www.cde.ca.gov/ds/sd/sd/acgrinfo.asp"
# table_source <- "Downloaded from https://www.cde.ca.gov/ds/ad/filesenrcum.asp. Headers were cleaned of characters like /, ., ), and (. Cells with values of * were nullified. Created cdscode by concatenating county, district, and school codes"
# 
# ## Run function to prep and export rda_shared_data table 
# source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
# df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db


#######Get enrollment data for population values------
enrollment <- st_read(con, query = "select * from education.cde_multigeo_enrollment_2018_19")
# # View(enrollment)
enrollment <- enrollment %>% mutate(districtcode = ifelse(!is.na(districtcode),paste0(enrollment$countycode,enrollment$districtcode), NA))

enrollment_df <- enrollment %>% filter(aggregatelevel %in% c("C", "T", "D") & charter == "All" & 
                                         
                                         reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%
  
  #select just fields we need
  select(aggregatelevel, cdscode, countyname, districtname, districtcode, reportingcategory, cumulativeenrollment) 
enrollment_df$cumulativeenrollment <- as.numeric(enrollment_df$cumulativeenrollment)

# # View(enrollment_df)

#rename to the race/ethnicity codes
enrollment_df$reportingcategory <- gsub("TA", "total", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RB", "nh_black", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RI", "nh_aian", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RA", "nh_asian", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RF", "nh_filipino", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RH", "latino", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RP", "nh_pacisl", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RT", "nh_twoormor", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RW", "nh_white", enrollment_df$reportingcategory)


#pivot wider
enrollment_wide <- enrollment_df %>% pivot_wider(names_from = reportingcategory, names_glue = "{reportingcategory}_pop", values_from = cumulativeenrollment)
enrollment_wide$countyname[enrollment_wide$countyname =='State'] <- 'California'   # update state row's countyname field values


#get county Geoids
census_api_key(census_key1, install = TRUE, overwrite = TRUE)

ca <- get_acs(geography = "county",
              variables = c("B01001_001"),
              state = "CA",
              year = 2021)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
## View(ca)


#add county geoids
enrollment_wide <- left_join(x=enrollment_wide,y=ca,by=c("countyname"="geoname")) %>% mutate(geoid = ifelse(aggregatelevel=="D", NA, geoid))
# add state geoid
enrollment_wide <- within(enrollment_wide, geoid[countyname == 'California'] <- '06')

###### Get staff demographics data ----
staff_demo <- st_read(con, query = "select * from education.cde_2018_19_staff_demo")
staff_demo$countycode <- stringr::str_extract(staff_demo$districtcode, "^.{2}")
#create a countycode by extracting the first two string from the disctrictcode
staff_demo$countyname <- str_to_title(staff_demo$countyname)
staff_demo$countyname <- trimws(staff_demo$countyname) #remove spacing from string, you need to remove this space so you can join by geoname to add county geoids
# View(staff_demo)

# select only needed 
df <- staff_demo %>% select(districtcode, countycode, countyname, districtname, ethnicgroup)
df$ethnicgroup <- gsub("0", "not_reported", df$ethnicgroup)
df$ethnicgroup <- gsub("6", "nh_black", df$ethnicgroup)
df$ethnicgroup <- gsub("1", "nh_aian", df$ethnicgroup)
df$ethnicgroup <- gsub("2", "nh_asian", df$ethnicgroup)
df$ethnicgroup <- gsub("4", "nh_filipino", df$ethnicgroup)
df$ethnicgroup <- gsub("5", "latino", df$ethnicgroup)
df$ethnicgroup <- gsub("3", "nh_pacisl", df$ethnicgroup)
df$ethnicgroup <- gsub("9", "nh_twoormor", df$ethnicgroup)
df$ethnicgroup <- gsub("7", "nh_white", df$ethnicgroup)

df_district <- df %>% group_by(countycode, countyname, districtcode, districtname, ethnicgroup) %>% mutate(raw = n()) %>% unique() %>% 
  ungroup() %>% group_by(countycode, countyname, districtcode, districtname) %>% 
  group_modify(~ bind_rows(., summarize(., across(where(is.numeric), sum)))) %>% ungroup() %>% mutate(ethnicgroup = coalesce(ethnicgroup, "total")) %>% as.data.frame()

# View(df_district)

#create rows for county and state
df_district$aggregatelevel <- "D"
df_county <- df_district %>% ungroup() %>% group_by(countycode, countyname, ethnicgroup) %>%  select(-c(districtcode,districtname,aggregatelevel))%>%  mutate(raw = sum(raw, na.rm=TRUE)) %>% unique()
df_county$districtcode <- NA
df_county$districtname <- NA
df_county$aggregatelevel <- "C"
# View(df_county)

df_district_county <- rbind(df_district, df_county)
#add county geoids
df_district_county <- left_join(x=df_district_county,y=ca,by=c("countyname"="geoname")) %>% mutate(geoid = ifelse(aggregatelevel=="D", NA, geoid))
# View(df_district_county)

# add the state data -----
df_state <- df_district %>% ungroup() %>% group_by(ethnicgroup) %>% select(-c(countycode,districtcode,countyname,districtname,aggregatelevel)) %>% mutate(raw = sum(raw)) %>% unique()
df_state$districtcode <- NA
df_state$districtname <- NA
df_state$countycode <- NA
df_state$countyname <- 'California'
df_state$aggregatelevel <- 'T'
df_state$geoid <- '06' # add state geoid
# View(df_state)

df_subset <- rbind(df_district_county, df_state)
# View(df_subset)

#pivot wider
df_wide <- df_subset %>% pivot_wider(names_from = "ethnicgroup", values_from = "raw", names_glue = "{ethnicgroup}_raw") %>% select(-not_reported_raw)
# View(df_wide)

#join together to calculate rate----
df_enroll_staff <- right_join(enrollment_wide, df_wide, by= c("districtcode", "countyname", "districtname", "geoid", "aggregatelevel"))
# View(df_enroll_staff)

#screen data
#set population screen threshold
pop_screen <- 100
df_enroll_staff <- df_enroll_staff %>% mutate(
  total_raw = ifelse(total_pop < pop_screen, NA, total_raw),
  nh_black_raw = ifelse(nh_black_pop < pop_screen, NA, nh_black_raw),
  nh_aian_raw = ifelse(nh_aian_pop < pop_screen, NA, nh_aian_raw),
  nh_asian_raw = ifelse(nh_asian_pop < pop_screen, NA, nh_asian_raw),
  nh_filipino_raw = ifelse(nh_filipino_pop < pop_screen, NA, nh_filipino_raw),
  latino_raw =   ifelse(latino_pop < pop_screen, NA, latino_raw),
  nh_pacisl_raw =  ifelse(nh_pacisl_pop < pop_screen, NA, nh_pacisl_raw),
  nh_twoormor_raw =  ifelse(nh_twoormor_pop < pop_screen, NA, nh_twoormor_raw),
  nh_white_raw =  ifelse(nh_white_pop < pop_screen, NA, nh_white_raw),
  
  total_rate = ifelse(is.na(total_raw) | is.na(total_pop) | total_pop == 0, NA, (total_raw / total_pop)*100),
  nh_black_rate = ifelse(is.na(nh_black_raw) | is.na(nh_black_pop) | nh_black_pop == 0, NA, (nh_black_raw / total_pop)*100),
  nh_aian_rate = ifelse(is.na(nh_aian_raw) | is.na(nh_aian_pop) |  nh_aian_pop == 0, NA, (nh_aian_raw / total_pop)*100),
  nh_asian_rate = ifelse(is.na(nh_asian_raw) | is.na(nh_asian_pop) | nh_asian_pop == 0, NA, (nh_asian_raw / total_pop)*100),
  nh_filipino_rate = ifelse(is.na(nh_filipino_raw) | is.na(nh_filipino_pop) | nh_filipino_pop == 0, NA, (nh_filipino_raw / total_pop)*100),
  latino_rate = ifelse(is.na(latino_raw) | is.na(latino_pop) | latino_pop == 0, NA, (latino_raw / total_pop)*100),
  nh_pacisl_rate = ifelse(is.na(nh_pacisl_raw) | is.na(nh_pacisl_pop) | nh_pacisl_pop == 0, NA, (nh_pacisl_raw / total_pop)*100),
  nh_white_rate = ifelse(is.na(nh_white_raw) | is.na(nh_white_pop) | nh_white_pop == 0, NA, (nh_white_raw / total_pop)*100),
  nh_twoormor_rate = ifelse(is.na(nh_twoormor_raw) | is.na(nh_twoormor_pop) | nh_twoormor_pop == 0, NA, (nh_twoormor_raw / total_pop)*100))
# View(df_enroll_staff)


####### GET SCHOOL DISTRICT GEOIDS ##### ---------------------------------------------------------------------
# get school district geoids - pull in active district records w/ geoids from CDE schools' list (NCES District ID).
## can't get archival data, so using 2019-20 bc that is closest match to data vintage (2018-19)
districts <- st_read(con, query = "SELECT cdscode, ncesdist AS geoid FROM education.cde_public_schools_2019_20 WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'")
# View(districts)

# join dist geoids to data df, coalesce combined geoid col, drop separate county/state and district geoid cols
df_final <- df_enroll_staff %>% left_join(districts, by="cdscode") %>% mutate(geoid = coalesce(geoid.x, geoid.y)) %>% 
  select(-c(geoid.x, geoid.y, countycode)) %>% relocate(geoid, .before = everything())

d <- df_final %>% drop_na(geoid) # drop records without geoids, these are all districts

####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update d$asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
# View(d)

#split STATE into separate table and format id, name columns ----
state_table <- d[d$aggregatelevel == 'T', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_id" = "geoid", "state_name" = "countyname") %>% select(-c(districtname, districtcode, cdscode, aggregatelevel))
View(state_table)

#remove state from county table
county_table <- d[d$aggregatelevel == 'C', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_id" = "geoid", "county_name" = "countyname") %>% select(-c(districtname, districtcode, cdscode, aggregatelevel))
View(county_table)

#remove county/state from place table -----
city_table <- d[d$aggregatelevel == 'D', ] %>% select(-c(aggregatelevel, districtcode))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "districtname", "county_name" = "countyname") %>% relocate(county_name, .after = district_name)
View(city_table)


###update info for postgres tables###
county_table_name <- "arei_educ_staff_diversity_county_2023"
state_table_name <- "arei_educ_staff_diversity_state_2023"
city_table_name <- "arei_educ_staff_diversity_district_2023"
rc_schema <- "v5"

indicator <- "Staff and Teacher Diversity Count and Rate. This data is"
source <- "CDE 2018-2019 https://www.cde.ca.gov/ds/ad/filesabd.asp"

# send tables to postgres
# to_postgres(county_table,state_table)
city_to_postgres()