### Chronic Absenteeism RC v7 ### 

#install packages if not already installed
packages <- c("data.table","stringr","dplyr","RPostgres","dbplyr","srvyr", "DBI", "tidyverse",
              "tidycensus","tidyr","rpostgis", "here", "sf", "usethis", "readr", "rvest")

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con_shared <- connect_to_db("rda_shared_data")
con_rc <- connect_to_db("racecounts")

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Sheet_Chr_Abs.docx"

# update each year
curr_yr <- '2023_24' # CDE data year, must keep this format
rc_yr <- '2025'    # RC year
rc_schema <- 'v7'

# other variables that may not update each year
# pop screen on number of chronically absent students (raw)
threshold <- 20
# ACS year to get list of counties
acs_yr <- 2023

###### Get supporting data for geolevel analysis #####
# county geoids
counties <- get_acs(geography = "county",
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = acs_yr) %>%
  select(GEOID, NAME) %>%
  rename(geoid=GEOID, geoname=NAME) %>%
  mutate(geoname = gsub(" County, California", "", geoname))

# get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
districts <- dbGetQuery(con_shared, statement = 
                          paste0("SELECT cdscode, ncesdist AS ncesdist_geoid FROM education.cde_public_schools_", 
                                 curr_yr, 
                                 " WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'"))

# get leg districts (senate, assembly) and join to districts
xwalk_school_sen <- dbGetQuery(con_shared, paste0("SELECT cdscode, ca_senate_district FROM crosswalks.cde_school_leg_districts_2022_23")) %>%
  mutate(leg_id = paste0('060', ca_senate_district),
         geolevel = 'sldu')

xwalk_school_assm <- dbGetQuery(con_shared, paste0("SELECT cdscode, ca_assembly_district FROM crosswalks.cde_school_leg_districts_2022_23")) %>%
  mutate(leg_id = paste0('060', ca_assembly_district),
         geolevel = 'sldl')

############### PREP RDA_SHARED_DATA TABLE ########################
# ## Get Chronic Absenteeism
#   filepath = "https://www3.cde.ca.gov/demo-downloads/attendance/chronicabsenteeism24.txt"   # will need to update each year
#   fieldtype = 1:12 # specify which cols should be varchar, the rest will be assigned numeric in table export
# 
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
#      table_schema <- "education"
#      table_name <- "cde_multigeo_chronicabs_2023_24"
#      table_comment_source <- "NOTE: Only use chronic absenteeism data from this link. The Dashboard download is incomplete and lacks data for most high schools (at least within LAUSD).
#      Chronic absenteeism data downloaded from https://www.cde.ca.gov/ds/ad/filesabd.asp"
#      table_source <- "Wide data format, multigeo table with state, county, district, and school"
# 
# ## Run function to prep and export rda_shared_data table
#   source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
#   df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db
#   View(df)
# 
# ## Run function to add rda_shared_data column comments
# # See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
# url <-  "https://www.cde.ca.gov/ds/ad/fsabd.asp"   # define webpage with metadata
# html_nodes <- "table"
# colcomments <- get_cde_metadata(url, html_nodes, table_schema, table_name)
# View(colcomments)

# Get rda_shared_data table and do initial RC prep (select cols, filter, apply rc naming)
df <- dbGetQuery(con_shared, statement = "SELECT * FROM education.cde_multigeo_chronicabs_2023_24") %>%
  #select just fields we need
  select(cdscode, countyname, districtname, aggregatelevel, charterschool, dass,
         reportingcategory, chronicabsenteeismeligiblecumulativeenrollment, 
         chronicabsenteeismcount, chronicabsenteeismrate) %>%
  # filter for RC races
  filter(reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%
  # rename columns for rc standards:
  rename(raw = chronicabsenteeismcount,
         pop = chronicabsenteeismeligiblecumulativeenrollment,
         rate = chronicabsenteeismrate) %>%
  # replace reportingcategory codes with rc race codes, add geolevel
  mutate(reportingcategory=
           case_when(
             reportingcategory == "TA" ~ "total", 
             reportingcategory == "RB" ~ "nh_black", 
             reportingcategory == "RI" ~ "nh_aian", 
             reportingcategory == "RA" ~ "nh_asian", 
             reportingcategory == "RF" ~ "nh_filipino", 
             reportingcategory == "RH" ~ "latino", 
             reportingcategory == "RP" ~ "nh_pacisl", 
             reportingcategory == "RT" ~ "nh_twoormor", 
             reportingcategory == "RW" ~ "nh_white", 
             .default = reportingcategory),
         geolevel = 
           case_when(
             aggregatelevel == "T"~"state",
             aggregatelevel == "C"~"county",
             aggregatelevel == "D"~"district",
             aggregatelevel == "S"~"school",
             .default = aggregatelevel)) %>%
  # apply population threshold
  mutate(raw = ifelse(raw < threshold, NA, raw),
         rate = ifelse(raw < threshold, NA, rate)) %>%
  # get related counties, school districts, senate, and assembly leg districts
  right_join(counties, by=c("countyname"="geoname")) %>%
  rename(county_geoid=geoid) %>%
  left_join(districts, by="cdscode") %>%
  left_join(xwalk_school_sen, by="cdscode", suffix=c("", "_senate")) %>% 
  rename(senate_geoid=leg_id) %>%
  left_join(xwalk_school_assm, by="cdscode", suffix=c("", "_assm")) %>%
  rename(assm_geoid=leg_id)


############### Leg District Prep ###############
# filter for schools and exclude those where last 7 digits of cdscode are all zeros
df_subset_leg <- df %>% 
  filter(geolevel=="school") %>%
  mutate(last_7_digits = substr(cdscode, nchar(cdscode) - 7 + 1, nchar(cdscode))) %>%
  filter(last_7_digits != "0000000") %>%
  select(-last_7_digits) 

# split into senate and assembly
df_subset_senate <- df_subset_leg %>%
  filter(!is.na(geolevel_senate)) %>% 
  mutate(geolevel="sldu",
         final_geoid=senate_geoid,
         geoname=paste("Senate District", ca_senate_district)) %>%
  # select needed cols
  select(final_geoid, geoname, geolevel, reportingcategory, raw, pop, rate) %>%
  group_by(final_geoid, geoname, geolevel, reportingcategory) %>%
  summarize(
    raw=sum(raw, na.rm=TRUE),
    pop=sum(pop, na.rm = TRUE),
    rate=sum(raw, na.rm=TRUE)/sum(pop, na.rm=TRUE)*100
  ) %>%
  ungroup()

df_subset_assm <- df %>% 
  filter(!is.na(geolevel_assm)) %>% 
  mutate(geolevel="sldl",
         final_geoid=assm_geoid,
         geoname=paste("Assembly District", ca_assembly_district)) %>%
  # select needed cols
  select(final_geoid, geoname, geolevel, reportingcategory, raw, pop, rate) %>%
  group_by(final_geoid, geoname, geolevel, reportingcategory) %>%
  summarize(
    raw=sum(raw, na.rm=TRUE),
    pop=sum(pop, na.rm = TRUE),
    rate=sum(raw, na.rm=TRUE)/sum(pop, na.rm=TRUE)*100
  ) %>%
  ungroup()

df_subset_leg <- rbind(df_subset_senate, df_subset_assm) %>%
  mutate(cdscode=NA)

############### County, State, and School District Prep ###############
df_subset <- df %>% 
  # filter for county, state, and school district rows, all types of schools
  filter(aggregatelevel %in% c("C", "T", "D") & charterschool == "All" & dass == "All") %>%
  mutate(final_geoid = 
           case_when(
             geolevel=="state"~"06",
             geolevel=="county"~county_geoid,
             geolevel=="district"~ncesdist_geoid),
         geoname=
           case_when(
             geolevel=="state"~"California",
             geolevel=="county"~countyname,
             geolevel=="district"~districtname)) %>%
  # select needed cols
  select(final_geoid, cdscode, geoname, geolevel, reportingcategory, raw, pop, rate) 

# View(df_subset)

####### Combine all geolevels (county, state, school district, leg district) ##### ---------------------------------------------------------------------
df_final <- rbind(df_subset_leg, df_subset) %>%
  rename(geoid=final_geoid) %>%
  # remove records without fips codes or "No Data"
  filter(!is.na(geoid) & geoid != "No Data") %>%
  # pivot to get into RC table format
  pivot_wider(names_from = reportingcategory, 
              values_from = c(raw, pop, rate),
              names_glue = "{reportingcategory}_{.value}") %>%
  relocate(geoid, geoname, cdscode, geolevel) %>%
  distinct()

# View(df_final)

d <- df_final
# table(d$geolevel)
# county district     sldl     sldu 
# 58      997       80       40 

####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
# source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")
source("./Functions/RC_Functions.R")

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

#split LEGISLATIVE DISTRICTS into separate table 
upper_leg_table <- d[d$geolevel == 'sldu', ]
lower_leg_table <- d[d$geolevel == 'sldl', ]

#calculate LEGISLATIVE DISTRICTS z-scores and bind
upper_leg_table <- calc_z(upper_leg_table)
upper_leg_table <- calc_ranks(upper_leg_table)
upper_leg_table <- upper_leg_table
#View(upper_leg_table)

lower_leg_table <- calc_z(lower_leg_table)
lower_leg_table <- calc_ranks(lower_leg_table)
lower_leg_table <- lower_leg_table
#View(lower_leg_table)

leg_table <- rbind(upper_leg_table, lower_leg_table) %>% dplyr::rename("leg_id" = "geoid", "leg_name" = "geoname")


###update info for postgres tables###
county_table_name <- paste0("arei_educ_chronic_absenteeism_county_", rc_yr)
state_table_name <- paste0("arei_educ_chronic_absenteeism_state_", rc_yr)
city_table_name <- paste0("arei_educ_chronic_absenteeism_district_", rc_yr)
leg_table_name <- paste0("arei_educ_chronic_absenteeism_leg_", rc_yr)


indicator <- paste0("Created on ", Sys.Date(), ". Chronic Absenteeism Eligible Cumulative Enrollment, Chronic Absenteeism Count, and Chronic Absenteeism Rate. This data is")

source <- paste0("CDE ", curr_yr, " https://www.cde.ca.gov/ds/ad/filesabd.asp")

#send tables to postgres
#to_postgres(county_table,state_table)
#city_to_postgres()
#leg_to_postgres(leg_table)


#disconnect
dbDisconnect(con_shared)
dbDisconnect(con_rc)

