## Homeownership for RC v7 ##
#install packages if not already installed
packages <- c("dplyr","data.table","tidycensus","sf","DBI","RPostgres","RPostgreSQL","stringr","tidyr","tigris","usethis", "here")  

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

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


### Update these variables each year ###
curr_yr = 2023 # you MUST UPDATE each year
rc_yr <- '2025'
rc_schema <- 'v7'


### Variables that do not need updates ###
cv_threshold = 40         
pop_threshold = 100       
asbest = 'max'            
schema = 'housing'
table_code = 'b25003'

df_wide_multigeo <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",table_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
df_wide_multigeo$name <- str_remove(df_wide_multigeo$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
df_wide_multigeo$name <- gsub("; California", "", df_wide_multigeo$name)

############## Pre-RC CALCS ##############
source(".\\Functions\\rdashared_functions.R")

df <- prep_acs(df_wide_multigeo, table_code, cv_threshold, pop_threshold)

df_screened <- dplyr::select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("003m"), -ends_with("003e"), -ends_with("_cv"))

d <- df_screened

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = asbest    # Adds asbest value for RC Functions

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity

### Split into geolevel tables
#split into STATE, COUNTY, CITY, SLDU, SLDL tables 
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table) %>% dplyr::select(-c(geolevel))
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table) 

## Calc county ranks##
county_table <- calc_ranks(county_table) %>% dplyr::select(-c(geolevel))
View(county_table)

#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
city_table <- calc_ranks(city_table) %>% dplyr::select(-c(geolevel))
View(city_table)

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


############### COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0("arei_hous_homeownership_county_", rc_yr)      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- paste0("arei_hous_homeownership_state_", rc_yr)        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- paste0("arei_hous_homeownership_city_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
leg_table_name <- paste0("arei_hous_homeownership_leg_", rc_yr)          # See most recent RC Workflow SQL Views for table name (remember to update year)
start_yr <- curr_yr-4

indicator <- " Owner-Occupied Housing Units (%)"                # See most recent Indicator Methodology for indicator description
source <- paste0("ACS (", start_yr, "-", curr_yr,") 5-Year Estimates, Tables B25003B-I, https://data.census.gov/cedsci/")   # See most recent Indicator Methodology for source info
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Sheet_Homeownership.docx"

####### SEND TO POSTGRES #######
# to_postgres(county_table,state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)
# 
# dbDisconnect(con)