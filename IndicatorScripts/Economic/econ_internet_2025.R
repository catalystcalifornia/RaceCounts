## Internet Access RC v7 ##
#install packages if not already installed
packages <- c("readr","tidyr","dplyr","DBI","RPostgres","tidycensus", "rvest", "tidyverse", "stringr", "usethis", "tigris")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 
if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 
for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


############## UPDATE FOR SPECIFIC INDICATOR HERE ##############
curr_yr = 2023     # you MUST UPDATE each year with the last year from the 5-year ACS you're using
rc_yr = '2025'     # you MUST UPDATE each year
rc_schema <- "v7"  # you MUST UPDATE each year
cv_threshold = 40         
pop_threshold = 100       
asbest = 'max'            
schema = 'economic'
table_code = "s2802"     

df_wide_multigeo <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",table_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
df_wide_multigeo$name <- str_remove(df_wide_multigeo$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
df_wide_multigeo$name <- gsub("; California", "", df_wide_multigeo$name)


############## PRE-CALCULATION DATA PREP ##############
source(".\\Functions\\rdashared_functions.R")

df <- prep_acs(df_wide_multigeo, table_code, cv_threshold, pop_threshold)

df_screened <- dplyr::select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"), -ends_with("_cv"))

df_screened$name <- gsub("State Senate", "Senate", df_screened$name)  # clean Sen geonames

d <- df_screened

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

# Adds asbest value for RC Functions
d$asbest = asbest

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure asbest is correct before running this function.
d <- calc_diff(d) 
d <- calc_avg_diff(d)
d <- calc_s_var(d)
d <- calc_id(d)

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

#calculate SLDU z-scores and ranks
upper_table <- calc_z(upper_table)

upper_table <- calc_ranks(upper_table)
View(upper_table)

#calculate SLDL z-scores and ranks
lower_table <- calc_z(lower_table)

lower_table <- calc_ranks(lower_table)
View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to state_id, county_id, city_id, leg_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")


############### COUNTY, STATE, CITY METADATA  ##############
  
###update info for postgres tables###
county_table_name <- paste0("arei_econ_internet_county_", rc_yr)    
state_table_name <- paste0("arei_econ_internet_state_", rc_yr)      
city_table_name <- paste0("arei_econ_internet_city_", rc_yr)        
leg_table_name <- paste0("arei_econ_internet_leg_", rc_yr) 

indicator <- paste0("Created on ", Sys.Date(), ". Persons with Internet Access (%)")  
start_yr <- curr_yr-4
source <- paste0(start_yr, "-", curr_yr, " ACS 5-Year Estimates, Table S2802, https://data.census.gov/cedsci/. QA doc: W:/Project/RACE COUNTS/2025_v7/Economic/QA_Internet_Access.docx")   # See most recent Indicator Methodology for source info

####### SEND TO POSTGRES #######
to_postgres()       # Sends county_table and state_table to postgres
city_to_postgres()  # Sends city_table to postgres
leg_to_postgres()   # Sends leg_table to postgres

dbDisconnect(con)
