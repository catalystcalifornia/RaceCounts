## Health Insurance for RC v7 ##

#install packages if not already installed
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
curr_yr = 2023            # You MUST UPDATE each year with the last year from the 5-year ACS you're using
rc_yr = '2025'            # You MUST UPDATE each year
rc_schema <- "v7"         # You MUST UPDATE each year
cv_threshold = 40         # You may need to update
pop_threshold = 130       # You may need to update
asbest = 'min'            # Do not update
schema = 'health'         # Do not update
table_code = 's2701'      # Do not update

df_wide_multigeo <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",table_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table

df_wide_multigeo$name <- str_remove(df_wide_multigeo$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
df_wide_multigeo$name <- gsub("; California", "", df_wide_multigeo$name)

############## Pre-RC CALCS ##############
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
#split into STATE, COUNTY, CITY tables
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
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
city_table <- rename(city_table, city_id = geoid, city_name = geoname)
leg_table <- rename(leg_table, leg_id = geoid, leg_name = geoname)


############### COUNTY, STATE, CITY METADATA  ##############

###update info for postgres tables###
county_table_name <- paste0("arei_hlth_health_insurance_county_", rc_yr)      
state_table_name <- paste0("arei_hlth_health_insurance_state_", rc_yr)       
city_table_name <- paste0("arei_hlth_health_insurance_city_", rc_yr)        
leg_table_name <- paste0("arei_econ_employment_leg_", rc_yr)

indicator <- "Uninsured Population (%)"   
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Health Access\\QA_Health_Insurance.docx"
start_yr <- curr_yr-4
source <- paste0(start_yr,"-",curr_yr," ACS 5-Year Estimates, Table S2701, https://data.census.gov/cedsci/. QA Doc: ", qa_filepath)   


####### SEND TO POSTGRES #######
to_postgres(county_table, state_table)
city_to_postgres(city_table)
leg_to_postgres(leg_table)

dbDisconnect(con)