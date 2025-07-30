## Incarceration (County and State-Level) for RC v7

## Set up ----------------------------------------------------------------
#install packages if not already installed
packages <- c("tidyr", "dplyr", "janitor", "tidycensus", "tidyverse", "usethis", "readxl", "RPostgres")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen = 999) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con_rc <- connect_to_db("racecounts")
con_shared <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2020-2024"  # must keep same format
dwnld_url <- "https://github.com/vera-institute/incarceration-trends"
rc_schema <- "v7"
rc_yr <- "2025"


############### PREP DATA ########################

county_data <- read_excel("W:/Data/Crime and Justice/vera_institute/2024/incarceration_trends_county.xlsx")

# filter for latest complete year and CA counties
df <- county_data %>% filter(year %in% c(2020, 2021, 2022, 2023, 2024), str_detect(fips, "^06")) %>% 
  
  # select columns we want
  select(fips, county_name, 
         total_pop_15to64, 
         aapi_pop_15to64, 
         black_pop_15to64, 
         latinx_pop_15to64, 
         native_pop_15to64, 
         white_pop_15to64, 
         
         total_jail_pop, 
         aapi_jail_pop, 
         black_jail_pop, 
         latinx_jail_pop, 
         native_jail_pop, 
         white_jail_pop) %>%
  
  # rename a couple
  rename(geoid = fips, geoname = county_name)

#COUNTY PREP
#rename columns and clean data. be sure to assign correct race/eth labels (non-Latinx or not etc.)
names(df) <- gsub("_15to64", "", names(df))
names(df) <- gsub("jail_pop", "raw", names(df))
names(df) <- gsub("aapi", "nh_api", names(df))
names(df) <- gsub("native", "nh_aian", names(df))
names(df) <- gsub("black", "nh_black", names(df))
names(df) <- gsub("white", "nh_white", names(df))
names(df) <- gsub("latinx", "latino", names(df))
df$geoname <- gsub(" County", "", df$geoname)

df_summary <- df %>%
  group_by(geoid, geoname) %>%
  summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)))

#STATE PREP
df_summary <- df_summary %>% adorn_totals(name = "06", fill = "California")
View(df_summary)

# add geolevel, remove NaNs, and order by geoid
d <- df_summary %>% mutate(geolevel = ifelse(geoid == '06', 'state', 'county')) %>%
  relocate(geolevel, .after = geoname) %>%
  mutate(total_raw = as.numeric(str_replace(as.character(total_raw), "NaN", "")),
         nh_api_raw = as.numeric(str_replace(as.character(nh_api_raw), "NaN", "")),
         nh_black_raw = as.numeric(str_replace(as.character(nh_black_raw), "NaN", "")),
         latino_raw = as.numeric(str_replace(as.character(latino_raw), "NaN", "")),
         nh_aian_raw = as.numeric(str_replace(as.character(nh_aian_raw), "NaN", "")),
         nh_white_raw = as.numeric(str_replace(as.character(nh_white_raw), "NaN", ""))) %>%
  arrange(geoid)



############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
#source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")
source("./Functions/RC_Functions.R")

#YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max' as 'best'
d$asbest = 'min'    

d <- calc_rates_100k(d) #calc rates
d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)


###update info for postgres tables###
county_table_name <- paste0("arei_crim_incarceration_county_", rc_yr)
state_table_name <- paste0("arei_crim_incarceration_state_", rc_yr)
indicator <- paste0("Created on ", Sys.Date(), ". Jail population per 100,000 15 to 64 year olds")
source <- paste0("Vera Institute (", curr_yr, ")")

#send tables to postgres
#to_postgres(county_table, state_table)

dbDisconnect(con_rc)
dbDisconnect(con_shared)
