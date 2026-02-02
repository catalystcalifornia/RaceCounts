### MOSAIC: Low Birthweight RC v7 ###

##install packages if not already installed ------------------------------
packages <- c("RPostgres","tidycensus","tidyverse","here","usethis", "janitor")

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

new.packages <- packages[!(packages %in% installed.packages()[,"Package"])]

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

source("W:\\RDA Team\\R\\credentials_source.R")


# update each year
curr_yr <- '2016-2024'     # Birthweight data yrs
cdc_dir <- paste0("W://Data//Health//Births//CDC//", curr_yr,"//")
rc_schema <- 'v7'
rc_yr <- '2025'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Health Access\\QA_Sheet_Low_Birthweight_MOSAIC.docx"

#test <- read.csv("W:\\Data\\Health\\Births\\CDC\\2016-2024\\Natality, 2016-2024 state Births asian.csv", colClasses = c("NULL",rep("character",4), "numeric")) %>% clean_names() %>% 
#filter(!mother_s_birth_country =="")
# Function to read State data --------------------------------------------

read_state_data_asian_nhpi <- function(x, y) {
  asian <- read.csv(file = x, colClasses = c("NULL",rep("character",4), "numeric")) %>% clean_names() %>%
    filter(!mother_s_birth_country =="")
  
  asian$Notes <- str_to_title(asian$mother_s_birth_country)
  
  asian_ <- asian %>% select(Notes, state_of_residence, state_of_residence_code, births) %>% 
    filter(!is.na(state_of_residence_code))
  
  nhpi <- read.csv(file = y, colClasses = c("NULL",rep("character",4), "numeric")) %>% clean_names() %>%
    filter(!mother_s_birth_country =="")
  
  nhpi$Notes <- str_to_title(nhpi$mother_s_birth_country)
  
  nhpi_ <- nhpi %>% select(Notes, state_of_residence, state_of_residence_code, births) %>% 
    filter(!is.na(state_of_residence_code))
  
  z <- rbind(asian_, nhpi_) %>%				# put all asian and nhpi data together
    rename(county_name = state_of_residence,    
           county_id = state_of_residence_code)
  
  return(z)
}


# Function to read County data --------------------------------------------

read_county_data_asian_nhpi <- function(x, y) {
  
  asian <- read.csv(file = x, colClasses = c("NULL",rep("character",4), "numeric")) %>% clean_names() %>%
    filter(!mother_s_birth_country =="")
  
  asian$Notes <- str_to_title(asian$mother_s_birth_country)
  
  asian <- asian %>% filter(county_of_residence != "Unidentified Counties, CA")
  
  asian_ <- asian %>% select(Notes, county_of_residence, county_of_residence_code, births) %>%
    filter(!is.na(county_of_residence_code))
  
  nhpi <- read.csv(file = y, colClasses = c("NULL",rep("character",4), "numeric")) %>% clean_names() %>%
    filter(!mother_s_birth_country =="")
  
  nhpi$Notes <- str_to_title(nhpi$mother_s_birth_country)
  
  nhpi <- nhpi %>% filter(county_of_residence != "Unidentified Counties, CA")
  
  nhpi_ <- nhpi %>% select(Notes, county_of_residence, county_of_residence_code, births) %>%
    filter(!is.na(county_of_residence_code))
  
  z <- rbind(asian_,nhpi_) %>%
    rename(county_name = county_of_residence,    
           county_id = county_of_residence_code)
  
  return(z)
  
}

# Read Birth Data ---------------------------------------------------------
## Data downloaded from: https://wonder.cdc.gov/natality-expanded-current.html

state_births_asian_nhpi <- read_state_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," state Births asian.csv"),paste0(cdc_dir, "Natality, ",curr_yr," state Births nhpi.csv")) # Get State ASIAN / NHPI Births

county_births_asian_nhpi <- read_county_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," county Births asian.csv"),paste0(cdc_dir, "Natality, ",curr_yr," county Births nhpi.csv")) # Get County ASIAN / NHPI Births


# Read Low Birth Weight Data ---------------------------------------------------------

state_lbw_asian_nhpi <- read_state_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," state LBW asian.csv"), paste0(cdc_dir, "Natality, ",curr_yr," state LBW nhpi.csv")) # Get State ASIAN / NHPI LBW

county_lbw_asian_nhpi <- read_county_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," county LBW asian.csv"), paste0(cdc_dir, "Natality, ",curr_yr," county LBW nhpi.csv")) # Get County ASIAN / NHPI LBW


# Bind Data Together ---------------------------------------------------------

## births

births <- rbind(county_births_asian_nhpi, state_births_asian_nhpi)

## lbw
lbw <- rbind(county_lbw_asian_nhpi, state_lbw_asian_nhpi)

lbw <- rename(lbw, lbw = "births")

## bind birth and lbw together - this appears to drop countries with fewer than ten (suppressed to zero) LBW births, e.g., Brunei
df <- left_join(lbw, births) %>% mutate(county_name = gsub(" County, CA", "", county_name)) %>%
  relocate(county_id, .before = county_name)

#calculate Rate ---------------------------------------------------------
df <- mutate(df, rate = lbw/births*100)

#Format and Pivot wider ---------------------------------------------------------
#update raw col name for RC fx
df <- rename(df, raw = "lbw")

#pivot
df_wide <- df %>% pivot_wider(names_from = Notes, names_glue = "{Notes}_{.value}", values_from = c(raw, births, rate))
df_wide <- df_wide %>% rename(geoid = county_id, geoname = county_name)
df_wide$geolevel <- ifelse(df_wide$geoname == 'California', 'state', 'county')


############## CALC ASIAN RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

# remove NHPI columns leaving Asian columns
d <- df_wide %>% select(!matches("Fiji|Marshall Islands|Micronesia|Palau|Samoa|Tonga")) %>%
  
  # calculate totals for RC stats
  rowwise() %>% mutate(total_raw = sum(c_across(contains("_raw")), na.rm = TRUE)) %>% ungroup() %>%
  rowwise() %>% mutate(total_births = sum(c_across(contains("_births")), na.rm = TRUE)) %>% ungroup() %>%
  mutate(total_rate = total_raw/total_births*100)
  
#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table<- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table<- calc_state_z(state_table)

state_table<- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table<- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table<- calc_z(county_table)
county_table<- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

###info for postgres tables will auto-update based on variables at top of script###
county_table_name <- paste0("asian_hlth_low_birthweight_county_",rc_yr)
state_table_name <- paste0("asian_hlth_low_birthweight_state_",rc_yr)

indicator <- paste0("Percentage of infants born at low birthweight (less than 2,500 grams or about 5lbs. 5oz) of all live births, by Asian country of birth of mother. This data is")
source <- paste0("US Department of Health and Human Services, Centers for Disease Control and Prevention (CDC), National Center for Health Statistics, Division of Vital Statistics, CDC WONDER Online Database (",curr_yr,"): https://wonder.cdc.gov/natality-expanded-current.html. Mothers birth country selected based on W:\\Data\\Health\\Births\\CDC\\Asian and NHPI countries.xlsx. QA doc: ", qa_filepath)

#send tables to postgres
# to_postgres(county_table,state_table,"mosaic")


############## CALC NHPI RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

# remove NHPI columns leaving Asian columns
d <- df_wide %>% select(geoid, geoname, geolevel, matches("Fiji|Marshall Islands|Micronesia|Palau|Samoa|Tonga")) %>%
  
  # calculate totals for RC stats
  rowwise() %>% mutate(total_raw = sum(c_across(contains("_raw")), na.rm = TRUE)) %>% ungroup() %>%
  rowwise() %>% mutate(total_births = sum(c_across(contains("_births")), na.rm = TRUE)) %>% ungroup() %>%
  mutate(total_rate = total_raw/total_births*100)

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table<- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table<- calc_state_z(state_table)

state_table<- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table<- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table<- calc_z(county_table)
county_table<- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

###info for postgres tables will auto-update based on variables at top of script###
county_table_name <- paste0("nhpi_hlth_low_birthweight_county_",rc_yr)
state_table_name <- paste0("nhpi_hlth_low_birthweight_state_",rc_yr)

indicator <- paste0("Percentage of infants born at low birthweight (less than 2,500 grams or about 5lbs. 5oz) of all live births, by NHPI country of birth of mother. This data is")
source <- paste0("US Department of Health and Human Services, Centers for Disease Control and Prevention (CDC), National Center for Health Statistics, Division of Vital Statistics, CDC WONDER Online Database (",curr_yr,"): https://wonder.cdc.gov/natality-expanded-current.html. Mothers birth country selected based on W:\\Data\\Health\\Births\\CDC\\Asian and NHPI countries.xlsx. QA doc: ", qa_filepath)

#send tables to postgres
# to_postgres(county_table,state_table,"mosaic")


