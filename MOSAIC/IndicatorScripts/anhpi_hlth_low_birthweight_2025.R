### MOSAIC: Disaggregated Asian/NHPI Low Birthweight RC v7 ###

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

# test <- read.csv("W:\\Data\\Health\\Births\\CDC\\2016-2024\\Natality, 2016-2024 state Births all asian.csv", colClasses = c("NULL",rep("character",4), "numeric")) %>% clean_names() %>%
# filter(!mother_s_birth_country =="")

# List of Asian and NHPI Birth Countries -----------------------------------
birth_countries <- read_excel("W:\\Data\\Health\\Births\\CDC\\Asian and NHPI countries.xlsx", range = "A1:B304") %>%
  filter(Asian_or_NHPI %in% c("Asian", "NHPI")) %>%
  mutate(country = toupper(CDC_Country))



# Function to read birth data --------------------------------------------
read_data_asian_nhpi <- function(x, y) {  # x = cdc birth or lbw data, y = birth country data
  cdc_data <- read.csv(file = x)
  colnames(cdc_data) <- tolower(gsub("\\.", "_", colnames(cdc_data)))     # clean col names
  cdc_data$births <- as.numeric(gsub("Suppressed", NA, cdc_data$births))  # make 'births' numeric
  cdc_data <- cdc_data %>%
    mutate(
      state_of_residence_code = if ("state_of_residence_code" %in% names(.))    # clean geoid
        paste0("0",state_of_residence_code)
      else
        NULL
    )
  cdc_data <- cdc_data %>%
    mutate(
      county_of_residence_code = if ("county_of_residence_code" %in% names(.))   # clean geoid
        paste0("0",county_of_residence_code)
      else
        NULL
    )
  
  cdc_data <- cdc_data %>% select(-(notes))                               # drop unneeded col
  
  # join to (filter) Asian/NHPI birth country list
  cdc_data <- right_join(cdc_data, birth_countries, by = c("mother_s_birth_country" = "country"))
  
return(cdc_data)
}


# Function to clean birth data -----------------------------------
clean_data <- function(z){
  z <- z %>%
    rename_with(~ tolower(.x)) %>%   # colnames: replace "." with "_" and make lower case
    rename_with(
      ~ dplyr::case_when(
        grepl("residence_code$", .x) ~ "geoid",
        grepl("residence$", .x)      ~ "geoname",
        TRUE                         ~ .x
      )
    ) %>%
    filter(!is.na(geoid)) %>%
    filter(geoid != '06999') %>%         
    filter(!mother_s_birth_country =="") %>%
    mutate(cdc_country = tolower(gsub(" ", "_", cdc_country))) %>%
    mutate(cdc_country = tolower(gsub(",", "", cdc_country))) %>%
    mutate(cdc_country = ifelse(grepl("micronesia", cdc_country), "micronesia", cdc_country)) %>%
    mutate(geoname = gsub(" County, CA", "", geoname)) %>%
    select(geoid, geoname, cdc_country, births, asian_or_nhpi) %>%
    relocate(geoid, .before = geoname) 
  return(z)
}


# Read & Clean Birth Data ---------------------------------------------------------
## Data downloaded from: https://wonder.cdc.gov/natality-expanded-current.html
state_births_asian_nhpi <- read_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," state expanded all.csv"), birth_countries) %>% # Get State ASIAN / NHPI Births
  clean_data()

state_lbw_asian_nhpi <- read_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," state expanded lbw.csv"), birth_countries) %>% # Get State ASIAN / NHPI LBW
  clean_data()

county_births_asian_nhpi <- read_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," county expanded all.csv"), birth_countries) %>% # Get county ASIAN / NHPI Births
  clean_data()

county_lbw_asian_nhpi <- read_data_asian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," county expanded lbw.csv"), birth_countries) %>% # Get county ASIAN / NHPI LBW
  clean_data()


# Bind Data Together ---------------------------------------------------------

## births

births <- rbind(county_births_asian_nhpi, state_births_asian_nhpi)

## lbw
lbw <- rbind(county_lbw_asian_nhpi, state_lbw_asian_nhpi)
lbw <- rename(lbw, raw = "births")

## Births & LBW final clean up & rate calc
final_data <- left_join(lbw, births) %>%
  # calculate lbw rate
  mutate(rate = raw/births * 100,
         geolevel = ifelse(geoname == 'California', 'state', 'county')) %>%
  select(geoid, geoname, cdc_country, asian_or_nhpi, births, raw, rate, geolevel)
  
# unique(final_data$cdc_country)  # list of birth countries included, n = 67

asian_wide <- final_data %>% 
  filter(asian_or_nhpi == 'Asian') %>%
  pivot_wider(names_from = cdc_country, 
              names_glue = "{cdc_country}_{.value}", 
              values_from = c(births, raw, rate)) %>%
  remove_empty("cols") %>%       # drop 11 rate cols where all vals are NA
  mutate(total_rate = NA) %>%    # add dummy total_rate for RC calcs
  select(-asian_or_nhpi)
  

nhpi_wide <- final_data %>% 
  filter(asian_or_nhpi == 'NHPI') %>%
  pivot_wider(names_from = cdc_country, 
              names_glue = "{cdc_country}_{.value}", 
              values_from = c(births, raw, rate)) %>%
  remove_empty("cols") %>%       # drop 22 rate cols where all vals are NA
  mutate(total_rate = NA) %>%    # add dummy total_rate for RC calcs
  select(-asian_or_nhpi)  

############## CALC ASIAN RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

d <- asian_wide 

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

d <- nhpi_wide

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
state_table <- state_table %>% select(-total_rate)
View(state_table)

#remove state from county table
county_table<- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table<- calc_z(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
county_table <- county_table %>% select(-total_rate)
View(county_table)

###info for postgres tables will auto-update based on variables at top of script###
county_table_name <- paste0("nhpi_hlth_low_birthweight_county_",rc_yr)
state_table_name <- paste0("nhpi_hlth_low_birthweight_state_",rc_yr)

indicator <- paste0("Percentage of infants born at low birthweight (less than 2,500 grams or about 5lbs. 5oz) of all live births, by NHPI country of birth of mother. This data is")
source <- paste0("US Department of Health and Human Services, Centers for Disease Control and Prevention (CDC), National Center for Health Statistics, Division of Vital Statistics, CDC WONDER Online Database (",curr_yr,"): https://wonder.cdc.gov/natality-expanded-current.html. Mothers birth country selected based on W:\\Data\\Health\\Births\\CDC\\Asian and NHPI countries.xlsx. QA doc: ", qa_filepath)

#send tables to postgres
# to_postgres(county_table,state_table,"mosaic")


