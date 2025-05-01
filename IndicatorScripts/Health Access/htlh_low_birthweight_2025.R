### Low Birthweight RC v7 ###

##install packages if not already installed ------------------------------
packages <- c("readr","dplyr","tidyr","RPostgres","tidycensus","tidyverse","here","usethis")

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
curr_yr <- '2016-2023'     # Birthweight data yrs
cdc_dir <- paste0("W://Data//Health//Births//CDC//", curr_yr,"//")
rc_schema <- 'v7'
rc_yr <- '2025'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Health Access\\QA_Sheet_Low_Birthweight.docx"


# Functions to read State data --------------------------------------------

read_state_data <- function(x, group) {
  y <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    `State of Residence` = col_character(),
    `State of Residence Code` = col_character(),
    Births = col_integer()))
  
  y <- rename(y, county_name = `State of Residence`, #will rename after rbind
                 county_id = `State of Residence Code`) #will rename after rbind
  
  
  ##### for nh races #### 
  if(is.null(group)) {
    
    y$Notes <- y$`Mother's Single Race 6`
    y$Notes <- gsub('White', 'nh_white', y$Notes)
    y$Notes <- gsub('Black or African American', 'nh_black', y$Notes)
    y$Notes <- gsub('Asian', 'nh_asian', y$Notes)
    y$Notes <- gsub('More than one race', 'nh_twoormor', y$Notes)
    
    
    y <- y[,-c(4:5)]
    
    y <- y %>% filter(!is.na(Notes))
    
    
    return(y)
    
  }
  else {
    
    ### for total and latino
    
    y$Notes <- group	
    y <- y %>% filter(!is.na(county_name))
    
    
    return(y)
    
  }
  
}
read_state_data_aian_nhpi <- function(x, y) {
  aian <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    `State of Residence` = col_character(),
    `State of Residence Code` = col_character(),
    Births = col_integer()))
  
  aian$Notes <- 'aian'
  
  aian_ <- aian %>% select(Notes, `State of Residence`, `State of Residence Code`, Births) %>% filter(!is.na(`State of Residence Code`))
  
  pacisl <- read_delim(file = y, delim = "\t", col_types = cols(
    Notes = col_character(),
    `State of Residence` = col_character(),
    `State of Residence Code` = col_character(),
    Births = col_integer()))
  
  pacisl$Notes <- 'pacisl'
  
  pacisl_ <- pacisl %>% select(Notes, `State of Residence`, `State of Residence Code`, Births) %>%
    filter(!is.na(`State of Residence Code`))
  
  
  z <- rbind(aian_, pacisl_) %>%				# put all aian and pacisl data together
    rename(county_name = `State of Residence`,    
           county_id = `State of Residence Code`)
  
  return(z)
}


# Functions to read County data --------------------------------------------

read_county_data <- function(x, group) {
  
  y <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    `County of Residence` = col_character(),
    `County of Residence Code`= col_character(),
    Births = col_integer()))
  
  y <- rename(y, county_name = `County of Residence`,
                 county_id = `County of Residence Code`)
  
  ##### for nh races #### 
  
  if(is.null(group)) {
    
    y$Notes <- y$`Mother's Single Race 6`
    y$Notes <- gsub('White', 'nh_white', y$Notes)
    y$Notes <- gsub('Black or African American', 'nh_black', y$Notes)
    y$Notes <- gsub('Asian', 'nh_asian', y$Notes)
    y$Notes <- gsub('More than one race', 'nh_twoormor', y$Notes)    
    y <- y[,-c(4:5)]
    
    y <- y %>% filter(!is.na(Notes))
    
    y <- y %>% filter(county_name != "Unidentified Counties, CA")
    
    
    return(y)
    
  }
  
  else
    
    ### for total and latino ###
    
  {
    
    y$Notes <- group
    
    y <- y %>% filter(!is.na(county_name))
    
    y <- y %>% filter(county_name != "Unidentified Counties, CA")
    
    return(y)
  }
  
  
}
read_county_data_aian_nhpi <- function(x, y) {
  
  aian <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    `County of Residence` = col_character(),
    `County of Residence Code` = col_character(),
    Births = col_integer()))
  
  aian$Notes <- 'aian'
  
  aian <- aian %>% filter(`County of Residence` != "Unidentified Counties, CA")
  
  aian_ <- aian %>% select(Notes, `County of Residence`, `County of Residence Code`, Births) %>%
    filter(!is.na(`County of Residence Code`))
  
  pacisl <- read_delim(file = y, delim = "\t", col_types = cols(
    Notes = col_character(),
    `County of Residence` = col_character(),
    `County of Residence Code` = col_character(),
    Births = col_integer()))
  
  pacisl$Notes <- 'pacisl'
  
  pacisl <- pacisl %>% filter(`County of Residence` != "Unidentified Counties, CA")
  
  pacisl_ <- pacisl %>% select(Notes, `County of Residence`, `County of Residence Code`, Births) %>%
    filter(!is.na(`County of Residence Code`))
  
  z <- rbind(aian_,pacisl_) %>%
    rename(county_name = `County of Residence`,    
           county_id = `County of Residence Code`)
  
  return(z)
  
}

# Read Birth Data ---------------------------------------------------------
## Data downloaded from: https://wonder.cdc.gov/natality-expanded-current.html
state_births_total <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state Births total.txt"), "total") # Get State Total Births

county_births_total <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county Births total.txt"), "total") # Get County Total Births

state_births_latino <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state Births latino.txt"), "latino") %>% filter(!duplicated(county_name)) %>% select(Notes, county_name, county_id, Births) # Get State Latino Births

county_births_latino <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county Births latino.txt"), "latino") %>% filter(!duplicated(county_name)) %>% select(Notes, county_name, county_id, Births) # Get County Latino Births

state_births_aian_nhpi <- read_state_data_aian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," state Births all aian.txt"),paste0(cdc_dir, "Natality, ",curr_yr," state Births all nhpi.txt")) # Get State All AIAN / NHPI Births

county_births_aian_nhpi <- read_county_data_aian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," county Births all aian.txt"),paste0(cdc_dir, "Natality, ",curr_yr," county Births all nhpi.txt")) # Get County All AIAN / NHPI Births

state_births_nh_races <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state Births nh races.txt"),group = NULL) # Get State NH Races Births for Asian, Black, White, Two or More Races

county_births_nh_races <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county Births nh races.txt"),group = NULL) # Get County NH Races Births for Asian, Black, White, Two or More Races

state_births_swana <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state Births all swana.txt"), "swana") # Get State SWANA Births

county_births_swana <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county Births all swana.txt"), "swana") # Get County SWANA Births


# Read Low Birth Weight Data ---------------------------------------------------------

state_lbw_total <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state LBW total.txt"), "total") # Get State Total LBW

county_lbw_total <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county LBW total.txt"), "total") # Get County Total LBW

state_lbw_latino <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state LBW latino.txt"), "latino") %>% select(-c(4:5)) %>% slice(1) # Get State Latino LBW

county_lbw_latino <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county LBW latino.txt"), "latino")  %>% filter(!duplicated(county_name)) %>%  select(Notes, county_name, county_id, Births) # Get County Latino LBW

state_lbw_aian_nhpi <- read_state_data_aian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," state LBW all aian.txt"), paste0(cdc_dir, "Natality, ",curr_yr," state LBW all nhpi.txt")) # Get State All AIAN / NHPI LBW

county_lbw_aian_nhpi <- read_county_data_aian_nhpi(paste0(cdc_dir, "Natality, ",curr_yr," county LBW all aian.txt"), paste0(cdc_dir, "Natality, ",curr_yr," county LBW all nhpi.txt")) # Get County All AIAN / NHPI LBW

state_lbw_nh_races <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state LBW nh races.txt"),group = NULL) # Get State NH Races LBW for Asian, Black, White, Two or More Races

county_lbw_nh_races <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county LBW nh races.txt"),group = NULL) # Get County NH Races LBW for Asian, Black, White, Two or More Races

state_lbw_swana <- read_state_data(paste0(cdc_dir, "Natality, ",curr_yr," state LBW all swana.txt"), "swana") # Get State SWANA LBW

county_lbw_swana <- read_county_data(paste0(cdc_dir, "Natality, ",curr_yr," county LBW all swana.txt"), "swana") # Get County SWANA LBW


# Bind Data Together ---------------------------------------------------------

## births

births <- rbind(county_births_total, county_births_latino, county_births_nh_races, county_births_aian_nhpi, county_births_swana,
                state_births_total, state_births_latino, state_births_nh_races, state_births_aian_nhpi, state_births_swana)

births <- rename(births, births = "Births")


## lbw
lbw <- rbind(county_lbw_total, county_lbw_latino, county_lbw_nh_races, county_lbw_aian_nhpi, county_lbw_swana,
             state_lbw_total, state_lbw_latino, state_lbw_nh_races, state_lbw_aian_nhpi, state_lbw_swana )

lbw <- rename(lbw, lbw = "Births")

## bind birth and lbw together
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

d <- df_wide
View(d)

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

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
county_table_name <- paste0("arei_hlth_low_birthweight_county_",rc_yr)
state_table_name <- paste0("arei_hlth_low_birthweight_state_",rc_yr)

indicator <- paste0("Percentage of infants born at low birthweight (less than 2,500 grams or about 5lbs. 5oz) of all live births, by race/ethnicity of mother. This data is")
source <- paste0("US Department of Health and Human Services, Centers for Disease Control and Prevention (CDC), National Center for Health Statistics, Division of Vital Statistics, CDC WONDER Online Database (",curr_yr,"): https://wonder.cdc.gov/natality-expanded-current.html. Races include: Total, All AIAN, All NHPI, All SWANA, Latinx of any race, NH Black Alone, NH White Alone, NH Asian Alone, NH Two or More Races. QA doc: ", qa_filepath)

#send tables to postgres
# to_postgres(county_table, state_table)
