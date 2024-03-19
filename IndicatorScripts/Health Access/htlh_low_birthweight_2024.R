### Low Birthweight RC v6 ###

##install packages if not already installed ------------------------------
list.of.packages <- c("readr","dplyr","tidyr","RPostgreSQL","tidycensus","tidyverse","usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

library(readr)
library(dplyr)
library(tidyr)
library(RPostgreSQL)
library(tidycensus)
library(tidyverse)
library(usethis)

source("W:\\RDA Team\\R\\credentials_source.R")
setwd("W:/Data/Health/Births/CDC/2016-22") # SET WD

# Functions to read State data --------------------------------------------

read_state_data <- function(x, group) {
  y <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    State.of.Residence = col_character(),
    State.of.Residence.Code = col_character(),
    Births = col_integer()))
  
  y <- rename(y, county_name = `State of Residence`, #will rename after rbind
              county_id = `State of Residence Code`) #will rename after rbind
  
  
  ##### for aian_nhpi and nh races #### 
  if(is.null(group)) {
    
    y$Notes <- y$`Mother's Single Race 6`
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
read_state_data_aian_nhpi <- function(x, group) {
  y <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    State.of.Residence = col_character(),
    State.of.Residence.Code = col_character(),
    Births = col_integer()))
  
  y <- rename(y, county_name = `State of Residence`, #will rename after rbind
              county_id = `State of Residence Code`) #will rename after rbind
  
  y$Notes <- y$`Mother's Single/Multi Race 31`
  y <- y[,-c(4:5)]
  
  y <- y %>% filter(!is.na(Notes))

  y$aian <- ifelse(grepl("AIAN",y$Notes), 1, 0)
  y$pacisl <- ifelse(grepl("NHOPI",y$Notes), 1, 0)
  
  aian_ <- y %>% group_by(county_id, county_name, aian) %>% summarise(Births = sum(Births)) %>% mutate(Notes = 'American Indian or Alaska Native') %>% filter(aian == 1)
  aian_ <- aian_ %>% select(Notes, county_name, county_id, Births)
  pacisl_ <- y %>% group_by(county_name, county_id, pacisl) %>% summarise(Births = sum(Births)) %>% mutate(Notes = 'Native Hawaiian or Other Pacific Islander') %>% filter(pacisl == 1)
  pacisl_ <- pacisl_ %>% select(Notes, county_name, county_id, Births)
  
  y <- rbind(aian_,pacisl_) # put all aian and pacisl back together
  
  return(y)
}

# Functions to read County data --------------------------------------------

read_county_data <- function(x, group) {
  
  y <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    County.of.Residence = col_character(),
    County.of.Residence.Code = col_character(),
    Births = col_integer()))
  
  y <- rename(y, county_name = `County of Residence`,
              county_id = `County of Residence Code`)
  
  ##### for aian_nhpi and nh races #### 
  
  if(is.null(group)) {
    
    y$Notes <- y$`Mother's Single Race 6`
    
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
read_county_data_aian_nhpi <- function(x, group) {
  
  y <- read_delim(file = x, delim = "\t", col_types = cols(
    Notes = col_character(),
    County.of.Residence = col_character(),
    County.of.Residence.Code = col_character(),
    Births = col_integer()))
  
  y <- rename(y, county_name = `County of Residence`,
              county_id = `County of Residence Code`)
  
    y$Notes <- y$`Mother's Single/Multi Race 31`
    y <- y[,-c(4:5)]
    y <- y %>% filter(!is.na(Notes))
    y <- y %>% filter(county_name != "Unidentified Counties, CA")
    
    y$aian <- ifelse(grepl("AIAN",y$Notes), 1, 0)
    y$pacisl <- ifelse(grepl("NHOPI",y$Notes), 1, 0)
    
    aian_ <- y %>% group_by(county_id, county_name, aian) %>% summarise(Births = sum(Births)) %>% mutate(Notes = 'American Indian or Alaska Native') %>% filter(aian == 1)
    aian_ <- aian_ %>% select(Notes, county_name, county_id, Births)
    pacisl_ <- y %>% group_by(county_name, county_id, pacisl) %>% summarise(Births = sum(Births)) %>% mutate(Notes = 'Native Hawaiian or Other Pacific Islander') %>% filter(pacisl == 1)
    pacisl_ <- pacisl_ %>% select(Notes, county_name, county_id, Births)
    
    y <- rbind(aian_,pacisl_)
    
return(y)
    
}


# Read Birth Data ---------------------------------------------------------
state_births_total <- read_state_data("Natality, 2016-2022 state Births total.txt", "Total") # Get State Total Births

county_births_total <- read_county_data("Natality, 2016-2022 county Births total.txt", "Total") # Get County Total Births

state_births_latino <- read_state_data("Natality, 2016-2022 state Births latino.txt", "Latino") %>% filter(!duplicated(county_name)) %>% select(Notes, county_name, county_id, Births) # Get State Latino Births

county_births_latino <- read_county_data("Natality, 2016-2022 county Births latino.txt", "Latino")  %>% filter(!duplicated(county_name)) %>% select(Notes, county_name, county_id, Births) # Get County Latino Births

state_births_aian_nhpi <- read_state_data_aian_nhpi("Natality, 2016-2022 state Births all aian and nhpi.txt", group = NULL) # Get State AIAN NHPI Births

county_births_aian_nhpi <- read_county_data_aian_nhpi("Natality, 2016-2022 county Births all aian and nhpi.txt",group = NULL) # Get County AIAN NHPI Births

state_births_nh_races <- read_state_data("Natality, 2016-2022 state Births nh races.txt",group = NULL) # Get State NH Races Births for Asian, Black, White, Two or More Races

county_births_nh_races <- read_county_data("Natality, 2016-2022 county Births nh races.txt",group = NULL) # Get County NH Races Births for Asian, Black, White, Two or More Races

state_births_swana <- read_state_data("Natality, 2016-2022 state Births all swana.txt", "SWANA") # Get State SWANA Births

county_births_swana <- read_county_data("Natality, 2016-2022 county Births all swana.txt", "SWANA") # Get County SWANA Births


# Read Low Birth Weight Data ---------------------------------------------------------

state_lbw_total <-  read_state_data("Natality, 2016-2022 state LBW total.txt", "Total") # Get State Total LBW

county_lbw_total <- read_county_data("Natality, 2016-2022 county LBW total.txt", "Total") # Get County Total LBW

state_lbw_latino <- read_state_data("Natality, 2016-2022 state LBW latino.txt", "Latino") %>% select(-c(4:5)) %>% slice(1) # Get State Latino LBW

county_lbw_latino <- read_county_data("Natality, 2016-2022 county LBW latino.txt", "Latino")  %>% filter(!duplicated(county_name)) %>%  select(Notes, county_name, county_id, Births) # Get County Latino LBW

state_lbw_aian_nhpi <-  read_state_data_aian_nhpi("Natality, 2016-2022 state LBW all aian and nhpi.txt",group = NULL) # Get State AIAN NHPI LBW

county_lbw_aian_nhpi <-  read_county_data_aian_nhpi("Natality, 2016-2022 county LBW all aian and nhpi.txt",group = NULL) # Get County AIAN NHPI LBW

state_lbw_nh_races <-  read_state_data("Natality, 2016-2022 state LBW nh races.txt",group = NULL) # Get State NH Races LBW for Asian, Black, White, Two or More Races

county_lbw_nh_races <- read_county_data("Natality, 2016-2022 county LBW nh races.txt",group = NULL) # Get County NH Races LBW for Asian, Black, White, Two or More Races

state_lbw_swana <-  read_state_data("Natality, 2016-2022 state LBW all swana.txt", "SWANA") # Get State SWANA LBW

county_lbw_swana <- read_county_data("Natality, 2016-2022 county LBW all swana.txt", "SWANA") # Get County SWANA LBW


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
df <- left_join(lbw, births) %>% mutate(county_name = gsub(" County, CA", "", county_name)) %>% relocate(county_id, .before = county_name)

#calculate Rate ---------------------------------------------------------
df <- mutate(df, rate = lbw/births*100)

#Format race/ethnic groups. Pivot wider ---------------------------------------------------------


#format Notes, lbw to pre-headers
df$Notes <- tolower(df$Notes)

# rename race/ethnicity
df$Notes <- gsub("white", "nh_white", df$Notes)
df$Notes <- gsub("asian", "nh_asian", df$Notes)
df$Notes <- gsub("black or african american", "nh_black", df$Notes)
df$Notes <- gsub("more than one race", "nh_twoormor", df$Notes)
df$Notes <- gsub("american indian or alaska native", "aian", df$Notes)
df$Notes <- gsub("native hawaiian or other pacific islander", "pacisl", df$Notes)
df <- rename(df, raw = "lbw")

#pivot
df_wide <- df %>% pivot_wider(names_from = Notes, names_glue = "{Notes}_{.value}", values_from = c(raw, births, rate))
df_wide <- df_wide %>% rename(geoid = county_id, geoname = county_name)

d <- df_wide


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

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

###update info for postgres tables###
county_table_name <- "arei_hlth_low_birthweight_county_2024"
state_table_name <- "arei_hlth_low_birthweight_state_2024"
rc_schema <- 'v6'

indicator <- "Percentage of infants born at low birthweight (less than 2,500 grams or about 5lbs. 5oz) of all live births, by race/ethnicity of mother. This data is"
source <- "US Department of Health and Human Services, Centers for Disease Control and Prevention (CDC), National Center for Health Statistics, Division of Vital Statistics, CDC WONDER Online Database (2016-2022): https://wonder.cdc.gov/natality-expanded-current.html"

#send tables to postgres
# to_postgres(county_table, state_table)

