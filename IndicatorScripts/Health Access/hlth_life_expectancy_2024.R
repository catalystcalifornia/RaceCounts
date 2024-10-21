## Life Expectancy for RC v6 ##

#install packages if not already installed
list.of.packages <- c("openxlsx","tidyverse","RPostgreSQL","httr","readxl","janitor","sf","usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)


# packages --------------------------------------------------------------
library(openxlsx)
library(tidyverse)
library(RPostgreSQL)
library(httr)
library(readxl)
library(janitor)
library(sf)
library(usethis)
options(scipen = 100)

# Update each year
curr_yr <- '2019-21' # data vintage of Life Exp data
acs_year = '2021'  # pop yr used to calc state raced rates
rc_yr <- '2024'
rc_schema <- 'v6'

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")


# Get latest county health rankings life expectancy data  --------
# metadata: https://www.countyhealthrankings.org/health-data/health-outcomes/length-of-life/life-expectancy?year=2024                      # update each year   
filepath = "https://www.countyhealthrankings.org/sites/default/files/media/document/2024_county_health_release_california_data_-_v1.xlsx"  # update each year
GET(filepath, write_disk(tf <- tempfile(fileext = ".xlsx")))
df <- read_excel(tf, sheet=4)                                 # get column index range for Life Exp


# Clean data  --------
df <- df %>% select(1:27) %>% row_to_names(row_number = 1)    # update col index each yr based on prev. step
colnames(df) <- tolower(gsub(" ", "_",colnames(df)))
df_ <- df %>% select(-c(state)) %>% mutate(across(-c(1:2), as.numeric)) 
df_calcs <- df_ %>% mutate(county = ifelse(is.na(county), 'California', county)) %>% mutate(fips = ifelse(fips == '06000', '06', fips))  # fix state name/fips

# format column headers 
clean_colnames <- function(x) {
  names(x) <-  gsub("fips", "geoid", names(x), fixed = TRUE)
  names(x) <-  gsub("county", "geoname", names(x), fixed = TRUE)
  names(x) <-  gsub("life_expectancy_(", "", names(x), fixed = TRUE)
  names(x) <-  gsub("life_expectancy", "total_rate", names(x), fixed = TRUE)
  names(x) <-  gsub("hispanic_(all_races)", "latino", names(x), fixed = TRUE)
  names(x) <-  gsub(")", "_rate", names(x), fixed = TRUE)
  names(x) <-  gsub("non-hispanic", "nh", names(x), fixed = TRUE)
  names(x) <-  gsub("native_hawaiian_and_other_pacific_islander", "pacisl", names(x), fixed = TRUE)
  names(x) <-  gsub("2+_races", "twoormor", names(x), fixed = TRUE)
  names(x) <-  gsub("hispanic", "latino", names(x), fixed = TRUE)
  names(x) <-  gsub("95%_ci_-_low", "low", names(x), fixed = TRUE)
  names(x) <-  gsub("95%_ci_-_high", "high", names(x), fixed = TRUE)
  names(x) <-  ifelse(names(x) == 'low', gsub("low", "total_rate_low", names(x), fixed = TRUE), names(x))
  names(x) <-  ifelse(names(x) == 'high', gsub("high", "total_rate_high", names(x), fixed = TRUE), names(x))
  
}

colnames(df_calcs) <- clean_colnames(df_calcs)

#add empty _raw columns
empty_cols <- c("total_raw", "latino_raw", "nh_aian_raw", "nh_asian_raw", "nh_black_raw", "nh_pacisl_raw", "nh_twoormor_raw", "nh_white_raw")
df_calcs[ , empty_cols] <- NA

#  Calc moe and cv at 95 percentile ----------------------------------------------
# https://clfuture.org/toolkit/analyzing-margins-of-error-and-coefficients-of-variation#:~:text=Data%20users%20can%20compute%20a,15%25%20of%20the%20estimate).
df_calcs_moe <- df_calcs %>% mutate(
  total_rate_moe = total_rate - total_rate_low,
  latino_rate_moe = latino_rate - latino_rate_low,
  nh_aian_rate_moe = nh_aian_rate - nh_aian_rate_low, 
  nh_asian_rate_moe = nh_asian_rate - nh_asian_rate_low,
  nh_black_rate_moe = nh_black_rate - nh_black_rate_low,
  nh_pacisl_rate_moe = nh_pacisl_rate - nh_pacisl_rate_low,
  nh_twoormor_rate_moe = nh_twoormor_rate - nh_twoormor_rate_low,
  nh_white_rate_moe = nh_white_rate - nh_white_rate_low,
  
  total_rate_cv = (total_rate_moe/1.96)/total_rate * 100,
  latino_rate_cv = (latino_rate_moe/1.96)/latino_rate * 100,
  nh_aian_rate_cv = (nh_aian_rate_moe/1.96)/nh_aian_rate * 100,
  nh_asian_rate_cv = (nh_asian_rate_moe/1.96)/nh_asian_rate * 100,
  nh_black_rate_cv =(nh_black_rate_moe/1.96)/nh_black_rate * 100,
  nh_pacisl_rate_cv =(nh_pacisl_rate_moe/1.96)/nh_pacisl_rate * 100,
  nh_twoormor_rate_cv =(nh_twoormor_rate_moe/1.96)/nh_twoormor_rate * 100,
  nh_white_rate_cv = (nh_white_rate_moe/1.96)/nh_white_rate * 100
  
) 


# Screen data if CI high is > 98 *  ----------------------------------------------
# Check for high CV's
# cv_high <- df_calcs_moe  %>% select(geoid, geoname, ends_with("cv")) %>%
#   filter(
#     total_rate_cv > 10 |
#       latino_rate_cv > 10 |
#       nh_aian_rate_cv > 10 |
#       nh_asian_rate_cv > 10 |
#       nh_black_rate_cv > 10 |
#       nh_pacisl_rate_cv > 10 |
#       nh_twoormor_rate_cv > 10 |
#       nh_white_rate_cv > 10
#   ) 

df_screened <- df_calcs_moe %>% 
  mutate(
    total_rate = ifelse(total_rate_high > 98, NA, total_rate),
    latino_rate = ifelse(latino_rate_high > 98, NA, latino_rate),
    nh_aian_rate = ifelse(nh_aian_rate_high > 98, NA, nh_aian_rate),
    nh_asian_rate = ifelse(nh_asian_rate_high > 98, NA,nh_asian_rate),
    nh_black_rate = ifelse(nh_black_rate_high > 98, NA, nh_black_rate),
    nh_pacisl_rate = ifelse(nh_pacisl_rate_high > 98, NA, nh_pacisl_rate),
    nh_twoormor_rate = ifelse(nh_twoormor_rate_high > 98, NA, nh_twoormor_rate),
    nh_white_rate = ifelse(nh_white_rate_high > 98, NA, nh_white_rate)
  )

# select only the columns we need: geoid, geoname, raw, and rate cols
df_screened <- df_screened %>% select(-ends_with(c("low", "high", "moe", "cv")))

# remove two or more rate, there appears to be no data 
two_ormor_rate <- df_screened %>% filter(!is.na(nh_twoormor_rate))
df_screened <- df_screened %>% select(-starts_with("nh_twoormor"))


# Get state raced data from CDPH 2024 Summary Report ------------------------------------------------
#### Page 16 of https://www.cdph.ca.gov/Programs/OPP/CDPH%20Document%20Library/California-State-of-Public-Health-Summary-Report-2024.pdf
raceeth <-  c("nh_aian_rate", "nh_asian_rate", "nh_black_rate", "latino_rate", "nh_pacisl_rate", "nh_white_rate")
rate <- c(76.8, 85.1, 73.6, 80.3, 74.8, 80.5)
state_data <- data.frame(raceeth, rate) %>% mutate(geoid = '06')
state_wide <- state_data %>% pivot_wider(names_from = raceeth, values_from = rate)

df_all <- df_screened %>% rows_update(state_wide, by = "geoid")  # fill in missing state raced rates

# make d 
d <- df_all


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'
d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
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
view(county_table)

###update info for postgres tables###
county_table_name <- paste0("arei_hlth_life_expectancy_county_", rc_yr)
state_table_name <- paste0("arei_hlth_life_expectancy_state_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". The number of years a baby can be expected to live if they experience current age-specific mortality rates throughout their life. This data is")

source <- paste0("from the ",rc_yr," Robert Wood Johnson Foundation County Health Rankings that use ",curr_yr," life exp data. 
https://www.countyhealthrankings.org/health-data/california/data-and-resources (and for state raced data only - https://www.cdph.ca.gov/Programs/OPP/CDPH%20Document%20Library/California-State-of-Public-Health-Summary-Report-2024.pdf). QA Doc: W:/Project/RACE COUNTS/2024_v6/Health Access/QA_Life_Expectancy_2024.docx")


#send tables to postgres
to_postgres(county_table, state_table)
