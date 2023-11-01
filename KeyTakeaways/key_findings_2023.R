## RC Automated Findings v5 ##

# Install packages if not already installed
list.of.packages <- c("usethis","dplyr","data.table", "tidycensus", "sf", "RPostgreSQL",
                      "stringr", "tidyr", "matrixStats", "tidyverse", "writexl")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#Load libraries
library(usethis)
library(dplyr)
library(data.table) 
library(tidycensus) # not used?
library(sf)
library(RPostgreSQL) # not used?
library(stringr) # not used?
library(tidyr)
library(matrixStats) # not used?            rowMaxs()
library(tidyverse) # not used?              reduce()
library(writexl) # not used?                to create csv
options(scipen=999)

# load the PostgreSQL driver & create connection for database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# Function to clean county/state data -------------------------------------
clean_data <- function(county, state, issue, indicator) {
  
  df_county <- county %>% select(county_id, county_name, ends_with("disparity_z"), values_count) %>%
    pivot_longer(cols = ends_with("disparity_z"),
                 names_to = "race",
                 values_to = "disparity_z_score"
    ) %>%
    
    rename(geoid = county_id,
           geoname = county_name) %>%
    
    mutate(
      issue = issue,
      indicator = indicator,
      geoname = paste0(geoname, " County"),
      race = (ifelse(race == 'disparity_z', 'total', race)),
      race = gsub('_disparity_z', '', race)) %>% 
    
    full_join(
      
      county %>% select(county_id, county_name, ends_with("performance_z"), values_count) %>%
        pivot_longer(cols = ends_with("performance_z"), 
                     names_to = "race",
                     values_to = "performance_z_score") %>%
        
        rename(geoid = county_id,
               geoname = county_name) %>% 
        
        mutate(
          issue = issue,
          indicator = indicator,
          geoname = paste0(geoname, " County"),
          race = (ifelse(race == 'performance_z', 'total', race)),
          race = gsub('_performance_z', '', race))
      
    ) %>% full_join(
      
      county %>% select(county_id, county_name, asbest, ends_with("_rate"), values_count) %>%
        pivot_longer(cols = ends_with("_rate"),
                     names_to = "race",
                     values_to = "rate") %>%
        
        rename(geoid = county_id,
               geoname = county_name) %>%
        
        mutate(
          issue = issue,
          indicator = indicator,
          geoname = paste0(geoname, " County"),
          race = (ifelse(race == 'rate', 'total', race)),
          race = gsub('_rate', '', race)) 
      
    )  %>%
    select(geoid, geoname, issue, indicator, race, asbest, rate, disparity_z_score, performance_z_score, values_count)
  
  df_state <- state %>% select(state_id, state_name, ends_with("disparity_z"), values_count) %>%
    pivot_longer(cols = ends_with("disparity_z"),
                 names_to = "race",
                 values_to = "disparity_z_score"
    ) %>%
    
    rename(geoid = state_id,
           geoname = state_name) %>%
    
    mutate(
      issue = issue,
      indicator = indicator,
      race = gsub('_disparity_z', '', race),
      performance_z_score = NA) %>% select(geoid, geoname, issue, indicator, race, disparity_z_score, performance_z_score, values_count) %>%
    
    full_join(
      
      state %>% select(state_id, state_name, asbest, ends_with("_rate"), values_count) %>%
        pivot_longer(cols = ends_with("_rate"),
                     names_to = "race",
                     values_to = "rate") %>%
        
        rename(geoid = state_id,
               geoname = state_name) %>%
        
        mutate(
          issue = issue,
          indicator = indicator,
          race = (ifelse(race == 'rate', 'total', race)),
          race = gsub('_rate', '', race))
      
    ) 
  
  df <- rbind(df_county, df_state) # combine county/state tables for same indicator
  
  df <- df %>% mutate(geolevel = ifelse(geoname == "California", "state", "county")) # add geolevel col
  
  df$race_generic = gsub('nh_', '', df$race) # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
  
  return(df)
}

# GET CRIME & JUSTICE DATA ---------------------------------------------------
## Import tables. Copy from current index view.
## county indicator tables
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_crim_incarceration_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_crim_perception_of_safety_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_crim_status_offenses_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_crim_use_of_force_county_2023")

## state indicator tables - in another window, find/replace county table lines above: 'county' to 'state' and 'c_' to 's_'.
s_1 <- st_read(con, query = "SELECT * FROM v5.arei_crim_incarceration_state_2023")
s_2 <- st_read(con, query = "SELECT * FROM v5.arei_crim_perception_of_safety_state_2023")
s_3 <- st_read(con, query = "SELECT * FROM v5.arei_crim_status_offenses_state_2023")
s_4 <- st_read(con, query = "SELECT * FROM v5.arei_crim_use_of_force_state_2023")

## update issue value. define variable names for clean_data_z function. Copy from current index view.
issue <- 'crim'
varname1 <- 'incarceration'
varname2 <- 'safety'
varname3 <- 'offenses'
varname4 <- 'force'

## run clean_data function on each indicator table. adjust for number of indicators in specific issue area.
df1 <- clean_data(c_1, s_1, issue, varname1)
df2 <- clean_data(c_2, s_2, issue, varname2)
df3 <- clean_data(c_3, s_3, issue, varname3)
df4 <- clean_data(c_4, s_4, issue, varname4)

## combine the cleaned indicator tables, add geolevel field
crime <- bind_rows(df1, df2, df3, df4) # adjust for number of indicators in specific issue area.

# GET DEMOCRACY DATA ---------------------------------------------------
## Import tables. Copy from current index view.
## county indicator tables
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_demo_census_participation_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_demo_diversity_of_candidates_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_demo_diversity_of_electeds_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_demo_registered_voters_county_2023")
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_demo_voting_midterm_county_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_demo_voting_presidential_county_2023")

## state indicator tables - in another window, find/replace 'county' for 'state'.
s_1 <- st_read(con, query = "SELECT * FROM v5.arei_demo_census_participation_state_2023")
s_2 <- st_read(con, query = "SELECT * FROM v5.arei_demo_diversity_of_candidates_state_2023")
s_3 <- st_read(con, query = "SELECT * FROM v5.arei_demo_diversity_of_electeds_state_2023")
s_4 <- st_read(con, query = "SELECT * FROM v5.arei_demo_registered_voters_state_2023")
s_5 <- st_read(con, query = "SELECT * FROM v5.arei_demo_voting_midterm_state_2023")
s_6 <- st_read(con, query = "SELECT * FROM v5.arei_demo_voting_presidential_state_2023")

## update issue value. define variable names for clean_data_z function. Copy from current index view.
issue <- 'demo'
varname1 <- 'census'
varname2 <- 'candidate'
varname3 <- 'elected'
varname4 <- 'voter'
varname5 <- 'midterm'
varname6 <- 'president'

## run clean_data function on each indicator table. adjust for number of indicators in specific issue area.
df1 <- clean_data(c_1, s_1, issue, varname1)
df2 <- clean_data(c_2, s_2, issue, varname2)
df3 <- clean_data(c_3, s_3, issue, varname3)
df4 <- clean_data(c_4, s_4, issue, varname4)
df5 <- clean_data(c_5, s_5, issue, varname5)
df6 <- clean_data(c_6, s_6, issue, varname6)


## combine the cleaned indicator tables, add geolevel field
democracy <- bind_rows(df1, df2, df3, df4, df5, df6) # adjust for number of indicators in specific issue area.

# GET ECONOMIC DATA ---------------------------------------------------
## Import tables. Copy from current index view. NOTE: I added in Living Wage bc it's not included in index.
## county indicator tables
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_econ_connected_youth_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_econ_employment_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_econ_internet_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_econ_officials_county_2023")
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_econ_per_capita_income_county_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_econ_real_cost_measure_county_2023")
c_7 <- st_read(con, query = "SELECT * FROM v5.arei_econ_living_wage_county_2023")

## state indicator tables - in another window, find/replace 'county' for 'state'.
s_1 <- st_read(con, query = "SELECT * FROM v5.arei_econ_connected_youth_state_2023")
s_2 <- st_read(con, query = "SELECT * FROM v5.arei_econ_employment_state_2023")
s_3 <- st_read(con, query = "SELECT * FROM v5.arei_econ_internet_state_2023")
s_4 <- st_read(con, query = "SELECT * FROM v5.arei_econ_officials_state_2023")
s_5 <- st_read(con, query = "SELECT * FROM v5.arei_econ_per_capita_income_state_2023")
s_6 <- st_read(con, query = "SELECT * FROM v5.arei_econ_real_cost_measure_state_2023")
s_7 <- st_read(con, query = "SELECT * FROM v5.arei_econ_living_wage_state_2023")

## update issue value. define variable names for clean_data_z function. Copy from current index view. Note: I added in Liv Wage bc it's not in index.
issue <- 'econ'
varname1 <- 'connected'
varname2 <- 'employ'
varname3 <- 'internet'
varname4 <- 'officials'
varname5 <- 'percap'
varname6 <- 'realcost'
varname7 <- 'livwage'


## run clean_data function on each indicator table. adjust for number of indicators in specific issue area.
df1 <- clean_data(c_1, s_1, issue, varname1)
df2 <- clean_data(c_2, s_2, issue, varname2)
df3 <- clean_data(c_3, s_3, issue, varname3)
df4 <- clean_data(c_4, s_4, issue, varname4)
df5 <- clean_data(c_5, s_5, issue, varname5)
df6 <- clean_data(c_6, s_6, issue, varname6)
df7 <- clean_data(c_7, s_7, issue, varname7)


## combine the cleaned indicator tables, add geolevel field
economic <- bind_rows(df1, df2, df3, df4, df5, df6, df7) # adjust for number of indicators in specific issue area.


# GET EDUCATION DATA ---------------------------------------------------
## Import tables. Copy from current index view.
## county indicator tables
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_educ_chronic_absenteeism_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_educ_hs_grad_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_educ_gr3_ela_scores_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_educ_gr3_math_scores_county_2023")
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_educ_suspension_county_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_educ_ece_access_county_2023")
c_7 <- st_read(con, query = "SELECT * FROM v5.arei_educ_staff_diversity_county_2023")

## state indicator tables - in another window, find/replace 'county' for 'state'.
s_1 <- st_read(con, query = "SELECT * FROM v5.arei_educ_chronic_absenteeism_state_2023")
s_2 <- st_read(con, query = "SELECT * FROM v5.arei_educ_hs_grad_state_2023")
s_3 <- st_read(con, query = "SELECT * FROM v5.arei_educ_gr3_ela_scores_state_2023")
s_4 <- st_read(con, query = "SELECT * FROM v5.arei_educ_gr3_math_scores_state_2023")
s_5 <- st_read(con, query = "SELECT * FROM v5.arei_educ_suspension_state_2023")
s_6 <- st_read(con, query = "SELECT * FROM v5.arei_educ_ece_access_state_2023")
s_7 <- st_read(con, query = "SELECT * FROM v5.arei_educ_staff_diversity_state_2023")

## update issue value. define variable names for clean_data_z function. Copy from current index view.
issue <- 'educ'
varname1 <- 'abst'
varname2 <- 'grad'
varname3 <- 'ela'
varname4 <- 'math'
varname5 <- 'susp'
varname6 <- 'ece'
varname7 <- 'diver'

## run clean_data function on each indicator table. adjust for number of indicators in specific issue area.
df1 <- clean_data(c_1, s_1, issue, varname1)
df2 <- clean_data(c_2, s_2, issue, varname2)
df3 <- clean_data(c_3, s_3, issue, varname3)
df4 <- clean_data(c_4, s_4, issue, varname4)
df5 <- clean_data(c_5, s_5, issue, varname5)
df6 <- clean_data(c_6, s_6, issue, varname6)
df7 <- clean_data(c_7, s_7, issue, varname7)


## combine the cleaned indicator tables, add geolevel field
education <- bind_rows(df1, df2, df3, df4, df5, df6, df7) # adjust for number of indicators in specific issue area.


# GET HBEN DATA ---------------------------------------------------
## Import tables. Copy from current index view.
## county indicator tables
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_hben_drinking_water_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_hben_food_access_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_hben_haz_weighted_avg_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_hben_toxic_release_county_2023")
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_hben_asthma_county_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_hben_lack_of_greenspace_county_2023")

## state indicator tables - in another window, find/replace 'county' for 'state'.
s_1 <- st_read(con, query = "SELECT * FROM v5.arei_hben_drinking_water_state_2023")
s_2 <- st_read(con, query = "SELECT * FROM v5.arei_hben_food_access_state_2023")
s_3 <- st_read(con, query = "SELECT * FROM v5.arei_hben_haz_weighted_avg_state_2023")
s_4 <- st_read(con, query = "SELECT * FROM v5.arei_hben_toxic_release_state_2023")
s_5 <- st_read(con, query = "SELECT * FROM v5.arei_hben_asthma_state_2023")
s_6 <- st_read(con, query = "SELECT * FROM v5.arei_hben_lack_of_greenspace_state_2023")

## update issue value. define variable names for clean_data_z function. Copy from current index view.
issue <- 'hben'
varname1 <- 'water'
varname2 <- 'food'
varname3 <- 'hazard'
varname4 <- 'toxic'
varname5 <- 'asthma'
varname6 <- 'green'

## run clean_data function on each indicator table. adjust for number of indicators in specific issue area.
df1 <- clean_data(c_1, s_1, issue, varname1)
df2 <- clean_data(c_2, s_2, issue, varname2)
df3 <- clean_data(c_3, s_3, issue, varname3)
df4 <- clean_data(c_4, s_4, issue, varname4)
df5 <- clean_data(c_5, s_5, issue, varname5)
df6 <- clean_data(c_6, s_6, issue, varname6)


## combine the cleaned indicator tables, add geolevel field
hbe <- bind_rows(df1, df2, df3, df4, df5, df6) # adjust for number of indicators in specific issue area.


# GET HEALTH DATA ---------------------------------------------------
## Import tables. Copy from current index view.
## county indicator tables
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_got_help_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_health_insurance_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_life_expectancy_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_low_birthweight_county_2023")
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_usual_source_of_care_county_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_preventable_hospitalizations_county_2023")


## state indicator tables - in another window, find/replace 'county' for 'state'.
s_1 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_got_help_state_2023")
s_2 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_health_insurance_state_2023")
s_3 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_life_expectancy_state_2023")
s_4 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_low_birthweight_state_2023")
s_5 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_usual_source_of_care_state_2023")
s_6 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_preventable_hospitalizations_state_2023")


## update issue value. define variable names for clean_data_z function. Copy from current index view.
issue <- 'hlth'
varname1 <- 'help'
varname2 <- 'insur'
varname3 <- 'life'
varname4 <- 'bwt'
varname5 <- 'usoc'
varname6 <- 'hosp'

## run clean_data function on each indicator table. adjust for number of indicators in specific issue area.
df1 <- clean_data(c_1, s_1, issue, varname1)
df2 <- clean_data(c_2, s_2, issue, varname2)
df3 <- clean_data(c_3, s_3, issue, varname3)
df4 <- clean_data(c_4, s_4, issue, varname4)
df5 <- clean_data(c_5, s_5, issue, varname5)
df6 <- clean_data(c_6, s_6, issue, varname6)


## combine the cleaned indicator tables, add geolevel field
health <- bind_rows(df1, df2, df3, df4, df5, df6) # adjust for number of indicators in specific issue area.


# GET HOUSING DATA ---------------------------------------------------
## Import tables. Copy from current index view.
## county indicator tables
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_hous_cost_burden_owner_county_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_hous_cost_burden_renter_county_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_hous_denied_mortgages_county_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_hous_eviction_filing_rate_county_2023") 
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_hous_foreclosure_county_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_hous_homeownership_county_2023")
c_7 <- st_read(con, query = "SELECT * FROM v5.arei_hous_overcrowded_county_2023")
c_8 <- st_read(con, query = "SELECT * FROM v5.arei_hous_housing_quality_county_2023")
c_9 <- st_read(con, query = "SELECT * FROM v5.arei_hous_student_homelessness_county_2023")
c_10 <- st_read(con, query = "SELECT * FROM v5.arei_hous_subprime_county_2023")

## state indicator tables - in another window, find/replace 'county' for 'state'.
s_1 <- st_read(con, query = "SELECT * FROM v5.arei_hous_cost_burden_owner_state_2023")
s_2 <- st_read(con, query = "SELECT * FROM v5.arei_hous_cost_burden_renter_state_2023")
s_3 <- st_read(con, query = "SELECT * FROM v5.arei_hous_denied_mortgages_state_2023")
s_4 <- st_read(con, query = "SELECT * FROM v5.arei_hous_eviction_filing_rate_state_2023")
s_5 <- st_read(con, query = "SELECT * FROM v5.arei_hous_foreclosure_state_2023")
s_6 <- st_read(con, query = "SELECT * FROM v5.arei_hous_homeownership_state_2023")
s_7 <- st_read(con, query = "SELECT * FROM v5.arei_hous_overcrowded_state_2023")
s_8 <- st_read(con, query = "SELECT * FROM v5.arei_hous_housing_quality_state_2023")
s_9 <- st_read(con, query = "SELECT * FROM v5.arei_hous_student_homelessness_state_2023")
s_10 <- st_read(con, query = "SELECT * FROM v5.arei_hous_subprime_state_2023")

## update issue value. define variable names for clean_data_z function. Copy from current index view.
issue <- 'hous'
varname1 <- 'burden_own'
varname2 <- 'burden_rent'
varname3 <- 'denied'
varname4 <- 'eviction'
varname5 <- 'forecl'
varname6 <- 'homeown'
varname7 <- 'overcrowded'
varname8 <- 'quality'
varname9 <- 'homeless'
varname10 <- 'subprime'

## run clean_data function on each indicator table. adjust for number of indicators in specific issue area.
df1 <- clean_data(c_1, s_1, issue, varname1)
df2 <- clean_data(c_2, s_2, issue, varname2)
df3 <- clean_data(c_3, s_3, issue, varname3)
df4 <- clean_data(c_4, s_4, issue, varname4)
df5 <- clean_data(c_5, s_5, issue, varname5)
df6 <- clean_data(c_6, s_6, issue, varname6)
df7 <- clean_data(c_7, s_7, issue, varname7)
df8 <- clean_data(c_8, s_8, issue, varname8)
df9 <- clean_data(c_9, s_9, issue, varname9)
df10 <- clean_data(c_10, s_10, issue, varname10)

## combine the cleaned indicator tables, add geolevel field
housing <- bind_rows(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10) # adjust for number of indicators in specific issue area.


## Combine all issue area tables into one ----------------------------------

# NOTE: when you call this df in your code chunk(s), rename it before running code on it bc it takes a LONG time to run again...
df <- bind_rows(crime, democracy, economic, education, hbe, health, housing)
# NOTE: when you call this df in your code chunk(s), rename it before running code on it bc it takes a LONG time to run again...

# Get long form race names for findings ------------------------------------------------
race_generic <- unique(df$race_generic)
long_name <- c("Total", "Asian / Pacific Islander", "Black", "Latinx", "American Indian / Alaska Native", "White", "Asian", "Two or More Races", "Native Hawaiian / Pacific Islander", "Other Race", "Filipinx")
race_names <- data.frame(race_generic, long_name)

# Create indicator long name df -------------------------------------------
indicator <- c("Employment","Living Wage","Per Capita Income","Cost-of-Living Adjusted Poverty","Overcrowded Housing", "Connected Youth","Officials and Managers",
               "Internet Access","Life Expectancy","Health Insurance","Preventable Hospitalizations","Low Birthweight","Usual Source of Care","Got Help",
               "High School Graduation","3rd Grade English Proficiency","3rd Grade Math Proficiency","Suspensions","Early Childhood Education Access",
               "Teacher & Staff Diversity","Chronic Absenteeism","Subprime Mortgages","Housing Quality","Renter Cost Burden","Homeowner Cost Burden","Foreclosures",
               "Denied Mortgages","Homeownership","Student Homelessness","Evictions","Diversity of Electeds","Diversity of Candidates","Voting in Presidential Elections",
               "Voting in Midterm Elections","Voter Registration","Census Participation","Arrests for Status Offenses","Use of Force","Incarceration","Perception of Safety",
               "Drinking Water Contaminants","Food Access","Proximity to Hazards","Toxic Releases from Facilities","Asthma","Lack of Greenspace")             

indicator_short <- c("employ","livwage","percap","realcost","overcrowded","connected","officials","internet","life","insur","hosp","bwt","usoc","help","grad","ela","math",     
                     "susp","ece","diver","abst","subprime","quality","burden_rent","burden_own","forecl","denied","homeown","homeless","eviction","elected","candidate",
                     "president","midterm","voter","census","offenses","force","incarceration","safety","water","food","hazard","toxic","asthma","green")

indicator <- data.frame(indicator, indicator_short)

### This section creates findings like: -----------------------------------------------------
## Race page: "Kern's Latinx residents have the worst rates for 7 of the 42 RACE COUNTS indicators." 
## and Place pages: "Across indicators, Contra Costa County Black residents are most impacted by racial disparity."
### Step 1: Get worst raced rate for each indicator and pull in race name grouped by geo + indicator
### Step 2: Count number of times each race has the worst rate grouped by geo
### Step 3: Generate sentences with # indicators with worst rates each race has out of all indicators grouped by geo + race
### Step 4: Generate sentences with # indicators with best rates each race has out of all indicators grouped by geo + race
### Step 5: Generate sentences identifying which race(s) faces the most racial disparity grouped by geo
### Step 6: Decide if we need to suppress/screen out findings for counties with few ID's like Alpine

# copy df before running any code
df_lf <- filter(df, race != 'total')   # remove total rates bc all findings in this section are raced

# duplicate API rows, assigning one set race_generic Asian and the other set PacIsl
api_asian <- filter(df_lf, race_generic == 'api') %>% mutate(race_generic = 'asian')
api_pacisl <- filter(df_lf, race_generic == 'api') %>% mutate(race_generic = 'pacisl')
df_lf <- filter(df_lf, race_generic != 'api')       # remove api rows
df_lf <- bind_rows(df_lf, api_asian, api_pacisl)    # add back api rows as asian AND pacisl rows

### Table counting number of non-NA rates per race+geo combo, used for screening best/worst counts later ### 
bestworst_screen <- #subset(df_lf, !race_generic %in% c('filipino', 'other', 'twoormor')) %>%           # keep only races we have RACE pages for on RC.org
  df_lf %>% group_by(geoid, race_generic) %>% summarise(rate_count = sum(!is.na(rate)))

### Table counting number of indicators with ID's (multiple raced disp_z scores) per geo, used for screening most impacted later ### 
impact_screen <- df_lf %>% group_by(geoid, geoname, indicator) %>% summarise(count = sum(!is.na(disparity_z_score)))
impact_screen <- filter(impact_screen, count > 1) %>% group_by(geoid, geoname) %>% summarise(id_count = n())    

### Worst rates - RACE PAGE ###
filter_nonRC <- #df_lf %>% filter(race_generic %in% c('filipino', 'other', 'twoormor') & values_count == "2" & !is.na(rate)) %>%
  df_lf %>% filter(values_count == "2" & !is.na(rate)) %>%
  select(geoid, geoname, issue, indicator) %>% mutate(remove = 1) ## create df of observations with non-RC groups as one of the only two rates. There are 2 observations

worst_table  <- df_lf %>% left_join(filter_nonRC) %>%   
  #filter(is.na(remove) & !race_generic %in% c('filipino', 'other', 'twoormor') & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate AND where 1 of 2 raced rates is a non-RC Race page group
  filter(is.na(remove) & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate AND where 1 of 2 raced rates is a non-RC Race page group
  group_by(geoid, indicator) %>% top_n(1, disparity_z_score) %>% # get worst raced rate by geo+indicator
  rename(worst_rate = race_generic) %>% select(-remove)

worst_table2 <- #subset(df_lf, (!race_generic %in% c('filipino', 'other', 'twoormor')) & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate
  subset(df_lf, values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate
  left_join(select(worst_table, geoid, indicator, worst_rate), by = c("geoid", "indicator")) %>%
  mutate(worst = ifelse((race_generic == worst_rate), 1, 0)) %>%             
  group_by(geoid, geoname, race_generic) %>% summarise(count = sum(worst, na.rm = TRUE)) %>%
  left_join(race_names, by = "race_generic") %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic"))
worst_table2 <- worst_table2 %>% mutate(count = ifelse(is.na(count) & rate_count > 0, 0, count)) 


worst_rate_count <- filter(worst_table2, !is.na(rate_count)) %>% mutate(geoname = gsub(' County', '', geoname), finding_type = 'worst count', findings_pos = 2) %>% 
  mutate(finding = ifelse(rate_count > 5, paste0(geoname, " County's ", long_name, " residents have the worst rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), paste0("Data for ", long_name, " residents of ", geoname, " County is too limited for this analysis.")))

### Best rates - RACE PAGE ###
#### Note: Code differs from Worst rates to account for when min is best and there is raced rate = 0, so we cannot use disparity_z for min asbest indicators ####
best_table <- #subset(df_lf, (!race_generic %in% c('filipino', 'other', 'twoormor')) & values_count > 1 & !is.na(rate)) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate
  subset(df_lf, values_count > 1 & !is.na(rate)) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate              
  select(c(geoid, issue, indicator, values_count, geolevel, asbest, rate, race_generic)) %>% 
  group_by(geoid, issue, indicator, values_count, geolevel, asbest) %>% 
  mutate(best_rank = ifelse(asbest == 'min', dense_rank(rate), dense_rank(-rate)))  %>% # use dense_rank to give ties the same rank, and all integer ranks
  mutate(best_rate = ifelse(best_rank == 1, race_generic, ""))    # identify race with best rate using best_rank
best_table <- best_table %>% left_join(filter_nonRC) %>% filter(is.na(remove)) %>% select(-geoname, -remove) # remove non-RC best group rates. Total of 2 obs


best_table2 <- #subset(df_lf, (!race_generic %in% c('filipino', 'other', 'twoormor')) & values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate 
  subset(df_lf, values_count > 1) %>%  # keep only races we have RACE pages for on RC.org, drop indicators with only 1 raced rate 
  left_join(select(best_table, geoid, indicator, best_rate), by = c("geoid", "indicator")) %>%
  mutate(best = ifelse((race_generic == best_rate), 1, 0)) %>%             
  group_by(geoid, geoname, race_generic) %>% summarise(count = sum(best, na.rm = TRUE)) %>%
  left_join(race_names, by = c("race_generic")) %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic"))
best_table2 <- best_table2 %>% mutate(count = ifelse(is.na(count) & rate_count > 0, 0, count))


best_rate_count <- filter(best_table2, !is.na(rate_count)) %>% mutate(geoname = gsub(' County', '', geoname), finding_type = 'best count', findings_pos = 1) %>%
  mutate(finding = ifelse(rate_count > 5, paste0(geoname, " County's ", long_name, " residents have the best rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), paste0("Data for ", long_name, " residents of ", geoname, " County is too limited for this analysis."))) 


### Bind worst and best tables - RACE PAGE ### ----------------------------------------------
worst_best_counts <- bind_rows(worst_rate_count, best_rate_count)
worst_best_counts <- rename(worst_best_counts, race = race_generic) %>% select(-long_name, -rate_count, -count)
worst_best_counts <- worst_best_counts %>% mutate(geo_level = ifelse(geoname == 'California', 'state', 'county')) %>% filter(!race %in% c('filipino', 'other', 'twoormor')) # filter out races that don't have RC Race Pages


### Most impacted - PLACE PAGE ### ---------------------------------------------------
impact_table <- worst_table2 %>% select(-rate_count) %>% group_by(geoid, geoname) %>% top_n(1, count) %>% # get race most impacted by racial disparity by geo
  left_join(select(impact_screen, geoid, id_count), by = "geoid")
# 5 counties have ties for group with the most worst rates: Amador, Madera, Mono, San Mateo, Tulare
## the next few lines concatenate the names of the tied groups to prep for findings
impact_table2 <- impact_table %>% 
  group_by(geoid, geoname, count) %>% 
  mutate(long_name2 = paste0(long_name, collapse = " and ")) %>%  select(-c(long_name, race_generic)) %>% unique()
most_impacted <- impact_table2 %>% mutate(finding_type = 'most impacted', finding = ifelse(id_count > 4, paste0("Across indicators, ", geoname, " ", long_name2, " residents are most impacted by racial disparity."), paste0("Data for residents of ", geoname, " is too limited for this analysis.")),
                                          findings_pos = 1)
most_impacted <- most_impacted %>% select(c(geoid, geoname, finding_type, finding, findings_pos))
most_impacted$geoname <- gsub(" County", "", most_impacted$geoname) 
most_impacted <- most_impacted[-c(1)]

### This section creates findings for Race pages - most disparate indicator by race & place. 
##Example:"Denied Mortgages is the most disparate indicator for American Indian/Alaska Native residents of San Francisco." ------------------------------------------------------------------

# Function to prep raced most_disparate tables 
most_disp_by_race <- function(x, y, d) {
  # Nested function to pull the column with the maximum value ----------------------
  find_first_max_index_na <- function(row) {
    
    head(which(row== max(row, na.rm=TRUE)), 1)[1]
  }
  
  if(is.null(d)) {       ## For races excluding Asian and PacIsl
    # filter by race, pivot_wider, select the columns we want, get race long_name
    z <- x %>% filter(race_generic == y) %>% pivot_wider(names_from = indicator, values_from = disparity_z_score) %>% group_by(geoid, geoname) %>% 
      fill(incarceration:subprime, .direction = 'updown') %>% 
      filter (!duplicated(geoname)) %>% select(-race) %>% rename(race = race_generic) %>% select(geoid, geoname, race, incarceration:subprime)
    z <- z %>% inner_join(race_names, by = c('race' = 'race_generic')) %>% select(geoid, geoname, race, long_name, everything()) 
    
    # count indicators
    indicator_count <- z %>% ungroup %>% select(-geoid:-long_name)
    indicator_count$indicator_count <- rowSums(!is.na(indicator_count))
    z$indicator_count <- indicator_count$indicator_count 
    
    # select columns we need
    z <- z %>% select(geoid, geoname, race, long_name, indicator_count, everything())
    
    # unique indicators that apply to race
    indicator_col <- z %>% ungroup %>% select(7:ncol(z))
    indicator_col <- names(indicator_col)
    
    # pull the column name with the maximum value
    z$max_col <- colnames(z[indicator_col]) [
      apply(
        z[indicator_col],
        MARGIN = 1,
        find_first_max_index_na )
    ]
    
    ## merge with indicator
    z <- left_join(z, indicator, by = c("max_col"="indicator_short"))
    
    ## add finding
    z <- z %>% mutate(
      finding = ifelse(indicator_count <= 5,     ## Suppress finding if race+geo combo has 5 or fewer indicator disparity_z scores
                       paste0("Data for ", long_name, " residents of ", geoname, " is too limited for this analysis."),
                       paste0(indicator, " is the most disparate indicator for ", long_name, " residents of ", geoname, "."))
    ) %>% select(
      geoid, geoname, race, long_name, indicator_count, finding) %>% arrange(geoname)
    
    return(z)
  }
  
  else {       ## For Asian and PacIsl only bc we count Asian+API and PacIsl+API
    # filter by race, pivot_wider, select the columns we want, get race long_name
    z <- x %>% filter(race_generic == y | race_generic == d) %>% pivot_wider(names_from = indicator, values_from = disparity_z_score) %>% group_by(geoid, geoname) %>% 
      fill(incarceration:subprime, .direction = 'updown') %>% 
      filter (!duplicated(geoname)) %>% select(-race_generic) %>% mutate(race = y) %>% select(geoid, geoname, race, incarceration:subprime)
    z <- z %>% inner_join(race_names, by = c('race' = 'race_generic')) %>% select(geoid, geoname, race, long_name, everything()) 
    
    indicator_count <- z %>% ungroup %>% select(-geoid:-long_name)
    indicator_count$indicator_count <- rowSums(!is.na(indicator_count))
    z$indicator_count <- indicator_count$indicator_count 
    
    # select columns we need
    z <- z %>% select(geoid, geoname, race, long_name, indicator_count, everything())
    
    # unique indicators that apply to race
    indicator_col <- z %>% ungroup %>% select(7:ncol(z))
    indicator_col <- names(indicator_col)
    
    # pull the column name with the maximum value
    z$max_col <- colnames(z[indicator_col]) [
      apply(
        z[indicator_col],
        MARGIN = 1,
        find_first_max_index_na )
    ]
    
    ## merge with indicator
    z <- left_join(z, indicator, by = c("max_col"="indicator_short"))
    
    ## add finding
    z <- z %>% mutate(
      finding = ifelse(indicator_count<=5, ## threshold of equal or less than 5
                       paste0("Data for ", long_name, " residents of ", geoname, " is too limited for this analysis."),
                       paste0(indicator, " is the most disparate indicator for ", long_name, " residents of ", geoname, "."))
    ) %>% select(
      geoid, geoname, race, long_name, indicator_count, finding) %>% arrange(geoname)
    
    return(z)
  }
}

# copy df before running any code
# Most Disparate Indicator by Race - RACE PAGE
df_ds <- filter(df, race != 'total')    # remove total rates bc all findings in this section are raced
aian_ <- most_disp_by_race(df_ds, 'aian', d = NULL)
asian_ <- most_disp_by_race(df_ds, 'asian', 'api')
black_ <- most_disp_by_race(df_ds, 'black', d = NULL)
latino_ <- most_disp_by_race(df_ds, 'latino', d = NULL)
pacisl_ <- most_disp_by_race(df_ds, 'pacisl', 'api')
white_ <- most_disp_by_race(df_ds, 'white', d = NULL)

final_findings <- bind_rows(aian_, asian_, black_, latino_, pacisl_, white_) 
final_findings <- final_findings %>% select(-indicator_count, -long_name)
most_disp <- final_findings %>% mutate(geo_level = ifelse(geoname == 'California', 'state', 'county'),
                                       finding_type = 'most disparate',
                                       findings_pos = 3, geoname = gsub(' County', '', geoname))


# Save most_disp, best_rate_counts, worst_rate_counts
rda_race_door_findings <- bind_rows(most_disp, worst_best_counts)
rda_race_door_findings <- rda_race_door_findings %>% relocate(geo_level, .after = geoname) %>% relocate(finding_type, .after = race) %>% mutate( src = 'rda', citations = '') %>%
  mutate(race = ifelse(race == 'pacisl', 'nhpi', race))  # rename pacisl to nhpi to feed API - will need to change API/front-end later so we can use RC standard pacisl

## Create postgres table
# dbWriteTable(con, c("v5", "arei_findings_races_multigeo"), rda_race_door_findings,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns
# comment <- paste0("COMMENT ON TABLE v5.arei_findings_races_multigeo IS 'findings for Race pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaway\\key_findings_2023.R.';",
#                   "COMMENT ON COLUMN v5.arei_findings_races_multigeo.finding_type
#                        IS 'Categorizes findings: count of best and worst rates by race/geo combo, most disparate indicator by race/geo combo';",
#                   "COMMENT ON COLUMN v5.arei_findings_races_multigeo.src
#                        IS 'Categorizes source of finding as either rda or program area';",
#                   "COMMENT ON COLUMN v5.arei_findings_races_multigeo.citations
#                        IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
#                   "COMMENT ON COLUMN v5.arei_findings_races_multigeo.findings_pos
#                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
# print(comment)
# dbSendQuery(con, comment)



#### HK: (manual) issue area findings (used on issue areas pages and the state places page) ####

issue_area_findings <- read.csv("./manual_findings_v5_2023.csv", encoding = "UTF-8")
colnames(issue_area_findings) <- c("issue_area", "finding", "findings_pos")

issue_area_findings_type_dict <- list(economy = "Economic Opportunity",
                                      education = "Education",
                                      housing = "Housing",
                                      health = "Health Care Access",
                                      democracy = "Democracy",
                                      crime = "Crime and Justice",
                                      hbe = "Healthy Built Environment")

issue_area_findings$finding_type <- ifelse(issue_area_findings$issue_area == 'economy', "Economic Opportunity",
                                            ifelse(issue_area_findings$issue_area == 'health', "Health Care Access",
                                                   ifelse(issue_area_findings$issue_area == 'crime', "Crime and Justice",
                                                          ifelse(issue_area_findings$issue_area == 'hbe', "Healthy Built Environment",
                                                                 str_to_title(issue_area_findings$issue_area)))))
issue_area_findings$src <- "rda"

issue_area_findings$citations <- ""

# dbWriteTable(con, c("v5", "arei_findings_issues"), issue_area_findings,
#              overwrite = FALSE, row.names = FALSE)

# comment on table and columns
#  comment <- paste0("COMMENT ON TABLE v5.arei_findings_issues IS 'findings for Issue Area pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaway\\key_findings_2023.R.';",
#                   "COMMENT ON COLUMN v5.arei_findings_issues.finding_type
#                        IS 'Categorizes findings: race most impacted by inequities in a geo, above/below avg disp, above/below perf, most disp indicator, worst perf indicator';",
#                   "COMMENT ON COLUMN v5.arei_findings_issues.src
#                        IS 'Categorizes source of finding as either rda or program area';",
#                   "COMMENT ON COLUMN v5.arei_findings_issues.citations
#                        IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
#                   "COMMENT ON COLUMN v5.arei_findings_issues.findings_pos
#                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
# print(comment)
# dbSendQuery(con, comment)


### This section creates findings for Place page - the most disparate and worst outcome indicators across counties #####
# Load Indexes
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_crim_index_2023")
c_2 <- st_read(con, query = "SELECT * FROM v5.arei_demo_index_2023")
c_3 <- st_read(con, query = "SELECT * FROM v5.arei_econ_index_2023")
c_4 <- st_read(con, query = "SELECT * FROM v5.arei_educ_index_2023")
c_5 <- st_read(con, query = "SELECT * FROM v5.arei_hben_index_2023")
c_6 <- st_read(con, query = "SELECT * FROM v5.arei_hlth_index_2023")
c_7 <- st_read(con, query = "SELECT * FROM v5.arei_hous_index_2023")

data_list <- list(c_1, c_2, c_3, c_4, c_5, c_6, c_7)


## Worst Disparity - PLACE PAGE ----

# Keep only disp_z column
select_cols <- lapply(data_list, select, county_id,
                      county_name,
                      ends_with(c("disp_z")))

# Merge into 1 matrix, removing duplicative county_id, county_name columns
merged_disp <- reduce(select_cols, inner_join, by = c("county_name", "county_id"))


# Identify most disparate indicator and lowest overall outcome indicator by county

### Convert table from wide to long format
disp_long <- reshape2::melt(merged_disp, id.vars=c("county_id", "county_name")) 
disp_long <- rename(disp_long, geoname = county_name, geoid = county_id)

#### Rank indicators by disp_z with worst/highest disp_z = 1
disp_final <- disp_long %>%
  group_by(geoid, geoname) %>%
  mutate(rk = min_rank(-value))

##### Select only worst/highest disparity indicator per county
disp_final <- disp_final %>% filter(rk == 1) %>% arrange(geoid) 

##### Rename variable and value fields
names(disp_final)[names(disp_final) == 'value'] <- 'worst_disp_z'
names(disp_final)[names(disp_final) == 'variable'] <- 'worst_disp_indicator'

# clean names for later merging
disp_final$worst_disp_indicator <- gsub('_disp_z', '', disp_final$worst_disp_indicator)

# join to get long indicator names for findings
worst_disp <- select(disp_final, -c(rk, worst_disp_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_disp_indicator" = "indicator_short")) %>% rename(long_disp_indicator = indicator)

# Adjust for counties with tied indicators
worst_disp2 <- worst_disp %>% 
  group_by(geoname) %>% 
  mutate(disp_ties = n()) %>%
  mutate(long_disp_indicator = paste0(long_disp_indicator, collapse = " and ")) %>% select(-c(worst_disp_indicator)) %>% unique()

# Write Findings using ifelse statements
worst_disp2 <- worst_disp2 %>% 
  mutate(finding_type = 'worst disparity', finding = ifelse(disp_ties > 1, 
                                                            paste0(geoname, " County's high racial disparity in ", long_disp_indicator," stand out most compared to other counties."),
                                                            paste0(geoname, " County's high racial disparity in ", long_disp_indicator," stands out most compared to other counties.")), 
         findings_pos = 4) %>% 
  select(geoid, geoname, finding_type, finding, findings_pos)


## Worst Overall Outcome - PLACE PAGE ----

# Keep only perf_z columns
select_cols <- lapply(data_list, select, county_id,
                      county_name,
                      ends_with(c("perf_z")))

# Merge into 1 matrix, removing duplicative county_id columns
merged_perf <- reduce(select_cols, inner_join, by = c("county_name", "county_id"))

### Convert table from wide to long format
perf_long <- reshape2::melt(merged_perf, id.vars=c("county_id", "county_name"))
perf_long <- rename(perf_long, geoname = county_name, geoid = county_id)

#### Rank indicators by perf_z with worst/lowest perf_z = 1
perf_final <- perf_long %>%
  group_by(geoid, geoname) %>%
  mutate(rk = min_rank(value))

##### Select only worst/lowest overall outcome indicator per county
perf_final <- perf_final %>% filter(rk == 1) %>% arrange(geoid)

##### Rename variable and value fields
names(perf_final)[names(perf_final) == 'value'] <- 'worst_perf_z'
names(perf_final)[names(perf_final) == 'variable'] <- 'worst_perf_indicator'    

# clean names for later merging
perf_final$worst_perf_indicator <- gsub('_perf_z', '', perf_final$worst_perf_indicator)

# join to get long indicator names for findings
worst_perf <- select(perf_final, -c(rk, worst_perf_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_perf_indicator" = "indicator_short")) %>% rename(long_perf_indicator = indicator)

# Adjust for counties with tied indicators
worst_perf2 <- worst_perf %>% 
  group_by(geoid, geoname) %>% 
  mutate(perf_ties = n()) %>%
  mutate(long_perf_indicator = paste0(long_perf_indicator, collapse = " and ")) %>% select(-c(worst_perf_indicator)) %>% unique()

# Write Findings using ifelse statements
worst_perf2 <- worst_perf2 %>% 
  mutate(finding_type = 'worst overall outcome', finding = ifelse(perf_ties > 1, 
                                                                  paste0(geoname, " County's low overall outcomes in ", long_perf_indicator, " stand out most compared to other counties."),
                                                                  paste0(geoname, " County's low overall outcome in ", long_perf_indicator," stands out most compared to other counties.")),  
         findings_pos = 5) %>% 
  select(geoid, geoname, finding_type, finding, findings_pos)

# Combine findings into one final df
worst_disp_perf <- union(worst_disp2, worst_perf2)


### AB: This section creates findings for Place page- the summary statements above/below avg disparity/overall outcomes across counties ####
# Indicators
c_1 <- st_read(con, query = "SELECT * FROM v5.arei_composite_index_2023")

# Above/Below Avg Disp/Perf - PLACE PAGE
sum_statement_df <- c_1 %>% 
  select(county_id, county_name, urban_type, disparity_z, performance_z) %>%
  mutate(perf_type = ifelse(performance_z < 0, 'below', 'above'),
         pop_type = ifelse(urban_type == 'Urban', 'more', 'less'),
         disp_type = ifelse(disparity_z < 0, 'below', 'above'))

disp_avg_statement <- sum_statement_df  %>% rename(geoid = county_id, geoname = county_name) %>%
  mutate(finding_type = 'disparity', finding = ifelse(is.na(disp_type), NA, paste0(geoname, " County's racial disparity across indicators is ", disp_type, " average for California counties.")),
         findings_pos = 2) %>% 
  select(geoid, geoname, finding_type, finding, findings_pos) 

perf_avg_statement <- sum_statement_df  %>% rename(geoid = county_id, geoname = county_name) %>%
  mutate(finding_type = 'outcomes', finding = ifelse(is.na(perf_type), NA, paste0(geoname, " County's overall outcomes across indicators are ", perf_type, " average for California counties.")),
         findings_pos = 3) %>% 
  select(geoid, geoname, finding_type, finding, findings_pos) 

rda_places_findings <- rbind(most_impacted, disp_avg_statement, perf_avg_statement, worst_disp_perf) %>%
  mutate(geo_level = ifelse(geoid == '06', 'state', 'county'), src = 'rda', citations = '') %>%
  relocate(geo_level, .after = geoname)

rda_places_findings <- rda_places_findings %>% mutate(finding = ifelse(is.na(finding), paste0("Data for ", geoname, " County is too limited for this analysis."), finding))


# prep issues table for addition to places_findings_table
state_issue_area_findings <- issue_area_findings

state_issue_area_findings <- state_issue_area_findings %>%
  select(-issue_area)

state_issue_area_findings$geoid <- "06"
state_issue_area_findings$geoname <- "California"
state_issue_area_findings$geo_level <- "state"

# reorder findings position for places positions
state_issue_area_findings <- state_issue_area_findings %>%
  mutate(findings_pos = row_number() + 1) 

findings_places_multigeo <- rbind(rda_places_findings,
                                  state_issue_area_findings)

## Create postgres table
dbWriteTable(con, c("v5", "arei_findings_places_multigeo"), findings_places_multigeo,
             overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE v5.arei_findings_places_multigeo IS 'findings for Race pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaway\\key_findings_2023.R.';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo.finding_type
                        IS 'Categorizes findings: race most impacted by inequities in a geo, above/below avg disp, above/below perf, most disp indicator, worst perf indicator';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo.src
                        IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo.citations
                        IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
print(comment)
dbSendQuery(con, comment)
