#### Better or worse current RC version compared to previous version of RC? ####
### Script produces findings about how much an indicator's overall outcome has changed since prev RC by calculating % change btwn curr and prev total_rate values.
### Script produces findings about how much an indicator's disparity has changed by calculating the % change btwn curr and prev Index of Disparity values.


<<<<<<< HEAD
##### Install and load packages #####
packages <- c("dplyr","RPostgreSQL","usethis")  
=======
#install and load packages if not already installed
packages <- c("tidyverse","RPostgreSQL","here","usethis")  
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

<<<<<<< HEAD
###### Load PostgreSQL driver, source scripts, adjust settings #####
source("W:\\RDA Team\\R\\credentials_source.R")
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Index_Functions.R")
=======
# Load PostgreSQL driver --------------------------------------------------
source("W:\\RDA Team\\R\\credentials_source.R")

# Set Source for Index Functions script -----------------------------------
source(here("Functions", "RC_Index_Functions.R"))
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b

options(scipen = 100)

con <- connect_to_db("racecounts")


<<<<<<< HEAD
##### UPDATE EACH YEAR: Define years to compare #####
=======
con <- connect_to_db("racecounts")

# update each yr
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b
curr_rc_yr <- '2024'
curr_rc_schema <- 'v6'
prev_rc_yr <- '2023'
prev_rc_schema <- 'v5'

<<<<<<< HEAD
##### create helper function to clean data #####
clean_data_z <- function(x, y, z) {
  
  # Select IDs. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% select(state_id, state_name, total_rate, asbest, index_of_disparity) %>% 
    mutate(variable = y,
           issue_area = z)
  
  return(x)
}

# define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname1 <- 'incarceration'
varname2 <- 'safety'
varname3 <- 'offenses'
varname4 <- 'force'
varname5 <- 'stops'
issue_area1 <- 'crime & justice'

## define variable names for clean_data_z function. you MUST UPDATE for each issue area. 
varname6 <- 'census'
varname7 <- 'candidate'
varname8 <- 'elected'
varname9 <- 'voter'
varname10 <- 'midterm'
varname11 <- 'president'
issue_area2 <- 'democracy'

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname12 <- 'connected'
varname13 <- 'employ'
varname14 <- 'internet'
varname15 <- 'officials'
varname16 <- 'percap'
varname17 <- 'realcost'
varname18 <- 'living wage'
issue_area3 <- 'economic opportunity'

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname19 <- 'abst'
varname20 <- 'grad'
varname21 <- 'ela'
varname22 <- 'math'
varname23 <- 'susp'
varname24 <- 'ece'
varname25 <- 'diver'
issue_area4 <- 'education'

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname26 <- 'help'
varname27 <- 'insur'
varname28 <- 'life'
varname29 <- 'bwt'
varname30 <- 'usoc'
varname31 <- 'hosp'
issue_area5 <- 'health access'

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname32 <- 'water'
varname33 <- 'food'
varname34 <- 'hazard'
varname35 <- 'toxic'
varname36 <- 'asthma'
varname37 <- 'green'
issue_area6 <- 'healthy built environment'

## define variable names for clean_data_z function. you MUST UPDATE for each issue area. Copy from v3 index view.
varname38 <- 'burden_own'
varname39 <- 'burden_rent'
varname40 <- 'denied'
varname41 <- 'eviction'
varname42 <- 'forecl'
varname43 <- 'homeown'
varname44 <- 'overcrowded'
varname45 <- 'quality'
varname46 <- 'homeless'
varname47 <- 'subprime'
issue_area7 <- 'housing'

##### Collect, Clean, and Combine all CURR state DATA #####
c_1 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_crim_incarceration_state_", curr_rc_yr))
c_2 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_crim_perception_of_safety_state_", curr_rc_yr))
c_3 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_crim_status_offenses_state_", curr_rc_yr))
c_4 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_crim_use_of_force_state_", curr_rc_yr))
c_5 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_crim_officer_initiated_stops_state_", curr_rc_yr))

c_6 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_demo_census_participation_state_", curr_rc_yr))
c_7 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_demo_diversity_of_candidates_state_", curr_rc_yr))
c_8 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_demo_diversity_of_electeds_state_", curr_rc_yr))
c_9 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_demo_registered_voters_state_", curr_rc_yr))
c_10 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_demo_voting_midterm_state_", curr_rc_yr))
c_11 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_demo_voting_presidential_state_", curr_rc_yr))

c_12 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_econ_connected_youth_state_", curr_rc_yr))
c_13 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_econ_employment_state_", curr_rc_yr))
c_14 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_econ_internet_state_", curr_rc_yr))
c_15 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_econ_officials_state_", curr_rc_yr))
c_16 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_econ_per_capita_income_state_", curr_rc_yr))
c_17 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_econ_real_cost_measure_state_", curr_rc_yr))
c_18 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_econ_living_wage_state_", curr_rc_yr))

c_19 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_educ_chronic_absenteeism_state_", curr_rc_yr))
c_20 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_educ_hs_grad_state_", curr_rc_yr))
c_21 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_educ_gr3_ela_scores_state_", curr_rc_yr))
c_22 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_educ_gr3_math_scores_state_", curr_rc_yr))
c_23 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_educ_suspension_state_", curr_rc_yr))
c_24 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_educ_ece_access_state_", curr_rc_yr))
c_25 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_educ_staff_diversity_state_", curr_rc_yr))

c_26 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hlth_got_help_state_", curr_rc_yr))
c_27 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hlth_health_insurance_state_", curr_rc_yr))
c_28 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hlth_life_expectancy_state_", curr_rc_yr))
c_29 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hlth_low_birthweight_state_", curr_rc_yr))
c_30 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hlth_usual_source_of_care_state_", curr_rc_yr))
c_31 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hlth_preventable_hospitalizations_state_", curr_rc_yr))

c_32 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hben_drinking_water_state_", curr_rc_yr))
c_33 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hben_food_access_state_", curr_rc_yr))
c_34 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hben_haz_weighted_avg_state_", curr_rc_yr))
c_35 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hben_toxic_release_state_", curr_rc_yr))
c_36 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hben_asthma_state_", curr_rc_yr))
c_37 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hben_lack_of_greenspace_state_", curr_rc_yr))

c_38 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_cost_burden_owner_state_", curr_rc_yr))
c_39 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_cost_burden_renter_state_", curr_rc_yr))
c_40 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_denied_mortgages_state_", curr_rc_yr))
c_41 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_eviction_filing_rate_state_", curr_rc_yr))
c_42 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_foreclosure_state_", curr_rc_yr))
c_43 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_homeownership_state_", curr_rc_yr))
c_44 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_overcrowded_state_", curr_rc_yr))
c_45 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_housing_quality_state_", curr_rc_yr))
c_46 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_student_homelessness_state_", curr_rc_yr))
c_47 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", curr_rc_schema, ".arei_hous_subprime_state_", curr_rc_yr))

dbDisconnect(con)

# Clean CURR data with helper function
c_1 <- clean_data_z(c_1, varname1, issue_area1)
c_2 <- clean_data_z(c_2, varname2, issue_area1)
c_3 <- clean_data_z(c_3, varname3, issue_area1)
c_4 <- clean_data_z(c_4, varname4, issue_area1)
c_5 <- clean_data_z(c_5, varname5, issue_area1)
c_6 <- clean_data_z(c_6, varname6, issue_area2)
c_7 <- clean_data_z(c_7, varname7, issue_area2)
c_8 <- clean_data_z(c_8, varname8, issue_area2)
c_9 <- clean_data_z(c_9, varname9, issue_area2)
c_10 <- clean_data_z(c_10, varname10, issue_area2)
c_11 <- clean_data_z(c_11, varname11, issue_area2)
c_12 <- clean_data_z(c_12, varname12, issue_area3)
c_13 <- clean_data_z(c_13, varname13, issue_area3)
c_14 <- clean_data_z(c_14, varname14, issue_area3)
c_15 <- clean_data_z(c_15, varname15, issue_area3)
c_16 <- clean_data_z(c_16, varname16, issue_area3)
c_17 <- clean_data_z(c_17, varname17, issue_area3)
c_18 <- clean_data_z(c_18, varname18, issue_area3)
c_19 <- clean_data_z(c_19, varname19, issue_area4)
c_20 <- clean_data_z(c_20, varname20, issue_area4)
c_21 <- clean_data_z(c_21, varname21, issue_area4)
c_22 <- clean_data_z(c_22, varname22, issue_area4)
c_23 <- clean_data_z(c_23, varname23, issue_area4)
c_24 <- clean_data_z(c_24, varname24, issue_area4)
c_25 <- clean_data_z(c_25, varname25, issue_area4)
c_26 <- clean_data_z(c_26, varname26, issue_area5)
c_27 <- clean_data_z(c_27, varname27, issue_area5)
c_28 <- clean_data_z(c_28, varname28, issue_area5)
c_29 <- clean_data_z(c_29, varname29, issue_area5)
c_30 <- clean_data_z(c_30, varname30, issue_area5)
c_31 <- clean_data_z(c_31, varname31, issue_area5)
c_32 <- clean_data_z(c_32, varname32, issue_area6)
c_33 <- clean_data_z(c_33, varname33, issue_area6)
c_34 <- clean_data_z(c_34, varname34, issue_area6)
c_35 <- clean_data_z(c_35, varname35, issue_area6)
c_36 <- clean_data_z(c_36, varname36, issue_area6)
c_37 <- clean_data_z(c_37, varname37, issue_area6)
c_38 <- clean_data_z(c_38, varname38, issue_area7)
c_39 <- clean_data_z(c_39, varname39, issue_area7)
c_40 <- clean_data_z(c_40, varname40, issue_area7)
c_41 <- clean_data_z(c_41, varname41, issue_area7)
c_42 <- clean_data_z(c_42, varname42, issue_area7)
c_43 <- clean_data_z(c_43, varname43, issue_area7)
c_44 <- clean_data_z(c_44, varname44, issue_area7)
c_45 <- clean_data_z(c_45, varname45, issue_area7)
c_46 <- clean_data_z(c_46, varname46, issue_area7)
c_47 <- clean_data_z(c_47, varname47, issue_area7)

# Join All Curr Data Together 
c_index <- full_join(c_1, c_2)
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
c_index <- full_join(c_index, c_5)
c_index <- full_join(c_index, c_6)
c_index <- full_join(c_index, c_7)
c_index <- full_join(c_index, c_8)
c_index <- full_join(c_index, c_9)
c_index <- full_join(c_index, c_10)
c_index <- full_join(c_index, c_11)
c_index <- full_join(c_index, c_12)
c_index <- full_join(c_index, c_13)
c_index <- full_join(c_index, c_14)
c_index <- full_join(c_index, c_15)
c_index <- full_join(c_index, c_16)
c_index <- full_join(c_index, c_17)
c_index <- full_join(c_index, c_18)
c_index <- full_join(c_index, c_19)
c_index <- full_join(c_index, c_20)
c_index <- full_join(c_index, c_21)
c_index <- full_join(c_index, c_22)
c_index <- full_join(c_index, c_23)
c_index <- full_join(c_index, c_24)
c_index <- full_join(c_index, c_25)
c_index <- full_join(c_index, c_26)
c_index <- full_join(c_index, c_27)
c_index <- full_join(c_index, c_28)
c_index <- full_join(c_index, c_29)
c_index <- full_join(c_index, c_30)
c_index <- full_join(c_index, c_31)
c_index <- full_join(c_index, c_32)
c_index <- full_join(c_index, c_33)
c_index <- full_join(c_index, c_34)
c_index <- full_join(c_index, c_35)
c_index <- full_join(c_index, c_36)
c_index <- full_join(c_index, c_37)
c_index <- full_join(c_index, c_38)
c_index <- full_join(c_index, c_39)
c_index <- full_join(c_index, c_40)
c_index <- full_join(c_index, c_41)
c_index <- full_join(c_index, c_42)
c_index <- full_join(c_index, c_43)
c_index <- full_join(c_index, c_44)
c_index <- full_join(c_index, c_45)
c_index <- full_join(c_index, c_46)
c_index <- full_join(c_index, c_47)

curr_data <- c_index

##### Collect, Clean, and Combine all PREV state DATA #####
con <- connect_to_db("racecounts")
#commenting out c5 bc it is not available in prev year
c_1 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_crim_incarceration_state_", prev_rc_yr))
c_2 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_crim_perception_of_safety_state_", prev_rc_yr))
c_3 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_crim_status_offenses_state_", prev_rc_yr))
c_4 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_crim_use_of_force_state_", prev_rc_yr))
# c_5 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_crim_officer_initiated_stops_state_", prev_rc_yr))

c_6 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_demo_census_participation_state_", prev_rc_yr))
c_7 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_demo_diversity_of_candidates_state_", prev_rc_yr))
c_8 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_demo_diversity_of_electeds_state_", prev_rc_yr))
c_9 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_demo_registered_voters_state_", prev_rc_yr))
c_10 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_demo_voting_midterm_state_", prev_rc_yr))
c_11 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_demo_voting_presidential_state_", prev_rc_yr))

c_12 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_econ_connected_youth_state_", prev_rc_yr))
c_13 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_econ_employment_state_", prev_rc_yr))
c_14 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_econ_internet_state_", prev_rc_yr))
c_15 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_econ_officials_state_", prev_rc_yr))
c_16 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_econ_per_capita_income_state_", prev_rc_yr))
c_17 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_econ_real_cost_measure_state_", prev_rc_yr))
c_18 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_econ_living_wage_state_", prev_rc_yr))

c_19 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_educ_chronic_absenteeism_state_", prev_rc_yr))
c_20 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_educ_hs_grad_state_", prev_rc_yr))
c_21 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_educ_gr3_ela_scores_state_", prev_rc_yr))
c_22 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_educ_gr3_math_scores_state_", prev_rc_yr))
c_23 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_educ_suspension_state_", prev_rc_yr))
c_24 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_educ_ece_access_state_", prev_rc_yr))
c_25 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_educ_staff_diversity_state_", prev_rc_yr))

c_26 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hlth_got_help_state_", prev_rc_yr))
c_27 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hlth_health_insurance_state_", prev_rc_yr))
c_28 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hlth_life_expectancy_state_", prev_rc_yr))
c_29 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hlth_low_birthweight_state_", prev_rc_yr))
c_30 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hlth_usual_source_of_care_state_", prev_rc_yr))
c_31 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hlth_preventable_hospitalizations_state_", prev_rc_yr))

c_32 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hben_drinking_water_state_", prev_rc_yr))
c_33 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hben_food_access_state_", prev_rc_yr))
c_34 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hben_haz_weighted_avg_state_", prev_rc_yr))
c_35 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hben_toxic_release_state_", prev_rc_yr))
c_36 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hben_asthma_state_", prev_rc_yr))
c_37 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hben_lack_of_greenspace_state_", prev_rc_yr))

c_38 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_cost_burden_owner_state_", prev_rc_yr))
c_39 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_cost_burden_renter_state_", prev_rc_yr))
c_40 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_denied_mortgages_state_", prev_rc_yr))
c_41 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_eviction_filing_rate_state_", prev_rc_yr))
c_42 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_foreclosure_state_", prev_rc_yr))
c_43 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_homeownership_state_", prev_rc_yr))
c_44 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_overcrowded_state_", prev_rc_yr))
c_45 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_housing_quality_state_", prev_rc_yr))
c_46 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_student_homelessness_state_", prev_rc_yr))
c_47 <- dbGetQuery(con, statement = paste0("SELECT state_id, state_name, total_rate, asbest, index_of_disparity FROM ", prev_rc_schema, ".arei_hous_subprime_state_", prev_rc_yr))

dbDisconnect(con)


# Clean PREV data
c_1 <- clean_data_z(c_1, varname1, issue_area1)
c_2 <- clean_data_z(c_2, varname2, issue_area1)
c_3 <- clean_data_z(c_3, varname3, issue_area1)
c_4 <- clean_data_z(c_4, varname4, issue_area1)
# c_5 <- clean_data_z(c_5, varname5, issue_area1)
c_6 <- clean_data_z(c_6, varname6, issue_area2)
c_7 <- clean_data_z(c_7, varname7, issue_area2)
c_8 <- clean_data_z(c_8, varname8, issue_area2)
c_9 <- clean_data_z(c_9, varname9, issue_area2)
c_10 <- clean_data_z(c_10, varname10, issue_area2)
c_11 <- clean_data_z(c_11, varname11, issue_area2)
c_12 <- clean_data_z(c_12, varname12, issue_area3)
c_13 <- clean_data_z(c_13, varname13, issue_area3)
c_14 <- clean_data_z(c_14, varname14, issue_area3)
c_15 <- clean_data_z(c_15, varname15, issue_area3)
c_16 <- clean_data_z(c_16, varname16, issue_area3)
c_17 <- clean_data_z(c_17, varname17, issue_area3)
c_18 <- clean_data_z(c_18, varname18, issue_area3)
c_19 <- clean_data_z(c_19, varname19, issue_area4)
c_20 <- clean_data_z(c_20, varname20, issue_area4)
c_21 <- clean_data_z(c_21, varname21, issue_area4)
c_22 <- clean_data_z(c_22, varname22, issue_area4)
c_23 <- clean_data_z(c_23, varname23, issue_area4)
c_24 <- clean_data_z(c_24, varname24, issue_area4)
c_25 <- clean_data_z(c_25, varname25, issue_area4)
c_26 <- clean_data_z(c_26, varname26, issue_area5)
c_27 <- clean_data_z(c_27, varname27, issue_area5)
c_28 <- clean_data_z(c_28, varname28, issue_area5)
c_29 <- clean_data_z(c_29, varname29, issue_area5)
c_30 <- clean_data_z(c_30, varname30, issue_area5)
c_31 <- clean_data_z(c_31, varname31, issue_area5)
c_32 <- clean_data_z(c_32, varname32, issue_area6)
c_33 <- clean_data_z(c_33, varname33, issue_area6)
c_34 <- clean_data_z(c_34, varname34, issue_area6)
c_35 <- clean_data_z(c_35, varname35, issue_area6)
c_36 <- clean_data_z(c_36, varname36, issue_area6)
c_37 <- clean_data_z(c_37, varname37, issue_area6)
c_38 <- clean_data_z(c_38, varname38, issue_area7)
c_39 <- clean_data_z(c_39, varname39, issue_area7)
c_40 <- clean_data_z(c_40, varname40, issue_area7)
c_41 <- clean_data_z(c_41, varname41, issue_area7)
c_42 <- clean_data_z(c_42, varname42, issue_area7)
c_43 <- clean_data_z(c_43, varname43, issue_area7)
c_44 <- clean_data_z(c_44, varname44, issue_area7)
c_45 <- clean_data_z(c_45, varname45, issue_area7)
c_46 <- clean_data_z(c_46, varname46, issue_area7)
c_47 <- clean_data_z(c_47, varname47, issue_area7)

# Join prev Data Together
c_index <- full_join(c_1, c_2)
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
# c_index <- full_join(c_index, c_5)
c_index <- full_join(c_index, c_6)
c_index <- full_join(c_index, c_7)
c_index <- full_join(c_index, c_8)
c_index <- full_join(c_index, c_9)
c_index <- full_join(c_index, c_10)
c_index <- full_join(c_index, c_11)
c_index <- full_join(c_index, c_12)
c_index <- full_join(c_index, c_13)
c_index <- full_join(c_index, c_14)
c_index <- full_join(c_index, c_15)
c_index <- full_join(c_index, c_16)
c_index <- full_join(c_index, c_17)
c_index <- full_join(c_index, c_18)
c_index <- full_join(c_index, c_19)
c_index <- full_join(c_index, c_20)
c_index <- full_join(c_index, c_21)
c_index <- full_join(c_index, c_22)
c_index <- full_join(c_index, c_23)
c_index <- full_join(c_index, c_24)
c_index <- full_join(c_index, c_25)
c_index <- full_join(c_index, c_26)
c_index <- full_join(c_index, c_27)
c_index <- full_join(c_index, c_28)
c_index <- full_join(c_index, c_29)
c_index <- full_join(c_index, c_30)
c_index <- full_join(c_index, c_31)
c_index <- full_join(c_index, c_32)
c_index <- full_join(c_index, c_33)
c_index <- full_join(c_index, c_34)
c_index <- full_join(c_index, c_35)
c_index <- full_join(c_index, c_36)
c_index <- full_join(c_index, c_37)
c_index <- full_join(c_index, c_38)
c_index <- full_join(c_index, c_39)
c_index <- full_join(c_index, c_40)
c_index <- full_join(c_index, c_41)
c_index <- full_join(c_index, c_42)
c_index <- full_join(c_index, c_43)
c_index <- full_join(c_index, c_44)
c_index <- full_join(c_index, c_45)
c_index <- full_join(c_index, c_46)
c_index <- full_join(c_index, c_47)

prev_data <- c_index


##### Combine ALL CURR and PREV data #####
combined_data <- full_join(curr_data,
                           prev_data,
                           by = "variable") %>%
  select(state_name.x,
         total_rate.x,
         index_of_disparity.x,
         variable,
         asbest.x,
         issue_area.x,
         total_rate.y,
         index_of_disparity.y)

names(combined_data) <- c("state", "total_rate_curr", "id_curr", "variable", 
                          "asbest","issue_area", "total_rate_prev", "id_prev")
=======

####################### GET STATE DATA #####################################
## CURR STATE DATA ##
# pull in list of all tables in current racecounts schema
table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_rc_schema, "' AND (table_name LIKE '%state_", curr_rc_yr, "');")
rc_list <- dbGetQuery(con, table_list) %>% rename('table' = 'table_name')
rc_list <- rc_list[order(rc_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step
rc_list # check indicator table list is correct and complete

# import all tables on rc_list
state_tables <- lapply(setNames(paste0("select * from ", curr_rc_schema, ".", rc_list), rc_list), DBI::dbGetQuery, conn = con)

# keep only needed columns
state_tables_ <- lapply(state_tables, function(x) x %>% select(state_name, index_of_disparity, total_rate, asbest))

# create columns with indicator and issue names
state_tables_ <- map2(state_tables_, names(state_tables_), ~ mutate(.x, indicator = .y))                 # create indicator col
state_tables_long <- state_tables_ %>% reduce(full_join)                                                 # convert list to "long" df
state_tables_long$issue <- substr(state_tables_long$indicator,6,9)                                       # create issue col
state_tables_long$indicator <- substr(state_tables_long$indicator,11,nchar(state_tables_long$indicator)) # step 1: clean indicator col
state_tables_long$indicator <- gsub(paste0("_state_",curr_rc_yr), "",state_tables_long$indicator)        # step 2: clean indicator col
state_tables_long <- state_tables_long %>% rename(curr_id = index_of_disparity, curr_total_rate = total_rate) # rename cols

## PREV STATE DATA ##
# pull in list of all tables in previous racecounts schema
prev_table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", prev_rc_schema, "' AND (table_name LIKE '%state_", prev_rc_yr, "');")
prev_rc_list <- dbGetQuery(con, prev_table_list) %>% rename('table' = 'table_name')
prev_rc_list <- prev_rc_list[order(prev_rc_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step
prev_rc_list # check indicator table list is correct and complete

# import all tables on prev_rc_list
prev_state_tables <- lapply(setNames(paste0("select * from ", prev_rc_schema, ".", prev_rc_list), prev_rc_list), DBI::dbGetQuery, conn = con)

# keep only needed columns
prev_state_tables_ <- lapply(prev_state_tables, function(x) x %>% select(state_name, index_of_disparity, total_rate))

# create columns with indicator and issue names
prev_state_tables_ <- map2(prev_state_tables_, names(prev_state_tables_), ~ mutate(.x, indicator = .y))                 # create indicator col
prev_state_tables_long <- prev_state_tables_ %>% reduce(full_join)                                                      # convert list to "long" df
prev_state_tables_long$indicator <- substr(prev_state_tables_long$indicator,11,nchar(prev_state_tables_long$indicator)) # step 1: clean indicator col
prev_state_tables_long$indicator <- gsub(paste0("_state_",prev_rc_yr), "",prev_state_tables_long$indicator)             # step 2: clean indicator col
prev_state_tables_long <- prev_state_tables_long %>% rename(prev_id = index_of_disparity, prev_total_rate = total_rate) # rename cols

# join CURR and PREV state data: Use of inner_join() keeps only indicators that appear in both RC years
combined_data <- state_tables_long %>% inner_join(prev_state_tables_long %>% select(prev_id, prev_total_rate, indicator), by = "indicator")
combined_data <- combined_data %>% relocate(indicator, .after = state_name) %>% relocate(issue, .after = state_name) %>% relocate(asbest, .after = indicator)  # reposition columns


# HK Analysis -------------------------------------------------------------
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b

##### Add columns needed to run findings calcs #####
combined_data <- combined_data %>% 
  
  # Calculate differences and percent differences in outcomes and disparity
<<<<<<< HEAD
  mutate(rate_diff = total_rate_curr - total_rate_prev,
         rate_pct_chng_nominal = rate_diff / total_rate_prev * 100,
         id_diff = id_curr - id_prev,
         id_pct_chng = id_diff / id_prev * 100
         ) %>%
=======
  mutate(rate_diff = curr_total_rate - prev_total_rate,
         rate_pct_chng_nominal = rate_diff / prev_total_rate * 100,
         rate_pct_chng_abs = abs(rate_pct_chng_nominal),
         id_diff = curr_id - prev_id,
         id_pct_chng = id_diff / prev_id * 100
  ) %>%
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b
  
  # Classify differences in outcome, disparity, and both 
  # (e.g., create conclusions like: Better and more equitable outcomes, Worse and more inequitable outcomes, etc.)
  mutate(outcome_change =
           case_when(asbest == "min" & rate_pct_chng_nominal < 0 ~ "better",
                     asbest == "min" & rate_pct_chng_nominal > 0 ~ "worse",
                     asbest == "max" & rate_pct_chng_nominal > 0 ~ "better",
                     asbest == "max" & rate_pct_chng_nominal < 0 ~ "worse", 
                     .default = "no_change"),
         disparity_change =
           case_when(id_pct_chng > 0 ~ "worse",
                     id_pct_chng < 0 ~ "better",
                     .default = "no_change"),
         conclusion = 
           case_when(outcome_change == "better" & disparity_change == "better" ~ "Better and more equitable outcomes",
                     outcome_change == "better" & disparity_change == "worse" ~ "Better but more inequitable outcomes",
                     outcome_change == "better" & disparity_change == "no_change" ~ "Better outcomes and the same disparity",
                     outcome_change == "no_change" & disparity_change == "better" ~ "Same but more equitable outcomes",
                     outcome_change == "no_change" & disparity_change == "worse" ~ "Same but more inequitable outcomes",
                     outcome_change == "no_change" & disparity_change == "no_change" ~ "no_changes",
                     outcome_change == "worse" & disparity_change == "better" ~ "Worse but more equitable outcomes",
                     outcome_change == "worse" & disparity_change == "worse" ~ "Worse and more inequitable outcomes",
                     outcome_change == "worse" & disparity_change == "no_change" ~ "Worse outcomes and the same disparity",
                     .default = "error")) %>%
<<<<<<< HEAD

=======
  
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b
  # Add "real" calculations by indicator to incorporate if _pct_chng_nominal is a positive or negative change (i.e., with respect to asbest)
  # will use to calc aggregate findings (e.g., across all indicators and by issue area)
  mutate(real_flag = 
           case_when(outcome_change=="worse" ~ -1, 
                     outcome_change=="better" ~ 1,
                     .default = 0),
<<<<<<< HEAD
         rate_pct_chng_real = real_flag * abs(rate_pct_chng_nominal))

# filter out indicators not in v5 (i.e., officer-initiated stops)
final_df <- combined_data %>%
  filter(!is.na(rate_pct_chng_nominal))

##### findings calculations #####
##### 1. Are State outcomes better or worse?:  Calculate overall and mean difference in outcomes across all indicators #####
outcome_pct_change_real_sum <- sum(final_df$rate_pct_chng_real, na.rm=TRUE)
outcome_pct_change_real_mean <- mean(final_df$rate_pct_chng_real, na.rm=TRUE) # -4.073114; overall worse (outcomes went down)

## Alternative: calculate overall and mean difference in outcomes (excl. indicators that weren't updated)
final_df %>%
  select(variable, real_flag, rate_pct_chng_real) %>%
=======
         rate_pct_chng_real = real_flag * rate_pct_chng_abs)

##### findings calculations #####
##### 1. Are State outcomes better or worse?:  Calculate overall and mean difference in outcomes across all indicators #####
outcome_pct_change_real_sum <- sum(combined_data$rate_pct_chng_real, na.rm=TRUE)
outcome_pct_change_real_mean <- mean(combined_data$rate_pct_chng_real, na.rm=TRUE) # -4.073114; overall worse (outcomes went down)

## Alternative: calculate overall and mean difference in outcomes (excl. indicators that weren't updated)
combined_data %>%
  select(indicator, real_flag, rate_pct_chng_real) %>%
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b
  filter(real_flag != 0) %>%
  summarize(mean = mean(rate_pct_chng_real)) # -5.510683

## Look at frequency of indicators that improved, worsened, or stayed the same
<<<<<<< HEAD
as.data.frame(table(final_df$outcome_change)) # more indicators improved than worsened or stayed the same

## Average overall outcomes by issue area and include counts of indicator changes
issues_ranked <- final_df %>%
  select(variable, issue_area, outcome_change, rate_pct_chng_real) %>%
  group_by(issue_area) %>%
  summarize(avg_pct_chng = mean(rate_pct_chng_real)) %>%
  arrange(-avg_pct_chng) 

issue_indicators_freq <- as.data.frame(table(final_df$issue_area, final_df$outcome_change)) %>%
=======
as.data.frame(table(combined_data$outcome_change)) # more indicators improved than worsened or stayed the same

## Average overall outcomes by issue area and include counts of indicator changes
issues_ranked <- combined_data %>%
  select(indicator, issue, outcome_change, rate_pct_chng_real) %>%
  group_by(issue) %>%
  summarize(avg_pct_chng = mean(rate_pct_chng_real)) %>%
  arrange(-avg_pct_chng) 

issue_indicators_freq <- as.data.frame(table(combined_data$issue, combined_data$outcome_change)) %>%
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b
  pivot_wider(names_from = Var2, values_from = Freq) %>%
  select(Var1, better, worse, no_change)

issues_ranked_outcomes <- issues_ranked %>%
<<<<<<< HEAD
  left_join(issue_indicators_freq, by = c("issue_area" = "Var1"))
  

##### 2. Are State outcomes more or less disparate?: Calculate overall and mean difference in disparity across all indicators #####
id_pct_change_sum <- sum(final_df$id_pct_chng, na.rm=TRUE)
id_pct_change_mean <- mean(final_df$id_pct_chng, na.rm=TRUE) # 1.202161; overall improve (less disparity) on average

## Look at frequency of indicators that improved, worsened, or stayed the same
as.data.frame(table(final_df$disparity_change)) # more indicators improved than worsened or stayed the same

## Average overall disparity by issue area and include counts of indicator changes
issues_ranked <- final_df %>%
  select(variable, issue_area, disparity_change, id_pct_chng) %>%
  group_by(issue_area) %>%
  summarize(avg_pct_chng = mean(id_pct_chng)) %>%
  arrange(avg_pct_chng) 

issue_indicators_freq <- as.data.frame(table(final_df$issue_area, final_df$disparity_change)) %>%
=======
  left_join(issue_indicators_freq, by = c("issue" = "Var1"))


##### 2. Are State outcomes more or less disparate?: Calculate overall and mean difference in disparity across all indicators #####
id_pct_change_sum <- sum(combined_data$id_pct_chng, na.rm=TRUE)   # 55.29939; if positive number then, overall worse disparity on average.
id_pct_change_mean <- mean(combined_data$id_pct_chng, na.rm=TRUE) # 1.202161; if positive number then, overall worse disparity on average.

## Look at frequency of indicators that improved, worsened, or stayed the same
as.data.frame(table(combined_data$disparity_change)) # more indicators improved than worsened or stayed the same

## Average overall disparity by issue area and include counts of indicator changes
issues_ranked <- combined_data %>%
  select(indicator, issue, disparity_change, id_pct_chng) %>%
  group_by(issue) %>%
  summarize(avg_pct_chng = mean(id_pct_chng)) %>%
  arrange(avg_pct_chng) 

issue_indicators_freq <- as.data.frame(table(combined_data$issue, combined_data$disparity_change)) %>%
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b
  pivot_wider(names_from = Var2, values_from = Freq) %>%
  select(Var1, better, worse, no_change)

issues_ranked_disparity <- issues_ranked %>%
<<<<<<< HEAD
  left_join(issue_indicators_freq, by = c("issue_area" = "Var1"))

##### 3. Indicators with greatest outcome percentage change (good) #####
## 5 most improved indicators
top_5_outcomes <- final_df %>%
=======
  left_join(issue_indicators_freq, by = c("issue" = "Var1"))

##### 3. Indicators with greatest outcome percentage change (good) #####
## 5 most improved indicators
top_5_outcomes <- combined_data %>%
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b
  filter(outcome_change=="better") %>%
  arrange(-rate_pct_chng_real) %>%
  head(5)


##### 4. Indicators with greatest outcome percentage change (bad) #####
## 5 most declined indicators
<<<<<<< HEAD
bottom_5_outcomes <- final_df %>%
  filter(outcome_change=="worse") %>%
  arrange(rate_pct_chng_real) %>%
  head(5)
=======
bottom_5_outcomes <- combined_data %>%
  filter(outcome_change=="worse") %>%
  arrange(rate_pct_chng_real) %>%
  head(5)


##### 5. Indicators with greatest disparity percentage change (good) #####
## 5 most improved indicators
top_5_improved_disparity <- combined_data %>%
  filter(disparity_change=="better") %>%
  arrange(id_pct_chng) %>%
  head(5)


##### 6. Indicators with greatest disparity percentage change (bad) #####
## 5 most declined indicators
bottom_5_worsened_disparity <- combined_data %>%
  filter(disparity_change=="worse") %>%
  arrange(-id_pct_chng) %>%
  head(5)


##### Extra tables #####
## Freq of conclusions covering both outcome and disparity changes
conclusions <- as.data.frame(table(combined_data$conclusion))








































































# CALCULATE CHANGES IN OUTCOMES & DISPARITY -------------------------------
combined_data$rate_diff <- combined_data$curr_total_rate - combined_data$prev_total_rate
combined_data$rate_pct_chng <- combined_data$rate_diff / combined_data$prev_total_rate * 100

combined_data$id_diff <- combined_data$curr_id - combined_data$prev_id
combined_data$id_pct_chng <- combined_data$id_diff / combined_data$prev_id * 100
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b


<<<<<<< HEAD
##### 5. Indicators with greatest disparity percentage change (good) #####
## 5 most improved indicators
top_5_improved_disparity <- final_df %>%
  filter(disparity_change=="better") %>%
  arrange(id_pct_chng) %>%
  head(5)


##### 6. Indicators with greatest disparity percentage change (bad) #####
## 5 most declined indicators
bottom_5_worsened_disparity <- final_df %>%
  filter(disparity_change=="worse") %>%
  arrange(-id_pct_chng) %>%
  head(5)


##### Extra tables #####
## Freq of conclusions covering both outcome and disparity changes
conclusions <- as.data.frame(table(final_df$conclusion))
=======
# Calculate difference in disparity by issue area
issue_disp_change <- combined_data %>% group_by(issue) %>% summarize(
  curr_id_sum=sum(curr_id),
  prev_id_sum=sum(prev_id),
  id_diff=sum(curr_id) - sum(prev_id),
  id_pct_chng=(sum(curr_id) - sum(prev_id))/ sum(prev_id) * 100
)

# calculate overall and mean difference in outcomes
combined_data <- combined_data %>% mutate(outcome_better = 
                                            ifelse(asbest == "min" & rate_pct_chng < 0, "better",
                                                   ifelse(asbest == "min" & rate_pct_chng > 0, "worse",
                                                          ifelse(asbest == "max" & rate_pct_chng > 0, "better",
                                                                 ifelse(asbest == "max" & rate_pct_chng < 0, "worse", "no change"
                                                                 )))))


rate_change_sum <- combined_data %>% group_by(outcome_better) %>% summarize(sum_rate_pct_chng = sum(rate_pct_chng), n=n())
>>>>>>> cbdcbb770e8156c73ac95b0b12256a3cabf8d07b

issue_rate_change <- combined_data %>% group_by(outcome_better, issue) %>% summarize(sum_rate_pct_chng = sum(rate_pct_chng), n=n())
