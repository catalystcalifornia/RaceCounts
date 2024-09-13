#### Better or worse current RC version compared to previous version of RC? ####
### Script produces findings about how much an indicator's overall outcome has changed since prev RC by calculating % change btwn curr and prev total_rate values.
### Script produces findings about how much an indicator's disparity has changed by calculating the % change btwn curr and prev Index of Disparity values.


#install packages if not already installed
packages <- c("tidyverse","RPostgreSQL","sf","usethis")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

# Load PostgreSQL driver and databases --------------------------------------------------
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# Set Source for Index Functions script -----------------------------------
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Index_Functions.R")

# remove exponentiation
options(scipen = 100) 

# update each yr
curr_rc_yr <- '2024'
curr_rc_schema <- 'v6'
prev_rc_yr <- '2023'
prev_rc_schema <- 'v5'

####################### ADD state DATA #####################################
#commenting out c5 bc it is not available both years
c_1 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_crim_incarceration_state_", curr_rc_yr))
c_2 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_crim_perception_of_safety_state_", curr_rc_yr))
c_3 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_crim_status_offenses_state_", curr_rc_yr))
c_4 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_crim_use_of_force_state_", curr_rc_yr))
#c_5 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_crim_officer_initiated_stops_state_", curr_rc_yr))

c_6 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_demo_census_participation_state_", curr_rc_yr))
c_7 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_demo_diversity_of_candidates_state_", curr_rc_yr))
c_8 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_demo_diversity_of_electeds_state_", curr_rc_yr))
c_9 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_demo_registered_voters_state_", curr_rc_yr))
c_10 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_demo_voting_midterm_state_", curr_rc_yr))
c_11 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_demo_voting_presidential_state_", curr_rc_yr))

c_12 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_econ_connected_youth_state_", curr_rc_yr))
c_13 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_econ_employment_state_", curr_rc_yr))
c_14 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_econ_internet_state_", curr_rc_yr))
c_15 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_econ_officials_state_", curr_rc_yr))
c_16 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_econ_per_capita_income_state_", curr_rc_yr))
c_17 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_econ_real_cost_measure_state_", curr_rc_yr))
c_18 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_econ_living_wage_state_", curr_rc_yr))

c_19 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_educ_chronic_absenteeism_state_", curr_rc_yr))
c_20 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_educ_hs_grad_state_", curr_rc_yr))
c_21 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_educ_gr3_ela_scores_state_", curr_rc_yr))
c_22 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_educ_gr3_math_scores_state_", curr_rc_yr))
c_23 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_educ_suspension_state_", curr_rc_yr))
c_24 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_educ_ece_access_state_", curr_rc_yr))
c_25 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_educ_staff_diversity_state_", curr_rc_yr))

c_26 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hlth_got_help_state_", curr_rc_yr))
c_27 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hlth_health_insurance_state_", curr_rc_yr))
c_28 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hlth_life_expectancy_state_", curr_rc_yr))
c_29 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hlth_low_birthweight_state_", curr_rc_yr))
c_30 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hlth_usual_source_of_care_state_", curr_rc_yr))
c_31 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hlth_preventable_hospitalizations_state_", curr_rc_yr))

c_32 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hben_drinking_water_state_", curr_rc_yr))
c_33 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hben_food_access_state_", curr_rc_yr))
c_34 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hben_haz_weighted_avg_state_", curr_rc_yr))
c_35 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hben_toxic_release_state_", curr_rc_yr))
c_36 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hben_asthma_state_", curr_rc_yr))
c_37 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hben_lack_of_greenspace_state_", curr_rc_yr))

c_38 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_cost_burden_owner_state_", curr_rc_yr))
c_39 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_cost_burden_renter_state_", curr_rc_yr))
c_40 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_denied_mortgages_state_", curr_rc_yr))
c_41 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_eviction_filing_rate_state_", curr_rc_yr)) 
c_42 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_foreclosure_state_", curr_rc_yr))
c_43 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_homeownership_state_", curr_rc_yr))
c_44 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_overcrowded_state_", curr_rc_yr))
c_45 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_housing_quality_state_", curr_rc_yr))
c_46 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_student_homelessness_state_", curr_rc_yr))
c_47 <- st_read(con, query = paste0("SELECT * FROM ", curr_rc_schema, ".arei_hous_subprime_state_", curr_rc_yr))

dbDisconnect(con)

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
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

# Select total, ID; add variable, issue to each table  -------------------------
clean_data_z <- function(x, y, z) {
  
  # Select IDs. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% select(state_id, state_name, total_rate, asbest, index_of_disparity) %>% 
    mutate(variable = y) %>%
    mutate(issue_area = z)
  
  return(x)
}

# Clean data --------
## apply the function

c_1 <- clean_data_z(c_1, varname1, issue_area1)
c_2 <- clean_data_z(c_2, varname2, issue_area1)
c_3 <- clean_data_z(c_3, varname3, issue_area1)
c_4 <- clean_data_z(c_4, varname4, issue_area1)
#c_5 <- clean_data_z(c_5, varname5, issue_area1)
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

# Join Data Together ------------------------------------------------------
c_index <- full_join(c_1, c_2) 
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
#c_index <- full_join(c_index, c_5)
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

new_data <- c_index

################################################
######################################## OLD - SAME CODE AS ABOVE W DIF rc_yr, rc_schema
#####################################


# Load PostgreSQL driver and databases --------------------------------------------------

####################### ADD state DATA #####################################
#commenting out c5 bc it is not available both years
c_1 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_crim_incarceration_state_", prev_rc_yr))
c_2 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_crim_perception_of_safety_state_", prev_rc_yr))
c_3 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_crim_status_offenses_state_", prev_rc_yr))
c_4 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_crim_use_of_force_state_", prev_rc_yr))
#c_5 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_crim_officer_initiated_stops_state_", prev_rc_yr))

c_6 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_demo_census_participation_state_", prev_rc_yr))
c_7 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_demo_diversity_of_candidates_state_", prev_rc_yr))
c_8 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_demo_diversity_of_electeds_state_", prev_rc_yr))
c_9 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_demo_registered_voters_state_", prev_rc_yr))
c_10 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_demo_voting_midterm_state_", prev_rc_yr))
c_11 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_demo_voting_presidential_state_", prev_rc_yr))

c_12 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_econ_connected_youth_state_", prev_rc_yr))
c_13 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_econ_employment_state_", prev_rc_yr))
c_14 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_econ_internet_state_", prev_rc_yr))
c_15 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_econ_officials_state_", prev_rc_yr))
c_16 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_econ_per_capita_income_state_", prev_rc_yr))
c_17 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_econ_real_cost_measure_state_", prev_rc_yr))
c_18 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_econ_living_wage_state_", prev_rc_yr))

c_19 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_educ_chronic_absenteeism_state_", prev_rc_yr))
c_20 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_educ_hs_grad_state_", prev_rc_yr))
c_21 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_educ_gr3_ela_scores_state_", prev_rc_yr))
c_22 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_educ_gr3_math_scores_state_", prev_rc_yr))
c_23 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_educ_suspension_state_", prev_rc_yr))
c_24 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_educ_ece_access_state_", prev_rc_yr))
c_25 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_educ_staff_diversity_state_", prev_rc_yr))

c_26 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hlth_got_help_state_", prev_rc_yr))
c_27 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hlth_health_insurance_state_", prev_rc_yr))
c_28 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hlth_life_expectancy_state_", prev_rc_yr))
c_29 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hlth_low_birthweight_state_", prev_rc_yr))
c_30 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hlth_usual_source_of_care_state_", prev_rc_yr))
c_31 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hlth_preventable_hospitalizations_state_", prev_rc_yr))

c_32 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hben_drinking_water_state_", prev_rc_yr))
c_33 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hben_food_access_state_", prev_rc_yr))
c_34 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hben_haz_weighted_avg_state_", prev_rc_yr))
c_35 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hben_toxic_release_state_", prev_rc_yr))
c_36 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hben_asthma_state_", prev_rc_yr))
c_37 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hben_lack_of_greenspace_state_", prev_rc_yr))

c_38 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_cost_burden_owner_state_", prev_rc_yr))
c_39 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_cost_burden_renter_state_", prev_rc_yr))
c_40 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_denied_mortgages_state_", prev_rc_yr))
c_41 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_eviction_filing_rate_state_", prev_rc_yr)) 
c_42 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_foreclosure_state_", prev_rc_yr))
c_43 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_homeownership_state_", prev_rc_yr))
c_44 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_overcrowded_state_", prev_rc_yr))
c_45 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_housing_quality_state_", prev_rc_yr))
c_46 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_student_homelessness_state_", prev_rc_yr))
c_47 <- st_read(con, query = paste0("SELECT * FROM ", prev_rc_schema, ".arei_hous_subprime_state_", prev_rc_yr))

dbDisconnect(con)


# Clean data --------
## apply the function

c_1 <- clean_data_z(c_1, varname1, issue_area1)
c_2 <- clean_data_z(c_2, varname2, issue_area1)
c_3 <- clean_data_z(c_3, varname3, issue_area1)
c_4 <- clean_data_z(c_4, varname4, issue_area1)
#c_5 <- clean_data_z(c_5, varname5, issue_area1)
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



# Join Data Together ------------------------------------------------------
c_index <- full_join(c_1, c_2) 
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
#c_index <- full_join(c_index, c_5)
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

old_data <- c_index


############################ join old and new data & analyze

combined_data <- full_join(new_data,
                           old_data,
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

# Calculate differences and percent differences in outcomes and disparity
combined_data$rate_diff <- combined_data$total_rate_curr - combined_data$total_rate_prev
combined_data$rate_pct_chng <- combined_data$rate_diff / combined_data$total_rate_prev * 100

combined_data$id_diff <- combined_data$id_curr - combined_data$id_prev
combined_data$id_pct_chng <- combined_data$id_diff / combined_data$id_prev * 100

# Calculate overall and mean difference in disparity
id_change_sum <- sum(combined_data$id_pct_chng)
id_change_mean <- mean(combined_data$id_pct_chng)

# Calculate difference in disparity by issue area
issue_change <- combined_data %>% group_by(issue_area) %>% summarize(
  id_curr=sum(id_curr),
  id_prev=sum(id_prev),
  id_diff=sum(id_curr) - sum(id_prev),
  id_pct_chng=(sum(id_curr) - sum(id_prev))/ sum(id_prev) * 100
)

# calculate overall and mean difference in outcomes
combined_data <- combined_data %>% mutate(outcome_better = 
                                            ifelse(asbest == "min" & rate_pct_chng < 0, "better",
                                                   ifelse(asbest == "min" & rate_pct_chng > 0, "worse",
                                                          ifelse(asbest == "max" & rate_pct_chng > 0, "better",
                                                                 ifelse(asbest == "max" & rate_pct_chng < 0, "worse", "no change"
                                                   )))))
                                            

rate_change_sum <- combined_data %>% group_by(outcome_better) %>% summarize(sum_rate_pct_chng = sum(rate_pct_chng), n=n())

issue_rate_change <- combined_data %>% group_by(outcome_better, issue_area) %>% summarize(sum_rate_pct_chng = sum(rate_pct_chng), n=n())


