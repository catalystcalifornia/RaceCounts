## Officer Initiated Stops RC v6

## Set up ----------------------------------------------------------------
#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse", "RPostgreSQL", "tidycensus", "readxl", "sf", "janitor", "stringr", "data.table", "openxlsx", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# packages
library(tidyverse)
library(readxl)
library(RPostgreSQL)
library(sf)
library(tidycensus)
library(DBI)
library(janitor)
library(stringr)
library(data.table) # %like% operator
library(openxlsx) # read xlsx from url
library(usethis)

options(scipen = 100)

# Update each year
curr_yr <- '2022'
rc_yr <- '2024'
rc_schema <- 'v6'

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("racecounts")

########## Prep rda_shared_data table: Comment out these sections once table has been created ###########

# # Import list of agencies with counties: Check for updated file each year
# agency_file <- read.xlsx("https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2023-06/NCIC%20Code%20Jurisdiction%20List_04242023.xlsx")
# 
# # Import & clean RIPA supplement data ---------------------------------------------
# ripa_supplement <- read_excel("W:/Data/Crime and Justice/CA DOJ/RIPA Stop Data/2022/12312022 Supplement RIPA SD.xlsx")
# ripa_supp_df <- ripa_supplement %>% mutate(Code=(str_sub(AGENCY_ORI,4,7)))
# 
# ripa_supp_df <- ripa_supp_df %>% left_join(agency_file %>% select(County, Code, Agency), by = "Code")
# # ripa_supp_df %>% filter(is.na(County)) # View unmatched RIPA records. San Ramon PD agency code in RIPA is diff than in agency file. 2 SD records also do not match. Manually adding county names.
# ripa_supp_df$County <- ifelse(ripa_supp_df$AGENCY_NAME == 'SAN RAMON PD', "Contra Costa County", ripa_supp_df$County) # Manual fix for San Ramon PD
# ripa_supp_df$County <- ifelse(grepl('SAN DIEGO COMM', ripa_supp_df$AGENCY_NAME), "San Diego County", ripa_supp_df$County) # Manual fix for San Diego Comm
# 
# 
# # Import & clean California Highway Patrol RIPA data ----------------------------------------
# ### this step takes several minutes
# ripa_CHP_q1 <- read_excel("W:/Data/Crime and Justice/CA DOJ/RIPA Stop Data/2022/RIPA Stop Data _ CHP 2022 Q1.xlsx")
# ripa_CHP_q2 <- read_excel("W:/Data/Crime and Justice/CA DOJ/RIPA Stop Data/2022/RIPA Stop Data _ CHP 2022 Q2.xlsx")
# ripa_CHP_q3 <- read_excel("W:/Data/Crime and Justice/CA DOJ/RIPA Stop Data/2022/RIPA Stop Data _ CHP 2022 Q3.xlsx")
# ripa_CHP_q4 <- read_excel("W:/Data/Crime and Justice/CA DOJ/RIPA Stop Data/2022/RIPA Stop Data _ CHP 2022 Q4.xlsx")
# 
# ripa_CHP <- rbind(ripa_CHP_q1, ripa_CHP_q2, ripa_CHP_q3, ripa_CHP_q4)
# ripa_CHP_df <- ripa_CHP %>% mutate(Code=(str_sub(AGENCY_ORI,4,7)))
# 
# # all CHP stops have the agency ID associated with CA Highway Patrol - Sacramento, which is incorrect
# ## ripa_CHP_df_test <- ripa_CHP_df %>% left_join(agency_file %>% select(County, Code, Agency), by = "Code")
# ## ripa_CHP_df_test %>% filter(is.na(County)) # View unmatched RIPA records.
# ## table(ripa_CHP_df_test$County)
# 
# # add a county field to CHP stops that just retains the agency as CHP so it doesn't get matched to Sac County
# ripa_CHP_df<-ripa_CHP_df%>%mutate(County='CA Highway Patrol')
# 
# # Import & clean county RIPA data -------------------------------------------------
# setwd("W:\\Data\\Crime and Justice\\CA DOJ\\RIPA Stop Data\\2022\\county_data")
# file_names <- dir(paste0("W:\\Data\\Crime and Justice\\CA DOJ\\RIPA Stop Data\\",curr_yr,"\\county_data")) # county data file location
# 
# # import all county data files, pull county names from filenames
# ### this step takes several minutes
# ripa_county <- file_names %>%
#                             set_names() %>%
#                                             map_dfr(read_excel, .id = "County")
# 
# # add county_name field based on filenames
# ripa_county_df <- ripa_county %>%
#   mutate(County = str_remove(County, "\\ 2022.xlsx$")) %>%
#   mutate(County = str_remove(County, "RIPA Stop Data _ ")) %>%
#   mutate(County = paste0(County, " County"))
# 
# 
# # Create RIPA data postgres table -------------------------------------------------
# 
# # bind county df with CHP and supplement
# ripa_df <- rbind(ripa_CHP_df%>%select(-Code), ripa_supp_df%>%select(-c(Agency, Code)))
# ripa_df <- rbind(ripa_df, ripa_county_df)
# unique(ripa_df$County)   # check if county names need cleaning
# ripa_df <- ripa_df %>% mutate(County = ifelse(grepl("Los Angeles", County), "Los Angeles County", County)) # clean up LAC rows bc LAC data was separated into 4 files
# ripa_df <- ripa_df %>% clean_names()
# ripa_df$date_of_stop <- as.character(as.POSIXct(ripa_df$date_of_stop,tz="UTC",format="%Y-%m-%d"))

# # push to postgres
# con <- connect_to_db("rda_shared_data")
# schema <- "crime_and_justice"
# table_name <- "cadoj_ripa_2022"
# table_comment <- paste0(curr_yr, " RIPA statewide data. Downloaded from https://openjustice.doj.ca.gov/data. Script: W:\\Project\\RACE COUNTS\\2024_v6\\RC_Github\\RaceCounts\\IndicatorScripts\\CrimeJustice\\crim_officer_initiated_stops_2024.R")
# 
# dbWriteTable(con, c(schema, table_name), ripa_df,
#              overwrite = FALSE, row.names = FALSE)


# # function to add table comments
# add_table_comments <- function(con, schema, table_name, indicator, source, column_names, column_comments) {
#   comments <- character()
#   comments <- c(comments, paste0("
#     COMMENT ON TABLE ", schema, ".", table_name, " IS '", table_comment, "';"))
#   for (i in seq_along(column_names)) {
#     comment <- paste0("
#       COMMENT ON COLUMN ", schema, ".", table_name, ".", column_names[i], " IS '", column_comments[i], "';
#       ")
#     comments <- c(comments, comment)
#   }
#   sql_commands <- paste(comments, collapse = "")
#   dbSendQuery(con, sql_commands)
# }
# 
# column_names <- colnames(ripa_df) # get column names
# 
# column_comments <- c(   # source: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf
#   "A unique system-generated incident identification number. Alpha-numeric",
#   "A system-generated number that is assigned to each individual who is involved in the stop or encounter. Numeric",
#   "The number for the reporting Agency. Nine digit alpha-numeric",
#   "Agency name. Alpha-numeric",
#   "Time of stop",
#   "Date of stop. ",
#   "Duration of stop in minutes",
#   "Location of stop closest city. Alpha",
#   "School code. Fourteen digit alphanumeric Blank",
#   "School name. Alpha Blank",
#   "Stop of student. 0 No 1 Yes",
#   "Stop on K12 school grounds. 0 No 1 Yes",
#   "Perceived Race or Ethnicity of Person stopped. Individuals perceived as more than one race/ethnicity  are counted as Multiracial for this variable. 1 Asian 2 Black/African American 3 Hispanic/Latino 4 Middle Eastern/South Asian 5 Native American 6 Pacific Islander 7 White 8 Multiracial",
#   "Perceived race or ethnicity of person stopped Asian. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stoppep Black or African American. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Hispanic or Latino. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Middle Eastern or South Asian. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Native. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped Pacific Islander. 0 No 1 Yes",
#   "Perceived race or ethnicity of person stopped White. 0 No 1 Yes",
#   "Indicates that the officer selected multiple values for the perceived race/ethnicity of the person stopped. columns N thru T",
#   "Perceived gender of person stopped.1 Male 2 Female 3 Transgender Man/Boy 4 Transgender Woman/Girl 5 Gender Nonconforming",
#   "Perceived gender of person stopped male. 0 No 1 Yes",
#   "Perceived gender of person stopped female. 0 No 1 Yes",
#   "Perceived gender of person stopped transgender man/boy. 0 No 1 Yes",
#   "Perceived gender of person stopped transgender woman/girl. 0 No 1 Yes",
#   "Perceived gender of person stopped gender non-conforming. 0 No 1 Yes",
#   "Perceived gender of person stopped gender non-conforming and one other gender group. 0 No 1 Yes",
#   "Person stopped perceived to be LGBT. 0 No 1 Yes",
#   "Perceived age of person stopped. Numeric",
#   "Perceived age of person stopped categorized into groups. 1 1 to 9 2 10 to 14 3 15 to 17 4 18 to24 5 25 to 34 6 35 to 44 7 45 to 54 8 55 to 64 9 65 and older",
#   "Person stopped has limited or no English fluency. 0 No 1 Yes",
#   "Perceived or known disability of person stopped. Individuals perceived as having one or more disabilities columns. 0 No Disability 1 Deafness 2 Speech Impairment 3 Blind 4 Mental Health Condition 5 Development 6 Hyperactivity 7 Other 8 Multiple Disability",
#   "Perceived or known disability of person stopped deafness or difficulty hearing. 0 No 1 Yes",
#   "Perceived or known disability of person stopped speech impairment or limited use of language. 0 No 1 Yes",
#   "Perceived or known disability of person stopped blind or limited vision. 0 No 1 Yes",
#   "Perceived or known disability of person stopped mental health condition. 0 No 1 Yes",
#   "Perceived or known disability of person stopped intellectual or developmental. 0 No",
#   "Perceived or known disability of person stopped disability related to hyperactivity or impulsive behavior. 0 No 1 Yes",
#   "Perceived or known disability of person stopped other disability. 0 No 1 Yes",
#   "Perceived or known disability of person stopped no disability. 0 No 1 Yes",
#   "Perceived or known disability of person stopped no disability, one disability, or more than one disability. 0 No Disability 1 One Disability 2 Multiple Disabilities",
#   "Reason for stop traffic violation, reasonable suspicion, parole/probation arrest warrant, investigation for truancy, consensual encounter. 1 Traffic Violation 2 Reasonable suspicion 3 Parole/probation/PRCS/ mandatory supervision 4 Knowledge of outstanding arrest/wanted person 5 Investigation to determine whether person was truant 6 Consensual encounter resulting in search 7 Possible conduct under Education Code 8 Determine whether student violated school policy",
#   "Type of traffic violation. 1 Moving 2 Equipment 3 Nonmoving Blank",
#   "Code section related to traffic violation. CJIS offense table code Blank",
#   "If known, code for suspected reason for stop violation. CJIS offense table code Blank",
#   "Reasonable suspicion officer witnessed commission of a crime. 0 No 1 Yes Blank",
#   "Reasonable suspicion matched suspect description. 0 No 1 Yes Blank",
#   "Reasonable suspicion witness or victim identification of suspect at the scene. 0 No 1 Yes Blank",
#   "Reasonable suspicion carrying suspicious object. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of casing a victim or location. 0 No 1 Yes Blank",
#   "Reasonable suspicion suspected of acting as a lookout. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of a drug transaction. 0 No 1 Yes Blank",
#   "Reasonable suspicion actions indicative of engaging a violent crime. 0 No 1 Yes Blank",
#   "Reasonable suspicion other reasonable suspicion of a crime. 0 No 1 Yes Blank",
#   "Section Code. 1 48900 2 48900.2 3 48900.3 4 48900.4 5 48900.7 Blank",
#   "When EC 48900 is selected, specify the subdivision. 1 48900a1 2 48900a2 3 48900b 4 48900 c 5 48900 d 6 48900 e 7 48900 f 8 48900 g",
#   "Stop made in response to a call for service. 0 No 1 Yes",
#   "Action taken by officer during stop person removed from vehicle by order. 0 No 1 Yes",
#   "Action taken by officer during stop person removed from vehicle by physical contact. 0 No 1 Yes",
#   "Action taken by officer during stop field sobriety test. 0 No 1 Yes",
#   "Action taken by officer during stop curbside detention. 0 No 1 Yes",
#   "Action taken by officer during stop handcuffed or flex cuffed. 0 No 1 Yes",
#   "Action taken by officer during stop patrol car detention. 0 No 1 Yes",
#   "Action taken by officer during stop canine removed search. 0 No 1 Yes",
#   "Action taken by officer during stop firearm pointed at person. 0 No 1 Yes",
#   "Action taken by officer during stop firearm discharged or used. 0 No 1 Yes",
#   "Action taken by officer during stop electronic device used. 0 No 1 Yes",
#   "Action taken by officer during stop impact projectile discharged or used e.g. blunt impact projectile, rubber bullets, bean bags. 0 No 1 Yes",
#   "Action taken by officer during stop canine bit or held person. 0 No 1 Yes",
#   "Action taken by officer during stop baton or other impact weapon used. 0 No 1 Yes",
#   "Action taken by officer during stop chemical spray use pepper spray, mace, tear gas, or other chemical irritants. 0 No 1 Yes",
#   "Action taken by officer during stop other physical or vehicle contact. 0 No 1 Yes",
#   "Action taken by officer during stop person photographed. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search person. 0 No 1 Yes",
#   "Action taken by officer during stop search of person was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop asked for consent to search. 0 No",
#   "Action taken by officer during stop search of property was conducted. 0 No 1 Yes",
#   "Action taken by officer during stop property was seized. 0 No 1 Yes",
#   "Action taken by officer during stop vehicle impound. 0 No 1 Yes",
#   "Action taken by officer during stop admission or written statement obtained from student. 0 No 1 Yes",
#   "Action taken by officer during stop none. 0 No 1 Yes",
#   "Action taken by officer during stop specify if consent was given for search of person. 0 No 1 Yes Blank",
#   "Action taken by officer during stop specify if consent was given for search of property. 0 No 1 Yes Blank",
#   "Basis for search consent given. 0 No 1 Yes Blank",
#   "Basis for search officer safety/safety of others. 0 No 1 Yes Blank",
#   "Basis for search search warrant. 0 No 1 Yes Blank",
#   "Basis for search condition of parole/probation/PRCS/mandatory supervision. 0 No 1 Yes Blank",
#   "Basis for search suspected weapons. 0 No 1 Yes Blank",
#   "Basis for search visible contraband. 0 No 1 Yes Blank",
#   "Basis for search odor of contraband. 0 No 1 Yes Blank",
#   "Basis for search canine detection. 0 No 1 Yes Blank",
#   "Basis for search evidence of crime. 0 No 1 Yes Blank",
#   "Basis for search incident to arrest. 0 No 1 Yes Blank",
#   "Basis for search exigent circumstances. 0 No 1 Yes Blank",
#   "Basis for search vehicle inventory for search property only. 0 No 1 Yes Blank",
#   "Basis for search suspected violation of school policy. 0 No 1 Yes",
#   "Contraband or evidence discovered none. 0 No 1 Yes",
#   "Contraband or evidence discovered firearm. 0 No",
#   "Contraband or evidence discovered ammunition. 0 No 1 Yes",
#   "Contraband or evidence discovered weapon. 0 No 1 Yes",
#   "Contraband or evidence discovered drugs/narcotics. 0 No 1 Yes",
#   "Contraband or evidence discovered alcohol. 0 No 1 Yes",
#   "Contraband or evidence discovered money. 0 No 1 Yes",
#   "Contraband or evidence discovered drug paraphernalia. 0 No 1 Yes",
#   "Contraband or evidence discovered stolen property. 0 No 1 Yes",
#   "Contraband or evidence discovered cell phone or electronic device. 0 No 1 Yes",
#   "Contraband or evidence discovered other contraband or evidence. 0 No 1 Yes",
#   "Basis for property seizure safekeeping as allowed by law/statute. 0 No 1 Yes Blank",
#   "Basis for property seizure contraband. 1 No 1 Yes Blank",
#   "Basis for property seizure evidence. 2 No 1 Yes Blank",
#   "Basis for property seizure impound of vehicle. 3 No 1 Yes Blank",
#   "Basis for property seizure abandoned property. 4 No 1 Yes Blank",
#   "Basis for property seizure suspected violation of school policy. 0 No 1 Yes Blank",
#   "Type of property seized firearm. 0 No 1 Yes Blank",
#   "Type of property seized ammunition. 0 No 1 Yes Blank",
#   "Type of property seized weapon other than firearm. 0 No 1 Yes Blank",
#   "Type of property seized drugs/narcotics. 0 No 1 Yes Blank",
#   "Type of property seized alcohol. 0 No 1 Yes Blank",
#   "Type of property seized money. 0 No 1 Yes Blank",
#   "Type of property seized drug paraphernalia. 0 No 1 Yes Blank",
#   "Type of property seized stolen property. 0 No 1 Yes Blank",
#   "Type of property seized cellphone. 0 No 1 Yes Blank",
#   "Type of property seized vehicle. 0 No 1 Yes Blank",
#   "Type of property seized other contraband. 0 No 1 Yes Blank",
#   "Result of stop no action. 0 No 1 Yes",
#   "Result of stop warning verbal or written. 0 No",
#   "Result of stop citation for infraction. 0 No 1 Yes",
#   "Result of stop in field cite and release. 0 No 1 Yes",
#   "Result of stop custodial pursuant to outstanding warrant. 0 No 1 Yes",
#   "Result of stop custodial arrest without warrant. 0 No 1 Yes",
#   "Result of stop field interview card completed. 0 No 1 Yes",
#   "Result of stop noncriminal transport or caretaking transport including transport by officer, transport by ambulance, or transport by another agency. 0 No 1 Yes",
#   "Result of stop contacted parent/legal guardian or other person responsible for minor. 0 No 1 Yes",
#   "Result of stop psychiatric hold. 0 No 1 Yes",
#   "Result of stop referred to US Department of Homeland Security ICE. 0 No 1 Yes",
#   "Result of stop referral to school administrator. 0 No 1 Yes",
#   "Result of stop referral to school counselor or other support staff. 0 No 1 Yes",
#   "Result of stop warning code. Five digit numeric code Blank",
#   "Result of stop citation for infraction codes. Five digit numeric code Blank",
#   "Result of stop in field cite and release codes. Five digit numeric code Blank",
#   "Result of stop custodial arrest without warrant codes. Five digit numeric code Blank",
#   "County Name"
# )
# 
# 
# add_table_comments(con, schema, table_name, indicator, source, column_names, column_comments)



##### Begin RACE COUNTS prep #
# Import RIPA postgres table --------------------------------------------------------
ripa_orig <- dbGetQuery(con, "SELECT county, agency_name, call_for_service, rae_hispanic_latino, rae_full, rae_native_american, 
                                  rae_pacific_islander, rae_middle_eastern_south_asian FROM crime_and_justice.cadoj_ripa_2022 WHERE call_for_service = 0;") 

# manual cleaning so that unique agency names to match to cities later
agency_names <- as.data.frame(unique(ripa_orig$agency_name))
colnames(agency_names)[1] = "agency_name"
agency_names_ <- agency_names %>% mutate(agency_name_new = gsub(" PD", "", agency_name))  # clean police dept names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SO", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SD", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" COUNTY SO", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SHERIFF'S DEPT", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" CO SHERIFF", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" COUNTY SHERIFF'S OFFICE", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" SHERIFF", " COUNTY", agency_name_new))  # clean Sheriff's names
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPARTMENT", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPARTME", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPAR", "", agency_name_new)) 
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DEPT", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" POLICE DE", "", agency_name_new))
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub("-COMM", "", agency_name_new))  
agency_names_ <- agency_names_ %>% mutate(agency_name_new = gsub(" - COMM", "", agency_name_new))  

agency_names_ <- agency_names_ %>% mutate(agency_name_new = str_to_title(agency_names_$agency_name_new))
agency_names_ <- agency_names_  %>% mutate(agency_name_new = 
                                                ifelse(agency_name_new == 'Lapd', "Los Angeles", agency_name_new))
agency_names_ <- agency_names_  %>% mutate(agency_name_new = 
                                                ifelse(agency_name_new == 'Atherton #1', "Atherton", agency_name_new)) 

ripa_final <- ripa_orig %>% left_join(agency_names_, by = "agency_name") # join cleaned agency names to ripa data


# filter data for officer-initiated stops by race/eth --------------------------------------------------------
ripa_cfs <- ripa_final %>% mutate(state_id = '06')  


#### Calc counts by race ####
# source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/crime_justice_functions.R")
# temporary file path used to run code before merge source("W:/Project/RACE COUNTS/2024_v6/RC_Github/EMG/RaceCounts/Functions/crime_justice_functions.R")
state_calcs <- stops_by_state(ripa_cfs)
county_calcs <- stops_by_county(ripa_cfs) %>% mutate(county = gsub(" County", "", county))
agency_calcs <- stops_by_agency(ripa_cfs) # this df includes agencies at all levels: state, county, city, school district, etc.


# get pop data by race
pop <- dbGetQuery(con2, "SELECT * FROM v6.arei_race_multigeo") %>% mutate(name = gsub(" County, California", "", name),
                                                                          name =  gsub(" CDP, California", "", name),
                                                                          name =  gsub(" city, California", "", name),
                                                                          name =  gsub(" town, California", "", name),
                                                                          name =  gsub(", California", "", name)) %>% 
                                                                   select(-c(contains(c("swana_", "nh_other_", "pct_"))))

nh_aian_pacisl <- dbGetQuery(con, "SELECT geoid, dp05_0081e AS nh_aian_pop, dp05_0083e as nh_pacisl_pop FROM demographics.acs_5yr_dp05_multigeo_2022 WHERE geolevel IN ('state', 'county', 'place')")
pop <- pop %>% full_join(nh_aian_pacisl)


state_df <- pop %>% filter(geolevel == 'state') %>% left_join(state_calcs, by = c("geoid" = "state_id"))
county_df <- pop %>% filter(geolevel == 'county') %>% left_join(county_calcs, by = c("name" = "county"))
city_df <- pop %>% filter(geolevel == 'place') %>% left_join(agency_calcs, by = c("name" = "agency_name_new"))
# na_cities <- city_df %>% filter(is.na(total_stops)) # n = 1281, the cities on this list are covered by County Sheriff's that serve many cities/areas, with exception of SF.

# combine city, county, state data
all_df <- rbind(state_df, county_df, city_df) %>% rename(geoname = name)

# copy SF County (Sheriff) data to SF City record since SF Sheriff primarily serves SF City
sf_stops <- all_df %>% filter(geoname == 'San Francisco' & geolevel == 'county') %>% select(geoname, ends_with("_stops"))
all_df <- all_df %>% rows_update(sf_stops, by = "geoname")

# copy 	S. San Francisco PD data to South San Francisco record
so_sf_stops <- agency_calcs %>% filter(agency_name_new=='S. San Francisco') %>% mutate(geoname="South San Francisco")%>%select(geoname, ends_with("_stops"))
all_df <- all_df %>% rows_update(so_sf_stops, by = "geoname")


# Data screening  --------------------------------------------------------
pop_threshold = 100
stop_threshold = 5  # update appropriately each year to ensure counties/cities with few stops and small pops do not result in outlier rates

df_screened <- all_df %>% mutate(
                # screen raw counts
                total_raw = ifelse(total_pop < pop_threshold | total_stops< stop_threshold, NA, total_stops),
                latino_raw = ifelse(latino_pop < pop_threshold | latino_stops< stop_threshold, NA, latino_stops),
                nh_white_raw = ifelse(nh_white_pop < pop_threshold | nh_white_stops < stop_threshold, NA, nh_white_stops),
                nh_black_raw = ifelse(nh_black_pop < pop_threshold | nh_black_stops < stop_threshold, NA, nh_black_stops),
                nh_asian_raw = ifelse(nh_asian_pop < pop_threshold | nh_asian_stops < stop_threshold, NA, nh_asian_stops),
                nh_twoormor_raw = ifelse(nh_twoormor_pop < pop_threshold | nh_twoormor_stops < stop_threshold, NA, nh_twoormor_stops),
                nh_aian_raw = ifelse( nh_aian_pop < pop_threshold |  nh_aian_stops < stop_threshold, NA,  nh_aian_stops),
                nh_pacisl_raw = ifelse(nh_pacisl_pop < pop_threshold | nh_pacisl_stops < stop_threshold, NA, nh_pacisl_stops),
                aian_raw = ifelse(aian_pop < pop_threshold |  aian_stops < stop_threshold, NA,  aian_stops),
                pacisl_raw = ifelse(pacisl_pop < pop_threshold | pacisl_stops < stop_threshold, NA, pacisl_stops),
                swanasa_raw = ifelse(swanasa_pop < pop_threshold | swanasa_stops < stop_threshold, NA, swanasa_stops),
                
                # calc rates
                total_rate = (total_raw/total_pop) * 1000,
                latino_rate = (latino_raw/latino_pop) * 1000,
                nh_white_rate = (nh_white_raw/nh_white_pop) * 1000,
                nh_black_rate = (nh_black_raw/nh_black_pop) * 1000,
                nh_asian_rate = (nh_asian_raw/nh_asian_pop) * 1000,
                nh_twoormor_rate = (nh_twoormor_raw/nh_twoormor_pop) * 1000,
                nh_aian_rate = (nh_aian_raw/nh_aian_pop) * 1000,
                nh_pacisl_rate = (nh_pacisl_raw/nh_pacisl_pop) * 1000,
                aian_rate = (aian_raw/aian_pop) * 1000,
                pacisl_rate = (pacisl_raw/pacisl_pop) * 1000,
                swanasa_rate = (swanasa_raw/swanasa_pop) * 1000
)

d <- df_screened %>% select(-c(contains(c("_stops")), starts_with(c("nh_twoormor","aian_","pacisl")))) # drop multiracial bc of mismatch with pop denominator and drop aian and nhpi alone or in combo

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'. 

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns. Drop unneeded cols.
state_table <- d[d$geoname == 'California', ] %>% select(-c(geolevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#split COUNTY into separate table and format id, name columns. Drop unneeded cols.
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'place', ] %>% select(-c(geolevel))

#calculate city z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


###update info for postgres tables will automatically update###
county_table_name <- paste0("arei_crim_officer_initiated_stops_county_", rc_yr)
state_table_name <- paste0("arei_crim_officer_initiated_stops_state_", rc_yr)
city_table_name <- paste0("arei_crim_officer_initiated_stops_city_", rc_yr)

indicator <- paste0("Officer initiated stops per 1,000 people. Raw is total number of officer initiated stops. Note: City data is based only on the largest agency in that city. In addition, stops are assigned to the geography where the law enforcement agency is located, not where the stop occurred. This data is")
source <- paste0("CADOJ RIPA ",curr_yr, " https://openjustice.doj.ca.gov/data")

# #send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# 
# dbDisconnect(con)
# dbDisconnect(con2)









