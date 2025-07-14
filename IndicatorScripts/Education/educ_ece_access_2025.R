##install packages if not already installed ------------------------------
packages <- c("dplyr","tidyr","tidycensus","tigris","readxl","sf","tidyverse","usethis","RPostgres")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  
  library(pkg, character.only = TRUE) 
} 

options(scipen=999)


# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
conn <- connect_to_db("rda_shared_data")

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Sheet_ECE.docx"

#set source for Weighted Average Functions & SWANA Ancestry scripts
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")

#### 1. SET UP: DEFINE VARIABLES, FX SOURCE, GET COUNTY NAMES ####

# define variables used in several places that must be updated each year
curr_yr <- "2020-2021"  # must keep same format
pop_yr <- 2020
rc_schema <- "v7"
rc_yr <- "2025"
sen_assm_threshold <- .25   # tracts with >= threshold % of tract pop w/i district OR district pop w/i tract are assigned to district 

# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'zcta_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'zcta_2020_state_senate_2024'     # Name of tract-Sen xwalk table


#### Check that P12 variables and RC race names still match each year and update if needed --------
## Population: All AIAN/PacIsl Latinx incl, NH Alone White/Black/Asian/Other, NH Two+, Latinx of any race
sum_file <- "dhc"   # select specific Census file
vars_list_ <- c("male_total_" = "P12_003N",     # male
                   "male_aian_" = "P12AE_003N", 
                   "male_pacisl_" = "P12AG_003N", 
                   "male_latino_" = "P12H_003N", 
                   "male_nh_white_" = "P12I_003N", 
                   "male_nh_black_" = "P12J_003N", 
                   "male_nh_asian_" = "P12L_003N", 
                   "male_nh_other_" = "P12N_003N", 
                   "male_nh_twoormor_" = "P12O_003N",
                   "female_total_" = "P12_027N",     # female
                   "female_aian_" = "P12AE_027N", 
                   "female_pacisl_" = "P12AG_027N", 
                   "female_latino_" = "P12H_027N", 
                   "female_nh_white_" = "P12AC_027N", 
                   "female_nh_black_" = "P12AD_027N", 
                   "female_nh_asian_" = "P12AF_027N", 
                   "female_nh_other_" = "P12AB_027N", 
                   "female_nh_twoormor_" = "P12O_027N") 

race_mapping <- data.frame(
  name = unlist(vars_list_),
  race = names(vars_list_),
  stringsAsFactors = FALSE
)

p12_curr <- load_variables(pop_yr, sum_file, cache = TRUE) %>% 
  filter(name %in% vars_list_) %>%
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop")) %>%
  select(-race)

vars_list_p12 <- vars_list_  # vars used in update_detailed_table_census{}


# CHECK THIS TABLE TO MAKE SURE THE LABEL AND RC_RACES COLUMNS MATCH UP
print(p12_curr) 


# set values for weighted average functions - You may need to update these
#year <- c(2020)                   # define your data vintage
#subgeo <- c('zcta')               # define your sub geolevel: can be tract or zcta (zcta may require some additions to the fx since they are mostly for tract)
#targetgeolevel <- c('county')     # define your target geolevel
#survey <- "census"                # define which Census survey you want
#pop_threshold = 50                # define population threshold for screening
#vars_list <- "vars_list_p12"      # pop under 5 by race/eth, the list of variables is in the WA fx script
#county_yr <- 2021
#targetgeo_names <- county_names(var_list = vars_list_acs, yr = county_yr, srvy = "acs5")              # use fx to get county names


#### 2. AIR TK ENR DATA ####
## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
county_name <- get_acs(geography = "county", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = pop_yr)

county_name <- county_name[,1:2]
#county_name$NAME <- str_remove(county_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geonames
county_name$NAME <- gsub(" County, California", "", county_name$NAME)
names(county_name) <- c("target_id", "target_name")
# View(county_name)



#get tk data
air_tk <- read_xlsx("W:/Data/Education/American Institute for Research/2020/tk.xlsx", range = "A4:F2566", na = "*") #keep this one don't push to postgres
# air_tk_meta <- read_xlsx("W:/Data/Education/American Institute for Research/2020/tk.xlsx", range = "A2:F5", na = "*")
# air_tk_meta <- air_tk_meta[-which(air_tk_meta[1]=='California'),] # check against subset below
# View(air_tk_meta)

names(air_tk) <- c("geoname", "pct_zip", "three", "four", "five", "tk") 

#join county ids
air_tk <- left_join(air_tk, county_name, by = c("geoname" = "target_name")) %>% 
  
  #fill in the blanks for county ids, 
  fill(target_id) %>% 
  
  #filter out county totals
  filter(pct_zip != "Percent of Zip Code Allocation")  %>%
  
  #create county-zip id
  mutate(zip_county_id = paste0(geoname, "-", target_id)) %>% 
  
  #remove unnecessary fields
  select(-"three", -"four", -"five")


#### 3. CCCRRN ENR DATA ####

#get CCCRRN data
cccrrn <- read_xlsx("W:/Data/Education/CCCRRN/2021/CatalystCA2021Data_rev.xlsx") %>% rename(geoname = ZIPCODE)

#format columns for join
cccrrn$geoname <- as.character(cccrrn$geoname) 


#### 4. Join AIR TK and CCCRRN then calculate enrollment counts ####

# calculated as we did for education.ece_zip_code_enrollment_rate_2018 used in RC v3
## which assumes ccrrn capacity = full enrollment. 
df <- full_join(air_tk, cccrrn, by = "geoname") %>% rename("sub_id" = "geoname")  # join AIR TK and CCCRRN enr data
df$enrollment <- rowSums(df[,c("INFCAP", "PRECAP", "FCCCAP", "tk")], na.rm=TRUE) # unweighted enrollment
df <- filter(df, enrollment >= 0)

# import ZCTA-County Relationship File from Census. https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2020.html#zcta
## Read more: https://www2.census.gov/geo/pdfs/maps-data/data/rel2020/zcta520/explanation_tab20_zcta520_county20_natl.pdf and
##   https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/2020-zcta-record-layout.html#county
filepath <- "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"
rel_file <- read_delim(file = filepath, delim = "|", na = c("*", ""))
county_xwalk <- filter(rel_file, (substr(GEOID_COUNTY_20,1,2) == '06' & !is.na(GEOID_ZCTA5_20))) %>%
  rename(zcta_id = GEOID_ZCTA5_20, county_id = GEOID_COUNTY_20, county_name = NAMELSAD_COUNTY_20) %>%
  mutate(pct_zcta = AREALAND_PART / AREALAND_ZCTA5_20) %>%      # calc pct of zcta within each county it overlaps with
  select(c(zcta_id, county_id, county_name, pct_zcta))


############# ASSEMBLY DISTRICTS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('zcta')              # define your sub geolevel: zcta (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldl')      # define your target geolevel: state assembly
survey <- "census"               # define which Census survey you want
pop_threshold = 50               # define population threshold for screening

### Load CT-Assm Crosswalk ### 
xwalk_assm <- dbGetQuery(conn, paste0("SELECT geo_id, ", assm_geoid, ", afact FROM crosswalks.", assm_xwalk))

#### Get ZCTA under 5 pop by race ##
pop <- update_detailed_table_census(vars = vars_list_p12, yr = pop_yr, srvy = survey, subgeo = subgeo, sumfile = sum_file)  # subgeolevel pop
pop_ <- as.data.frame(pop) %>% select(-NAME)
pop_ <- pop_ %>% mutate(variable = gsub("female_|male_", "", variable))
pop_ <- pop_ %>% group_by(GEOID, variable, geolevel) %>% summarise(value_sum=sum(value, na.rm=TRUE))
pop_wide <- pop_ %>% pivot_wider(id_cols = c(GEOID, geolevel), names_from = variable, values_from = value_sum)

pop_wide_assm <- pop_wide %>% right_join(select(xwalk_assm, c(geo_id, assm_geoid, afact)), by = c("GEOID" = "geo_id"))  # join target geoids/names
pop_wide_assm <- dplyr::rename(pop_wide_assm, sub_id = GEOID, target_id = assm_geoid)                            # rename to generic column names for WA functions
pop_wide_assm <- pop_wide_assm %>% rename_with(~ paste0(.x, "_sub_pop"), ends_with("_"))

# Calc ZCTA pop in each targetgeo using afact (pct of zcta pop in targetgeo)
pop_wt <- pop_wide_assm %>% 
  select(c(sub_id, target_id, afact, ends_with("sub_pop"))) %>% 
  mutate(across(where(is.numeric), ~.x * afact)) %>% #calc wt zcta pop
  select(-afact) 


# Get Targetgeo Pop
pop_df <- update_detailed_table_census(vars = vars_list_p12, yr = pop_yr, srvy = survey, subgeo = "State Legislative District (Lower Chamber)", sumfile = sum_file)
pop_df_ <- as.data.frame(pop_df) %>% select(-NAME)
pop_df_ <- pop_df_ %>% mutate(variable = gsub("female_|male_", "", variable))
pop_df_ <- pop_df_ %>% group_by(GEOID, variable, geolevel) %>% summarise(value_sum=sum(value, na.rm=TRUE))
pop_wide_ <- pop_df_ %>% pivot_wider(id_cols = c(GEOID, geolevel), names_from = variable, values_from = value_sum)
pop_wide_ <- pop_wide_ %>% rename(target_id = GEOID)
pop_wide_ <- pop_wide_ %>% rename_with(~ paste0(.x, "_target_pop"), ends_with("_"))


# Calc % of each targetgeo pop that each ZCTA pop
pop_wt <- pop_wt %>% left_join(pop_wide_, by = "target_id")       #join subpop data to targetpop data to get pct_zcta
n_df <- pop_wt %>% select(target_id, sub_id) %>% group_by(target_id) %>% summarise(n = n()) # count of sub_geos in each target_geo
pop_wt <- pop_wt %>% left_join(n_df, by = "target_id")

pct_df <- pop_pct_multi(pop_wt)        # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel


#colnames(pop_wt) <- gsub('unw_', '', colnames(pop_wt)) # drop unw (unweighted) part of colnames
#n_df <- pop_wt %>% select(target_id, sub_id) %>% group_by(target_id) %>% summarise(n = n()) # count of sub_geos in each target_geo
#pop_wt <- pop_wt %>% left_join(n_df, by = "target_id")



##### ASSEMBLY WEIGHTED AVG CALCS ###
pop_df <- targetgeo_pop(pop_wide_assm) # calc target geolevel pop and number of sub geolevels per target geolevel
pct_df <- pop_pct_multi(pop_df)        # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
assm_wa <- wt_avg(pct_df)              # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% mutate(geolevel = 'sldl')                  # add geolevel


# pop_wide <- pop_ %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, geolevel), names_from = p12_race, values_from = value_sum)
# pop_wide <- pop_wide %>% right_join(select(xwalk, c(zcta_id, county_id, county_name)), by = c("GEOID" = "zcta_id"))  # join target geoids/names
# pop_wide <- pop_wide %>% relocate(county_id, .after = "GEOID") %>% relocate(county_name, .after = "county_id")
# colnames(pop_wide)[5:ncol(pop_wide)] <- paste(colnames(pop_wide)[5:ncol(pop_wide)], "unw_sub_pop", sep = "_") # rename sub_pop columns
# pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = county_id, target_name = county_name) # rename to generic column names for WA functions




# Weight ZCTA pop by % of ZCTA within each county it overlaps with
# pop_wide <- pop_wide %>% left_join(xwalk %>% select(zcta_id, county_id, pct_zcta), by = c("sub_id" = "zcta_id", "target_id" = "county_id")) #join pop data to xwalk to get pct_zcta
# pop_wt <- pop_wide %>% select(c(sub_id, target_id, pct_zcta, ends_with("sub_pop"))) %>% mutate(across(where(~is.numeric(.)), ~.x*pct_zcta)) #calc wt zcta pop
# colnames(pop_wt) <- gsub('unw_', '', colnames(pop_wt)) # drop unw (unweighted) part of colnames
# n_df <- pop_wt %>% select(target_id, sub_id) %>% group_by(target_id) %>% summarise(n = n()) # count of sub_geos in each target_geo
# pop_wt <- pop_wt %>% left_join(n_df, by = "target_id")


#### 6. Get county and state under 5 pop by race ####

###### County Pop ##
# Get target pop directly from API, rather than use targetgeo_pop{}, bc ZCTAs don't cover all of counties and don't fully nest into counties as CTs do.
### This is total county pop, not the total of county pop that resides within a zcta.
pop_target <- list(get_decennial(geography = "county", state = "CA", variables = vars_list_p12, year = year, sumfile = "dhc") %>% 
                    mutate(geolevel = "county"))
pop_target <- lapply(pop_target, function(x) cbind(x, table = str_extract(x$variable, "[^_]+"))) # create table field used in next step
pop_target_ <- as.data.frame(pop_target) %>% select(c(GEOID, table, value)) %>% group_by(GEOID, table) %>% summarise(value_sum=sum(value))
pop_target_ <- pop_target_ %>% left_join(p12_meta, by = c("table" = "p12_table")) # add race field
pop_target_wide <- pop_target_ %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID), names_from = p12_race, values_from = value_sum)
pop_target_wide <- pop_target_wide %>% rename(target_id = GEOID)
colnames(pop_target_wide)[2:ncol(pop_target_wide)] <- paste(colnames(pop_target_wide)[2:ncol(pop_target_wide)], "target_pop", sep = "_") # rename target_pop columns

#join county data to zcta data and format
pop_df <- left_join(pop_wt, pop_target_wide, by="target_id")


###### State Pop ##
### This is total state pop, not the total of state pop that resides within a zcta.
pop_state <- list(get_decennial(geography = "state", state = "CA", variables = vars_list_p12, year = year, sumfile = "dhc") %>% 
                     mutate(geolevel = "state"))
ca_pop <- lapply(pop_state, function(x) cbind(x, table = str_extract(x$variable, "[^_]+"))) # create table field used in next step
ca_pop <- as.data.frame(ca_pop) %>% select(c(GEOID, table, value)) %>% group_by(GEOID, table) %>% summarise(value_sum=sum(value))
ca_pop <- ca_pop %>% left_join(p12_meta, by = c("table" = "p12_table")) %>% select(-c(table)) # add race field
ca_pop <- ca_pop %>% rename(target_id = GEOID, target_pop = value_sum, raceeth = p12_race)
ca_pop_wide <- pivot_wider(ca_pop, id_cols = target_id, names_from = raceeth, names_glue = "{raceeth}_{.value}", values_from = target_pop)


#### 7. Calc indicator by zcta ################
# get indicator for county (weighted total enr rate for zctas)
ind_df <- df %>% select(sub_id, enrollment) %>% left_join(pop_df %>% select(sub_id, target_id, total_sub_pop, pct_zcta), by = c("sub_id"), relationship = "many-to-many") 
ind_df <- ind_df %>% mutate(wt_enr = ind_df$enrollment * ind_df$pct_zcta) %>% unique() # calc weighted enrollment and keep only unique rows (zcta-county combos)

ind_df$indicator <- ind_df$wt_enr / ind_df$total_sub_pop * 100 # calc indicator (weighted total enr rate by zcta)
ind_df$indicator[ind_df$indicator == "Inf"] <- 100
ind_df_cnty <- ind_df # NOTE: ind_df will be overwritten later during state WA calcs, so storing as another df for later


#### 8. Calc weighted averages ################
pop_threshold = 50 # same threshold as used in previous calcs

##### COUNTY WEIGHTED AVG CALCS ###
    pct_df <- pop_pct_multi(pop_df) # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
    wa <- wt_avg(pct_df)            # calc weighted average and apply reliability screens
    
    wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')    # add in target geolevel names and geolevel type


##### STATE WEIGHTED AVG CALCS ###
# This code comes from/replaces ca_pop_pct{} which works with tracts but not zctas
    subpop <- pop_wide %>% select(-c(pct_zcta, target_name, geolevel))
    subpop$target_id <- '06'                                           # replace county target_id values w/ state-level target_id value
    subpop_long <- pivot_longer(subpop, 3:ncol(subpop), names_to="raceeth", values_to="sub_pop") %>% unique()
    subpop_long$raceeth <- gsub("_unw_sub_pop","-",as.character(subpop_long$raceeth))              # update to generic raceeth names
    
    ca_pop_long <- pivot_longer(ca_pop_wide, 2:ncol(ca_pop_wide), names_to="raceeth", values_to="target_pop")
    ca_pop_long$raceeth <- gsub("_target_pop","-",as.character(ca_pop_long$raceeth))               # update to generic raceeth names
    
    subpop_long <- subpop_long %>% left_join(ca_pop_long, by=c("target_id" = "target_id", "raceeth" = "raceeth"))  # join target and sub pops in long form
    ca_pcts_long <- subpop_long %>% mutate(pct = ifelse(target_pop < pop_threshold, NA, (sub_pop / target_pop)),   # calc pcts of each target geolevel pop per sub geolevel pop
                                           measure_pct=sub("-", "_pct_target_pop", raceeth))           # create new column names
    ca_pct_df <- ca_pcts_long %>% select(sub_id, target_id, measure_pct, pct) %>%              # pivot long table back to wide keeping only new columns
      pivot_wider(names_from=measure_pct, values_from=pct)
    
    # get indicator for state (unweighted total enr rate for zctas)
    ### unweighted bc for state-level, it doesn't matter if a zcta is split btwn 1 or more counties
    ind_df_st <- ind_df %>% select(c(sub_id, enrollment))
    ind_df_st <- ind_df_st %>% unique() %>% left_join(pop_wide %>% select(sub_id, total_unw_sub_pop) %>% unique(), by = "sub_id")
    ind_df_st$indicator <- ind_df_st$enrollment / ind_df_st$total_unw_sub_pop * 100
    ind_df <- ind_df_st  # note: this overwrites the weighted indicator ind_df
    
    ca_wa <- ca_wt_avg(ca_pct_df) %>% mutate(geolevel = 'state')   # add geolevel type
    n_st <- length(unique(pop_wide$sub_id))  # get count of zctas statewide
    ca_wa$n <- n_st # add unique count of zctas
    
######## Leg Weighted Avg Calcs ######
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Sheet_ECE_Access.docx"
    
it_leg_data <- dbGetQuery(con2, "SELECT * FROM education.air_leg_unmet_it_2020")
prek_leg_data <- dbGetQuery(con2, "SELECT * FROM education.air_leg_unmet_prek_2020")
    
#geocorr crosswalks these are pop weighted air is area weighted but leave air alone.
xwalk_sldl_zcta <- dbGetQuery(con2, "SELECT * FROM crosswalks.zcta_2020_state_assembly_2024")
xwalk_sldu_zcta <- dbGetQuery(con2, "SELECT * FROM crosswalks.zcta_2020_state_senate_2024")

# ECE Access Prep for Leg Dist 
# 
# Data management-related 
# 
# Download TK by Assm/Sen from AIR/ELNAT, like this file: W:\Data\Education\American Institute for Research\tk.xlsx 
# 
# Get Infant/Prek from: W:\Data\Education\CCCRRN\2021\CatalystCA2021Data_rev.xlsx 
# 
# Then use xwalks (crosswalks.zcta_2020_state_assembly_2024 / crosswalks.zcta_2020_state_senate_2024) to assign to Assm/Sen. 
# 
# Use afact/afact2 to decide which zcta’s to assign to Assm/Sen. See RC methodology here: RC 2025 Workflow.xlsx 
#columns afact and afact2 represent the percetage of the zcta thats in the district and vice versa: we are using a 25% threshold. Also the data is population weighted. would only assign to the district if the xcta is 25% weighted in that district


#### 9. Join county & state WA tables  ##################
wa_all <- union(wa, ca_wa)
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid)# move geoname column

d <- wa_all
View(d)


#### 10. Calc RACE COUNTS stats ##############
###### To use the following RC Functions, 'd' will need the following columns at minimum: 
###### county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
#View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
View(county_table)

#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to state_id, county_id, city_id, leg_id
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
leg_table <- rename(leg_table, leg_id = geoid, leg_name = geoname)

### update info for postgres tables - automatically updates based on variables at top of script ###
county_table_name <- paste0("arei_educ_ece_access_county_",yr)
state_table_name <- paste0("arei_educ_ece_access_state_",yr)
leg_table_name <- paste0("arei_educ_ece_access_leg_", rc_yr)
indicator <- paste0("Created on ", Sys.Date(), ". ECE Access")
source <- paste0("CCCRRN https://rrnetwork.org/ and AIR ELNAT https://elneedsassessment.org/ (", curr_yr, ")")

#send tables to postgres
#to_postgres(county_table, state_table)
# leg_to_postgres(leg_table)

#disconnect
dbDisconnect(con)
dbDisconnect(con2)





