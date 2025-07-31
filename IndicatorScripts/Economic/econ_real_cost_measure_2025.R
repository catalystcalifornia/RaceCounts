### Cost-of-Living-Adjusted Poverty v7 ### 

#install packages if not already installed
packages <- c("DBI", "tidyverse","RPostgres", "tidycensus", "readxl", "sf", "janitor")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]

if(length(install_packages) > 0) {
  install.packages(install_packages)
} else {
  print("All required packages are already installed.")
}

for(pkg in packages){
  library(pkg, character.only = TRUE)
}

options(scipen = 100) # disable scientific notation

source("W:\\RDA Team\\R\\credentials_source.R")
con_rc <- connect_to_db("racecounts")
con_shared <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2023"  # must keep same format
data_yr <- "2023"
rc_yr <- "2025"
dwnld_url <- "https://unitedwaysca.org/download-the-public-data-set/"
rc_schema <- "v7"

qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Economic\\QA_Real_Cost_Measure.docx"

# Read Data ---------------------------------------------------------------
rcm <- dbGetQuery(con_shared, "SELECT * FROM economic.uw_2025_county_state_real_cost_measure")

# select only fields we want
# asian = api for UW, so after initial grab I start renaming asian to api
rcm_subset <- rcm %>% select(geoid, geoname, geolevel,  
                             num_hh_below_rcm, pct_hh_below_rcm, 
                             num_nh_white_hh_below_rcm, pct_nh_white_hh_below_rcm,
                             num_nh_black_hh_below_rcm, pct_nh_black_hh_below_rcm,
                             num_nh_api_hh_below_rcm, pct_nh_api_hh_below_rcm,
                             num_latino_hh_below_rcm, pct_latino_hh_below_rcm,
                             num_nh_aian_hh_below_rcm, pct_nh_aian_hh_below_rcm,
                             num_nh_other_hh_below_rcm, pct_nh_other_hh_below_rcm,
                             num_hh) %>%
 
   rename(total_pop = num_hh)

# calculate rest of universes (_pops)
rcm_subset <- rcm_subset %>% mutate(nh_white_pop = num_nh_white_hh_below_rcm/pct_nh_white_hh_below_rcm,
                                    nh_black_pop = num_nh_black_hh_below_rcm/pct_nh_black_hh_below_rcm,
                                    nh_api_pop = num_nh_api_hh_below_rcm/pct_nh_api_hh_below_rcm,
                                    latino_pop = num_latino_hh_below_rcm/pct_latino_hh_below_rcm,
                                    nh_aian_pop = num_nh_aian_hh_below_rcm/pct_nh_aian_hh_below_rcm,
                                    nh_other_pop = num_nh_other_hh_below_rcm/pct_nh_other_hh_below_rcm)

# calculate ABOVE RCM raws
rcm_subset <- rcm_subset %>% mutate(total_raw = total_pop - num_hh_below_rcm,
                                    nh_white_raw = nh_white_pop - num_nh_white_hh_below_rcm,
                                    nh_black_raw = nh_black_pop - num_nh_black_hh_below_rcm,
                                    nh_api_raw = nh_api_pop - num_nh_api_hh_below_rcm,
                                    latino_raw = latino_pop - num_latino_hh_below_rcm,
                                    nh_aian_raw = nh_aian_pop - num_nh_aian_hh_below_rcm,
                                    nh_other_raw = nh_other_pop - num_nh_other_hh_below_rcm)


# calculate ABOVE RCM rates
rcm_subset <- rcm_subset %>% mutate(total_rate = (1 - pct_hh_below_rcm) * 100,
                                    nh_white_rate = (1 - pct_nh_white_hh_below_rcm) * 100,
                                    nh_black_rate = (1 - pct_nh_black_hh_below_rcm) * 100,
                                    nh_api_rate = (1 - pct_nh_api_hh_below_rcm) * 100,
                                    latino_rate = (1 - pct_latino_hh_below_rcm) * 100,
                                    nh_aian_rate = (1 - pct_nh_aian_hh_below_rcm) * 100,
                                    nh_other_rate = (1 - pct_nh_other_hh_below_rcm) * 100)

# add select just fields we want
d <- rcm_subset %>% select(geoid, geoname, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate")) %>%
  
  # filter out regions
  filter(!geoid %in% c("06117","06119","06121","06123","06125","06127","06129"))


################# STATE ASSEMBLY ########################

rcm_puma <- dbGetQuery(con_shared, "SELECT * FROM economic.uw_2025_puma_real_cost_measure") %>%
  rename(puma = geoid)

# read in croswalk
assm_crosswalk <- dbGetQuery(con_shared, "select geo_id AS puma, sldl24 AS geoid, afact from crosswalks.puma_2020_state_assembly_2024")

# join assm crosswalk to data expecting pumas will be in multiple districts
rcm_assm <- left_join(rcm_puma, assm_crosswalk, by="puma")

# select only fields we want
rcm_assm_subset <- rcm_assm %>% select(geoid, geoname, geolevel, afact,
                             num_hh_below_rcm, pct_hh_below_rcm,
                             num_nh_white_hh_below_rcm, pct_nh_white_hh_below_rcm,
                             num_nh_black_hh_below_rcm, pct_nh_black_hh_below_rcm,
                             num_nh_api_hh_below_rcm, pct_nh_api_hh_below_rcm,
                             num_latino_hh_below_rcm, pct_latino_hh_below_rcm,
                             num_nh_aian_hh_below_rcm, pct_nh_aian_hh_below_rcm,
                             num_nh_other_hh_below_rcm, pct_nh_other_hh_below_rcm,
                             num_hh) %>%

  rename(total_pop = num_hh)


# calculate rest of universes (_pops)
rcm_assm_subset <- rcm_assm_subset %>% mutate(nh_white_pop = num_nh_white_hh_below_rcm/pct_nh_white_hh_below_rcm,
                                    nh_black_pop = num_nh_black_hh_below_rcm/pct_nh_black_hh_below_rcm,
                                    nh_api_pop = num_nh_api_hh_below_rcm/pct_nh_api_hh_below_rcm,
                                    latino_pop = num_latino_hh_below_rcm/pct_latino_hh_below_rcm,
                                    nh_aian_pop = num_nh_aian_hh_below_rcm/pct_nh_aian_hh_below_rcm,
                                    nh_other_pop = num_nh_other_hh_below_rcm/pct_nh_other_hh_below_rcm)

# calculate ABOVE RCM raws
rcm_assm_subset <- rcm_assm_subset  %>% mutate(total_raw = total_pop - num_hh_below_rcm,
                                    nh_white_raw = nh_white_pop - num_nh_white_hh_below_rcm,
                                    nh_black_raw = nh_black_pop - num_nh_black_hh_below_rcm,
                                    nh_api_raw = nh_api_pop - num_nh_api_hh_below_rcm,
                                    latino_raw = latino_pop - num_latino_hh_below_rcm,
                                    nh_aian_raw = nh_aian_pop - num_nh_aian_hh_below_rcm,
                                    nh_other_raw = nh_other_pop - num_nh_other_hh_below_rcm)

# calculate pop-wt'd pct of indicator to each leg dist,
# e.g., if puma is in 2 districts, and the afact for dist 1 is .7 and the other .3, then we'd attribute 70% of incidents in that zip to dist 1 and 30% of incidents to dist 2.
#mutate(pop_wt_num_involved_civilians = num_involved_civilians * afact) %>%

# calculate rest of universes (_pops) - WEIGHTED
rcm_assm_subset <- rcm_assm_subset %>% mutate(nh_white_pop = nh_white_pop * afact,
                                              nh_black_pop = nh_black_pop * afact,
                                              nh_api_pop = nh_api_pop * afact,
                                              latino_pop = latino_pop * afact,
                                              nh_aian_pop = nh_aian_pop * afact,
                                              nh_other_pop = nh_other_pop * afact)

# calculate ABOVE RCM raws - WEIGHTED
rcm_assm_subset <- rcm_assm_subset  %>% mutate(total_raw = total_raw * afact,
                                               nh_white_raw = nh_white_raw * afact,
                                               nh_black_raw = nh_black_raw * afact,
                                               nh_api_raw = nh_api_raw * afact,
                                               latino_raw = latino_raw * afact,
                                               nh_aian_raw = nh_aian_raw * afact,
                                               nh_other_raw = nh_other_raw * afact)

# select columns we want
d_assm <- rcm_assm_subset %>% select(geoid, ends_with("_pop"), ends_with("_raw")) %>%

  # summarize by district
  group_by(geoid) %>% summarise_all(sum, na.rm=TRUE) %>%

  # calculate district rates
  mutate(total_rate = total_raw / total_pop * 100,
  nh_white_rate = nh_white_raw / nh_white_pop * 100,
  nh_black_rate = nh_black_raw / nh_black_pop * 100,
  nh_api_rate = nh_api_raw / nh_api_pop * 100,
  latino_rate = latino_raw / latino_pop * 100,
  nh_aian_rate = nh_aian_raw / nh_aian_pop * 100,
  nh_other_rate = nh_other_raw /  nh_other_pop * 100) %>%
  
  mutate(geolevel="sldl",
         geoname=paste("Assembly District", substr(geoid, 4, 5))) %>%
  select(geoid, geoname, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate")) 

# THIS RESULTS IN TOO FEW ASSEMBLY DISTRICT RATES SO DROPPING LEG DIST AS AN OPTION
# LOOKING BACK AT THE ORIGINAL DATA I ONLY SAW 1 PUMA WITH RATES FOR 3 RACES AND MAY WITH ZERO
# UPDATE: include na.rm=TRUE, there are 16 assembly districts with 3 races, 48 with 2 races, and 15 with 1 (only 1 has 0 races)
  
# NOT DOING STATE SENATE BECAUSE ASSEMBLY IS NOT GOOD ENOUGH.
  
  ################# STATE SENATE ########################

# read in croswalk
#sen_crosswalk <- dbGetQuery(con_shared, "select geo_id AS puma, sldu24 AS geoid, afact from crosswalks.puma_2020_state_senate_2024")



# Screen data ----------------------------------------------------------
# No screening needed as these estimates are pre-screened


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
#source("./Functions/RC_Functions.R")
source("W:/Project/RACE COUNTS/2025_v7/RC_Github/CR/Functions/RC_Functions.R")

d <- rbind(d, d_assm)

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity

#split STATE into separate table and format id, name columns
state_table <- d %>%
  filter(geolevel=="state")

#calculate STATE z-scores
state_table <- calc_state_z(state_table)

state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table <- d %>%
  filter(geolevel=="county")

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

#split LEG DIST into separate tables
# upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d %>%
  filter(geolevel=="sldl")
# 
# #calculate SLDU z-scores and ranks
# upper_table <- calc_z(upper_table)
# 
# upper_table <- calc_ranks(upper_table)
# #View(upper_table)
# 
#calculate SLDL z-scores and ranks
lower_table <- calc_z(lower_table)
lower_table <- calc_ranks(lower_table)
View(lower_table)
# 
# ## Bind sldu and sldl tables into one leg_table##
# leg_table <- rbind(upper_table, lower_table)
# View(leg_table)

#state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
#county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
#leg_table <- leg_table %>% dplyr::rename("leg_name" = "geoname", "leg_id" = "geoid")



###update info for postgres tables will update automatically###
county_table_name <- paste0("arei_econ_real_cost_measure_county_", rc_yr)
state_table_name <- paste0("arei_econ_real_cost_measure_state_", rc_yr)
#leg_table_name <- paste0("arei_econ_real_cost_measure_leg_", rc_yr)


indicator <- paste0("Created on ", Sys.Date(), ". Households above the real cost of living (data from ", data_yr, "). This data is")
source <- paste0("United Ways of California ", curr_yr, " ", dwnld_url, ". QA doc: ", qa_filepath)

#to_postgres(county_table,state_table)
#leg_to_postgres()


dbDisconnect(con_rc)
dbDisconnect(con_shared)

