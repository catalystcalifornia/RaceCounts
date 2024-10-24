### Cost-of-Living-Adjusted Poverty v6 ### 

#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse","RPostgreSQL", "tidycensus", "readxl", "sf", "janitor")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

## packages
library(DBI)
library(tidyverse)
library(RPostgreSQL)
library(tidycensus)
library(readxl)
library(sf)
library(janitor)

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2023"  # must keep same format
data_yr <- "2021"
rc_yr <- "2024"
dwnld_url <- "https://unitedwaysca.org/download-the-public-data-set/"
rc_schema <- "v6"

# Read Data ---------------------------------------------------------------
rcm <- dbGetQuery(con2, "SELECT * FROM economic.uw_2023_county_state_real_cost_measure")

# select only fields we want
# asian = api for UW, so after initial grab I start renaming asian to api
rcm_subset <- rcm %>% select(geoid, county,  
                             num_hh_below_rcm, pct_hh_below_rcm, 
                             num_nh_white_hh_below_rcm, pct_nh_white_hh_below_rcm,
                             num_nh_black_hh_below_rcm, pct_nh_black_hh_below_rcm,
                             num_nh_api_hh_below_rcm, pct_nh_api_hh_below_rcm,
                             num_latino_hh_below_rcm, pct_latino_hh_below_rcm,
                             num_nh_aian_hh_below_rcm, pct_nh_aian_hh_below_rcm,
                             num_nh_other_hh_below_rcm, pct_nh_other_hh_below_rcm,
                             num_hh_california) %>%
 
   rename(geoname = county, total_pop = num_hh_california)

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

# select just fields we want
d <- rcm_subset %>% select(geoid, geoname, ends_with("_pop"), ends_with("_raw"), ends_with("_rate")) %>%
  
  #filter out regions
  filter(!geoid %in% c("06117","06119","06121","06123","06125","06127","06129"))



# Screen data ----------------------------------------------------------
# No screening needed as these estimates are pre-screened


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

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
View(county_table)

###update info for postgres tables will update automatically###
county_table_name <- paste0("arei_econ_real_cost_measure_county_", rc_yr)
state_table_name <- paste0("arei_econ_real_cost_measure_state_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Households above the real cost of living (data from ", data_yr, "). This data is")
source <- paste0("United Ways of California ", curr_yr, " ", dwnld_url)

#to_postgres(county_table,state_table)

dbDisconnect(con)
dbDisconnect(con2)

