### Status Offenses RC v6 ### 

#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse","RPostgreSQL", "tidycensus", "readxl", "sf", "janitor")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

## packages
library(tidyverse)
library(readxl)
library(RPostgreSQL)
library(sf)
library(tidycensus)
library(DBI)
library(janitor)

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2010-2022"  # must keep same format
yrs_list <- c("2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022")
rc_yr <- "2024"
dwnld_url <- "https://openjustice.doj.ca.gov/data"
rc_schema <- "v6"

# Read Data ---------------------------------------------------------------
# Metadata: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/Arrests%20Context_081122.pdf
df_disposition <- read_csv("W:/Data/Crime and Justice/CA DOJ/Arrests/OnlineArrestDispoData1980-2022.csv") %>% filter(YEAR %in% yrs_list)

# make cols lower
colnames(df_disposition) <- tolower(colnames(df_disposition))


# Calculate Total Status Offenses by race/group and total -----------------
df <- df_disposition %>% group_by(county) %>%
    summarize(s_total = sum(s_total)) %>% mutate(race = 'total')

races <- df_disposition %>% group_by(county, race) %>%
            summarize(s_total = sum(s_total)) %>% filter(race != 'Other')

df <- df %>% rbind(races) 
df$race <- gsub('Black', 'nh_black', df$race)
df$race <- gsub('Hispanic', 'latino', df$race)
df$race <- gsub('White', 'nh_white', df$race)

df_wide <- df %>% pivot_wider(names_from = race, values_from = s_total)

# calculate total for state and clean up table
df_wide <- df_wide %>% adorn_totals("row") %>% as.data.frame(df_wide) # add state totals row
names(df_wide)[-(1)] <- paste0(names(df_wide)[-(1)], "_sum_arrests") # add suffix to 10y sums
df_wide$county[df_wide$county == 'Total'] <- 'California'
df_wide$county <-  gsub(" County", "", df_wide$county)


# Population data by race and age ---------------------------------------------------
### Note: Black pop is Latinx-inclusive while Black Status Offense data is Latinx-exclusive
pop <- st_read(con2, query = "SELECT * FROM demographics.acs_5yr_b01001_multigeo_2022") %>% filter(geolevel %in% c("state", "county"))
pop$total_und_18_pop <- pop$b01001_003e + pop$b01001_004e + pop$b01001_005e + pop$b01001_006e + pop$b01001_027e + pop$b01001_028e + pop$b01001_029e + pop$b01001_030e
pop$black_und_18_pop <- pop$b01001b_003e + pop$b01001b_004e + pop$b01001b_005e + pop$b01001b_006e + pop$b01001b_018e + pop$b01001b_019e + pop$b01001b_020e + pop$b01001b_021e
pop$nh_white_und_18_pop <- pop$b01001h_003e + pop$b01001h_004e + pop$b01001h_005e + pop$b01001h_006e + pop$b01001h_018e + pop$b01001h_019e + pop$b01001h_020e + pop$b01001h_021e
pop$latino_und_18_pop <- pop$b01001i_003e + pop$b01001i_004e + pop$b01001i_005e + pop$b01001i_006e + pop$b01001i_018e + pop$b01001i_019e + pop$b01001i_020e + pop$b01001i_021e

pop_df <- pop %>% select(geoid, name, geolevel, ends_with("_und_18_pop"))

# update pop_df geonames
pop_df$name <- gsub(" County, California", "", pop_df$name)


# Merge pop data with status offenses data ----------------------------------------------------------
df_pop <- left_join(df_wide, pop_df, by = c("county" = "name")) %>% arrange(county) %>% select(county, geoid, everything())


# Screen data ----------------------------------------------------------
pop_threshold <- 100
raw_threshold <- 30
num_yrs <- length(unique(yrs_list))

df_screened <- df_pop %>%
  mutate(
    # calculate raw
    total_raw = total_sum_arrests/num_yrs,
    nh_black_raw =  nh_black_sum_arrests/num_yrs,
    nh_white_raw = nh_white_sum_arrests/num_yrs,
    latino_raw = latino_sum_arrests/num_yrs,
    
    # screening by total number of arrests and pop
    total_rate =    ifelse(total_sum_arrests < raw_threshold & total_und_18_pop < pop_threshold, NA, ifelse(total_sum_arrests < raw_threshold, NA, total_raw/total_und_18_pop * 10000)),
    nh_black_rate = ifelse(nh_black_sum_arrests < raw_threshold & black_und_18_pop < pop_threshold, NA, ifelse(nh_black_sum_arrests < raw_threshold, NA, nh_black_raw/black_und_18_pop * 10000)),
    nh_white_rate = ifelse(nh_white_sum_arrests < raw_threshold & nh_white_und_18_pop < pop_threshold, NA, ifelse(nh_white_sum_arrests < raw_threshold, NA, nh_white_raw /nh_white_und_18_pop * 10000)),
    latino_rate = ifelse(latino_sum_arrests < raw_threshold & latino_und_18_pop < pop_threshold, NA, ifelse(latino_sum_arrests < raw_threshold, NA, latino_raw/latino_und_18_pop * 10000))
  )

df_screened <- df_screened %>% rename(geoname = county)

# make d 
d <- df_screened

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
county_table_name <- paste0("arei_crim_status_offenses_county_", rc_yr)
state_table_name <- paste0("arei_crim_status_offenses_state_", rc_yr)

indicator <- paste0("Annual average number of arrests for status offenses over ", num_yrs, " years. Raw is also ", num_yrs, "-yr annual average. This data is")
source <- paste0("CADOJ ", curr_yr, " ", dwnld_url)

#to_postgres(county_table,state_table)

dbDisconnect(con)
dbDisconnect(con2)
