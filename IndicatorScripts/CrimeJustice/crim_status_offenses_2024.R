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
curr_yr <- "2010_22"  # must keep same format
dwnld_url <- "https://openjustice.doj.ca.gov/data"
rc_schema <- "v6"
yr <- "2024"

# Read Data ---------------------------------------------------------------
# Metadata: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/Arrests%20Context_081122.pdf
df_disposition <- read_csv("W:/Data/Crime and Justice/CA DOJ/Arrests/OnlineArrestDispoData1980-2022.csv") %>% filter(YEAR %in% c("2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022"))

# make cols lower
colnames(df_disposition) <- tolower(colnames(df_disposition))


# We don't need to filter age group to equal under 18 because all of the observations for s_total = sum of all arrests for status offenses which only apply to those under 18.
#df_disposition %>% group_by(age_group) %>% summarize(
#  sum = sum(s_total))

# Calculate Total Status Offenses by race/group and total -----------------
df <- df_disposition %>% group_by(county) %>%
  # total
  summarize(total_s = sum(s_total)) %>%
  
  ## merge with nh_black
  left_join(
    df_disposition %>% filter(
      race == "Black"
    ) %>% group_by(county) %>%
      # 
      summarize(nh_black_s = sum(s_total))
    
  ) %>% 
  
  ## merge with nh_white
  left_join(
    
    df_disposition %>% filter(
      race == "White"
    ) %>% group_by(county) %>%
      # 
      summarize(nh_white_s = sum(s_total))
    
  ) %>%
  
  left_join(
    
    # merge with latino
    df_disposition %>% filter(
      race == "Hispanic"
    ) %>% group_by(county) %>%
      # 
      summarize(latino_s = sum(s_total))
    
  )

# calculate total for state
df <- df %>% adorn_totals("row") %>% as.data.frame(df)


# Total Population data by age ---------------------------------------------------
totalpop <- st_read(con2, query = "SELECT * FROM demographics.acs_5yr_b01001_multigeo_2022") %>% filter(geolevel %in% c("state", "county"))

totalpop <- totalpop %>% select(geoid, name, b01001_003e, b01001_004e, b01001_005e, b01001_006e, b01001_027e, b01001_028e, b01001_029e, b01001_030e)

totalpop$total_und_18_pop <- totalpop$b01001_003e + totalpop$b01001_004e + totalpop$b01001_005e + totalpop$b01001_006e + totalpop$b01001_027e + totalpop$b01001_028e + totalpop$b01001_029e + totalpop$b01001_030e

totalpop <- totalpop %>% select(geoid, name, total_und_18_pop)

# pop under 18 by race/ethnicity
pop2 <- st_read(con2, query = "SELECT * FROM demographics.acs_5yr_b01001ai_multigeo_2022") %>% filter(geolevel %in% c("state", "county"))


# African American alone by age: sub-table B -------------------------------------- Note: There is no age by race for nh_black, so these are Latinx inclusive while arrest data is nh_black
black_pop <- pop2 %>% select(geoid, name, b01001b_003e, b01001b_004e, b01001b_005e, b01001b_006e, b01001b_018e, b01001b_019e, b01001b_020e, b01001b_021e)

black_pop$black_und_18_pop <- black_pop$b01001b_003e+ black_pop$b01001b_004e+ black_pop$b01001b_005e+ black_pop$b01001b_006e+ black_pop$b01001b_018e+ black_pop$b01001b_019e+ black_pop$b01001b_020e+ black_pop$b01001b_021e

black_pop <- black_pop %>% select(geoid, name, black_und_18_pop)


# White alone, not Hispanic or Latino by age: sub-table H --------------------------------------
white_pop <- pop2 %>% select(geoid, name, b01001h_003e, b01001h_004e, b01001h_005e, b01001h_006e, b01001h_018e, b01001h_019e, b01001h_020e, b01001h_021e)

white_pop$nh_white_und_18_pop <- white_pop$b01001h_003e+ white_pop$b01001h_004e+ white_pop$b01001h_005e+ white_pop$b01001h_006e+ white_pop$b01001h_018e+ white_pop$b01001h_019e+ white_pop$b01001h_020e+ white_pop$b01001h_021e

white_pop <- white_pop %>% select(geoid, name, nh_white_und_18_pop)



# Latino: table I -------------------------------------------------------
latino_pop <- pop2 %>% select(geoid, name, b01001i_003e, b01001i_004e, b01001i_005e, b01001i_006e, b01001i_018e, b01001i_019e, b01001i_020e, b01001i_021e)

latino_pop$latino_und_18_pop <- latino_pop$b01001i_003e+ latino_pop$b01001i_004e+ latino_pop$b01001i_005e+ latino_pop$b01001i_006e+ latino_pop$b01001i_018e+ latino_pop$b01001i_019e+ latino_pop$b01001i_020e+ latino_pop$b01001i_021e

latino_pop <- latino_pop %>% select(geoid, name, latino_und_18_pop)


# merge together total, black, white, and latino under 18 pop
pop_df <- left_join(totalpop, black_pop)
pop_df <- left_join(pop_df, white_pop)
pop_df <- left_join(pop_df, latino_pop)

# update pop_df geonames
pop_df$name <- gsub(" County, California", "", pop_df$name)


# merge with status offenses df ----------------------------------------------------------

# update df geonames
df$county[df$county == 'Total'] <- 'California'
df$county <-  gsub(" County", "", df$county)


df_pop <- left_join(df, pop_df, by = c("county" = "name")) %>% arrange(county) %>% select(county, geoid, everything())


# screening ----------------------------------------------------------
df_screened <- df_pop %>%
  mutate(
    # calculate raw
    total_raw = total_s/10,
    nh_black_raw =  nh_black_s/10,
    nh_white_raw = nh_white_s/10,
    latino_raw = latino_s/10,
    
    # screening by total number of arrests and pop
    total_rate = ifelse(total_s < 30 | total_und_18_pop < 100 , NA, total_raw/total_und_18_pop * 10000),
    nh_black_rate = ifelse(nh_black_s < 30 | black_und_18_pop < 100, NA,  nh_black_raw/black_und_18_pop * 10000),
    nh_white_rate = ifelse(nh_white_s < 30 | nh_white_und_18_pop < 100, NA, nh_white_raw /nh_white_und_18_pop * 10000),
    latino_rate = ifelse(latino_s < 30 | latino_und_18_pop < 100, NA, latino_raw/latino_und_18_pop * 10000),
    
  )

df_screened <- df_screened %>% rename(geoname = county)

# replace _s with _sum_arrests
colnames(df_screened) <- gsub("_s", "_sum_arrests", colnames(df_screened))


# make d 
d <- df_screened


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

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

###update info for postgres tables###
county_table_name <- "arei_crim_status_offenses_county_2024"
state_table_name <- "arei_crim_status_offenses_state_2024"

indicator <- "Annual average number of arrests for status offenses over 12 years. Raw is also 12y annual average. This data is"

source <- "CADOJ 2010-2022 https://openjustice.doj.ca.gov/data"

to_postgres(county_table,state_table)

dbDisconnect(con)
dbDisconnect(con2)
