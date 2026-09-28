### Status Offenses RC v7 ### 

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


source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")

# define variables used in several places: Update each year
curr_yr <- "2010-2023"  # must keep same format, CA DOJ year
acs_yr <- "2023"
yrs_list <- c("2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023")
rc_yr <- "2025"
dwnld_url <- "https://openjustice.doj.ca.gov/data"
rc_schema <- "v7"

pop_threshold <- 100  # data is screened where pop is < threshold
raw_threshold <- 30   # data is screened where raw count is < threshold

# Read Data: Update each year ---------------------------------------------------------------
# Metadata: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-07/arrests-context-06062024.pdf
df_disposition <- read_csv("W:/Data/Crime and Justice/CA DOJ/Arrests/OnlineArrestDispoData1980-2023.csv") %>% filter(YEAR %in% yrs_list)

# make cols lower
colnames(df_disposition) <- tolower(colnames(df_disposition))

# get # of data yrs by county+race combo
data_yrs <- df_disposition %>%
  select(county, race, year) %>%
  unique() %>%
  group_by(county, race) %>%
  summarise(num_yrs = n())

totaldata_yrs <- df_disposition %>%
  select(county, year) %>%
  unique() %>%
  group_by(county) %>%
  summarise(num_yrs = n()) %>%
  mutate(race = 'total_yrs')

data_yrs <- rbind(data_yrs, totaldata_yrs) %>%
  filter(race != 'Other')

data_yrs$race <- gsub('Black', 'nh_black_yrs', data_yrs$race)
data_yrs$race <- gsub('Hispanic', 'latino_yrs', data_yrs$race)
data_yrs$race <- gsub('White', 'nh_white_yrs', data_yrs$race)

ca_max <- data_yrs %>%  # add CA rows
  group_by(race) %>%
  summarise(num_yrs = max(num_yrs, na.rm = TRUE), .groups = "drop") %>%
  mutate(county = "California") %>%
  select(county, race, num_yrs)

data_yrs <- bind_rows(data_yrs, ca_max)
data_yrs <- data_yrs %>% pivot_wider(names_from = race, values_from = num_yrs)


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
names(df_wide)[-(1)] <- paste0(names(df_wide)[-(1)], "_sum_arrests")  # add suffix to multi-year sums
df_wide$county[df_wide$county == 'Total'] <- 'California'


# Population data by race and age ---------------------------------------------------
### Note: Black pop is Latinx-inclusive while Black Status Offense data is Latinx-exclusive
pop <- dbGetQuery(con2, paste0("SELECT * FROM demographics.acs_5yr_b01001_multigeo_", acs_yr)) %>% filter(geolevel %in% c("state", "county"))
pop$total_und_18_pop <- pop$b01001_003e + pop$b01001_004e + pop$b01001_005e + pop$b01001_006e + pop$b01001_027e + pop$b01001_028e + pop$b01001_029e + pop$b01001_030e
pop$black_und_18_pop <- pop$b01001b_003e + pop$b01001b_004e + pop$b01001b_005e + pop$b01001b_006e + pop$b01001b_018e + pop$b01001b_019e + pop$b01001b_020e + pop$b01001b_021e
pop$nh_white_und_18_pop <- pop$b01001h_003e + pop$b01001h_004e + pop$b01001h_005e + pop$b01001h_006e + pop$b01001h_018e + pop$b01001h_019e + pop$b01001h_020e + pop$b01001h_021e
pop$latino_und_18_pop <- pop$b01001i_003e + pop$b01001i_004e + pop$b01001i_005e + pop$b01001i_006e + pop$b01001i_018e + pop$b01001i_019e + pop$b01001i_020e + pop$b01001i_021e

pop_df <- pop %>% select(geoid, name, geolevel, ends_with("_und_18_pop"))

# update pop_df geonames
pop_df$name <- gsub(", California", "", pop_df$name)


# Merge pop data with status offenses data ----------------------------------------------------------
df_pop <- left_join(df_wide, pop_df, by = c("county" = "name")) %>% arrange(county) %>% select(county, geoid, everything())

# Merge data with data_yrs
df_pop <- left_join(df_pop, data_yrs, by = "county")

# Screen data ----------------------------------------------------------
df_screened <- df_pop %>%
  mutate(
    # calculate annual raw by dividing by data_yrs specific to geo+race combo
    total_raw = total_sum_arrests/total_yrs,
    nh_black_raw =  nh_black_sum_arrests/nh_black_yrs,
    nh_white_raw = nh_white_sum_arrests/nh_white_yrs,
    latino_raw = latino_sum_arrests/latino_yrs,
    
    # screening by total number of arrests and pop
    total_rate =    ifelse(total_sum_arrests < raw_threshold & total_und_18_pop < pop_threshold, NA, ifelse(total_sum_arrests < raw_threshold, NA, total_raw/total_und_18_pop * 10000)),
    nh_black_rate = ifelse(nh_black_sum_arrests < raw_threshold & black_und_18_pop < pop_threshold, NA, ifelse(nh_black_sum_arrests < raw_threshold, NA, nh_black_raw/black_und_18_pop * 10000)),
    nh_white_rate = ifelse(nh_white_sum_arrests < raw_threshold & nh_white_und_18_pop < pop_threshold, NA, ifelse(nh_white_sum_arrests < raw_threshold, NA, nh_white_raw /nh_white_und_18_pop * 10000)),
    latino_rate = ifelse(latino_sum_arrests < raw_threshold & latino_und_18_pop < pop_threshold, NA, ifelse(latino_sum_arrests < raw_threshold, NA, latino_raw/latino_und_18_pop * 10000))
  )

# Make raw NA when rate is NA
race_prefixes <- names(df_screened) %>%
  str_subset("_raw$") %>%
  str_remove("_raw$")

df_final <- df_screened %>%
  mutate(across(
    all_of(paste0(race_prefixes, "_raw")),
    ~ ifelse(is.na(get(cur_column() %>% str_replace("_raw$", "_rate"))), NA, .x)
  ))


df_final$county <- gsub(" County", "", df_final$county)
df_final <- df_final %>%
  rename(geoname = county) %>%
  select(-ends_with("_yrs"))

# make d 
d <- df_final

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("./Functions/RC_Functions.R")

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

indicator <- paste0("Created on ", Sys.Date(), ". Annual average number of arrests for status offenses between ", yrs_list[1], "-", yrs_list[length(yrs_list)], ". Raw is also ", length(yrs_list), "-yr annual average. This data is")
source <- paste0("CADOJ ", curr_yr, " and ACS ", acs_yr, " 5y Table B01001 data. ", dwnld_url)

#to_postgres(county_table,state_table)

dbDisconnect(con)
dbDisconnect(con2)
