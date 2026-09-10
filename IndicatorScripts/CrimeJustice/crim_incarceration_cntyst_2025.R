## Incarceration (County and State-Level) for RC v7

## Set up ----------------------------------------------------------------
#install packages if not already installed
packages <- c("tidyr", "dplyr", "janitor", "tidycensus", "tidyverse", "usethis", "readxl", "RPostgres")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen = 999) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con_rc <- connect_to_db("racecounts")
con_shared <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2020-2024"  # must keep same format
dwnld_url <- "https://github.com/vera-institute/incarceration-trends"
rc_schema <- "v7"
rc_yr <- "2025"
data_yrs <- c('2020', '2021', '2022', '2023', '2024')
# min_q <- 3   # Keep data where county/yr combo has 3Q+ of data.
min_q <- 0   # Keep all data

qa_filepath <- "W://Project//RACE COUNTS//2025_v7//Crime and Justice//QA_Sheet_Incarceration_CountySt.docx"

############### PREP DATA ########################

county_data <- read_excel("W:/Data/Crime and Justice/vera_institute/2024/incarceration_trends_county.xlsx")
county_data$year <- as.character(county_data$year)
county_data$quarter <- as.character(county_data$quarter)
# check b/c seems like there are way too many rows
county_data <- county_data %>%
  select(fips, county_name, state_abbr, year, quarter,
         total_pop_15to64, aapi_pop_15to64, black_pop_15to64, latinx_pop_15to64, native_pop_15to64, white_pop_15to64, 
         total_jail_pop, aapi_jail_pop, black_jail_pop, latinx_jail_pop, native_jail_pop, white_jail_pop) %>%
  filter(state_abbr=='CA') %>%
  filter(year %in% data_yrs)
# look at the raw source before any select() drops columns
View(county_data)

county_data <- county_data %>%
  filter(year != '2024')
county_data %>% names()

county_data %>%
  count(fips, year) %>%
  filter(n > 1)  # n=224

incomplete_data <- county_data %>% filter(year %in% c('2020', '2021', '2022', '2023', '2024')) %>%
  group_by(fips, county_name, year) %>%
  summarise(n = n()) %>%
  filter(n < min_q)  # n = 66 (county/yr combos wo 4Q of data, eg: 06003 has only Q2 data in 2020-23)
View(incomplete_data)

to_drop <- incomplete_data %>%
  filter(n < min_q) %>%
  left_join(county_data, by = c("fips", "county_name", "year"))  # these are the rows that will be dropped
View(to_drop)
# Drop 62 rows: 2024 for all counties, drop all data for Alpine and Sierra

# screen out incomplete data years using min_q filter defined above
county_complete <- county_data %>%
  anti_join(incomplete_data %>% filter(n < min_q) %>% select(-n), by = c("fips", "year"))
## dropped 62 rows

# check cleaned data 
View(county_complete %>% group_by(fips, county_name) %>% summarise(num_qtr = n(), num_year = n_distinct(year)))


# rename columns
df <- county_complete %>% 
  rename(geoid = fips, geoname = county_name)

# COUNTY PREP #####
#rename columns and clean data. be sure to assign correct race/eth labels (non-Latinx or not etc.)
names(df) <- gsub("_15to64", "", names(df))
names(df) <- gsub("jail_pop", "raw", names(df))
names(df) <- gsub("aapi", "nh_api", names(df))
names(df) <- gsub("native", "nh_aian", names(df))
names(df) <- gsub("black", "nh_black", names(df))
names(df) <- gsub("white", "nh_white", names(df))
names(df) <- gsub("latinx", "latino", names(df))
df$geoname <- gsub(" County", "", df$geoname)

# check for cols that are NA
# df %>% dplyr::summarise(across(contains("pop"), ~ sum(is.na(.))))
# df %>% dplyr::summarise(across(contains("raw"), ~ sum(is.na(.))))

## aggregate at the year level first 
df_annual <- df %>%
  group_by(geoid, geoname, year) %>%
  summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)), .groups = "drop")

#check non-NA dupes before running it through the function then again after. right now reach has like 4 to 5 dupes
View(df_annual %>%
  group_by(geoid, geoname) %>%
  summarise(across(where(is.numeric), ~ sum(!is.na(.x)))) %>%
  filter(nh_aian_raw != nh_aian_pop | nh_black_raw != nh_black_pop |
           nh_white_raw != nh_white_pop | nh_api_raw != nh_api_pop |
           latino_raw != latino_pop))

# Make raw values NA when pop is NA and vice versa, based on sync_voted_vap_na{} from ./Functions/democracy_functions.R
sync_na <- function(df, race_groups) {
  for (r in race_groups) { # for each group in the race_groups list loop through this process
    # safety check that the columns exist
    raw_col <- paste0(r, "_raw")
    pop_col   <- paste0(r, "_pop")
    
    if (raw_col %in% names(df) && pop_col %in% names(df)) {
      na_mask <- is.na(df[[raw_col]]) | # find the row that needs to be fixed
        is.na(df[[pop_col]])  # and creates a TRUE/FALSE flag for every row. Its TRUE if either raw or pop is NA
      # force both columns to match each other so if na_mask is TRUE then it makes both race_raw and race_pop NA
      df[[raw_col]][na_mask] <- NA 
      df[[pop_col]][na_mask] <- NA
    }
  }
  df # return the fixed df
}

# variables for the new sync_na function
race_groups <- c("total", "latino", "nh_white", "nh_black", "nh_aian", "nh_api")
df_ <- sync_na(df_annual, race_groups = race_groups)

# look for non-NA dupe rows. should be zero now
df_ %>%
  group_by(geoid, geoname) %>%
  summarise(across(where(is.numeric), ~ sum(!is.na(.x))), .groups = "drop") %>%
  filter(nh_aian_raw != nh_aian_pop | nh_black_raw != nh_black_pop |
           nh_white_raw != nh_white_pop | nh_api_raw != nh_api_pop |
           latino_raw != latino_pop)

# check fx worked
# dfpop <- df_annual %>% filter(geoid == '06013') %>% group_by(geoid) %>% dplyr::summarize(latino_pop = sum(latino_pop, na.rm=TRUE))
# df_pop <- df_ %>% filter(geoid == '06013') %>% group_by(geoid) %>% dplyr::summarize(latino_pop = sum(latino_pop, na.rm=TRUE))
# 
# dfyrs <- df_annual %>% filter(geoid == '06013') %>% group_by(geoid) %>% dplyr::summarize(count = sum(!is.na(latino_raw)))
# df_yrs <- df_ %>% filter(geoid == '06013') %>% group_by(geoid) %>% dplyr::summarize(count = sum(!is.na(latino_raw)))
# 
# dfpop$latino_pop / dfyrs$count    # wo function, should be more
# df_pop$latino_pop / df_yrs$count  # w function, should be less bc some pop value(s) were suppressed
# 
# df_summary <- df_ %>%
#   group_by(geoid, geoname) %>%
#   dplyr::summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)))

## QA Check ##
# qa_check <- df_annual %>% filter(geoid == '06013') %>%
#   select(geoid, geoname, year, starts_with("latino")) %>%
#   mutate(pop_ = ifelse(is.na(latino_raw), NA, latino_pop),
#          raw_ = ifelse(is.na(latino_pop), NA, latino_raw))
# qa_check1 <- df_ %>% filter(geoid == '06013') %>%
#       select(geoid, geoname, year, starts_with("latino")) %>%
#       mutate(pop_ = ifelse(is.na(latino_raw), NA, latino_pop),
#              raw_ = ifelse(is.na(latino_pop), NA, latino_raw))
# 
# # # these two should be the same
# qa_check %>% group_by(geoid, geoname) %>%
#   summarise(avg_pop_ = mean(pop_, na.rm=TRUE),
#             avg_raw = mean(latino_raw, na.rm=TRUE))
# df_summary %>% filter(geoid == '06013') %>% select(geoid, geoname, starts_with("latino"))


# STATE PREP #### 
# Keep old method (aggregate from county, instead of using updated state-level data so we're using consistent data across geos.)
# state_data <- read_excel("W:/Data/Crime and Justice/vera_institute/2024/incarceration_trends_state.xlsx") #there is no latinx_jail_pop field for some reason
# state_data <- read_csv("W:/Data/Crime and Justice/vera_institute/2025/incarceration_trends_state_20260908.csv")
# state_data %>% names()
# 
# state_data$year <- as.character(state_data$year)
# 
# state_data <- state_data %>%
#   select(state_fips, state_name, state_abbr, year, #quarter,
#          total_pop_15to64, aapi_pop_15to64, black_pop_15to64, latinx_pop_15to64, native_pop_15to64, white_pop_15to64,
#          total_jail_pop, aapi_jail_pop, black_jail_pop, latinx_jail_pop, native_jail_pop, white_jail_pop) %>%
#   filter(state_fips=='06') %>%
#   filter(year %in% data_yrs)
# # look at the raw source before any select() drops columns
# View(state_data)
# 
# Calc state-level avg stats
# state_data_avg <- state_data %>%
#   filter(year != '2024') %>% # drop 2024 bc missing raced data
#   group_by(state_fips, state_name) %>%
#   dplyr::summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)))


df_summary <- df_summary %>% adorn_totals(name = "06", fill = "California")
# View(df_summary)

# add geolevel, remove NaNs, and order by geoid
d <- df_summary %>% mutate(geolevel = ifelse(geoid == '06', 'state', 'county')) %>%
  relocate(geolevel, .after = geoname) %>%  mutate(across(where(is.numeric), ~ as.numeric(gsub("NaN", NA, .x)))) %>%
  arrange(geoid)


############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

#YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max' as 'best'
d$asbest = 'min'    

d <- calc_rates_100k(d) #calc rates
d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- dplyr::rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- dplyr::rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)


###update info for postgres tables###
county_table_name <- paste0("arei_crim_incarceration_county_", rc_yr, "_v4")
state_table_name <- paste0("arei_crim_incarceration_state_", rc_yr, "_v4")
indicator <- "Jail population per 100,000 15 to 64 year olds"
source <- paste0("Vera Institute (", curr_yr, ")", ". QA doc: ", qa_filepath)

#send tables to postgres
# to_postgres(county_table, state_table)

dbDisconnect(con_rc)
dbDisconnect(con_shared)


# check results using new FX against old table
# state_old <- dbGetQuery(con_rc, "SELECT * FROM v7.arei_crim_incarceration_state_2025")
# county_old <- dbGetQuery(con_rc, "SELECT * FROM v7.arei_crim_incarceration_county_2025")
# 
# install.packages("arsenal")
# library(arsenal)
# comparison_s <- comparedf(state_table, state_old)
# summary(comparison_s)
# 
# disprk_report <- inner_join(county_table, county_old, by = c("county_id","county_name"), suffix = c("_new", "_old")) %>%
#   filter(disparity_rank_new != disparity_rank_old) %>%
#   select(county_id, county_name, disparity_rank_new, disparity_rank_old)
# disprk_report  # 12 counties moved ranks, all were +/- 1 or 2 except San Bernardino which moved up 4 (28 to 24).
# 
# perfrk_report <- inner_join(county_table, county_old, by = c("county_id","county_name"), suffix = c("_new", "_old")) %>%
#   filter(performance_rank_new != performance_rank_old) %>%
#   select(county_id, county_name, performance_rank_new, performance_rank_old)
# perfrk_report  # 21 counties moved ranks, all were +/- 1 or 2 except Mariposa which moved up 3 and Tehama which moved down 3 ranks.