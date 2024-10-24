### Diversity of Candidates RC v5 ###

#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse","RPostgreSQL", "tidycensus", "readxl", "sf", "tigris")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)


## packages
library(tidyverse)
library(readxl)
library(RPostgreSQL)
library(sf)
library(tidycensus)
library(DBI)
library(sf)
library(tigris)

# connect to database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("racecounts")

# Clean data Function --------------------------------------------------------------
clean_data <- function(x) {
  
  #make column names lowercase
  colnames(x) <- tolower(colnames(x))
  
  # remove hyphen from African-American
  x$race <- gsub("-", " ", x$race)
  
  # make under-score
  names(x) <- gsub(" ", "_", names(x))
  
  # get race categories
  x <- x %>% mutate(
    race_catg= case_when(
      
      race == "White" ~ "nh_white_candidates",
      race == "Black or African American" ~ "nh_black_candidates",
      race == "Hispanic or Latino" ~ "latino_candidates",
      race == "Asian American or Pacific Islander" ~ "nh_api_candidates",
      race == "American Indian or Alaska Native" ~ "nh_aian_candidates",
      race == "Multiracial" ~ "nh_twoormor_candidates"
      
    ))
  
  # filter for CA
  x <- x %>% filter(state == "CA")

return(x)  
}


# Get Data ----------------------------------------------------------------

## 2014 data
df_2014 <-  read_csv("W:/Data/Democracy/WDN_WhoLeadsUs/Diversity of Candidates/wdn_2014_wholeadsus_candidates_race.csv", col_types = cols(.default = "c")) %>% select(-c("Candidate Party", "RaceGender Category", "WhiteNonWhite"))
df_2014 <- clean_data(df_2014)

## 2016 data
df_2016 <-  read_csv("W:/Data/Democracy/WDN_WhoLeadsUs/Diversity of Candidates/wdn_2016_wholeadsus_candidates_race.csv") %>% select(-c("Candidate Party", "Race/Gender Category", "White/Non-White"))
df_2016 <- clean_data(df_2016) 
df_2016$year <- '2016' # add year

## 2018 data
df_2018 <-  read_csv("W:/Data/Democracy/WDN_WhoLeadsUs/Diversity of Candidates/wdn_2018_wholeadsus_candidates_race.csv") %>% select(-c("Candidate Party", "Race/Gender Category", "White/Non-White"))
df_2018 <- clean_data(df_2018)
df_2018$year <- '2018' # add year

## 2020 data
df_2020 <- read_csv("W:/Data/Democracy/WDN_WhoLeadsUs/Diversity of Candidates/rdc_system_failure_2020_primary_report_data_analysis.csv") %>% select("State", "Candidate Name", "Electoral District", "Office Name", "Race", "Gender", "Office Level", "Race")
df_2020 <- clean_data(df_2020) 
df_2020$year = '2020'

############## COUNTY & STATE CALCS ############## 
### County Crosswalks --------------------------------------------------------------
# load crosswalks for 2014, 2016, 2018 data
crosswalks_2012_2018 <- st_read(con2, query = "SELECT * FROM data.wdn_2017_wholeadsus_candidates_crosswalk")
crosswalks_2014 <- crosswalks_2012_2018 %>% filter(year == "2014")
crosswalks_2016 <- crosswalks_2012_2018 %>% filter(year == "2016")
crosswalks_2018 <- crosswalks_2012_2018 %>% filter(year == "2018")

# Join crosswalks to data -------------------------------------------------
# join 2014 crosswalk with 2014 data
df_2014_crosswalk <- left_join(df_2014, crosswalks_2014, by = c("office_name", "year")) %>% select(-id)

# join 2016 crosswalk with 2016 data
df_2016_crosswalk <- left_join(df_2016, crosswalks_2016, by = c("office_name", "year")) %>% select(-id)

# join 2018 crosswalk with 2018 data
df_2018_crosswalk <- left_join(df_2018, crosswalks_2018, by = c("office_name", "year")) %>% select(-id)

# get 2020 geographies, then create crosswalks-------------------------------------------------------------------------
# first, start with counties
  counties <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_county_500k")
  counties <- st_transform(counties, 3310) # reproject to 3310

# ## create df specifically for counties. We will keep the counties already in the "jurisdiction" column and just create a "geoid" column
#   df_2020_county <- df_2020 %>% filter(grepl('County', jurisdiction))
#   counties2 <- as.data.frame(counties) %>% select(namelsad, county_geoid)
# 
# # remove extra county string for LA county observation
#   df_2020_county$jurisdiction <- str_replace_all(df_2020_county$jurisdiction, "Los Angeles County County", "Los Angeles County")
# 
# # final df for counties. we will combine these later
#   df_2020_county <- left_join(df_2020_county, counties2, by = c("jurisdiction" = "namelsad"))


make_xwalk <- function(x,y,z) {
              y$area <- st_area(y) # calculate area of user-defined district
              xy_intersection <- st_intersection(y, x)    # run intersect
              xy_intersection$intersect_area <- st_area(xy_intersection)   #  calculate area of intersection
              # calculate percent of intersect out of total congressional district area
              xy_intersection$prc_area <- as.numeric(xy_intersection$intersect_area/xy_intersection$area)
              xy_intersection <- as.data.frame(xy_intersection) %>% select(namelsad, area, county_geoid, name, intersect_area, prc_area)  # convert to df
              xy_intersection$Seat <- paste0("CA", " ", xy_intersection$namelsad) # create Seat column
              xy_intersection <- xy_intersection %>% filter(prc_area >= z) # filter percent where the intersect between x / y is equal or greater than 5% of the area of y
 
return(xy_intersection)
}

# next, congressional district-county 2020 crosswalk --------------------------
  congressional_district <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_cd116_500k")
  cong_xwalk <- make_xwalk(counties, congressional_district, .05) %>% select(Seat, county_geoid)
  df_2020_congressd <- df_2020 %>% filter(grepl('Congressional District', electoral_district)) # filter data for House of Rep only
  # create final congressional district df to merge. We will merge this later
  df_2020_congressd <- left_join(df_2020_congressd, cong_xwalk, by = c( "electoral_district" = "Seat"))  


# next, state assembly-county 2020 crosswalk ----------------------------------------
  ca_assembly <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_sldl_500k")
  assm_xwalk <- make_xwalk(counties, ca_assembly, .05) %>% select(Seat, county_geoid)
  df_2020_assembly <- df_2020 %>% filter(office_name == "CA State Assembly Member") # filter data for State Assm only
  df_2020_assembly$electoral_district <- str_replace_all(df_2020_assembly$electoral_district, "State House", "Assembly") # adjust electoral_district so matches assm_xwalk$Seat
  # create state assembly df. we will merge later
  df_2020_assembly <- left_join(df_2020_assembly, assm_xwalk, by = c("electoral_district" = "Seat"))
  

# next, state senate-county 2020 crosswalk ----------------------------------------
  ca_senate <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_sldu_500k")
  sen_xwalk <- make_xwalk(counties, ca_senate, .05) %>% select(Seat, county_geoid)
  df_2020_sen <- df_2020 %>% filter(office_name == "CA State Senator")
  # create state senate df. We will merge later
  df_2020_sen <- left_join(df_2020_sen, sen_xwalk, by = c("electoral_district" = "Seat"))


# There is no city-level or Board of Equalization data in the Candidates data: ----------------------------------------
  # unique(df_2014$office_level) [1] "House"             "State Legislature" "Governor" 
  # unique(df_2016$office_level) [1] "State Legislature" "Senate"            "House"          
  # unique(df_2018$office_level) [1] "Governor"          "House"             "Senate"            "State Legislature"
  # unique(df_2020$office_name)  [1] "U.S. Representative"      "CA State Assembly Member"     "CA State Senator"


# missing crosswalks: state/federal positions. There are no instances of this in the 2020 Candidate data. ------------------------------------------------------
  # combine 3 df's together: assembly, congress districts, senate 
  df_2020_crosswalk <- rbind(df_2020_assembly, df_2020_congressd)
  df_2020_crosswalk <- rbind(df_2020_crosswalk, df_2020_sen)

  # rename
  df_2020_crosswalk <- df_2020_crosswalk %>% arrange(county_geoid) %>% rename(county_id = county_geoid)


# County and state calculations ---------------------------------
county_calc <- function(x, year) { # the following two functions calculate the total candidates by race_catg and total for counties
  
      x <- x %>% group_by(
        race_catg, county_id) %>% 
        
       summarize(count = n()) %>% pivot_wider(
                  names_from = race_catg, values_from = count) %>%
        # an inline anonymous function to add the needed columns
        (function(.df){
          races <- c("nh_aian_candidates", "nh_twoormor_candidates")  # addt'l races which may/not be missing from each data yr
          # adding cls columns with zeroes if not present in the piped data.frame
          .df[races[!(races %in% colnames(.df))]] = 0
          return(.df)
        }) %>%
        
        select(county_id, latino_candidates, nh_aian_candidates, nh_api_candidates, nh_black_candidates, nh_twoormor_candidates, nh_white_candidates) %>%

          left_join(
            x %>% group_by(
              county_id) %>%
                summarize(total_candidates = n())) %>%
                  arrange(county_id) %>% filter(!is.na(county_id))

      x <- x %>% mutate(
                  latino_candidates =    ifelse(is.na(latino_candidates), 0,  latino_candidates),
                  nh_aian_candidates =   ifelse(is.na(nh_aian_candidates), 0, nh_aian_candidates),
                  nh_api_candidates =    ifelse(is.na(nh_api_candidates), 0,  nh_api_candidates),
                  nh_black_candidates =  ifelse(is.na(nh_black_candidates), 0,  nh_black_candidates),
                  nh_twoormor_candidates = ifelse(is.na(nh_twoormor_candidates), 0, nh_twoormor_candidates),
                  nh_white_candidates = ifelse(is.na(nh_white_candidates), 0, nh_white_candidates),
                  total_candidates =    ifelse(is.na(total_candidates), 0,  total_candidates))

return(x)        
}


state_calc <- function(x){ # Use the original data to calculate state since the county may have duplicates due to multiple counties in each geographic area
    x %>% group_by(
      race_catg, state
      
      ) %>% summarize(count = n()) %>% pivot_wider(
        names_from = race_catg, values_from = count
      )  %>%
    # an inline anonymous function to add the needed columns
    (function(.df){
      races <- c("nh_aian_candidates", "nh_twoormor_candidates")  # addt'l races which may/not be missing from each data yr
      # adding cls columns with zeroes if not present in the piped data.frame
      .df[races[!(races %in% colnames(.df))]] = 0
      return(.df)
    }) %>%
        left_join(
          
          x %>% group_by(
            state
          ) %>% summarize(total_candidates = n() )
          
        )  %>% select(state, latino_candidates, nh_aian_candidates, nh_api_candidates, nh_black_candidates, nh_twoormor_candidates, nh_white_candidates, total_candidates)
    
}


# 2014 calculations  --------------------------------------------------------------
## Counties
df_2014_calc <- county_calc(df_2014_crosswalk)
names(df_2014_calc) <- ifelse(names(df_2014_calc) == "county_id", "geoid",  paste0(names(df_2014_calc),"_14")) 

## State: use original df for state calc NOT county data
df_2014_state <- state_calc(df_2014)
names(df_2014_state) <- ifelse(names(df_2014_state) == "state", "geoid",  paste0(names(df_2014_state),"_14"))
  
# combine county and state
df_2014_final <- rbind(df_2014_calc, df_2014_state)


# 2016 calculations ---------------------------------------------------------------
## Counties
df_2016_calc <- county_calc(df_2016_crosswalk)
names(df_2016_calc) <- ifelse(names(df_2016_calc) == "county_id", "geoid",  paste0(names(df_2016_calc),"_16")) 

## State
df_2016_state <- state_calc(df_2016)
names(df_2016_state) <- ifelse(names(df_2016_state) == "state", "geoid",  paste0(names(df_2016_state),"_16"))

# combine county and state
df_2016_final <- rbind(df_2016_calc, df_2016_state)


# 2018 calculations ---------------------------------------------------------------
## Counties
df_2018_calc <- county_calc(df_2018_crosswalk)
names(df_2018_calc) <- ifelse(names(df_2018_calc) == "county_id", "geoid",  paste0(names(df_2018_calc),"_18")) 

## State
df_2018_state <- state_calc(df_2018)
names(df_2018_state) <- ifelse(names(df_2018_state) == "state", "geoid",  paste0(names(df_2018_state),"_18"))

# combine county and state
df_2018_final <- rbind(df_2018_calc, df_2018_state)


# 2020 calculations -------------------------------------------------------
## Counties
df_2020_calc <- county_calc(df_2020_crosswalk)
names(df_2020_calc) <- ifelse(names(df_2020_calc) == "county_id", "geoid",  paste0(names(df_2020_calc),"_20")) 

## State
df_2020_state <- state_calc(df_2020)
names(df_2020_state) <- ifelse(names(df_2020_state) == "state", "geoid",  paste0(names(df_2020_state),"_20"))

# combine county and state
df_2020_final <- rbind(df_2020_calc, df_2020_state)

# Merge all data years together
final <- full_join(df_2014_final, df_2016_final, by = "geoid")
final <- full_join(final, df_2018_final, by = "geoid")
final <- full_join(final, df_2020_final, by = "geoid")


# Get average Candidates across all data years ------------------------------------------------------------
data_yrs = 4  # update based on number of data yrs included

final_df <- final %>% mutate(
  nh_white_candidates = rowSums(select(.,nh_white_candidates_14, nh_white_candidates_16, nh_white_candidates_18, nh_white_candidates_20), na.rm = TRUE)/data_yrs,
  
  nh_black_candidates = rowSums(select(.,nh_black_candidates_14, nh_black_candidates_16, nh_black_candidates_18, nh_black_candidates_20), na.rm = TRUE)/data_yrs,
  
  latino_candidates = rowSums(select(.,latino_candidates_14, latino_candidates_16, latino_candidates_18, latino_candidates_20), na.rm = TRUE)/data_yrs,
  
  nh_api_candidates = rowSums(select(.,nh_api_candidates_14, nh_api_candidates_16, nh_api_candidates_18, nh_api_candidates_20), na.rm = TRUE)/data_yrs,
  
  nh_aian_candidates = rowSums(select(.,nh_aian_candidates_14, nh_aian_candidates_16, nh_aian_candidates_18, nh_aian_candidates_20), na.rm = TRUE)/data_yrs,
  
  nh_twoormor_candidates = rowSums(select(.,nh_twoormor_candidates_14, nh_twoormor_candidates_16,  nh_twoormor_candidates_18, nh_twoormor_candidates_20), na.rm = TRUE)/data_yrs,
  
  total_candidates = rowSums(select(.,total_candidates_14, total_candidates_16, total_candidates_18, total_candidates_20), na.rm = TRUE)/data_yrs
  
) %>% select(
  geoid,  nh_white_candidates, nh_black_candidates , latino_candidates,  nh_api_candidates,  nh_aian_candidates,  nh_twoormor_candidates, total_candidates
  
)

# replace CA with 06
final_df$geoid[final_df$geoid == "CA"] <- "06"


# Get Total Population ----------------------------------------------------
ACS <- st_read(con, query = "SELECT * FROM demographics.acs_5yr_dp05_multigeo_2020")


# create columns we need to merge for total and race/ethnicity categories
ACS$total_pop <- ACS$dp05_0070e
ACS$latino_pop <- ACS$dp05_0071e
ACS$nh_white_pop <- ACS$dp05_0077e
ACS$nh_black_pop <- ACS$dp05_0078e
ACS$aian_pop <- ACS$dp05_0066e
ACS$nh_api_pop <- ACS$dp05_0080e + ACS$dp05_0081e
ACS$nh_twoormor_pop <- ACS$dp05_0083e

ACS <- ACS %>% select(geoid, total_pop, latino_pop, nh_white_pop, nh_black_pop, aian_pop, nh_api_pop, nh_twoormor_pop)

# merge with final_df
final_df <- left_join(final_df, ACS, by = "geoid")

# Calculate County/State Raw and Rates -------------------------------------------------
final_df_rates <- final_df %>%
  
  mutate(
    total_raw = total_candidates,
    nh_white_raw = nh_white_candidates,
    nh_black_raw = nh_black_candidates,
    latino_raw = latino_candidates, 
    nh_api_raw = ifelse(nh_api_pop == 0, NA, nh_api_candidates),  ## screening out API pop with 0 
    nh_aian_raw = nh_aian_candidates,
    nh_twoormor_raw = nh_twoormor_candidates,
    
    total_rate = (total_candidates / total_pop) * 100000,
    nh_white_rate = (nh_white_candidates / nh_white_pop) * 100000,
    nh_black_rate = (nh_black_candidates / nh_black_pop) * 100000,
    latino_rate = (latino_candidates / latino_pop) * 100000,
    nh_api_rate = ifelse(nh_api_pop == 0, NA, (nh_api_candidates / nh_api_pop) * 100000), ## screening out API pop with 0 
    nh_aian_rate = (nh_aian_candidates / aian_pop) * 100000,
    nh_twoormor_rate = (nh_twoormor_candidates / nh_twoormor_pop) * 100000
    )


## get census geonames ------------------------------------------------------
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

# add county geonames
df <- merge(x=ca,y=final_df_rates,by="geoid", all=T)

# add state geoname
df <- within(df, geoname[geoid == '06'] <- 'California')

# Add geolevel field
df$geolevel <- ifelse(df$geoid == '06', 'state', 'county')
d <- df


############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity

# get STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ] %>% select(-c(geolevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

# get COUNTY into separate table and format id, name columns
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)


###update info for postgres tables###
county_table_name <- "arei_demo_diversity_of_candidates_county_2023"
state_table_name <- "arei_demo_diversity_of_candidates_state_2023"
rc_schema <- 'v5'

indicator <- paste0("Created on ", Sys.Date(), ". Annual average number of candidates for elected office of a race per 100k constituents of the same race. This data is")
source <- "Who Leads Us Campaign (county & state: 2014, 2016, 2018, and 2020)  https://wholeads.us/research, American Community Survey 5-Year Estimates, Table DP05 (2016-2020)"

#send tables to postgres
#to_postgres(county_table, state_table)

# close db connections
dbDisconnect(con)
dbDisconnect(con2)

