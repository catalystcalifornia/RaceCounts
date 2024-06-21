### Diversity of Electeds RC v5 ###

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
      
      race == "White" ~ "nh_white_electeds",
      race == "Black or African American" ~ "nh_black_electeds",
      race == "Hispanic or Latino" ~ "latino_electeds",
      race == "Asian American or Pacific Islander" ~ "nh_api_electeds",
      race == "American Indian or Alaska Native" ~ "nh_aian_electeds",
      race == "Multiracial" ~ "nh_twoormor_electeds"
      
    ))
  
return(x)  
}


# Get Data ----------------------------------------------------------------

## 2017 data
df_2017 <- read_csv("W:/Data/Democracy/WDN_WhoLeadsUs/Diversity of elected officials/elected_officials_rd2017.csv")  %>% filter(State == "CA")
df_2017 <- clean_data(df_2017)

## 2019 data
df_2019 <- read_csv("W:/Data/Democracy/WDN_WhoLeadsUs/Diversity of elected officials/elected_officials_rd2019.csv")  %>% filter(State == "CA")
df_2019 <- clean_data(df_2019)

## 2020 data
df_2020 <- read_xlsx("W:/Data/Democracy/WDN_WhoLeadsUs/Diversity of elected officials/Mapping Black Representation During a Turbulent Era Data and Summary Tabs 22421.xlsx",sheet=1) %>% filter(State == "CA") %>% select(-state)
df_2020 <- clean_data(df_2020)
df_2020$id = 1:nrow(df_2020)


############## COUNTY & STATE CALCS ############## 
### County Crosswalks --------------------------------------------------------------
# load crosswalks for 2017 and 2019 data
crosswalks_2017 <- st_read(con2, query = "SELECT * FROM data.wdn_2017_wholeadsus_electeds_crosswalk")
crosswalks_2019 <- st_read(con2, query = "SELECT * FROM data.wdn_2019_wholeadsus_electeds_crosswalk")


# Load CA Board of Equalization Member District-County df and join geonames from Census API -------------------------------------------------------------------------
board_equalization <- read_excel("W:/Data/Geographies/Board of Equalization Member Districts 1-4/20110727_q2_boe_final_draft_county_splits.xls", sheet = 2)

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

# merge county geoid 
board_equalization <- left_join(board_equalization, ca, by = c("County" = "geoname"))

# create a new column: office_name
board_equalization <- board_equalization %>% mutate(
  office_name = case_when(District == "01" ~ "Board of Equalization Member District 1",
                          District == "02" ~ "Board of Equalization Member District 2",
                          District == "03" ~ "Board of Equalization Member District 3",
                          District == "04" ~ "Board of Equalization Member District 4")
) %>%  rename(county_id = geoid) %>% select(
  county_id, office_name
)


# Join crosswalks to data -------------------------------------------------
# join 2017 crosswalk with 2017 data
df_2017_crosswalk <- left_join(df_2017, crosswalks_2017)

# join 2019 crosswalk with 2019 data

# first, remove Board of Equalization member District from df 2019 and make it a separate df to rbind later
df_2019_board_equalization <- df_2019 %>% filter(grepl('Equalization', office_name))
# merge with county id
df_2019_board_equalization <- left_join(df_2019_board_equalization, board_equalization)
# add arbitrary ID to merge with crosswalk data.
df_2019_board_equalization$id = NA

# remove Board of Equalization Member District observations from df
df_2019 <- df_2019 %>% filter(!grepl('Equalization', office_name))

# merge crosswalk
df_2019_crosswalk <- left_join(df_2019, crosswalks_2019)

# add in the board of equalization member observations
df_2019_crosswalk <- rbind(df_2019_crosswalk, df_2019_board_equalization)


# get 2020 geographies, then create crosswalks-------------------------------------------------------------------------
# first, start with counties
  counties <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_county_500k")
  counties <- st_transform(counties, 3310) # reproject to 3310

## create df specifically for counties. We will keep the counties already in the "jurisdiction" column and just create a "geoid" column
  df_2020_county <- df_2020 %>% filter(grepl('County', jurisdiction))
  counties2 <- as.data.frame(counties) %>% select(namelsad, county_geoid)

# remove extra county string for LA county observation
  df_2020_county$jurisdiction <- str_replace_all(df_2020_county$jurisdiction, "Los Angeles County County", "Los Angeles County")

# final df for counties. we will combine these later
  df_2020_county <- left_join(df_2020_county, counties2, by = c("jurisdiction" = "namelsad"))


make_xwalk <- function(x,y,z) {
              y$area <- st_area(y) # calculate area of congressional districts
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
  df_2020_congressd <- df_2020 %>% filter(body_name == "U.S. House of Representatives") # filter data for House of Rep only
  # create final congressional district df to merge. We will merge this later
  df_2020_congressd <- left_join(df_2020_congressd, cong_xwalk, by = c( "seat" = "Seat"))  


# next, state assembly-county 2020 crosswalk ----------------------------------------
  ca_assembly <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_sldl_500k")
  assm_xwalk <- make_xwalk(counties, ca_assembly, .05) %>% select(Seat, county_geoid)
  df_2020_assembly <- df_2020 %>% filter(body_name == "CA State Assembly") # filter data for State Assm only
  df_2020_assembly$electoral_district <- str_replace_all(df_2020_assembly$electoral_district, "CA State Assembly", "CA Assembly") # adjust electoral_district so matches assm_xwalk$Seat
  # create state assembly df. we will merge later
  df_2020_assembly <- left_join(df_2020_assembly, assm_xwalk, by = c("electoral_district" = "Seat"))
  

# next, state senate-county 2020 crosswalk ----------------------------------------
  ca_senate <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_sldu_500k")
  sen_xwalk <- make_xwalk(counties, ca_senate, .05) %>% select(Seat, county_geoid)
  df_2020_sen <- df_2020 %>% filter(body_name == "CA State Senate")
  # create state senate df. We will merge later
  df_2020_sen <- left_join(df_2020_sen, sen_xwalk, by = c("electoral_district" = "Seat"))


# next, city-county 2020 crosswalk ----------------------------------------
  ca_cities <- st_read(con, query = "SELECT * FROM geographies_ca.cb_2020_06_place_500k")
  city_xwalk <- make_xwalk(counties, ca_cities, .05) %>% select(namelsad, county_geoid)
  df_2020_cities <- df_2020 %>% filter(grepl('city', jurisdiction))
  # create city df
  df_2020_cities <- left_join(df_2020_cities, city_xwalk, by = c("jurisdiction" = "namelsad"))
  
# compare against arei_city_list table
            #city_crosswalk <- df_2020_cities %>% select(jurisdiction, county_geoid)
            
            #city_crosswalk <- distinct(city_crosswalk)
            
            #city_crosswalk$jurisdiction <- gsub("city", "", city_crosswalk$jurisdiction)
            #city_crosswalk$jurisdiction <- trimws(city_crosswalk$jurisdiction, "r")
            
            #arei_city_list <- st_read(con2, query = "SELECT * FROM v4.arei_city_list")
            
            #arei_city_list <- left_join(arei_city_list, city_crosswalk, by = c("city_name" = "jurisdiction"))

# next, Board Equalization Member Districts-County 2020 crosswalk ----------------------------------------
  df_2020_board_equalization <- df_2020 %>% filter(office_name == "CA Board of Equalization Member") %>% 
    select(-office_name) %>% mutate(
   office_name = case_when(seat == "District 1" ~ "Board of Equalization Member District 1",
                           seat == "District 2" ~ "Board of Equalization Member District 2",
                            seat == "District 3" ~ "Board of Equalization Member District 3",
                            seat == "District 4" ~ "Board of Equalization Member District 4")
  ) %>% select(state, office_uuid, office_name, seat, everything())
  
  # rename county_id to county_geoid to match with the rest of the df's
  df_2020_board_equalization <- left_join(df_2020_board_equalization, board_equalization) %>% rename(county_geoid = county_id)


# missing crosswalks: state/federal positions ------------------------------------------------------

  ## not every record will have a county_geoid. 
  df_2020_state_fed <- df_2020 %>% filter(grepl('County', jurisdiction) | body_name == "CA State Assembly" | body_name == "U.S. House of Representatives" | body_name == "CA State Senate" | grepl('city', jurisdiction) | office_name == "CA Board of Equalization Member")
  
  df_2020_state_fed$has_geoid = 1
  
  df_2020_state_fed <- left_join(df_2020, df_2020_state_fed)
  
  df_2020_state_fed <- df_2020_state_fed %>% filter(is.na(df_2020_state_fed$has_geoid)) %>% select(-has_geoid)
  # make county_geoid = NA
  df_2020_state_fed$county_geoid = NA
  
  # we have 21 observations that will not have county_geoid. this is for state and federal positions: "CA Attorney General", "CA Governor", "CA Insurance Commissioner", 
        # "CA Lieutenant Governor", "CA Secretary of State", "CA State Controller", "CA State Superintendent of Public Instruction", "CA State Treasurer", 
        # "CA Supreme Court Justice" x2, "U.S. Senator" x2
  # We will manually add them to each county's observations
  
  # combine 7 df's together: county, assembly, congress districts, senate, cities, board equalization, and state/fed positions(with no county geoid's)
  
  df_2020_crosswalk <- rbind(df_2020_county, df_2020_assembly)
  df_2020_crosswalk <- rbind(df_2020_crosswalk, df_2020_congressd)
  df_2020_crosswalk <- rbind(df_2020_crosswalk, df_2020_sen )
  df_2020_crosswalk <- rbind(df_2020_crosswalk, df_2020_cities)
  df_2020_crosswalk <- rbind(df_2020_crosswalk, df_2020_board_equalization)
  df_2020_crosswalk <- rbind(df_2020_crosswalk, df_2020_state_fed)
  
  # rename
  df_2020_crosswalk <- df_2020_crosswalk %>% arrange(id) %>% rename(county_id = county_geoid)


# County and state calculations ---------------------------------
county_calc <- function(x, year) { # the following two functions calculates the total elected officials by race_catg and total for counties
     x <- x %>% group_by(
        race_catg, county_id) %>% 
        
       summarize(count = n()) %>% pivot_wider(
                  names_from = race_catg, values_from = count) %>%
        select(county_id, latino_electeds, nh_aian_electeds, nh_api_electeds, nh_black_electeds, nh_twoormor_electeds, nh_white_electeds) %>%
        
          left_join(
            x %>% group_by(
              county_id) %>% 
                summarize(total_electeds = n())) %>% 
                  arrange(county_id) %>% filter(!is.na(county_id)) 
  
      x <- x %>% mutate(
                  latino_electeds =    ifelse(is.na(latino_electeds), 0,  latino_electeds),
                  nh_aian_electeds =   ifelse(is.na(nh_aian_electeds), 0, nh_aian_electeds),
                  nh_api_electeds =    ifelse(is.na(nh_api_electeds), 0,  nh_api_electeds),
                  nh_black_electeds =  ifelse(is.na(nh_black_electeds), 0,  nh_black_electeds),
                  nh_twoormor_electeds = ifelse(is.na(nh_twoormor_electeds), 0, nh_twoormor_electeds),
                  nh_white_electeds = ifelse(is.na(nh_white_electeds), 0, nh_white_electeds),
                  total_electeds =    ifelse(is.na(total_electeds), 0,  total_electeds))

return(x)        
}


state_calc <- function(x){ # Use the original data to calculate state since the county may have duplicates due to multiple counties in each geographic area
    x %>% group_by(
      race_catg, state
      
      ) %>% summarize(count = n()) %>% pivot_wider(
        names_from = race_catg, values_from = count
      )  %>%
      
        left_join(
          
          x %>% group_by(
            state
          ) %>% summarize(total_electeds = n() )
          
        )  %>% select(state, latino_electeds, nh_aian_electeds, nh_api_electeds, nh_black_electeds, nh_twoormor_electeds, nh_white_electeds, total_electeds)
    
}


# 2017 calculations  --------------------------------------------------------------
## Counties
df_2017_calc <- county_calc(df_2017_crosswalk)

# add state/fed positions
  # change na to 0
  df_2017_calc <- df_2017_calc %>%  mutate(
      ## add statewide/federal positions to each county
      # df_2017_crosswalk %>% filter(is.na(county_id)) %>% group_by(race_catg) %>% summarize(count = n()) %>% bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~'total')))
      # 2 for latino
      # 2 for nh nh api
      # 1 for nh two or more
      # 5 for nh white
      # 10 for total 
    
      latino_electeds=   latino_electeds + 2,
      nh_api_electeds =    nh_api_electeds + 2,
      nh_twoormor_electeds =  nh_twoormor_electeds + 1,
      nh_white_electeds =  nh_white_electeds + 5,
      total_electeds=     total_electeds + 10
      )

names(df_2017_calc) <- ifelse(names(df_2017_calc) == "county_id", "geoid",  paste0(names(df_2017_calc),"_17")) 

## State: use original df for state calc NOT county data
df_2017_state <- state_calc(df_2017)
names(df_2017_state) <- ifelse(names(df_2017_state) == "state", "geoid",  paste0(names(df_2017_state),"_17"))
  
# combine county and state
df_2017_final <- rbind(df_2017_calc, df_2017_state)


# 2019 calculations ---------------------------------------------------------------
## Counties
df_2019_calc <- county_calc(df_2019_crosswalk)

# add state/fed totals to each county
df_2019_calc <- df_2019_calc %>%  mutate(
    ## add statewide/federal positions to each city
    # df_2019_crosswalk %>% filter(is.na(county_id)) %>% group_by(race_catg) %>% summarize(count = n()) %>% bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~'total')))
    # 4 for latino
    # 5 for nh nh api
    # 2 for nh black
    # 1 for nh two or more
    # 5 for nh white
    # 17 for total 
    
    latino_electeds=   latino_electeds + 4,
    nh_api_electeds =    nh_api_electeds + 5,
    nh_black_electeds =  nh_black_electeds + 2,
    nh_twoormor_electeds =  nh_twoormor_electeds + 1,
    nh_white_electeds =  nh_white_electeds + 5,
    total_electeds=     total_electeds + 17
  )

names(df_2019_calc) <- ifelse(names(df_2019_calc) == "county_id", "geoid",  paste0(names(df_2019_calc),"_19")) 

## State
df_2019_state <- state_calc(df_2019)
names(df_2019_state) <- ifelse(names(df_2019_state) == "state", "geoid",  paste0(names(df_2019_state),"_19"))

# combine county and state
df_2019_final <- rbind(df_2019_calc, df_2019_state)


# 2020 calculations -------------------------------------------------------
## Counties
df_2020_calc <- county_calc(df_2020_crosswalk)

# add state/fed positions
  # change na to 0
df_2020_calc <- df_2020_calc %>%  mutate(
    ## add statewide/federal positions to each county
    # df_2020_crosswalk %>% filter(is.na(county_id)) %>% group_by(race_catg) %>% summarize(count = n()) %>% bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~'total')))
    # 4 for latino
    # 5 for nh nh api
    # 2 for nh_black
    # 1 for nh two or more
    # 5 for nh white
    # 17 for total 
    
    latino_electeds =   latino_electeds + 4,
    nh_api_electeds =    nh_api_electeds + 5,
    nh_black_electeds =  nh_black_electeds + 2,
    nh_twoormor_electeds =  nh_twoormor_electeds + 1,
    nh_white_electeds =  nh_white_electeds + 5,
    total_electeds =     total_electeds + 17
    )

names(df_2020_calc) <- ifelse(names(df_2020_calc) == "county_id", "geoid",  paste0(names(df_2020_calc),"_20")) 

## State
df_2020_state <- state_calc(df_2020)
names(df_2020_state) <- ifelse(names(df_2020_state) == "state", "geoid",  paste0(names(df_2020_state),"_20"))

# combine county and state
df_2020_final <- rbind(df_2020_calc, df_2020_state)

# Merge all data years together
final <- full_join(df_2017_final, df_2019_final, by = "geoid")
final <- full_join(final, df_2020_final, by = "geoid")


# Get average Electeds across all data years ------------------------------------------------------------
data_yrs = 3  # update based on number of data yrs included

final_df <- final %>% mutate(
  nh_white_electeds = rowSums(select(.,nh_white_electeds_17, nh_white_electeds_19, nh_white_electeds_20), na.rm = TRUE)/data_yrs,
  
  nh_black_electeds = rowSums(select(.,nh_black_electeds_17, nh_black_electeds_19, nh_black_electeds_20), na.rm = TRUE)/data_yrs,
  
  latino_electeds = rowSums(select(.,latino_electeds_17, latino_electeds_19, latino_electeds_20), na.rm = TRUE)/data_yrs,
  
  nh_api_electeds = rowSums(select(.,nh_api_electeds_17, nh_api_electeds_19, nh_api_electeds_20), na.rm = TRUE)/data_yrs,
  
  nh_aian_electeds = rowSums(select(.,nh_aian_electeds_17, nh_aian_electeds_19, nh_aian_electeds_20), na.rm = TRUE)/data_yrs,
  
  nh_twoormor_electeds = rowSums(select(.,nh_twoormor_electeds_17, nh_twoormor_electeds_19, nh_twoormor_electeds_20), na.rm = TRUE)/data_yrs,
  
  total_electeds = rowSums(select(.,total_electeds_17, total_electeds_19, total_electeds_20), na.rm = TRUE)/data_yrs
  
) %>% select(
  geoid,  nh_white_electeds, nh_black_electeds , latino_electeds,  nh_api_electeds,  nh_aian_electeds,  nh_twoormor_electeds, total_electeds
  
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
    total_raw = total_electeds,
    nh_white_raw = nh_white_electeds,
    nh_black_raw = nh_black_electeds,
    latino_raw = latino_electeds, 
    nh_api_raw = ifelse(nh_api_pop == 0, NA, nh_api_electeds),  ## screening out API pop with 0 
    nh_aian_raw = nh_aian_electeds,
    nh_twoormor_raw = nh_twoormor_electeds,
    
    total_rate = (total_electeds / total_pop) * 100000,
    nh_white_rate = (nh_white_electeds / nh_white_pop) * 100000,
    nh_black_rate = (nh_black_electeds / nh_black_pop) * 100000,
    latino_rate = (latino_electeds / latino_pop) * 100000,
    nh_api_rate = ifelse(nh_api_pop == 0, NA, (nh_api_electeds / nh_api_pop) * 100000), ## screening out API pop with 0 
    nh_aian_rate = (nh_aian_electeds / aian_pop) * 100000,
    nh_twoormor_rate = (nh_twoormor_electeds / nh_twoormor_pop) * 100000
    )


## get census geonames ------------------------------------------------------
# add county geonames
df <- merge(x=ca,y=final_df_rates,by="geoid", all=T)

# add state geoname
df <- within(df, geoname[geoid == '06'] <- 'California')

# Add geolevel field
df$geolevel <- ifelse(df$geoid == '06', 'state', 'county')


############## CITY CALCS ############## 
# Clean up city names
#### NOTE: This section will need to be revisited each year to account for inconsistencies in original data
df_2019_cities <- filter(df_2019, office_level == 'locality')
df_2019_cities$geoname <- gsub(" City Council District |City Council Ward | Council District ", "", df_2019_cities$electoral_district) 
df_2019_cities$geoname <- gsub("[[:digit:]]", "", df_2019_cities$geoname)  
df_2019_cities$geoname <- trimws(df_2019_cities$geoname)  # trim trailing spaces

df_2020_cities$geoname <- gsub(" city", "", df_2020_cities$jurisdiction) # clean up city names
df_2020_cities$geoname <- trimws(df_2020_cities$geoname)  # trim trailing spaces


# City calculations
city_calc <- function(x, year) { # the following two functions calculates the total elected officials by race_catg and total for counties
  x <- x %>% group_by(
    race_catg, geoname) %>% 
    
    summarize(count = n()) %>% pivot_wider(
      names_from = race_catg, values_from = count) %>%
    select(geoname, latino_electeds, nh_aian_electeds, nh_api_electeds, nh_black_electeds, nh_twoormor_electeds, nh_white_electeds) %>%
    
    left_join(
      x %>% group_by(
        geoname) %>% 
        summarize(total_electeds = n())) %>% 
    arrange(geoname) %>% filter(!is.na(geoname)) 
  
  x <- x %>% mutate(
    latino_electeds =    ifelse(is.na(latino_electeds), 0,  latino_electeds),
    nh_aian_electeds =   ifelse(is.na(nh_aian_electeds), 0, nh_aian_electeds),
    nh_api_electeds =    ifelse(is.na(nh_api_electeds), 0,  nh_api_electeds),
    nh_black_electeds =  ifelse(is.na(nh_black_electeds), 0,  nh_black_electeds),
    nh_twoormor_electeds = ifelse(is.na(nh_twoormor_electeds), 0, nh_twoormor_electeds),
    nh_white_electeds = ifelse(is.na(nh_white_electeds), 0, nh_white_electeds),
    total_electeds =    ifelse(is.na(total_electeds), 0,  total_electeds))
  
  return(x)        
}

df_2019_calc_city <- city_calc(df_2019_cities)
df_2020_calc_city <- city_calc(df_2020_cities)


## add county id's to city data -----------------------------------------------
  counties_places_final <- st_read(con, query = "select * from crosswalks.county_place_2020")
  df_2019_calc_city <- left_join(df_2019_calc_city, select(counties_places_final, place_name, place_geoid, county_geoid, county_name), by = c("geoname" = "place_name")) %>%
                            select(geoname, place_geoid, county_name, county_geoid, everything())
  df_2020_calc_city <- left_join(df_2020_calc_city, select(counties_places_final, place_name, place_geoid, county_geoid, county_name), by = c("geoname" = "place_name")) %>%
                            select(geoname, place_geoid, county_name, county_geoid, everything())    
  
# 2019: add statewide/fed totals to each city
  df_2019_calc_city <- df_2019_calc_city %>%  mutate(
#### ADD COUNTYWIDE/STATEWIDE/FEDERAL POSITIONS TO EACH CITY ####
  ## add statewide/federal positions to each city
  # df_2019_crosswalk %>% filter(is.na(county_id)) %>% group_by(race_catg) %>% summarize(count = n()) %>% bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~'total')))
  # 4 for latino
  # 5 for nh nh api
  # 2 for nh black
  # 1 for nh two or more
  # 5 for nh white
  # 17 for total 
  
  latino_electeds =   latino_electeds + 4,
  nh_api_electeds =    nh_api_electeds + 5,
  nh_black_electeds =  nh_black_electeds + 2,
  nh_twoormor_electeds =  nh_twoormor_electeds + 1,
  nh_white_electeds =  nh_white_electeds + 5,
  total_electeds =     total_electeds + 17
)

  ## add 2019 COUNTYWIDE positions to each city. Excludes County Supervisors who represent districts NOT the whole county the city is within.
  county_electeds19 <- df_2019_crosswalk %>% filter(office_level == "administrativeArea2" & office_role != "legislatorUpperBody") %>% 
                            group_by(county_id, race_catg) %>% summarize(county_count = n()) %>% 
                                  bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~'total'))) %>%
                                        rename(county_geoid = county_id)
  df_2019_final_long <- df_2019_calc_city %>% pivot_longer(cols = ends_with("_electeds"), names_to = "race_catg", values_to = "electeds") # pivot city data longer
  df_2019_final_long <- left_join(df_2019_final_long, county_electeds19, by = c("county_geoid", "race_catg"))
  df_2019_final_long$all_electeds <- rowSums(df_2019_final_long[,c("electeds", "county_count")], na.rm=TRUE)
  
  #df_2019_final_long$race_catg <- paste0(df_2019_calc_city$race_catg, '_19') # add year suffix to 2019 data


# 2020: add statewide/fed totals to each city
  df_2020_calc_city <- df_2020_calc_city %>%  mutate(
    #### ADD COUNTY/STATEWIDE/FEDERAL POSITIONS TO EACH CITY ####
    ## add statewide/federal positions to each city
    # df_2020_crosswalk %>% filter(is.na(county_id)) %>% group_by(race_catg) %>% summarize(count = n()) %>% bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~'total')))
    # 4 for latino
    # 5 for nh nh api
    # 2 for nh black
    # 1 for nh two or more
    # 5 for nh white
    # 17 for total 
    
    latino_electeds =   latino_electeds + 4,
    nh_api_electeds =    nh_api_electeds + 5,
    nh_black_electeds =  nh_black_electeds + 2,
    nh_twoormor_electeds =  nh_twoormor_electeds + 1,
    nh_white_electeds =  nh_white_electeds + 5,
    total_electeds =     total_electeds + 17
  )
  
  ## add 2020 COUNTYWIDE positions to each city. Excludes County Supervisors who represent districts NOT the whole county the city is within.
  county_electeds20 <- df_2020_crosswalk %>% filter(office_level == "administrativeArea2" & office_role != "legislatorUpperBody") %>% 
                      group_by(county_id, race_catg) %>% summarize(county_count = n()) %>% 
                            bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~'total'))) %>%
                                  rename(county_geoid = county_id)
  df_2020_final_long <- df_2020_calc_city %>% pivot_longer(cols = ends_with("_electeds"), names_to = "race_catg", values_to = "electeds") # pivot city data longer
  df_2020_final_long <- left_join(df_2020_final_long, county_electeds20, by = c("county_geoid", "race_catg"))
  df_2020_final_long$all_electeds <- rowSums(df_2020_final_long[,c("electeds", "county_count")], na.rm=TRUE)  


### Calc city raw and rates -------------------------------------------------
# merge all city data years together
final_city <- rbind(df_2019_final_long, df_2020_final_long) # combine

# Get city average electeds (raw) across all data years 
data_yrs_city = 2  # update based on number of data yrs included  
final_city <- final_city %>% group_by(across(where(is.character))) %>% summarise(avg_electeds = sum(all_electeds)/data_yrs_city) # get total # of electeds by race

# Add city pop data
# merge with final_df
final_city <- left_join(final_city, ACS, by = c("place_geoid" = "geoid"))
final_city_wide <- final_city %>% pivot_wider(names_from = "race_catg", values_from = "avg_electeds") # pivot city data + pop data wider

# Get city average electeds (rate) across all data years 
df_city <- final_city_wide %>%
          mutate(
            total_raw = total_electeds,
            nh_white_raw = nh_white_electeds,
            nh_black_raw = nh_black_electeds,
            latino_raw = latino_electeds, 
            nh_api_raw = ifelse(nh_api_pop == 0, NA, nh_api_electeds),  ## screening out API pop with 0 
            nh_aian_raw = nh_aian_electeds,
            nh_twoormor_raw = nh_twoormor_electeds,
            
            total_rate = (total_electeds / total_pop) * 100000,
            nh_white_rate = (nh_white_electeds / nh_white_pop) * 100000,
            nh_black_rate = (nh_black_electeds / nh_black_pop) * 100000,
            latino_rate = (latino_electeds / latino_pop) * 100000,
            nh_api_rate = (nh_api_electeds / nh_api_pop) * 100000,
            nh_aian_rate = (nh_aian_electeds / aian_pop) * 100000,
            nh_twoormor_rate = (nh_twoormor_electeds / nh_twoormor_pop) * 100000
          )


# Merge city, county/state data -------------------------------------------
df_city <- df_city %>% rename(geoid = place_geoid) %>% mutate(geolevel = 'city') %>% ungroup() %>% select(-c(county_name, county_geoid))
d <- bind_rows(df_city, df)


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


# get CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)


###update info for postgres tables###
county_table_name <- "arei_demo_diversity_of_electeds_county_2023"
state_table_name <- "arei_demo_diversity_of_electeds_state_2023"
city_table_name <- "arei_demo_diversity_of_electeds_city_2023"
rc_schema <- 'v5'

indicator <- "Annual average number of elected official of a race per 100k constituents of the same race. This data is"
source <- "Who Leads Us Campaign (county & state: 2017, 2019, and 2020; city: 2019, 2020)  https://wholeads.us/research, American Community Survey 5-Year Estimates, Table DP05 (2016-2020)"

#send tables to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)

# close db connections
dbDisconnect(con)
dbDisconnect(con2)

