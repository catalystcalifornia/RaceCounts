## Low Quality Housing Units RC v6 City ##

# Install packages if not already installed
list.of.packages <- c("data.table", "stringr", "dplyr", "RPostgreSQL", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "here", "sf", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#Load libraries
library(data.table)
library(stringr)
library(dplyr)
library(RPostgreSQL)
library(dbplyr)
library(srvyr)
library(tidycensus)
library(tidyr)
library(rpostgis)
library(here)
library(sf)
library(tigris)
library(usethis)
options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")
con2<- connect_to_db("racecounts")


#set source for weighted averages script
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")

# Update each year
rc_yr <- '2025'
rc_schema <- 'v7'
curr_yr = 2023
survey = "acs5"
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
var =  c("B25003_001", "B25003B_001", "B25003C_001", "B25003D_001", "B25003E_001", "B25003F_001","B25003G_001", "B25003H_001", "B25003I_001", "B25040_010","B25048_003", "B25052_003")


# Data Dictionary ------------------------------------------------------
data_dict <- load_variables(year = curr_yr, dataset = survey, cache = TRUE) %>% filter (name %in% var)

# TENURE HOUSEHOLDER, NO FUEL, NO PUMBLING, NO KITCHEN ------------------------------------------------------------------

############## UPDATE FOR SPECIFIC INDICATOR HERE ##############

# Pull data from Census API
df <- do.call(rbind.data.frame, list(
  get_acs(geography = "tract", state = "CA", variables = var, year = curr_yr, survey = survey, cache_table = TRUE)
  %>% mutate(geolevel = "tract")
))

# Rename estimate and moe columns to e and m, respectively
df <- df %>%
  rename(e = estimate, m = moe)

# Spread! Switch to wide format.
df <- df %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

# Rename, calculate moe, pct, cv's
df_calcs <- df %>% rename(
  total_universe = B25003_001e,
  total_universe_moe = B25003_001m,
  black_universe = B25003B_001e,
  black_universe_moe = B25003B_001m,
  aian_universe = B25003C_001e,
  aian_universe_moe = B25003C_001m,
  asian_universe = B25003D_001e,
  asian_universe_moe = B25003D_001m,
  pacisl_universe = B25003E_001e,
  pacisl_universe_moe = B25003E_001m,
  other_universe = B25003F_001e,
  other_universe_moe = B25003F_001m,
  twoormor_universe = B25003G_001e,
  twoormor_moe = B25003G_001m,
  nh_white_universe = B25003H_001e,
  nh_white_universe_moe = B25003H_001m ,
  latino_universe = B25003I_001e,
  latino_universe_moe = B25003I_001m,
  
  num_incomplete_heating = B25040_010e,
  moe_num_incomplete_heating = B25040_010m,
  
  num_incomplete_plumbing = B25048_003e,
  moe_num_incomplete_plumbing = B25048_003m,
  
  
  num_incomplete_kitchen = B25052_003e,
  moe_num_incomplete_kitchen = B25052_003m) %>%
  
  mutate(
    pct_incomplete_heating = ifelse(total_universe == 0, NA, num_incomplete_heating/ total_universe * 100),
    
    pct_incomplete_plumbing= ifelse(total_universe == 0, NA, num_incomplete_plumbing/ total_universe * 100),
    
    pct_incomplete_kitchen= ifelse(total_universe == 0, NA, num_incomplete_kitchen/ total_universe * 100))
 


### CT-Place Crosswalk ###
# set source for Crosswalk Function script
source("W:/Project/RACE COUNTS/Functions/RC_CT_Place_Xwalk.R")
crosswalk <- make_ct_place_xwalk(curr_yr) # must specify which data year

# universe(total housing units and by race)
universe <- df_calcs %>% select(GEOID, NAME, ends_with("universe"))

# join with cross walk
universe <- universe  %>% right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid")) %>% dplyr::rename(sub_id = GEOID, target_id = place_geoid)  # join target geoids/names and rename to generic column names for WA functions 

# rename to pop
pop <- universe %>% rename_all(.funs = funs(sub("universe*", "pop", names(universe)))) %>% as.data.frame() %>%
                    mutate(geolevel = "tract")

#  CITY WEIGHTED AVG CALCS ------------------------------------------------
############## UPDATE FOR SPECIFIC INDICATOR HERE ##############
pop_threshold = 0                 # define population threshold for screening: note this is only for households. We need to assign pop threshold a value in order for functions to work

# select pop columns and rename to RC names
b <- select(pop, sub_id, target_id, ends_with("pop"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- gsub("_pop", "_target_pop", colnames(c))

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop, sub_id, target_id, geolevel, ends_with("pop"), -NAME) 
names(e) <-gsub("_pop", "_sub_pop", colnames(e))
pop <- e %>% left_join(c, by = "target_id")
pop_df <- as.data.frame(pop)


## indicator 1: Lack of Kitchen

# Rename so WA functions can work
ind_df <- df_calcs  %>% select(GEOID, NAME, pct_incomplete_kitchen) %>% rename(indicator = pct_incomplete_kitchen, sub_id = GEOID )

pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel

# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df) 

# rename columns for RC functions
kitchen_city_wa <- city_wa %>% left_join(crosswalk %>% select(place_geoid, place_name) %>% distinct(), by = c("target_id" = "place_geoid"), relationship = "one-to-many") %>%
  rename(geoname = place_name, geoid = target_id) %>% dplyr::relocate(geoname, .after = geoid)   

# rename columns
kitchen_city_wa  <- kitchen_city_wa  %>% rename_all(.funs = funs(sub("rate", "kitchen", names(kitchen_city_wa))))


## indicator 2: Lack of Heating

# Rename so WA functions can work
ind_df <- df_calcs %>% select(GEOID, NAME, pct_incomplete_heating) %>% rename(indicator = pct_incomplete_heating, sub_id = GEOID )

# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df)        

heating_city_wa <- city_wa %>% left_join(crosswalk %>% select(place_geoid, place_name) %>% distinct(), by = c("target_id" = "place_geoid"), relationship = "one-to-many") %>%
  rename(geoname = place_name, geoid = target_id) %>% dplyr::relocate(geoname, .after = geoid)   

# rename columns
heating_city_wa  <- heating_city_wa  %>% rename_all(.funs = funs(sub("rate", "heating", names(heating_city_wa))))


## indicator 3: Lack of Plumbing

# Rename so WA functions can work
ind_df <- df_calcs %>% select(GEOID, NAME, pct_incomplete_plumbing) %>% rename(indicator = pct_incomplete_plumbing, sub_id = GEOID)

# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df)        

plumbing_city_wa <- city_wa %>% left_join(crosswalk %>% select(place_geoid, place_name) %>% distinct(), by = c("target_id" = "place_geoid"), relationship = "one-to-many") %>%
  rename(geoname = place_name, geoid = target_id) %>% dplyr::relocate(geoname, .after = geoid)   

# rename columns
plumbing_city_wa <- plumbing_city_wa %>% rename_all(.funs = funs(sub("rate", "plumbing", names(plumbing_city_wa))))


# Merge all 3 -------------------------------------------------------------

df <- kitchen_city_wa %>% full_join(heating_city_wa) %>% full_join(plumbing_city_wa) %>% select(geoid, geoname, ends_with("kitchen"), ends_with("plumbing"), ends_with("heating"), everything())

## average out the rates, ## filter for cities that only have 1 of the 3 indicator for total/by race. 
df_average <- df %>% mutate(
  
  nh_white_rate = ifelse(!is.na(nh_white_heating) & is.na(nh_white_kitchen) & is.na(nh_white_plumbing) | is.na(nh_white_heating) & !is.na(nh_white_kitchen) & is.na(nh_white_plumbing) | is.na(nh_white_heating) & is.na(nh_white_kitchen) & !is.na(nh_white_plumbing), NA, (nh_white_heating + nh_white_kitchen + nh_white_plumbing) / 3),
  
  black_rate = ifelse(!is.na(black_heating) & is.na(black_kitchen) & is.na(black_plumbing) | is.na(black_heating) & !is.na(black_kitchen) & is.na(black_plumbing) | is.na(black_heating) & is.na(black_kitchen) & !is.na(black_plumbing), NA, (black_heating + black_kitchen + black_plumbing) / 3),
  
  aian_rate = ifelse(!is.na(aian_heating) & is.na(aian_kitchen) & is.na(aian_plumbing) | is.na(aian_heating) & !is.na(aian_kitchen) & is.na(aian_plumbing) | is.na(aian_heating) & is.na(aian_kitchen) & !is.na(aian_plumbing), NA, (aian_heating + aian_kitchen + aian_plumbing) / 3),
  
  latino_rate = ifelse(!is.na(latino_heating) & is.na(latino_kitchen) & is.na(latino_plumbing) | is.na(latino_heating) & !is.na(latino_kitchen) & is.na(latino_plumbing) | is.na(latino_heating) & is.na(latino_kitchen) & !is.na(latino_plumbing), NA, (latino_heating + latino_kitchen + latino_plumbing) / 3),
  
  
  asian_rate = ifelse(!is.na(asian_heating) & is.na(asian_kitchen) & is.na(asian_plumbing) | is.na(asian_heating) & !is.na(asian_kitchen) & is.na(asian_plumbing) | is.na(asian_heating) & is.na(asian_kitchen) & !is.na(asian_plumbing), NA, (asian_heating + asian_kitchen + asian_plumbing) / 3),
  
  
  pacisl_rate = ifelse(!is.na(pacisl_heating) & is.na(pacisl_kitchen) & is.na(pacisl_plumbing) | is.na(pacisl_heating) & !is.na(pacisl_kitchen) & is.na(pacisl_plumbing) | is.na(pacisl_heating) & is.na(pacisl_kitchen) & !is.na(pacisl_plumbing), NA, (pacisl_heating + pacisl_kitchen + pacisl_plumbing) / 3),
  
  
  other_rate = ifelse(!is.na(other_heating) & is.na(other_kitchen) & is.na(other_plumbing) | is.na(other_heating) & !is.na(other_kitchen) & is.na(other_plumbing) | is.na(other_heating) & is.na(other_kitchen) & !is.na(other_plumbing), NA, (other_heating + other_kitchen + other_plumbing) / 3),
  
  twoormor_rate = ifelse(!is.na( twoormor_heating) & is.na( twoormor_kitchen) & is.na( twoormor_plumbing) | is.na( twoormor_heating) & !is.na( twoormor_kitchen) & is.na( twoormor_plumbing) | is.na( twoormor_heating) & is.na( twoormor_kitchen) & !is.na( twoormor_plumbing), NA, ( twoormor_heating +  twoormor_kitchen +  twoormor_plumbing) / 3),
  
  
  total_rate = ifelse(!is.na(total_heating) & is.na(total_kitchen) & is.na(total_plumbing) | is.na(total_heating) & !is.na(total_kitchen) & is.na(total_plumbing) | is.na(total_heating) & is.na(total_kitchen) & !is.na(total_plumbing), NA, (total_heating + total_kitchen + total_plumbing) / 3)

) %>% select(
  
  
  geoid, geoname, ends_with("rate")
) 


# Pop Screening -----------------------------------------------------------

# Load population estimates that match our data: total, black, aian, asian, nhpi, other, two or more, latino, nh white ------------------------------
var2 = c("B01001_001", "B01001B_001", "B01001C_001", "B01001D_001", "B01001E_001", "B01001F_001", "B01001G_001", "B01001H_001", "B01001I_001")

# Pull data from Census API
pop2 <- do.call(rbind.data.frame, list(
  get_acs(geography = "place", state = "CA", variables = var2, year = curr_yr, survey = survey, cache_table = TRUE)
  #%>% mutate(geolevel = "place" )
))

# Rename estimate and moe columns to e and m, respectively
pop2 <- pop2 %>%
  rename(e = estimate, m = moe)

# Spread! Switch to wide format.
pop2 <- pop2 %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

# rename columns to population estimates
pop2 <- pop2 %>% rename(geoid = GEOID,
                        total_pop = B01001_001e,
                        black_pop = B01001B_001e,
                        aian_pop = B01001C_001e,
                        asian_pop = B01001D_001e,
                        nhpi_pop = B01001E_001e,
                        other_pop = B01001F_001e,
                        twoormor_pop = B01001G_001e,
                        nh_white_pop = B01001H_001e,
                        latino_pop = B01001I_001e) %>% select(geoid, ends_with("pop"))

population_threshold = 250


df_screen <- df_average %>% left_join(pop2) %>%
  mutate(
    total_rate = ifelse(total_pop < population_threshold, NA, total_rate),
    nh_white_rate = ifelse(nh_white_pop < population_threshold, NA, nh_white_rate),
    black_rate = ifelse(black_pop < population_threshold, NA, black_rate),
    aian_rate = ifelse(aian_pop < population_threshold, NA, aian_rate),
    asian_rate = ifelse(asian_pop < population_threshold, NA, asian_rate),
    pacisl_rate = ifelse(nhpi_pop < population_threshold, NA, pacisl_rate),
    other_rate = ifelse(other_pop < population_threshold, NA, other_rate),
    twoormor_rate = ifelse(twoormor_pop < population_threshold, NA, twoormor_rate),
    latino_rate = ifelse(latino_pop < population_threshold, NA, latino_rate)
  )


d <- df_screen %>% mutate(geolevel="city") #the function is expecting the df to have a geolevel column which is likely why it was not running before



############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
# View(city_table)

# Table metadata will auto update #
city_table_name <- paste0("arei_hous_housing_quality_city_", rc_yr)
start_yr <- curr_yr-4

indicator <- paste0("Created on ", Sys.Date(), ". Average percent of households that lack kitchen, plumbing, and heat in comparison to total households.")
source <- paste0("American Community Survey 5-Year Estimates (", start_yr,"-",curr_yr, ") Tables B25048, B25052, B25003B-I, B25040. W:/Project/RACE COUNTS/2025_v7/Housing/Documentation/QA_Housing_Quality_City.docx")

city_to_postgres(city_table)

#close connection
dbDisconnect(con)
dbDisconnect(con2)