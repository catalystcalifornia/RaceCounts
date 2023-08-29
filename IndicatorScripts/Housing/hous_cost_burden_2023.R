# Install packages if not already installed
list.of.packages <- c("data.table", "stringr", "dplyr", "RPostgreSQL", "dbplyr", 
                      "srvyr", "tidycensus", "rpostgis",  "tidyr", "readxl")
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
library(rpostgis)
library(tidyr)
library(readxl)

######################Data set-up######################

setwd("W:/Project/RACE COUNTS/2022_v4/Housing/Data/")

# data file path (for countys)
root <- "W:/Data/Housing/HUD/CHAS/"

# Load the CHAS data and dictionary

state_data <- fread(paste0(root, "2014thru2018-040-csv/040/Table9.csv"), header = TRUE, data.table = FALSE)
state_data$geoid <- substring(state_data$geoid,8)
state_data$geolevel <- "state"
# View(state_data)

county_data <- fread(paste0(root, "2014thru2018-050-csv/050/Table9.csv"), header = TRUE, data.table = FALSE)
county_data$geoid <- substring(county_data$geoid,8)
county_data$geolevel <- "county"
# View(county_data)

city_data <- fread(paste0(root, "2014thru2018-160-csv/160/Table9.csv"), header = TRUE, data.table = FALSE)
city_data$geoid <- substring(city_data$geoid,8)
city_data$geolevel <- "city"
# View(city_data)

dict <- read_excel(paste0(root, "2014thru2018-050-csv/050/CHAS data dictionary 14-18.xlsx"), sheet = "Table 9")
# View(dict)

state_data['cnty'] <- NA
state_data['place'] <-  NA
county_data['place'] <-  NA
city_data['cnty'] <- NA
ppl <- rbind(state_data, county_data, city_data)
# View(ppl)


# export incarceration  to rda shared table ------------------------------------------------------------
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# con2 <- connect_to_db("rda_shared_data")
# table_schema <- "housing"
# table_name <- "hud_chas_cost_burden_multigeo_2014_18"
# table_comment_source <- "The percentage of owner-occupied housing units experiencing cost burden (Monthly housing costs, including utilities, exceeding 30% of monthly income. White, Black, Asian, AIAN, and PacIsl one race alone and Latinx-exclusive. Other includes other race and two or more races, and is Latinx-exclusive.  "
# table_source <- "HUD CHAS (2014-2018) https://www.huduser.gov/portal/datasets/cp.html#2006-2018_data"
# 
# dbWriteTable(con2, c(table_schema, table_name), ppl, overwrite = FALSE, row.names = FALSE)


# data cleaning
ppl <- ppl %>%
  mutate(cntyname = gsub("^(.*?),.*", "\\1", ppl$name)) %>% # get county name
  filter(substr(geoid, start = 1, stop = 2) == "06") %>% # keep only CA counties
  select(geoid, cntyname, starts_with("T9"), geolevel) # drop unneeded cols
View(ppl)
# make longer
ppl <- pivot_longer(ppl, cols = starts_with("T9"), names_to = "Column Name", 
                    values_to = "housingunits")

# create a separate field to join.

ppl$number <- substring(ppl$`Column Name`, first = 7)
dict$number <- substring(dict$`Column Name`, first = 7)
#Move 'number' field to first column
dict <- select(dict, number, everything())

# join race and cost burden information from data dictionary and recode
ppl <-ppl %>% 
  left_join(dict[,c(1,4:6)], by = c("number")) %>%
  mutate(`Race/ethnicity` = recode(`Race/ethnicity`, 
                                   "Black or African-American alone, non-Hispanic" = "nh_black",
                                   "Asian alone, non-Hispanic" = "nh_asian",
                                   "American Indian or Alaska Native alone, non-Hispanic" = "nh_aian",
                                   "Pacific Islander alone, non-Hispanic" = "nh_pacisl",
                                   "Hispanic, any race" = "latino",
                                   "White alone, non-Hispanic" = "nh_white",
                                   "other (including multiple races, non-Hispanic)" = "nh_other"),
         `Cost burden` = recode(`Cost burden`,
                                "greater than 30% but less than or equal to 50%" = "30.50", 
                                "greater than 50%" = "50.100",
                                "not computed (no/negative income)" = "not_computed",
                                "less than or equal to 30%" = "0.30")) %>%
  rename(race = `Race/ethnicity`, burden = `Cost burden`)



######################Begin Analysis######################

# filter what you want here.

ppl <- filter(ppl, burden %in% c("0.30", "30.50", "50.100"), # exclude records where burden = not_computed
              !(race %in% c("All"))) 

ppl$cost_burdened <- ifelse(ppl$burden == "0.30", 0, 1) #set your definition of cost burden here

# we will need moe later.
moe <- filter(ppl, substring(`Column Name`, 4, 6) == "moe")

ppl <- filter(ppl, !str_detect(ppl$`Column Name`, 'moe'))

### calc by race


## calculate raw number first
costburden_race <-
  ppl %>%
  # filter(!is.na(cost_burdened)) %>%
  group_by(geoid, cntyname, race, cost_burdened, Tenure, geolevel) %>%  
  summarise(
    raw = sum(housingunits)) %>%       
  
  left_join(ppl %>%                                                   
              group_by(geoid, cntyname, race, Tenure, geolevel) %>%                                  
              summarise(pop = sum(housingunits)))
View(costburden_race)
## calculate rate moe
costburden_moe <-
  moe %>%
  group_by(geoid, cntyname, race, cost_burdened, Tenure, geolevel) %>%  
  summarise(
    num_moe = norm(housingunits, type = "2")) %>%       
  
  left_join(moe %>%                                                   
              group_by(geoid, cntyname, race, Tenure, geolevel) %>%                                  
              summarise(den_moe = norm(housingunits, type = "2")))

## put it all together
costburden_race <- costburden_race %>% 
  merge(costburden_moe) %>%
  mutate(rate = raw/pop * 100,                                      
         rate_moe = moe_prop(raw, pop, num_moe, den_moe) * 100,   
         rate_cv = ifelse(rate == 0, NA, (rate_moe/1.645)/rate * 100)) 

### calc by total

## calculate proportions first
costburden_tot <-
  ppl %>%
  group_by(geoid, cntyname, cost_burdened, Tenure, geolevel) %>%  
  summarise(
    raw = sum(housingunits)) %>%       
  
  left_join(ppl %>%                                                   
              group_by(geoid, cntyname, Tenure, geolevel) %>%                                  
              summarise(pop = sum(housingunits)))

## calculate rate moe
costburden_moe_tot <-
  moe %>%
  group_by(geoid, cost_burdened, cntyname, Tenure, geolevel) %>%  
  summarise(
    num_moe = norm(housingunits, type = "2")) %>%       
  
  left_join(moe %>%                                                   
              group_by(geoid, cntyname, Tenure, geolevel) %>%                                  
              summarise(den_moe = norm(housingunits, type = "2")))

## put it all together
costburden_tot <- costburden_tot %>% 
  merge(costburden_moe_tot) %>%
  mutate(race = "total",
         rate = raw/pop * 100,                                      
         rate_moe = moe_prop(raw, pop, num_moe, den_moe) * 100,   
         rate_cv = ifelse(rate == 0, NA, (rate_moe/1.645)/rate * 100)) 

######## Prepare data for RACE COUNTS

## merge datasets
cost_burden_county <- bind_rows(costburden_race, costburden_tot)

### get the count of non-NA values
cost_burden_county <- 
  cost_burden_county %>% 
  
  # filter only records for housing units with cost burden: change if need be
  filter(cost_burdened == 1) %>%  
  
  # Group by county
  group_by(geoid, cntyname) %>% 
  
  as.data.frame()

cost_burden_county$Tenure <- ifelse(cost_burden_county$Tenure == "Owner occupied",
                                   "owner", "renter")

# convert long format to wide
cost_burden_county_rc <- 
  
  cost_burden_county %>% 
  
  
  # convert to wide format
  pivot_wider(id_cols = c(geoid, cntyname, Tenure, geolevel),
              names_from = c(race),
              values_from = c("raw", "pop", "rate", "rate_moe", "rate_cv"),
              names_glue = "{race}_{.value}")%>% 
              dplyr::rename("geoname" = "cntyname") %>% as.data.frame()
# View(cost_burden_county_rc)

## Screen out values with high CVs and small populations

# set thresholds 
cv_threshold <- 35
pop_threshold <- 100


df <- cost_burden_county_rc
df$geoname <- gsub(" County", "", df$geoname)
# Convert Nan and Inf values to NA
df[sapply(df, is.nan)] <- NA
df[sapply(df, is.infinite)] <- NA

#Screen data: Convert rate to NA if its greater than the cv_threshold or less than the pop_threshold
df$total_rate <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_rate))
df$nh_asian_rate <- ifelse(df$nh_asian_rate_cv > cv_threshold, NA, ifelse(df$nh_asian_pop < pop_threshold, NA, df$nh_asian_rate))
df$nh_black_rate <- ifelse(df$nh_black_rate_cv > cv_threshold, NA, ifelse(df$nh_black_pop < pop_threshold, NA, df$nh_black_rate))
df$nh_white_rate <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_rate))
df$latino_rate <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_rate))
df$nh_other_rate <- ifelse(df$nh_other_rate_cv > cv_threshold, NA, ifelse(df$nh_other_pop < pop_threshold, NA, df$nh_other_rate))
df$nh_pacisl_rate <- ifelse(df$nh_pacisl_rate_cv > cv_threshold, NA, ifelse(df$nh_pacisl_pop < pop_threshold, NA, df$nh_pacisl_rate))
df$nh_aian_rate <- ifelse(df$nh_aian_rate_cv > cv_threshold, NA, ifelse(df$nh_aian_pop < pop_threshold, NA, df$nh_aian_rate))
df$total_raw <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_raw))
df$nh_asian_raw <- ifelse(df$nh_asian_rate_cv > cv_threshold, NA, ifelse(df$nh_asian_pop < pop_threshold, NA, df$nh_asian_raw))
df$nh_black_raw <- ifelse(df$nh_black_rate_cv > cv_threshold, NA, ifelse(df$nh_black_pop < pop_threshold, NA, df$nh_black_raw))
df$nh_white_raw <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_raw))
df$latino_raw <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_raw))
df$nh_other_raw <- ifelse(df$nh_other_rate_cv > cv_threshold, NA, ifelse(df$nh_other_pop < pop_threshold, NA, df$nh_other_raw))
df$nh_pacisl_raw <- ifelse(df$nh_pacisl_rate_cv > cv_threshold, NA, ifelse(df$nh_pacisl_pop < pop_threshold, NA, df$nh_pacisl_raw))
df$nh_aian_raw <- ifelse(df$nh_aian_rate_cv > cv_threshold, NA, ifelse(df$nh_aian_pop < pop_threshold, NA, df$nh_aian_raw))

df <- df %>% relocate(ends_with("_raw"), .after = ends_with("_pop")) # reorder fields so raw/rate cols are next to each other

#Create an owners dataframe so that it creates two sets of graphs for the RC_Functions for each owners and renters
owners <- filter(df, Tenure == "owner")


############################# Owners #################################################################
# assign d so that it runs the calculations with owners data
d <- owners

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")%>% select(-c(geolevel, Tenure))
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ]


#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")%>% select(-c(geolevel, Tenure))
View(county_table)

#remove county/state from place table -----
city_table <- d[d$geolevel == 'city', ]

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% 
  dplyr::rename("city_id" = "geoid", "city_name" = "geoname")%>% select(-c(geolevel, Tenure))
# Clean geo names
city_table$city_name <- gsub(" city", "", city_table$city_name)
city_table$city_name <- gsub(" town", "", city_table$city_name)
city_table$city_name <- gsub(" CDP", "", city_table$city_name)
city_table$city_name <- gsub(" City", "", city_table$city_name)
View(city_table)

###update info for postgres tables###
county_table_name <- "arei_hous_cost_burden_owner_county_2023"
state_table_name <- "arei_hous_cost_burden_owner_state_2023"

city_table_name <- "arei_hous_cost_burden_owner_city_2023"
rc_schema <- "v5"

indicator <- "The percentage of owner-occupied housing units experiencing cost burden (Monthly housing costs, including utilities, exceeding 30% of monthly income. White, Black, Asian, AIAN, and PacIsl one race alone and Latinx-exclusive. Other includes other race and two or more races, and is Latinx-exclusive. This data is"
source <- "HUD CHAS (2014-2018) https://www.huduser.gov/portal/datasets/cp.html#2006-2018_data"

# #send tables to postgres
# to_postgres()

city_to_postgres()
################### Renters ################################################################
#Create a renters dataframe by filtering out owners so that it creates two sets of graphs for the RC_Functions for each owners and renters

renters <- filter(df, Tenure == "renter")

#reassign d so that it runs the same calculations with renters data
d <- renters

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")%>% select(-c(geolevel, Tenure))
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>% select(-c(geolevel, Tenure))
View(county_table)

#remove county/state from place table -----
city_table <- d[d$geolevel == 'city', ]

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% 
  dplyr::rename("city_id" = "geoid", "city_name" = "geoname")%>% select(-c(geolevel, Tenure))
# Clean geo names
city_table$city_name <- gsub(" city", "", city_table$city_name)
city_table$city_name <- gsub(" town", "", city_table$city_name)
city_table$city_name <- gsub(" CDP", "", city_table$city_name)
city_table$city_name <- gsub(" City", "", city_table$city_name)
View(city_table)

###update info for postgres tables###
county_table_name <- "arei_hous_cost_burden_renter_county_2023"
state_table_name <- "arei_hous_cost_burden_renter_state_2023"

city_table_name <- "arei_hous_cost_burden_renter_city_2023"
rc_schema <- "v5"

indicator <- "The percentage of rented housing units experiencing cost burden (Monthly housing costs, including utilities, exceeding 30% of monthly income. White, Black, Asian, AIAN, and PacIsl one race alone and Latinx-exclusive. Other includes other race and two or more races, and is Latinx-exclusive. This data is"
source <- "HUD CHAS (2014-2018) https://www.huduser.gov/portal/datasets/cp.html#2006-2018_data"

# #send tables to postgres
# to_postgres()


city_to_postgres()
