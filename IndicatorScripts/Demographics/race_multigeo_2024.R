## Pop by race/ethnicity for RC 2024 v6 ###
#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr", "usethis", "sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(tidyr)
library(stringr)
library(tidycensus)
library(dplyr)
library(DBI)
library(RPostgreSQL)
library(usethis)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# Variables - Update each yr
yr = 2022
rc_year <- 2024
rc_schema <- "v6"

##### Get pop data #####
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/acs_rda_shared_tables.R")  # this fx pulls specified table from postgres db, or creates table then pulls it if it does not exist already
filepath <- paste0("W:/Project/RACE COUNTS/",rc_year,"_",rc_schema, "/RC_Github/RaceCounts/IndicatorScripts/Demographics/race_multigeo_", rc_year, ".R")
b04006 <- update_acs(yr, 'B04006', filepath)
list2env(b04006, envir = .GlobalEnv)  # convert list to df

dp05 <- update_acs(yr, 'DP05', filepath)
list2env(dp05, envir = .GlobalEnv)  # convert list to df

b02018 <- update_acs(yr, 'B02018', filepath)
list2env(b02018, envir = .GlobalEnv)  # convert list to df

## Clean and join pop tables
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/SWANA_Ancestry_List.R") # current swana_ancestry() list
vars_list_acs_swana <- get_swana_var(yr, "acs5") # use fx to generate current swana ancestry vars
swana_df <- B04006 %>% select(geoid, geolevel, matches(vars_list_acs_swana)) %>% select(!ends_with("m")) %>% filter(geolevel %in% c('state', 'county', 'place'))
swana_df$swana_pop <- rowSums(swana_df[sapply(swana_df, is.numeric)], na.rm = TRUE) # calc SWANA pop
races_df <- select(DP05, geoid, name, geolevel, dp05_0001e, dp05_0068e, dp05_0068pe,  dp05_0070e, dp05_0070pe, dp05_0073e, dp05_0073pe, dp05_0079e, dp05_0079pe, dp05_0080e, dp05_0080pe, dp05_0082e, dp05_0082pe,  dp05_0084e, dp05_0084pe, dp05_0085e, dp05_0085pe) %>% filter(geolevel %in% c('state', 'county', 'place'))
pop_df <- races_df %>% left_join(swana_df %>% select(geoid, swana_pop), by = c("geoid"))
pop_df <- pop_df %>% mutate(pct_swana_pop = swana_pop / pop_df$dp05_0001e * 100) %>% mutate_all(function(x) ifelse(is.nan(x), NA, x))


vars_list_acs_soasian <- get_soasian_var(yr, "acs5") # use fx to generate current So asian ancestry vars
soasian_df <- B02018 %>% select(geoid, geolevel, matches(vars_list_acs_soasian)) %>% select(!ends_with("m")) %>% filter(geolevel %in% c('state', 'county', 'place'))

soasian_df$soasian_pop <- rowSums(soasian_df[sapply(soasian_df, is.numeric)], na.rm = TRUE) # calc So Asian pop

soasian_pop <- soasian_df %>% select(geoid, geolevel, soasian_pop)

pop_df <- pop_df %>% left_join(soasian_pop) %>% mutate(swanasa_pop = swana_pop + soasian_pop, 
                                                       pct_swanasa_pop = swanasa_pop/pop_df$dp05_0001e * 100) %>% select(-soasian_pop)

# check that DP05 variable names haven't changed by pulling in metadata from API
dp05_vars <- load_variables(yr, "acs5/profile", cache = TRUE) %>% filter(grepl("DP05", name)) %>% mutate(name = tolower(name)) # get all DP05 vars
dp05_vars$label <- gsub("Estimate!!|HISPANIC OR LATINO AND RACE!!", "", dp05_vars$label)

pop_df_cols <- colnames(pop_df) %>% as.data.frame() %>% rename(name = 1)
pop_df_cols$name <- gsub("pe", "p", pop_df_cols$name)
pop_df_cols$name <- ifelse(grepl("dp05", pop_df_cols$name), gsub("e", "", pop_df_cols$name), pop_df_cols$name)
View(pop_df_cols)

pop_df_cols_meta <- pop_df_cols %>% left_join(dp05_vars %>% select(name, label), by = 'name')  # join pop_df colnames with corresponding metadata from API

new_names <- c("geoid", "name", "geolevel", "total_pop", "aian_pop", "pct_aian_pop", "pacisl_pop", "pct_pacisl_pop", "latino_pop", "pct_latino_pop", "nh_white_pop", "pct_nh_white_pop", "nh_black_pop", "pct_nh_black_pop", "nh_asian_pop", "pct_nh_asian_pop", "nh_other_pop", "pct_nh_other_pop", "nh_twoormor_pop", "pct_nh_twoormor_pop", "swana_pop", "pct_swana_pop", "swanasa_pop", "pct_swanasa_pop")
pop_df_cols_meta$new_name <- new_names
View(pop_df_cols_meta)  # check that label and new_name match

colnames(pop_df) <- pop_df_cols_meta$new_name  # update to RC colnames

############### SEND COUNTY, STATE, CITY POP TO POSTGRES ##############

### info for postgres tables automatically updates ###
table_name <- "arei_race_multigeo"    
indicator <- "City, County, and State population by race/ethnicity for RC Place and Race pages"       
start_yr <- yr - 4
source <- paste0("ACS ", start_yr, "-", yr, " Tables DP05, B04006 (SWANA), B02018 (S Asian). All AIAN, All NHPI, All Latinx, All SWANA/SWANA+SA, all other groups are one race alone and non-Latinx") 

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

dbWriteTable(con, c(rc_schema, table_name), pop_df,
             overwrite = FALSE, row.names = FALSE)

#comment on table and columns
comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", table_name,  " IS '", indicator, " from ", source, ".';
                                                                                      COMMENT ON COLUMN v5.", table_name, ".geoid IS 'FIPS code';")
print(comment)            
dbSendQuery(con, comment)

#dbDisconnect(con) 


