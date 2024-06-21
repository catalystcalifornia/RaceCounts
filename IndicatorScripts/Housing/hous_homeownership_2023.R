## Homeownership for RC v5 ##
#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","RPostgreSQL","tidycensus", "rvest", "tidyverse", "stringr", "usethis", "sf", "tigris")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(tidyr)
library(stringr)
library(tidycensus)
library(dplyr)
library(DBI)
library(RPostgreSQL)
library(sf)
library(tigris)
library(dplyr)
library(usethis)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# API Call Info - Change each year
yr = 2021
srvy = "acs5"

############## UPDATE FOR SPECIFIC INDICATOR HERE ##############

table_code = "B25003"     # YOU MUST UPDATE based on most recent Indicator Methodology or Workflow/Cnty-State Indicator Tracking
dataset = "acs5"      # YOU MUST UPDATE: "acs5/subject" for subject ("S") tables OR "acs5/profile" for data profile ("DP") tables OR "acs5" for detailed ("B") tables
cv_threshold = 40         # YOU MUST UPDATE based on most recent Indicator Methodology
pop_threshold = 100        # YOU MUST UPDATE based on most recent Indicator Methodology or set to NA B19301
asbest = 'max'            # YOU MUST UPDATE based on indicator, set to 'min' if S2701



############## PRE-CALCULATION DATA PREP ##############

possible_vars <- list("DP05" = c("DP05_0033", "DP05_0033P","DP05_0071", "DP05_0071P", "DP05_0077", "DP05_0077P", "DP05_0078", "DP05_0078P", "DP05_0066", "DP05_0066P", "DP05_0080", "DP05_0080P", "DP05_0068", "DP05_0068P", "DP05_0082", "DP05_0082P", "DP05_0083", "DP05_0083P"),
                      "S2701"= c("S2701_C01_001", "S2701_C01_017", "S2701_C01_018", "S2701_C01_019","S2701_C01_020","S2701_C01_021", "S2701_C01_022", "S2701_C01_023", "S2701_C01_024", "S2701_C05_001", "S2701_C05_017", "S2701_C05_018", "S2701_C05_019","S2701_C05_020","S2701_C05_021", "S2701_C05_022", "S2701_C05_023", "S2701_C05_024", "S2701_C04_001", "S2701_C04_017", "S2701_C04_018", "S2701_C04_019","S2701_C04_020","S2701_C04_021", "S2701_C04_022", "S2701_C04_023", "S2701_C04_024"),
                      "S2802"= c("S2802_C01_001", "S2802_C01_006", "S2802_C01_007", "S2802_C01_008","S2802_C01_009","S2802_C01_010", "S2802_C01_011", "S2802_C01_012", "S2802_C01_013", "S2802_C03_001", "S2802_C03_006", "S2802_C03_007", "S2802_C03_008","S2802_C03_009","S2802_C03_010", "S2802_C03_011", "S2802_C03_012", "S2802_C03_013", "S2802_C02_001", "S2802_C02_006", "S2802_C02_007", "S2802_C02_008","S2802_C02_009","S2802_C02_010", "S2802_C02_011", "S2802_C02_012", "S2802_C02_013"),
                      "S2301"= c("S2301_C01_001", "S2301_C01_013", "S2301_C01_014", "S2301_C01_015","S2301_C01_016","S2301_C01_017", "S2301_C01_018", "S2301_C01_019", "S2301_C01_020", "S2301_C03_001", "S2301_C03_013", "S2301_C03_014", "S2301_C03_015","S2301_C03_016","S2301_C03_017", "S2301_C03_018", "S2301_C03_019", "S2301_C03_020"),
                      "B19301"= c("B19301_001", "B19301B_001", "B19301C_001", "B19301D_001", "B19301E_001", "B19301F_001", "B19301G_001", "B19301H_001", "B19301I_001"),
                      "B25003"= c("B25003_001", "B25003_002", "B25003B_001", "B25003B_002", "B25003C_001", "B25003C_002", "B25003D_001", "B25003D_002", "B25003E_001", "B25003E_002", "B25003F_001", "B25003F_002", "B25003G_001", "B25003G_002", "B25003H_001", "B25003H_002", "B25003I_001", "B25003I_002"))

vars_list <- possible_vars[table_code][[1]]

# Confirm variables are correct
acs_vars <- load_variables(year = yr, dataset = dataset, cache = TRUE)
df_metadata <- subset(acs_vars, acs_vars$name %in% vars_list)

# Pull data from Census API
# get array of zctas in CA ---- 
state_shp <- states(year = yr, cb = FALSE) %>% filter(, NAME == 'California') # non-cbf shape to match zctas
zcta_shp <- zctas(year = yr, cb = FALSE) %>% filter(substr(GEOID20,1,1)=="9") # non-cbf bc 2021 cbf zctas are not available yet, also not avail. by state. filter for zctas that start with '9' bc it makes processing faster and CA zctas start with 9.
zcta_shp$area <- st_area(zcta_shp)
zcta_shp <- st_intersection(zcta_shp, state_shp) %>%   # clip zctas to state shape aka create our own CA-only zcta shape
             dplyr::select(c("GEOID20", "area", "geometry"))  # keep only needed columns
zcta_shp$new_area <- st_area(zcta_shp) # calc area of zcta that intersects with CA
zcta_shp$pct_area <- (zcta_shp$new_area / zcta_shp$area) * 100 # calc % of zcta that intersects with CA to check we are only including zcta's that are mainly in CA

list_ca_zctas <- zcta_shp$GEOID20

df <- do.call(rbind.data.frame, list(
  get_acs(geography = "state", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE)
  %>% mutate(geolevel = "state"),
  get_acs(geography = "county", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE)
  %>% mutate(geolevel = "county"),
  get_acs(geography = "place", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE)
  %>% mutate(geolevel = "place"),
  get_acs(geography = "puma", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE)
  %>% mutate(geolevel = "puma"),
  get_acs(geography = "tract", state = "CA", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE)
  %>% mutate(geolevel = "tract"),
  get_acs(geography = "zcta", variables = vars_list, year = yr, survey = srvy, cache_table = TRUE) 
  %>% mutate(geolevel = "zcta") %>% 
    filter(GEOID %in% list_ca_zctas)) 
)

# Rename estimate and moe columns to e and m, respectively
df <- df %>%
  rename(e = estimate, m = moe)

# Spread! Switch to wide format.
df_wide_multigeo <- df %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

# renaming rules will change depending on type of census table
if (startsWith(table_code, "B")) {
  total_table_code = paste(table_code, "_", sep="")
  table_b_code = paste(table_code, "B_", sep="")
  table_c_code = paste(table_code, "C_", sep="")
  table_d_code = paste(table_code, "D_", sep="")
  table_e_code = paste(table_code, "E_", sep="")
  table_f_code = paste(table_code, "F_", sep="")
  table_g_code = paste(table_code, "G_", sep="")
  table_h_code = paste(table_code, "H_", sep="")
  table_i_code = paste(table_code, "I_", sep="")
  
  names(df_wide_multigeo) <- gsub(total_table_code, "total", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_b_code, "black", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_c_code, "aian", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_d_code, "asian", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_e_code, "pacisl", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_f_code, "other", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_g_code, "twoormor", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_h_code, "nh_white", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub(table_i_code, "latino", names(df_wide_multigeo))
}

if(endsWith(table_code, "B19301")) {
  
  names(df_wide_multigeo) <- gsub("001e", "_rate", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("001m", "_rate_moe", names(df_wide_multigeo))
}

if(endsWith(table_code, "B25003")) {
  
  names(df_wide_multigeo) <- gsub("001e", "_pop", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("001m", "_pop_moe", names(df_wide_multigeo))
  
  names(df_wide_multigeo) <- gsub("002e", "_raw", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("002m", "_raw_moe", names(df_wide_multigeo))
  
  # [race]_rate  ### UPDATE _POP == 0 TO _POP <= 0
  df_wide_multigeo$total_rate <- ifelse(df_wide_multigeo$total_pop <= 0, NA, df_wide_multigeo$total_raw/df_wide_multigeo$total_pop*100)
  df_wide_multigeo$asian_rate <- ifelse(df_wide_multigeo$asian_pop <= 0, NA, df_wide_multigeo$asian_raw/df_wide_multigeo$asian_pop*100)
  df_wide_multigeo$black_rate <- ifelse(df_wide_multigeo$black_pop <= 0, NA, df_wide_multigeo$black_raw/df_wide_multigeo$black_pop*100)
  df_wide_multigeo$nh_white_rate <- ifelse(df_wide_multigeo$nh_white_pop <= 0, NA, df_wide_multigeo$nh_white_raw/df_wide_multigeo$nh_white_pop*100)
  df_wide_multigeo$latino_rate <- ifelse(df_wide_multigeo$latino_pop <= 0, NA, df_wide_multigeo$latino_raw/df_wide_multigeo$latino_pop*100)
  df_wide_multigeo$other_rate <- ifelse(df_wide_multigeo$other_pop <= 0, NA, df_wide_multigeo$other_raw/df_wide_multigeo$other_pop*100)
  df_wide_multigeo$pacisl_rate <- ifelse(df_wide_multigeo$pacisl_pop <= 0, NA, df_wide_multigeo$pacisl_raw/df_wide_multigeo$pacisl_pop*100)
  df_wide_multigeo$twoormor_rate <- ifelse(df_wide_multigeo$twoormor_pop <= 0, NA, df_wide_multigeo$twoormor_raw/df_wide_multigeo$twoormor_pop*100)
  df_wide_multigeo$aian_rate <- ifelse(df_wide_multigeo$aian_pop <= 0, NA, df_wide_multigeo$aian_raw/df_wide_multigeo$aian_pop*100)
  
  # [race]_rate_moe
  df_wide_multigeo$total_rate_moe <- moe_prop(df_wide_multigeo$total_raw,
                                              df_wide_multigeo$total_pop,
                                              df_wide_multigeo$total_raw_moe,
                                              df_wide_multigeo$total_pop_moe)*100
  
  df_wide_multigeo$asian_rate_moe <- moe_prop(df_wide_multigeo$asian_raw,
                                              df_wide_multigeo$asian_pop,
                                              df_wide_multigeo$asian_raw_moe,
                                              df_wide_multigeo$asian_pop_moe)*100
  
  df_wide_multigeo$black_rate_moe <- moe_prop(df_wide_multigeo$black_raw,
                                              df_wide_multigeo$black_pop,
                                              df_wide_multigeo$black_raw_moe,
                                              df_wide_multigeo$black_pop_moe)*100  
  
  df_wide_multigeo$nh_white_rate_moe <- moe_prop(df_wide_multigeo$nh_white_raw,
                                                 df_wide_multigeo$nh_white_pop,
                                                 df_wide_multigeo$nh_white_raw_moe,
                                                 df_wide_multigeo$nh_white_pop_moe)*100
  
  df_wide_multigeo$latino_rate_moe <- moe_prop(df_wide_multigeo$latino_raw,
                                               df_wide_multigeo$latino_pop,
                                               df_wide_multigeo$latino_raw_moe,
                                               df_wide_multigeo$latino_pop_moe)*100
  
  df_wide_multigeo$other_rate_moe <- moe_prop(df_wide_multigeo$other_raw,
                                              df_wide_multigeo$other_pop,
                                              df_wide_multigeo$other_raw_moe,
                                              df_wide_multigeo$other_pop_moe)*100
  
  df_wide_multigeo$pacisl_rate_moe <- moe_prop(df_wide_multigeo$pacisl_raw,
                                               df_wide_multigeo$pacisl_pop,
                                               df_wide_multigeo$pacisl_raw_moe,
                                               df_wide_multigeo$pacisl_pop_moe)*100
  
  df_wide_multigeo$twoormor_rate_moe <- moe_prop(df_wide_multigeo$twoormor_raw,
                                                 df_wide_multigeo$twoormor_pop,
                                                 df_wide_multigeo$twoormor_raw_moe,
                                                 df_wide_multigeo$twoormor_pop_moe)*100
  
  df_wide_multigeo$aian_rate_moe <- moe_prop(df_wide_multigeo$aian_raw,
                                             df_wide_multigeo$aian_pop,
                                             df_wide_multigeo$aian_raw_moe,
                                             df_wide_multigeo$aian_pop_moe)*100
  
}

if(endsWith(table_code, "B25014")) {
  
  names(df_wide_multigeo) <- gsub("001e", "_pop", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("001m", "_pop_moe", names(df_wide_multigeo))
  
  names(df_wide_multigeo) <- gsub("006e", "_raw_owner_1", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("006m", "_raw_owner_moe_1", names(df_wide_multigeo))
  
  names(df_wide_multigeo) <- gsub("007e", "_raw_owner_2", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("007m", "_raw_owner_moe_2", names(df_wide_multigeo))
  
  names(df_wide_multigeo) <- gsub("012e", "_raw_renter_1", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("012m", "_raw_renter_moe_1", names(df_wide_multigeo))
  
  names(df_wide_multigeo) <- gsub("013e", "_raw_renter_2", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("013m", "_raw_renter_moe_2", names(df_wide_multigeo))
  
  # For Overcrowded housing need to aggregate the owner and renter raw values
  df_wide_multigeo$total_raw <- df_wide_multigeo$total_raw_owner_1 + df_wide_multigeo$total_raw_owner_2 +df_wide_multigeo$total_raw_renter_1 + df_wide_multigeo$total_raw_renter_2
  df_wide_multigeo$asian_raw <- df_wide_multigeo$asian_raw_owner_1 + df_wide_multigeo$asian_raw_owner_2 +df_wide_multigeo$asian_raw_renter_1 + df_wide_multigeo$asian_raw_renter_2
  df_wide_multigeo$black_raw <- df_wide_multigeo$black_raw_owner_1 + df_wide_multigeo$black_raw_owner_2 +df_wide_multigeo$black_raw_renter_1 + df_wide_multigeo$black_raw_renter_2
  df_wide_multigeo$nh_white_raw <- df_wide_multigeo$nh_white_raw_owner_1 + df_wide_multigeo$nh_white_raw_owner_2 +df_wide_multigeo$nh_white_raw_renter_1 + df_wide_multigeo$nh_white_raw_renter_2
  df_wide_multigeo$latino_raw <- df_wide_multigeo$latino_raw_owner_1 + df_wide_multigeo$latino_raw_owner_2 +df_wide_multigeo$latino_raw_renter_1 + df_wide_multigeo$latino_raw_renter_2
  df_wide_multigeo$other_raw <- df_wide_multigeo$other_raw_owner_1 + df_wide_multigeo$other_raw_owner_2 +df_wide_multigeo$other_raw_renter_1 + df_wide_multigeo$other_raw_renter_2
  df_wide_multigeo$pacisl_raw <- df_wide_multigeo$pacisl_raw_owner_1 + df_wide_multigeo$pacisl_raw_owner_2 +df_wide_multigeo$pacisl_raw_renter_1 + df_wide_multigeo$pacisl_raw_renter_2
  df_wide_multigeo$twoormor_raw <- df_wide_multigeo$twoormor_raw_owner_1 + df_wide_multigeo$twoormor_raw_owner_2 +df_wide_multigeo$twoormor_raw_renter_1 + df_wide_multigeo$twoormor_raw_renter_2
  df_wide_multigeo$aian_raw <- df_wide_multigeo$aian_raw_owner_1 + df_wide_multigeo$aian_raw_owner_2 +df_wide_multigeo$aian_raw_renter_1 + df_wide_multigeo$aian_raw_renter_2
  
  # Because we've aggregated these values, will need to calculate a new moe
  # Formula: sqrt(moe1^2 + moe2^2 + moe3^2 + moe4^2)
  df_wide_multigeo$total_raw_moe <- sqrt(total_raw_owner_moe_1^2 + total_raw_owner_moe_2^2 + total_raw_renter_moe_1^2 + total_raw_renter_moe_2^2)
  df_wide_multigeo$asian_raw_moe <- sqrt(asian_raw_owner_moe_1^2 + asian_raw_owner_moe_2^2 + asian_raw_renter_moe_1^2 + asian_raw_renter_moe_2^2)
  df_wide_multigeo$black_raw_moe <- sqrt(black_raw_owner_moe_1^2 + black_raw_owner_moe_2^2 + black_raw_renter_moe_1^2 + black_raw_renter_moe_2^2)
  df_wide_multigeo$nh_white_raw_moe <- sqrt(nh_white_raw_owner_moe_1^2 + nh_white_raw_owner_moe_2^2 + nh_white_raw_renter_moe_1^2 + nh_white_raw_renter_moe_2^2)
  df_wide_multigeo$latino_raw_moe <- sqrt(latino_raw_owner_moe_1^2 + latino_raw_owner_moe_2^2 + latino_raw_renter_moe_1^2 + latino_raw_renter_moe_2^2)
  df_wide_multigeo$other_raw_moe <- sqrt(other_raw_owner_moe_1^2 + other_raw_owner_moe_2^2 + other_raw_renter_moe_1^2 + other_raw_renter_moe_2^2)
  df_wide_multigeo$pacisl_raw_moe <- sqrt(pacisl_raw_owner_moe_1^2 + pacisl_raw_owner_moe_2^2 + pacisl_raw_renter_moe_1^2 + pacisl_raw_renter_moe_2^2)
  df_wide_multigeo$twoormor_raw_moe <- sqrt(twoormor_raw_owner_moe_1^2 + twoormor_raw_owner_moe_2^2 + twoormor_raw_renter_moe_1^2 + twoormor_raw_renter_moe_2^2)
  df_wide_multigeo$aian_raw_moe <- sqrt(aian_raw_owner_moe_1^2 + aian_raw_owner_moe_2^2 + aian_raw_renter_moe_1^2 + total_raw_renter_moe_2^2)
}

if (startsWith(table_code, "S2802") | startsWith(table_code, "S2701")) {
  old_names <- colnames(df_wide_multigeo)[-(1:3)]
  new_names <- c("total_pop", "black_pop", "aian_pop", "asian_pop", "pacisl_pop",
                 "other_pop", "twoormor_pop", "latino_pop", "nh_white_pop", "total_rate", 
                 "black_rate", "aian_rate", "asian_rate", "pacisl_rate", "other_rate", 
                 "twoormor_rate", "latino_rate", "nh_white_rate", "total_raw", 
                 "black_raw", "aian_raw", "asian_raw", "pacisl_raw", "other_raw", 
                 "twoormor_raw", "latino_raw", "nh_white_raw", "total_pop_moe", 
                 "black_pop_moe", "aian_pop_moe", "asian_pop_moe", "pacisl_pop_moe", 
                 "other_pop_moe", "twoormor_pop_moe", "latino_pop_moe", "nh_white_pop_moe", 
                 "total_rate_moe", "black_rate_moe", "aian_rate_moe", "asian_rate_moe", 
                 "pacisl_rate_moe", "other_rate_moe", "twoormor_rate_moe", "latino_rate_moe", 
                 "nh_white_rate_moe", "total_raw_moe", "black_raw_moe", "aian_raw_moe", "asian_raw_moe", 
                 "pacisl_raw_moe", "other_raw_moe", "twoormor_raw_moe", "latino_raw_moe", 
                 "nh_white_raw_moe")
  df_wide_multigeo <- df_wide_multigeo %>%
    rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
}

if (startsWith(table_code, "S2301")) {
  old_names <- colnames(df_wide_multigeo)[-(1:3)]
  new_names <- c("total_pop", "black_pop", "aian_pop", "asian_pop", "pacisl_pop",
                 "other_pop", "twoormor_pop", "latino_pop", "nh_white_pop", "total_rate", 
                 "black_rate", "aian_rate", "asian_rate", "pacisl_rate", "other_rate", 
                 "twoormor_rate", "latino_rate", "nh_white_rate", "total_pop_moe", 
                 "black_pop_moe", "aian_pop_moe", "asian_pop_moe", "pacisl_pop_moe", 
                 "other_pop_moe", "twoormor_pop_moe", "latino_pop_moe", "nh_white_pop_moe", 
                 "total_rate_moe", "black_rate_moe", "aian_rate_moe", "asian_rate_moe", 
                 "pacisl_rate_moe", "other_rate_moe", "twoormor_rate_moe", "latino_rate_moe", 
                 "nh_white_rate_moe")
  df_wide_multigeo <- df_wide_multigeo %>%
    rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
  
  # Employment data doesn't include _raw or _raw_moe values - adding here using _pop * _rate columns
  df_wide_multigeo$total_raw <- ifelse(df$total_pop <= 0, df$total_pop*df$total_rate/100, NA)
  df_wide_multigeo$asian_raw <- ifelse(df$asian_pop <= 0, df$asian_pop*df$asian_rate/100, NA)
  df_wide_multigeo$black_raw <- ifelse(df$black_pop <= 0, df$black_pop*df$black_rate/100, NA)
  df_wide_multigeo$nh_white_raw <- ifelse(df$nh_white_pop <= 0, df$nh_white_pop*df$nh_white_rate/100, NA)
  df_wide_multigeo$latino_raw <- ifelse(df$latino_pop <= 0, df$latino_pop*df$latino_rate/100, NA)
  df_wide_multigeo$other_raw <- ifelse(df$other_pop <= 0, df$other_pop*df$other_rate/100, NA)
  df_wide_multigeo$pacisl_raw <- ifelse(df$pacisl_pop <= 0, df$pacisl_pop*df$pacisl_rate/100, NA)
  df_wide_multigeo$twoormor_raw <- ifelse(df$twoormor_pop <= 0, df$twoormor_pop*df$twoormor_rate/100, NA)
  df_wide_multigeo$aian_raw <- ifelse(df$aian_pop <= 0, df$aian_pop*df$aian_rate/100, NA)
  
  df_wide_multigeo$total_raw_moe <- sqrt(df_wide_multigeo$total_pop^2 * (df_wide_multigeo$total_rate_moe/100)^2 + (df_wide_multigeo$total_rate/100)^2 * df_wide_multigeo$total_pop_moe^2)
  
  df_wide_multigeo$asian_raw_moe <- sqrt(df_wide_multigeo$asian_pop^2 * (df_wide_multigeo$asian_rate_moe/100)^2 + (df_wide_multigeo$asian_rate/100)^2 * df_wide_multigeo$asian_pop_moe^2)
  
  df_wide_multigeo$black_raw_moe <- sqrt(df_wide_multigeo$black_pop^2 * (df_wide_multigeo$black_rate_moe/100)^2 + (df_wide_multigeo$black_rate/100)^2 * df_wide_multigeo$black_pop_moe^2)
  
  df_wide_multigeo$nh_white_raw_moe <- sqrt(df_wide_multigeo$nh_white_pop^2 * (df_wide_multigeo$nh_white_rate_moe/100)^2 + (df_wide_multigeo$nh_white_rate/100)^2 * df_wide_multigeo$nh_white_pop_moe^2)
  
  df_wide_multigeo$latino_raw_moe <- sqrt(df_wide_multigeo$latino_pop^2 * (df_wide_multigeo$latino_rate_moe/100)^2 + (df_wide_multigeo$latino_rate/100)^2 * df_wide_multigeo$latino_pop_moe^2)
  
  df_wide_multigeo$other_raw_moe <- sqrt(df_wide_multigeo$other_pop^2 * (df_wide_multigeo$other_rate_moe/100)^2 + (df_wide_multigeo$other_rate/100)^2 * df_wide_multigeo$other_pop_moe^2)
  
  df_wide_multigeo$pacisl_raw_moe <- sqrt(df_wide_multigeo$pacisl_pop^2 * (df_wide_multigeo$pacisl_rate_moe/100)^2 + (df_wide_multigeo$pacisl_rate/100)^2 * df_wide_multigeo$pacisl_pop_moe^2)
  
  df_wide_multigeo$twoormor_raw_moe <- sqrt(df_wide_multigeo$twoormor_pop^2 * (df_wide_multigeo$twoormor_rate_moe/100)^2 + (df_wide_multigeo$twoormor_rate/100)^2 * df_wide_multigeo$twoormor_pop_moe^2)
  
  df_wide_multigeo$aian_raw_moe <- sqrt(df_wide_multigeo$aian_pop^2 * (df_wide_multigeo$aian_rate_moe/100)^2 + (df_wide_multigeo$aian_rate/100)^2 * df_wide_multigeo$aian_pop_moe^2)
}

if (startsWith(table_code, "DP05")) {
  old_names <- colnames(df_wide_multigeo)[-(1:3)]
  new_names <- c("total_pop", "total_rate", "aian_pop", "pct_aian_pop", "pacisl_pop", 
                 "pct_pacisl_pop", "latino_pop", "pct_latino_pop", "nh_white_pop", "pct_nh_white_pop", 
                 "black_pop", "pct_black_pop", "asian_pop", "pct_asian_pop", "other_pop", "pct_other_pop", 
                 "twoormor_pop", "pct_twoormor_pop", "total_pop_moe", "pct_total_pop_moe", "aian_pop_moe", "pct_aian_pop_moe", "pacisl_pop_moe", 
                 "pct_pacisl_pop_moe", "latino_pop_moe", "pct_latino_pop_moe", "nh_white_pop_moe", "pct_nh_white_pop_moe", 
                 "black_pop_moe", "pct_black_pop_moe", "asian_pop_moe", "pct_asian_pop_moe", "other_pop_moe", "pct_other_pop_moe", 
                 "twoormor_pop_moe", "pct_twoormor_pop_moe")
  df_wide_multigeo <- df_wide_multigeo %>%
    rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
  
  # drop total_rate - is just a copy of total_pop & drop _moe values (get clarification: aren't included in arei_race_county_2021)
  df_wide_multigeo <- dplyr::select(df_wide_multigeo, -ends_with("_moe"), -ends_with("_rate"))
}


# Finish up data cleaning
# make colnames lower case
colnames(df_wide_multigeo) <- tolower(colnames(df_wide_multigeo))

# Clean geo names
df_wide_multigeo$name <- gsub(", California", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" County", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" city", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" town", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" CDP", "", df_wide_multigeo$name)


############## CV CALCS AND EXPORT TO RDA_SHARED_DATA ##############

df <- df_wide_multigeo

### calc cv's
## Calculate CV values for all rates - store in columns as cv_[race]_rate
if (!is.na(cv_threshold)){
  df$total_rate_cv <- ifelse(df$total_rate==0, NA, df$total_rate_moe/1.645/df$total_rate*100)
  df$asian_rate_cv <- ifelse(df$asian_rate==0, NA, df$asian_rate_moe/1.645/df$asian_rate*100)
  df$black_rate_cv <- ifelse(df$black_rate==0, NA, df$black_rate_moe/1.645/df$black_rate*100)
  df$nh_white_rate_cv <- ifelse(df$nh_white_rate==0, NA, df$nh_white_rate_moe/1.645/df$nh_white_rate*100)
  df$latino_rate_cv <- ifelse(df$latino_rate==0, NA, df$latino_rate_moe/1.645/df$latino_rate*100)
  df$other_rate_cv <- ifelse(df$other_rate==0, NA, df$other_rate_moe/1.645/df$other_rate*100)
  df$pacisl_rate_cv <- ifelse(df$pacisl_rate==0, NA, df$pacisl_rate_moe/1.645/df$pacisl_rate*100)
  df$twoormor_rate_cv <- ifelse(df$twoormor_rate==0, NA, df$twoormor_rate_moe/1.645/df$twoormor_rate*100)
  df$aian_rate_cv <- ifelse(df$aian_rate==0, NA, df$aian_rate_moe/1.645/df$aian_rate*100)
  
} 

## Run function to prep and export rda_shared_data table 
source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
table_schema <- "housing"
table_name <- "acs_5yr_b25003_multigeo_2021"
table_comment_source <- "ACS 2017-2021 5-Year Estimate Table B25003 https://data.census.gov/cedsci/. State, county, place, PUMA, tract, and ZCTA"
# df <- get_acs_data(df, table_schema, table_name, table_comment_source) # function to create and export rda_shared_table to postgres db
View(df)

### NOTE: THIS PIECE DOESN'T WORK YET ###
# # Run function to add column comments
# colcomments <- get_acs_metadata(df_metadata, table_schema, table_name)
# View(colcomments)

############## PRE-CALCULATION POPULATION AND/OR CV CHECKS ##############
if (!is.na(pop_threshold) & is.na(cv_threshold)) {
  # if pop_threshold exists and cv_threshold is NA, do pop check but no CV check (doesn't apply to any at this time, may need to add _raw screens later.)
  ## Screen out low populations
  df$total_rate <- ifelse(df$total_pop < pop_threshold, NA, df$total_rate)
  df$asian_rate <- ifelse(df$asian_pop < pop_threshold, NA, df$asian_rate)
  df$black_rate <- ifelse(df$black_pop < pop_threshold, NA, df$black_rate)
  df$nh_white_rate <- ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_rate)
  df$latino_rate <- ifelse(df$latino_pop < pop_threshold, NA, df$latino_rate)
  df$other_rate <- ifelse(df$other_pop < pop_threshold, NA, df$other_rate)
  df$pacisl_rate <- ifelse(df$pacisl_pop < pop_threshold, NA, df$pacisl_rate)
  df$twoormor_rate <- ifelse(df$twoormor_pop < pop_threshold, NA, df$twoormor_rate)
  df$aian_rate <- ifelse(df$aian_pop < pop_threshold, NA, df$aian_rate)
  
} else if (is.na(pop_threshold) & !is.na(cv_threshold)){
  # if pop_threshold is NA and cv_threshold exists, check cv only (i.e. only B19301). As of now, the only table that uses this does not have _raw values, may need to add _raw screens later.
  ## Screen out rates with high CVs
  df$total_rate <- ifelse(df$total_rate_cv > cv_threshold, NA, df$total_rate)
  df$asian_rate <- ifelse(df$asian_rate_cv > cv_threshold, NA, df$asian_rate)
  df$black_rate <- ifelse(df$black_rate_cv > cv_threshold, NA, df$black_rate)
  df$nh_white_rate <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, df$nh_white_rate)
  df$latino_rate <- ifelse(df$latino_rate_cv > cv_threshold, NA, df$latino_rate)
  df$other_rate <- ifelse(df$other_rate_cv > cv_threshold, NA, df$other_rate)
  df$pacisl_rate <- ifelse(df$pacisl_rate_cv > cv_threshold, NA, df$pacisl_rate)
  df$twoormor_rate <- ifelse(df$twoormor_rate_cv > cv_threshold, NA, df$twoormor_rate)
  df$aian_rate <- ifelse(df$aian_rate_cv > cv_threshold, NA, df$aian_rate)
  
} else if (!is.na(pop_threshold) & !is.na(cv_threshold)){
  # if pop_threshold exists and cv_threshold exists, check population and cv (i.e. B25003, S2301, S2802, S2701, B25014)
  ## Screen out rates with high CVs and low populations
  df$total_rate <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_rate))
  df$asian_rate <- ifelse(df$asian_rate_cv > cv_threshold, NA, ifelse(df$asian_pop < pop_threshold, NA, df$asian_rate))
  df$black_rate <- ifelse(df$black_rate_cv > cv_threshold, NA, ifelse(df$black_pop < pop_threshold, NA, df$black_rate))
  df$nh_white_rate <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_rate))
  df$latino_rate <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_rate))
  df$other_rate <- ifelse(df$other_rate_cv > cv_threshold, NA, ifelse(df$other_pop < pop_threshold, NA, df$other_rate))
  df$pacisl_rate <- ifelse(df$pacisl_rate_cv > cv_threshold, NA, ifelse(df$pacisl_pop < pop_threshold, NA, df$pacisl_rate))
  df$twoormor_rate <- ifelse(df$twoormor_rate_cv > cv_threshold, NA, ifelse(df$twoormor_pop < pop_threshold, NA, df$twoormor_rate))
  df$aian_rate <- ifelse(df$aian_rate_cv > cv_threshold, NA, ifelse(df$aian_pop < pop_threshold, NA, df$aian_rate))
  df$total_raw <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_raw))
  df$asian_raw <- ifelse(df$asian_rate_cv > cv_threshold, NA, ifelse(df$asian_pop < pop_threshold, NA, df$asian_raw))
  df$black_raw <- ifelse(df$black_rate_cv > cv_threshold, NA, ifelse(df$black_pop < pop_threshold, NA, df$black_raw))
  df$nh_white_raw <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_raw))
  df$latino_raw <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_raw))
  df$other_raw <- ifelse(df$other_rate_cv > cv_threshold, NA, ifelse(df$other_pop < pop_threshold, NA, df$other_raw))
  df$pacisl_raw <- ifelse(df$pacisl_rate_cv > cv_threshold, NA, ifelse(df$pacisl_pop < pop_threshold, NA, df$pacisl_raw))
  df$twoormor_raw <- ifelse(df$twoormor_rate_cv > cv_threshold, NA, ifelse(df$twoormor_pop < pop_threshold, NA, df$twoormor_raw))
  df$aian_raw <- ifelse(df$aian_rate_cv > cv_threshold, NA, ifelse(df$aian_pop < pop_threshold, NA, df$aian_raw))  
  
} else {
  # Only DP05 should hit this condition
  # Will use to change population values < 0 to NA (negative values are Census annotations)
  pop_columns <- colnames(dplyr::select(df, ends_with("_pop")))
  df[,pop_columns] <- sapply(df[,pop_columns], function(x) ifelse(x<0, NA, x))
  
}

df <- dplyr::select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"))

############## CALC RACE COUNTS STATS ##############

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")
d <- df[df$geolevel %in% c('state', 'county', 'place'), ]

if (table_code != "DP05") {
  
  
  # Adds asbest value for RC Functions
  d$asbest = asbest
  
  d <- count_values(d) #calculate number of "_rate" values
  d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
  d <- calc_diff(d) # check if 393-396 work as is with updated rc functions
  d <- calc_avg_diff(d)
  d <- calc_s_var(d)
  d <- calc_id(d)
  
  ### Split into geolevel tables
  #split into STATE, COUNTY, CITY tables 
  state_table <- d[d$geolevel == 'state', ]
  county_table <- d[d$geolevel == 'county', ]
  city_table <- d[d$geolevel == 'place', ]
  
  #calculate STATE z-scores
  state_table <- calc_state_z(state_table) %>% dplyr::select(-c(geolevel))
  View(state_table)
  
  #calculate COUNTY z-scores
  county_table <- calc_z(county_table) 
  
  ## Calc county ranks##
  county_table <- calc_ranks(county_table) %>% dplyr::select(-c(geolevel))
  
  View(county_table)
  
  
  # #calculate CITY z-scores
  city_table <- calc_z(city_table)
  # 
  # ## Calc city ranks##
  city_table <- calc_ranks(city_table) %>% dplyr::select(-c(geolevel))
  View(city_table)
  
  #rename geoid to state_id, county_id, city_id
  colnames(state_table)[1:2] <- c("state_id", "state_name")
  colnames(county_table)[1:2] <- c("county_id", "county_name")
  colnames(city_table)[1:2] <- c("city_id", "city_name")
  
  
  ############### NON-DP05 ----- COUNTY, STATE, CITY METADATA  ##############
  
  ###update info for postgres tables###
  county_table_name <- "arei_hous_homeownership_county_2023"      # See most recent RC Workflow SQL Views for table name (remember to update year)
  state_table_name <- "arei_hous_homeownership_state_2023"        # See most recent RC Workflow SQL Views for table name (remember to update year)
  city_table_name <- "arei_hous_homeownership_city_2023"          # See most recent RC Workflow SQL Views for table name (remember to update year)
  indicator <- "Owner-Occupied Housing Units (%)"                 # See most recent Indicator Methodology for indicator description
  source <- "2017-2021 ACS 5-Year Estimates, Tables B25003B-I, https://data.census.gov/cedsci/"   # See most recent Indicator Methodology for source info
  rc_schema <- "v5"
  
} else {
  
  # ############## DPO5 ONLY ----- COUNTY, STATE, CITY METADATA ##############
  county_table <- d
  ###update info for postgres tables###
  county_table_name <- "arei_race_multigeo_2023"      # See most recent RC Workflow SQL Views for table name (remember to update year)
  indicator <- "County and State population by race/ethnicity for RC Place page"        # See most recent Indicator Methodology for indicator description
  source <- "ACS 2017-2021, Table DP05. All AIAN, All NHPI, All Latinx, all other groups are one race alone and non-Latinx."   # See most recent Indicator Methodology for source info
  rc_schema <- "v5"
}

####### SEND TO POSTGRES #######
#to_postgres(county_table,state_table)
#city_to_postgres(city_table)