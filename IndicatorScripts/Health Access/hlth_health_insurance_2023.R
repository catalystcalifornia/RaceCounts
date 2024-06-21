## Health Insurance for RC v5 ##

#install packages if not already installed
list.of.packages <- c("tidyr", "stringr", "tidycensus", "dplyr", "DBI", "RPostgreSQL", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(tidyr)
library(stringr)
library(tidycensus)
library(dplyr)
library(DBI)
library(RPostgreSQL)
library(usethis)

############## UPDATE DATA YEAR ##############
# API Call Info - Shouldn't need to change until next year
yr = 2021

############## UPDATE FOR SPECIFIC INDICATOR HERE ##############

table_code = "S2701"     # YOU MUST UPDATE based on Indicator Methodology 2021 or RC 2022 Workflow/Cnty-State Indicator Tracking
cv_threshold = 40         # YOU MUST UPDATE based on Indicator Methodology 2021
pop_threshold = 130        # YOU MUST UPDATE based on Indicator Methodology 2021 or set to NA B19301
asbest = 'min'            # YOU MUST UPDATE based on indicator, set to 'min' if S2701

############## DEFINES RELATIONSHIPS BETWEEN CENSUS TABLE CODES AND RDA SHARED TABLE NAMES - DO NOT MODIFY ##############
# schema relationships pulled from: W:\\RDA Team\\R\\ACS Updates\\Update ACS Subject Detail Profile Multigeo Tables.R
rda_shared_schema <- list("DP05" = c("demographics"), # population,
                          "S2701" = c("health"), # health insurance
                          "S2802" = c("economic"), # internet access
                          "S2301" = c("economic"), # employment
                          "B19301" = c("economic"), # per capita income
                          "B25003" = c("housing"), # homeownership
                          "B25014" = c("housing")) # overcrowded housing

############## DEFINES R-REQUIRED COLS - DO NOT MODIFY ##############
rc_vars <- list("DP05" = c("DP05_0033", "DP05_0033P","DP05_0071", "DP05_0071P", "DP05_0077", "DP05_0077P", "DP05_0078", "DP05_0078P", "DP05_0066", "DP05_0066P", "DP05_0080", "DP05_0080P", "DP05_0068", "DP05_0068P", "DP05_0082", "DP05_0082P", "DP05_0083", "DP05_0083P"),

                      "S2701"= c("S2701_C01_001", "S2701_C01_017", "S2701_C01_018", "S2701_C01_019","S2701_C01_020","S2701_C01_021", "S2701_C01_022", "S2701_C01_023", "S2701_C01_024", "S2701_C05_001", "S2701_C05_017", "S2701_C05_018", "S2701_C05_019","S2701_C05_020","S2701_C05_021", "S2701_C05_022", "S2701_C05_023", "S2701_C05_024", "S2701_C04_001", "S2701_C04_017", "S2701_C04_018", "S2701_C04_019","S2701_C04_020","S2701_C04_021", "S2701_C04_022", "S2701_C04_023", "S2701_C04_024"),
                      "S2802"= c("S2802_C01_001", "S2802_C01_006", "S2802_C01_007", "S2802_C01_008","S2802_C01_009","S2802_C01_010", "S2802_C01_011", "S2802_C01_012", "S2802_C01_013", "S2802_C03_001", "S2802_C03_006", "S2802_C03_007", "S2802_C03_008","S2802_C03_009","S2802_C03_010", "S2802_C03_011", "S2802_C03_012", "S2802_C03_013", "S2802_C02_001", "S2802_C02_006", "S2802_C02_007", "S2802_C02_008","S2802_C02_009","S2802_C02_010", "S2802_C02_011", "S2802_C02_012", "S2802_C02_013"),
                      "S2301"= c("S2301_C01_001", "S2301_C01_013", "S2301_C01_014", "S2301_C01_015","S2301_C01_016","S2301_C01_017", "S2301_C01_018", "S2301_C01_019", "S2301_C01_020", "S2301_C03_001", "S2301_C03_013", "S2301_C03_014", "S2301_C03_015","S2301_C03_016","S2301_C03_017", "S2301_C03_018", "S2301_C03_019", "S2301_C03_020"),
                      "B19301"= c("B19301_001", "B19301B_001", "B19301C_001", "B19301D_001", "B19301E_001", "B19301F_001", "B19301G_001", "B19301H_001", "B19301I_001"),
                      "B25003"= c("B25003_001", "B25003_002", "B25003B_001", "B25003B_002", "B25003C_001", "B25003C_002", "B25003D_001", "B25003D_002", "B25003E_001", "B25003E_002", "B25003F_001", "B25003F_002", "B25003G_001", "B25003G_002", "B25003H_001", "B25003H_002", "B25003I_001", "B25003I_002"),
                      "B25014" = c("B25014_001", "B25014_003", "B25014_005", "B25014_006", "B25014_007", "B25014_012", "B25014_013", 
                                   "B25014B_001", "B25014B_003", "B25014B_005", "B25014B_006", "B25014B_007", "B25014B_012", "B25014B_013", 
                                   "B25014C_001", "B25014C_003", "B25014C_005", "B25014C_006", "B25014C_007", "B25014C_012", "B25014C_013", 
                                   "B25014D_001", "B25014D_003", "B25014D_005", "B25014D_006", "B25014D_007", "B25014D_012", "B25014D_013", 
                                   "B25014E_001", "B25014E_003", "B25014E_005", "B25014E_006", "B25014E_007", "B25014E_012", "B25014E_013", 
                                   "B25014F_001", "B25014F_003", "B25014F_005", "B25014F_006", "B25014F_007", "B25014F_012", "B25014F_013", 
                                   "B25014G_001", "B25014G_003", "B25014G_005", "B25014G_006", "B25014G_007", "B25014G_012", "B25014G_013", 
                                   "B25014H_001", "B25014H_003", "B25014H_005", "B25014H_006", "B25014H_007", "B25014H_012", "B25014H_013", 
                                   "B25014I_001", "B25014I_003", "B25014I_005", "B25014I_006", "B25014I_007", "B25014I_012", "B25014I_013"))


ind_rc_vars <- tolower(rc_vars[table_code][[1]])
all_rc_ind_vars <- append(c("geoid", "name", "geolevel"), ind_rc_vars)

##### Pull table from rda_shared #####
source("W:\\RDA Team\\R\\credentials_source.R")
rda_shared_conn <- connect_to_db("rda_shared_data")

### identify rda shared data table name
# rda_shared_data table format: [schema].acs_5yr_[table_code]_multigeo_[yr]
ind_rda_shared_schema <- rda_shared_schema[table_code][[1]]
rda_shared_table <- paste0(ind_rda_shared_schema,
                           ".",
                           "acs_5yr_",
                           tolower(table_code),
                           "_multigeo_",
                           yr)
print(rda_shared_table)

### pull data and filter geolevels so we aren't running any calculations/pivots on unneeded rows
sqlquery <- paste0("SELECT * FROM ", rda_shared_table, ";")
rda_shared_data <- dbGetQuery(rda_shared_conn, sqlquery)

### filter for RC required cols
rc_data_cols <- rda_shared_data %>%
  select(starts_with(all_rc_ind_vars))

### filter for RC required geolevels
geolevels = c("state", "county", "place") # should be the same for all indicators included in this script
rc_data_cols_geos <- rc_data_cols %>% 
  filter(geolevel %in% geolevels)


### set column order for easier renaming (geoid, name, geolevel, alphabetize the rest)
# alphabetize
alpha_col_order <- sort(colnames(rc_data_cols_geos))
rc_data_cols_alpha <- rc_data_cols_geos[, alpha_col_order]

# bring "geoid", "name", "geolevel" to front
rc_data_cols_ordered <- rc_data_cols_alpha %>%
  select(geoid, name, geolevel, everything())

### rename for next section
df_wide_multigeo <- rc_data_cols_ordered

############## PRE-CALCULATION: TABLE-SPECIFIC DATA PREP ##############

# renaming rules will change depending on type of census table
if (startsWith(table_code, "B")) {
  table_code_lower <- tolower(table_code)
  total_table_code = paste(table_code_lower, "_", sep="")
  table_b_code = paste(table_code_lower, "b_", sep="")
  table_c_code = paste(table_code_lower, "c_", sep="")
  table_d_code = paste(table_code_lower, "d_", sep="")
  table_e_code = paste(table_code_lower, "e_", sep="")
  table_f_code = paste(table_code_lower, "f_", sep="")
  table_g_code = paste(table_code_lower, "g_", sep="")
  table_h_code = paste(table_code_lower, "h_", sep="")
  table_i_code = paste(table_code_lower, "i_", sep="")
  
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
                                              df_wide_multigeo$total_pop_moe)
  
  df_wide_multigeo$asian_rate_moe <- moe_prop(df_wide_multigeo$asian_raw,
                                              df_wide_multigeo$asian_pop,
                                              df_wide_multigeo$asian_raw_moe,
                                              df_wide_multigeo$asian_pop_moe)
  
  df_wide_multigeo$black_rate_moe <- moe_prop(df_wide_multigeo$black_raw,
                                              df_wide_multigeo$black_pop,
                                              df_wide_multigeo$black_raw_moe,
                                              df_wide_multigeo$black_pop_moe)  
  
  df_wide_multigeo$nh_white_rate_moe <- moe_prop(df_wide_multigeo$nh_white_raw,
                                                 df_wide_multigeo$nh_white_pop,
                                                 df_wide_multigeo$nh_white_raw_moe,
                                                 df_wide_multigeo$nh_white_pop_moe)
  
  df_wide_multigeo$latino_rate_moe <- moe_prop(df_wide_multigeo$latino_raw,
                                               df_wide_multigeo$latino_pop,
                                               df_wide_multigeo$latino_raw_moe,
                                               df_wide_multigeo$latino_pop_moe)
  
  df_wide_multigeo$other_rate_moe <- moe_prop(df_wide_multigeo$other_raw,
                                              df_wide_multigeo$other_pop,
                                              df_wide_multigeo$other_raw_moe,
                                              df_wide_multigeo$other_pop_moe)
  
  df_wide_multigeo$pacisl_rate_moe <- moe_prop(df_wide_multigeo$pacisl_raw,
                                               df_wide_multigeo$pacisl_pop,
                                               df_wide_multigeo$pacisl_raw_moe,
                                               df_wide_multigeo$pacisl_pop_moe)
  
  df_wide_multigeo$twoormor_rate_moe <- moe_prop(df_wide_multigeo$twoormor_raw,
                                                 df_wide_multigeo$twoormor_pop,
                                                 df_wide_multigeo$twoormor_raw_moe,
                                                 df_wide_multigeo$twoormor_pop_moe)
  
  df_wide_multigeo$aian_rate_moe <- moe_prop(df_wide_multigeo$aian_raw,
                                             df_wide_multigeo$aian_pop,
                                             df_wide_multigeo$aian_raw_moe,
                                             df_wide_multigeo$aian_pop_moe)
  
}

if(endsWith(table_code, "B25014")) {  # Overcrowded Housing #
  ## Occupants per Room
  names(df_wide_multigeo) <- gsub("001e", "_pop", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("001m", "_pop_moe", names(df_wide_multigeo))
  
  names(df_wide_multigeo) <- gsub("003e", "_raw", names(df_wide_multigeo))
  names(df_wide_multigeo) <- gsub("003m", "_raw_moe", names(df_wide_multigeo))
  
  ## total data (more disaggregated than raced values so different prep needed)
  
  ### Extract total values to perform the various calculations needed
  totals <- df_wide_multigeo %>%
    select(geoid, starts_with("total"))
  
  total_e_cols <- totals %>%
    select(matches("[0-9]e$")) %>%
    colnames()
  
  total_m_cols <- totals %>%
    select(matches("[0-9]m$")) %>%
    colnames()
  
  totals <- totals %>% pivot_longer(all_of(total_e_cols), names_to="var_name", values_to = "estimate")
  totals <- totals %>% pivot_longer(all_of(total_m_cols), names_to="var_name2", values_to = "moe")
  totals$var_name <- substr(totals$var_name, 1, nchar(totals$var_name)-1)
  totals$var_name2 <- substr(totals$var_name2, 1, nchar(totals$var_name2)-1)
  totals <- totals[totals$var_name == totals$var_name2, ]
  totals <- select(totals, -c(var_name, var_name2))
  
  ### sum the numerator columns 005e-013e (total_raw):
  total_raw_values <- totals %>%
    select(geoid, estimate) %>%
    group_by(geoid) %>%
    summarise(total_raw = sum(estimate))
  
  #### join these calculations back to df_wide_multigeo
  df_wide_multigeo <- left_join(df_wide_multigeo, total_raw_values, by = "geoid")
  
  ### calculate the total_raw_moe using moe_sum (need to sort MOE values first to make sure highest MOE is used in case of multiple zero estimates)
  ### methodology source is text under table on slide 52 here: https://www.census.gov/content/dam/Census/programs-surveys/acs/guidance/training-presentations/20180418_MOE.pdf
  total_raw_moes <- totals %>%
    select(geoid, estimate, moe) %>%
    group_by(geoid) %>%
    arrange(desc(moe), .by_group = TRUE) %>%
    summarise(total_raw_moe = moe_sum(moe, estimate, na.rm=TRUE))   # https://walker-data.com/tidycensus/reference/moe_sum.html
  
  #### Spot checking moe_sum() -- still need to test that the arrange function is properly sorting moes 
  #### resulting in the right calculation if multiple zero estimates present
  
  #####  test_moe_sum <- totals[1:6, c(1, 4:5)]
  #####  moe_sum(test_moe_sum$moe, test_moe_sum$estimate, na.rm=TRUE)
  ##### [1] 8632.893
  
  #####  norm(test_moe_sum$moe, type ="2")
  #####[1] 8632.893
  
  #### join these calculations back to df_wide_multigeo
  df_wide_multigeo <- left_join(df_wide_multigeo, total_raw_moes, by = "geoid")
  
  ### calculate total_rate
  total_rates <- left_join(total_raw_values, totals[, 1:2])
  total_rates$total_rate <- total_rates$total_raw/total_rates$total_pop*100
  total_rates <- total_rates %>%
    select(geoid, total_rate) %>%
    distinct()
  
  #### join these calculations back to df_wide_multigeo
  df_wide_multigeo <- left_join(df_wide_multigeo, total_rates, by = "geoid")
  
  ### calculate the moe for total_rate
  total_pop_data <- totals %>%
    select(geoid, total_pop, total_pop_moe) %>%
    distinct()
  total_rate_moes <- left_join(total_raw_values, total_raw_moes, by='geoid') %>%
    left_join(., total_pop_data, by='geoid')
  total_rate_moes$total_rate_moe <- moe_prop(total_rate_moes$total_raw,    # https://walker-data.com/tidycensus/reference/moe_prop.html
                                             total_rate_moes$total_pop, 
                                             total_rate_moes$total_raw_moe, 
                                             total_rate_moes$total_pop_moe)
  total_rate_moes <- total_rate_moes %>%
    select(geoid, total_rate_moe)
  
  #### join these calculations back to df_wide_multigeo
  df_wide_multigeo <- left_join(df_wide_multigeo, total_rate_moes, by = "geoid")
  
  
  ## raced data (raw values don't need aggregation like total values do)
  
  ### calculate raced rates
  df_wide_multigeo$asian_rate <- ifelse(df_wide_multigeo$asian_pop <= 0, NA, df_wide_multigeo$asian_raw/df_wide_multigeo$asian_pop*100)
  df_wide_multigeo$black_rate <- ifelse(df_wide_multigeo$black_pop <= 0, NA, df_wide_multigeo$black_raw/df_wide_multigeo$black_pop*100)
  df_wide_multigeo$nh_white_rate <- ifelse(df_wide_multigeo$nh_white_pop <= 0, NA, df_wide_multigeo$nh_white_raw/df_wide_multigeo$nh_white_pop*100)
  df_wide_multigeo$latino_rate <- ifelse(df_wide_multigeo$latino_pop <= 0, NA, df_wide_multigeo$latino_raw/df_wide_multigeo$latino_pop*100)
  df_wide_multigeo$other_rate <- ifelse(df_wide_multigeo$other_pop <= 0, NA, df_wide_multigeo$other_raw/df_wide_multigeo$other_pop*100)
  df_wide_multigeo$pacisl_rate <- ifelse(df_wide_multigeo$pacisl_pop <= 0, NA, df_wide_multigeo$pacisl_raw/df_wide_multigeo$pacisl_pop*100)
  df_wide_multigeo$twoormor_rate <- ifelse(df_wide_multigeo$twoormor_pop <= 0, NA, df_wide_multigeo$twoormor_raw/df_wide_multigeo$twoormor_pop*100)
  df_wide_multigeo$aian_rate <- ifelse(df_wide_multigeo$aian_pop <= 0, NA, df_wide_multigeo$aian_raw/df_wide_multigeo$aian_pop*100)
  
  
  ### calculate moes for raced rates
  df_wide_multigeo$asian_rate_moe <- moe_prop(df_wide_multigeo$asian_raw,
                                              df_wide_multigeo$asian_pop,
                                              df_wide_multigeo$asian_raw_moe,
                                              df_wide_multigeo$asian_pop_moe)
  df_wide_multigeo$black_rate_moe <- moe_prop(df_wide_multigeo$black_raw,
                                              df_wide_multigeo$black_pop,
                                              df_wide_multigeo$black_raw_moe,
                                              df_wide_multigeo$black_pop_moe)
  df_wide_multigeo$nh_white_rate_moe <- moe_prop(df_wide_multigeo$nh_white_raw,
                                                 df_wide_multigeo$nh_white_pop,
                                                 df_wide_multigeo$nh_white_raw_moe,
                                                 df_wide_multigeo$nh_white_pop_moe)
  df_wide_multigeo$latino_rate_moe <- moe_prop(df_wide_multigeo$latino_raw,
                                               df_wide_multigeo$latino_pop,
                                               df_wide_multigeo$latino_raw_moe,
                                               df_wide_multigeo$latino_pop_moe)
  df_wide_multigeo$other_rate_moe <- moe_prop(df_wide_multigeo$other_raw,
                                              df_wide_multigeo$other_pop,
                                              df_wide_multigeo$other_raw_moe,
                                              df_wide_multigeo$other_pop_moe)
  df_wide_multigeo$pacisl_rate_moe <- moe_prop(df_wide_multigeo$pacisl_raw,
                                               df_wide_multigeo$pacisl_pop,
                                               df_wide_multigeo$pacisl_raw_moe,
                                               df_wide_multigeo$pacisl_pop_moe)
  df_wide_multigeo$twoormor_rate_moe <- moe_prop(df_wide_multigeo$twoormor_raw,
                                                 df_wide_multigeo$twoormor_pop,
                                                 df_wide_multigeo$twoormor_raw_moe,
                                                 df_wide_multigeo$twoormor_pop_moe)
  df_wide_multigeo$aian_rate_moe <- moe_prop(df_wide_multigeo$aian_raw,
                                             df_wide_multigeo$aian_pop,
                                             df_wide_multigeo$aian_raw_moe,
                                             df_wide_multigeo$aian_pop_moe)
  
  
  ### Convert any NaN to NA
  df_wide_multigeo <- df_wide_multigeo %>% 
    mutate_all(function(x) ifelse(is.nan(x), NA, x))
  
  ### drop the total006-013 e and m columns and pop_moe cols
  df_wide_multigeo <- df_wide_multigeo %>%
    select(-starts_with("total0"), -ends_with("_pop_moe"))
}

if (startsWith(table_code, "S2802") | startsWith(table_code, "S2701")) {
  old_names <- colnames(df_wide_multigeo)[-(1:3)]
  new_names <- c("total_pop", "total_pop_moe", 
                 "black_pop", "black_pop_moe",   
                 "aian_pop", "aian_pop_moe", 
                 "asian_pop", "asian_pop_moe",  
                 "pacisl_pop", "pacisl_pop_moe",  
                 "other_pop", "other_pop_moe",  
                 "twoormor_pop", "twoormor_pop_moe",  
                 "latino_pop", "latino_pop_moe", 
                 "nh_white_pop", "nh_white_pop_moe", 
                 "total_raw", "total_raw_moe", 
                 "black_raw", "black_raw_moe",
                 "aian_raw", "aian_raw_moe",
                 "asian_raw", "asian_raw_moe",
                 "pacisl_raw", "pacisl_raw_moe",
                 "other_raw", "other_raw_moe",
                 "twoormor_raw", "twoormor_raw_moe",  
                 "latino_raw", "latino_raw_moe",
                 "nh_white_raw", "nh_white_raw_moe",
                 "total_rate", "total_rate_moe",
                 "black_rate", "black_rate_moe",
                 "aian_rate", "aian_rate_moe", 
                 "asian_rate", "asian_rate_moe",
                 "pacisl_rate", "pacisl_rate_moe",
                 "other_rate", "other_rate_moe", 
                 "twoormor_rate", "twoormor_rate_moe",
                 "latino_rate", "latino_rate_moe",
                 "nh_white_rate", "nh_white_rate_moe")

  df_wide_multigeo <- df_wide_multigeo %>%
    rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
}

if (startsWith(table_code, "S2301")) {
  old_names <- colnames(df_wide_multigeo)[-(1:3)]
  new_names <- c("total_pop", "total_pop_moe",
                 "black_pop", "black_pop_moe",
                 "aian_pop", "aian_pop_moe", 
                 "asian_pop", "asian_pop_moe", 
                 "pacisl_pop", "pacisl_pop_moe", 
                 "other_pop", "other_pop_moe",
                 "twoormor_pop", "twoormor_pop_moe", 
                 "latino_pop", "latino_pop_moe", 
                 "nh_white_pop", "nh_white_pop_moe",
                 "total_rate", "total_rate_moe", 
                 "black_rate", "black_rate_moe", 
                 "aian_rate", "aian_rate_moe", 
                 "asian_rate", "asian_rate_moe", 
                 "pacisl_rate", "pacisl_rate_moe", 
                 "other_rate", "other_rate_moe", 
                 "twoormor_rate", "twoormor_rate_moe", 
                 "latino_rate", "latino_rate_moe",
                 "nh_white_rate", "nh_white_rate_moe")

  df_wide_multigeo <- df_wide_multigeo %>%
    rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
  
  # Employment data doesn't include _raw or _raw_moe values - adding here using _pop * _rate columns
  df_wide_multigeo$total_raw <- ifelse(df_wide_multigeo$total_pop <= 0, NA, df_wide_multigeo$total_pop*df_wide_multigeo$total_rate/100)
  df_wide_multigeo$asian_raw <- ifelse(df_wide_multigeo$asian_pop <= 0, NA, df_wide_multigeo$asian_pop*df_wide_multigeo$asian_rate/100)
  df_wide_multigeo$black_raw <- ifelse(df_wide_multigeo$black_pop <= 0, NA, df_wide_multigeo$black_pop*df_wide_multigeo$black_rate/100)
  df_wide_multigeo$nh_white_raw <- ifelse(df_wide_multigeo$nh_white_pop <= 0, NA, df_wide_multigeo$nh_white_pop*df_wide_multigeo$nh_white_rate/100)
  df_wide_multigeo$latino_raw <- ifelse(df_wide_multigeo$latino_pop <= 0, NA, df_wide_multigeo$latino_pop*df_wide_multigeo$latino_rate/100)
  df_wide_multigeo$other_raw <- ifelse(df_wide_multigeo$other_pop <= 0, NA, df_wide_multigeo$other_pop*df_wide_multigeo$other_rate/100)
  df_wide_multigeo$pacisl_raw <- ifelse(df_wide_multigeo$pacisl_pop <= 0, NA, df_wide_multigeo$pacisl_pop*df_wide_multigeo$pacisl_rate/100)
  df_wide_multigeo$twoormor_raw <- ifelse(df_wide_multigeo$twoormor_pop <= 0, NA, df_wide_multigeo$twoormor_pop*df_wide_multigeo$twoormor_rate/100)
  df_wide_multigeo$aian_raw <- ifelse(df_wide_multigeo$aian_pop <= 0, NA, df_wide_multigeo$aian_pop*df_wide_multigeo$aian_rate/100)
  
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
  
  # drop total_rate (same value as total_pop) & drop _moe values: these fields are not needed for arei_race_county_2022
  df_wide_multigeo <- select(df_wide_multigeo, -ends_with("_moe"), -ends_with("_rate"))
}

############## PRE-CALCULATION: FINAL DATA PREP ##############
# Finish up data cleaning
# # make colnames lower case
# colnames(df_wide_multigeo) <- tolower(colnames(df_wide_multigeo))

# Clean geo names
df_wide_multigeo$name <- gsub(", California", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" County", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" city", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" town", "", df_wide_multigeo$name)
df_wide_multigeo$name <- gsub(" CDP", "", df_wide_multigeo$name)

############## PRE-CALCULATION: POPULATION AND/OR CV CHECKS ##############

df <- df_wide_multigeo

### Do population checks and cv checks
if (!is.na(pop_threshold) & is.na(cv_threshold)) {
  # if pop_threshold exists and cv_threshold is NA, do pop check but no CV check (doesn't apply to any at this time)
  # Apply screen(s) to [race]_rate columns (potential screens: CVs > cv_threshold and _pop < pop_threshold)
  
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
  # if pop_threshold is NA and cv_threshold exists, check cv only (i.e. only B19301)
  ## Calculate CV values for all rates - store in columns as cv_[race]_rate
  df$total_rate_cv <- ifelse(df$total_rate==0, NA, df$total_rate_moe/1.645/df$total_rate*100)
  df$asian_rate_cv <- ifelse(df$asian_rate==0, NA, df$asian_rate_moe/1.645/df$asian_rate*100)
  df$black_rate_cv <- ifelse(df$black_rate==0, NA, df$black_rate_moe/1.645/df$black_rate*100)
  df$nh_white_rate_cv <- ifelse(df$nh_white_rate==0, NA, df$nh_white_rate_moe/1.645/df$nh_white_rate*100)
  df$latino_rate_cv <- ifelse(df$latino_rate==0, NA, df$latino_rate_moe/1.645/df$latino_rate*100)
  df$other_rate_cv <- ifelse(df$other_rate==0, NA, df$other_rate_moe/1.645/df$other_rate*100)
  df$pacisl_rate_cv <- ifelse(df$pacisl_rate==0, NA, df$pacisl_rate_moe/1.645/df$pacisl_rate*100)
  df$twoormor_rate_cv <- ifelse(df$twoormor_rate==0, NA, df$twoormor_rate_moe/1.645/df$twoormor_rate*100)
  df$aian_rate_cv <- ifelse(df$aian_rate==0, NA, df$aian_rate_moe/1.645/df$aian_rate*100)

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
  ## Calculate CV values for all rates - store in columns as cv_[race]_rate
  df$total_rate_cv <- ifelse(df$total_rate==0, NA, df$total_rate_moe/1.645/df$total_rate*100)
  df$asian_rate_cv <- ifelse(df$asian_rate==0, NA, df$asian_rate_moe/1.645/df$asian_rate*100)
  df$black_rate_cv <- ifelse(df$black_rate==0, NA, df$black_rate_moe/1.645/df$black_rate*100)
  df$nh_white_rate_cv <- ifelse(df$nh_white_rate==0, NA, df$nh_white_rate_moe/1.645/df$nh_white_rate*100)
  df$latino_rate_cv <- ifelse(df$latino_rate==0, NA, df$latino_rate_moe/1.645/df$latino_rate*100)
  df$other_rate_cv <- ifelse(df$other_rate==0, NA, df$other_rate_moe/1.645/df$other_rate*100)
  df$pacisl_rate_cv <- ifelse(df$pacisl_rate==0, NA, df$pacisl_rate_moe/1.645/df$pacisl_rate*100)
  df$twoormor_rate_cv <- ifelse(df$twoormor_rate==0, NA, df$twoormor_rate_moe/1.645/df$twoormor_rate*100)
  df$aian_rate_cv <- ifelse(df$aian_rate==0, NA, df$aian_rate_moe/1.645/df$aian_rate*100)

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
  pop_columns <- colnames(select(df, ends_with("_pop")))
  df[,pop_columns] <- sapply(df[,pop_columns], function(x) ifelse(x<0, NA, x))
  
}

df <- select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"))

############## CALCULATE RACE COUNTS STATS AND SEND FINAL TABLES TO POSTGRES##############

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")
d <- df

if (table_code != "DP05") {
  

# Adds asbest value for RC Functions
d$asbest = asbest

d <- count_values(d) 
d <- calc_best(d) 
d <- calc_diff(d) 
d <- calc_avg_diff(d)
d <- calc_s_var(d)
d <- calc_id(d)

### Split into geolevel tables
#split into STATE, COUNTY, CITY tables 
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]
          
#calculate STATE z-scores
state_table <- calc_state_z(state_table)
View(state_table)
          
#calculate COUNTY z-scores
county_table <- calc_z(county_table)
          
## Calc county ranks##
county_table <- calc_ranks(county_table)
View(county_table)
          
          
# #calculate CITY z-scores
city_table <- calc_z(city_table)
# 
# ## Calc city ranks##
city_table <- calc_ranks(city_table)
View(city_table)
          
#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")


# ############## NON-DP05 ----- SEND COUNTY, STATE, CITY CALCULATIONS TO POSTGRES ##############

###update info for postgres tables###
county_table_name <- "arei_hlth_health_insurance_county_2023"      # See RC 2022 Workflow/v3 2021 SQL Views for table name (remember to update year to 2022)
state_table_name <- "arei_hlth_health_insurance_state_2023"        # See RC 2022 Workflow/v3 2021 SQL Views for table name (remember to update year to 2022)
city_table_name <- "arei_hlth_health_insurance_city_2023"         # See RC 2022 Workflow/v3 2021 SQL Views for table name (remember to update year to 2022)
indicator <- "Uninsured Population (%)"                         # See Indicator Methodology 2021 for indicator description
source <- "2017-2021 ACS 5-Year Estimates, Table S2701, https://data.census.gov/cedsci/"   # See Indicator Methodology 2021 for source info
rc_schema <- "v5"
#send tables to postgres COMMENTED OUT FOR QA
# to_postgres(county_table, state_table)
city_to_postgres(city_table)

} else {
  
# # ############## DPO5 ONLY ----- SEND COUNTY, STATE, CITY CALCULATIONS TO POSTGRES ##############
   county_table <- d
   ###update info for postgres tables###
   county_table_name <- "arei_race_county_2023"      # See RC 2022 Workflow/v3 2021 SQL Views for table name (remember to update year to 2022)
   indicator <- "County and State population by race/ethnicity for RC Place page"        # See Indicator Methodology 2021 for indicator description
   source <- "ACS 2017-2021, Table DP05. All AIAN, All NHPI, All Latinx, all other groups are one race alone and non-Latinx."   # See Indicator Methodology 2021 for source info

  #send tables to postgres COMMENTED OUT FOR QA
  # to_postgres(county_table)
}


