#install packages if not already installed
list.of.packages <- c("readr", "DBI", "RPostgreSQL", "rvest", "tidyverse", "stringr", "dplyr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(readr)
library(RPostgreSQL)
library(DBI)
library(tidyverse) # to scrape metadata table from cde website
library(rvest) # to scrape metadata table from cde website
library(stringr) # cleaning up data
library(data.table) # %like% function
library(dplyr)


### For prep_acs: Check table variables each year as they may change. Modify the URLs below by year and table. ###
# Subject Tables: https://api.census.gov/data/2022/acs/acs5/subject/groups/S2701.html
# Detailed Tables: https://api.census.gov/data/2022/acs/acs5/groups/B19301.html
# Profile Tables: https://api.census.gov/data/2022/acs/acs5/profile/groups/DP05.html


##### Prep ACS tables for RC fx #####
prep_acs <- function(x, table_code, cv_threshold, pop_threshold) {
  
  # renaming rules will change depending on type of census table
  if (startsWith(table_code, "b")) {
    total_table_code = paste0(table_code, "_")
    table_b_code = paste0(table_code, "b_")
    table_c_code = paste0(table_code, "c_")
    table_d_code = paste0(table_code, "d_")
    table_e_code = paste0(table_code, "e_")
    table_f_code = paste0(table_code, "f_")
    table_g_code = paste0(table_code, "g_")
    table_h_code = paste0(table_code, "h_")
    table_i_code = paste0(table_code, "i_")
    
    names(x) <- gsub(total_table_code, "total", names(x))
    names(x) <- gsub(table_b_code, "black", names(x))
    names(x) <- gsub(table_c_code, "aian", names(x))
    names(x) <- gsub(table_d_code, "asian", names(x))
    names(x) <- gsub(table_e_code, "pacisl", names(x))
    names(x) <- gsub(table_f_code, "other", names(x))
    names(x) <- gsub(table_g_code, "twoormor", names(x))
    names(x) <- gsub(table_h_code, "nh_white", names(x))
    names(x) <- gsub(table_i_code, "latino", names(x))
  }
  
  if(endsWith(table_code, "b25014")) {
    # Overcrowded Housing #
    ## Occupants per Room
    names(x) <- gsub("001e", "_pop", names(x))
    names(x) <- gsub("001m", "_pop_moe", names(x))
    
    names(x) <- gsub("003e", "_raw", names(x))
    names(x) <- gsub("003m", "_raw_moe", names(x))
    
    ## total data (more disaggregated than raced values so different prep needed)
    
    ### Extract total values to perform the various calculations needed
    totals <- x %>%
      select(geoid, starts_with("total"))
    
    totals <- totals %>% pivot_longer(total005e:total013e, names_to="var_name", values_to = "estimate")
    totals <- totals %>% pivot_longer(total005m:total013m, names_to="var_name2", values_to = "moe")
    totals$var_name <- substr(totals$var_name, 1, nchar(totals$var_name)-1)
    totals$var_name2 <- substr(totals$var_name2, 1, nchar(totals$var_name2)-1)
    totals <- totals[totals$var_name == totals$var_name2, ]
    totals <- select(totals, -c(var_name, var_name2))
    
    ### sum the numerator columns 005e-013e (total_raw):
    total_raw_values <- totals %>%
      select(geoid, estimate) %>%
      group_by(geoid) %>%
      summarise(total_raw = sum(estimate))
    
    #### join these calculations back to x
    x <- left_join(x, total_raw_values, by = "geoid")
    
    ### calculate the total_raw_moe using moe_sum (need to sort MOE values first to make sure highest MOE is used in case of multiple zero estimates)
    ### methodology source is text under table on slide 52 here: https://www.census.gov/content/dam/Census/programs-surveys/acs/guidance/training-presentations/20180418_MOE.pdf
    total_raw_moes <- totals %>%
      select(geoid, estimate, moe) %>%
      group_by(geoid) %>%
      arrange(desc(moe), .by_group = TRUE) %>%
      summarise(total_raw_moe = moe_sum(moe, estimate, na.rm=TRUE))   # https://walker-data.com/tidycensus/reference/moe_sum.html
    
    #### join these calculations back to x
    x <- left_join(x, total_raw_moes, by = "geoid")
    
    ### calculate total_rate
    total_rates <- left_join(total_raw_values, totals[, 1:2])
    total_rates$total_rate <- total_rates$total_raw/total_rates$total_pop*100
    total_rates <- total_rates %>%
      select(geoid, total_rate) %>%
      distinct()
    
    #### join these calculations back to x
    x <- left_join(x, total_rates, by = "geoid")
    
    ### calculate the moe for total_rate
    total_pop_data <- totals %>%
      select(geoid, total_pop, total_pop_moe) %>%
      distinct()
    total_rate_moes <- left_join(total_raw_values, total_raw_moes, by='geoid') %>%
      left_join(., total_pop_data, by='geoid')
    total_rate_moes$total_rate_moe <- moe_prop(total_rate_moes$total_raw,    # https://walker-data.com/tidycensus/reference/moe_prop.html
                                               total_rate_moes$total_pop, 
                                               total_rate_moes$total_raw_moe, 
                                               total_rate_moes$total_pop_moe)*100
    total_rate_moes <- total_rate_moes %>%
      select(geoid, total_rate_moe)
    
    #### join these calculations back to x
    x <- left_join(x, total_rate_moes, by = "geoid")
    
    ## raced data (raw values don't need aggregation like total values do)
    
    ### calculate raced rates
    x$asian_rate <- ifelse(x$asian_pop <= 0, NA, x$asian_raw/x$asian_pop*100)
    x$black_rate <- ifelse(x$black_pop <= 0, NA, x$black_raw/x$black_pop*100)
    x$nh_white_rate <- ifelse(x$nh_white_pop <= 0, NA, x$nh_white_raw/x$nh_white_pop*100)
    x$latino_rate <- ifelse(x$latino_pop <= 0, NA, x$latino_raw/x$latino_pop*100)
    x$other_rate <- ifelse(x$other_pop <= 0, NA, x$other_raw/x$other_pop*100)
    x$pacisl_rate <- ifelse(x$pacisl_pop <= 0, NA, x$pacisl_raw/x$pacisl_pop*100)
    x$twoormor_rate <- ifelse(x$twoormor_pop <= 0, NA, x$twoormor_raw/x$twoormor_pop*100)
    x$aian_rate <- ifelse(x$aian_pop <= 0, NA, x$aian_raw/x$aian_pop*100)
    
    
    ### calculate moes for raced rates
    x$asian_rate_moe <- moe_prop(x$asian_raw,
                                 x$asian_pop,
                                 x$asian_raw_moe,
                                 x$asian_pop_moe)*100
    
    x$black_rate_moe <- moe_prop(x$black_raw,
                                 x$black_pop,
                                 x$black_raw_moe,
                                 x$black_pop_moe)*100
    
    x$nh_white_rate_moe <- moe_prop(x$nh_white_raw,
                                    x$nh_white_pop,
                                    x$nh_white_raw_moe,
                                    x$nh_white_pop_moe)*100
    
    x$latino_rate_moe <- moe_prop(x$latino_raw,
                                  x$latino_pop,
                                  x$latino_raw_moe,
                                  x$latino_pop_moe)*100
    
    x$other_rate_moe <- moe_prop(x$other_raw,
                                 x$other_pop,
                                 x$other_raw_moe,
                                 x$other_pop_moe)*100
    
    x$pacisl_rate_moe <- moe_prop(x$pacisl_raw,
                                  x$pacisl_pop,
                                  x$pacisl_raw_moe,
                                  x$pacisl_pop_moe)*100
    
    x$twoormor_rate_moe <- moe_prop(x$twoormor_raw,
                                    x$twoormor_pop,
                                    x$twoormor_raw_moe,
                                    x$twoormor_pop_moe)*100
    
    x$aian_rate_moe <- moe_prop(x$aian_raw,
                                x$aian_pop,
                                x$aian_raw_moe,
                                x$aian_pop_moe)*100
    
    
    ### Convert any NaN to NA
    x <- x %>% 
      mutate_all(function(x) ifelse(is.nan(x), NA, x))
    
    ### drop the total006-013 e and m columns and pop_moe cols
    x <- x %>%
      select(-starts_with("total0"), -ends_with("_pop_moe"))
    
  }
  
  if(endsWith(table_code, "b19301")) {
    
    names(x) <- gsub("001e", "_rate", names(x))
    names(x) <- gsub("001m", "_rate_moe", names(x))
  }
  
  if(endsWith(table_code, "b25003")) {
    ## Occupants per Room
    names(x) <- gsub("001e", "_pop", names(x))
    names(x) <- gsub("001m", "_pop_moe", names(x))
    
    names(x) <- gsub("003e", "_raw", names(x))
    names(x) <- gsub("003m", "_raw_moe", names(x))
    
    ## total data (more disaggregated than raced values so different prep needed)
    
    ### Extract total values to perform the various calculations needed
    totals <- x %>%
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
    
    #### join these calculations back to x
    x <- left_join(x, total_raw_values, by = "geoid")
    
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
    
    #### join these calculations back to x
    x <- left_join(x, total_raw_moes, by = "geoid")
    
    ### calculate total_rate
    total_rates <- left_join(total_raw_values, totals[, 1:2])
    total_rates$total_rate <- total_rates$total_raw/total_rates$total_pop*100
    total_rates <- total_rates %>%
      select(geoid, total_rate) %>%
      distinct()
    
    #### join these calculations back to x
    x <- left_join(x, total_rates, by = "geoid")
    
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
    
    #### join these calculations back to x
    x <- left_join(x, total_rate_moes, by = "geoid")
    
    
    ## raced data (raw values don't need aggregation like total values do)
    
    ### calculate raced rates
    x$asian_rate <- ifelse(x$asian_pop <= 0, NA, x$asian_raw/x$asian_pop*100)
    x$black_rate <- ifelse(x$black_pop <= 0, NA, x$black_raw/x$black_pop*100)
    x$nh_white_rate <- ifelse(x$nh_white_pop <= 0, NA, x$nh_white_raw/x$nh_white_pop*100)
    x$latino_rate <- ifelse(x$latino_pop <= 0, NA, x$latino_raw/x$latino_pop*100)
    x$other_rate <- ifelse(x$other_pop <= 0, NA, x$other_raw/x$other_pop*100)
    x$pacisl_rate <- ifelse(x$pacisl_pop <= 0, NA, x$pacisl_raw/x$pacisl_pop*100)
    x$twoormor_rate <- ifelse(x$twoormor_pop <= 0, NA, x$twoormor_raw/x$twoormor_pop*100)
    x$aian_rate <- ifelse(x$aian_pop <= 0, NA, x$aian_raw/x$aian_pop*100)
    
    
    ### calculate moes for raced rates
    x$asian_rate_moe <- moe_prop(x$asian_raw,
                                 x$asian_pop,
                                 x$asian_raw_moe,
                                 x$asian_pop_moe)
    x$black_rate_moe <- moe_prop(x$black_raw,
                                 x$black_pop,
                                 x$black_raw_moe,
                                 x$black_pop_moe)
    x$nh_white_rate_moe <- moe_prop(x$nh_white_raw,
                                    x$nh_white_pop,
                                    x$nh_white_raw_moe,
                                    x$nh_white_pop_moe)
    x$latino_rate_moe <- moe_prop(x$latino_raw,
                                  x$latino_pop,
                                  x$latino_raw_moe,
                                  x$latino_pop_moe)
    x$other_rate_moe <- moe_prop(x$other_raw,
                                 x$other_pop,
                                 x$other_raw_moe,
                                 x$other_pop_moe)
    x$pacisl_rate_moe <- moe_prop(x$pacisl_raw,
                                  x$pacisl_pop,
                                  x$pacisl_raw_moe,
                                  x$pacisl_pop_moe)
    x$twoormor_rate_moe <- moe_prop(x$twoormor_raw,
                                    x$twoormor_pop,
                                    x$twoormor_raw_moe,
                                    x$twoormor_pop_moe)
    x$aian_rate_moe <- moe_prop(x$aian_raw,
                                x$aian_pop,
                                x$aian_raw_moe,
                                x$aian_pop_moe)
    
    
    ### Convert any NaN to NA
    x <- x %>% 
      mutate_all(function(x) ifelse(is.nan(x), NA, x))
    
    ### drop the total006-013 e and m columns and pop_moe cols
    x <- x %>%
      select(-starts_with("total0"), -ends_with("_pop_moe"))
    
    
  }
  
  if (startsWith(table_code, "s2802") | startsWith(table_code, "s2701")) {
    old_names <- colnames(x)[-(1:3)]
    new_names <- c("total_pop", "black_pop", "aian_pop", "asian_pop", "pacisl_pop",
                   "other_pop", "twoormor_pop", "latino_pop", "nh_white_pop", 
                   "total_raw", "black_raw", "aian_raw", "asian_raw", "pacisl_raw",
                   "other_raw", "twoormor_raw", "latino_raw", "nh_white_raw", 
                   "total_rate", "black_rate", "aian_rate", "asian_rate", "pacisl_rate",
                   "other_rate", "twoormor_rate", "latino_rate", "nh_white_rate", 
                   "total_pop_moe", "black_pop_moe", "aian_pop_moe", "asian_pop_moe", "pacisl_pop_moe",
                   "other_pop_moe", "twoormor_pop_moe", "latino_pop_moe", "nh_white_pop_moe",
                   "total_raw_moe", "black_raw_moe", "aian_raw_moe", "asian_raw_moe",
                   "pacisl_raw_moe", "other_raw_moe", "twoormor_raw_moe", "latino_raw_moe",
                   "nh_white_raw_moe", 
                   "total_rate_moe", "black_rate_moe", "aian_rate_moe", "asian_rate_moe",
                   "pacisl_rate_moe", "other_rate_moe", "twoormor_rate_moe", "latino_rate_moe",
                   "nh_white_rate_moe")
    x <- x %>%
      rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
  }
  
  if (startsWith(table_code, "s2301")) {
    old_names <- colnames(x)[-(1:3)]
    new_names <- c("total_pop", "black_pop", "aian_pop", "asian_pop", "pacisl_pop",
                   "other_pop", "twoormor_pop", "latino_pop", "nh_white_pop", "total_rate", 
                   "black_rate", "aian_rate", "asian_rate", "pacisl_rate", "other_rate", 
                   "twoormor_rate", "latino_rate", "nh_white_rate", "total_pop_moe", 
                   "black_pop_moe", "aian_pop_moe", "asian_pop_moe", "pacisl_pop_moe", 
                   "other_pop_moe", "twoormor_pop_moe", "latino_pop_moe", "nh_white_pop_moe", 
                   "total_rate_moe", "black_rate_moe", "aian_rate_moe", "asian_rate_moe", 
                   "pacisl_rate_moe", "other_rate_moe", "twoormor_rate_moe", "latino_rate_moe", 
                   "nh_white_rate_moe")
    x <- x %>%
      rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
    
    # Employment data doesn't include _raw or _raw_moe values - adding here using _pop * _rate columns
    x$total_raw <- ifelse(x$total_pop <= 0, NA, x$total_pop*x$total_rate/100)
    x$asian_raw <- ifelse(x$asian_pop <= 0, NA, x$asian_pop*x$asian_rate/100)
    x$black_raw <- ifelse(x$black_pop <= 0, NA, x$black_pop*x$black_rate/100)
    x$nh_white_raw <- ifelse(x$nh_white_pop <= 0, NA, x$nh_white_pop*x$nh_white_rate/100)
    x$latino_raw <- ifelse(x$latino_pop <= 0, NA, x$latino_pop*x$latino_rate/100)
    x$other_raw <- ifelse(x$other_pop <= 0, NA, x$other_pop*x$other_rate/100)
    x$pacisl_raw <- ifelse(x$pacisl_pop <= 0, NA, x$pacisl_pop*x$pacisl_rate/100)
    x$twoormor_raw <- ifelse(x$twoormor_pop <= 0, NA, x$twoormor_pop*x$twoormor_rate/100)
    x$aian_raw <- ifelse(x$aian_pop <= 0, NA, x$aian_pop*x$aian_rate/100)
    
    x$total_raw_moe <- sqrt(x$total_pop^2 * (x$total_rate_moe/100)^2 + (x$total_rate/100)^2 * x$total_pop_moe^2)
    
    x$asian_raw_moe <- sqrt(x$asian_pop^2 * (x$asian_rate_moe/100)^2 + (x$asian_rate/100)^2 * x$asian_pop_moe^2)
    
    x$black_raw_moe <- sqrt(x$black_pop^2 * (x$black_rate_moe/100)^2 + (x$black_rate/100)^2 * x$black_pop_moe^2)
    
    x$nh_white_raw_moe <- sqrt(x$nh_white_pop^2 * (x$nh_white_rate_moe/100)^2 + (x$nh_white_rate/100)^2 * x$nh_white_pop_moe^2)
    
    x$latino_raw_moe <- sqrt(x$latino_pop^2 * (x$latino_rate_moe/100)^2 + (x$latino_rate/100)^2 * x$latino_pop_moe^2)
    
    x$other_raw_moe <- sqrt(x$other_pop^2 * (x$other_rate_moe/100)^2 + (x$other_rate/100)^2 * x$other_pop_moe^2)
    
    x$pacisl_raw_moe <- sqrt(x$pacisl_pop^2 * (x$pacisl_rate_moe/100)^2 + (x$pacisl_rate/100)^2 * x$pacisl_pop_moe^2)
    
    x$twoormor_raw_moe <- sqrt(x$twoormor_pop^2 * (x$twoormor_rate_moe/100)^2 + (x$twoormor_rate/100)^2 * x$twoormor_pop_moe^2)
    
    x$aian_raw_moe <- sqrt(x$aian_pop^2 * (x$aian_rate_moe/100)^2 + (x$aian_rate/100)^2 * x$aian_pop_moe^2)
  }
  

  if (startsWith(table_code, "dp05")) {
    old_names <- colnames(x)[-(1:3)]
    new_names <- c("total_pop", "total_rate", "aian_pop", "pct_aian_pop", "pacisl_pop",
                   "pct_pacisl_pop", "latino_pop", "pct_latino_pop", "nh_white_pop", "pct_nh_white_pop",
                   "black_pop", "pct_black_pop", "asian_pop", "pct_asian_pop", "other_pop", "pct_other_pop",
                   "twoormor_pop", "pct_twoormor_pop", "total_pop_moe", "pct_total_pop_moe", "aian_pop_moe", "pct_aian_pop_moe", "pacisl_pop_moe",
                   "pct_pacisl_pop_moe", "latino_pop_moe", "pct_latino_pop_moe", "nh_white_pop_moe", "pct_nh_white_pop_moe",
                   "black_pop_moe", "pct_black_pop_moe", "asian_pop_moe", "pct_asian_pop_moe", "other_pop_moe", "pct_other_pop_moe",
                   "twoormor_pop_moe", "pct_twoormor_pop_moe")
    x <- x %>%
      rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
    
    # drop total_rate - is just a copy of total_pop & drop _moe values (get clarification: aren't included in arei_race_county_2021)
    x <- dplyr::select(x, -ends_with("_moe"), -ends_with("_rate"))
  }
  
  # Finish up data cleaning
  # make colnames lower case
  colnames(x) <- tolower(colnames(x))
  
  # Clean geo names
  x$name <- gsub(", California", "", x$name)
  x$name <- gsub(" County", "", x$name)
  x$name <- gsub(" city", "", x$name)
  x$name <- gsub(" town", "", x$name)
  x$name <- gsub(" CDP", "", x$name)
  
  
  ### Coefficient of Variation (CV) CALCS ###
  df <- x
  
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
  
  
  return(df)
}

#### Use this fx to get most CDE data ####
get_cde_data <- function(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) {
                df <- read_delim(file = filepath, delim = "\t", na = c("*", ""))#, #name_repair=make.names ),
                                 #col_types = cols('District Code' = col_character()))
                
                #format column names
                names(df) <- str_replace_all(names(df), "[^[:alnum:]]", "") # remove non-alphanumeric characters
                names(df) <- gsub(" ", "", names(df)) # remove spaces
                names(df) <- tolower(names(df))  # make col names lowercase
                Encoding(df$schoolname) <- "ISO 8859-1"  # added this piece in 2023 script bc Spanish accents weren't appearing properly bc CDE native encoding is not UTF-8
                Encoding(df$districtname) <- "ISO 8859-1"  # added this piece in 2023 script bc Spanish accents weren't appearing properly bc CDE native encoding is not UTF-8
                df$districtcode<-as.character(df$districtcode)
                
                #create cdscode field
                df$cdscode <- ifelse(df$aggregatelevel == "D", paste0(df$countycode,df$districtcode,"0000000"),
                                     ifelse(df$aggregatelevel == "S", paste0(df$countycode,df$districtcode,df$schoolcode), paste0(df$countycode,"000000000000")))
                df <- df %>% relocate(cdscode) # make cds code the first col
                
                #  WRITE TABLE TO POSTGRES DB
                
                # make character vector for field types in postgres table
                charvect = rep('numeric', dim(df)[2]) 
                charvect[fieldtype] <- "varchar" # specify which cols are varchar, the rest will be numeric
                
                # add names to the character vector
                names(charvect) <- colnames(df)
                
                dbWriteTable(con, c(table_schema, table_name), df, 
                             overwrite = FALSE, row.names = FALSE,
                             field.types = charvect)
                
                # write comment to table, and the first three fields that won't change.
                table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
                
                # send table comment to database
                dbSendQuery(conn = con, table_comment)      			

return(df)
}

### Use this fx to get most CDE metadata ####
get_cde_metadata <- function(url, html_nodes, table_schema, table_name) {
# See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
                    df_metadata <- url %>% 
                      read_html() %>% 
                      html_nodes(html_nodes) %>% 
                      html_table(fill = T) %>% 
                      lapply(., function(x) setNames(x, c("label", "variable"))) # define/rename columns
                    
                    df_metadata <- data.frame(df_metadata)
                    df_metadata <- df_metadata %>% add_row(label = "cdscode", variable = "unique id")
                    n <- nrow(df_metadata)
                    df_metadata <- df_metadata[c(n, (1:nrow(df_metadata))[-n]), ] # move newly added cdscode row (last row) to row 1 to match order of df_names
                    df_metadata <- subset(df_metadata, label!="Errata Flag (Y/N)") # removes extra row in metadata that is not in data, ex. in suspensions            

                    # format metadata
                    df_names <- data.frame(names(df))  # pull in df col names 

    #### BEFORE THIS STEP, YOU MUST FIRST CHECK THAT THE COLS IN DF AND METADATA TABLES ARE IN THE SAME ORDER ####
                    colcomments <- df_metadata %>% 
                      mutate(label = df_names$names.df., # sub in df col names
                             variable = str_squish(variable))  # remove extra spaces from variables
                    
                    # Adapted from W:\RDA Team\R\ACS Updates\Update Detailed Tables - template.R
                    # make character vectors for column names and metadata. 
                    colcomments_charvar <- colcomments$variable
                    colname_charvar <- colcomments$label
                    
                    # loop through the columns that will change depending on the table. This loop writes comments for all columns, then sends to the postgres db. 
                     for (i in seq_along(colname_charvar)){
                       sqlcolcomment <-
                        paste0("COMMENT ON COLUMN ", table_schema, ".", table_name, ".",
                                colname_charvar[[i]], " IS '", colcomments_charvar[[i]], "'; COMMENT ON COLUMN ", table_schema, ".", table_name, ".",
                                colname_charvar[[i]], " IS '", colcomments_charvar[[i]], "';" )

                    # send sql comment to database
                       dbSendQuery(conn = con, sqlcolcomment)
                     }

return(colcomments)
}

### Use this fx to get CAASPP (ELA/Math testing) data ####
get_caaspp_data <- function(url, zipfile, file, url2, zipfile2, file2, exdir) {#, table_schema, table_name, table_comment_source, table_source) {
  # Create rda_shared_data table metadata -----------------------------------
  table_name <- paste0("caaspp_multigeo_school_research_file_reformatted_",curr_yr)
  table_comment_source <- paste0("Downloaded from ", dwnld_url,". File layout: ", layout_url)
  
  #Download and unzip data ------------------------------------------------
   if(!file.exists(file)) { download.file(url=url, destfile=zipfile) } # download file ONLY if it is not already in exdir
   if(!file.exists(file)) { unzip(zipfile, exdir = exdir) }
  
   #Read in data
   all_student_groups <- read_fwf(file, na = c("*", ""),
                                  fwf_widths(c(14,4,4,3,1,7,7,2,2,7,7,6,6,6,6,6,6,7,6,6,6,6,6,6,6,6,6,6,6,6,2),
                                             col_names = c("cdscode","filler","test_year","student_grp_id",
                                                           "test_type","total_tested_at_reporting_level","total_tested_with_scores",
                                                           "grade","test_id","students_enrolled","students_tested","mean_scale_score",
                                                           "percentage_standard_exceeded","percentage_standard_met","percentage_standard_met_and_above",
                                                           "percentage_standard_nearly_met","percentage_standard_not_met","students_with_scores",
                                                           "area_1_percentage_above_standard","area_1_percentage_near_standard","area_1_percentage_below_standard",
                                                           "area_2_percentage_above_standard","area_2_percentage_near_standard","area_2_percentage_below_standard",
                                                           "area_3_percentage_above_standard","area_3_percentage_near_standard","area_3_percentage_below_standard",
                                                           "area_4_percentage_above_standard","area_4_percentage_near_standard","area_4_percentage_below_standard",
                                                           "type_id")))

  all_student_groups <- all_student_groups %>% select(-c(filler))
  
  if(!file.exists(file2)) { download.file(url=url2, destfile=zipfile2) } # download file ONLY if it is not already in exdir
  if(!file.exists(file2)) { unzip(zipfile2, exdir = exdir) } 
  
  #Read in entities
  entities <- read_fwf(file2, fwf_widths(c(14,4,4,2,50), col_names = c("cdscode","filler","test_year","type_id", "geoname")))
  entities <- entities %>% select(-c(filler))
  
  df <-left_join(x=all_student_groups,y=entities,by= c("cdscode", "test_year", "type_id")) %>%
    select(cdscode, everything())
  df <- df %>% dplyr::relocate(geoname, .after = cdscode)
  
  
  ## from get_cde_data
   #  WRITE TABLE TO POSTGRES DB
   
   #make character vector for field types in postgres table
   charvect = rep('numeric', dim(df)[2])
   charvect[fieldtype] <- "varchar" # specify which cols are varchar, the rest will be numeric

   # add names to the character vector
   names(charvect) <- colnames(df)
   
    dbWriteTable(con, c(table_schema, table_name), df,
                 overwrite = FALSE, row.names = FALSE,
                 field.types = charvect)
   
    # write comment to table, and the first three fields that won't change.
    table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
   
    # send table comment to database
    dbSendQuery(conn = con, table_comment)
   
  return(df)
}

### Use this fx to get CAASPP (ELA/Math testing) metadata ####
get_caaspp_metadata <- function(url3, table_schema, table_name)  {
        df_metadata <- url3 %>%
          read_html() %>%
  # the following line doesn't work unless it's 'hardcoded'. will need to be updated each year. follow instructions here to get xpath when there is more than 1 table on the page: https://www.r-bloggers.com/2015/01/using-rvest-to-scrape-an-html-table/
          html_nodes(xpath = '//*[@id="MainContent_divResearchFileLayout2023"]/div/table' ) %>%  # NOTE: this line will need to be updated each year
          html_table(fill = T) %>%
          lapply(., function(x) setNames(x, c("variable"))) # define/rename columns
        
        df_metadata <- data.frame(df_metadata)
        df_metadata <- df_metadata %>% select(contains("variable")) # remove unneeded columns
        df_metadata$variable <- gsub("/", "-", df_metadata$variable)
        df_metadata$variable <- gsub("'", '', df_metadata$variable)
        df_metadata <- subset(df_metadata, variable!="County Code" & variable!="District Code" & variable!="School Code" & variable!="Filler") # remove unneeded rows
        df_metadata <- df_metadata %>% add_row(variable = "CDS Code") %>% add_row(variable = "Entity Name")
        num_rows <- nrow(df_metadata)
        df_metadata <- rbind(tail(df_metadata, 1), head(df_metadata, -1)) # TEST move entity name to first row
        df_metadata <- rbind(tail(df_metadata, 1), head(df_metadata, -1)) # TEST move cdscode to first row
        
 
        colnames(df_metadata)[1] = "variable"
        df_metadata <- cbind(df_metadata, label=NA) # add blank label column to be populated using data table (df)

        # format metadata
        df_names <- data.frame(names(df))  # pull in df col names
        colcomments <- df_metadata %>%
          mutate(label = df_names$names.df.) # bring df col names into label column

        # Adapted from W:\RDA Team\R\ACS Updates\Update Detailed Tables - template.R
        # make character vectors for column names and metadata.
        colcomments_charvar <- colcomments$variable
        colname_charvar <- colcomments$label

        # loop through the columns that will change depending on the table. This loop writes comments for all columns, then sends to the postgres db.
         for (i in seq_along(colname_charvar)){
           sqlcolcomment <-
            paste0("COMMENT ON COLUMN ", table_schema, ".", table_name, ".",
                    colname_charvar[[i]], " IS '", colcomments_charvar[[i]], "'; COMMENT ON COLUMN ", table_schema, ".", table_name, ".",
                    colname_charvar[[i]], " IS '", colcomments_charvar[[i]], "';" )

        # send sql comment to database
           dbSendQuery(conn = con, sqlcolcomment)
         }

return(colcomments)
}

### Use this fx to get URSUS (Use of Force) data ####
get_ursus_data <- function(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) {
        # CA DOJ Use of Force data
        df <- read_csv(file = filepath, na = c("*", ""))
        
        #format column names
        names(df) <- tolower(names(df)) # make col names lowercase
        df <- df %>% mutate_all(as.character) # make all data characters
        
        ##  WRITE TABLE TO POSTGRES DB ##               NOTE: con2 must be rda_shared_data for function to work.
        # make character vector for field types in postgres table
        charvect = rep('numeric', dim(df)[2]) 
        charvect[fieldtype] <- "varchar" # specify which cols are varchar, the rest will be numeric
        
        # add names to the character vector
        names(charvect) <- colnames(df)
        
        dbWriteTable(con2, c(table_schema, table_name), df,
                     overwrite = FALSE, row.names = FALSE,
                     field.types = charvect)
        
        # write comment to table, and the first three fields that won't change.
        table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
        
        # send table comment to database
        dbSendQuery(conn = con2, table_comment)    

return(df)
}




