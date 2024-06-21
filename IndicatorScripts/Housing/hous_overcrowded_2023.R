# B25014: Overcrowded Housing for RC v5
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
library(sf)


# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# API Call Info - Change each year
yr = 2021
srvy = "acs5"

############## UPDATE FOR SPECIFIC INDICATOR HERE ##############

table_code = "b25014"     # YOU MUST UPDATE based on most recent Indicator Methodology or Workflow/Cnty-State Indicator Tracking
dataset = "acs5"         # YOU MUST UPDATE: "acs5/subject" for subject ("S") tables OR "acs5/profile" for data profile ("DP") tables OR "acs5" for detailed ("B") tables
cv_threshold = 40         # YOU MUST UPDATE based on most recent Indicator Methodology
pop_threshold = 100        # YOU MUST UPDATE based on most recent Indicator Methodology or set to NA B19301
asbest = 'min'            # YOU MUST UPDATE based on indicator, set to 'min' if S2701 or B25014

##### pull QAed data from pgadmin ######
df_wide_multigeo <- st_read(con, query = "SELECT * FROM housing.acs_5yr_b25014_multigeo_2021 WHERE geolevel IN ('place', 'county', 'state')") #%>% select(-c(starts_with("b25014_")))


############## PRE-CALCULATION: TABLE-SPECIFIC DATA PREP ##############

total_table_code = paste(table_code, "_", sep="")
table_b_code = paste(table_code, "b_", sep="")
table_c_code = paste(table_code, "c_", sep="")
table_d_code = paste(table_code, "d_", sep="")
table_e_code = paste(table_code, "e_", sep="")
table_f_code = paste(table_code, "f_", sep="")
table_g_code = paste(table_code, "g_", sep="")
table_h_code = paste(table_code, "h_", sep="")
table_i_code = paste(table_code, "i_", sep="")

names(df_wide_multigeo) <- gsub(total_table_code, "total", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_b_code, "black", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_c_code, "aian", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_d_code, "asian", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_e_code, "pacisl", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_f_code, "other", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_g_code, "twoormor", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_h_code, "nh_white", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub(table_i_code, "latino", names(df_wide_multigeo))

# Overcrowded Housing #
## Occupants per Room
names(df_wide_multigeo) <- gsub("001e", "_pop", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub("001m", "_pop_moe", names(df_wide_multigeo))

names(df_wide_multigeo) <- gsub("003e", "_raw", names(df_wide_multigeo))
names(df_wide_multigeo) <- gsub("003m", "_raw_moe", names(df_wide_multigeo))

## total data (more disaggregated than raced values so different prep needed)

### Extract total values to perform the various calculations needed
totals <- df_wide_multigeo %>%
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

#### join these calculations back to df_wide_multigeo
df_wide_multigeo <- left_join(df_wide_multigeo, total_raw_values, by = "geoid")

### calculate the total_raw_moe using moe_sum (need to sort MOE values first to make sure highest MOE is used in case of multiple zero estimates)
### methodology source is text under table on slide 52 here: https://www.census.gov/content/dam/Census/programs-surveys/acs/guidance/training-presentations/20180418_MOE.pdf
total_raw_moes <- totals %>%
          select(geoid, estimate, moe) %>%
          group_by(geoid) %>%
          arrange(desc(moe), .by_group = TRUE) %>%
          summarise(total_raw_moe = moe_sum(moe, estimate, na.rm=TRUE))   # https://walker-data.com/tidycensus/reference/moe_sum.html

#### Spot checking moe_sum() -- test that the arrange function is properly sorting moes 
#### resulting in the right calculation if multiple zero estimates present

#####  test_moe_sum <- totals[1:6, c(1, 4:5)]
#####  moe_sum(test_moe_sum$moe, test_moe_sum$estimate, na.rm=TRUE)
##### [1] 8632.893

#####  norm(test_moe_sum$moe, type ="2")
##### [1] 8632.893

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
                                           total_rate_moes$total_pop_moe)*100
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


### Convert any NaN to NA
df_wide_multigeo <- df_wide_multigeo %>% 
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

### drop the total006-013 e and m columns and pop_moe cols
df_wide_multigeo <- df_wide_multigeo %>%
  select(-starts_with("total0"), -ends_with("_pop_moe"))


############## PRE-CALCULATION: FINAL DATA PREP ##############
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
df$total_rate_cv <- ifelse(df$total_rate==0, NA, df$total_rate_moe/1.645/df$total_rate*100)
df$asian_rate_cv <- ifelse(df$asian_rate==0, NA, df$asian_rate_moe/1.645/df$asian_rate*100)
df$black_rate_cv <- ifelse(df$black_rate==0, NA, df$black_rate_moe/1.645/df$black_rate*100)
df$nh_white_rate_cv <- ifelse(df$nh_white_rate==0, NA, df$nh_white_rate_moe/1.645/df$nh_white_rate*100)
df$latino_rate_cv <- ifelse(df$latino_rate==0, NA, df$latino_rate_moe/1.645/df$latino_rate*100)
df$other_rate_cv <- ifelse(df$other_rate==0, NA, df$other_rate_moe/1.645/df$other_rate*100)
df$pacisl_rate_cv <- ifelse(df$pacisl_rate==0, NA, df$pacisl_rate_moe/1.645/df$pacisl_rate*100)
df$twoormor_rate_cv <- ifelse(df$twoormor_rate==0, NA, df$twoormor_rate_moe/1.645/df$twoormor_rate*100)
df$aian_rate_cv <- ifelse(df$aian_rate==0, NA, df$aian_rate_moe/1.645/df$aian_rate*100)
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

df <- select(df, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"))

############## CALCULATE RACE COUNTS STATS AND SEND FINAL TABLES TO POSTGRES##############

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")
d <- df[df$geolevel %in% c('state', 'county', 'place'), ]

# Adds asbest value for RC Functions
d$asbest = asbest   # defined earlier

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
state_table <- calc_state_z(state_table) %>% select(-c(geolevel))
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table)

## Calc county ranks##
county_table <- calc_ranks(county_table) %>% select(-c(geolevel))
View(county_table)


#calculate CITY z-scores
city_table <- calc_z(city_table)

## Calc city ranks##
city_table <- calc_ranks(city_table) %>% select(-c(geolevel))
View(city_table)


#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")


# ############## SEND COUNTY, STATE, CITY CALCULATIONS TO POSTGRES ##############

###update info for postgres tables###
county_table_name <- "arei_hous_overcrowded_county_2023"      # See most recent RC Workflow SQL Views for table name (remember to update year)
state_table_name <- "arei_hous_overcrowded_state_2023"        # See most recent RC Workflow SQL Views for table name (remember to update year)
city_table_name <- "arei_hous_overcrowded_city_2023"         # See most recent RC Workflow SQL Views for table name (remember to update year)
indicator <- "Overcrowded Housing Units (%) (> 1 person per room)"                         # See most recent Indicator Methodology for indicator description
source <- "2017-2021 ACS 5-Year Estimates, Tables B25014B-I, https://data.census.gov/cedsci/"   # See most recent Indicator Methodology for source info
rc_schema <- "v5"


####### SEND TO POSTGRES #######
# to_postgres(county_table,state_table)
# city_to_postgres()


############## CHECK COVERAGE WITH CV_THRESHOLD = 40 AND POP_THRESHOLD = 100##############
# coverage_cv40_pop100 <- county_table %>%
#   select(ends_with("_rate"))
#   
# race_coverage <- as.data.frame(colSums(!is.na(coverage_cv40_pop100)))
# 
# top_10_perf_rank <-
#   head(county_table[order(county_table$performance_rank, decreasing = FALSE), ], n = 10)
# top_10_perf_rank <- top_10_perf_rank %>%
#   select(-(ends_with("moe") | ends_with("diff") | ends_with("z")))
# 
# bottom_10_perf_rank <-
#   head(county_table[order(county_table$performance_rank, decreasing = TRUE), ], n = 10)
# bottom_10_perf_rank <- bottom_10_perf_rank %>%
#   select(-(ends_with("moe") | ends_with("diff") | ends_with("z")))
# 
# top_10_disp_rank <-
#   head(county_table[order(county_table$disparity_rank, decreasing = FALSE), ], n = 10)
# top_10_disp_rank <- top_10_disp_rank %>%
#   select(-(ends_with("moe") | ends_with("diff") | ends_with("z")))
# 
# bottom_10_disp_rank <-
#   head(county_table[order(county_table$disparity_rank, decreasing = TRUE), ], n = 10)
# bottom_10_disp_rank <- bottom_10_disp_rank %>%
#   select(-(ends_with("moe") | ends_with("diff") | ends_with("z")))
# 
# pop_100_150 <-
#   county_table %>% filter(if_any(ends_with("pop"), ~ . < 150 &
#                                    . >= 100))
# pop_100_150 <-
#   pop_100_150 %>% select(
#     county_name,
#     ends_with("pop"),
#     ends_with("raw"),
#     ends_with("cv"),
#     ends_with("rank"),
#     ends_with("quadrant")
#   )
# pop_100_150 <- pop_100_150 %>%
#   pivot_longer(cols = total_pop:latino_pop,
#                names_to = "pop_group",
#                values_to = "pop_value") %>%
#   pivot_longer(cols = black_raw:total_raw,
#                names_to = "raw_group",
#                values_to = "raw_values") %>%
#   pivot_longer(
#     cols = total_rate_cv:aian_rate_cv,
#     names_to = "cv_group",
#     values_to = "cv_values"
#   )
# pop_100_150$pop_group <-
#   sapply(strsplit(pop_100_150$pop_group, "_"), `[`, 1)
# pop_100_150$raw_group <-
#   sapply(strsplit(pop_100_150$raw_group, "_"), `[`, 1)
# pop_100_150$cv_group <-
#   sapply(strsplit(pop_100_150$cv_group, "_"), `[`, 1)
# pop_100_150 <-
#   pop_100_150[pop_100_150$pop_group == pop_100_150$raw_group &
#                 pop_100_150$pop_group == pop_100_150$cv_group &
#                 pop_100_150$raw_group == pop_100_150$cv_group, ]
# pop_100_150 <-
#   pop_100_150[pop_100_150$pop_value > 100 &
#                 pop_100_150$pop_value <= 150, ]
# pop_100_150 <- pop_100_150 %>%
#   select(-c(raw_group, cv_group))
# 
# cv_35_40 <-
#   county_table %>% filter(if_any(ends_with("cv"), ~ . <= 40 & . > 35))
# cv_35_40 <-
#   cv_35_40 %>% select(
#     county_name,
#     ends_with("pop"),
#     ends_with("raw"),
#     ends_with("cv"),
#     ends_with("rank"),
#     ends_with("quadrant")
#   )
# cv_35_40 <- cv_35_40 %>%
#   pivot_longer(cols = total_pop:latino_pop,
#                names_to = "pop_group",
#                values_to = "pop_value") %>%
#   pivot_longer(cols = black_raw:total_raw,
#                names_to = "raw_group",
#                values_to = "raw_values") %>%
#   pivot_longer(
#     cols = total_rate_cv:aian_rate_cv,
#     names_to = "cv_group",
#     values_to = "cv_values"
#   )
# cv_35_40$pop_group <-
#   sapply(strsplit(cv_35_40$pop_group, "_"), `[`, 1)
# cv_35_40$raw_group <-
#   sapply(strsplit(cv_35_40$raw_group, "_"), `[`, 1)
# cv_35_40$cv_group <-
#   sapply(strsplit(cv_35_40$cv_group, "_"), `[`, 1)
# cv_35_40 <-
#   cv_35_40[cv_35_40$pop_group == cv_35_40$raw_group &
#              cv_35_40$pop_group == cv_35_40$cv_group &
#              cv_35_40$raw_group == cv_35_40$cv_group, ]
# cv_35_40 <-
#   cv_35_40[cv_35_40$cv_values > 35 & cv_35_40$cv_values <= 40,]
# cv_35_40 <- cv_35_40 %>%
#   select(-c(raw_group, cv_group))
# cv_35_40 <- cv_35_40 %>% drop_na(county_name)
#  View(cv_35_40)