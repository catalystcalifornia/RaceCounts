source("W:\\RDA Team\\R\\credentials_source.R")
library(daff)
library(tidyr)

con <- connect_to_db("racecounts")
final_cities <- dbGetQuery(con, "SELECT * FROM v7.arei_multigeo_list where geolevel = 'place'")

# check subprime source data tables and see if issue is present
# ran hous_subprime_2025.R lines 1-255

# check for pooling/averages in rda_shared_data tables
# df_applications20 has multiple years (not pooled yet)
colnames(df_applications20)
table(df_applications20$as_of_year, useNA="always")
# df_subprime20 has multiple years (not pooled yet)
colnames(df_subprime20)
table(df_subprime20$as_of_year, useNA="always")

# next, get data availability per race per year for applications and subprime
# added to calculations function a group by (geolevel, year) confirm this worked
check <- applications_crosswalk %>% filter(place_geoid=="0600156")
check2 <- applications_city %>% filter(place_geoid=='0600156')
sum(check2$total_applications, na.rm=TRUE) # 137
nrow(unique(check)) # 822 - but on closer look it seems like there's duplicates? If I divide 822/6 I get 137

# I think it worked so i'm going to merge applications_city and subprime_city to compare data availability and see if there's any concerns to address
df_merge <- merge(applications_city, subprime_city, by=c("place_geoid", "as_of_year"))
df_long <- df_merge %>%
  pivot_longer(
    cols = -c(place_geoid, as_of_year),
    names_to = c("race", ".value"),
    names_pattern = "^(.*)_(applications|subprime)$"
  ) %>%
  # ignoring cases where a place is just missing data (year is NA)
  filter(!is.na(as_of_year)) %>%
  # flag where applications or subprime is NA (exclusive)
  mutate(mismatch_flag = xor(is.na(applications), is.na(subprime)))

# getting a list of mismatches (application or subprime is NA but not both)
mismatches <- df_long %>% filter(mismatch_flag==TRUE)

# YES - final RC cities are impacted by bug BUT it's all instances of subprime as NA (should maybe be 0?)
check_relevance_cities <- mismatches %>% filter(place_geoid %in% final_cities$geoid)

# repeating above for county, state, leg
# counties
df_merge <- merge(applications_county, subprime_county, by=c("county_id", "as_of_year"))
df_long <- df_merge %>%
  pivot_longer(
    cols = -c(county_id, as_of_year),
    names_to = c("race", ".value"),
    names_pattern = "^(.*)_(applications|subprime)$"
  ) %>%
  # ignoring cases where a place is just missing data (year is NA)
  filter(!is.na(as_of_year)) %>%
  # flag where applications or subprime is NA (exclusive)
  mutate(mismatch_flag = xor(is.na(applications), is.na(subprime)))

# getting a list of mismatches (application or subprime is NA but not both)
mismatches <- df_long %>% filter(mismatch_flag==TRUE)

# YES - final RC cities are impacted by bug but all cases are when subprime is NA (but should be zero?)
nrow(mismatches) 

# state
df_merge <- merge(applications_state, subprime_state, by=c("state_code", "as_of_year"))
df_long <- df_merge %>%
  pivot_longer(
    cols = -c(state_code, as_of_year),
    names_to = c("race", ".value"),
    names_pattern = "^(.*)_(applications|subprime)$"
  ) %>%
  # ignoring cases where a place is just missing data (year is NA)
  filter(!is.na(as_of_year)) %>%
  # flag where applications or subprime is NA (exclusive)
  mutate(mismatch_flag = xor(is.na(applications), is.na(subprime)))

# getting a list of mismatches (application or subprime is NA but not both)
mismatches <- df_long %>% filter(mismatch_flag==TRUE)

# NO impact
nrow(mismatches) # 0
  
# leg districts -assm
df_merge <- merge(applications_assm, subprime_assm, by=c("assm_geoid", "as_of_year"))
df_long <- df_merge %>%
  pivot_longer(
    cols = -c(assm_geoid, as_of_year),
    names_to = c("race", ".value"),
    names_pattern = "^(.*)_(applications|subprime)$"
  ) %>%
  # ignoring cases where a place is just missing data (year is NA)
  filter(!is.na(as_of_year)) %>%
  # flag where applications or subprime is NA (exclusive)
  mutate(mismatch_flag = xor(is.na(applications), is.na(subprime)))

# getting a list of mismatches (application or subprime is NA but not both)
mismatches <- df_long %>% filter(mismatch_flag==TRUE)

# YES impact but same as above (subprime NAs only)
nrow(mismatches) # 434


# leg districts - sen
df_merge <- merge(applications_sen, subprime_sen, by=c("sen_geoid", "as_of_year"))
df_long <- df_merge %>%
  pivot_longer(
    cols = -c(sen_geoid, as_of_year),
    names_to = c("race", ".value"),
    names_pattern = "^(.*)_(applications|subprime)$"
  ) %>%
  # ignoring cases where a place is just missing data (year is NA)
  filter(!is.na(as_of_year)) %>%
  # flag where applications or subprime is NA (exclusive)
  mutate(mismatch_flag = xor(is.na(applications), is.na(subprime)))

# getting a list of mismatches (application or subprime is NA but not both)
mismatches <- df_long %>% filter(mismatch_flag==TRUE)

# YES impact but same as above (subprime NAs only)
nrow(mismatches) # 106


# get all original and v2 tables and see if anything changed - delete v2
##### CITIES #####
# additionally need to filter for final cities list
city <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_city_2025;") %>% 
  filter(city_id %in% final_cities$geoid)
city_v2 <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_city_2025_v2;") %>% 
  filter(city_id %in% final_cities$geoid)

# There are cell changes 485 out of 499 cities for  downstream impacts 
# Sometimes it's counts and rates going to NA (with a proportion of these leading to downstream impacts on z-scores, ranks, quartiles)
# Sometimes the counts and rates do NOT change but we still see downstream impacts on z-scores, ranks, quartiles - confused by this.
check_city <- diff_data(city, city_v2)
render_diff(check_city)


##### COUNTIES #####
county <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_county_2025;")
county_v2 <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_county_2025_v2;")

# No changes
check_county <- diff_data(county, county_v2)
render_diff(check_county)


##### STATE #####
state <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_state_2025;")
state_v2 <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_state_2025_v2;")

# No changes
check_state <- diff_data(state, state_v2)
render_diff(check_state)


##### LEG DISTRICTS #####
leg <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_leg_2025;")
leg_v2 <- dbGetQuery(con, "SELECT * FROM v7.arei_hous_subprime_leg_2025_v2;")

# No stat changes - there is a naming change (Senate to State Senate)?
check_leg <- diff_data(leg, leg_v2)
render_diff(check_leg)