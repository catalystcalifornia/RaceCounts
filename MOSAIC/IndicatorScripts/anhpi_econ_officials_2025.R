### Disaggregated Asian and NHPI Officials & Mangers RC v7###

# Set up workspace --------------------------------------------------------
# Install packages if not already installed
packages <- c("data.table", "stringr", "dplyr", "RPostgres", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "sf", "DBI", "readxl", "usethis") 

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
source("./MOSAIC/Functions/pums_fx.R")
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("mosaic")
ancestry_list <- read_excel("W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\Asian_NHPI_Ancestry_2024.xlsx", sheet = "ancestry") # list of ANHPI ANC1P/ANC2P codes

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Economic\\QA_Officials_Mgrs_MOSAIC.docx"

#### Step 1: Define Variables ####
root <- "W:/Data/Demographics/PUMS/CA_2020_2024/"
rc_yr = '2025'      # you MUST UPDATE each year
curr_yr <- 2024
start_yr <- curr_yr - 4
rc_schema <- 'v7'


## CHECK FOR UPDATES EACH YEAR
### define common inputs for calc_pums{} and pums_screen{}
indicator = 'officials'         # name of column that contains indicator data, eg: 'connected_youth' which contains values 'connected' and 'not connected'
indicator_val = 'officials'         # desired indicator value, eg: 'connected' (not 'not connected')        
weight = 'PWGTP'                  # PWGTP for person-level (psam_p06.csv) or WGTP for housing unit-level (psam_h06.csv) analysis
cv_threshold <- 20                # threshold and CV must be displayed as a percentage not decimal, eg: 30 not .3
raw_rate_threshold <- 0           # data values less than threshold are screened, for RC indicators threshold is 0.
pop_threshold <- 400              # data for geos+race combos with pop smaller than threshold are screened.


#### Step 2: GET PUMA CROSSWALKS ######
crosswalk <- dbGetQuery(con, "select county_id AS geoid, county_name AS geoname, geo_id AS puma, num_county, afact, afact2 from crosswalks.puma_2022_county_2020")

## Drop counties that have only 1 PUMA that is shared with another county that also has only 1 PUMA.
dupe_pumas <- crosswalk %>% 
  filter(num_county == 1) %>%   # keep only counties with 1 PUMA
  group_by(puma) %>% 
  summarise(n=n()) %>%   		    # count the # of counties assigned to each PUMA
  filter(n > 1)		   		        # keep only counties that have only 1 PUMA that is shared with another county with only 1 PUMA

county_crosswalk <- crosswalk %>%   
  subset(!(puma %in% dupe_pumas$puma)) # remove only counties that have only 1 PUMA that is shared with another county with only 1 PUMA

# length(unique(county_crosswalk$geoid))  # this number should be lower bc it is filtered xwalk
# length(unique(crosswalk$geoid))         # this number should be higher bc it is unfiltered xwalk


#### Step 3: GET PUMS DATA ######
# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf
# Load ONLY the PUMS columns needed for this indicator
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0))    # get all PUMS cols 
cols_wts <- grep("^PWGTP*", cols, value = TRUE)                   # filter for PUMS weight colnames

ppl <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE, select = c(cols_wts, "RT", "SERIALNO", "AGEP", "ESR", "SCH", "SOCP",
                                                                               "PUMA", "ANC1P", "ANC2P", "HISP", "RAC1P", "RACASN", "RACPI", "RACNH"),
             colClasses = list(character = c("PUMA", "ANC1P", "ANC2P", "HISP", "RAC1P", "RACASN", "RACPI", "RACNH"
             )))

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with puma-county xwalk
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid, ppl$PUMA)

# create list of replicate weights
repwlist = rep(paste0(weight, 1:80))

# save copy of original data
orig_data <- ppl

# add nhpi value
ppl$RACNHPI <- case_when(
  ppl$RACNH == 1 | ppl$RACPI ==1 ~ 1,
  TRUE ~ 0
)

#### Step 4: Subset Data & Calc Officials/Managers ####
# Keep records only for those ages 16-24
ppl <- ppl %>% filter(AGEP >= 16 & AGEP <= 64)

## Tag people who are officials or managers: SOCP value starts with "11" 
###### See p110 in W:\Data\Demographics\PUMS\CA_2019_2024\PUMS_Data_Dictionary_2024.pdf
ppl$offmgr <- 
  case_when(
    grepl('^11', ppl$SOCP) ~ as.integer(1),
    TRUE ~ as.integer(0))

# review
summary(ppl$offmgr)

## Code for labor force status: ESR on p56 of PUMS_Data_Dictionary_2024.pdf
table(ppl$ESR, useNA = "always")

# NOTE: 'This includes 4-Armed forces, at work' and '5-Armed forces, with a job but not at work'. It excludes '6-Not in labor force'.
ppl$emply <- as.factor(ifelse(ppl$ESR %in% c(1, 2, 3, 4, 5), "in labor force", "not in labor force")) 
ppl <- filter(ppl, emply=='in labor force')

## Factor for Officials & Managers
ppl$officials <- ifelse(ppl$offmgr == 1, "officials", "not official")
ppl$officials <- as.factor(ppl$officials)
ppl$indicator <- as.factor(ppl$officials)

## check officials results
table(ppl$indicator, useNA = "always")


#### Step 5: Join subgroup labels to data ###

ppl_ <- anhpi_reclass(ppl, ancestry_list)  # returns list containing ppl (reclassified pums data) and aapi_incl (list of AAPI ancestries in data)
list2env(ppl_, .GlobalEnv)  # creates dfs 'people' and 'aapi_incl'

# Add a new column for each anc_label, populated with 1 or 0
for (label in aapi_incl$anc_label) {
  people[[label]] <- as.integer(people$anc_label.x == label | people$anc_label.y == label)
}

# Add a new column for any asian ancestry and same for nhpi, populated with 1 or 0
# Create a named lookup vector: anc_label -> asian value
asian_lookup <- setNames(aapi_incl$asian, aapi_incl$anc_label)

# Populate the new 'asian' column
people$asian <- as.integer(
  (people$anc_label.x %in% names(asian_lookup[asian_lookup == 1])) |
    (people$anc_label.y %in% names(asian_lookup[asian_lookup == 1]))
)

# Create a named lookup vector: anc_label -> nhpi value
nhpi_lookup <- setNames(aapi_incl$nhpi, aapi_incl$anc_label)

# Populate the new 'nhpi' column
people$nhpi <- as.integer(
  (people$anc_label.x %in% names(nhpi_lookup[nhpi_lookup == 1])) |
    (people$anc_label.y %in% names(nhpi_lookup[nhpi_lookup == 1]))
)


## check a few of the new ancestry & asian/nhpi cols
table(thai = people$thai, asian_race = people$RACASN)        # check how many thai ancestry rows are also marked Asian race
table(thai = people$thai, asian_anc = people$asian)          # check that all thai ancestry rows are also marked asian ancestry
table(asian_anc = people$asian, asian_race = people$RACASN)  # 1,845 responses w/ asian ancestry who are not coded race = Asian
#       asian_race
# thai         0      1
#       0 676109  38313
#       1   1845 146299

table(fijian = people$fijian, nhpi_race = people$RACNHPI)    # check how many fijian ancestry rows are also marked NHPI race
table(fijian = people$fijian, nhpi_anc = people$nhpi)        # check that all fijian ancestry rows are also marked nhpi ancestry
table(nhpi_anc = people$nhpi, nhpi_race = people$RACNHPI)    # 400 responses w/ nhpi ancestry who are not coded race = NHPI
#       nhpi_race
# fijian        0     1
#       0 855404   3423
#       1    400   3339

# For this analysis, we include anyone with an Asian ancestry and anyone with an NHPI ancestry, regardless of race.
## E.g. For NHPI, we include all records where nhpi == 1 regardless of RACNHPI value.


#### Step 6: Join crosswalks to data ####
# join county crosswalk to data
ppl_county <- left_join(people, county_crosswalk, by=c("puma_id" = "puma"))   # join FILTERED county-puma crosswalk

# prep state df
ppl_state <- people %>% rename(geoid = state_geoid) %>% mutate(geoname = 'California')


#### Step 7: Set up for PUMS calc fx ####
# get list of subgroups for calcs based on aapi_incl
vars <- aapi_incl %>% pull(anc_label)
vars

table_state <- calc_pums_ind(
  d         = ppl_state,
  weight    = weight,
  repwlist  = repwlist,
  vars      = vars,
  indicator = indicator
)

table_county <- calc_pums_ind(
  d         = ppl_county,
  weight    = weight,
  repwlist  = repwlist,
  vars      = vars,
  indicator = indicator
)


##### SCREENING EXPORATION (STATE DATA) #####

# Method 1: Try ERI screening method, suppress data where subgroup has <100 unweighted responses
aapi_filtered <- ppl_state %>% filter(asian == 1 | nhpi == 1)

screen_unw_count <- full_join(
  aapi_filtered %>% count(anc_label = anc_label.x),
  aapi_filtered %>% count(anc_label = anc_label.y),
  by = "anc_label"
) %>%
  transmute(
    anc_label,
    tot_count = rowSums(cbind(n.x, n.y), na.rm = TRUE)
  ) %>%
  right_join(aapi_incl, by = "anc_label") %>% 
  arrange(tot_count) %>%
  mutate(unw_flag = ifelse(tot_count < 100, 1, 0))
message("there are 7 groups who don't meet ERI's screening threshold.")
screen_unw_count %>% filter(unw_flag == 1)

rm(aapi_filtered)

# Method 2: Try RC screening method, suppress data where rate_cv > cv_threshold OR num < pop_threshold (see top of script for values)
screen_rate_cv_pop <- table_state %>%
  filter(.[[indicator]] == indicator_val) %>%
  mutate(rate_cv_flag = ifelse(rate_cv > cv_threshold, 1, 0), 
         pop_flag = ifelse(pop < pop_threshold, 1, 0)) %>%  # screen on pop bc these are indicator data
  arrange(desc(rate_cv), desc(num))
nrow(screen_rate_cv_pop %>% filter(rate_cv_flag == 1 | pop_flag == 1))
message("15 suppressed estimates are for 'officials':")
screen_rate_cv_pop %>% filter(rate_cv_flag == 1 | pop_flag == 1)

# Six of the 7 estimates suppressed by ERI's method are also suppressed by our method.


#### Step 7: Screen data (incl. recoding suppressed subgroups & recalcs) ####
# STATE-LEVEL ONLY: Recode suppressed asian subgroups as other_asian and same for nhpi. If we present county data, we could recode any suppressed grps for that county too.

recode_asian <- screen_rate_cv_pop %>%
  filter(officials == 'officials'& (rate_cv_flag == 1 | pop_flag == 1)) %>%
  select(group, subgroup) %>%
  filter(group == 'asian')

recode_nhpi <- screen_rate_cv_pop %>%
  filter(officials == 'officials'& (rate_cv_flag == 1 | pop_flag == 1)) %>%
  select(group, subgroup) %>%
  filter(group == 'nhpi')

oth_asian_srvy <- ppl_state %>%
  mutate(subgroup = case_when(
    if_any(all_of(recode_asian$subgroup), ~ . == 1) ~ "other_asian",
    TRUE ~ "total")) %>%
  as_survey_rep(
    variables        = c(geoid, geoname, asian, subgroup, !!sym(indicator)),
    weights          = !!sym(weight),
    repweights       = all_of(repwlist),
    combined_weights = TRUE,
    mse              = TRUE,
    type             = "other",
    scale            = 4/80,
    rscale           = rep(1, 80)
  ) %>%
  filter(!is.na(!!sym(indicator))) %>%
  filter(subgroup == 'other_asian')

# Denominators (asian group-level stats) ────
den_oth_asian <- oth_asian_srvy %>%
  group_by(geoid, geoname) %>%
  summarise(pop = survey_total(na.rm = TRUE), .groups = "drop")


# Numerator
num_df_oth_asian <- oth_asian_srvy %>%
  group_by(geoid, geoname, !!sym(indicator)) %>%
  summarise(
    num  = survey_total(na.rm = TRUE),
    rate  = survey_mean(na.rm = TRUE),
    .groups = "drop"
  ) %>%
  
  # Join + metrics
  left_join(den_oth_asian, by = c("geoid", "geoname")) %>%
  mutate(
    subgroup  = 'other_asian',
    group     = 'asian',
    rate_moe  = rate_se * 1.645 * 100,
    rate_cv   = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
    rate      = rate * 100,
    count_moe = num_se * 1.645,
    count_cv  = ifelse(num > 0, (num_se / num) * 100, NA_real_)
  )


oth_nhpi_srvy <- ppl_state %>%
  mutate(subgroup = case_when(
    if_any(all_of(recode_nhpi$subgroup), ~ . == 1) ~ "other_pacific",
    TRUE ~ "total")) %>%
  as_survey_rep(
    variables        = c(geoid, geoname, nhpi, subgroup, !!sym(indicator)),
    weights          = !!sym(weight),
    repweights       = all_of(repwlist),
    combined_weights = TRUE,
    mse              = TRUE,
    type             = "other",
    scale            = 4/80,
    rscale           = rep(1, 80)
  ) %>%
  filter(!is.na(!!sym(indicator))) %>%
  filter(subgroup == 'other_pacific')

# Denominators (nhpi group-level stats) ────
den_oth_nhpi <- oth_nhpi_srvy %>%
  group_by(geoid, geoname) %>%
  summarise(pop = survey_total(na.rm = TRUE), .groups = "drop")


# Numerator
num_df_oth_nhpi <- oth_nhpi_srvy %>%
  group_by(geoid, geoname, !!sym(indicator)) %>%
  summarise(
    num  = survey_total(na.rm = TRUE),
    rate  = survey_mean(na.rm = TRUE),
    .groups = "drop"
  ) %>%
  
  # Join + metrics
  left_join(den_oth_nhpi, by = c("geoid", "geoname")) %>%
  mutate(
    subgroup  = 'other_pacific',
    group     = 'nhpi',
    rate_moe  = rate_se * 1.645 * 100,
    rate_cv   = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
    rate      = rate * 100,
    count_moe = num_se * 1.645,
    count_cv  = ifelse(num > 0, (num_se / num) * 100, NA_real_)
  )


# combine re-calc'd other_asian and pop_table_state, drop old 'other_asian' row
table_state <- rbind(num_df_oth_asian, num_df_oth_nhpi, table_state %>% filter(!subgroup %in% c('other_asian','other_pacific')))

# recheck state screening
screen_rate_cv_pop2 <- table_state %>%
  filter(.[[indicator]] == indicator_val) %>%
  mutate(rate_cv_flag = ifelse(rate_cv > cv_threshold, 1, 0), 
         pop_flag = ifelse(pop < pop_threshold, 1, 0)) %>%  # screen on pop bc these are indicator data
  arrange(desc(rate_cv), desc(num))
nrow(screen_rate_cv_pop2 %>% filter(rate_cv_flag == 1 | pop_flag == 1))
message("13 suppressed estimates for 'officials'. other_asian and other_pacific are now NOT suppressed.")
View(screen_rate_cv_pop2 %>% filter(rate_cv_flag == 1 | pop_flag == 1))
table_cs <- rbind(table_state, table_county) %>%
  rename('indicator' = indicator) %>%      # update to generic colname
  filter(indicator == indicator_val) %>%   # keep only the rows with indicator value we want, eg: keep connected, and drop  not connected
  filter(!is.na(geoid))                    # drop rows for responses that are not incl. in our county-xwalk (rows where geoid is NA)

# RC screening method (CV and pop thresholds), see screen_rate_cv_pop above
table_screened <- table_cs %>%
  mutate(rate = ifelse(rate_cv > cv_threshold | pop < pop_threshold, NA, rate), 
         raw = ifelse(rate_cv > cv_threshold | pop < pop_threshold, NA, num)) %>%
  arrange(desc(rate_cv), desc(num)) 

screened_out <- table_screened %>%
  filter(is.na(rate))

length(unique(table_screened$geoid))      # number of unique geos in final data, n = 37
table(subgroup = screened_out$subgroup)   # count of suppressed values by subgroup, some subgroups had no data reported for some counties which is not noted here.
geo_subgroup_combos <- table_screened %>%
  group_by(geoname) %>%
  summarise(unique_subgroups = n_distinct(subgroup))  # max = 43. up to 40 subgroups + 2 groups + 1 total

table_screened <- table_screened %>%
  select(-c(num_se, rate_se, pop_se)) %>%    # drop unneeded cols
  mutate(geolevel = ifelse(geoid == '06', 'state', 'county'))


############## ASIAN: CALC RACE COUNTS STATS ##############
race_name <- 'asian'  # this var is used to create the RC table name

d <- table_screened %>% filter(group %in% c(race_name, "total"))

d <- d %>%
  pivot_wider(id_cols = c(geoid, geoname, geolevel),
              names_from = subgroup,
              values_from = c(raw, rate, pop, rate_moe, rate_cv, count_moe, count_cv),
              names_glue = "{subgroup}_{.value}") %>%
  select(where(~!all(is.na(.))))      # drop cols where all values are NA


#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
View(state_table)

#split COUNTY into separate table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
View(county_table)

state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>%
  select(where(~!all(is.na(.))))    # drop cols where all values are NA


###update info for postgres tables###
county_table_name <- paste0(race_name, "_econ_officials_county_", rc_yr)
state_table_name <- paste0(race_name, "_econ_officials_state_", rc_yr)
indicator <- paste0("Number of Officials & Managers per 1k People by ", race_name, " Ancestry at state and county level. Only people ages 18-64 who are in the labor force are included. PUMAs are assigned to counties based on Geocorr 2022 crosswalks. We also screened by pop (", pop_threshold, ") and CV (", cv_threshold, "%). QA Doc:", qa_filepath, ". This data is")
source <- paste0("ACS PUMS (", start_yr, "-", curr_yr, ")")

#send tables to postgres
to_postgres(county_table,state_table,"mosaic")




############## NHPI: CALC RACE COUNTS STATS ##############
race_name <- 'nhpi'  # this var is used to create the RC table name

d <- table_screened %>% filter(group %in% c(race_name, "total"))

d <- d %>%
  pivot_wider(id_cols = c(geoid, geoname, geolevel),
              names_from = subgroup,
              values_from = c(raw, rate, pop, rate_moe, rate_cv, count_moe, count_cv),
              names_glue = "{subgroup}_{.value}") %>%
  select(where(~!all(is.na(.))))      # drop cols where all values are NA


#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
View(state_table)

#split COUNTY into separate table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
View(county_table)

state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>%
  select(where(~!all(is.na(.))))    # drop cols where all values are NA


###update info for postgres tables###
county_table_name <- paste0(race_name, "_econ_officials_county_", rc_yr)
state_table_name <- paste0(race_name, "_econ_officials_state_", rc_yr)
indicator <- paste0("Number of Officials & Managers per 1k People by ", race_name, " Ancestry at state and county level. Only people ages 18-64 who are in the labor force are included. PUMAs are assigned to counties based on Geocorr 2022 crosswalks. We also screened by pop (", pop_threshold, ") and CV (", cv_threshold, "%). QA Doc:", qa_filepath, ". This data is")
source <- paste0("ACS PUMS (", start_yr, "-", curr_yr, ")")

#send tables to postgres
to_postgres(county_table,state_table,"mosaic")

# #close connection
# dbDisconnect(con)
# dbDisconnect(con2)
