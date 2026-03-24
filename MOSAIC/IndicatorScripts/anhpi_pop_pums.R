# Disaggregated Asian and NHPI Pop from PUMS

# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2024.pdf

# Install packages if not already installed
packages <- c("tidyverse", "data.table", "readxl","tidycensus", "srvyr", "stringr", "openxlsx", "dplyr", "janitor") 

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(install_packages) > 0) {
  install.packages(install_packages)
} else {
  print("All required packages are already installed.")
}

for(pkg in packages){
  library(pkg, character.only = TRUE)
}

options(scipen = 999)

#SOURCE from the script that has: styling, packages, dbconnection, colors
source("W:\\RDA Team\\R\\credentials_source.R")
source("./MOSAIC/Functions/pums_fx.R")
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("mosaic")
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\QA_Sheet_MOSAIC_Pop_PUMS.docx"
ancestry_list <- read_excel("W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\Asian_NHPI_Ancestry_2024.xlsx", sheet = "ancestry") # list of ANHPI ANC1P/ANC2P codes

#### Step 1: Define Variables ####

# PUMS Data
root <- "W:/Data/Demographics/PUMS/CA_2020_2024/"
curr_yr <- 2024
start_yr <- curr_yr - 4
cv_threshold <- 30     # taken from Liv Wage
pop_threshold <- 400   # taken from Liv Wage
schema <- 'v7'

# set survey components
weight <- 'PWGTP'  # person weight
repwlist = rep(paste0(weight, 1:80)) # replicate weights


##### Step 2: GET PUMA CROSSWALKS ######
crosswalk <- dbGetQuery(con, "select county_id AS geoid, county_name AS geoname, geo_id AS puma, geo_name AS puma_name, num_county, afact, afact2 from crosswalks.puma_2022_county_2020")

## Drop counties that have only 1 PUMA that is shared with another county that also has only 1 PUMA.
dupe_pumas <- crosswalk %>% 
  filter(num_county == 1) %>%   # keep only counties with 1 PUMA
  group_by(puma) %>% 
  summarise(n=n()) %>%   		    # count the # of counties assigned to each PUMA
  filter(n > 1)		   		        # keep only counties that have only 1 PUMA that is shared with another county with only 1 PUMA

county_crosswalk <- crosswalk %>%   
  subset(!(puma %in% dupe_pumas$puma)) # remove only counties that have only 1 PUMA that is shared with another county with only 1 PUMA


##### Step 3: GET PUMS DATA ######
# Load ONLY the PUMS columns needed for this indicator
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0))    # get all PUMS cols 
cols_wts <- grep(paste0("^", weight, "*"), cols, value = TRUE)    # filter for PUMS weight colnames

people <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE,
                select = c(cols_wts, "RT", "SERIALNO", "PUMA", "RAC1P", "RAC3P", "RAC2P19", "RAC2P23", "HISP", "ANC1P", "ANC2P", "RACASN", "RACNH", "RACPI"),
                colClasses = list(character = c("PUMA", "RAC1P", "RAC3P", "RAC2P19", "RAC2P23", "HISP", "ANC1P", "ANC2P", "RACASN", "RACNH", "RACPI")))
orig_data <- people


# add nhpi value
people$RACNHPI <- case_when(
  people$RACNH == 1 | people$RACPI ==1 ~ 1,
  TRUE ~ 0) %>%
  as.character()

# Add state_geoid to people, add state_geoid to PUMA id, so it aligns with same vintage county-puma xwalk
people$state_geoid <- "06"
people$puma_id <- paste0(people$state_geoid, people$PUMA)

#### Step 4: Join subgroup labels to data ####
people <- anhpi_reclass(people, curr_yr, ancestry_list)  # returns list containing people (reclassified pums data) and aapi_incl (list of AAPI ancestries in data)
list2env(people, .GlobalEnv)

# Add a new column for each anc_label, populated with 1 or 0
for (label in aapi_incl$anc_label) {
  people[[label]] <- as.integer(people$anc_label.x == label | people$anc_label.y == label)
}

# check reclass worked
## ancestry code 603 = Bangladeshi ancestry. See: page 50 in https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2024.pdf
check_reclass <- people %>%
  filter((ANC1P == '603') |
           (ANC2P == '603')) %>%
  select(bangladeshi, anc_label.x, ANC1P, anc_label.y, ANC2P)
nrow(check_reclass) == nrow(check_reclass %>% filter(anc_label.x == 'bangladeshi' | anc_label.y == 'bangladeshi')) # Should be TRUE: check ancestry label columns against anc codes
nrow(check_reclass %>% filter(bangladeshi != 1 & (ANC1P == '603' | ANC2P == '603')))   # Should be 0: check bangladeshi binary column against anc codes

# Add a new column for any asian ancestry and same for nhpi, populated with 1 or 0
# Create a named lookup vector: anc_label -> asian value
asian_lookup <- setNames(aapi_incl$asian, aapi_incl$anc_label)

# Populate the new 'asian' column
people$asian <- as.integer(
  (people$anc_label.x %in% names(asian_lookup[asian_lookup == 1])) |
    (people$anc_label.y %in% names(asian_lookup[asian_lookup == 1]))
)

people$asian[people$ANC1P == '999' & people$ANC2P == '999'] <- NA # recode ancestry Not Reported to NA from 999

# Create a named lookup vector: anc_label -> nhpi value
nhpi_lookup <- setNames(aapi_incl$nhpi, aapi_incl$anc_label)

# Populate the new 'nhpi' column
people$nhpi <- as.integer(
  (people$anc_label.x %in% names(nhpi_lookup[nhpi_lookup == 1])) |
    (people$anc_label.y %in% names(nhpi_lookup[nhpi_lookup == 1]))
)

people$nhpi[people$ANC1P == '999' & people$ANC2P == '999'] <- NA  # recode ancestry Not Reported to NA from 999


#### Step 4.1: Check race/ancestry relationships ####
# check records where ancestry is Not Reported
race_names <- function(x){      # https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2024.pdf
  x <- x %>% mutate(race = case_when(
    x$RAC1P == 1 ~ 'White Alone',
    x$RAC1P == 2 ~ 'Black Alone',
    x$RAC1P == 3 ~ 'Am Ind Alone',
    x$RAC1P == 4 ~ 'AK Native Alone',
    x$RAC1P == 5 ~ 'AIAN other',
    x$RAC1P == 6 ~ 'Asian Alone',  
    x$RAC1P == 7 ~ 'NHPI Alone',   
    x$RAC1P == 8 ~ 'Other Race Alone',
    x$RAC1P == 9 ~ 'Multiracial',  
    TRUE ~ 'NA'
  ))
return(x)
}

# what pct of race alone groups (unweighted) have no ancestry data?
check_anc_nr2 <- people %>%
  select(RAC1P) %>%
    group_by(RAC1P) %>%
    summarise(count = n()) %>%
  left_join(check_anc_nr %>%
    group_by(RAC1P) %>%
    summarise(anc_na = n()), by = "RAC1P") %>%
    adorn_totals("row") %>%
    mutate(pct_anc_nr = anc_na / count * 100) %>%
  race_names()  # 'Asian Alone', 17.8% have no ancestry data, slightly lower than avg (19.3%)
                # 'NHPI Alone', 27.5% have no ancestry data, higher than avg (19.3%)
                # 'Multiracial', 16.5% have no ancestry data, lower than avg. we know many NHPI are in this group.
 
# what % of Asian AOIC and NHPI AOIC (unweighted) have no ancestry data?
message("Among those with no ancestry data...")
table(check_anc_nr = check_anc_nr$RACASN) %>%
  setNames(c('Not Asian', 'Asian AOIC')) # 1 = Asian AOIC, 0 = Not Asian race. 67,506 Asian AOIC have no anc data.
message("Among those with no ancestry data...")
table(check_anc_nr = check_anc_nr$RACNHPI) %>%
  setNames(c('Not NHPI', 'NHPI AOIC'))   # 1 = NHPI AOIC, 0 = Not NHPI race. 3,631 NHPI AOIC have no anc data.



# check how many Asian and NHPI ancestry are Multiracial
asian_multir <- people %>% select(asian, RAC1P) %>% 
  race_names() %>%
  group_by(RAC1P, race, asian) %>%
  summarise(count_asian_anc = n()) %>%
  filter(asian == 1) %>%
  left_join(people %>% select(asian, RAC1P) %>%
              group_by(asian) %>%
              summarise(all_asian_anc = n()), by = "asian") %>%
  mutate(pct_asian_anc = count_asian_anc / all_asian_anc * 100)  # 10.2% of Asian ancestry are Multiracial
asian_multir

nhpi_multir <- people %>% select(nhpi, RAC1P) %>% 
  race_names() %>%
  group_by(RAC1P, race, nhpi) %>%
  summarise(count_nhpi_anc = n()) %>%
  filter(nhpi == 1) %>%
  left_join(people %>% select(nhpi, RAC1P) %>%
              group_by(nhpi) %>%
              summarise(all_nhpi_anc = n()), by = "nhpi") %>%
  mutate(pct_nhpi_anc = count_nhpi_anc / all_nhpi_anc * 100)     # 41.4% of nhpi ancestry are Multiracial
nhpi_multir

asian_aoic <- people %>% select(asian, RACASN) %>% 
  group_by(RACASN, asian) %>%
  summarise(count_asian_anc = n()) %>%
  left_join(people %>% select(asian, RACASN) %>%
              group_by(RACASN) %>%
              summarise(all_asian_aoic = n()), by = "RACASN") %>%
  mutate(pct_asian_anc = count_asian_anc / all_asian_aoic * 100) %>%  # 77.3% of Asian AOIC have asian ancestry, 17.9% have no ancestry
  filter(RACASN == 1)
asian_aoic

nhpi_aoic <- people %>% select(nhpi, RACNHPI) %>% 
  group_by(RACNHPI, nhpi) %>%
  summarise(count_nhpi_anc = n()) %>%
  left_join(people %>% select(nhpi, RACNHPI) %>%
              group_by(RACNHPI) %>%
              summarise(all_nhpi_aoic = n()), by = "RACNHPI") %>%
  mutate(pct_nhpi_anc = count_nhpi_anc / all_nhpi_aoic * 100) %>%  # 47.1% of NHPI AOIC have nhpi ancestry, 28.2% have non-nhpi ancestry, 24.7% have no ancestry
  filter(RACNHPI == 1)
nhpi_aoic

## check a few of the new ancestry & asian/nhpi cols
table(chinese = people$chinese, asian_race = people$RACASN)  # check how many chinese ancestry rows are also marked Asian race
table(chinese = people$chinese, asian_anc = people$asian)    # check that all chinese ancestry rows are also marked asian ancestry
table(asian_anc = people$asian, asian_race = people$RACASN)  # 4,021 people (unw) w/ asian ancestry who are not coded race = Asian
#           asian_race
# chinese         0        1
#        0  1489923   296800
#        1      539    79801   # 539 people (unw) with chinese ancestry are not marked Asian AOIC
no_asian_anc <- people %>% filter(asian == 0 & RACASN == '1')   # Asian AOIC records w/o asian ancestry, more than half have anc_label.y = not_reported
no_asian_unique_values <- unique(c(no_asian_anc$anc_label.x, no_asian_anc$anc_label.y))


table(samoan = people$samoan, nhpi_race = people$RACNHPI)    # check how many samoan ancestry rows are also marked NHPI race
table(samoan = people$samoan, nhpi_anc = people$nhpi)        # check that all samoan ancestry rows are also marked nhpi ancestry
table(nhpi_anc = people$nhpi, nhpi_race = people$RACNHPI)    # 814 people (unw) w/ nhpi ancestry who are not coded race = NHPI
#           nhpi_race
# samoan         0      1
#        0 1852295  13229
#        1      58   1481   # 58 people (unw) with samoan ancestry are not marked NHPI AOIC
no_nhpi_anc <- people %>% filter(nhpi == 0 & RACNHPI == '1')   # NHPI AOIC records w/o nhpi ancestry, more than half have anc_label.y = not_reported
no_nhpi_unique_values <- unique(c(no_nhpi_anc$anc_label.x, no_nhpi_anc$anc_label.y)) 

# For this analysis, we include anyone with an Asian ancestry and anyone with an NHPI ancestry, regardless of race.
## E.g. For NHPI, we include all records where nhpi == 1 regardless of RACNHPI value.


#### Step 5: Join crosswalks to data ####
# join county crosswalk to data: county, puma, state
ppl_cs <- left_join(people, county_crosswalk, by=c("puma_id" = "puma"))   # join FILTERED county-puma crosswalk

ppl_state <- people %>% rename(geoid = state_geoid) %>%
  mutate(geoname = 'California')


#### Step 6: Set up for PUMS calc fx ####
# get list of subgroups for calcs based on aapi_incl
vars <- aapi_incl %>% pull(anc_label)
vars

## STATE
# run fx to create survey and calc pop rate denominators
state_list <- pums_pop_srvy_denom(ppl_state, weight, repwlist, vars)

# add list elements to Environment - these df's are used in calc_pums_pop fx
list2env(state_list, envir = .GlobalEnv)

# run PUMS calcs
pop_table_state <- map(vars, calc_pums_pop) |> list_rbind() %>%
  rbind(num_df_group)   # add any asian ancestry and any nhpi ancestry pop data
#rm(ppl_state)


## COUNTY
# run fx to create survey and calc pop rate denominators
county_list <- pums_pop_srvy_denom(ppl_cs, weight, repwlist, vars)

# add list elements to Environment - these df's are used in calc_pums_pop fx
list2env(county_list, envir = .GlobalEnv)

# run PUMS calcs
pop_table_county <- map(vars, calc_pums_pop) |> list_rbind() %>%
  rbind(num_df_group) %>%  # add any asian ancestry and any nhpi ancestry pop data
  filter(!is.na(geoid))    # drop rows with summary data for PUMAs that are excluded from our filtered xwalk
#rm(ppl_cs)


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
  # there are 3 groups who don't meet ERI's screening threshold: bhutanese (18), micronesian (63), marshallese (70)
  
  rm(aapi_filtered)
  
# Method 2: Try RC screening method, suppress data where rate_cv > cv_threshold OR num < pop_threshold (see top of script for values)
  screen_rate_cv_pop <- pop_table_state %>%
    mutate(rate_cv_flag = ifelse(rate_cv > cv_threshold, 1, 0), 
           pop_flag = ifelse(num < pop_threshold, 1, 0)) %>%  # screen on num not pop bc these are pop data, not indicator data
    arrange(desc(rate_cv), desc(num))
  # there is 1 group who doesn't meet rate_cv threshold: Bhutanese (43.5). there is 1 group not meeting pop_threshold: Bhutanese (pop is 234)

  
#### Step 7: Screen data (incl. recoding suppressed subgroups & recalcs) ####
# STATE-LEVEL ONLY: Recode Bhutanese as other_asian. If we present county data, we could recode any suppressed grps for that county too.
  oth_asian_srvy <- ppl_state %>%
    filter(asian == 1) %>%
    mutate(subgroup = case_when(
      bhutanese == 1 | other_asian == 1 ~ 'other_asian',  # recode bhutanese as oth_asian
      TRUE ~ 'total')) %>%                                # recode non-bhutanese as total
    as_survey_rep(
      variables = c(geoid, geoname, subgroup), # dplyr::select grouping variables.
      weights = weight,                       # person weight
      repweights = all_of(repwlist),          # list of replicate weights
      combined_weights = TRUE,                # tells the function that replicate weights are included in the data
      mse = TRUE,                             # tells the function to calc mse
      type="other",                           # statistical method
      scale=4/80,                             # scaling set by ACS
      rscale=rep(1,80)                        # setting specific to ACS-scaling
    )
  
  # Numerator
  num_df_oth_asian <- oth_asian_srvy %>%
    group_by(geoid, geoname) %>%
    summarise(
      num  = survey_total(subgroup == 'other_asian', na.rm = TRUE),
      rate = survey_mean(subgroup == 'other_asian', na.rm = TRUE),
      .groups = "drop"
    ) %>%
    
    # Join + metrics
    left_join(state_list$den_total %>% filter(pop_group == 'asian'), by = c("geoid", "geoname")) %>%
    mutate(
      subgroup  = 'other_asian',
      group     = 'asian',
      rate_moe  = rate_se * 1.645 * 100,
      rate_cv   = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
      rate      = rate * 100,
      count_moe = num_se * 1.645,
      count_cv  = ifelse(num > 0, (num_se / num) * 100, NA_real_)
    )
  
# combine re-calc'd other_asian and pop_table_state, drop old 'other_asian' and 'bhutanese' rows
  pop_table_state <- pop_table_state_ %>% filter(!subgroup %in% c('other_asian', 'bhutanese')) %>%
    bind_rows(num_df_oth_asian) 
  pop_table <- rbind(pop_table_state, pop_table_county)  # combine state and county data
  
# RC screening method (CV and pop thresholds), see screen_rate_cv_pop above
  pop_table_screened <- pop_table %>%
    mutate(rate = ifelse(rate_cv > cv_threshold | num < pop_threshold, NA, rate),    # screen on num not pop bc these are pop data, not indicator data
           num = ifelse(rate_cv > cv_threshold | num < pop_threshold, NA, num)) %>%
    arrange(desc(rate_cv), desc(num)) 
  
  screened_out <- pop_table_screened %>%
    filter(is.na(rate))

  length(unique(pop_table_screened$geoid))  # number of unique geos in final data, n = 38
  table(subgroup = screened_out$subgroup)   # count of suppressed values by subgroup
  
  pop_table_screened <- pop_table_screened %>%
    select(-c(pop_group, num_se, rate_se, pop_se)) %>%    # drop unneeded cols
    rename(group_ = group)
  
  pop_table_final <- pop_table_screened %>%
    filter(!is.na(rate)) 
  
#### Step 8: Send data to postgres ####
table_name <- 'anhpi_pop_pums'
indicator <- "Disaggregated Asian & NHPI Ancestry Population"
source <- paste0("American Community Survey ", start_yr, "-", curr_yr, " 5-year PUMS estimates for Asian & NHPI Ancestry at state and county level. Note: Bhutanese is included in Other Asian at state level")
column_names <- colnames(pop_table_final) # n = 11
column_comments <- c('fips code', 
                     '', 
                     'number of people in subgroup', 
                     'subgroup (ancestry) as percent of group pop (any Asian or any NHPI ancestry), where subgroup is asian or nhpi: any asian or nhpi ancestry as percent of total pop',
                     'group pop (any Asian or any NHPI ancestry), where subgroup is asian or nhpi: total pop', 
                     'Ancestry derived from ANC1P/ANC2P, where subgroup is asian or nhpi: any asian or any nhpi ancestry',
                     'asian = any asian ancestry, nhpi = any nhpi ancestry', 
                     'margin of error for rate', 
                     'coefficient of variation for rate', 
                     'moe for num', 
                     'cv for num')

dbWriteTable(con2, 
             Id(schema = schema, table = table_name),
             pop_table_final, overwrite = FALSE)

# comment on table and columns
add_table_comments(con2, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#disconnect
dbDisconnect(con)
dbDisconnect(con2)