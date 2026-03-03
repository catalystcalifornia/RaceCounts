# Disaggregated Asian and NHPI Pop from PUMS

# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf

# Install packages if not already installed
packages <- c("tidyverse", "data.table", "readxl","tidycensus", "srvyr","stringr", "openxlsx", "dplyr") 

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(install_packages) > 0) {
  install.packages(install_packages)
} else {
  print("All required packages are already installed.")
}

for(pkg in packages){
  library(pkg, character.only = TRUE)
}

#SOURCE from the script that has: styling, packages, dbconnection, colors
source("W:\\RDA Team\\R\\credentials_source.R")
source("./MOSAIC/Functions/pums_fx.R")
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("mosaic")
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\QA_Sheet_MOSAIC_Pop_PUMS.docx"
ancestry_list <- read_excel("W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\Asian_NHPI_Ancestry.xlsx", sheet = "ancestry") # list of ANHPI ANC1P/ANC2P codes

#### Step 1: Define Variables ####

# PUMS Data
root <- "W:/Data/Demographics/PUMS/CA_2019_2023/"
indicator_name <- "Disaggregated Asian"
curr_yr <- 2023
start_yr <- curr_yr - 4
# created this excel document separate by opening PUMS Data Dictionary in excel and deleting everything but RAC3P, 
## updating col names, using text-to-columns to split description into separate cols. Adding binary cols for each subgroup.
data_dict <- paste0(root, "PUMS_Data_Dictionary_2019-2023_RAC3P.xlsx")
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
  TRUE ~ 0
)

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
table(chinese = people$chinese, asian_race = people$RACASN)  # check how many chinese ancestry rows are also marked Asian race
table(chinese = people$chinese, asian_anc = people$asian)    # check that all chinese ancestry rows are also marked asian ancestry
table(asian_anc = people$asian, asian_race = people$RACASN)  # 4,452 people w/ asian ancestry who are not coded race = Asian
#         asian_race
# asian_anc       0       1
#         0 1481976   79536
#         1    4452  287465

table(samoan = people$samoan, nhpi_race = people$RACNHPI)    # check how many samoan ancestry rows are also marked NHPI race
table(samoan = people$samoan, nhpi_anc = people$nhpi)        # check that all samoan ancestry rows are also marked nhpi ancestry
table(nhpi_anc = people$nhpi, nhpi_race = people$RACNHPI)    # 941 people w/ nhpi ancestry who are not coded race = NHPI
#       nhpi_race
# nhpi_anc       0       1
#       0 1838025    7536
#       1     941    6927

# For this analysis, we consider anyone with an Asian ancestry and anyone with an NHPI ancestry, regardless of race.
## E.g. For NHPI, we include all records where nhpi == 1 regardless of RACNHPI value.


#### Step 5: Join crosswalks to data ####
# join county crosswalk to data: county, puma, state
ppl_cs <- left_join(people, county_crosswalk, by=c("puma_id" = "puma"))   # join FILTERED county-puma crosswalk

ppl_state <- people %>% rename(geoid = state_geoid) %>%
  mutate(geoname = 'California')

# ppl_puma <- left_join(people, county_crosswalk %>%
#                       select(puma, puma_name), by=c("puma_id" = "puma")) %>%  # join puma names
#   rename(geoid = puma_id, geoname = puma_name)


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
pop_table_state <- map_dfr(vars, calc_pums_pop) %>%
  rbind(num_df_group)   # add any asian ancestry and any nhpi ancestry pop data
#rm(ppl_state)

## COUNTY
# run fx to create survey and calc pop rate denominators
county_list <- pums_pop_srvy_denom(ppl_cs, weight, repwlist, vars)

# add list elements to Environment - these df's are used in calc_pums_pop fx
list2env(county_list, envir = .GlobalEnv)

# run PUMS calcs
pop_table_county <- map_dfr(vars, calc_pums_pop) %>%
  rbind(num_df_group)   # add any asian ancestry and any nhpi ancestry pop data
#rm(ppl_cs)

## PUMA
# run fx to create survey and calc pop rate denominators
# puma_list <- pums_pop_srvy_denom(ppl_puma, weight, repwlist, vars)
# 
# # add list elements to Environment - these df's are used in calc_pums_pop fx
# list2env(puma_list, envir = .GlobalEnv)
# 
# # run PUMS calcs
# pop_table_puma <- map_dfr(vars, calc_pums_pop) %>%
#   rbind(num_df_group)   # add any asian ancestry and any nhpi ancestry pop data
#rm(ppl_puma)


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
    arrange(tot_count)
  # there are 3 groups who don't meet ERI's screening threshold: bhutanese (17), micronesian (70), marshallese (71)
  
  rm(aapi_filtered)
  
  # Method 2: Try RC screening method, suppress data where rate_cv > cv_threshold OR num < pop_threshold (see top of script for values)
  screen_rate_cv_pop <- pop_table_state %>%
    mutate(rate_cv_flag = ifelse(rate_cv > cv_threshold, 1, 0), 
           pop_flag = ifelse(num < pop_threshold, 1, 0)) %>%
    arrange(desc(rate_cv), desc(num))
  # there are 0 groups who don't meet our rate_cv threshold. there is 1 group not meeting pop_threshold: Bhutanese (pop is 212)

  
#### Step 7: Screen data (incl. recoding suppressed subgroups & recalcs) ####
# STATE-LEVEL ONLY: Recode Bhutanese as other_asian. If we present county data, we could recode any suppressed grps for that county too.
  oth_asian_srvy <- ppl_state %>%
    mutate(subgroup = case_when(
      bhutanese == 1 | other_asian == 1 ~ 'other_asian',  # recode bhutanese as oth_asian
      TRUE ~ 'total')) %>%                            # recode non-bhutanese as total
    as_survey_rep(
      variables = c(geoid, geoname, subgroup), # dplyr::select grouping variables.
      weights = weight,                       # person weight
      repweights = repwlist,                  # list of replicate weights
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
    left_join(state_list$den_asian, by = c("geoid", "geoname")) %>%
    mutate(
      subgroup  = 'other_asian',
      group     = 'asian',
      rate      = rate * 100,
      rate_moe  = rate_se * 1.645 * 100,
      rate_cv   = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
      count_moe = num_se * 1.645,
      count_cv  = ifelse(num > 0, (num_se / num) * 100, NA_real_)
    )
  
# combine re-calc'd other_asian and pop_table_state, drop old 'other_asian' row
  pop_table_state <- rbind(num_df_oth_asian, pop_table_state %>% filter(subgroup != 'other_asian'))
  
  pop_table <- rbind(pop_table_state, pop_table_county)
  
# RC screening method (CV and pop thresholds), see screen_rate_cv_pop above
  pop_table_screened <- pop_table %>%
    mutate(rate = ifelse(rate_cv > cv_threshold | num < pop_threshold, NA, rate), 
           num = ifelse(rate_cv > cv_threshold | num < pop_threshold, NA, num)) %>%
    arrange(desc(rate_cv), desc(num)) 
  
  screened_out <- pop_table_screened %>%
    filter(is.na(rate))

  length(unique(pop_table_screened$geoid))  # number of unique geos in final data, n = 38
  table(subgroup = screened_out$subgroup)   # count of suppressed values by subgroup
  
  pop_table_screened <- pop_table_screened %>%
    select(-c(num_se, rate_se, pop_se))     # drop unneeded cols
  
#### Step 8: Send data to postgres ####
table_name <- 'anhpi_pop_pums'
indicator <- "Disaggregated Asian & NHPI Ancestry Population"
source <- paste0("American Community Survey 2019-2023 5-year PUMS estimates for Asian & NHPI Ancestry at state and county level")
column_names <- colnames(pop_table_screened) # n = 11
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
             pop_table_screened, overwrite = FALSE)

# comment on table and columns
add_table_comments(con2, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


#disconnect
dbDisconnect(con)
dbDisconnect(con2)