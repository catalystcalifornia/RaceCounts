######### Eviction Filings (WA) for RC v7 #########

##install packages if not already installed ------------------------------
packages <- c("dplyr", "data.table", "readxl", "tidycensus", "sf", "DBI", "RPostgres", "tidyr", "tigris", "usethis", "readr", "rvest", "tidyverse", "stringr", "here")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen=999)


###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

#set source for RC Functions script
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")  # temporary re-direct to LF local

#set QA filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_Sheet_Eviction.docx"

# update each year: variables used throughout script
data_yrs <- c('2014', '2015', '2016', '2017', '2018')  # Note: 2018 filtered out later bc there is only data for 1 county
acs_yr <- 2020
rc_schema <- 'v7'
rc_yr <- '2025'

# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with Senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table


# Check that B25003 variables and RC race names still match each year and update if needed ####
## Renter Households: total, black, aian, asian, pacisl, other, twoormor, nh_white, latinx (All except Two+ and Latinx are 1 race alone, all except Latinx are non-Latinx.)
vars_list_b25003 <- list("total_" = "B25003_003", 
                         "black_" = "B25003B_003", 
                         "aian_" = "B25003C_003", 
                         "asian_" = "B25003D_003", 
                         "pacisl_" = "B25003E_003", 
                         "other_" = "B25003F_003", 
                         "twoormor_" = "B25003G_003", 
                         "nh_white_" = "B25003H_003", 
                         "latino_" = "B25003I_003")

race_mapping <- data.frame(
  name = unlist(vars_list_b25003),
  race = names(vars_list_b25003),
  stringsAsFactors = FALSE
)

b25003_curr <- load_variables(acs_yr, "acs5", cache = TRUE) %>% 
  filter(name %in% vars_list_b25003) %>%
  dplyr::select(-c(geography)) %>% 
  left_join(race_mapping, by="name") %>%
  dplyr::mutate(rc_races = paste0(race, "pop"))


# CHECK THIS TABLE TO MAKE SURE THE CONCEPT AND RC_RACES COLUMNS MATCH UP
print(b25003_curr) 


# Load the data & prep data for weighted average function
#### data in the 'valid' file has already been screened for large year-on-year fluctuations by Evictions Lab
df_orig <- fread("W:/Data/Housing/Eviction Lab/2000-2018/tract_proprietary_valid_2000_2018.csv", header = TRUE, data.table = FALSE)  

df <- df_orig %>% dplyr::filter((state == "California") & grepl(paste(data_yrs, collapse="|"), year)) %>% 
  dplyr::mutate(county_id = paste0("0", cofips), county_name = gsub(" County", "", county), fips = paste0("0", fips)) %>% 
  dplyr::select(fips, county_id, county_name, year, filings) 
# View(df)


######### County Screening / Data Exploration ##########
# get count of ct's per county for context
census_api_key(census_key1, overwrite=FALSE) # In practice, may need to include install=TRUE if switching between census api keys
# Sys.getenv("CENSUS_API_KEY")
cts <- get_acs(geography = "tract", 
               variables = c("B19013_001"), 
               state = "CA", 
               year = acs_yr)
cts <- cts %>% dplyr::select(GEOID, NAME)
cts <- cts %>%
  dplyr::mutate(county_id = stringr::str_extract(GEOID, "^.{5}")) %>%
  dplyr::group_by(county_id) %>%
  dplyr::summarise(n = dplyr::n(), .groups = "drop")  # dplyr::summarise is cleaner thandplyr::mutate + distinct

# cts <- cts[,1:2]
# cts <- cts %>%dplyr::mutate(county_id = str_extract(GEOID, "^.{5}")) %>% 
#   dplyr::group_by(county_id) %>%dplyr::mutate(n_ct = n()) %>% 
#   distinct(county_id, .keep_all = TRUE) %>% 
#   dplyr::select(-c(GEOID, NAME))
# View(cts)

# get median # of non-na filing values grouped by county/year. then calc diff from median #. then calc % diff from median.
med <- na.omit(df) %>%
  # dplyr::filter(!is.na(filings)) %>%                          # remove NA filings
  dplyr::group_by(county_id, county_name, year) %>%           # group by all three
  dplyr::summarise(non_na_count = n(), .groups = "drop") %>%  # count rows per group
  dplyr::group_by(county_id, county_name) %>%                 # regroup by county
  dplyr::mutate(
    med_non_na_count = median(non_na_count),
    diff_from_med = non_na_count - med_non_na_count,
    pct_diff_from_med = diff_from_med / med_non_na_count * 100
  ) %>%
  dplyr::ungroup()
# med <- na.omit(df) %>% dplyr::group_by(county_id, county_name) %>% count(county_id, year) %>% 
#  dplyr::rename(non_na_count = n) %>% 
#  dplyr::mutate(med_non_na_count = median(non_na_count)) %>% 
#  dplyr::mutate(diff_from_med = non_na_count - med_non_na_count) %>% 
#  dplyr::mutate(pct_diff_from_med = diff_from_med / med_non_na_count * 100)
# round numeric values. join total # of cts per county.
med <- med %>%dplyr::mutate_if(is.numeric, ~round(., 1)) %>% left_join(cts, by = "county_id")
# View(med)

# get count of data yrs with non-na filing_rate by county.    
data_yrs <- filter(med, !year == 2018)  # remove 2018 data since they only report it for 1 county (SF)
data_yrs <- data_yrs %>% dplyr::group_by(county_id, county_name) %>% 
  dplyr::mutate(num_yrs = n()) %>% 
  distinct(county_id, county_name, .keep_all = TRUE) %>% 
  dplyr::select(county_id, county_name, num_yrs)
# View(data_yrs)
# summary(data_yrs)

df_join <- df %>% inner_join(data_yrs, by = c("county_id", "county_name")) # n = 52

screened <- filter(df_join, num_yrs > 2) # suppress data for counties with fewer than 3 years of data, n = 39
# View(screened)
# unique(screened$county_name)   # 39 counties pass num_yrs screening

##### CONVERT DATA FROM 2010-2020 TRACTS ######

# calc sum and avg # of evictions by tract (2010)
df_wide <- screened %>% dplyr::group_by(fips, county_name) %>%
 dplyr::mutate(sum_eviction = sum(filings, na.rm = TRUE)) %>%  # total number of evictions over all data yrs available
 dplyr::mutate(avg_eviction = sum_eviction / num_yrs) %>%      # avg annual number of evictions
 distinct(fips, county_id, county_name, sum_eviction, avg_eviction, num_yrs, .keep_all = FALSE)

df_wide <- filter(df_wide, sum_eviction != 0) # screen out tracts (n = 341) where all filings for all data years = NA, since these should be NA not 0's. there are no 0's in orig. data.

ind_df_2010 <- df_wide %>% dplyr::rename(c("target_id" = "county_id", "target_name" = "county_name", "sub_id" = "fips")) #dplyr::rename fields for WA fx
View(ind_df_2010)

#convert 2010 tracts to 2020 tracks then use the 2020 tract to leg crosswalk
# note: Evictions uses 2010 tract vintage, population estimates use 2020 tract vintage
cb_tract_2010_2020 <- fread("W:\\Data\\Geographies\\Relationships\\tract20_tract10\\cb_tract2020_tract2010_st06.txt", sep="|", colClasses = 'character', data.table = FALSE) %>%
  dplyr::select(GEOID_TRACT_10, NAMELSAD_TRACT_10, AREALAND_TRACT_10, GEOID_TRACT_20, NAMELSAD_TRACT_20, AREALAND_TRACT_20, AREALAND_PART) %>%
  dplyr::mutate_at(vars(contains("AREALAND")), function(x) as.numeric(x)) %>%
  # calculate overlapping land area of 2010 and 2020 tracts (AREALAND_PART) as a percent of 2010 tract land area (AREALAND_TRACT_10)
  dplyr::mutate(prc_overlap=AREALAND_PART/AREALAND_TRACT_10)

# Because Evictions uses 2010 vintage tracts - need to convert to 2020 vintage and allocate score accordingly
ind_2010_2020 <- ind_df_2010 %>%
  left_join(cb_tract_2010_2020, by=c("sub_id"="GEOID_TRACT_10")) %>%
  dplyr::select(sub_id, sum_eviction, num_yrs, AREALAND_TRACT_10, GEOID_TRACT_20, AREALAND_TRACT_20, AREALAND_PART, prc_overlap)  %>%
  # Allocate Evictions from 2010 tracts to 2020 using prc_overlap
 dplyr::mutate(eviction_20=sum_eviction*prc_overlap)

# # check prc_overlaps sums/ note: there will be prc_overlap values > 1 bc some 2010 tracts were split into 2+ 2020 tracts and each 2020 tract comes solely from the 2010 tract 
# check_prc_is_1 <- cb_tract_2010_2020 %>%
#   dplyr::group_by(GEOID_TRACT_10) %>%
#   dplyr::summarise(total_prc=sum(prc_overlap))

# create indicator df (2020 tracts) to be used in WA calcs
ind_2020 <- ind_2010_2020 %>%
  dplyr::group_by(GEOID_TRACT_20) %>%
  # sum weighted evictions by 2020 tract
  dplyr::summarize(sum_eviction = sum(eviction_20),
            num_yrs = min(num_yrs)) %>% 
  # clean up names
 dplyr::rename(sub_id = GEOID_TRACT_20) %>%
  # calc 4-yr avg eviction rate (2014-2017)
 dplyr::mutate(avg_eviction = sum_eviction / num_yrs)

ind_df <- ind_2020 #dplyr::rename to ind_df for WA fx

############# COUNTY CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold <- 200              # define minimum pop threshold for pop screen

##### CREATE COUNTY GEOID & NAMES TABLE ###
# You will NOT need this chunk if your indicator data table has target geolevel names already
targetgeo_names <- county_names(vars_list_b25003, yr = acs_yr, srvy = survey)

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop
pop <- lapply(pop, function(x) x %>% dplyr::rename(e = estimate, m = moe))

# transform pop data to wide format 
pop_wide <- to_wide(pop)

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>%
  dplyr::mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              #dplyr::rename to generic column name for WA functions

##### GET TARGET GEOLEVEL POP DATA ###
pop_df <- targetgeo_pop(pop_wide)

##### EXTRA STEP: Calc avg annual evictions per 100 renter hh's (rate) by tract bc WA avg should be calc'd using this, not avg # of evictions (raw)
ind_df <- ind_df %>% 
  left_join(pop_df %>% 
  dplyr::select(sub_id, total_sub_pop), by = "sub_id") %>% 
  dplyr::mutate(indicator = (avg_eviction / total_sub_pop) * 100)

##### COUNTY WEIGHTED AVG CALCS ######
pct_df <- pop_pct(pop_df)             # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df, ind_df)          # calc weighted average and apply reliability screens
wa <- wa %>% 
  left_join(targetgeo_names, by = "target_id") %>% 
  dplyr::mutate(geolevel = 'county')     # add in target geolevel names

############# STATE CALCS ##################

############### CUSTOMIZED VERSION OF CA_POP_WIDE FUNCTION HERE THAT WORKS WITH OWNER HOUSEHOLD AS POP BASIS ###
# custom ca_pop_wide calcs
ca_pop <- do.call(rbind.data.frame, list(
  get_acs(geography = "state", state = "CA", variables = vars_list_b25003, year = acs_yr, survey = survey, cache_table = TRUE)))
ca_pop <- ca_pop %>%  
 dplyr::mutate(geolevel = "state")
ca_pop_wide <- dplyr::select(ca_pop, GEOID, NAME, variable, estimate) %>% 
  pivot_wider(names_from=variable, values_from=estimate, names_glue = "{variable}target_pop")
ca_pop_wide <- ca_pop_wide %>% 
  dplyr::rename("target_id" = "GEOID", "target_name" = "NAME")

ca_pct_df <- ca_pop_pct(ca_pop_wide)

# calc state WA
ca_wa <- ca_wt_avg(ca_pct_df, ind_df) %>%
  dplyr::mutate(geolevel = 'state')   # add geolevel type

############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')      # define your target geolevel: city (state is handled separately)
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 200               # define population threshold for screening


#### CT-Place Crosswalk ###
xwalk_city <- st_read(con, query = paste0("SELECT * FROM crosswalks.ct_place_", acs_yr)) # comment out code above after xwalk is created and pull in postgres table instead.

##### GET SUB GEOLEVEL POP DATA ###
census_api_key(census_key1)       # reload census API key

####### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop
pop <- lapply(pop, function(x) x %>%dplyr::rename(e = estimate, m = moe))

# transform pop data to wide format 
pop_wide_city <- to_wide(pop)

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide_city <- as.data.frame(pop_wide_city) %>% 
  right_join(select(xwalk_city, c(ct_geoid, place_geoid, place_name)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names, length(unique(pop_wide_city$GEOID)) = 7,753 bc some cts aren't matched to cities in xwalk
pop_wide_city <- dplyr::rename(pop_wide_city, sub_id = GEOID, target_id = place_geoid, target_name = place_name) #dplyr::rename to generic column names for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df_city <- targetgeo_pop(pop_wide_city)


##### CITY WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df_city)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df, ind_df)     # calc weighted average and apply reliability screens
city_wa <- city_wa %>% left_join(distinct(xwalk_city, place_geoid, place_name), by = c("target_id" = "place_geoid"))  # add in target geolevel names
city_wa <- city_wa %>% dplyr::rename(target_name = place_name) %>%
  dplyr::mutate(geolevel = 'city')                                 # change place_name to target_name, add geolevel


############# ASSEMBLY CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ###
# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldl')      # define your target geolevel: state assembly
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 200              # define population threshold for screening

### CT-Assm Crosswalk ##
# Import CT-Assm Crosswalk
xwalk_assm <- dbGetQuery(con, paste0("SELECT geo_id AS ct_geoid, ", assm_geoid, " AS assm_geoid, afact, afact2 FROM crosswalks.", assm_xwalk)) %>%
  filter(afact >= .25 | afact2 >= .25)  # screen xwalk based on pct of ct pop in dist OR pct of dist pop in ct

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop
pop <- lapply(pop, function(x) x %>% dplyr::rename(e = estimate, m = moe))

# transform pop data to wide format
pop_wide <- to_wide(pop)

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide_assm <- as.data.frame(pop_wide) %>% 
  right_join(select(xwalk_assm, c(ct_geoid, assm_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide_assm <- dplyr::rename(pop_wide_assm, sub_id = GEOID, target_id = assm_geoid) #dplyr::rename to generic column names for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df_assm <- targetgeo_pop(pop_wide_assm)


##### ASSM WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df_assm)     # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
assm_wa <- wt_avg(pct_df, ind_df)        # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% 
  dplyr::mutate(geolevel = 'sldl')  # add geolevel

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
assm_name <- get_acs(geography = "State Legislative District (Lower Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = acs_yr)

assm_name <- assm_name[,1:2]
assm_name$NAME <- str_remove(assm_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
assm_name$NAME <- gsub(", California", "", assm_name$NAME)
names(assm_name) <- c("target_id", "target_name")
# # View(assm_name)

#add geonames to WA
assm_wa <- merge(x=assm_name,y=assm_wa,by="target_id", all=T)
## View(assm_wa)

############# SENATE CALCS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldu')      # define your target geolevel: state senate
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 200              # define population threshold for screening

### CT-Sen Crosswalk ##
# Import CT-Sen Crosswalk
xwalk_sen <- dbGetQuery(con, paste0("SELECT geo_id AS ct_geoid, ", sen_geoid, " AS sen_geoid, afact, afact2 FROM crosswalks.", sen_xwalk)) %>%
  filter(afact >= .25 | afact2 >= .25)  # screen xwalk based on pct of ct pop in dist OR pct of dist pop in ct

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_b25003, yr = acs_yr, srvy = survey)  # subgeolevel pop
pop <- lapply(pop, function(x) x %>% dplyr::rename(e = estimate, m = moe))

# transform pop data to wide format
pop_wide <- to_wide(pop)

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% 
  right_join(select(xwalk_sen, c(ct_geoid, sen_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = sen_geoid) #dplyr::rename to generic column names for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide)

##### SEN WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)          # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
sen_wa <- wt_avg(pct_df, ind_df)         # calc weighted average and apply reliability screens
sen_wa <- sen_wa %>%
  dplyr::mutate(geolevel = 'sldu')  # add geolevel

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_acs(geography = "State Legislative District (Upper Chamber)", 
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = acs_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
sen_name$NAME <- gsub(", California", "", sen_name$NAME)
names(sen_name) <- c("target_id", "target_name")
# # View(sen_name)

#add geonames to WA
sen_wa <- merge(x=sen_name,y=sen_wa,by="target_id", all=T)
## View(sen_wa)

############ JOIN CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% union(city_wa) %>% union(assm_wa) %>% union(sen_wa)
wa_all <-dplyr::rename(wa_all, geoid = target_id, geoname = target_name)   #dplyr::rename columns for RC functions
wa_all <- wa_all %>% dplyr::relocate(geoname, .after = geoid) %>%          # move geoname column 
  dplyr::relocate(total_rate, .after = twoormor_rate) %>% 
  dplyr::relocate(total_pop, .after = twoormor_pop)


#### EXTRA SCREENING BC NA'S SHOULD NOT BE TREATED AS ZEROES IN THIS DATASET ####
library(naniar)
wa_all <- wa_all %>% 
  replace_with_na_at(.vars = c("total_rate", "black_rate", "asian_rate", "aian_rate", "pacisl_rate", "other_rate", "twoormor_rate", "nh_white_rate", "latino_rate"),
                     condition = ~.x == 0.00000000)

d <- wa_all
# View(d)

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ] %>% dplyr::select(-c(geolevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
# View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ] %>% dplyr::select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
# View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% dplyr::select(-c(geolevel))

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
# View(city_table)

#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
# View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
# View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
# View(leg_table)

#rename geoid to state_id, county_id, city_id
state_table <- dplyr::rename(state_table, state_id = geoid, state_name = geoname)
county_table <- dplyr::rename(county_table, county_id = geoid, county_name = geoname)
city_table <-  dplyr::rename(city_table, city_id = geoid, city_name = geoname) 
leg_table <-  dplyr::rename(leg_table, leg_id = geoid, leg_name = geoname) 

###update info for postgres tables###
county_table_name <- paste0("arei_hous_eviction_filing_rate_county_", rc_yr)
state_table_name <- paste0("arei_hous_eviction_filing_rate_state_", rc_yr)
city_table_name <- paste0("arei_hous_eviction_filing_rate_city_", rc_yr)
leg_table_name <- paste0("arei_hous_eviction_filing_rate_leg_", rc_yr)

indicator <- "Rate of eviction filings per 100 renter households (weighted average) - annual average from 2014-2017. Data is converted to 2020 tracts, 2020 ACS Table B25003 pop is used as denominator. The data is"
source <- "the (2000-2017) valid proprietary tract-level data downloaded from the Eviction Lab. https://data-downloads.evictionlab.org/#data-for-analysis/"

#send tables to postgres
to_postgres(county_table, state_table)
city_to_postgres(city_table)
leg_to_postgres(leg_table)
# dbDisconnect(con)
