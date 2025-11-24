### Drinking Water Contamination (Weighted Avg) RC v7 ### 

#install packages if not already installed
packages <- c("dplyr","data.table","tidycensus","sf","DBI","RPostgres","stringr","tidyr","tigris","usethis")  

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
conn <- connect_to_db("rda_shared_data")

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Environment\\QA_Sheet_Drinking_Water.docx"

#set source for Weighted Average Functions & SWANA Ancestry scripts
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/SWANA_Ancestry_List.R")

# update variables used throughout each year
curr_yr <- 2021 # yr of CES data release
ces_v <- '4.0'  # CES version
acs_yr <- 2020 
rc_yr <- '2025'
rc_schema <- 'v7'
pop_threshold = 250              # define population threshold for screening
leg_int_threshold = .25          # tracts with >= threshold pct of pop in a leg dist or where the tract makes up >= threshold pct of leg dist pop are assigned to a leg dist 

# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table


# Check that DP05 variables and RC race names still match each year and update if needed --------
## Population: All AIAN/PacIsl Latinx incl, NH Alone White/Black/Asian/Other, NH Two+, Latinx of any race
vars_list_dp05 <- c("total_" = "DP05_0001",
                    "aian_" = "DP05_0066", 
                    "pacisl_" = "DP05_0068", 
                    "latino_" = "DP05_0071", 
                    "nh_white_" = "DP05_0077", 
                    "nh_black_" = "DP05_0078", 
                    "nh_asian_" = "DP05_0080", 
                    "nh_other_" = "DP05_0082", 
                    "nh_twoormor_" = "DP05_0083") #https://api.census.gov/data/2020/acs/acs5/profile/groups/DP05.html

race_mapping <- data.frame(
  name = unlist(vars_list_dp05),
  race = names(vars_list_dp05),
  stringsAsFactors = FALSE
)

dp05_curr <- load_variables(curr_yr, "acs5/profile", cache = TRUE) %>% 
  select(-c(concept)) %>% 
  filter(name %in% vars_list_dp05) %>%
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop"),
         label = gsub("Estimate!!HISPANIC OR LATINO AND RACE!!", "", label))


# CHECK THIS TABLE TO MAKE SURE THE LABEL AND RC_RACES COLUMNS MATCH UP
print(dp05_curr) 


##### GET INDICATOR DATA ######
# load indicator data
ind_df <- dbGetQuery(conn, paste0("select ct_geoid AS geoid, drinkwat from built_environment.oehha_ces4_tract_", curr_yr))
ind_df <- dplyr::rename(ind_df, sub_id = geoid, indicator = drinkwat)       # rename columns for functions
ind_df <- filter(ind_df, indicator >= 0) # screen out NA values of -999

# note: CES uses 2010 tract vintage, population estimates use 2020 tract vintage
cb_tract_2010_2020 <- fread("W:\\Data\\Geographies\\Relationships\\tract20_tract10\\cb_tract2020_tract2010_st06.txt", sep="|", colClasses = 'character', data.table = FALSE) %>%
  select(GEOID_TRACT_10, NAMELSAD_TRACT_10, AREALAND_TRACT_10, GEOID_TRACT_20, NAMELSAD_TRACT_20, AREALAND_TRACT_20, AREALAND_PART) %>%
  mutate_at(vars(contains("AREALAND")), function(x) as.numeric(x)) %>%
  # calculate overlapping land area of 2010 and 2020 tracts (AREALAND_PART) as a percent of 2020 tract land area (AREALAND_TRACT_20)
  mutate(prc_overlap=AREALAND_PART/AREALAND_TRACT_20) 

# # check prc_overlaps sums/ note: there will be prc_overlap values > 1 bc some 2010 tracts were split into 2+ 2020 tracts and each 2020 tract comes solely from the 2010 tract 
# check_prc_is_1 <- cb_tract_2010_2020 %>%
#   group_by(GEOID_TRACT_10) %>%
#   summarise(total_prc=sum(prc_overlap))

# Because CES uses 2010 vintage tracts - need to convert to 2020 vintage and allocate score accordingly
ind_2010_2020 <- ind_df %>%
  left_join(cb_tract_2010_2020, by=c("sub_id"="GEOID_TRACT_10")) %>%
  select(sub_id, indicator, AREALAND_TRACT_10, AREALAND_PART, GEOID_TRACT_20, AREALAND_TRACT_20, prc_overlap) %>%
  # Allocate CES scores from 2010 tracts to 2020 using prc_overlap
  mutate(ind_2020=indicator*prc_overlap)

# create indicator df (2020 tracts) to be used in WA calcs
ind_2020 <- ind_2010_2020 %>%
  select(GEOID_TRACT_20, ind_2020) %>%
  # sum weighted CES scores by 2020 tract
  group_by(GEOID_TRACT_20) %>%
  summarize(indicator = sum(ind_2020)) %>%
  # clean up names
  rename(sub_id = GEOID_TRACT_20)

ind_df <- ind_2020 # rename to ind_df for WA fx

############# ASSEMBLY DISTRICTS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- 'tract'             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- 'sldl'      # define your target geolevel: state assembly
survey <- "acs5"              # define which Census survey you want

### CT-Assm Crosswalk ### 
# Import CT-Assm Crosswalk
xwalk_assm <- dbGetQuery(conn, paste0("SELECT geo_id AS ct_geoid, ", assm_geoid, " AS assm_geoid, afact, afact2 FROM crosswalks.", assm_xwalk)) %>%
  filter(afact >= leg_int_threshold | afact2 >= leg_int_threshold)  # screen xwalk based on pct of ct pop in dist OR pct of dist pop in ct

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_dp05, yr = acs_yr, srvy = survey)  # subgeolevel pop

# get SWANA pop
vars_list_acs_swana <- get_swana_var(acs_yr, survey)
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = acs_yr, srvy = survey) %>% 
  as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana_") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>%
  rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- to_wide(pop_)


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% 
  right_join(select(xwalk_assm, c(ct_geoid, assm_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = assm_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### ASSM WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)     # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
assm_wa <- wt_avg(pct_df, ind_df)   # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% mutate(geolevel = 'sldl')  # change drop geometry, add geolevel

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
# View(assm_name)

# add geonames to WA
assm_wa <- merge(x=assm_name,y=assm_wa,by="target_id", all=T)
# View(assm_wa)

############# SENATE DISTRICTS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- 'tract'             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- 'sldu'      # define your target geolevel: state senate
survey <- "acs5"              # define which Census survey you want


### CT-Sen Crosswalk ### 
# Import CT-Sen Crosswalk
xwalk_sen <- dbGetQuery(conn, paste0("SELECT geo_id AS ct_geoid, ", sen_geoid, " AS sen_geoid, afact, afact2 FROM crosswalks.", sen_xwalk)) %>%
  filter(afact >= leg_int_threshold | afact2 >= leg_int_threshold)  # screen xwalk based on pct of ct pop in dist OR pct of dist pop in ct

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_dp05, yr = acs_yr, srvy = survey)  # subgeolevel pop

# get SWANA pop
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = acs_yr, srvy = survey) %>% 
  as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana_") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>%
  rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- to_wide(pop_)


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% 
  right_join(select(xwalk_sen, c(ct_geoid, sen_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = sen_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### Sen WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)    # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
sen_wa <- wt_avg(pct_df, ind_df)   # calc weighted average and apply reliability screens
sen_wa <- sen_wa %>% mutate(geolevel = 'sldu')  # change drop geometry, add geolevel

## Add census geonames
# census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_acs(geography = "State Legislative District (Upper Chamber)", 
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = acs_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
sen_name$NAME <- gsub(", California", "", sen_name$NAME)
names(sen_name) <- c("target_id", "target_name")
# View(sen_name)

# add geonames to WA
sen_wa <- merge(x=sen_name,y=sen_wa,by="target_id", all=T)
# View(sen_wa)

############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- 'tract'             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- 'place'     # define your target geolevel: place
survey <- "acs5"              # define which Census survey you want

### CT-Place Crosswalk ### 
#set source for CT-Place Crosswalk fx
source("./Functions/RC_CT_Place_Xwalk.R")
xwalk_city <- make_ct_place_xwalk(acs_yr) %>% 
  select(ct_geoid, place_geoid, place_name)

places <- xwalk_city %>% 
  select(c(place_geoid, place_name)) %>% unique()

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_dp05, yr = acs_yr, srvy = survey)  # subgeolevel pop

# get SWANA pop
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = acs_yr, srvy = survey) %>% as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana_") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>% 
  rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- to_wide(pop_)


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% 
  right_join(select(xwalk_city, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### CITY WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)     # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df, ind_df)   # calc weighted average and apply reliability screens
city_wa <- city_wa %>% 
  left_join(select(places, c(place_geoid, place_name)), by = c("target_id" = "place_geoid")) # add in target geolevel names
city_wa <- city_wa %>% 
  rename(target_name = place_name) %>% 
  mutate(geolevel = 'city')  # change NAME to target_name, drop geometry, add geolevel


############# COUNTY ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- 'tract'              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- 'county'     # define your target geolevel: county
survey <- "acs5"               # define which Census survey you want

##### CREATE COUNTY GEOID & NAMES TABLE ######  You will NOT need this chunk if your indicator data table has target geolevel names already
targetgeo_names <- county_names(var_list = vars_list_dp05, yr = acs_yr, srvy = survey)


##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_dp05, yr = acs_yr, srvy = survey)  # subgeolevel pop

pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = acs_yr, srvy = survey) %>% 
  as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>%
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana_") # subgeolevel pop

# combine DP05 groups with swana estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>% rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- to_wide(pop_)

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% 
  mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id

pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              # rename to generic column name for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide)


##### COUNTY WEIGHTED AVG CALCS ###
pct_df <- pop_pct(pop_df)     # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df, ind_df)  # calc weighted average and apply reliability screens
wa <- wa %>% 
  left_join(targetgeo_names, by = "target_id") %>% 
  mutate(geolevel = 'county')    # add in target geolevel names and geolevel type


############# STATE CALCS ##################
# get and prep state pop
ca_pop_wide <- state_pop(vars = vars_list_dp05, vars2 = vars_list_acs_swana, yr = acs_yr, srvy = survey)

# calc state wa
ca_pct_df <- ca_pop_pct(ca_pop_wide)
ca_wa <- ca_wt_avg(ca_pct_df, ind_df) %>% 
  mutate(geolevel = 'state')   # add geolevel type


############ JOIN LEG, CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% 
  union(city_wa) %>% 
  union(assm_wa) %>% 
  union(sen_wa) 
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% 
  dplyr::relocate(geoname, .after = geoid)        # move geoname column

d <- wa_all
View(d)

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

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
state_table <- d[d$geoname == 'California', ] %>% select(-c(geolevel))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
View(state_table)


#split COUNTY into separate table and format id, name columns
county_table <- d[d$geolevel == 'county', ] %>% select(-c(geolevel))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
View(county_table)


#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
View(city_table)


#split LEG DISTRICTS into separate tables and format id, name columns
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores
upper_table <- calc_z(upper_table)

## Calc SLDU ranks##
upper_table <- calc_ranks(upper_table)
#View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
#View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")

### info for postgres tables will auto update ###
county_table_name <- paste0("arei_hben_drinking_water_county_", rc_yr)
state_table_name <- paste0("arei_hben_drinking_water_state_", rc_yr)
city_table_name <- paste0("arei_hben_drinking_water_city_", rc_yr)
leg_table_name <- paste0("arei_hben_drinking_water_leg_", rc_yr)

start_yr <- acs_yr - 4

indicator <- "Exposure to Contaminated Drinking Water Score"
source <- paste0("CalEnviroScreen ", ces_v, " (", curr_yr, ") https://oehha.ca.gov/calenviroscreen/report/calenviroscreen-40, ACS DP05 (", start_yr, "-", acs_yr, "). QA doc: ", qa_filepath)


## send tables to postgres -----------------------------------------------
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)

dbDisconnect(conn)