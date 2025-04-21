### Lack of Greenspace (Weighted Avg) RC v7 ### 
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
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Environment\\QA_Sheet_Lack_of_Greenspace.docx"

# set source for Weighted Average Functions & SWANA Ancestry scripts
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")
source("W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/SWANA_Ancestry_List.R")

# update variables used throughout each year
curr_yr <- 2023  # NLCD and ACS year
rc_yr <- '2025'
rc_schema <- 'v7'

# Check that variables in vars_list_acs used in WA fx haven't changed --------
# select acs race/eth pop variables: All AIAN/PacIsl, NH Alone White/Black/Asian/Other, NH Two+, Latinx of any race
## the variables MUST BE in this order:
rc_races <-      c('total',     'aian',      'pacisl',    'latino',    'nh_white',  'nh_black',  'nh_asian',  'nh_other',  'nh_twoormor')
vars_list_acs <- c("DP05_0001", "DP05_0071", "DP05_0073", "DP05_0076", "DP05_0082", "DP05_0083", "DP05_0085", "DP05_0087", "DP05_0088")

dp05_curr <- load_variables(curr_yr, "acs5/profile", cache = TRUE) %>% 
  select(-c(concept)) %>% 
  filter(name %in% vars_list_acs) %>%
  mutate(name = tolower(name)) # get all DP05 vars
dp05_curr$label <- gsub("Estimate!!|HISPANIC OR LATINO AND RACE!!", "", dp05_curr$label)
dp05_curr <- dp05_curr %>% cbind(rc_races)

# CHECK THIS TABLE TO MAKE SURE THE LABEL AND RC_RACES COLUMNS MATCH UP
print(dp05_curr) 


# may need to update each year: variables for state assm and senate calcs
assm_geoid <- 'sldl24'			                    # Define column with Assm geoid
assm_xwalk <- 'tract_2020_state_assembly_2024'  # Name of tract-Assm xwalk table
sen_geoid <- 'sldu24'			                      # Define column with senate geoid
sen_xwalk <- 'tract_2020_state_senate_2024'     # Name of tract-Sen xwalk table


##### 1. GET INDICATOR DATA ######
# You MUST load indicator data using these table/column names (ind_df with cols subid / indicator) in order for WA functions to work
ind_df <- dbGetQuery(conn, paste0("SELECT ct_geoid AS sub_id, median AS indicator FROM built_environment.nlcd_tract_impervious_land_", curr_yr))

############# ASSEMBLY DISTRICTS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldl')      # define your target geolevel: state assembly
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 250              # define population threshold for screening

### CT-Assm Crosswalk ### 
# Import CT-Assm Crosswalk
xwalk_filter <- dbGetQuery(conn, paste0("SELECT geo_id AS ct_geoid, ", assm_geoid, " AS assm_geoid FROM crosswalks.", assm_xwalk))

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_acs, yr = curr_yr, srvy = survey)  # subgeolevel pop
#pop <- as.data.frame(pop)

# get SWANA pop
vars_list_acs_swana <- get_swana_var(curr_yr, survey)
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = curr_yr, srvy = survey) %>%
  as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>%
  rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- pop_ %>% 
  pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = c(e, m), names_glue = "{variable}{.value}")


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% 
  right_join(select(xwalk_filter, c(ct_geoid, assm_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = assm_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### ASSM WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
assm_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
assm_wa <- assm_wa %>% mutate(geolevel = 'sldl')  # change drop geometry, add geolevel

## Add census geonames
census_api_key(census_key1, overwrite=TRUE)
assm_name <- get_acs(geography = "State Legislative District (Lower Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = curr_yr)

assm_name <- assm_name[,1:2]
assm_name$NAME <- str_remove(assm_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
assm_name$NAME <- gsub("; California", "", assm_name$NAME)
names(assm_name) <- c("target_id", "target_name")
# View(assm_name)

#add geonames to WA
assm_wa <- merge(x=assm_name,y=assm_wa,by="target_id", all=T)
#View(assm_wa)

############# SENATE DISTRICTS ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('sldu')      # define your target geolevel: state senate
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 250              # define population threshold for screening

### CT-Sen Crosswalk ### 
# Import CT-Sen Crosswalk
xwalk_filter <- dbGetQuery(conn, paste0("SELECT geo_id AS ct_geoid, ", sen_geoid, " AS sen_geoid FROM crosswalks.", sen_xwalk))

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_acs, yr = curr_yr, srvy = survey)  # subgeolevel pop

# get SWANA pop
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = curr_yr, srvy = survey) %>%
  as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>%
  rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- pop_ %>%
  pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = c(e, m), names_glue = "{variable}{.value}")


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% 
  right_join(select(xwalk_filter, c(ct_geoid, sen_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = sen_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### Sen WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
sen_wa <- wt_avg(pct_df)         # calc weighted average and apply reliability screens
sen_wa <- sen_wa %>% mutate(geolevel = 'sldu')  # change drop geometry, add geolevel

## Add census geonames
# census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_acs(geography = "State Legislative District (Upper Chamber)", 
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = curr_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
sen_name$NAME <- gsub("; California", "", sen_name$NAME)
names(sen_name) <- c("target_id", "target_name")
# View(sen_name)

# add geonames to WA
sen_wa <- merge(x=sen_name,y=sen_wa,by="target_id", all=T)
#View(sen_wa)

############# CITY ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')             # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('place')     # define your target geolevel: place
survey <- "acs5"                 # define which Census survey you want
pop_threshold = 250              # define population threshold for screening

### CT-Place Crosswalk ### 
#THIS FX ISN'T WORKING WHEN RUN AS A FX, BUT DOES WORK WHEN RUN IN THE SCRIPT: set source for CT-Place Crosswalk fx
source("./Functions/RC_CT_Place_Xwalk.R")
xwalk_filter <- make_ct_place_xwalk(curr_yr) %>%
  select(c(ct_geoid, place_geoid, place_name))

places <- xwalk_filter %>% 
  select(c(place_geoid, place_name)) %>% unique()

##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_acs, yr = curr_yr, srvy = survey)  # subgeolevel pop

# get SWANA pop
pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = curr_yr, srvy = survey) %>% as.data.frame() %>%
  group_by(GEOID, NAME, geolevel) %>% 
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana") # subgeolevel pop


# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>% 
  rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- pop_ %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = c(e, m), names_glue = "{variable}{.value}")


#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% right_join(select(xwalk_filter, c(ct_geoid, place_geoid)), by = c("GEOID" = "ct_geoid"))  # join target geoids/names
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = place_geoid) # rename to generic column names for WA functions


# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide) 

##### CITY WEIGHTED AVG CALCS ###
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel
city_wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
city_wa <- city_wa %>% 
  left_join(select(places, c(place_geoid, place_name)), by = c("target_id" = "place_geoid")) # add in target geolevel names
city_wa <- city_wa %>% 
  rename(target_name = place_name) %>% 
  mutate(geolevel = 'city')  # change NAME to target_name, drop geometry, add geolevel


############# COUNTY ##################

###### DEFINE VALUES FOR FUNCTIONS ###

# set values for weighted average functions - You may need to update these
subgeo <- c('tract')              # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
targetgeolevel <- c('county')     # define your target geolevel: county
survey <- "acs5"                  # define which Census survey you want
pop_threshold = 250               # define population threshold for screening

##### CREATE COUNTY GEOID & NAMES TABLE ######  You will NOT need this chunk if your indicator data table has target geolevel names already
targetgeo_names <- county_names(vars = vars_list_acs, yr = curr_yr, srvy = survey)


##### GET SUB GEOLEVEL POP DATA ###
pop <- update_detailed_table(vars = vars_list_acs, yr = curr_yr, srvy = survey)  # subgeolevel pop

pop_swana <- update_detailed_table(vars = vars_list_acs_swana, yr = curr_yr, srvy = survey) %>% as.data.frame() %>%
  group_by(GEOID, NAME, geolevel)%>%
  summarise(estimate=sum(estimate),
            moe=moe_sum(moe,estimate)) %>% mutate(variable = "swana") # subgeolevel pop

# combine DP05 groups with SWANA tract level estimates 
pop_ <- rbind(pop, pop_swana %>% filter(geolevel == 'tract')) %>% rename(e = estimate, m = moe)

# transform pop data to wide format 
pop_wide <- pop_ %>% pivot_wider(id_cols = c(GEOID, NAME, geolevel), names_from = variable, values_from = c(e, m), names_glue = "{variable}{.value}")

#### add target_id field, you may need to update this bit depending on the sub and target_id's in the data you're using
pop_wide <- as.data.frame(pop_wide) %>% mutate(target_id = substr(GEOID, 1, 5))  # use left 5 characters as target_id
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID)                              # rename to generic column name for WA functions

# calc target geolevel pop and number of sub geolevels per target geolevel
pop_df <- targetgeo_pop(pop_wide)

##### COUNTY WEIGHTED AVG CALCS ###
pct_df <- pop_pct(pop_df)   # calc pct of target geolevel pop in each sub geolevel
wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% 
  mutate(geolevel = 'county')    # add in target geolevel names and geolevel type


############# STATE CALCS ##################
# get and prep state pop
ca_pop_wide <- state_pop(vars = vars_list_acs, vars2 = vars_list_acs_swana, yr = curr_yr, srvy = survey)

# calc state wa
ca_pct_df <- ca_pop_pct(ca_pop_wide)
ca_wa <- ca_wt_avg(ca_pct_df) %>% 
  mutate(geolevel = 'state')   # add geolevel type

############ JOIN LEG, CITY, COUNTY & STATE WA TABLES  ##################
wa_all <- union(wa, ca_wa) %>% 
  union(city_wa) %>% 
  union(assm_wa) %>% 
  union(sen_wa) 
wa_all <- rename(wa_all, geoid = target_id, geoname = target_name)   # rename columns for RC functions
wa_all <- wa_all %>% 
  dplyr::relocate(geoname, .after = geoid)# move geoname column

d <- wa_all
View(d)

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
View(upper_table)

#calculate SLDL z-scores
lower_table <- calc_z(lower_table)

## Calc SLDL ranks##
lower_table <- calc_ranks(lower_table)
View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

#rename geoid to state_id, county_id, city_id
colnames(state_table)[1:2] <- c("state_id", "state_name")
colnames(county_table)[1:2] <- c("county_id", "county_name")
colnames(city_table)[1:2] <- c("city_id", "city_name")
colnames(leg_table)[1:2] <- c("leg_id", "leg_name")

### info for postgres tables automatically updates ###
county_table_name <- paste0("arei_hben_lack_of_greenspace_county_",rc_yr)
state_table_name <- paste0("arei_hben_lack_of_greenspace_state_",rc_yr)
city_table_name <- paste0("arei_hben_lack_of_greenspace_city_",rc_yr)
leg_table_name <- paste0("arei_hben_lack_of_greenspace_leg_", rc_yr)

start_yr <- curr_yr - 4
indicator <- "Impervious Landcover (%) is the weighted average of percentage of impervious land cover out of all land cover by race. Impervious land cover includes roads, roof tops, and parking lots"
source <- paste0("Multi-Resolution Land Characteristics Consortium, National Land Cover Database (", curr_yr, "), ACS DP05 (", start_yr, "-", curr_yr, "). QA doc: ", qa_filepath)


#send tables to postgres
# to_postgres(county_table, state_table)
# city_to_postgres(city_table)
# leg_to_postgres(leg_table)

# dbDisconnect(conn)