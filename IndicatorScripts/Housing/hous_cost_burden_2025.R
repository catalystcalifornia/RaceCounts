## Housing Cost Burden for RC v7 ##
#### Run W:\Project\RACE COUNTS\2025_v7\RC_Github\staging\RaceCounts\Functions\housing_functions.R to prep rda_shared_data tables first.

# Install and load packages ------------------------------
packages <- c("data.table", "stringr", "dplyr", "RPostgres", "dbplyr", 
              "srvyr", "tidycensus", "rpostgis",  "tidyr", "readxl", "sf")
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

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


# Update variables each year --------
rc_schema <- "v7"
rc_yr <- '2025'
data_yrs <- c("2017", "2018", "2019", "2020", "2021") # all data yrs included in analysis
metadata_file <- "CHAS-data-dictionary-17-21.xlsx"  # update with name of latest metadata file
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Housing\\QA_HousingBurden_Renter_Owners.docx"

assm_xwalk <- "tract_2020_state_assembly_2024"
assm_geoid <- "sldl24"
sen_xwalk <- "tract_2020_state_senate_2024"
sen_geoid <- "sldu24"
  
# set screening thresholds: suppress values with high CVs and small populations. generally does not need update.
cv_threshold <- 35    # data for geo+race combos with CV > threshold are suppressed
pop_threshold <- 100  # data for geo+race combos with pop < threshold are suppressed

# does not need update
chas_table <- paste0("housing.hud_chas_cost_burden_multigeo_", data_yrs[[1]], "_", substr(data_yrs[length(data_yrs)],3,4))


######### Pull in metadata and data ######################
dict <- read_excel(paste0("W:/Data/Housing/HUD/CHAS/", data_yrs[[1]], "-", data_yrs[length(data_yrs)], "/", metadata_file), sheet = "Table 9")
chas_data <- dbGetQuery(con, paste0("SELECT * FROM ", chas_table, " WHERE geolevel IN ('census tract', 'city', 'county', 'state')")) 

######## Pull in crosswalks for tract to legislative districts#############
xwalk_senate <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.", sen_xwalk))
xwalk_assembly <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.", assm_xwalk))

chas_data_leg_senate <- chas_data %>%
  filter(geolevel == 'census tract') %>%
  inner_join(xwalk_senate, by = c("geoid" = "geo_id")) %>%
  rename(sen_geoid = {{sen_geoid}}) %>%
  select(sen_geoid, geoid, everything()) %>%
  mutate(across(starts_with("t9_"), ~ .x * afact, .names = "allocated_{.col}")) %>%  # weight CHAS data by afact
  group_by(sen_geoid) %>%
  summarise(across(starts_with("allocated_"), sum, na.rm = TRUE), .groups = "drop") %>% # aggregate weighted data
  rename_with(~ sub("^allocated_", "", .x), starts_with("allocated_")) %>% 
  mutate(name = paste0("State Senate District ", as.numeric(str_sub(sen_geoid, -2))),
         geolevel = 'sldu') %>%
  rename(geoid = sen_geoid)

chas_data_leg_assm <- chas_data %>%
  filter(geolevel == 'census tract') %>%
  inner_join(xwalk_assembly, by = c("geoid" = "geo_id")) %>%
  rename(assm_geoid = {{assm_geoid}}) %>%
  select(assm_geoid, geoid, everything()) %>%
  mutate(across(starts_with("t9_"), ~ .x * afact, .names = "allocated_{.col}")) %>%  # weight CHAS data by afact
  group_by(assm_geoid) %>%
  summarise(across(starts_with("allocated_"), sum, na.rm = TRUE), .groups = "drop") %>% # aggregate weighted data
  rename_with(~ sub("^allocated_", "", .x), starts_with("allocated_")) %>% 
  mutate(name = paste0("State Assembly District ", as.numeric(str_sub(assm_geoid, -2))),
         geolevel = 'sldl') %>%
  rename(geoid = assm_geoid)

chas_data_all <- bind_rows(chas_data, chas_data_leg_senate, chas_data_leg_assm) %>%
  filter(geolevel != 'census tract')
  

######### Clean and reformat raw data ######################
# data cleaning
names(dict) <- gsub(" ", "_", names(dict))
names(dict) <- gsub("/", "_", names(dict))
names(dict) <- tolower(names(dict))
dict$column_name <- tolower(dict$column_name)

chas_data_all <- chas_data_all %>%
  mutate(geoname = gsub("^(.*?),.*", "\\1", chas_data_all$name)) %>% # clean geonames
  select(geoid, geoname, geolevel, starts_with("t9")) # drop unneeded cols
# View(chas_data_all)


# pivot long
chas_data_long <- pivot_longer(chas_data_all, cols = starts_with("t9"), 
                               names_to = "variable", 
                               values_to = "housing_units") %>%
                  mutate(variable_generic = as.numeric(gsub("\\D", "", variable))) # add 'generic' variable field where est and moe have the same value

# join race and cost burden information from data dictionary and recode
chas_data_long <- chas_data_long %>% 
  left_join(dict[,c(1,3:5)], by = c( "variable" = "column_name")) %>%
  mutate(race_ethnicity = recode(race_ethnicity, 
                                   "Black or African-American alone, non-Hispanic" = "nh_black",
                                   "Asian alone, non-Hispanic" = "nh_asian",
                                   "American Indian or Alaska Native alone, non-Hispanic" = "nh_aian",
                                   "Pacific Islander alone, non-Hispanic" = "nh_pacisl",
                                   "Hispanic, any race" = "latino",
                                   "White alone, non-Hispanic" = "nh_white",
                                   "other (including multiple races, non-Hispanic)" = "nh_other"),
         cost_burden = recode(cost_burden,
                                "greater than 30% but less than or equal to 50%" = "30.50", 
                                "greater than 50%" = "50.100",
                                "not computed (no/negative income)" = "not_computed",
                                "less than or equal to 30%" = "0.30")) %>%
  rename(race = race_ethnicity, burden = cost_burden)


# drop rows with missing cost burden, cost burden and race 'universe' rows
chas_data_long <- filter(chas_data_long, !(burden %in% c('not_computed','All')), 
                                         !(race %in% c("All"))) 

# set definition of cost burden at >30%
chas_data_long$cost_burdened <- ifelse(chas_data_long$burden == "0.30", 0, 1) 

# save margins of error for later
moe <- filter(chas_data_long, substring(variable, 4, 6) == "moe") %>% 
  select(-c(tenure, race, burden, cost_burdened)) %>% 
  left_join(chas_data_long %>% select(geoid, variable_generic, race, tenure, cost_burdened, geolevel), by =c("geoid", "variable_generic", "geolevel")) %>% # fill in missing race / tenure info
  filter(!is.na(race)) %>%
  rename(num_moe = housing_units)

chas_data_ <- filter(chas_data_long, !str_detect(chas_data_long$variable, 'moe'))

######### Raced Calcs ######################

## calculate raw counts
costburden_race <- chas_data_ %>%
  group_by(geoid, geoname, race, cost_burdened, tenure, geolevel, variable_generic) %>%  
  summarize(raw = sum(housing_units)) %>%
  left_join(chas_data_) 
# View(costburden_race)

## calculate den moe
costburden_moe <- costburden_race %>% 
  left_join(moe, by = c("geoid", "geoname", "variable_generic", "race", "tenure")) %>%
  select(-c(variable_generic)) %>%
  group_by(geoid, geoname, race, tenure) %>%
  summarize(pop = sum(raw),
            den_moe = moe_sum(num_moe, raw))
  
## put it all together
costburden_race_ <- costburden_race %>% 
  left_join(moe, by = c("geoid", "geoname", "variable_generic", "race", "tenure", "cost_burdened", "geolevel")) %>%
  select(-c(variable_generic)) %>%
  group_by(geoid, geoname, geolevel, race, cost_burdened, tenure) %>%
  summarize(raw = sum(raw),
            num_moe = moe_sum(num_moe, raw)) %>%
  left_join(costburden_moe, by = c("geoid", "geoname", "race", "tenure"), relationship = "many-to-many") %>%
  mutate(rate = raw/pop * 100,                                      
         rate_moe = moe_prop(raw, pop, num_moe, den_moe) * 100,   
         rate_cv = ifelse(rate == 0, NA, (rate_moe/1.645)/rate * 100)) 

######### Total Calcs ######################

## calculate raw counts/moe
costburden_tot <- chas_data_ %>%
  left_join(moe %>% select(-c(cost_burdened, variable)), by = c("geoid", "geoname", "variable_generic", "race", "tenure", "geolevel")) %>% 
  select(c(geoid, geoname, cost_burdened, tenure, geolevel, housing_units, num_moe)) %>%
  group_by(geoid, geoname, cost_burdened, tenure, geolevel) %>%  
  summarize(raw = sum(housing_units),
            num_moe = moe_sum(num_moe, housing_units))

## calculate pop counts/moe
costburden_moe_tot <- chas_data_ %>%
  left_join(moe %>% select(c(geoid, geoname, tenure, cost_burdened, race, variable_generic, num_moe, geolevel)), by = c("geoid", "geoname", "tenure", "cost_burdened", "race", "variable_generic", "geolevel")) %>%
  group_by(geoid, geoname, tenure, geolevel) %>%  
  summarise(pop = sum(housing_units),
            den_moe = moe_sum(num_moe, housing_units))

## put it all together
costburden_tot_ <- costburden_tot %>% 
  left_join(costburden_moe_tot, by = c("geoid", "geoname", "tenure", "geolevel")) %>%
  mutate(race = "total",
         rate = raw/pop * 100,                                      
         rate_moe = moe_prop(raw, pop, num_moe, den_moe) * 100,   
         rate_cv = ifelse(rate == 0, NA, (rate_moe/1.645)/rate * 100)) 

######### Prepare data for RACE COUNTS -----------------------------------

## merge raced/total datasets
cost_burden_calcs <- bind_rows(costburden_race_, costburden_tot_) %>%
  filter(cost_burdened == 1)

cost_burden_calcs$tenure <- ifelse(cost_burden_calcs$tenure == "Owner occupied", "owner", "renter")

# convert long format to wide
cost_burden_calcs_rc <- cost_burden_calcs %>% 
  # convert to wide format
  pivot_wider(id_cols = c(geoid, geoname, geolevel, tenure),
              names_from = c(race),
              values_from = c("raw", "pop", "rate", "rate_moe", "rate_cv"),
              names_glue = "{race}_{.value}")%>% 
  as.data.frame()
# View(cost_burden_calcs_rc)

## Screen data and clean geonames
df <- cost_burden_calcs_rc

# Clean geo names
df$geoname <- gsub(" County", "", df$geoname)
df$geoname <- gsub(" city", "", df$geoname)
df$geoname <- gsub(" town", "", df$geoname)
df$geoname <- gsub(" CDP", "", df$geoname)
df$geoname <- gsub(" City", "", df$geoname)

# Convert Nan and Inf values to NA
df[sapply(df, is.nan)] <- NA
df[sapply(df, is.infinite)] <- NA

#Screen data: Convert rate to NA if its CV greater than the cv_threshold or its pop is less than the pop_threshold
df$total_rate <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_rate))
df$nh_asian_rate <- ifelse(df$nh_asian_rate_cv > cv_threshold, NA, ifelse(df$nh_asian_pop < pop_threshold, NA, df$nh_asian_rate))
df$nh_black_rate <- ifelse(df$nh_black_rate_cv > cv_threshold, NA, ifelse(df$nh_black_pop < pop_threshold, NA, df$nh_black_rate))
df$nh_white_rate <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_rate))
df$latino_rate <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_rate))
df$nh_other_rate <- ifelse(df$nh_other_rate_cv > cv_threshold, NA, ifelse(df$nh_other_pop < pop_threshold, NA, df$nh_other_rate))
df$nh_pacisl_rate <- ifelse(df$nh_pacisl_rate_cv > cv_threshold, NA, ifelse(df$nh_pacisl_pop < pop_threshold, NA, df$nh_pacisl_rate))
df$nh_aian_rate <- ifelse(df$nh_aian_rate_cv > cv_threshold, NA, ifelse(df$nh_aian_pop < pop_threshold, NA, df$nh_aian_rate))

df$total_raw <- ifelse(df$total_rate_cv > cv_threshold, NA, ifelse(df$total_pop < pop_threshold, NA, df$total_raw))
df$nh_asian_raw <- ifelse(df$nh_asian_rate_cv > cv_threshold, NA, ifelse(df$nh_asian_pop < pop_threshold, NA, df$nh_asian_raw))
df$nh_black_raw <- ifelse(df$nh_black_rate_cv > cv_threshold, NA, ifelse(df$nh_black_pop < pop_threshold, NA, df$nh_black_raw))
df$nh_white_raw <- ifelse(df$nh_white_rate_cv > cv_threshold, NA, ifelse(df$nh_white_pop < pop_threshold, NA, df$nh_white_raw))
df$latino_raw <- ifelse(df$latino_rate_cv > cv_threshold, NA, ifelse(df$latino_pop < pop_threshold, NA, df$latino_raw))
df$nh_other_raw <- ifelse(df$nh_other_rate_cv > cv_threshold, NA, ifelse(df$nh_other_pop < pop_threshold, NA, df$nh_other_raw))
df$nh_pacisl_raw <- ifelse(df$nh_pacisl_rate_cv > cv_threshold, NA, ifelse(df$nh_pacisl_pop < pop_threshold, NA, df$nh_pacisl_raw))
df$nh_aian_raw <- ifelse(df$nh_aian_rate_cv > cv_threshold, NA, ifelse(df$nh_aian_pop < pop_threshold, NA, df$nh_aian_raw))

df <- df %>% relocate(ends_with("_raw"), .after = ends_with("_pop")) # reorder fields so raw/rate cols are next to each other

#Create an owners dataframe so that it creates two sets of graphs for the RC_Functions for each owners and renters
owners <- filter(df, tenure == "owner")

# set variables used in RC tables
hud_yrs <- paste(first(data_yrs), last(data_yrs), sep = "-")
source <- paste0("HUD CHAS (", hud_yrs, ") for city, county, state legislative districts and state from https://www.huduser.gov/portal/datasets/cp.html")


########## OWNER: RACE COUNTS STATS ##############
#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

# assign d so that it runs RC calculations with owners data
d <- owners

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
# View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid") %>% select(-c(geolevel, tenure))
# View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>% select(-c(geolevel, tenure))
# View(county_table)

#remove county/state from place table
city_table <- d[d$geolevel == 'city', ]

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") %>% select(-c(geolevel, tenure))
# View(city_table)

#split LEGISLATIVE DISTRICTS into separate table 
upper_leg_table <- d[d$geolevel == 'sldu', ]
lower_leg_table <- d[d$geolevel == 'sldl', ]

#calculate LEGISLATIVE DISTRICTS z-scores and bind
upper_leg_table <- calc_z(upper_leg_table)
upper_leg_table <- calc_ranks(upper_leg_table)
upper_leg_table <- upper_leg_table
#View(upper_leg_table)

lower_leg_table <- calc_z(lower_leg_table)
lower_leg_table <- calc_ranks(lower_leg_table)
lower_leg_table <- lower_leg_table
#View(lower_leg_table)

leg_table <- rbind(upper_leg_table, lower_leg_table) %>% dplyr::rename("leg_id" = "geoid", "leg_name" = "geoname")

###update info for postgres tables###
county_table_name <- paste0("arei_hous_cost_burden_owner_county_", rc_yr)
state_table_name <- paste0("arei_hous_cost_burden_owner_state_", rc_yr)
city_table_name <- paste0("arei_hous_cost_burden_owner_city_", rc_yr)
leg_table_name <- paste0("arei_hous_cost_burden_owner_leg_", rc_yr)

indicator <- paste0("The percentage of owner-occupied housing units experiencing cost burden (Monthly housing costs, including utilities, exceeding 30% of monthly income. White, Black, Asian, AIAN, and PacIsl one race alone and Latinx-exclusive. Other includes other race and two or more races, and is Latinx-exclusive. QA doc: ", qa_filepath, ". This data is")

# send tables to postgres
to_postgres(county_table, state_table)
city_to_postgres(city_table)
leg_to_postgres(leg_table) 

#### RC CALCS: RENTERS ################################################################
#Create a renters dataframe by filtering out owners so that it creates two sets of graphs for the RC_Functions for each owners and renters

renters <- filter(df, tenure == "renter")

#reassign d so that it runs the RC calculations with renters data
d <- renters

########## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source(".\\Functions\\RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
# View(d)

#split STATE into separate table and format id, name columns
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid") %>% select(-c(geolevel, tenure))
# View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>% select(-c(geolevel, tenure))
# View(county_table)

#remove county/state from place table
city_table <- d[d$geolevel == 'city', ]

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") %>% select(-c(geolevel, tenure))

# View(city_table)
#split LEGISLATIVE DISTRICTS into separate table 
upper_leg_table <- d[d$geolevel == 'sldu', ]
lower_leg_table <- d[d$geolevel == 'sldl', ]

#calculate LEGISLATIVE DISTRICTS z-scores and bind
upper_leg_table <- calc_z(upper_leg_table)
upper_leg_table <- calc_ranks(upper_leg_table)
upper_leg_table <- upper_leg_table
#View(upper_leg_table)

lower_leg_table <- calc_z(lower_leg_table)
lower_leg_table <- calc_ranks(lower_leg_table)
lower_leg_table <- lower_leg_table
#View(lower_leg_table)

leg_table <- rbind(upper_leg_table, lower_leg_table) %>% dplyr::rename("leg_id" = "geoid", "leg_name" = "geoname")

###update info for postgres tables###
county_table_name <- paste0("arei_hous_cost_burden_renter_county_", rc_yr)
state_table_name <- paste0("arei_hous_cost_burden_renter_state_", rc_yr)
city_table_name <- paste0("arei_hous_cost_burden_renter_city_", rc_yr)
leg_table_name <- paste0("arei_hous_cost_burden_renter_leg_", rc_yr)

indicator <- paste0("The percentage of rented housing units experiencing cost burden (Monthly housing costs, including utilities, exceeding 30% of monthly income. White, Black, Asian, AIAN, and PacIsl one race alone and Latinx-exclusive. Another includes another race and multiracial, and is Latinx-exclusive. QA doc: ", qa_filepath, ". This data is")

# send tables to postgres
to_postgres(county_table, state_table)
city_to_postgres(city_table)
leg_to_postgres(leg_table) 



# Disconnect db -----------------------------------------------------------
dbDisconnect(con)