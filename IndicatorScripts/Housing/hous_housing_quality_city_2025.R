## Low Quality Housing Units RC v7 City ##

# Install packages if not already installed ###
packages <- c("data.table", "stringr", "dplyr", "RPostgres", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "here", "sf", "usethis")

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
con <- connect_to_db("rda_shared_data")
con2<- connect_to_db("racecounts")

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Sheet_Housing_Quality_City.docx"

#set source for weighted averages fx
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/Cnty_St_Wt_Avg_Functions.R")


############## UPDATE FOR SPECIFIC INDICATOR HERE ##############
rc_yr <- '2025'
rc_schema <- 'v7'
acs_yr = 2023
survey = "acs5"
subgeo <- c('tract')        # define your sub geolevel: tract (unless the WA functions are adapted for a different subgeo)
pop_threshold = 0           # 0 bc this is a placeholder threshold for housing units. We need to assign pop threshold a value in order for functions to work.
population_threshold = 250  # Actual threshold for population used to screen data.

# Check that B25003 variables and RC race names still match each year and update if needed ####
## Total Households: total, black, aian, asian, pacisl, other, twoormor, nh_white, latinx (All except Two+ and Latinx are 1 race alone, all except Latinx are non-Latinx.)
vars_list_b25003 <- list("total_" = "B25003_001", 
                         "black_" = "B25003B_001", 
                         "aian_" = "B25003C_001", 
                         "asian_" = "B25003D_001", 
                         "pacisl_" = "B25003E_001", 
                         "other_" = "B25003F_001", 
                         "twoormor_" = "B25003G_001", 
                         "nh_white_" = "B25003H_001", 
                         "latino_" = "B25003I_001")

race_mapping <- data.frame(
  name = unlist(vars_list_b25003),
  race = names(vars_list_b25003),
  stringsAsFactors = FALSE
)

b25003_curr <- load_variables(acs_yr, "acs5", cache = TRUE) %>% 
  filter(name %in% vars_list_b25003) %>%
  select(-c(geography)) %>% 
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop"))


# CHECK THIS TABLE TO MAKE SURE THE CONCEPT AND RC_RACES COLUMNS MATCH UP
print(b25003_curr) 


# Check that B01001 variables and RC race names still match each year and update if needed ####
## Population used for screening: total, black, aian, asian, pacisl, other, twoormor, nh_white, latinx (All except Two+ and Latinx are 1 race alone, all except Latinx are non-Latinx.)
vars_list_b01001 <- list("total_" = "B01001_001", 
                         "black_" = "B01001B_001", 
                         "aian_" = "B01001C_001", 
                         "asian_" = "B01001D_001", 
                         "pacisl_" = "B01001E_001", 
                         "other_" = "B01001F_001", 
                         "twoormor_" = "B01001G_001", 
                         "nh_white_" = "B01001H_001", 
                         "latino_" = "B01001I_001")

race_mapping <- data.frame(
  name = unlist(vars_list_b01001),
  race = names(vars_list_b01001),
  stringsAsFactors = FALSE
)

b01001_curr <- load_variables(acs_yr, "acs5", cache = TRUE) %>% 
  filter(name %in% vars_list_b01001) %>%
  select(-c(geography)) %>% 
  left_join(race_mapping, by="name") %>%
  mutate(rc_races = paste0(race, "pop"))


# CHECK THIS TABLE TO MAKE SURE THE CONCEPT AND RC_RACES COLUMNS MATCH UP
print(b01001_curr) 


# Check that B25040 / B25048 / B25052 variables are still correct, update if needed ####
## Total Incomplete Heating
vars_list_b25040 <- list("incomplete_heating_" = "B25040_010")

ind_mapping <- data.frame(
  name = unlist(vars_list_b25040),
  ind = names(vars_list_b25040),
  stringsAsFactors = FALSE
)


b25040_curr <- load_variables(acs_yr, "acs5", cache = TRUE) %>% 
  filter(name %in% vars_list_b25040) %>%
  select(-c(geography)) %>% 
  left_join(ind_mapping, by="name")


# CHECK THIS TABLE TO MAKE SURE THE CONCEPT IS STILL CORRECT
print(b25040_curr) 

## Total Incomplete Plumbing
vars_list_b25048 <- list("incomplete_plumbing_" = "B25048_003")

ind_mapping <- data.frame(
  name = unlist(vars_list_b25048),
  ind = names(vars_list_b25048),
  stringsAsFactors = FALSE
)


b25048_curr <- load_variables(acs_yr, "acs5", cache = TRUE) %>% 
  filter(name %in% vars_list_b25048) %>%
  select(-c(geography)) %>% 
  left_join(ind_mapping, by="name")

# CHECK THIS TABLE TO MAKE SURE THE CONCEPT IS STILL CORRECT
print(b25048_curr) 

## Total Incomplete Kitchen
vars_list_b25052 <- list("incomplete_kitchen_" = "B25052_003")

ind_mapping <- data.frame(
  name = unlist(vars_list_b25052),
  ind = names(vars_list_b25052),
  stringsAsFactors = FALSE
)


b25052_curr <- load_variables(acs_yr, "acs5", cache = TRUE) %>% 
  filter(name %in% vars_list_b25052) %>%
  select(-c(geography)) %>% 
  left_join(ind_mapping, by="name")


# CHECK THIS TABLE TO MAKE SURE THE CONCEPT IS STILL CORRECT
print(b25052_curr) 


vars_list <- as.list(bind_cols(vars_list_b25003, vars_list_b25040, vars_list_b25048, vars_list_b25052))


# HOUSING UNITS BY HOUSEHOLDER RACE WITH NO FUEL / NO PLUMBING / NO KITCHEN ------------------------------------------------------------------

# Pull data from Census API
df <- do.call(rbind.data.frame, list(
  get_acs(geography = subgeo, state = "CA", variables = vars_list, year = acs_yr, survey = survey, cache_table = TRUE)
  %>% mutate(geolevel = subgeo)
))


# Rename estimate and moe columns to e and m, respectively
df <- df %>%
  rename(e = estimate, m = moe)

# Spread! Switch to wide format.
df <- df %>%
  pivot_wider(names_from=variable, values_from=c(e, m), names_glue = "{variable}{.value}")

# Calc % of low quality housing in each category
df <- df %>%
  mutate(pct_incomplete_heating = ifelse(total_e == 0, NA, incomplete_heating_e / total_e * 100),
         pct_incomplete_plumbing = ifelse(total_e == 0, NA, incomplete_plumbing_e / total_e * 100),
         pct_incomplete_kitchen = ifelse(total_e == 0, NA, incomplete_kitchen_e / total_e * 100)) %>%
  rename(sub_id = GEOID)
 

### CT-Place Crosswalk #####
# set source for Crosswalk Function script
source("./Functions/RC_CT_Place_Xwalk.R")
crosswalk <- make_ct_place_xwalk(acs_yr) # must specify which data year

# universe (housing units total and by race)
universe <- df %>% select(sub_id, NAME, (!starts_with("incomplete") & ends_with("_e")))

# join subgeo pop data with xwalk to get target geoids/names and rename to generic column names for WA functions
universe <- universe %>% 
  right_join(select(crosswalk, c(ct_geoid, place_geoid)), by = c("sub_id" = "ct_geoid")) %>%
  dplyr::rename(target_id = place_geoid)  

# rename to pop
pop <- universe %>% rename_all(.funs = funs(sub("*_e", "_pop", names(universe)))) %>%
  as.data.frame() %>%
  mutate(geolevel = subgeo)


# CITY WEIGHTED AVG CALCS ------------------------------------------------
# select pop columns and rename to RC names
b <- select(pop, sub_id, target_id, ends_with("pop"), -NAME)

# aggregate sub geolevel pop to target geolevel
c <- b %>% group_by(target_id) %>% summarise_if(is.numeric, sum)
colnames(c) <- gsub("_pop", "_target_pop", colnames(c))

# count number of sub geolevels  per target geolevel and join to target geolevel pop
d <- b %>% dplyr::count(target_id)
c <- c %>% left_join(d, by = "target_id")

# join target geolevel pop and sub geolevel counts to df, drop margin of error cols, rename tract pop cols
e <- select(pop, sub_id, target_id, geolevel, ends_with("pop"), -NAME) 
names(e) <-gsub("_pop", "_sub_pop", colnames(e))
pop <- e %>% left_join(c, by = "target_id")
pop_df <- as.data.frame(pop)

# calc % of subgeo pop in target geos
pct_df <- pop_pct_multi(pop_df)  # NOTE: use function for cases where a subgeo can match to more than 1 targetgeo to calc pct of target geolevel pop in each sub geolevel


## indicator 1: Lack of Kitchen

# Rename so WA functions can work
ind_df <- df %>% select(sub_id, NAME, pct_incomplete_kitchen) %>% rename(indicator = pct_incomplete_kitchen)


# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df, ind_df) 

# rename columns for RC functions
kitchen_city_wa <- city_wa %>%
  left_join(crosswalk %>% select(place_geoid, place_name) %>%
            distinct(), by = c("target_id" = "place_geoid"), relationship = "one-to-many")

# rename columns
kitchen_city_wa <- kitchen_city_wa %>%
  rename_all(.funs = funs(sub("rate", "kitchen", names(kitchen_city_wa))))


## indicator 2: Lack of Heating

# Rename so WA functions can work
ind_df <- df %>% select(sub_id, NAME, pct_incomplete_heating) %>% rename(indicator = pct_incomplete_heating)

# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df, ind_df)        

heating_city_wa <- city_wa %>%
  left_join(crosswalk %>% select(place_geoid, place_name) %>%
            distinct(), by = c("target_id" = "place_geoid"), relationship = "one-to-many")
  
# rename columns
heating_city_wa <- heating_city_wa %>%
  rename_all(.funs = funs(sub("rate", "heating", names(heating_city_wa))))


## indicator 3: Lack of Plumbing

# Rename so WA functions can work
ind_df <- df %>% select(sub_id, NAME, pct_incomplete_plumbing) %>% rename(indicator = pct_incomplete_plumbing)

# calc weighted average and apply reliability screens
city_wa <- wt_avg(pct_df, ind_df)        

plumbing_city_wa <- city_wa %>% 
  left_join(crosswalk %>% select(place_geoid, place_name) %>%
            distinct(), by = c("target_id" = "place_geoid"), relationship = "one-to-many")

# rename columns
plumbing_city_wa <- plumbing_city_wa %>%
  rename_all(.funs = funs(sub("rate", "plumbing", names(plumbing_city_wa))))


# Merge all 3 indicators

df_calcs <- kitchen_city_wa %>% full_join(heating_city_wa) %>% full_join(plumbing_city_wa) %>%
  rename(geoname = place_name, geoid = target_id) %>%
  select(geoid, geoname, ends_with("kitchen"), ends_with("plumbing"), ends_with("heating"), everything())

## average out the rates, ## filter for cities that only have 1 of the 3 indicator for total/by race. 
df_average <- df_calcs %>% mutate(
  
  nh_white_rate = ifelse(!is.na(nh_white_heating) & is.na(nh_white_kitchen) & is.na(nh_white_plumbing) | is.na(nh_white_heating) & !is.na(nh_white_kitchen) & is.na(nh_white_plumbing) | is.na(nh_white_heating) & is.na(nh_white_kitchen) & !is.na(nh_white_plumbing), NA, (nh_white_heating + nh_white_kitchen + nh_white_plumbing) / 3),
  
  black_rate = ifelse(!is.na(black_heating) & is.na(black_kitchen) & is.na(black_plumbing) | is.na(black_heating) & !is.na(black_kitchen) & is.na(black_plumbing) | is.na(black_heating) & is.na(black_kitchen) & !is.na(black_plumbing), NA, (black_heating + black_kitchen + black_plumbing) / 3),
  
  aian_rate = ifelse(!is.na(aian_heating) & is.na(aian_kitchen) & is.na(aian_plumbing) | is.na(aian_heating) & !is.na(aian_kitchen) & is.na(aian_plumbing) | is.na(aian_heating) & is.na(aian_kitchen) & !is.na(aian_plumbing), NA, (aian_heating + aian_kitchen + aian_plumbing) / 3),
  
  latino_rate = ifelse(!is.na(latino_heating) & is.na(latino_kitchen) & is.na(latino_plumbing) | is.na(latino_heating) & !is.na(latino_kitchen) & is.na(latino_plumbing) | is.na(latino_heating) & is.na(latino_kitchen) & !is.na(latino_plumbing), NA, (latino_heating + latino_kitchen + latino_plumbing) / 3),
  
  asian_rate = ifelse(!is.na(asian_heating) & is.na(asian_kitchen) & is.na(asian_plumbing) | is.na(asian_heating) & !is.na(asian_kitchen) & is.na(asian_plumbing) | is.na(asian_heating) & is.na(asian_kitchen) & !is.na(asian_plumbing), NA, (asian_heating + asian_kitchen + asian_plumbing) / 3),
  
  pacisl_rate = ifelse(!is.na(pacisl_heating) & is.na(pacisl_kitchen) & is.na(pacisl_plumbing) | is.na(pacisl_heating) & !is.na(pacisl_kitchen) & is.na(pacisl_plumbing) | is.na(pacisl_heating) & is.na(pacisl_kitchen) & !is.na(pacisl_plumbing), NA, (pacisl_heating + pacisl_kitchen + pacisl_plumbing) / 3),
  
  other_rate = ifelse(!is.na(other_heating) & is.na(other_kitchen) & is.na(other_plumbing) | is.na(other_heating) & !is.na(other_kitchen) & is.na(other_plumbing) | is.na(other_heating) & is.na(other_kitchen) & !is.na(other_plumbing), NA, (other_heating + other_kitchen + other_plumbing) / 3),
  
  twoormor_rate = ifelse(!is.na( twoormor_heating) & is.na( twoormor_kitchen) & is.na( twoormor_plumbing) | is.na( twoormor_heating) & !is.na( twoormor_kitchen) & is.na( twoormor_plumbing) | is.na( twoormor_heating) & is.na( twoormor_kitchen) & !is.na( twoormor_plumbing), NA, ( twoormor_heating +  twoormor_kitchen +  twoormor_plumbing) / 3),
  
  total_rate = ifelse(!is.na(total_heating) & is.na(total_kitchen) & is.na(total_plumbing) | is.na(total_heating) & !is.na(total_kitchen) & is.na(total_plumbing) | is.na(total_heating) & is.na(total_kitchen) & !is.na(total_plumbing), NA, (total_heating + total_kitchen + total_plumbing) / 3)

) %>% select(geoid, geoname, ends_with("rate")
) 


# Pop Screening -----------------------------------------------------------

# Pull data from Census API
pop2 <- do.call(rbind.data.frame, list(
  get_acs(geography = "place", state = "CA", variables = vars_list_b01001, year = acs_yr, survey = survey, cache_table = TRUE)
  %>% mutate(geolevel = "place" )
))

# Rename estimate and moe columns to e and m, respectively
pop2 <- pop2 %>%
  rename(pop = estimate, m = moe, geoid = GEOID)

# Spread! Switch to wide format.
pop2 <- pop2 %>%
  pivot_wider(names_from=variable, values_from=c(pop, m), names_glue = "{variable}{.value}") %>%
  select(geoid, geolevel, ends_with("pop"))



df_screen <- df_average %>% left_join(pop2) %>%
  mutate(
    total_rate = ifelse(total_pop < population_threshold, NA, total_rate),
    nh_white_rate = ifelse(nh_white_pop < population_threshold, NA, nh_white_rate),
    black_rate = ifelse(black_pop < population_threshold, NA, black_rate),
    aian_rate = ifelse(aian_pop < population_threshold, NA, aian_rate),
    asian_rate = ifelse(asian_pop < population_threshold, NA, asian_rate),
    pacisl_rate = ifelse(pacisl_pop < population_threshold, NA, pacisl_rate),
    other_rate = ifelse(other_pop < population_threshold, NA, other_rate),
    twoormor_rate = ifelse(twoormor_pop < population_threshold, NA, twoormor_rate),
    latino_rate = ifelse(latino_pop < population_threshold, NA, latino_rate)
  )


d <- df_screen



############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ county_id and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d)    #calculate number of "_rate" values
d <- calc_best(d)       #calculate best rates -- be sure to define 'asbest' accordingly before running this function.
d <- calc_diff(d)       #calculate difference from best
d <- calc_avg_diff(d)   #calculate (row wise) mean difference from best
d <- calc_s_var(d)      #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d)         #calculate index of disparity

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'place', ] %>% select(-c(geolevel))

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
# View(city_table)

# Table metadata will auto update #
city_table_name <- paste0("arei_hous_housing_quality_city_", rc_yr)
start_yr <- acs_yr-4

indicator <- paste0("Average percent of households that lack kitchen, plumbing, and heat in comparison to total households. QA doc: ", qa_filepath)
source <- paste0("American Community Survey 5-Year Estimates (", start_yr,"-",acs_yr, ") Tables B25048, B25052, B25003B-I, B25040.")

city_to_postgres(city_table)

#close connection
dbDisconnect(con)
dbDisconnect(con2)