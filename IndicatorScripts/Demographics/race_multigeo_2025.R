## Pop by race/ethnicity for RC 2025 v7 ###
#install packages if not already installed
packages <- c("readr", "tidyr", "dplyr", "DBI", "RPostgreSQL", "tidycensus", "tidyverse", "stringr", "usethis")
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


############## UPDATE VARIABLES ##############
curr_yr = 2023      # you MUST UPDATE each year
rc_yr = '2025'      # you MUST UPDATE each year
rc_schema <- "v7"   # you MUST UPDATE each year
schema = 'demographics'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\QA_Sheet_Race.docx"


# Check that variables in vars_list_acs haven't changed --------
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

# Get Raced Pop ----------------------------------------------------------
table_code = 'dp05'
DP05 <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",table_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
DP05$name <- str_remove(DP05$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
DP05$name <- gsub("; California", "", DP05$name)
cols_to_select <- tolower((paste0(c(vars_list_acs),"e")))  # format colnames to match postgres table colnames

## Clean and join pop tables
races_df <- select(DP05, geoid, name, geolevel, all_of(cols_to_select))

races_df_cols <- colnames(races_df) %>%             # get colnames
  as.data.frame() %>% 
  rename(name = 1)
races_df_cols$name <- ifelse(grepl("dp05", races_df_cols$name), gsub("e", "", races_df_cols$name), races_df_cols$name)

races_df_cols_meta <- races_df_cols %>% 
  left_join(dp05_curr %>% 
              select(name, rc_races), by = 'name')  # join RC race names to data

races_df_cols_meta$rc_races <- ifelse(grepl("dp05", races_df_cols_meta$name), paste0(races_df_cols_meta$rc_races, "_pop"), races_df_cols_meta$name)

# update colnames to RC races
colnames(races_df) <- races_df_cols_meta$rc_races

races_df <- races_df %>% mutate(pct_aian_pop = round((races_df$aian_pop / races_df$total_pop * 100),1), 
                                pct_pacisl_pop = round((races_df$pacisl_pop / races_df$total_pop * 100),1), 
                                pct_latino_pop = round((races_df$latino_pop / races_df$total_pop * 100),1), 
                                pct_nh_white_pop = round((races_df$nh_white_pop / races_df$total_pop * 100),1), 
                                pct_nh_black_pop = round((races_df$nh_black_pop / races_df$total_pop * 100),1), 
                                pct_nh_asian_pop = round((races_df$nh_asian_pop / races_df$total_pop * 100),1), 
                                pct_nh_other_pop = round((races_df$nh_other_pop / races_df$total_pop * 100),1), 
                                pct_nh_twoormor_pop = round((races_df$nh_twoormor_pop / races_df$total_pop * 100),1)) %>% 
  mutate_all(function(x) ifelse(is.nan(x), NA, x))


# Get SWANA / SWANASA Pop ----------------------------------------------
# swana pop
swana_code = 'b04006'
B04006 <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",swana_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
B04006$name <- str_remove(B04006$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
B04006$name <- gsub("; California", "", B04006$name)

# south asian pop
sasian_code = 'b02018'
B02018 <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",sasian_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
B02018$name <- str_remove(B02018$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
B02018$name <- gsub("; California", "", B02018$name)

## Clean and join pop tables
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/SWANA_Ancestry_List.R") # current swana_ancestry() list
vars_list_acs_swana <- get_swana_var(curr_yr, "acs5") # use fx to generate current swana ancestry vars

swana_df <- B04006 %>% select(geoid, geolevel, matches(vars_list_acs_swana)) %>% 
  select(!ends_with("m")) 
swana_df$swana_pop <- rowSums(swana_df[sapply(swana_df, is.numeric)], na.rm = TRUE) # calc SWANA pop

pop_df <- races_df %>% 
  left_join(swana_df %>% 
  select(geoid, geolevel, swana_pop), by = c("geoid","geolevel"))

vars_list_acs_soasian <- get_soasian_var(curr_yr, "acs5") # use fx to generate current So asian ancestry vars

soasian_df <- B02018 %>% select(geoid, geolevel, matches(vars_list_acs_soasian)) %>% 
  select(!ends_with("m"))
soasian_df$soasian_pop <- rowSums(soasian_df[sapply(soasian_df, is.numeric)], na.rm = TRUE) # calc So Asian pop

pop_df <- pop_df %>% 
  left_join(soasian_df %>% 
  select(geoid, geolevel, soasian_pop), by = c("geoid","geolevel"))

pop_df <- pop_df %>% 
  mutate(pct_swana_pop = round((swana_pop / total_pop * 100),1), 
         swanasa_pop = swana_pop + soasian_pop, 
         pct_swanasa_pop = round((swanasa_pop/total_pop * 100),1)) %>%
  relocate(swana_pop, .after = nh_twoormor_pop) %>%
  relocate(swanasa_pop, .after = swana_pop) %>%
  select(-soasian_pop)

############### SEND COUNTY, STATE, CITY POP TO POSTGRES ##############

### info for postgres tables automatically updates ###
table_name <- "arei_race_multigeo"    
indicator <- "City, County, State Senate, State Assembly, and State population by race/ethnicity for RC Place and Race pages"       
start_yr <- curr_yr - 4
source <- paste0("ACS ", start_yr, "-", curr_yr, " Tables DP05, B04006 (SWANA), B02018 (S Asian). All AIAN, All NHPI, All Latinx, All SWANA/SWANA+SA, all other groups are one race alone and non-Latinx") 
column_names <- colnames(pop_df)
column_comments <- races_df_cols_meta$label  # note swana/swanasa cols do not have comments

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

dbWriteTable(con, 
             Id(schema = rc_schema, table = table_name), 
             pop_df, overwrite = FALSE)

# comment on table and columns
add_table_comments(con, rc_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

#dbDisconnect(con) 


