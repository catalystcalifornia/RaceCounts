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


############## UPDATE FOR SPECIFIC INDICATOR HERE ##############
curr_yr = 2023      # you MUST UPDATE each year
rc_yr = '2025'      # you MUST UPDATE each year
rc_schema <- "v7"   # you MUST UPDATE each year
schema = 'demographics'

# Get Raced Pop ----------------------------------------------------------
table_code = 'dp05'
DP05 <- dbGetQuery(con, paste0("select * from ",schema,".acs_5yr_",table_code,"_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
DP05$name <- str_remove(DP05$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
DP05$name <- gsub("; California", "", DP05$name)

# check that DP05 variable names haven't changed by pulling in metadata from API
dp05_vars <- load_variables(curr_yr, "acs5/profile", cache = TRUE) %>% 
  filter(grepl("DP05", name)) %>% 
  mutate(name = tolower(name)) # get all DP05 vars
dp05_vars$label <- gsub("Estimate!!|HISPANIC OR LATINO AND RACE!!", "", dp05_vars$label)

prev_yr <- curr_yr - 1
prev_dp05_vars <- load_variables(prev_yr, "acs5/profile", cache = TRUE) %>% 
  filter(grepl("DP05", name)) %>% 
  mutate(name = tolower(name)) # get all DP05 vars
prev_dp05_vars$label <- gsub("Estimate!!|HISPANIC OR LATINO AND RACE!!", "", prev_dp05_vars$label)

all.equal(dp05_vars, prev_dp05_vars)  # if NOT equal, you will likely need to revise the list of dp05 variables selected to create races_df below 

## Clean and join pop tables
races_df <- select(DP05, geoid, name, geolevel, dp05_0001e, dp05_0071e, dp05_0073e, dp05_0076e, dp05_0082e, dp05_0083e, dp05_0085e, dp05_0087e, dp05_0088e)

races_df <- races_df %>% mutate(dp05_0071pe = round((races_df$dp05_0071e / races_df$dp05_0001e * 100),1), 
                                dp05_0073pe = round((races_df$dp05_0073e / races_df$dp05_0001e * 100),1), 
                                dp05_0076pe = round((races_df$dp05_0076e / races_df$dp05_0001e * 100),1), 
                                dp05_0082pe = round((races_df$dp05_0082e / races_df$dp05_0001e * 100),1), 
                                dp05_0083pe = round((races_df$dp05_0083e / races_df$dp05_0001e * 100),1), 
                                dp05_0085pe = round((races_df$dp05_0085e / races_df$dp05_0001e * 100),1), 
                                dp05_0087pe = round((races_df$dp05_0087e / races_df$dp05_0001e * 100),1), 
                                dp05_0088pe = round((races_df$dp05_0088e / races_df$dp05_0001e * 100),1)) %>% 
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

races_df_cols <- colnames(races_df) %>% 
  as.data.frame() %>% 
  rename(name = 1)
races_df_cols$name <- gsub("pe", "p", races_df_cols$name)
races_df_cols$name <- ifelse(grepl("dp05", races_df_cols$name), gsub("e", "", races_df_cols$name), races_df_cols$name)
races_df_cols

races_df_cols_meta <- races_df_cols %>% 
  left_join(dp05_vars %>% 
  select(name, label), by = 'name')  # join pop_df colnames with corresponding metadata from API

new_names <- c("geoid", "name", "geolevel", "total_pop", "aian_pop", "pacisl_pop", "latino_pop", "nh_white_pop", "nh_black_pop", "nh_asian_pop", "nh_other_pop", "nh_twoormor_pop", "pct_aian_pop", "pct_pacisl_pop", "pct_latino_pop", "pct_nh_white_pop", "pct_nh_black_pop", "pct_nh_asian_pop", "pct_nh_other_pop", "pct_nh_twoormor_pop")
races_df_cols_meta$new_name <- new_names
races_df_cols_meta  # check that label and new_name match

colnames(races_df) <- races_df_cols_meta$new_name  # update to RC colnames


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
  relocate(swanasa_pop, .after = swana_pop) %>%
  select(-soasian_pop)

############### SEND COUNTY, STATE, CITY POP TO POSTGRES ##############

### info for postgres tables automatically updates ###
table_name <- "arei_race_multigeo"    
indicator <- "City, County, State Senate, State Assembly, and State population by race/ethnicity for RC Place and Race pages"       
start_yr <- curr_yr - 4
source <- paste0("ACS ", start_yr, "-", curr_yr, " Tables DP05, B04006 (SWANA), B02018 (S Asian). All AIAN, All NHPI, All Latinx, All SWANA/SWANA+SA, all other groups are one race alone and non-Latinx") 
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\QA_Sheet_Race.docx"
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


