#### Most disparate & Worst outcome issue area and indicators ####
## This script identifies the most disparate issue, most disparate indicators, and most disparate indicators within each issue.
## This script identifies the worst outcome issue, worst outcome indicators, and worst outcome indicators within each issue.

#install packages if not already installed
packages <- c("tidyverse","RPostgres","sf","data.table","usethis")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

# Load PostgreSQL driver and databases --------------------------------------------------
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# Set Source for Index Functions script -----------------------------------
source("./Functions/RC_Index_Functions.R")

# remove exponentiation
options(scipen = 100) 

# udpate each yr
rc_yr <- '2025'
rc_schema <- 'v7'
table_geolevel <- 'leg'  # state, county, leg, city (geolevel in the postgres table name)
geolev <- 'sldu'         # state, county, sldl, sldu, city (geolevel in the geolevel column)
geoid <- '06021'         # fips code

# Add indicators and arei_multigeo_list ------------------------------------------------------
####################### ADD state DATA #####################################
rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", rc_schema, "' AND table_name LIKE '%", table_geolevel, "_", rc_yr, "';")
rc_list <- dbGetQuery(con, rc_list_query)
table_list <- rc_list[order(rc_list$table_name), ] # alphabetize list of indicator tables, changes df to list the needed format for next step
table_list <- table_list[!grepl("index", table_list)] # filter out index tables


data_tables <- lapply(setNames(paste0("select * from ", rc_schema, ".", table_list), table_list), DBI::dbGetQuery, conn = con)
length(data_tables) # check that number of indicator tables is correct

# filter for geolevel and geoid, as appropriate
data_tables_ <- if (table_geolevel == 'leg') {
  lapply(data_tables, function(x) x %>% filter(geolevel == geolev & leg_id == geoid))
} else if (table_geolevel == 'county') {
  lapply(data_tables, function(x) x %>% filter(county_id == geoid))
} else if (table_geolevel == 'city') {
  lapply(data_tables, function(x) x %>% filter(city_id == geoid))
} else {    # no filtering required for state data
  data_tables
}

data_tables_ <- map2(data_tables_, names(data_tables_), ~ mutate(.x, table_name = .y)) # create column with table_name used to create issue and indicator cols
data_tables_ <- lapply(data_tables_, function(x) x %>% select(ends_with("_id"), ends_with("_name"), index_of_disparity, disparity_z, performance_z, table_name) %>% # create issue area name column
         mutate(issue_area = substring(table_name, 6,9), variable = substring(table_name, 11,nchar(table_name))) %>%      # create indicator name column 
         mutate(variable = gsub(paste0("_", table_geolevel, "_", rc_yr), "", variable)))                                                 # clean indicator name column


### by disparity_z (same as key_findings methodology)
#most disparate issue area
most_disp_issuez <- rbindlist(data_tables_) %>%
  select(issue_area, disparity_z) %>%
  group_by(issue_area) %>%
  summarise(avg_id = mean(disparity_z)) %>%
  arrange(, desc(avg_id))  # sort from most disparate to least disparate issue (compares across issues in 1 geo, not across geos)
View(most_disp_issuez)

#most disparate indicators
most_disp_indicatorsz <- rbindlist(data_tables_) %>%
  select(variable, disparity_z) %>%
  arrange(, desc(disparity_z))  # sort from most disparate to least disparate indicator
View(most_disp_indicatorsz)

#most disparate indicator by issue
most_disp_indicator_by_issuez <- rbindlist(data_tables_) %>%
  select(issue_area, variable, disparity_z) %>%
  group_by(issue_area) %>%
  slice_max(disparity_z) %>%    # max bc highest disp_z is worst
  arrange(, desc(disparity_z))  # sort from most disparate to least disparate indicator
View(most_disp_indicator_by_issuez)

### by performance_z (same as key_findings methodology)
#worst outcomes issue area
worst_outc_issuez <- rbindlist(data_tables_) %>%
  select(issue_area, performance_z) %>%
  group_by(issue_area) %>%
  summarise(avg_perf = mean(performance_z)) %>%
  arrange(, avg_perf)  # sort from worst outcome to best outcome issue
View(worst_outc_issuez)

#worst outcome indicators
worst_outc_indicatorsz <- rbindlist(data_tables_) %>%
  select(variable, performance_z) %>%
  arrange(, performance_z)  # sort from worst outcome to best outcome indicator
View(worst_outc_indicatorsz)

#worst outcome indicator by issue
worst_outc_indicator_by_issuez <- rbindlist(data_tables_) %>%
  select(issue_area, variable, performance_z) %>%
  group_by(issue_area) %>%
  slice_min(performance_z) %>%  # min bc lowest perf_z is worst
  arrange(, performance_z)  # sort from worst outcome to best outcome indicator
View(worst_outc_indicator_by_issuez)



# ### by ID - alternate measure for disparity (not scaled)
# #most disparate issue area
# most_disp_issue <- rbindlist(data_tables_) %>% select(issue_area, index_of_disparity) %>% group_by(issue_area) %>% summarise(avg_id = mean(index_of_disparity)) %>%
#                       arrange(, desc(avg_id))  # sort from most disparate to least disparate issue
# View(most_disp_issue)
# 
# #most disparate indicators
# most_disp_indicators <- rbindlist(data_tables_) %>% select(variable, index_of_disparity) %>% arrange(, desc(index_of_disparity))  # sort from most disparate to least disparate indicator
# View(most_disp_indicators)
# 
# #most disparate indicator by issue
# most_disp_indicator_by_issue <- rbindlist(data_tables_) %>% select(issue_area, variable, index_of_disparity) %>% group_by(issue_area) %>% slice_max(index_of_disparity) %>%
#   arrange(, desc(index_of_disparity))  # sort from most disparate to least disparate indicator
# View(most_disp_indicator_by_issue)

dbDisconnect(con)

