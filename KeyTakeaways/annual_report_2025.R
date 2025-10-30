## QA Filepath: W:\Project\RACE COUNTS\2025_v7\Annual_Report\QA_Sheet_Ann_Report_Findings.docx
## Output saved: W:\Project\RACE COUNTS\2025_v7\Annual_Report\statewide_findings.xlsx

##### Findings #2 & 3: Rank issues and indicators by disparity by race
#### PULLED & ADAPTED FROM key_findings_2025.R and W:/Project/RACE COUNTS/2025_v7/RC_Github/LF/RaceCounts/KeyTakeaways/most_disp_issue.R

#### Most disparate issue area and indicator ####

#install packages if not already installed
packages <- c("tidyverse","RPostgres","sf","data.table","usethis","xfun","openxlsx")  

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
source("./KeyTakeaways/key_findings_functions.R")

# remove exponentiation
options(scipen = 100) 

# udpate each yr
rc_yr <- '2025'
rc_schema <- 'v7'

#########################

# State Data Tables -------------------------------------------------------------------
rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", rc_schema, "' AND table_name LIKE '%_", rc_yr, "';")

rc_list <- dbGetQuery(con, rc_list_query)

# Note state tables do not have performance z scores
# filter for only state level indicator tables
state_list <- rc_list %>%
  filter(grepl(paste0("^arei_.*_state_", rc_yr, "$"), table_name)) %>%
  arrange(table_name) %>% # alphabetize
  pull(table_name) # converts from df object to list; important for next steps using lapply

# import all tables on state_list
state_tables <- lapply(setNames(paste0("select * from ", rc_schema, ".", state_list), state_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
state_tables <- map2(state_tables, names(state_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

# create a long df of race disparity values from every arei_ state table
state_tables_disparity <- lapply(state_tables, function(x)
  x %>% select(state_id, state_name, asbest,ends_with("disparity_z"), indicator, values_count))

state_disparity <- imap_dfr(state_tables_disparity, ~
                              .x %>% pivot_longer(cols = ends_with("disparity_z"),
                                                  names_to = "race",
                                                  values_to = "disparity_z_score")) %>%
  mutate(race = (ifelse(race == 'disparity_z', 'total', race)),
         race = gsub('_disparity_z', '', race))

# create a long df of race rates from every arei_ state table
state_tables_rate <- lapply(state_tables, function(x)
  x %>% select(state_id, state_name, asbest, ends_with("_rate"), indicator, values_count))

state_rate <- imap_dfr(state_tables_rate , ~
                         .x %>% pivot_longer(cols = ends_with("_rate"),
                                             names_to = "race",
                                             values_to = "rate")) %>% 
  mutate(race = (ifelse(race == 'rate', 'total', race)),
         race = gsub('_rate', '', race))

# merge all 2 (disparity, rate) long dfs - note: state tables do not have performance z scores
df_merged_state <- state_disparity %>% 
  full_join(state_rate)

# create issue, indicator, geo_level columns for issue tables
df_state <- df_merged_state %>% 
  mutate(issue = substring(indicator, 6,9),
         indicator = substring(indicator, 11),
         indicator = gsub(paste0('_state_', rc_yr), '', indicator),
         geo_level = "state",
         race_generic = gsub('nh_', '', race),						  # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
         race_generic = gsub('swanasa', 'swana', race_generic)) %>%   # recode swanasa as swana to help generate counts by race later
  rename(geoid = state_id, geo_name = state_name) %>%
  select(geoid, geo_name, everything())

final_df <- df_state

# Get long form race names for findings ------------------------------------------------
race_generic <- unique(final_df$race_generic)
long_name <- c("Asian / Pacific Islander", "Black", "Latinx", "American Indian / Alaska Native", "White", "Asian", "Native Hawaiian / Pacific Islander", "Southwest Asian / North African", "Multiracial", "Another Race", "Filipinx", "Total")
race_names <- data.frame(race_generic, long_name)
print("Check the console to ensure race_generic and long_name match.")
race_names

# Create indicator long name df -------------------------------------------
### NOTE: This list may need to be updated or re-ordered. ###
indicator <- dbGetQuery(con, paste0("SELECT arei_indicator AS indicator_long, api_name AS indicator, arei_issue_area FROM ", rc_schema, ".arei_indicator_list_cntyst")) %>%
  mutate(arei_issue_area = gsub("Crime", "Safety", arei_issue_area))

#####################################

df_3 <- final_df %>%
  filter(race != 'total')   	# remove total rates bc all findings in this section are raced

# race_generic == api records are duplicated, then race_generic is renamed as "asian" and "pacisl"
df_3 <- api_split(df_3) %>%
  left_join(indicator, by = "indicator") # add issue names
  
######################################

#rank issue areas by disparity for each race
disp_issue_rks <- df_3 %>% select(race_generic, issue, arei_issue_area, disparity_z_score) %>% 
  group_by(race_generic, issue, arei_issue_area) %>% 
  summarise(avg_id = mean(disparity_z_score)) %>%
  group_by(race_generic) %>%
  mutate(most_disp_rk = dense_rank(-avg_id),
         least_disp_rk = dense_rank(avg_id)) 

disp_issue_rks <- disp_issue_rks %>%
  left_join(race_names, by = "race_generic") %>%
  select(long_name, race_generic, arei_issue_area, issue, most_disp_rk, least_disp_rk, avg_id) %>%
  arrange(race_generic, most_disp_rk) # sort from most disparate to least disparate issue
View(disp_issue_rks)

#rank indicators by disparity for each race
disp_indicator_rks <- df_3 %>% select(race_generic, indicator, indicator_long, disparity_z_score) %>% 
  group_by(race_generic) %>% 
  mutate(most_disp_rk = dense_rank(-disparity_z_score),
         least_disp_rk = dense_rank(disparity_z_score)) 

disp_indicator_rks <- disp_indicator_rks %>%
  left_join(race_names, by = "race_generic") %>%
  select(long_name, race_generic, indicator_long, indicator, most_disp_rk, least_disp_rk, disparity_z_score) %>%
  arrange(race_generic, most_disp_rk) # sort from most disparate to least disparate indicator
View(disp_indicator_rks)

## export data to excel
wb <- loadWorkbook("W:\\Project\\RACE COUNTS\\2025_v7\\Annual_Report\\ann_report_findings.xlsx")

# export issue area disparity findings 
addWorksheet(wb, sheetName = "findings_2")
writeData(wb, sheet = "findings_2", x = disp_issue_rks)

# export indicator disparity findings 
addWorksheet(wb, sheetName = "findings_3")
writeData(wb, sheet = "findings_3", x = disp_indicator_rks)

# export 'date of export' 
addWorksheet(wb, sheetName = "export_date")
export_date <- paste0("R export date: ", Sys.Date())
writeData(wb, sheet = "export_date", x = export_date)

# save modified workbook
saveWorkbook(wb, file = "W:\\Project\\RACE COUNTS\\2025_v7\\Annual_Report\\ann_report_findings.xlsx", overwrite = TRUE)
