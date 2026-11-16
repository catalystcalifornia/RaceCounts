# MOSAIC Race Brief Findings - formerly Annual Report Findings #

## QA Filepath: W:\Project\RACE COUNTS\2025_v7\MOSAIC\QA_Sheet_Ann_Report_Findings.docx
## Output saved: W:\Project\RACE COUNTS\2025_v7\MOSAIC\race_brief_findings.xlsx

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

# Race Findings Table -------------------------------------------------------------------
# adapted from: W:\Project\RACE COUNTS\2025_v7\Archive_Annual_Report\ann_report_findings_queries.sql
findings_rc <- dbGetQuery(con, "SELECT geo_name, finding_type, race, finding FROM v7.arei_findings_races_multigeo WHERE geo_level = 'state' AND finding_type IN ('worst count', 'best count')")


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
  left_join(indicator, by = "indicator") %>% # add issue names
  mutate(is_api_split = race == "nh_api" & race_generic %in% c("asian", "pacisl"))

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

# rank outcomes
# 1. rank with the API value counted once (drop the duplicated pacisl copy)
ranked <- df_3 %>%
  filter(!(is_api_split & race_generic == "pacisl")) %>%
  mutate(rate_adj = if_else(asbest == "max", rate, -rate)) %>%  # higher = better outcome
  group_by(indicator, geoid, geo_name) %>%
  mutate(values_count = n(),                 # recalculated: API counted once
         worst_rk = min_rank(rate_adj),      # 1 = worst outcome
         best_rk  = min_rank(-rate_adj)) %>% # 1 = best outcome
  ungroup()

# 2. give the dropped pacisl rows the same rank/count as their asian twin
api_ranks <- ranked %>%
  filter(is_api_split) %>%
  select(indicator, geoid, geo_name, values_count, worst_rk, best_rk)

pacisl_rows <- df_3 %>%
  filter(is_api_split, race_generic == "pacisl") %>%
  select(-any_of("values_count")) %>%
  inner_join(api_ranks, by = c("indicator", "geoid", "geo_name"))

race_rks <- bind_rows(ranked, pacisl_rows) %>%
  select(indicator, indicator_long, arei_issue_area, geoid, geo_name, geo_level,
         race_generic, rate, values_count, asbest, worst_rk, best_rk) %>%
  arrange(indicator, geoid, worst_rk)

race_issue <- race_rks %>%
  filter(!is.na(rate), values_count > 1) %>%
  mutate(best_pct  = (best_rk  - 1) / (values_count - 1),     # 0 = best, 1 = worst
         worst_pct = (worst_rk - 1) / (values_count - 1)) %>% # 0 = worst, 1 = best
  group_by(arei_issue_area, race_generic) %>%
  summarise(avg_best_pct  = mean(best_pct),   # lower = better outcomes overall
            avg_worst_pct = mean(worst_pct),  # lower = worse outcomes overall
            n_total       = n(),
            .groups = "drop") %>%
  group_by(race_generic) %>%
  mutate(best_issue_rk  = dense_rank(avg_best_pct),    # 1 = issue area with best outcomes
         worst_issue_rk = dense_rank(avg_worst_pct)) %>% # 1 = issue area with worst outcomes
  ungroup()

best_worst_issue <- bind_rows(
  race_issue3 %>% filter(best_issue_rk == 1)  %>% mutate(outcome = "best"),
  race_issue3 %>% filter(worst_issue_rk == 1) %>% mutate(outcome = "worst")
) %>%
  select(race_generic, outcome, arei_issue_area, avg_best_pct, avg_worst_pct, n_total) %>%
  arrange(race_generic, outcome) %>%
  filter(!race_generic %in% c("filipino", "other", "twoormor", "white"))

## export data to excel
wb <- loadWorkbook("W:\\Project\\RACE COUNTS\\2025_v7\\MOSAIC\\race_brief_findings.xlsx")

# export RC race findings 
addWorksheet(wb, sheetName = "findings_1")
writeData(wb, sheet = "findings_1", x = findings_rc)


# export issue area disparity findings 
addWorksheet(wb, sheetName = "findings_2")
writeData(wb, sheet = "findings_2", x = disp_issue_rks)

# export indicator disparity findings 
addWorksheet(wb, sheetName = "findings_3")
writeData(wb, sheet = "findings_3", x = disp_indicator_rks)

# export indicator outcome findings 
addWorksheet(wb, sheetName = "findings_4")
writeData(wb, sheet = "findings_4", x = best_worst_issue)

# export 'date of export' 
addWorksheet(wb, sheetName = "export_date")
export_date <- paste0("R export date: ", Sys.Date())
writeData(wb, sheet = "export_date", x = export_date)

# save modified workbook
saveWorkbook(wb, file = "W:\\Project\\RACE COUNTS\\2025_v7\\MOSAIC\\race_brief_findings.xlsx", overwrite = TRUE)

############# Top SD and AD by Race ##############
race_pop <- dbGetQuery(con, "SELECT * FROM v7.arei_race_multigeo") %>%
  select(geoid, name, geolevel, (!starts_with("pct_") & ends_with("_pop") & !contains("swanasa") & !contains("total"))) %>%
  filter(geolevel %in% c("sldu", "sldl"))

top3_dist <- race_pop %>%
  pivot_longer(
    cols = ends_with("_pop"),
    names_to = "pop_group",
    values_to = "pop"
  ) %>%
  group_by(geolevel, pop_group) %>%
  slice_max(pop, n = 3, with_ties = FALSE) %>%
  mutate(rank = row_number()) %>%
  ungroup() %>%
  arrange(geolevel, pop_group, rank)

race_pct <- dbGetQuery(con, "SELECT * FROM v7.arei_race_multigeo") %>%
  select(geoid, name, geolevel, (starts_with("pct_") & !contains("swanasa"))) %>%
  filter(geolevel %in% c("sldu", "sldl"))

pop_pct <- race_pct %>%
  pivot_longer(
    cols = ends_with("_pop"),
    names_to = "pop_group",
    values_to = "pct_pop"
  ) %>%
  mutate(pop_group = gsub("pct_", "", pop_group))

top3_pct <- pop_pct %>%
  group_by(geolevel, pop_group) %>%
  slice_max(pct_pop, n = 3, with_ties = FALSE) %>%
  mutate(rank = row_number()) %>%
  ungroup() %>%
  arrange(geolevel, pop_group, rank)

top3_dist <- top3 %>%
  left_join(pop_pct %>% select(geoid, geolevel, pop_group, pct_pop), by =c("geoid", "geolevel", "pop_group")) %>%
  select(-geolevel, -geoid)

View(top3)