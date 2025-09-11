#### Most disparate issue area and indicator ####

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

####################### ADD DATA #####################################
rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", rc_schema, "' AND table_name LIKE '%leg_", rc_yr, "' AND table_name NOT LIKE '%index_%' ;")
rc_list <- dbGetQuery(con, rc_list_query)
table_list <- rc_list[order(rc_list$table_name), ] # alphabetize list of state tables, changes df to list the needed format for next step

leg_tables <- lapply(setNames(paste0("select * from ", rc_schema, ".", table_list), table_list), DBI::dbGetQuery, conn = con)
length(leg_tables) # check that number of indicator tables is correct

leg_tables_ <- map2(leg_tables, names(leg_tables), ~ mutate(.x, table_name = .y)) # create column with table_name used to create issue and indicator cols
leg_tables_ <- lapply(leg_tables_, function(x) x %>% 
  select(leg_id, leg_name, geolevel, index_of_disparity, performance_z, table_name) %>%                          
  mutate(issue_area = substring(table_name, 6,9), variable = substring(table_name, 11, nchar(table_name))) %>%   # create issue & indicator name columns 
  mutate(variable = gsub(paste0("_leg_", rc_yr), "", variable), leg_name = gsub("State ", "", leg_name)))        # clean indicator & name columns

####################### RANK ISSUES AND INDICATORS BY DISPARITY #####################################

#rank issues by disparity for Sen/Assm
## note: there is only 1 indicator in democracy and health for v7
most_disp_issue <- rbindlist(leg_tables_) %>% select(leg_id, leg_name, geolevel, issue_area, index_of_disparity) %>% 
  group_by(leg_id, leg_name, geolevel, issue_area) %>% 
  summarise(avg_id = mean(index_of_disparity)) %>%
  group_by(leg_id, leg_name, geolevel) %>% 
  mutate(rk = dense_rank(-avg_id)) %>%                         # calc rank, 1 = most disp issue
  arrange(leg_id, geolevel, desc(avg_id))                      # sort from most disparate to least disparate issue
View(most_disp_issue)

#rank indicators by disparity for Sen/Assm
most_disp_indicators <- rbindlist(leg_tables_) %>% 
  select(leg_id, leg_name, geolevel, variable, index_of_disparity) %>% 
  group_by(leg_id, leg_name, geolevel) %>% 
  mutate(rk = dense_rank(-index_of_disparity)) %>%              # calc rank, 1 = most disp indicator  
  arrange(leg_id, geolevel, desc(index_of_disparity))           # sort from most disparate to least disparate indicator
View(most_disp_indicators)

#rank indicators by disparity in each issue for Sen/Assm
## note: there is only 1 indicator in democracy and health for v7
most_disp_indicator_by_issue <- rbindlist(leg_tables_) %>% 
  select(leg_id, leg_name, geolevel, issue_area, variable, index_of_disparity) %>%
  group_by(leg_id, geolevel, issue_area) %>% 
  slice_max(index_of_disparity) %>%
  arrange(leg_id, geolevel, desc(index_of_disparity))           # sort from most disparate to least disparate indicator by issue
View(most_disp_indicator_by_issue)


####################### RANK ISSUES AND INDICATORS BY OUTCOMES #####################################

#rank issues by outcome for Sen/Assm
## note: there is only 1 indicator in democracy and health for v7
worst_outc_issue <- rbindlist(leg_tables_) %>% select(leg_id, leg_name, geolevel, issue_area, performance_z) %>% 
  group_by(leg_id, leg_name, geolevel, issue_area) %>% 
  summarise(avg_perf = mean(performance_z)) %>%
  group_by(leg_id, leg_name, geolevel) %>% 
  mutate(rk = dense_rank(avg_perf)) %>%                        # calc rank, 1 = worst outcome issue
  arrange(leg_id, geolevel, avg_perf)                          # sort from worst outcome to best outcome issue
View(worst_outc_issue)

#rank indicators by outcome for Sen/Assm
worst_outc_indicators <- rbindlist(leg_tables_) %>% 
  select(leg_id, leg_name, geolevel, variable, performance_z) %>% 
  group_by(leg_id, leg_name, geolevel) %>% 
  mutate(rk = dense_rank(performance_z)) %>%                  # calc rank, 1 = worst outcome indicator
  arrange(leg_id, geolevel, performance_z)                    # sort from worst outcome to best outcome indicator
View(worst_outc_indicators)

#rank indicators by outcome in each issue for Sen/Assm
## note: there is only 1 indicator in democracy and health for v7
worst_outc_indicator_by_issue <- rbindlist(leg_tables_) %>% 
  select(leg_id, leg_name, geolevel, issue_area, variable, performance_z) %>%
  group_by(leg_id, geolevel, issue_area) %>% 
  slice_min(performance_z) %>%
  arrange(leg_id, geolevel, performance_z)                    # sort from worst outcome to best outcome indicator by issue
View(worst_outc_indicator_by_issue)

####################### EXPORT ONLY RANKED INDICATOR TABLES #####################################
con2 <- connect_to_db("mosaic")

disp_table_name <- 'indicator_disp_rk'
outc_table_name <- 'indicator_outc_rk'

# Most disparate indicator table
dbBegin(con2)
dbWriteTable(con2,
             Id(schema = rc_schema, table = disp_table_name),
             most_disp_indicators, overwrite = FALSE, row.names = FALSE)

# Worst outcome indicator table
dbWriteTable(con2,
             Id(schema = rc_schema, table = outc_table_name),
             worst_outc_indicators, overwrite = FALSE, row.names = FALSE)

dbCommit(con2)

# Most disparate comments
tcomment1 <- paste0("COMMENT ON TABLE ", rc_schema, ".", disp_table_name, " IS 'Created ", Sys.Date(), ". Findings for MOSAIC Leg Dist Profiles created using W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\MOSAIC\\mosaic_findings.R.';")

ccomment1a <- paste0("COMMENT ON COLUMN ", rc_schema, ".", disp_table_name, ".variable
                       IS 'is the indicator api_name in racecounts.", rc_schema, ".arei_indicator_list_[geolevel]';")

ccomment1b <- paste0("COMMENT ON COLUMN ", rc_schema, ".", disp_table_name, ".rk
                       IS 'Indicator ranked 1 is the most disparate indicator in that leg district';")

dbSendQuery(con2, tcomment1)
dbSendQuery(con2, ccomment1a)
dbSendQuery(con2, ccomment1b)


# Worst outcome comments
tcomment2 <- paste0("COMMENT ON TABLE ", rc_schema, ".", outc_table_name, " IS 'Created ", Sys.Date(), ". Findings for MOSAIC Leg Dist Profiles created using W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\MOSAIC\\mosaic_findings.R.';")

ccomment2a <- paste0("COMMENT ON COLUMN ", rc_schema, ".", outc_table_name, ".variable
                       IS 'is the indicator api_name in racecounts.", rc_schema, ".arei_indicator_list_[geolevel]';")

ccomment2b <- paste0("COMMENT ON COLUMN ", rc_schema, ".", outc_table_name, ".rk
                       IS 'Indicator ranked 1 is the worst outcome indicator in that leg district';")

dbSendQuery(con2, tcomment2)
dbSendQuery(con2, ccomment2a)
dbSendQuery(con2, ccomment2b)


dbDisconnect(con2)

