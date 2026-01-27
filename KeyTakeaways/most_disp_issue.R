#### Most disparate issue area and indicator ####
## This script identifies the most disparate issue, most disparate indicators, and most disparate indicators within each issue.

#install packages if not already installed
packages <- c("tidyverse","RPostgreSQL","sf","data.table","usethis")  

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

# Add indicators and arei_multigeo_list ------------------------------------------------------
####################### ADD state DATA #####################################
rc_list_query <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", rc_schema, "' AND table_name LIKE '%state_", rc_yr, "';")
rc_list <- dbGetQuery(con, rc_list_query)
table_list <- rc_list[order(rc_list$table_name), ] # alphabetize list of state tables, changes df to list the needed format for next step

state_tables <- lapply(setNames(paste0("select * from ", rc_schema, ".", table_list), table_list), DBI::dbGetQuery, conn = con)
length(state_tables) # check that number of indicator tables is correct

state_tables_ <- map2(state_tables, names(state_tables), ~ mutate(.x, table_name = .y)) # create column with table_name used to create issue and indicator cols
state_tables_ <- lapply(state_tables_, function(x) x %>% select(state_id, state_name, index_of_disparity, table_name) %>% # create issue area name column
         mutate(issue_area = substring(table_name, 6,9), variable = substring(table_name, 11,nchar(table_name))) %>%      # create indicator name column 
         mutate(variable = gsub(paste0("_state_", rc_yr), "", variable)))                                                 # clean indicator name column


#most disparate issue area
most_disp_issue <- rbindlist(state_tables_) %>% select(issue_area, index_of_disparity) %>% group_by(issue_area) %>% summarise(avg_id = mean(index_of_disparity)) %>%
                      arrange(, desc(avg_id))  # sort from most disparate to least disparate issue
View(most_disp_issue)

#most disparate indicators
most_disp_indicators <- rbindlist(state_tables_) %>% select(variable, index_of_disparity) %>% arrange(, desc(index_of_disparity))  # sort from most disparate to least disparate indicator
View(most_disp_indicators)

#most disparate indicator by issue
most_disp_indicator_by_issue <- rbindlist(state_tables_) %>% select(issue_area, variable, index_of_disparity) %>% group_by(issue_area) %>% slice_max(index_of_disparity) %>%
  arrange(, desc(index_of_disparity))  # sort from most disparate to least disparate indicator
View(most_disp_indicator_by_issue)

dbDisconnect(con)

