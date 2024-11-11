#### Better or worse current RC version compared to previous version of RC? ####
### Script produces findings about how much an indicator's overall outcome has changed since prev RC by calculating % change btwn curr and prev total_rate values.
### Script produces findings about how much an indicator's disparity has changed by calculating the % change btwn curr and prev Index of Disparity values.


#install packages if not already installed
packages <- c("tidyverse","RPostgreSQL","sf","here","usethis")  

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
source(here("Functions", "RC_Index_Functions.R"))

# remove exponentiation
options(scipen = 100) 

# update each yr
curr_rc_yr <- '2024'
curr_rc_schema <- 'v6'
prev_rc_yr <- '2023'
prev_rc_schema <- 'v5'

####################### GET STATE DATA #####################################
## CURR STATE DATA ##
# pull in list of all tables in current racecounts schema
table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_rc_schema, "' AND (table_name LIKE '%state_", curr_rc_yr, "');")
rc_list <- dbGetQuery(con, table_list) %>% rename('table' = 'table_name')
rc_list <- rc_list[order(rc_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step
rc_list # check indicator table list is correct and complete

# import all tables on rc_list
state_tables <- lapply(setNames(paste0("select * from ", curr_rc_schema, ".", rc_list), rc_list), DBI::dbGetQuery, conn = con)

# keep only needed columns
state_tables_ <- lapply(state_tables, function(x) x %>% select(state_name, index_of_disparity, total_rate, asbest))

# create columns with indicator and issue names
state_tables_ <- map2(state_tables_, names(state_tables_), ~ mutate(.x, indicator = .y))                 # create indicator col
state_tables_long <- state_tables_ %>% reduce(full_join)                                                 # convert list to "long" df
state_tables_long$issue <- substr(state_tables_long$indicator,6,9)                                       # create issue col
state_tables_long$indicator <- substr(state_tables_long$indicator,11,nchar(state_tables_long$indicator)) # step 1: clean indicator col
state_tables_long$indicator <- gsub(paste0("_state_",curr_rc_yr), "",state_tables_long$indicator)        # step 2: clean indicator col
state_tables_long <- state_tables_long %>% rename(curr_id = index_of_disparity, curr_total_rate = total_rate) # rename cols

## PREV STATE DATA ##
# pull in list of all tables in previous racecounts schema
prev_table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", prev_rc_schema, "' AND (table_name LIKE '%state_", prev_rc_yr, "');")
prev_rc_list <- dbGetQuery(con, prev_table_list) %>% rename('table' = 'table_name')
prev_rc_list <- prev_rc_list[order(prev_rc_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step
prev_rc_list # check indicator table list is correct and complete

# import all tables on prev_rc_list
prev_state_tables <- lapply(setNames(paste0("select * from ", prev_rc_schema, ".", prev_rc_list), prev_rc_list), DBI::dbGetQuery, conn = con)

# keep only needed columns
prev_state_tables_ <- lapply(prev_state_tables, function(x) x %>% select(state_name, index_of_disparity, total_rate))

# create columns with indicator and issue names
prev_state_tables_ <- map2(prev_state_tables_, names(prev_state_tables_), ~ mutate(.x, indicator = .y))                 # create indicator col
prev_state_tables_long <- prev_state_tables_ %>% reduce(full_join)                                                      # convert list to "long" df
prev_state_tables_long$indicator <- substr(prev_state_tables_long$indicator,11,nchar(prev_state_tables_long$indicator)) # step 1: clean indicator col
prev_state_tables_long$indicator <- gsub(paste0("_state_",prev_rc_yr), "",prev_state_tables_long$indicator)             # step 2: clean indicator col
prev_state_tables_long <- prev_state_tables_long %>% rename(prev_id = index_of_disparity, prev_total_rate = total_rate) # rename cols

# join CURR and PREV state data: Use of inner_join() keeps only indicators that appear in both RC years
combined_data <- state_tables_long %>% inner_join(prev_state_tables_long %>% select(prev_id, prev_total_rate, indicator), by = "indicator")
combined_data <- combined_data %>% relocate(indicator, .after = state_name) %>% relocate(issue, .after = state_name) %>% relocate(asbest, .after = indicator)  # reposition columns


# CALCULATE CHANGES IN OUTCOMES & DISPARITY -------------------------------
combined_data$rate_diff <- combined_data$curr_total_rate - combined_data$prev_total_rate
combined_data$rate_pct_chng <- combined_data$rate_diff / combined_data$prev_total_rate * 100

combined_data$id_diff <- combined_data$curr_id - combined_data$prev_id
combined_data$id_pct_chng <- combined_data$id_diff / combined_data$prev_id * 100

# Calculate overall and mean difference in disparity
id_change_sum <- sum(combined_data$id_pct_chng)
id_change_mean <- mean(combined_data$id_pct_chng)

# Calculate difference in disparity by issue area
issue_disp_change <- combined_data %>% group_by(issue) %>% summarize(
  curr_id_sum=sum(curr_id),
  prev_id_sum=sum(prev_id),
  id_diff=sum(curr_id) - sum(prev_id),
  id_pct_chng=(sum(curr_id) - sum(prev_id))/ sum(prev_id) * 100
)

# calculate overall and mean difference in outcomes
combined_data <- combined_data %>% mutate(outcome_better = 
                                            ifelse(asbest == "min" & rate_pct_chng < 0, "better",
                                                   ifelse(asbest == "min" & rate_pct_chng > 0, "worse",
                                                          ifelse(asbest == "max" & rate_pct_chng > 0, "better",
                                                                 ifelse(asbest == "max" & rate_pct_chng < 0, "worse", "no change"
                                                                 )))))

combined_data$rate_pct_chng_abs <- abs(combined_data$rate_pct_chng)  # add absolute value of rate_pct_chng since min/max asbest varies by indicator

rate_change_sum <- combined_data %>% group_by(outcome_better) %>% summarize(sum_rate_pct_chng = sum(rate_pct_chng), n=n())

issue_rate_change <- combined_data %>% group_by(outcome_better, issue) %>% summarize(sum_rate_pct_chng = sum(rate_pct_chng), n=n())
