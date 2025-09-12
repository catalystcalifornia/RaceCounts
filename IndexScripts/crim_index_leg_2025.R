#### Crime and Justice (z-score) for RC v7 ####

#install packages if not already installed
packages <- c("tidyverse","RPostgres","sf","usethis")  

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
source <- "Prison Policy Initiative (2020) with Census 2020 Tables P06 and P09, California Department of Justice Open Justice Data (CADOJ) (2016-2024 Use of Force) and (2022-2023 RIPA stops) with American Community Survey (ACS) 5-Year Estimates, Table DP05 (2019-2023)"
ind_threshold <- 2  # geos with < threshold # of indicator values are excluded from index. depends on the number of indicators in the issue area

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Composite Index\\QA_Sheet_Leg_Indexes.docx"

issue <- 'crime_and_justice'

####################### ADD DATA #####################################
# you MUST update this section if we add or remove any indicators in an issue #
c_1 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_crim_incarceration_leg_", rc_yr))
c_2 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_crim_use_of_force_leg_", rc_yr))
c_3 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_crim_officer_initiated_stops_leg_", rc_yr))

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname1 <- 'incarceration'
varname2 <- 'force'
varname3 <- 'stops'

# Clean data --------
## use function to select cols we want, cap z-scores, and rename z-score cols
### this step drops geo_level, so need to add back in later...

### c1 
c_1 <- clean_data_z(c_1, varname1)

### c2
c_2 <- clean_data_z(c_2, varname2)

### c3
c_3 <- clean_data_z(c_3, varname3)


# Join Data Together ------------------------------------------------------
index_list <- list(c_1, c_2, c_3)

c_index <- index_list %>% reduce(full_join, by=c('leg_id', 'leg_name', 'geolevel'))

colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names


# ASSEMBLY CALCS ------------------------------------------------------
assm_index <- filter(c_index, geolevel == 'sldl')

# calculate z-scores.
assm_index <- calculate_z(assm_index, ind_threshold)

# SENATE CALCS ------------------------------------------------------
sen_index <- filter(c_index, geolevel == 'sldu')

# calculate z-scores.
sen_index <- calculate_z(sen_index, ind_threshold)


# JOIN LEG INDEX TOGETHER ------------------------------------------------------
c_index <- rbind(assm_index, sen_index)

# rename columns
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("_rank"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("performance_z"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("disparity_z"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("quartile"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("quadrant"))

# select/reorder final columns for index table
index_table <- c_index %>% select(leg_id, leg_name, geolevel, ends_with("_rank"), ends_with("quadrant"), disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("performance_z"), ends_with("disparity_z_quartile"), ends_with("performance_z_quartile"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 
index_table_name <- paste0("arei_crim_index_leg_", rc_yr)
index <- paste0("QA doc: ", qa_filepath, ". Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is")

index_to_postgres(index_table, rc_schema)
dbDisconnect(con)
	
