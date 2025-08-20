#### Education Index (z-score) for RC v7 ####

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

# update each yr
rc_yr <- '2025'
rc_schema <- 'v7'
source <- "CALIFORNIA DEPARTMENT OF EDUCATION (2023-24) for Suspensions, 
Chronic Absenteeism, High School Graduation, 3rd Grade English and Math, 
and for Diversity of Teachers, 
CALIFORNIA CHILD CARE RESOURCE & REFERRAL NETWORK (2020-2021), 
AMERICAN INSTITUTES FOR RESEARCH EARLY LEARNING NEEDS ASSESSMENT TOOL (2020)"
ind_threshold <- 4  # geos with < threshold # of indicator values are excluded from index. depends on the number of indicators in the issue area

qa_filepath <- 'W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Educ_Index.docx'

issue <- 'education'

# Add indicators and arei_county_region_urban_type ------------------------------------------------------
####################### ADD COUNTY DATA #####################################
# you MUST update this section if we add or remove any indicators in an issue #

c_1 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_chronic_absenteeism_county_", rc_yr))
c_2 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_hs_grad_county_", rc_yr))
c_3 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_gr3_ela_scores_county_", rc_yr))
c_4 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_gr3_math_scores_county_", rc_yr))
c_5 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_suspension_county_", rc_yr))
c_6 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_ece_access_county_", rc_yr))
c_7 <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".arei_educ_staff_diversity_county_", rc_yr))

## define variable names for clean_data_z function. you MUST UPDATE for each issue area.
varname1 <- 'abst'
varname2 <- 'grad'
varname3 <- 'ela'
varname4 <- 'math'
varname5 <- 'susp'
varname6 <- 'ece'
varname7 <- 'diver'


region_urban_type <- dbGetQuery(con, paste0("select county_id, region, urban_type from ", rc_schema, ".arei_county_region_urban_type")) # get region, urban_type


# Clean data --------
# use function to select cols we want, cap z-scores, and rename z-score cols

### c1 
c_1 <- clean_data_z(c_1, varname1)

### c2
c_2 <- clean_data_z(c_2, varname2)

### c3
c_3 <- clean_data_z(c_3, varname3)

## c4
c_4 <- clean_data_z(c_4, varname4)

## c5
c_5 <- clean_data_z(c_5, varname5)

## c6 
c_6 <- clean_data_z(c_6, varname6)

### c7
c_7 <- clean_data_z(c_7, varname7)


# Join Data Together ------------------------------------------------------
c_index <- full_join(c_1, c_2) 
c_index <- full_join(c_index, c_3)
c_index <- full_join(c_index, c_4)
c_index <- full_join(c_index, c_5)
c_index <- full_join(c_index, c_6)
c_index <- full_join(c_index, c_7)

colnames(c_index) <- gsub("performance", "perf", names(c_index))  # shorten col names
colnames(c_index) <- gsub("disparity", "disp", names(c_index))    # shorten col names

# calculate z-scores
c_index <- calculate_z(c_index, ind_threshold)

# merge region and urban type from current arei_county_region_urban_type
c_index <- left_join(c_index, region_urban_type)

# rename columns
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("_rank"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("performance_z"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("disparity_z"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("quartile"))
c_index <- c_index %>% rename_with(~ paste0(issue, "_", .x), ends_with("quadrant"))

# select/reorder final columns for index table
index_table <- c_index %>% select(county_id, county_name, region, urban_type, ends_with("_rank"), ends_with("quadrant"), disp_avg, perf_avg, disp_values_count, perf_values_count, ends_with("_disparity_z"), ends_with("performance_z"), ends_with("disparity_z_quartile"), ends_with("performance_z_quartile"), everything())
index_table <- index_table[order(index_table[[5]]), ]  # order by disparity rank
View(index_table)

# Send table to postgres 
index_table_name <- paste0("arei_educ_index_", rc_yr)
index <- paste0("QA doc: ", qa_filepath, ". Includes all issue indicators. Issue area z-scores are the average z-scores for performance and disparity across all issue indicators. This data is")

index_to_postgres(index_table, rc_schema)
dbDisconnect(con)




