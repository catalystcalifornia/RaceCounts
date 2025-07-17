#Read in AIR infant-toddler and preschool data, format, and upload to postgres
## install packages if not already installed ------------------------------
packages <- c("readxl","dplyr","stringr", "DBI", "RPostgres", "tidycensus")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
}

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con2 <- connect_to_db("rda_shared_data")
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Sheet_ECE_Access.docx"

#read in data
it_sen_data  <- read_excel("W:/Data/Education/American Institute for Research/2020/air_it_senate_2020.xlsx", skip = 4, n_max = 2190)
it_assm_data  <- read_excel("W:/Data/Education/American Institute for Research/2020/air_it_assembly_2020.xlsx", skip = 4, n_max = 2412)
prek_sen_data <- read_excel("W:/Data/Education/American Institute for Research/2020/air_prek_senate_2020.xlsx", skip = 2, n_max = 2190)
prek_assm_data <- read_excel("W:/Data/Education/American Institute for Research/2020/air_prek_assembly_2020.xlsx", skip = 2, n_max = 2412)


#format names
names(it_sen_data) <- c(
  "name",
  "percent_of_zip_code_allocation",
  "children_0_11mo",
  "children_12_23mo",
  "children_24_35mo",
  "children_it",
  "eligible_children_0_11mo",
  "eligible_children_12_23mo",
  "eligible_children_24_35mo",
  "eligible_children_it",
  "enrolled_children_0_11mo",
  "enrolled_children_12_23mo",
  "enrolled_children_24_35mo",
  "enrolled_children_it",
  "unmet_need_children_0_11mo",
  "unmet_need_children_12_23mo",
  "unmet_need_children_24_35mo",
  "unmet_need_children_it",
  "pct_unmet_need_children_0_11mo",
  "pct_unmet_need_children_12_23mo",
  "pct_unmet_need_children_24_35mo",
  "pct_unmet_need_children_it"
)

names(it_assm_data) <- c(
  "name",
  "percent_of_zip_code_allocation",
  "children_0_11mo",
  "children_12_23mo",
  "children_24_35mo",
  "children_it",
  "eligible_children_0_11mo",
  "eligible_children_12_23mo",
  "eligible_children_24_35mo",
  "eligible_children_it",
  "enrolled_children_0_11mo",
  "enrolled_children_12_23mo",
  "enrolled_children_24_35mo",
  "enrolled_children_it",
  "unmet_need_children_0_11mo",
  "unmet_need_children_12_23mo",
  "unmet_need_children_24_35mo",
  "unmet_need_children_it",
  "pct_unmet_need_children_0_11mo",
  "pct_unmet_need_children_12_23mo",
  "pct_unmet_need_children_24_35mo",
  "pct_unmet_need_children_it"
)

names(prek_sen_data) <- c(
  "name",
  "percent_of_zip_code_allocation",
  "children_3yr",
  "children_4yr",
  "children_5yr",
  "children_prek",
  "eligible_children_3yr",
  "eligible_children_4yr",
  "eligible_children_5yr",
  "eligible_children_prek",
  "enrolled_children_3yr",
  "enrolled_children_4yr",
  "enrolled_children_5yr",
  "enrolled_children_prek",
  "unmet_need_children_3yr",
  "unmet_need_children_4yr",
  "unmet_need_children_5yr",
  "unmet_need_children_prek",
  "pct_unmet_need_children_3yr",
  "pct_unmet_need_children_4yr",
  "pct_unmet_need_children_5yr",
  "pct_unmet_need_children_prek"
)

names(prek_assm_data) <- c(
  "name",
  "percent_of_zip_code_allocation",
  "children_3yr",
  "children_4yr",
  "children_5yr",
  "children_prek",
  "eligible_children_3yr",
  "eligible_children_4yr",
  "eligible_children_5yr",
  "eligible_children_prek",
  "enrolled_children_3yr",
  "enrolled_children_4yr",
  "enrolled_children_5yr",
  "enrolled_children_prek",
  "unmet_need_children_3yr",
  "unmet_need_children_4yr",
  "unmet_need_children_5yr",
  "unmet_need_children_prek",
  "pct_unmet_need_children_3yr",
  "pct_unmet_need_children_4yr",
  "pct_unmet_need_children_5yr",
  "pct_unmet_need_children_prek"
)

it_sen_data  <- it_sen_data %>% filter(str_detect(name, "District"))
it_assm_data  <- it_assm_data %>% filter(str_detect(name, "District"))
prek_sen_data <- prek_sen_data %>% filter(str_detect(name, "District"))
prek_assm_data <- prek_assm_data %>% filter(str_detect(name, "District"))

#set geoids for counties and state with acs geoids
senate_list <- get_acs(
  geography = "state legislative district (upper chamber)",
  state = "CA",
  variables = "B01001_001",
  year = 2020
)

assembly_list <- get_acs(
  geography = "state legislative district (lower chamber)",
  state = "CA",
  variables = "B01001_001",
  year = 2020
)

#select and reformat just name and geoid columns
senate_list <- senate_list %>% select(NAME, GEOID) %>% dplyr::rename(name = NAME, geoid = GEOID)
assembly_list <- assembly_list %>% select(NAME, GEOID) %>% dplyr::rename(name = NAME, geoid = GEOID)

# fix names
senate_list$name <- gsub("State ", "",senate_list$name)
senate_list$name <- gsub(" \\(2018\\), California", "", senate_list$name)
assembly_list$name <- gsub(" \\(2018\\), California", "", assembly_list$name)

#join
it_sen_data <- merge(x = it_sen_data, y = senate_list, by = "name", all.x = TRUE)
it_assm_data <- merge(x = it_assm_data, y = assembly_list, by = "name", all.x = TRUE)
prek_sen_data <- merge(x = prek_sen_data, y = senate_list, by = "name", all.x = TRUE)
prek_assm_data <- merge(x = prek_assm_data, y = assembly_list, by = "name", all.x = TRUE)

#replace
it_sen_data$geoid[is.na(it_sen_data$geoid)] <- it_sen_data$name
it_assm_data$geoid[is.na(it_assm_data$geoid)] <- it_assm_data$name
prek_sen_data$geoid[is.na(prek_sen_data$geoid)] <- prek_sen_data$name 
prek_assm_data$geoid[is.na(prek_assm_data$geoid)] <- prek_assm_data$name 

#clean up Zip allocation column
it_sen_data$percent_of_zip_code_allocation[it_sen_data$percent_of_zip_code_allocation == "Percent of Zip Code Allocation"] <- NA 
prek_sen_data$percent_of_zip_code_allocation[prek_sen_data$percent_of_zip_code_allocation == "Percent of Zip Code Allocation"] <- NA
it_assm_data$percent_of_zip_code_allocation[it_assm_data$percent_of_zip_code_allocation == "Percent of Zip Code Allocation"] <- NA 
prek_assm_data$percent_of_zip_code_allocation[prek_assm_data$percent_of_zip_code_allocation == "Percent of Zip Code Allocation"] <- NA

#remove asterixes
it_sen_data$unmet_need_children_0_11mo <- gsub('[*]', NA, it_sen_data$unmet_need_children_0_11mo)
it_sen_data$unmet_need_children_12_23mo <- gsub('[*]', NA, it_sen_data$unmet_need_children_12_23mo)
it_sen_data$unmet_need_children_24_35mo <- gsub('[*]', NA, it_sen_data$unmet_need_children_24_35mo)
it_sen_data$unmet_need_children_it <- gsub('[*]', NA, it_sen_data$unmet_need_children_it)

it_sen_data$pct_unmet_need_children_0_11mo <- gsub('[*]', NA, it_sen_data$pct_unmet_need_children_0_11mo)
it_sen_data$pct_unmet_need_children_12_23mo <- gsub('[*]', NA, it_sen_data$pct_unmet_need_children_12_23mo)
it_sen_data$pct_unmet_need_children_24_35mo <- gsub('[*]', NA, it_sen_data$pct_unmet_need_children_24_35mo)
it_sen_data$pct_unmet_need_children_it <- gsub('[*]', NA, it_sen_data$pct_unmet_need_children_it)

prek_sen_data$unmet_need_children_3yr <- gsub('[*]', NA, prek_sen_data$unmet_need_children_3yr)
prek_sen_data$unmet_need_children_4yr <- gsub('[*]', NA, prek_sen_data$unmet_need_children_4yr)
prek_sen_data$unmet_need_children_5yr <- gsub('[*]', NA, prek_sen_data$unmet_need_children_5yr)
prek_sen_data$unmet_need_children_prek <- gsub('[*]', NA, prek_sen_data$unmet_need_children_prek)

prek_sen_data$pct_unmet_need_children_3yr <- gsub('[*]', NA, prek_sen_data$pct_unmet_need_children_3yr)
prek_sen_data$pct_unmet_need_children_4yr <- gsub('[*]', NA, prek_sen_data$pct_unmet_need_children_4yr)
prek_sen_data$pct_unmet_need_children_5yr <- gsub('[*]', NA, prek_sen_data$pct_unmet_need_children_5yr)
prek_sen_data$pct_unmet_need_children_prek <- gsub('[*]', NA, prek_sen_data$pct_unmet_need_children_prek)

it_assm_data$unmet_need_children_0_11mo <- gsub('[*]', NA, it_assm_data$unmet_need_children_0_11mo)
it_assm_data$unmet_need_children_12_23mo <- gsub('[*]', NA, it_assm_data$unmet_need_children_12_23mo)
it_assm_data$unmet_need_children_24_35mo <- gsub('[*]', NA, it_assm_data$unmet_need_children_24_35mo)
it_assm_data$unmet_need_children_it <- gsub('[*]', NA, it_assm_data$unmet_need_children_it)

it_assm_data$pct_unmet_need_children_0_11mo <- gsub('[*]', NA, it_assm_data$pct_unmet_need_children_0_11mo)
it_assm_data$pct_unmet_need_children_12_23mo <- gsub('[*]', NA, it_assm_data$pct_unmet_need_children_12_23mo)
it_assm_data$pct_unmet_need_children_24_35mo <- gsub('[*]', NA, it_assm_data$pct_unmet_need_children_24_35mo)
it_assm_data$pct_unmet_need_children_it <- gsub('[*]', NA, it_assm_data$pct_unmet_need_children_it)

prek_assm_data$unmet_need_children_3yr <- gsub('[*]', NA, prek_assm_data$unmet_need_children_3yr)
prek_assm_data$unmet_need_children_4yr <- gsub('[*]', NA, prek_assm_data$unmet_need_children_4yr)
prek_assm_data$unmet_need_children_5yr <- gsub('[*]', NA, prek_assm_data$unmet_need_children_5yr)
prek_assm_data$unmet_need_children_prek <- gsub('[*]', NA, prek_assm_data$unmet_need_children_prek)

prek_assm_data$pct_unmet_need_children_3yr <- gsub('[*]', NA, prek_assm_data$pct_unmet_need_children_3yr)
prek_assm_data$pct_unmet_need_children_4yr <- gsub('[*]', NA, prek_assm_data$pct_unmet_need_children_4yr)
prek_assm_data$pct_unmet_need_children_5yr <- gsub('[*]', NA, prek_assm_data$pct_unmet_need_children_5yr)
prek_assm_data$pct_unmet_need_children_prek <- gsub('[*]', NA, prek_assm_data$pct_unmet_need_children_prek)

#set column types to numeric
it_sen_data$percent_of_zip_code_allocation <- as.numeric(as.character(it_sen_data$percent_of_zip_code_allocation))

it_sen_data$unmet_need_children_0_11mo <- as.numeric(as.character(it_sen_data$unmet_need_children_0_11mo))
it_sen_data$unmet_need_children_12_23mo <- as.numeric(as.character(it_sen_data$unmet_need_children_12_23mo))
it_sen_data$unmet_need_children_24_35mo <- as.numeric(as.character(it_sen_data$unmet_need_children_24_35mo))
it_sen_data$unmet_need_children_it <- as.numeric(as.character(it_sen_data$unmet_need_children_it))

it_sen_data$pct_unmet_need_children_0_11mo <- as.numeric(as.character(it_sen_data$pct_unmet_need_children_0_11mo))
it_sen_data$pct_unmet_need_children_12_23mo <- as.numeric(as.character(it_sen_data$pct_unmet_need_children_12_23mo))
it_sen_data$pct_unmet_need_children_24_35mo <- as.numeric(as.character(it_sen_data$pct_unmet_need_children_24_35mo))
it_sen_data$pct_unmet_need_children_it <- as.numeric(as.character(it_sen_data$pct_unmet_need_children_it))

prek_sen_data$percent_of_zip_code_allocation <- as.numeric(as.character(prek_sen_data$percent_of_zip_code_allocation))

prek_sen_data$unmet_need_children_3yr <- as.numeric(as.character(prek_sen_data$unmet_need_children_3yr))
prek_sen_data$unmet_need_children_4yr <- as.numeric(as.character(prek_sen_data$unmet_need_children_4yr))
prek_sen_data$unmet_need_children_5yr <- as.numeric(as.character(prek_sen_data$unmet_need_children_5yr))
prek_sen_data$unmet_need_children_prek <- as.numeric(as.character(prek_sen_data$unmet_need_children_prek))

prek_sen_data$pct_unmet_need_children_3yr <- as.numeric(as.character(prek_sen_data$pct_unmet_need_children_3yr))
prek_sen_data$pct_unmet_need_children_4yr <- as.numeric(as.character(prek_sen_data$pct_unmet_need_children_4yr))
prek_sen_data$pct_unmet_need_children_5yr <- as.numeric(as.character(prek_sen_data$pct_unmet_need_children_5yr))
prek_sen_data$pct_unmet_need_children_prek <- as.numeric(as.character(prek_sen_data$pct_unmet_need_children_prek))

it_assm_data$percent_of_zip_code_allocation <- as.numeric(as.character(it_assm_data$percent_of_zip_code_allocation))

it_assm_data$unmet_need_children_0_11mo <- as.numeric(as.character(it_assm_data$unmet_need_children_0_11mo))
it_assm_data$unmet_need_children_12_23mo <- as.numeric(as.character(it_assm_data$unmet_need_children_12_23mo))
it_assm_data$unmet_need_children_24_35mo <- as.numeric(as.character(it_assm_data$unmet_need_children_24_35mo))
it_assm_data$unmet_need_children_it <- as.numeric(as.character(it_assm_data$unmet_need_children_it))

it_assm_data$pct_unmet_need_children_0_11mo <- as.numeric(as.character(it_assm_data$pct_unmet_need_children_0_11mo))
it_assm_data$pct_unmet_need_children_12_23mo <- as.numeric(as.character(it_assm_data$pct_unmet_need_children_12_23mo))
it_assm_data$pct_unmet_need_children_24_35mo <- as.numeric(as.character(it_assm_data$pct_unmet_need_children_24_35mo))
it_assm_data$pct_unmet_need_children_it <- as.numeric(as.character(it_assm_data$pct_unmet_need_children_it))

prek_assm_data$percent_of_zip_code_allocation <- as.numeric(as.character(prek_assm_data$percent_of_zip_code_allocation))

prek_assm_data$unmet_need_children_3yr <- as.numeric(as.character(prek_assm_data$unmet_need_children_3yr))
prek_assm_data$unmet_need_children_4yr <- as.numeric(as.character(prek_assm_data$unmet_need_children_4yr))
prek_assm_data$unmet_need_children_5yr <- as.numeric(as.character(prek_assm_data$unmet_need_children_5yr))
prek_assm_data$unmet_need_children_prek <- as.numeric(as.character(prek_assm_data$unmet_need_children_prek))

prek_assm_data$pct_unmet_need_children_3yr <- as.numeric(as.character(prek_assm_data$pct_unmet_need_children_3yr))
prek_assm_data$pct_unmet_need_children_4yr <- as.numeric(as.character(prek_assm_data$pct_unmet_need_children_4yr))
prek_assm_data$pct_unmet_need_children_5yr <- as.numeric(as.character(prek_assm_data$pct_unmet_need_children_5yr))
prek_assm_data$pct_unmet_need_children_prek <- as.numeric(as.character(prek_assm_data$pct_unmet_need_children_prek))

#reorder
it_sen_data <- it_sen_data %>% select(geoid, name, everything()) %>% mutate(geolevel="sldu")
prek_sen_data <- prek_sen_data %>% select(geoid, name, everything())%>% mutate(geolevel="sldu")

it_assm_data <- it_assm_data %>% select(geoid, name, everything())%>% mutate(geolevel="sldl")
prek_assm_data <- prek_assm_data %>% select(geoid, name, everything())%>% mutate(geolevel="sldl")

#join both into leg tables
it_data <- rbind(it_sen_data, it_assm_data)
prek_data <- rbind(prek_sen_data, prek_assm_data)

#upload to postgres
table_schema <- "education"
table_name <- "air_leg_unmet_it_2020"
indicator <- "Subsidized child care eligibility and enrollment data"
source <- "AIR ELNAT: https://elneedsassessment.org."
dbWriteTable(con2,
             Id(schema = table_schema, table = table_name),
             it_data,  overwrite = FALSE, row.names = FALSE)

#comment on table and columns
column_names <- colnames(it_data) # get column names
column_comments <- c(
  'Geoid',
  'Name',
  'Percent of ZIP Code Allocation',
  'Children 0 to 11 months',
  'Children 12 to 23 months',
  'Children 24 to 35 months',
  'Infant-toddler Children',
  'Eligible Children 0 to 11 months',
  'Eligible Children 12 to 23 months',
  'Eligible Children 24 to 35 months',
  'Eligible Infant-toddler Children',
  'Enrolled Children 0 to 11 months',
  'Enrolled Children 12 to 23 months',
  'Enrolled Children 24 to 35 months',
  'Enrolled Infant-toddler Children',
  'Unmet Need Children 0 to 11 months',
  'Unmet Need Children 12 to 23 months',
  'Unmet Need Children 24 to 35 months',
  'Unmet Need Infant-toddler Children',
  'Percent Unmet Need Children 0 to 11 months',
  'Percent Unmet Need Children 12 to 23 months',
  'Percent Unmet Need Children 24 to 35 months',
  'Percent Unmet Need Infant-toddler Children')

add_table_comments(con2, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

table_name <- "air_leg_unmet_prek_2020"
dbWriteTable(con2,
             Id(schema = table_schema, table = table_name),
             prek_data,  overwrite = FALSE, row.names = FALSE)
#comment on table and columns
column_names <- colnames(prek_data) # get column names

column_comments  <- c( 
'Geoid',
'Name',
'Percent of ZIP Code Allocation',
'Children three years old',
'Children four years old',
'Children five years old',
'Preschool Children',
'Eligible Children three years old',
'Eligible Children four years old',
'Eligible Children five years old',
'Eligible Preschool Children',
'Enrolled Children three years old',
'Enrolled Children four years old',
'Enrolled Children five years old',
'Enrolled Preschool Children',
'Unmet Need Children three years old',
'Unmet Need Children four years old',
'Unmet Need Children five years old',
'Unmet Need Preschool Children',
'Percent Unmet Need Children three years old',
'Percent Unmet Need Children four years old',
'Percent Unmet Need Children five years old',
'Percent Unmet Need Preschool Children'
)

add_table_comments(con2, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)


dbDisconnect(con2)
