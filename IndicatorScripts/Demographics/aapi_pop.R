## Disaggregated AAPI Pop for RC 2025 v7 ###
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
con2 <- connect_to_db("rda_shared_data")
con <- connect_to_db("mosaic")


############## UPDATE VARIABLES ##############
curr_yr = 2023      # you MUST UPDATE each year
rc_yr = '2025'      # you MUST UPDATE each year
rc_schema <- "v7"   # you MUST UPDATE each year
schema = 'demographics'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\QA_Sheet_AAPI_Pop.docx"

# Create Table B02019 in Postgres db: Only run this section if the table has not been created yet
# source("W:\\RDA Team\\R\\Github\\RDA Functions\\LF\\RDA-Functions\\acs_rda_shared_tables.R")
# B02019 <- update_acs(curr_yr, 'B02019', "W://Project//RACE COUNTS//2025_v7//RC_Github//LF//RaceCounts//IndicatorScripts//Demographics/aapi_pop.R")

# Get AA and NHPI Pop variables --------
acs_var <- load_variables(curr_yr, 'acs5')

clean_vars <- function(table_name, region_name){
  x <- acs_var %>% filter(grepl({{table_name}}, name))                           # get variables
  x$name <- tolower(paste0(x$name, "e"))                                     # format variable names to match postgres table later
  x$label <- gsub("Estimate!!Total Groups Tallied:!!", "", x$label)          # clean up labels
  x <- x %>% separate(label, c("region", "subgroup"), "!!")
  x$name <- gsub("e", "", x$name)
  x <- x %>% rename(variable = name)
  x$region <- gsub(":", "", x$region)
  x$region <- ifelse(x$region == 'Estimate', region_name, x$region)
  x$subgroup <- ifelse(x$subgroup == 'Total Groups Tallied:', 'All', x$subgroup)
  x$subgroup <- ifelse(is.na(x$subgroup), x$region, x$subgroup)
  x <- x %>% select(-c(geography))
print(x)

return(x)
}

# Vars - NHPI Alone or in any combination by selected groups: https://api.census.gov/data/2023/acs/acs5/groups/B02019.html --------
b02019_var <- clean_vars('B02019', 'Hawaii and Other Pacific Islands')

# Vars - Asian Alone or in any combination by selected groups: https://api.census.gov/data/2023/acs/acs5/groups/B02018.html --------
b02018_var <- clean_vars('B02018', 'Asia')



# Get AA and NHPI Pop ----------------------------------------------------------
B02019 <- dbGetQuery(con2, paste0("select * from ",schema,".acs_5yr_b02019_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
B02019$name <- str_remove(B02019$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
B02019$name <- gsub("; California", "", B02019$name)

B02018 <- dbGetQuery(con2, paste0("select * from ",schema,".acs_5yr_b02018_multigeo_",curr_yr," WHERE geolevel IN ('place', 'county', 'state', 'sldu', 'sldl')")) # import rda_shared_data table
B02018$name <- str_remove(B02018$name,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
B02018$name <- gsub("; California", "", B02018$name)

## Calc subgroup % of AA or NHPI pop
calc_pct <- function(pop_table, tot_pop_col){
  # can work on this bit later to calc pct moe's
  # est <- colnames(B02018)                         # get all colnames
  # ends_with_m <- str_detect(est, "m$")            # get moe colnames
  # est <- est[!ends_with_m]                        # drop moe colnames
  # 
  # moe <- est
  # moe_ <- gsub("e", "m", moe)                     # get moe colnames
  # moe_ <- grep(glob2rx("b*"), moe_, value = TRUE) # select only moe colnames
  # moe_ <- c("geoid", moe_)                        # add geoid col back
  
  pcts <- pop_table %>%
    mutate(across(ends_with("e") & is.numeric, ~ .x / tot_pop_col * 100, .names = "{.col}_pct"))
  colnames(pcts) <- gsub("e_pct", "_pct", colnames(pcts))
  
return(pcts)
}

B02018_pop <- calc_pct(B02018, B02018$b02018_001e)
B02019_pop <- calc_pct(B02019, B02019$b02019_001e)


############### SEND AA / NHPI POP & VARIABLES TO POSTGRES ##############

### info for postgres tables automatically updates ###
send_to_postgres <- function(acs_table, var_table, df, table_schema){

  table_name <- ifelse(acs_table == 'b02018', paste0("aa_pop_", acs_table), paste0("pacisl_pop_", acs_table))    
  group <- ifelse(acs_table == 'b02018', "Asian", "NHPI")
  indicator <-  paste0("Disaggregated ", group, " population alone or in combination with another race by City, County, State Senate, State Assembly, and State population")       
  source <- paste0("ACS ", curr_yr - 4, "-", curr_yr, ", Table ", toupper(acs_table)) 
  column_names <- colnames(df)
  
  var_table <- as.data.frame(var_table)
  var_table$comment <- paste0(var_table$region, ": ", var_table$subgroup)
  var_table$variable <- paste0(var_table$variable, "e")
  column_names <- var_table$variable
  column_comments <- var_table$comment
  
  pct_column_names <- gsub("e", "_pct", column_names)
  pct_comments <- paste0("Percent ", column_comments)
  
  column_names <- c(column_names, pct_column_names)
  column_comments <- c(column_comments, pct_comments)
  
  # send table to postgres
  dbWriteTable(con, 
               Id(schema = table_schema, table = table_name), 
               df, overwrite = FALSE)
  
  # comment on table and columns
  add_table_comments(con, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)

  #dbDisconnect(con) 
}

send_to_postgres('b02018', b02018_var, B02018_pop, 'v7')
send_to_postgres('b02019', b02019_var, B02019_pop, 'v7')

