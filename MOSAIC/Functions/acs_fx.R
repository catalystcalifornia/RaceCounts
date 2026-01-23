### MOSAIC-related functions for ACS data ###
#install packages if not already installed
packages <- c("readr", "tidyr", "dplyr", "DBI", "RPostgres", "tidycensus", "tidyverse", "stringr", "usethis", "httr", "rlang")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 


#### Automate writing API calls and pull ACS SPT detailed Asian & NHPI race tables for city/county/state ####
get_detailed_race <- function(table, race, year = 2021) {
  # race = for MOSAIC, either 'asian' or 'nhpi', case-insensitive
  # table = ACS table name, eg: "B25003" or "S2701", case-insensitive
  # year = ACS data year, defaults to 2021 if none specified
  
  race_code <- case_when(
    str_detect(race, regex('asian', ignore_case = TRUE)) ~ '-04',
    str_detect(race, regex('nhpi', ignore_case = TRUE)) ~ '-05',
    .default = ''
    )

  # return error if race is not already included in available race_code list
    if (!(race_code %in% c('-04','-05'))) {
       return(print("This function doesn't pull data for the race you selected. Please select either Asian or NHPI or talk to Leila about adding an additional race."))
   }

  table_name <- toupper(table)
  
  city_api_call <- sprintf(
    "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&POPGROUP=pseudo(%s)&ucgid=pseudo(0400000US06$1600000)",
    year, table_name, race_code)
  
  county_api_call <- sprintf(
    "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&POPGROUP=pseudo(%s)&ucgid=pseudo(0400000US06$0500000)",
    year, table_name, race_code)
  
  state_api_call <- sprintf(
    "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&POPGROUP=pseudo(%s)&ucgid=0400000US06",
    year, table_name, race_code)

api_call_list <- c(city_api_call, county_api_call, state_api_call)  
  
# Loop through API calls
parsed_list <- list()    # create empty list for loop results

for (i in api_call_list) {
  
  response <- GET(i)
  status_code(response)
  
  # Check if the request was successful
  if (status_code(response) == 200) {
    print("API request successful.")
  } else {
    stop("API request failed with status code: ", status_code(response))
  }
  
  raw_content <- content(response, "text")   # returns the response body as text
  parsed_data <- fromJSON(raw_content)
  
  parsed_list[[i]] <- parsed_data               # put loop results into a list
  
}

# clean up data
parsed_data <- do.call(rbind, parsed_list) %>% as.data.frame()
clean_data <- parsed_data
colnames(clean_data) <- clean_data[1, ]  # replace col names with first row values
clean_data <- clean_data[, !duplicated(names(clean_data))] # drop any duplicate cols, eg: POPGROUP
clean_data <- clean_data %>%
  mutate(across(contains(table_name), as.numeric))         # assign numeric cols to numeric type
clean_data <- clean_data %>%
  filter(!if_all(where(is.numeric), is.na))                # drop rows where all numeric values are NA, this also removes the extra 'header' rows
clean_data$geoid <- str_replace(clean_data$GEO_ID, ".*US", "")  # clean geoids
clean_data$geolevel <- case_when(                                # add geolevel bc it's a multigeo table
  nchar(clean_data$geoid) == 2 ~ 'state',
  nchar(clean_data$geoid) == 5 ~ 'county',
  .default = 'city'
)

  clean_data <- clean_data %>%
    select(where(~!all(is.na(.))))         # drop cols where all vals are NA, eg: X_EA and X_MA the annotation cols
  # clean_data <- clean_data %>%             # drop 'E' if last character in colname that starts with 'B'
  #   rename_at(vars(starts_with("B")), ~ str_remove(., paste0('B', "$")))
  # clean_data <- clean_data %>%             # drop 'E' if last character in colname that starts with 'B'
  #   rename_at(vars(ends_with("M")), ~ str_remove(., paste0('E', "$")))
  # clean_data$race <- gsub('alone or in any combination', 'aoic', clean_data$POPGROUP_LABEL)
  # clean_data$race <- gsub('alone', '', clean_data$race)
  # clean_data$race <- tolower(clean_data$race)         # make lowercase
  # clean_data$race <- gsub(', except taiwanese', '_ntaiw', clean_data$race)
  # clean_data$race <- trimws(clean_data$race)          # remove trailing white spaces
  # clean_data$race <- gsub(' ', '_', clean_data$race)  # replace spaces with underscores
  # clean_data <- clean_data %>% rename(fips = GEO_ID) %>%  # rename old geoid
  #   relocate(geoid, .before = fips)                      # make clean geoid first col
  # 
  # # convert to long format
  # clean_data <- clean_data %>%
  #   pivot_longer(cols = starts_with("B"),
  #                names_to = "var",
  #                values_to = "raw")
                                            
  # load variable names
  v21 <- load_variables(2021, "acs5", cache = TRUE)
  table_vars <- v21 %>% filter(grepl(table_name, name))
  
  # join variable names to data
  join_data <- clean_data %>%
    left_join(table_vars %>% select(name, label), by = c('var' = 'name'))
  colnames(join_data) <- tolower(colnames(join_data))
  
return(join_data)
}
  

### Send raw detailed tables to postgres - info for postgres tables automatically updates ###
send_to_mosaic <- function(acs_table, df, table_schema){
  
  name_as_string <- as_name(enquo(df))
  
  table_name <- sprintf("%s_acs_5yr_%s_multigeo_%s",
                        str_replace(name_as_string, "_df*.", ""), tolower(table_code), curr_yr)
  print(table_name)
  
  # generate table comment
  indicator <-  paste0("Disaggregated ", str_replace(name_as_string, "_df*.", ""), " detailed race alone and alone or in combination with another race by City, County, and State")       
  source <- paste0("ACS ", curr_yr - 4, "-", curr_yr, ", Table ", toupper(acs_table)) 
  column_names <- colnames(df)
  print(indicator)
  print(source)
  
  column_comments <- c()
  
  # send table to postgres
  dbWriteTable(con, 
               Id(schema = table_schema, table = table_name), 
               df, overwrite = FALSE)
  
  # comment on table and columns
  add_table_comments(con, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
  
}