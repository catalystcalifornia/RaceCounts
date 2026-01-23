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

  # reformat clean data
  df_wide <- clean_data %>%
    pivot_longer(
      cols = starts_with("B25003"),
      names_to = "orig_col",
      values_to = "value"
    ) %>%
    mutate(
      suffix = str_remove(orig_col, "^B25003_?"),
      new_col = paste0("B25003_", POPGROUP, "_", suffix)
    ) %>%
    select(NAME, geoid, geolevel, new_col, value) %>%
    pivot_wider(
      names_from = new_col,
      values_from = value
    )
  
  colnames(df_wide) <- tolower(colnames(df_wide))
  
  source(".//Functions//rdashared_functions.R")
  df_wide <- df_wide %>% 
    mutate(geoname = name) %>%
    clean_geo_names() %>%
    mutate(name = geoname) %>%
    select(-geoname)

  # prep metadata
  metadata <- clean_data %>%
    pivot_longer(cols = starts_with("B"),
                 names_to = "var",
                 values_to = "raw")
  
  metadata <- metadata %>%
    select(POPGROUP, POPGROUP_LABEL, var) %>%
    unique() %>%
    mutate(var_suff = sub(".*_", "", var),
           generic_var = gsub(("E|M"), "", var),
           new_var = tolower(paste0(table_code, "_", POPGROUP, "_", var_suff)))
                                              
  # load variable names
  v21 <- load_variables(2021, "acs5", cache = TRUE)
  table_vars <- v21 %>% filter(grepl(table_name, name))
  
  # join variable names to metadata
  metadata <- metadata %>% 
    left_join(table_vars %>% select(name, label), by = c("generic_var" = "name")) %>%
    mutate(new_label = paste0(label, " ", POPGROUP_LABEL)) %>%
    mutate(new_label = case_when(
      grepl("M", var) == TRUE ~ gsub("Estimate", "MOE", new_label),
      .default = new_label))
  
  metadata_ <- metadata %>%
    select(new_var, new_label) %>%
    arrange(new_var)
  
  new_rows <- data.frame(
    new_var = c("name", "geoid", "geolevel"),
    new_label = c("","fips code","city, county, state"))
  
  metadata_final <- rbind(new_rows, metadata_)

data_list <- list(df_wide, metadata_final)

names(data_list) <- c(paste0(race,"_df"), "metadata")

return(data_list)
}
  

### Send raw detailed tables to postgres - info for postgres tables automatically updates ###
send_to_mosaic <- function(acs_table, df_list, table_schema){
  
  name_as_string <- names(df_list)[1]
  
  table_name <- sprintf("%s_acs_5yr_%s_multigeo_%s",
                        str_replace(name_as_string, "_df*.", ""), tolower(table_code), curr_yr)
  print(table_name)
  
  # generate table comment
  indicator <-  paste0("Disaggregated ", str_replace(name_as_string, "_df*.", ""), " detailed race alone and alone or in combination with another race by City, County, and State")       
  source <- paste0("ACS ", curr_yr - 4, "-", curr_yr, ", Table ", toupper(acs_table)) 
  print(indicator)
  print(source)
  
  column_names <- df_list[[2]]$new_var
  column_comments <- df_list[[2]]$new_label
  
  # send table to postgres
  dbWriteTable(con, 
               Id(schema = table_schema, table = table_name), 
               df_list[[1]], overwrite = FALSE)
  
  # comment on table and columns
  add_table_comments(con, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
  
}