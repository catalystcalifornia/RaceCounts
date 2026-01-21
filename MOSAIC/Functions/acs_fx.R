### MOSAIC-related functions for ACS data ###
#install packages if not already installed
packages <- c("readr", "tidyr", "dplyr", "DBI", "RPostgres", "tidycensus", "tidyverse", "stringr", "usethis", "httr", "janitor")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 



#### Function to automate writing API Calls ####
get_detailed_data <- function(table, race, year = 2021) {
  # race = for MOSAIC, either 'asian' or 'nhpi'
  # table = ACS table name, eg: "B25003" or "S2701"
  # year = ACS data year, defaults to 2021 if none specified
  race_code <- case_when(
    str_detect(race, regex('asian', ignore_case = TRUE)) ~ '-04',
    str_detect(race, regex('nhpi', ignore_case = TRUE)) ~ '-05',
    .default = ''
    )
  
  table_name <- toupper(table)
  
  api_call <- sprintf(
    "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&POPGROUP=pseudo(%s)&ucgid=pseudo(0400000US06$0500000)",
    year, table_name, race_code)

  print(api_call)
  
  response <- GET(api_call)
  
  status_code(response)
  # Check if the request was successful
  if (status_code(response) == 200) {
    print("API request successful.")
  } else {
    stop("API request failed with status code: ", status_code(response))
  }
  
  raw_content <- content(response, "text")
  parsed_data <- fromJSON(raw_content)

  # Optional: Convert to a data frame if appropriate
  if (is.data.frame(parsed_data)) {
    data_frame <- parsed_data
  } else if (is.list(parsed_data) && !is.null(parsed_data$results)) {
    # Handle cases where data is nested, e.g., within a 'results' field
    data_frame <- as.data.frame(parsed_data$results)
  }
  
  
  #parsed_data <- as.data.frame(parsed_data) 
  #parsed_data <- janitor::row_to_names(parsed_data, row_number = 1)

return(data_frame)
}












## Get list of Detailed Asian and Detailed NHPI codes
# View available variables for 2023 5-year ACS
v21 <- load_variables(2021, "acs5/spt", cache = TRUE)


spt_data <- get_acs(
  geography = "state",  # Example geography (can be county, tract, etc. based on availability)
  variables = c("B25003_001"), # Example variable ID for a specific race/ethnicity table
  year = 2021,          # The end year of the 5-year estimate
  survey = "acs5/spt"   # Specifies the Selected Population Tables dataset
)


# Filter for detailed codes (Table B02018 & B02019)
asian_vars <- v21 %>%
  filter(str_detect(name, "B02018"))

nhpi_vars <- v21 %>%
  filter(str_detect(name, "B02019"))

clean_detailed_names <- function(df) {
  df <- df %>% mutate()
  
  
  asian_vars2 <- v21 %>%
    filter(str_detect(name, "B02001"))
  
return(df)  
}