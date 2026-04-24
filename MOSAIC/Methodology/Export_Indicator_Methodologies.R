# Run automated Indicator Methodology documents for MOSAIC: County / State


#### Set Up Environment ####
packages <- c("formattable", "knitr", "stringr", "tidyr", "dplyr", "tidyverse", "RPostgres", "glue", "formatR", "openxlsx", "fedmatch", "listr", "usethis", "here")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 


### Create Connection to RACE COUNTS Database ####
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("mosaic")

#### UPDATE VARIABLES EACH YEAR ####
curr_yr <- '2025'       # RC release year
curr_schema <- 'v7'     # RC postgres db schema
date <- 'July 2026'  # RC data release date
method_doc <- "W:\\Project\\RACE COUNTS\\2025_v7\\MOSAIC\\indicator_list.xlsx"
method_sheet <- "metadata"

#### Prep Methodology: All Geolevels ####
# get short api issue names
issues <- dbGetQuery(con, paste0("SELECT arei_issue_area, api_name_short FROM ", curr_schema, ".arei_issue_list")) %>%
  mutate(api_name_short = ifelse(api_name_short == "crim", "safe", api_name_short)) %>%
  arrange(api_name_short) # reorder so matches order of issues in fx

#### Methodology Functions ####
# Function to prep specific geolevel methodology
prep_method <- function (geo, curr_yr, curr_schema) {
  methodology <- read.xlsx(method_doc, sheet = method_sheet) %>%
    select(arei_indicator, arei_issue_area, bar_chart_header, data_source, racenote, method_long, link1, link2)
  methodology$links = ifelse(!is.na(methodology$link2), paste0(gsub(" &&&", ",", methodology$link1), ", ", methodology$link2), gsub(" &&&", ",", methodology$link1))
  
  # join api_name_short and race_eth to methodology
  methodology <- methodology %>% left_join(issues)
  
  # add api_name_short where missing
  methodology$api_name_short <- case_when(
    methodology$arei_issue_area == 'Safety & Justice' ~ 'crim',
    methodology$arei_issue_area == 'Demographics' ~ 'aapi',
    TRUE ~ methodology$api_name_short
  )
  
  # split methodology into list by issue area
  method_list <- split(methodology, f = methodology$api_name_short)
  method_list <- list_rename(method_list, hlthy = "hben")         # rename hben to hlthy so issues can be ordered as on website
  method_list <- list_rename(method_list, safe = "crim")          # rename crim to safe so issues can be ordered as on website
  method_list <- method_list[order(names(method_list))]           # put list into alpha order as issues are ordered on website
  
  # convert list elements into dfs
  invisible(list2env(method_list,envir=.GlobalEnv))
  
  return(method_list)
  
}

# Function to render specific geolevel methodology - this fx runs the RMD
render_method <- function(geoname, date, method_list) {
  
  rmarkdown::render(
    input = here(paste0("MOSAIC/Methodology/Indicator_Methodology_Prep.Rmd")),
    output_format = "html_document",
    output_file = paste0("MOSAIC/MOSAIC_Indicator_Methodology.html"),  
    output_dir = here(paste0("MOSAIC/Methodology")))

}


#### County / State Methodology ####
# prep methodology
geo <- 'cntyst'  # fx input to set geolevel
method_list <- prep_method(geo, curr_yr, curr_schema)

# render methodology - this code runs the RMD
geoname <- 'CountyState'
render_method(geoname, date, method_list)

