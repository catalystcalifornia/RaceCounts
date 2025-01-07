# Run automated Indicator Methodology documents: County / State, City, Legislative District

packages <- c("formattable", "knitr", "stringr", "tidyr", "dplyr", "tidyverse", "RPostgreSQL", "glue", "formatR", "readxl", "usethis", "here")
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

### UPDATE VARIABLES EACH YEAR ####
curr_yr <- '2024'       # RC release year
curr_schema <- 'v6'     # RC postgres db schema
date <- 'October 2024'  # RC data release date


#### Prep Methodology: All Geolevels ####
# get short api issue names
issues <- dbGetQuery(con, paste0("SELECT arei_issue_area, api_name_short FROM ", curr_schema, ".arei_issue_list")) %>%
  arrange(api_name_short) # reorder so matches order of issues in fx

# get race types
races_ <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".arei_race_type"))
races_ <- races_[order(races_$race_type, races_$race_label_short), ] # generate race-eth strings
races_$race_eth = ave(as.character(races_$race_label_long),
                      races_$race_type,
                      FUN = function(x){
                        paste0(x,collapse = ", ")
                      })
races <- races_ %>% select(race_type, race_eth) %>% unique()


### REMOVE AFTER WE'VE ADDED LINKS AND RACE/ETHNICITY NOTES TO PGADMIN TABLES
links <- read_excel("W:\\Project\\RACE COUNTS\\2025_v7\\RaceCounts\\data_src_links.xlsx") %>% select(-c(api_name_short)) %>%
  mutate(clean_ind = clean_strings(arei_indicator)) %>% mutate(links = ifelse(!is.na(link2), paste0(link1, ", ", link2), link1))


#### Methodology Functions ####
# Function to prep specific geolevel methodology
prep_method <- function (geo, curr_yr, curr_schema) {

  methodology <- dbGetQuery(con, paste0("SELECT arei_indicator, arei_issue_area, methodology, bar_chart_header, data_source, race_type FROM ", curr_schema, ".arei_indicator_list_", geo))
  
  # join api_name_short and race_eth to methodology
  methodology <- methodology %>% left_join(issues) %>%
    left_join(races)
  
  ### REMOVE AFTER ADDING LINKS TO POSTGRES: join data src links to methodology
  methodology$clean_ind <- clean_strings(methodology$arei_indicator) 
  methodology <- methodology %>% left_join(links %>% select(c(clean_ind, links)), by = 'clean_ind')
  
  # split methodology into list by issue area
  method_list <- split(methodology, f = methodology$api_name_short)
  
  # convert list elements into dfs
  invisible(list2env(method_list,envir=.GlobalEnv))

return(method_list)

}

# Function to render specific geolevel methodology
render_method <- function(geoname, date, method_list) {

  rmarkdown::render(#input = paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/LF/RaceCounts/Methodology/Indicator_Methodology_", geoname, ".Rmd"),
    input = here(paste0("Methodology/Indicator_Methodology_", geoname, ".Rmd")),
    output_format = "html_document",
    output_file = paste0("index.html"),  
    #output_dir = paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/RaceCounts/Methodology/Indicator_Methodology_", geoname))
    output_dir = here(paste0("Methodology/Indicator_Methodology_", geoname)))

}


#### County / State Methodology ####
# prep methodology
geo <- 'cntyst'  # fx input to set geolevel
method_list <- prep_method(geo, curr_yr, curr_schema)

# render methodology
geoname <- 'County_State'
render_method(geoname, date, method_list)


#### City Methodology####
# prep methodology
geo <- 'city'  # fx input to set geolevel
method_list <- prep_method(geo, curr_yr, curr_schema)

# render methodology
geoname <- 'City'
render_method(geoname, date, method_list)


#### Legislative District Methodology####
# prep methodology
geo <- 'leg'  # fx input to set geolevel
method_list <- prep_method(geo, curr_yr, curr_schema)

# render methodology
geoname <- 'Leg_District'
render_method(geoname, date, method_list)
