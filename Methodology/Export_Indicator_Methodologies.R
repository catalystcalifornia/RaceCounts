# Run automated Indicator Methodology documents: County / State, City, Legislative District


#### Set Up Environment ####
packages <- c("formattable", "knitr", "stringr", "tidyr", "dplyr", "tidyverse", "RPostgres", "glue", "formatR", "readxl", "fedmatch", "listr", "usethis", "here")
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

#### UPDATE VARIABLES EACH YEAR ####
curr_yr <- '2025'       # RC release year
curr_schema <- 'v7'     # RC postgres db schema
date <- 'November 2025'  # RC data release date


#### Prep Methodology: All Geolevels ####
# get short api issue names
issues <- dbGetQuery(con, paste0("SELECT arei_issue_area, api_name_short FROM ", curr_schema, ".arei_issue_list")) %>%
  mutate(api_name_short = ifelse(api_name_short == "crim", "safe", api_name_short)) %>%
  arrange(api_name_short) # reorder so matches order of issues in fx

# get race types
races_ <- dbGetQuery(con, paste0("SELECT * FROM ", curr_schema, ".arei_race_type"))  # remove _updated after all updates finalized
races_ <- races_[order(races_$race_type, races_$race_label_short), ] # generate race-eth strings
races_$race_label_long2 <- gsub("\\*", paste0("\\\\","*"), races_$race_label_long) # create new col with escape char for * so does not appear italicized
races_$race_eth = ave(as.character(races_$race_label_long2),
                      races_$race_type,
                      FUN = function(x){
                        paste0(x,collapse = ", ")
                      })
races <- races_ %>% select(race_type, race_eth) %>% unique()


#### Methodology Functions ####
# Function to prep specific geolevel methodology
prep_method <- function (geo, curr_yr, curr_schema) {
  methodology <- dbGetQuery(con, paste0("SELECT arei_indicator, arei_issue_area, bar_chart_header, data_source, race_type, method_long, link1, link2, ind_order FROM ", curr_schema, ".arei_indicator_list_", geo))
  methodology$links = ifelse(!is.na(methodology$link2), paste0(gsub(" &&&", ",", methodology$link1), ", ", methodology$link2), gsub(" &&&", ",", methodology$link1))
  
  # join api_name_short and race_eth to methodology
  methodology <- methodology %>% left_join(issues) %>%
    left_join(races)
  methodology$arei_issue_area = ifelse(methodology$arei_issue_area == "Crime & Justice", "Safety & Justice", methodology$arei_issue_area)
  methodology <- methodology %>% arrange(arei_issue_area, ind_order)
  
  # split methodology into list by issue area
  method_list <- split(methodology, f = methodology$api_name_short)
  method_list <- list_rename(method_list, hlthy = "hben")         # rename hben to hlthy so issues can be ordered as on website
  method_list <- method_list[order(names(method_list))]           # put list into alpha order as issues are ordered on website
  
  # convert list elements into dfs
  invisible(list2env(method_list,envir=.GlobalEnv))
  
  return(method_list)
  
}

# Function to render specific geolevel methodology - this fx runs the RMD
render_method <- function(geoname, date, method_list) {
  
  rmarkdown::render(
    input = here(paste0("Methodology/Indicator_Methodology_Prep.Rmd")),
    output_format = "html_document",
    output_file = paste0("Indicator_Methodology_", geoname,".html"),  
    output_dir = here(paste0("Methodology")))

}


#### County / State Methodology ####
# prep methodology
geo <- 'cntyst'  # fx input to set geolevel
method_list <- prep_method(geo, curr_yr, curr_schema)

# render methodology - this code runs the RMD
geoname <- 'CountyState'
render_method(geoname, date, method_list)


#### City Methodology ####
# prep methodology
geo <- 'city'  # fx input to set geolevel
method_list <- prep_method(geo, curr_yr, curr_schema)

# # render methodology
geoname <- 'City'
render_method(geoname, date, method_list)


#### Legislative District Methodology ####
# prep methodology
geo <- 'leg'  # fx input to set geolevel
method_list <- prep_method(geo, curr_yr, curr_schema)

# render methodology
geoname <- 'Leg_District'
render_method(geoname, date, method_list)

