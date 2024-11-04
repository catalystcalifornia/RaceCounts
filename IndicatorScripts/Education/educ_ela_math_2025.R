### 3rd Grade English Language Arts & Math RC v7 ###

## install and load packages ------------------------------
packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "readxl", "usethis", "here")
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
con <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2023_24"  # must keep same format
dwnld_url <- "https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB?ps=true&lstTestYear=2024&lstTestType=B&lstCounty=00&lstDistrict=00000" # try just updating the yr in the URL
data_url <- "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2024_1_ascii_v1.zip"          # Copy URL for CA Statewide research file, All Students, fixed width (TXT) on dwndl_url
entities_url <- "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2024entities_ascii.zip"   # Copy URL for Entities List, fixed width (TXT) on dwndl_url
layout_url <- "https://caaspp-elpac.ets.org/caaspp/ResearchFileFormatSB?ps=true&lstTestYear=2024&lstTestType=B" # try just updating the year in the URL
layout_dwlnd_url <- "https://caaspp-elpac.ets.org/caaspp/docs/2024_SBAC_Research%20File%20Layout.xlsx" # try just updating the year in the URL
rc_schema <- "v7"
yr <- "2025"
# You must also update the xpath in the "html_nodes" line in the get_caaspp_data{} in rdashared_functions.R. More info in that file. #


############### PREP CAASPP RDA_SHARED_DATA TABLE ########################

# SKIP THIS CODE AFTER RDA_SHARED_DATA TABLE HAS BEEN CREATED AND GO TO NEXT STEP.

      ## Create test data download URL and filenames
       url = data_url      # "All Student Groups" txt file.
       data_file <- str_remove_all(data_url, "https://caaspp-elpac.ets.org/caaspp/researchfiles/|.zip")
       zipfile = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",data_file,".zip")  
       file = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",data_file,".txt")     
      
       exdir = paste0("W:\\Data\\Education\\CAASPP\\", curr_yr)  # set data download filepath
      
       ## Create entities data download URL and filenames
       url2 = entities_url   
       entities_file <- str_remove_all(entities_url, "https://caaspp-elpac.ets.org/caaspp/researchfiles/|.zip")
       zipfile2 = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",entities_file,".zip") 
       file2 = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",entities_file,".txt")    
      
       ## Create layout download URL and filenames
       url3 = layout_dwlnd_url

       ## Create file layout URL, other variables should not change
       #fieldtype = c(1:5,8,9,31) # specify which cols should be varchar, the rest will be assigned numeric - this may NOT match metadata table
       table_schema <- "education"
       table_source <- "Wide data format, multigeo table with state, county, district, and school"
      
       # set functions source
       source(here("Functions", "rdashared_functions.R"))
      
       df <- get_caaspp_data(url, zipfile, file, url2, zipfile2, file2, url3, exdir)
       head(df)
      
      
       # Run function to add rda_shared_data column comments ------------------------------------------------------------------
       # See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
       #### NOTE: EACH YEAR, the xpath needs to be updated in get_caaspp_metadata{} in rdashared_functions.R ###
      
       #### NOTE: Before running this fx, you need to update the get_caaspp_metadata{} code in rdashared_functions.R after "html_nodes(xpath =" on or around line 221.
       colcomments <- get_caaspp_metadata(layout_url, table_schema, table_name)
       View(colcomments)



# Get County GEOIDS --------------------------------------------------------------------
### Always run this code before running ELA or Math sections.
##### Used in clean_ela_math{} later
census_api_key(census_key1, overwrite=TRUE) # In practice, may need to include install=TRUE if switching between census api keys
Sys.getenv("CENSUS_API_KEY") # confirms value saved to .renviron

counties <- get_acs(geography = "county",
                    variables = c("B01001_001"), 
                    state = "CA", 
# Update 'year' to match CAASPP data year.
                    year = 2022)      

counties <- counties[,1:2]
counties$NAME <- gsub(" County, California", "", counties$NAME) 
names(counties) <- c("geoid", "geoname")


###### ELA: PREP FOR RC FUNCTIONS #######
# comment out code to pull data from CDE above and use this once rda_shared_data table is created
df <- dbGetQuery(con, "SELECT * FROM education.caaspp_multigeo_school_research_file_reformatted_2022_23")

# set functions source
source("W:/Project/RACE COUNTS/Functions/RC_ELA_Math_Functions.R")
# define test_id as "01" for ELA or "02" for Math
test_id <- "01" # ELA

df_final_e <- clean_ela_math(df, test_id)

# pivot to wide format, ensure correct col names for RC functions
df_final_e <- df_final_e %>% pivot_wider(names_from = race, names_glue = "{race}_{.value}", values_from = c(pop, raw, rate)) 


####### ELA: CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d <- df_final_e # set ela df as d
d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update $asbest line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table
state_table <- d[d$geoname == 'California', ] %>% select(-c(cdscode, type_id))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
View(state_table)

#split COUNTY into separate table
county_table <- d[d$type_id == '05', ] %>% select(-c(cdscode, type_id))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
View(county_table)

#split CITY into separate table
city_table <- d[d$type_id == '06', ] %>% select(-c(type_id))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname") %>% relocate(cdscode, .after = dist_id)
View(city_table)


###update info for postgres tables###
test <- "ela"
county_table_name <- paste0("arei_educ_gr3_",test,"_scores_county_",yr)
state_table_name <- paste0("arei_educ_gr3_",test,"_scores_state_",yr)
city_table_name <- paste0("arei_educ_gr3_",test,"_scores_district_",yr)

indicator <- "Students scoring proficient or better on 3rd grade English Language Arts (%)"
source <- paste0("CAASPP ", curr_yr, " ", dwnld_url)

#send tables to postgres
#to_postgres(county_table,state_table)
#city_to_postgres()



###### MATH: PREP FOR RC FUNCTIONS #######
# comment out code to pull data from CDE above and use this once rda_shared_data table is created
# if you have NOT already run the ELA code above, you will need to run this line. The table is very large, so it takes awhile to run.
#df <- dbGetQuery(con, "SELECT * FROM education.caaspp_multigeo_school_research_file_reformatted_2022_23")

# set functions source
source("W:/Project/RACE COUNTS/Functions/RC_ELA_Math_Functions.R")
# define test_id as "01" for ELA or "02" for Math
test_id <- "02" # Math

df_final_m <- clean_ela_math(df, test_id)

# pivot to wide format, ensure correct col names for RC functions
df_final_m <- df_final_m %>% pivot_wider(names_from = race, names_glue = "{race}_{.value}", values_from = c(pop, raw, rate)) 


####### MATH: CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d <- df_final_m # set math df as d
d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update $asbest line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table
state_table <- d[d$geoname == 'California', ] %>% select(-c(cdscode, type_id))

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
View(state_table)

#split COUNTY into separate table
county_table <- d[d$type_id == '05', ] %>% select(-c(cdscode, type_id))

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
View(county_table)

#split CITY into separate table
city_table <- d[d$type_id == '06', ] %>% select(-c(type_id))

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname") %>% relocate(cdscode, .after = dist_id)
View(city_table)


###update info for postgres tables###
test <- "math"
county_table_name <- paste0("arei_educ_gr3_",test,"_scores_county_",yr)
state_table_name <- paste0("arei_educ_gr3_",test,"_scores_state_",yr)
city_table_name <- paste0("arei_educ_gr3_",test,"_scores_district_",yr)

indicator <- paste0("Created on ", Sys.Date(), ". Students scoring proficient or better on 3rd grade Math (%)")
source <- paste0("CAASPP ", curr_yr, " ", dwnld_url)

#send tables to postgres
#to_postgres(county_table,state_table)
#city_to_postgres()

dbDisconnect(con)