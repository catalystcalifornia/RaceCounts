### 3rd Grade English Language Arts & Math RC v6 ### 

##install packages if not already installed ------------------------------
list.of.packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)
library(dplyr)
library(tidyr)
library(tidycensus)
library(sf)
library(tidyverse) # to scrape metadata table from cde website
library(usethis) # connect to github

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2022_23"  # must keep same format
dwnld_url <- "https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB?ps=true&lstTestYear=2023&lstTestType=B&lstCounty=00&lstDistrict=00000"
rc_schema <- "v6"
yr <- "2024"

############### PREP CAASPP RDA_SHARED_DATA TABLE ########################

# SKIP THIS CODE AFTER RDA_SHARED_DATA TABLE HAS BEEN CREATED AND GO TO NEXT STEP.

      # ## Manually update entities data download URL and filenames
      # url = "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2023_all_ascii_v1.zip"      # Copy/paste the URL for the "All Student Groups" txt file.
      # zipfile = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\sb_ca2023_all_ascii_v1.zip")  # Copy/paste the filename for the "All Student Groups" zip txt file.
      # file = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\sb_ca2023_all_ascii_v1.txt")     # Copy/paste the filename for the "All Student Groups" txt file.
      # 
      # exdir = paste0("W:\\Data\\Education\\CAASPP\\", curr_yr)
      # 
      # ## Manually update entities data download URL and filenames
      # url2 = "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2023entities_ascii.zip"     # Copy/paste the URL for the Entities txt file.
      # zipfile2 = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\sb_ca2023entities_ascii.zip") # Copy/paste the filename for the Entities zip txt file.
      # file2 = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\sb_ca2023entities_ascii.txt")    # Copy/paste the filename for the Entities txt file.
      # 
      # ## Manually update file layout URL, other variables should not change
      # fieldtype = c(1:5,8,9,31) # specify which cols should be varchar, the rest will be assigned numeric - this may NOT match metadata table
      # table_schema <- "education"
      # layout_url <- "https://caaspp-elpac.ets.org/caaspp/ResearchFileFormatSB?ps=true&lstTestYear=2023"
      # table_source <- "Wide data format, multigeo table with state, county, district, and school"
      # 
      # # set functions source
      # source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/rdashared_functions.R")
      # 
      # df <- get_caaspp_data(url, zipfile, file, url2, zipfile2, file2, exdir)
      # #head(df)
      # 
      # 
      # # Run function to add rda_shared_data column comments ------------------------------------------------------------------
      # # See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
      # #### NOTE: EACH YEAR, the xpath needs to be updated in get_caaspp_metadata{} in rdashared_functions.R ###
      # 
      # #### NOTE: Before running this fx, you need to update the get_caaspp_metadata{} code in rdashared_functions.R after "html_nodes(xpath =" on or around line 221.
      # colcomments <- get_caaspp_metadata(layout_url, table_schema, table_name)
      # #View(colcomments)



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

indicator <- "Students scoring proficient or better on 3rd grade Math (%)"
source <- paste0("CAASPP ", curr_yr, " ", dwnld_url)

#send tables to postgres
#to_postgres(county_table,state_table)
#city_to_postgres()

dbDisconnect(con)