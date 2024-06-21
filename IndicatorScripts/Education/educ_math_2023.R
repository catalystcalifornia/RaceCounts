### 3rd Grade Math RC v5 ### 

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


############### PREP RDA_SHARED_DATA TABLE ########################

# ## Manually define test data download site, file names, file location etc.
# url = "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2022_all_ascii_v1.zip"
# zipfile = "W:\\Data\\Education\\CAASPP\\2021-22\\sb_ca2022_all_ascii_v1.zip"
# file = "W:\\Data\\Education\\CAASPP\\2021-22\\sb_ca2022_all_ascii_v1.txt"
# exdir = "W:\\Data\\Education\\CAASPP\\2021-22"
# 
# ## Manually define entities data download site, file names, file location etc.
# url2 = "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2022entities_ascii.zip"
# zipfile2 = "W:\\Data\\Education\\CAASPP\\2021-22\\sb_ca2022entities_ascii.zip"
# file2 = "W:\\Data\\Education\\CAASPP\\2021-22\\sb_ca2022entities_ascii.txt"
# 
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# fieldtype = c(1:5,8,9,31) # specify which cols should be varchar, the rest will be assigned numeric - this may NOT match metadata table
# table_schema <- "education"
# table_name <- "caaspp_multigeo_school_research_file_reformatted_2021_22"
# table_comment_source <- "Downloaded from https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB?ps=true&lstTestYear=2022&lstTestType=B&lstCounty=00&lstDistrict=00000. File layout: https://caaspp-elpac.cde.ca.gov/caaspp/research_fixfileformat19."
# table_source <- "Wide data format, multigeo table with state, county, district, and school"
# 
# source("W:/Project/RACE COUNTS/Functions/rdashared_functions.R")
# df <- get_caaspp_data(url, zipfile, file, url2, zipfile2, file2, exdir)
# head(df)
# 
# ## Run function to add rda_shared_data column comments
# # See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
# 
# #### NOTE: Each year, the xpath needs to be updated in this function. See rdashared_functions.R for more info ###
# url3 <- "https://caaspp-elpac.ets.org/caaspp/ResearchFileFormatSB?ps=true&lstTestYear=2022&lstTestType=B"   # define webpage with metadata
# #colcomments <- get_caaspp_metadata(url3, table_schema, table_name)
# View(colcomments)

df <- st_read(con, query = "SELECT * FROM education.caaspp_multigeo_school_research_file_reformatted_2021_22") # comment out code to pull data and use this once rda_shared_data table is created

###### PREP FOR RC FUNCTIONS #######
df_subset <- rename(df, rate = percentage_standard_met_and_above, pop = students_with_scores, race = student_grp_id)

# Filter for 3rd grade, math test, race/ethnicity subgroups, county/state/district/ level 
df_subset <- df_subset %>% filter(grade == "03" & test_id == "02" & race %in% c("001","074","075","076","077","078","079","080","144")
                                  & type_id %in% c("04", "05", "06")) %>%    
  
  ## calc raw/rate and screen ---------------------------------------------------------
#calculate raw
mutate(raw = round(pop * rate / 100, 0)) 

#pop screen
threshold = 20
df_subset <- df_subset %>% mutate(raw = ifelse(pop < threshold, NA, raw), rate = ifelse(pop < threshold, NA, rate))

#select just fields we need
df_subset <- df_subset %>% select(geoname, cdscode, type_id, race, rate, raw, pop) 

#rename race/eth categories
df_subset$race <- gsub("001", "total", df_subset$race)
df_subset$race <- gsub("074", "nh_black", df_subset$race)
df_subset$race <- gsub("075", "nh_aian", df_subset$race)
df_subset$race <- gsub("076", "nh_asian", df_subset$race)
df_subset$race <- gsub("077", "nh_filipino", df_subset$race)
df_subset$race <- gsub("078", "latino", df_subset$race)
df_subset$race <- gsub("079", "nh_pacisl", df_subset$race)
df_subset$race <- gsub("080", "nh_white", df_subset$race)
df_subset$race <- gsub("144", "nh_twoormor", df_subset$race)
df_subset$geoname <- gsub("State of ", "", df_subset$geoname)

#get county geoids
census_api_key(census_key1, overwrite=TRUE) # In practice, may need to include install=TRUE if switching between census api keys
Sys.getenv("CENSUS_API_KEY") # confirms value saved to .renviron

# county geoids
counties <- get_acs(geography = "county",
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = 2021)

counties <- counties[,1:2]
counties$NAME <- gsub(" County, California", "", counties$NAME) 
names(counties) <- c("geoid", "geoname")
county_match <- filter(df_subset,type_id=="05") %>% right_join(counties,by='geoname') %>% mutate(district='')

# get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
districts <- st_read(con, query = "SELECT cdscode, district, ncesdist AS geoid FROM education.cde_public_schools_2021_22 WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'")
district_match <- filter(df_subset,type_id=="06") %>% right_join(districts,by='cdscode')

matched <- union(county_match, district_match) %>% select(c(cdscode, geoid, district)) %>% distinct() # combine distinct county and district geoid matched df's
df_final <- df_subset %>% full_join(matched, by='cdscode')
df_final <- df_final %>% relocate(geoid) %>% mutate(geoname = ifelse(type_id == "04", "California", geoname), # add geoname and geoid for state
                                                    geoid = ifelse(type_id == "04", "06", geoid),
                                                    geoname = ifelse(type_id == "06", district, geoname)) %>% select(-c(district)) # sub district name into geoname for district rows
df_final <- filter(df_final, !is.na(geoid)) # remove records without fips codes

d <- df_final %>% pivot_wider(names_from = race, names_glue = "{race}_{.value}", values_from = c(pop, raw, rate)) # pivot to wide format, ensure correct col names for RC functions


####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

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
county_table_name <- "arei_educ_gr3_math_scores_county_2023"
state_table_name <- "arei_educ_gr3_math_scores_state_2023"
city_table_name <- "arei_educ_gr3_math_scores_district_2023"
rc_schema <- "v5"

indicator <- "Students scoring proficient or better on 3rd grade Math (%)"
source <- "CAASPP 2021-22 https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB?ps=true&lstTestYear=2022&lstTestType=B&lstCounty=00&lstDistrict=00000"

#send tables to postgres
#to_postgres(county_table,state_table)
#city_to_postgres()