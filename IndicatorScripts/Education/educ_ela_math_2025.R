### 3rd Grade English Language Arts & Math RC v7 ###

## install and load packages ------------------------------
packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "rpostgis", "usethis")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen = 100) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
source("./Functions/RC_ELA_Math_Functions.R")

con <- connect_to_db("rda_shared_data")

qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Sheet_ELA_Math.docx"

# update each year --------
curr_yr <- "2023_24"  # CAASPP year - must keep same format
acs_yr <- 2023        # county geoid year - match as closely to curr_yr
rc_schema <- "v7"
yr <- "2025"
threshold = 20        # If pop (students tested) at target level (sch dist for city, county, state, leg dist) is less than threshold, data is screened out
dwnld_url <- "https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB?ps=true&lstTestYear=2024&lstTestType=B&lstCounty=00&lstDistrict=00000" # try just updating the yr in the URL
data_url <- "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2024_all_ascii_v1.zip"        # Copy URL for CA Statewide "combined research file, All Student Groups, fixed width" (TXT) on dwndl_url
entities_url <- "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2024entities_ascii.zip"   # Copy URL for Entities List, fixed width (TXT) on dwndl_url
layout_url <- "https://caaspp-elpac.ets.org/caaspp/ResearchFileFormatSB?ps=true&lstTestYear=2024&lstTestType=B" # try just updating the year in the URL

school_dwnld_url <- "https://www.cde.ca.gov/ds/si/ds/pubschls.asp"           # this link may or may not need to be updated.
school_url <- "https://www.cde.ca.gov/schooldirectory/report?rid=dl1&tp=txt" # this link may or may not need to be updated. check school_dwnld_url to find out.
school_layout_url <- "https://www.cde.ca.gov/ds/si/ds/fspubschls.asp"        # this link may or may not need to be updated. check school_dwnld_url to find out.

# You must also update the xpath in the "html_nodes" line in the get_caaspp_data{} and in get_caaspp_metadata{} in rdashared_functions.R. #
## More info in that script. #

############### PREP SCHOOLS AND CAASPP RDA_SHARED_DATA TABLES (IF NEEDED) ########################

# SKIP THIS CODE AFTER SCHOOLS AND CAASPP RDA_SHARED_DATA TABLES HAVE BEEN CREATED AND GO TO NEXT STEP.
      # table_schema <- "education"
      # table_source <- "Wide data format, multigeo table with state, county, district, and school"
      # source("./Functions/rdashared_functions.R")         # set functions source
      # 
      # ## Create test data download URL and filenames
      #  url = data_url      # "All Student Groups" txt file.
      #  data_file <- str_remove_all(data_url, "https://caaspp-elpac.ets.org/caaspp/researchfiles/|.zip")
      #  zipfile = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",data_file,".zip")
      #  file = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",data_file,".txt")
      # 
      #  exdir = paste0("W:\\Data\\Education\\CAASPP\\", curr_yr)  # set data download filepath
      # 
      #  ## Create entities data download URL and filenames
      #  url2 = entities_url
      #  entities_file <- str_remove_all(entities_url, "https://caaspp-elpac.ets.org/caaspp/researchfiles/|.zip")
      #  zipfile2 = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",entities_file,".zip")
      #  file2 = paste0("W:\\Data\\Education\\CAASPP\\",curr_yr,"\\",entities_file,".txt")
      # 
      #  ## Create layout URL
      #  url3 = layout_url
      # 
      #  ## Run fx to create schools rda_shared_table ------------------------------------------------------------------
      # # schools <- get_cde_schools(school_url, school_dwnld_url, school_layout_url, table_source)
      #  
      #      # Run function to add schools rda_shared_data column comments 
      #      # See for more on scraping tables from websites: https://stackoverflow.com/questions/55092329/extract-table-from-webpage-using-r and https://cran.r-project.org/web/packages/rvest/rvest.pdf
      #      html_nodes <- "table"
      #      school_colcomments <- get_cde_schools_metadata(school_layout_url, html_nodes, table_schema)
      #      
      #  ## Run fx to create CAASPP rda_shared_table: This may take awhile bc the file is large.  ------------------------------------------------------------------
      #  #### NOTE: EACH YEAR, the xpath needs to be updated in get_caaspp_data{} in rdashared_functions.R ###
      # df <- get_caaspp_data(url, zipfile, file, url2, zipfile2, file2, url3, dwnld_url, exdir, table_source)
      # head(df)
      # 
      #      # Run function to add CAASPP rda_shared_data column comments
      #      #### NOTE: EACH YEAR, the xpath needs to be updated in get_caaspp_metadata{} in rdashared_functions.R ###
      #      colcomments <- get_caaspp_metadata(url3, table_schema)
      #      View(colcomments)

# Get County GEOIDS --------------------------------------------------------------------
### Always run this code before running ELA or Math sections.
##### Used in clean_ela_math{} later
counties <- get_acs(geography = "county",
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = acs_yr)      

counties <- counties[,1:2]
counties$NAME <- gsub(" County, California", "", counties$NAME) 
names(counties) <- c("geoid", "geoname")


### Pull in CAASPP data from postgres ####
# comment out code to pull data from CDE above and use this once rda_shared_data table is created
caaspp_df <- dbGetQuery(con, paste0("SELECT * FROM education.caaspp_multigeo_school_research_file_reformatted_", curr_yr)) %>%
  #adding geolevels
  mutate(geolevel = ifelse(type_id == "04", "state", 
                           ifelse(type_id == "05", "county",
                                  ifelse(type_id == "06", "district",
                                         ifelse(type_id == "07", "school",
                                                ifelse(type_id == "09", "school",
                                                       ifelse(type_id == "10", "school",""))))))) %>%
  relocate(geolevel, .before = 3)


##### ELA: PREP FOR RC FUNCTIONS #######
# define test_id as "01" for ELA or "02" for Math
test_id <- "01" # ELA

df_final_e <- clean_ela_math(caaspp_df, test_id)

# pivot to wide format, ensure correct col names for RC functions
df_final_e_ <- df_final_e %>% pivot_wider(names_from = race, names_glue = "{race}_{.value}", values_from = c(pop, raw, rate)) 
schools_final_e <- df_final_e_ %>% filter(geolevel == 'school')  # create school-level only df
df_final_e <- df_final_e %>% filter(geolevel != 'school')        # drop school-level rows, keep sch dist/county/state rows

# ####### Legislative Districts Prep From School District Data #######
# ##Step 1: Prep school district data for Leg Dist calcs
# leg_schdist_df <- clean_leg_elamath(caaspp_df)
# 
# 
# ##Step 2: Pull xwalks for district level aggregation
# #Elementary School Districts 
# xwalk_esd_sen <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.esd_2020_state_senate_2024"))
# xwalk_esd_assm <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.esd_2020_state_assembly_2024"))
# 
# #Unified School Districts
# xwalk_usd_sen <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.usd_2020_state_senate_2024"))
# xwalk_usd_assm <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.usd_2020_state_assembly_2024"))
# 
# xwalk_sen <- rbind(xwalk_esd_sen, xwalk_usd_sen) %>%
#   rename(geoid = geo_id,
#          geoname = geo_name,
#          leg_id = sldu24)  %>%
#   mutate(geolevel = 'sldu')
# 
# xwalk_assm <- rbind(xwalk_esd_assm, xwalk_usd_assm) %>%
#   rename(geoid = geo_id,
#          geoname = geo_name,
#          leg_id = sldl24) %>%
#   mutate(geolevel = 'sldl')
# 
# 
# ##Step 3: Calc & screen weighted Leg Dist data from school dist data
# sen_df_e <- calc_leg_elamath_sd(leg_schdist_df, xwalk_sen, threshold) %>% 
#   filter(test_id == test) %>%
#   mutate(geoname = paste0("State Senate District ", #adding geoname
#                           as.numeric(str_sub(geoid, -2))))
# assm_df_e <- calc_leg_elamath_sd(leg_schdist_df, xwalk_assm, threshold) %>% 
#   filter(test_id == test) %>%
# mutate(geoname = paste0("State Assembly District ", #adding geoname
#                         as.numeric(str_sub(geoid, -2))))
# df_join_e_v1 <- bind_rows(df_final_e, sen_df_e, assm_df_e) %>%
#   select(-type_id, -test_id, -cdscode)


####### Legislative Districts Prep From School Data #######
##Step 1: Pull xwalks for district level aggregation & Join to data
xwalk_school_sen <- dbGetQuery(con, paste0("SELECT cdscode as geoid, ca_senate_district FROM crosswalks.cde_school_leg_districts_2022_23")) %>%
  mutate(leg_id = paste0('060', ca_senate_district),
         geolevel = 'sldu')

xwalk_school_assm <- dbGetQuery(con, paste0("SELECT cdscode as geoid, ca_assembly_district FROM crosswalks.cde_school_leg_districts_2022_23")) %>%
  mutate(leg_id = paste0('060', ca_assembly_district),
         geolevel = 'sldl')

df_subset_leg <- schools_final_e %>% select(-geolevel, -type_id) %>%
  left_join(xwalk_school_sen, by = c("cdscode"="geoid")) %>% 
  left_join(xwalk_school_assm, by = c("cdscode"="geoid"))
colnames(df_subset_leg) <- gsub(".x", "_sen", colnames(df_subset_leg))
df_subset_leg <- df_subset_leg %>% rename(leg_id_assm = leg_id.y, geolevel_assm = geolevel.y)

# Check schools-leg dist matches, x = sen, y = assm
no_leg_id_sen <- df_subset_leg %>% 
  filter(is.na(leg_id_sen)) %>% 
  filter(!is.na(total_rate))
length(unique(no_leg_id_sen$cdscode))  # 155
no_leg_id_assm <- df_subset_leg %>% 
  filter(is.na(leg_id_assm)) %>% 
  filter(!is.na(total_rate))
length(unique(no_leg_id_assm$cdscode))  # 155

con_shared <- connect_to_db("rda_shared_data")
open_schools <- st_read(con_shared, query = paste0("SELECT cdscode, statustype, geom_3310 FROM education.cde_public_schools_", curr_yr, "_emg"))
no_leg_id <- open_schools %>% right_join(no_leg_id_sen) %>% relocate(statustype, .before = cdscode) %>% 
  select(cdscode)

## get Leg Dist shapes, manually join unmatched schools to Leg Dist
sen_shp <- st_read(con_shared, query = "SELECT * FROM geographies_ca.cb_2023_06_sldu_500k") 
assm_shp <- st_read(con_shared, query = "SELECT * FROM geographies_ca.cb_2023_06_sldl_500k")
sch_sen_int <- st_join(no_leg_id, sen_shp) %>% rename(leg_id_sen = geoid) %>% 
  mutate(ca_senate_district = str_sub(leg_id_sen, -2), geolevel_sen = "sldu") %>%
  select(cdscode, leg_id_sen, ca_senate_district, geolevel_sen) %>%
  st_drop_geometry()
sch_assm_int <- st_join(no_leg_id, assm_shp) %>% rename(leg_id_assm = geoid) %>% 
  mutate(ca_assembly_district = str_sub(leg_id_assm, -2), geolevel_assm = "sldl") %>%
  select(cdscode, leg_id_assm, ca_assembly_district, geolevel_assm) %>%
  st_drop_geometry()

## join Leg Dist info back to df_subset_leg before calcs
df_subset_leg_e <- rows_update(df_subset_leg, sch_sen_int)
df_subset_leg_e <- rows_update(df_subset_leg_e, sch_assm_int)


##Step 2: Calc & screen weighted Leg Dist data from school dist data
# split into senate and assembly
df_subset_senate <- df_subset_leg_e %>%
  filter(!is.na(geolevel_sen)) %>%
  rename(geolevel = geolevel_sen, final_geoid = leg_id_sen) %>%
  mutate(geoname=paste("State Senate District", ca_senate_district))%>%
# select needed cols
  select(final_geoid, geoname, geolevel, cdscode, ends_with("_pop"), ends_with("_raw"), ends_with("_rate")) #%>%

df_subset_senate <- calc_leg_elamath(df_subset_senate, threshold)

df_subset_assm <- df_subset_leg_e %>%
  filter(!is.na(geolevel_assm)) %>%
  rename(geolevel = geolevel_assm, final_geoid = leg_id_assm) %>%
  mutate(geoname=paste("State Assembly District", ca_assembly_district))%>%
  # select needed cols
  select(final_geoid, geoname, geolevel, cdscode, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"))

df_subset_assm <- calc_leg_elamath(df_subset_assm, threshold)

df_subset_leg_e <- rbind(df_subset_senate, df_subset_assm)
  # add cdscode and countyname (needed for school districts) - to bind all geos later
  #mutate(cdscode=NA,
  #       countyname=NA)


####### Combine all geolevels (county, state, school district, leg district) for RC Calcs ##### ---------------------------------------------------------------------
df_join_e_v2 <- plyr::rbind.fill(df_subset_leg_e, df_final_e) %>%
  # remove records without fips codes or "No Data"
  filter(!is.na(geoid) & geoid != "No Data") %>%
  # apply population threshold
  mutate(raw = ifelse(raw < threshold, NA, raw),
         rate = ifelse(raw < threshold, NA, rate)) %>%
  # pivot to get into RC table format
  pivot_wider(names_from = race, 
              values_from = c(raw, pop, rate),
              names_glue = "{race}_{.value}") %>%
  relocate(geoid, geoname, cdscode, geolevel) %>%
  distinct()


####### ELA: CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

#d <- df_join_e_v1   # set sch dist-based ela df as d
d <- df_join_e_v2   # set school-based ela df as d

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d)    #calculate number of "_rate" values
d <- calc_best(d)       #calculate best rates -- be sure to update $asbest line of code accordingly before running this function.
d <- calc_diff(d)       #calculate difference from best
d <- calc_avg_diff(d)   #calculate (row wise) mean difference from best
d <- calc_p_var(d)      #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d)         #calculate index of disparity
View(d)

#split STATE into separate table
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid") %>%
  select(-cdscode, -type_id)
View(state_table)

#split COUNTY into separate table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>%
  select(-cdscode, -type_id)
View(county_table)

#split CITY into separate table
city_table <- d[d$geolevel == 'district', ]

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname")
View(city_table)

#split LEGISLATIVE DISTRICTS into separate table 
upper_leg_table <- d[d$geolevel == 'sldu', ]
lower_leg_table <- d[d$geolevel == 'sldl', ]

#calculate LEGISLATIVE DISTRICTS z-scores and bind
upper_leg_table <- calc_z(upper_leg_table)
upper_leg_table <- calc_ranks(upper_leg_table)
upper_leg_table <- upper_leg_table
#View(upper_leg_table)

lower_leg_table <- calc_z(lower_leg_table)
lower_leg_table <- calc_ranks(lower_leg_table)
lower_leg_table <- lower_leg_table
#View(lower_leg_table)

leg_table <- rbind(upper_leg_table, lower_leg_table) %>% dplyr::rename("leg_id" = "geoid", "leg_name" = "geoname") %>%
  select(-cdscode, -type_id)

###update info for postgres tables###
test <- "ela"
county_table_name <- paste0("arei_educ_gr3_",test,"_scores_county_",yr)
state_table_name <- paste0("arei_educ_gr3_",test,"_scores_state_",yr)
city_table_name <- paste0("arei_educ_gr3_",test,"_scores_district_",yr)
leg_table_name <- paste0("arei_educ_gr3_",test,"_scores_leg_",yr)

indicator <- "Students scoring proficient or better on 3rd grade English Language Arts (%)"
source <- paste0("CAASPP ", curr_yr, " ", dwnld_url, ". QA doc: ", qa_filepath)

#send tables to postgres
#to_postgres(county_table,state_table)
#city_to_postgres()
#leg_to_postgres(leg_table)


##### MATH: PREP FOR RC FUNCTIONS #######
# set functions source
source("./Functions/RC_ELA_Math_Functions.R")
# define test_id as "01" for ELA or "02" for Math
test_id <- "02" # Math

df_final_m <- clean_ela_math(caaspp_df, test_id)

# pivot to wide format, ensure correct col names for RC functions
df_final_m_ <- df_final_m %>% pivot_wider(names_from = race, names_glue = "{race}_{.value}", values_from = c(pop, raw, rate)) 
schools_final_m <- df_final_m_ %>% filter(geolevel == 'school')  # create school-level only df
df_final_m <- df_final_m %>% filter(geolevel != 'school')        # drop school-level rows, keep sch dist/county/state rows

# ####### Legislative Districts Prep From School District Data #######
# ##Step 1: Prep school district data for Leg Dist calcs
# leg_schdist_df <- clean_leg_elamath(caaspp_df)
# 
# 
# ##Step 2: Pull xwalks for district level aggregation
# #Elementary School Districts 
# xwalk_esd_sen <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.esd_2020_state_senate_2024"))
# xwalk_esd_assm <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.esd_2020_state_assembly_2024"))
# 
# #Unified School Districts
# xwalk_usd_sen <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.usd_2020_state_senate_2024"))
# xwalk_usd_assm <- dbGetQuery(con, paste0("SELECT * FROM crosswalks.usd_2020_state_assembly_2024"))
# 
# xwalk_sen <- rbind(xwalk_esd_sen, xwalk_usd_sen) %>%
#   rename(geoid = geo_id,
#          geoname = geo_name,
#          leg_id = sldu24)  %>%
#   mutate(geolevel = 'sldu')
# 
# xwalk_assm <- rbind(xwalk_esd_assm, xwalk_usd_assm) %>%
#   rename(geoid = geo_id,
#          geoname = geo_name,
#          leg_id = sldl24) %>%
#   mutate(geolevel = 'sldl')
# 
# 
# ##Step 3: Calc & screen weighted Leg Dist data from school dist data
# sen_df_m <- calc_leg_elamath_sd(leg_schdist_df, xwalk_sen, threshold) %>% 
#   filter(test_id == test) %>%
#   mutate(geoname = paste0("State Senate District ", #adding geoname
#                           as.numeric(str_sub(geoid, -2))))
# assm_df_m <- calc_leg_elamath_sd(leg_schdist_df, xwalk_assm, threshold) %>% 
#   filter(test_id == test) %>%
#   mutate(geoname = paste0("State Assembly District ", #adding geoname
#                           as.numeric(str_sub(geoid, -2))))
# df_join_m_v1 <- bind_rows(df_final_m, sen_df_m, assm_df_m) %>%
#   select(-type_id, -test_id, -cdscode)

####### Legislative Districts Prep From School Data #######
##Step 1: Join to xwalks for district level aggregation
df_subset_leg <- schools_final_m %>% select(-geolevel, -type_id) %>%
  left_join(xwalk_school_sen, by = c("cdscode"="geoid")) %>% 
  left_join(xwalk_school_assm, by = c("cdscode"="geoid"))
colnames(df_subset_leg) <- gsub(".x", "_sen", colnames(df_subset_leg))
df_subset_leg <- df_subset_leg %>% rename(leg_id_assm = leg_id.y, geolevel_assm = geolevel.y)

## join Leg Dist info back to df_subset_leg before calcs - manual matching done in ELA section
df_subset_leg_m <- rows_update(df_subset_leg, sch_sen_int)
df_subset_leg_m <- rows_update(df_subset_leg_m, sch_assm_int)


##Step 2: Calc & screen weighted Leg Dist data from school dist data
df_subset_senate <- df_subset_leg_m %>%
  filter(!is.na(geolevel_sen)) %>%
  rename(geolevel = geolevel_sen, final_geoid = leg_id_sen) %>%
  mutate(geoname=paste("State Senate District", ca_senate_district))%>%
  # select needed cols
  select(final_geoid, geoname, geolevel, cdscode, ends_with("_pop"), ends_with("_raw"), ends_with("_rate")) #%>%

df_subset_senate <- calc_leg_elamath(df_subset_senate, threshold)

df_subset_assm <- df_subset_leg_m %>%
  filter(!is.na(geolevel_assm)) %>%
  rename(geolevel = geolevel_assm, final_geoid = leg_id_assm) %>%
  mutate(geoname=paste("State Assembly District", ca_assembly_district))%>%
  # select needed cols
  select(final_geoid, geoname, geolevel, cdscode, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"))

df_subset_assm <- calc_leg_elamath(df_subset_assm, threshold)

df_subset_leg_m <- rbind(df_subset_senate, df_subset_assm)

####### Combine all geolevels (county, state, school district, leg district) for RC Calcs ##### ---------------------------------------------------------------------
df_join_m_v2 <- plyr::rbind.fill(df_subset_leg_m, df_final_m) %>%
  # remove records without fips codes or "No Data"
  filter(!is.na(geoid) & geoid != "No Data") %>%
  # apply population threshold
  mutate(raw = ifelse(raw < threshold, NA, raw),
         rate = ifelse(raw < threshold, NA, rate)) %>%
  # pivot to get into RC table format
  pivot_wider(names_from = race, 
              values_from = c(raw, pop, rate),
              names_glue = "{race}_{.value}") %>%
  relocate(geoid, geoname, cdscode, geolevel) %>%
  distinct()

####### MATH: CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

#d <- df_join_m_v1 # set school district-based df as d
d <- df_join_m_v2 # set school-based df as d

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d)    #calculate number of "_rate" values
d <- calc_best(d)       #calculate best rates -- be sure to update $asbest line of code accordingly before running this function.
d <- calc_diff(d)       #calculate difference from best
d <- calc_avg_diff(d)   #calculate (row wise) mean difference from best
d <- calc_p_var(d)      #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d)         #calculate index of disparity
View(d)

#split STATE into separate table
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid") %>%
  select(-cdscode, -type_id)
View(state_table)

#split COUNTY into separate table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid") %>%
  select(-cdscode, -type_id)
View(county_table)

#split CITY into separate table
city_table <- d[d$geolevel == 'district', ]

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname")
View(city_table)

#split LEGISLATIVE DISTRICTS into separate table 
upper_leg_table <- d[d$geolevel == 'sldu', ]
lower_leg_table <- d[d$geolevel == 'sldl', ]

#calculate LEGISLATIVE DISTRICTS z-scores and bind
upper_leg_table <- calc_z(upper_leg_table)
upper_leg_table <- calc_ranks(upper_leg_table)
upper_leg_table <- upper_leg_table
#View(upper_leg_table)

lower_leg_table <- calc_z(lower_leg_table)
lower_leg_table <- calc_ranks(lower_leg_table)
lower_leg_table <- lower_leg_table
#View(lower_leg_table)

leg_table <- rbind(upper_leg_table, lower_leg_table) %>% dplyr::rename("leg_id" = "geoid", "leg_name" = "geoname") %>%
  select(-cdscode, -type_id)

###update info for postgres tables###
test <- "math"
county_table_name <- paste0("arei_educ_gr3_",test,"_scores_county_",yr)
state_table_name <- paste0("arei_educ_gr3_",test,"_scores_state_",yr)
city_table_name <- paste0("arei_educ_gr3_",test,"_scores_district_",yr)
leg_table_name <- paste0("arei_educ_gr3_",test,"_scores_leg_",yr)

indicator <- paste0("Created on ", Sys.Date(), ". Students scoring proficient or better on 3rd grade Math (%)")
source <- paste0("CAASPP ", curr_yr, " ", dwnld_url, ". QA doc: ", qa_filepath)

#send tables to postgres
#to_postgres(county_table,state_table)
#city_to_postgres()
#leg_to_postgres(leg_table) 

#dbDisconnect(con)