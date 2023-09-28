### Officials & Managers RC v5 ###
# install.packages(c("httr", "jsonlite"))

#Load libraries
library(data.table)
library(stringr)
library(dplyr)
library(RPostgreSQL)
library(dbplyr)
library(srvyr)
library(tidycensus)
library(tidyr)
library(rpostgis)
library(tidyr)
library(here)
library(sf)
library(readxl)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

##### County Calculations ####
##### GET PUMA-COUNTYCROSSWALK ######
crosswalk <- st_read(con, query = "select county_id AS geoid, county_name AS geoname, puma, num_county from crosswalks.puma_county_2020")


# Get PUMS Data -----------------------------------------------------------
# path where my data lives (not pulling pums data from the postgres db, takes too long to run calcs that way) 
root <- "W:/Data/Demographics/PUMS/"

# Load the people PUMS data
ppl <- fread(paste0(root, "CA_2016_2020/psam_p06.csv"), header = TRUE, data.table = FALSE, 
             colClasses = list(character = c("PUMA", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH", "SOCP", "ESR", "AGEP")))



# For this project we only wanted data for people in labor market - ages between 18 and 64
ppl <- ppl[ppl$AGEP >= 18 & ppl$AGEP <= 64 , ]

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with crosswalks.puma_county_2020
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid, ppl$PUMA)

# create list of replicate weights
repwlist = rep(paste0("PWGTP", 1:80))

# join county crosswalk using left join function
ppl <- left_join(ppl, crosswalk, by=c("puma_id" = "puma"))    # specify the field join

############## Data Dictionary: W:\Data\Demographics\PUMS\CA_2016_2020\PUMS_Data_Dictionary_2016-2020.pdf ###############

##### Reclassify Race/Ethnicity ########
# check how many records there are for RACAIAN (AIAN alone/combo) versus RAC1P (AIAN alone) and same for NHPI
#View(subset(ppl, RACAIAN =="1"))
#View(subset(ppl, RAC1P >= 3 & ppl$RAC1P <=5))
#View(subset(ppl, RACNH =="1" | RACPI =="1"))
#View(subset(ppl, RAC1P == 7))

source("W:/RDA Team/R/Functions/PUMS_Functions.R")
ppl <- race_reclass(ppl)

# review data 
# table(ppl$race, useNA = "always")
# table(ppl$race, ppl$latino, useNA = "always")
# table(ppl$race, ppl$aian, useNA = "always")
# table(ppl$race, ppl$pacisl, useNA = "always")
# #table(ppl$aian, useNA = "always")
# #table(ppl$pacisl, useNA = "always")

##### Define Officials and Managers ###########
## Tag people who are officials or managers: SOCP value starts with "11" 
###### See p107 in W:\Data\Demographics\PUMS\CA_2016_2020\PUMS_Data_Dictionary_2016-2020.pdf
ppl$offmgr <- 
  case_when(
    grepl('^11', ppl$SOCP) ~ as.integer(1),
    TRUE ~ as.integer(0))

# review
summary(ppl$offmgr)

## Code for labor force status: ESR on p53 of PUMS_Data_Dictionary_2016-2020.pdf
table(ppl$ESR, useNA = "always")

# NOTE: 'This includes 4-Armed forces, at work' and '5-Armed forces, with a job but not at work'. It excludes '6-Not in labor force'.
ppl$emply <- as.factor(ifelse(ppl$ESR %in% c(1, 2, 3, 4, 5), "in labor force", "not in labor force")) 
ppl <- filter(ppl, emply=='in labor force')

## Factor for Officials & Managers
ppl$indicator <- as.factor(ifelse(ppl$offmgr == 1, "officials", "not official"))

## check race and officials results
table(ppl$race, useNA ="always")
table(ppl$indicator, useNA = "always")

############### CALC COUNTY AND STATE ESTIMATES/CVS ETC. ############### 
# Define indicator and weight variables for function
key_indicator <- 'officials'  # update this to the indicator you are working with
# You must use to WGTP (if you are using psam_h06.csv and want housing units, like for Low Quality Housing) or PWGTP (if you want person units, like for Connected Youth)
weight <- 'PWGTP'           
# You must specify the population base you want to use for the rate calc. Ex. 100 for percents, or 1000 for rate per 1k.
pop_base <- 1000

rc_state <- state_pums(ppl)
View(rc_state)

rc_county <- county_pums(ppl)
View(rc_county)


############ COMBINE & SCREEN COUNTY/STATE DATA ############# 
# Define threshold variables for function
cv_threshold <- 20          # threshold and CV must be displayed as a percentage (not decimal)
raw_rate_threshold <- 0
pop_threshold <- 400

screened <- pums_screen(rc_state, rc_county) 
View(screened)

d_county_state <- screened %>% mutate(geolevel = ifelse(geoname=="California","State", "County"))


# ##### City Calculations #####
# Exploring EEOC 2014-18 Place Data csv file ----
## NOTE: Table EEOALL4R is NOT available thru Census API, though other EEOC tables are.
# csv <- read.csv('https://www2.census.gov/EEO_2014_2018/EEO_Tables_By_Geographic_Area_By_State/State_Place/California/EEOALL4R_160_CA.csv') # pull in data from Census FTP
# 
# # temp <- unique(csv[ , c("PROFLN", "TITLE")])
# step1 <- filter(csv, PROFLN < 6.1) # filter for rows re: Officials and Managers
# 
# step2 <- filter(step1, PROFLN == 1 | PROFLN == 2) %>% select(-c(TBLID, PROFLN)) # filter for rows re: raw/rate for male+female, drop unneeded columns
# # It seems like numbers on 'Estimate' columns, would align with var names in other related EEO tables like https://api.census.gov/data/2018/acs/acs5/eeo/groups/EEOALL6R.html and https://api.census.gov/data/2018/acs/acs5/eeo/groups/EEOALL1R.html.
# ## For example, EEOALL6R_C02_006E is "Estimate!!Hispanic or Latino!!...", so "ESTIMATE_2" in our table is likely also Latinx.
# ## I confirmed the values/col names in EEOALL4R against https://www.census.gov/acs/www/data/eeo-data/eeo-tables-2018/tableview.php?geotype=place&place=16000us0600562&filetype=all4r&geoName=Alameda%20city,%20California , and it matches up as follows:
# 		## ESTIMATE_1 = Total
# 		## ESTIMATE_2 = Latinx, Any Race
# 		## ESTIMATE_3 = Non-Latinx White Alone
# 		## ESTIMATE_4 = Non-Latinx Black Alone
# 		## ESTIMATE_5 = Non-Latinx AIAN Alone
# 		## ESTIMATE_6 = Non-Latinx Asian Alone
# 		## ESTIMATE_7 = Non-Latinx PI Alone
# 		## ESTIMATE_8 = Remainder Non-Latinx Pop -- NOTE: We will not use this category bc it is hard to say what it corresponds to. It's sort of a combo Non-Latinx Two+ Races and Non-Latinx Other Race...
# ## UNIVERSE -- See W:\Project\RACE COUNTS\2023_v5\Economic\EEOTabulation2014-2018-Documentation-1.31.2022.xlsx > Universe tab for info on that. The Universe for this table is "Civilian labor force 16 years and over".
# 
# step2 <- step2 %>% dplyr::rename(
#   "Total" = "ESTIMATE_1",
#   "Latinx, Any Race" = "ESTIMATE_2",
#   "Non-Latinx White Alone" = "ESTIMATE_3",
#   "Non-Latinx Black Alone" = "ESTIMATE_4",
#   "Non-Latinx AIAN Alone" = "ESTIMATE_5",
#   "Non-Latinx Asian Alone" = "ESTIMATE_6",
#   "Non-Latinx PI Alone" = "ESTIMATE_7",
#   "Remainder Non-Latinx Pop" = "ESTIMATE_8",
# 
# 
#   "MG_ERROR_Total" = "MG_ERROR_1",
#   "MG_ERROR_Latinx, Any Race" = "MG_ERROR_2",
#   "MG_ERROR_NH_White" = "MG_ERROR_3",
#   "MG_ERROR_NH_Black" = "MG_ERROR_4",
#   "MG_ERROR_NH_AIAN" = "MG_ERROR_5",
#   "MG_ERROR_NH_Asian" = "MG_ERROR_6",
#   "MG_ERROR_NH_PI" = "MG_ERROR_7",
#   "MG_ERROR_NH_Remainder" = "MG_ERROR_8",
# )
# 
# step3 <- step2 %>% pivot_wider(id_cols = c(GEOID, GEONAME),
#               names_from = c(TITLE),
#               values_from = c("Total",
#   "Latinx, Any Race",
#   "Non-Latinx White Alone",
#   "Non-Latinx Black Alone",
#   "Non-Latinx AIAN Alone",
#   "Non-Latinx Asian",
#   "Non-Latinx PI",
#   "Remainder Non-Latinx Pop",
# 
# 
#   "MG_ERROR_Total",
#   "MG_ERROR_Latinx, Any Race",
#   "MG_ERROR_NH_White" ,
#   "MG_ERROR_NH_Black",
#   "MG_ERROR_NH_AIAN" ,
#   "MG_ERROR_NH_Asian",
#   "MG_ERROR_NH_PI",
#   "MG_ERROR_NH_Remainder",),
#               names_glue = "{.value}_{TITLE}") %>%
#               as.data.frame()
# 
# step3$GEOID <- gsub("16000US", "", step3$GEOID)
# step3$GEONAME <- gsub(" city, California", "", step3$GEONAME)
# step3$GEONAME <- gsub(" town, California", "", step3$GEONAME)
# step3$GEONAME <- gsub(" CDP, California", "", step3$GEONAME)
# step3$GEONAME <- gsub(" City, California", "", step3$GEONAME)
# # export officials to rda shared table ------------------------------------------------------------
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# 
# con2 <- connect_to_db("rda_shared_data")
# table_schema <- "economic"
# table_name <- "acs_eeo_2014_18"
# table_comment_source <- "ACS EEO (2014-2018)"
# table_source <- "This place level data is EEO 4r Job Categories by Sex, and Race/Ethnicity for Residence Geography, Total Population, downloaded from https://www2.census.gov/EEO_2014_2018/EEO_Tables_By_Geographic_Area_By_State/State_Place/California/ via FTP and selecting EEOALL4R_160_CA.csv"
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# dbWriteTable(con2, c(table_schema, table_name), step3, overwrite = TRUE, row.names = FALSE)
# dbSendQuery(conn = con2, table_comment)
# dbDisconnect(con2)
### Data dictionary "W:/Project/RACE COUNTS/2023_v5/Economic/EEOTabulation2014-2018 Documentation-1.31.2022.xlsx"

########## City Calcs #########
officials <- dbGetQuery(con, "SELECT * FROM economic.acs_eeo_2014_18") %>% select(-c(ends_with("_Percent")))
# View(officials)

#### Please note: the documentation says that its using not hispanic or Latino race alone variables
officials <- officials %>% dplyr::rename("geoid" = "GEOID", "geoname" = "GEONAME", 

                           "total_raw" = "Total_Number",
                           "latino_raw" = "Latinx, Any Race_Number",
                           "white_raw" = "Non-Latinx White Alone_Number",
                           "black_raw" = "Non-Latinx Black Alone_Number",
                           "aian_raw" = "Non-Latinx AIAN Alone_Number",
                           "asian_raw" = "Non-Latinx Asian_Number",
                           "pacisl_raw" = "Non-Latinx PI_Number",
                           "other_raw" = "Remainder Non-Latinx Pop_Number",
                           
                           "total_moe" = "MG_ERROR_Total_Number", 
                           "latino_moe" = "MG_ERROR_Latinx, Any Race_Number",
                           "white_moe" = "MG_ERROR_NH_White_Number",
                           "black_moe" = "MG_ERROR_NH_Black_Number",
                           "aian_moe" = "MG_ERROR_NH_AIAN_Number",
                           "asian_moe" = "MG_ERROR_NH_Asian_Number",
                           "pacisl_moe" = "MG_ERROR_NH_PI_Number", 
                           "other_moe" = "MG_ERROR_NH_Remainder_Number")

# pivot longer to simplify later calculations
officials_1 <- officials %>% select(-c(ends_with("_moe"))) %>% pivot_longer(cols=c(ends_with("_raw")), names_to="raceeth", values_to='raw')
officials_1$raceeth <- gsub("_raw", "", officials_1$raceeth)

officials_2 <- officials %>% select(-c(ends_with("_raw"))) %>% pivot_longer(cols=c(ends_with("_moe")), names_to="raceeth", values_to='raw_moe')
officials_2$raceeth <- gsub("_moe", "", officials_2$raceeth)
officials_2$raw_moe <- gsub("[^[:alnum:] ]", "", officials_2$raw_moe)

officials_mngrs <- full_join(officials_1, officials_2, by=c("geoid", "geoname", "raceeth"))
# View(officials_mngrs)

# convert to numeric to make sure that it can be used in calculations
officials_mngrs$raw <- gsub(",", "", officials_mngrs$raw)   #first get rid of the commas
officials_mngrs$raw_moe <- gsub(",", "", officials_mngrs$raw_moe)  

officials_mngrs$raw <- as.numeric(officials_mngrs$raw)
officials_mngrs$raw_moe <- as.numeric(officials_mngrs$raw_moe)

# export  data to rda shared table ------------------------------------------------------------
#since the tidycensus dataset does not have B08301 data by race, download it manually then upload it to pgadmin
### Download place data for labor force (used as the pop values) ------

# workers_data <- read.csv("W:/Project/RACE COUNTS/2023_v5/Economic/ACSDT5YSPT2021.B08301_2023-09-20T210308/ACSDT5YSPT2021.B08301-Data.csv") 
# names(workers_data) <- workers_data[1,]
# workers_data <- workers_data[-1,]
# 
# workers_data <- workers_data %>% select(Geography, `Geographic Area Name`, `Population Groups`, `Estimate!!Total:`, `Margin of Error!!Total:`)  
#
## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# source("W:\\RDA Team\\R\\credentials_source.R")
# con2 <- connect_to_db("rda_shared_data")
# table_schema <- "economic"
# table_name <- "acs_5yr_b08301_place_2021"
# table_comment_source <- "Means of Transportation for workers 16 years and over by race/ethnicity and census place"
# table_source <- "ACS 5 Year (2027-2021) https://data.census.gov/table?t=-0C:400:463&g=040XX00US06$1600000&y=2021&d=ACS+5-Year+Estimates+Selected+Population+Detailed+Tables&tid=ACSDT5YSPT2021.B08301"
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# 
# # send table and comment to postgres
# dbWriteTable(con2, c(table_schema, table_name), workers_data, overwrite = TRUE, row.names = FALSE) #, overwrite = FALSE, row.names = FALSE
# dbSendQuery(conn = con2, table_comment)
# dbDisconnect(con2)

###### Load in Denominator Variable -----
#read in data from 
cities <- st_read(con, query = "SELECT * FROM economic.acs_5yr_b08301_place_2021") %>% 
  ##### start cleaning the data ----
  rename("geoid" = "Geography", "geoname" = "Geographic Area Name", 
         "raceeth" = "Population Groups", 
         "pop" = "Estimate!!Total:", 
         "pop_moe" = "Margin of Error!!Total:")
dbDisconnect(con)

#total employment variables
cities$geoid <- str_sub(cities$geoid, 10) #remove the first characters from a string

#rename race variables
cities$raceeth <- gsub("Total population", "total", cities$raceeth) 
cities$raceeth <- gsub("Hispanic or Latino [[:punct:]]of any race[[:punct:]]", "latino", cities$raceeth) 
cities$raceeth <- gsub("Black or African American alone, not Hispanic or Latino",  "black", cities$raceeth)
cities$raceeth <- gsub("American Indian and Alaskan Native alone, not Hispanic or Latino", "aian", cities$raceeth)
cities$raceeth <- gsub("Asian alone, not Hispanic or Latino", "asian", cities$raceeth)
cities$raceeth <- gsub("Native Hawaiian and Other Pacific Islander alone, not Hispanic or Latino", "pacisl", cities$raceeth) #NHPI
cities$raceeth <- gsub("Some Other Race alone, not Hispanic or Latino", "other", cities$raceeth)
cities$raceeth <- gsub("White alone, not Hispanic or Latino", "white", cities$raceeth)

#remove other_race b/c numbers look weird for this one
cities <- cities %>% filter(raceeth!="other")

#standardize place naming convention
cities$geoname <- gsub("[[:punct:]].*","",cities$geoname) #remove anything after the punctuation mark 
cities$geoname <- gsub(" City", "", cities$geoname)
cities$geoname <- gsub(" city", "", cities$geoname)
cities$geoname <- gsub(" town", "", cities$geoname)
cities$geoname <- gsub(" CDP", "", cities$geoname)
# View(cities)

#make columns numeric
cities$pop <- as.numeric(cities$pop)
cities$pop_moe <- as.numeric(cities$pop_moe)

#change names back that got altered w/ punctuation gsub
cities$geoname <- gsub("Arden", "Arden-Arcade", cities$geoname)
cities$geoname <- gsub("Florence", "Florence-Graham", cities$geoname)


#combine cities and officials df to calculate rate and rate moe ----

#set population base
pop_base <- 1000

# set thresholds     #methodology only said that it was screened for low reliability but not what number so I might remove this screening
df <- left_join(officials_mngrs, cities, by=c("geoid", "geoname", "raceeth")) 

#calculate rate values 
df <- df %>% group_by(geoid, geoname, raceeth) %>%
  mutate(rate = ((raw/pop) * pop_base),
         rate_moe = moe_prop(raw, pop, raw_moe, pop_moe) * pop_base,
         rate_cv = ((rate_moe/1.645)/rate) * pop_base) # calculate the coefficient of variation for the rate

# View(df)

############## CV CALCS AND EXPORT TO RDA_SHARED_DATA ##############
# cv_threshold <- 40 #cv screening was taken out b/c it wasn't done in the previous city update, also the cv screening screened out all of the data.
pop_threshold <- 150
### calc cv's
## Calculate CV values for all rates - store in columns as cv_[race]_rate

#Screen data: Convert rate to NA if its greater than the cv_threshold or less than the pop_threshold
df_screened <- df %>% 
mutate(rate = ifelse(pop < pop_threshold, NA, rate),
       raw = ifelse(pop < pop_threshold, NA, raw))


df_wide <- df_screened %>% ungroup() %>% 
  pivot_wider(names_from = raceeth, values_from = c(raw, pop, rate, raw_moe, pop_moe, rate_moe, rate_cv), names_glue = "{raceeth}_{.value}") %>% 
select(geoid, geoname, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), ends_with("_cv"), everything(), -ends_with("pop_moe"), -ends_with("raw_moe")) %>% 
  mutate(geolevel = "City")

d <- full_join(d_county_state, df_wide, by=c("geoid", "geoname", "geolevel", "total_raw", "total_rate", "latino_raw", "latino_rate", 
                                         "pacisl_rate", "pacisl_raw", "aian_raw", "aian_rate"))

col_order <- c(  "geoid",                   "geoname",                 "geolevel",  
                 "total_raw",               "latino_raw",              "aian_raw",                
                 "pacisl_raw",              "black_raw",               "asian_raw",               
                 "other_raw",               "white_raw",               "nh_white_raw",            
                 "nh_black_raw",            "nh_asian_raw",            "nh_other_raw",            
                 "nh_twoormor_raw",    
                 
                 "total_rate",              "latino_rate",             "aian_rate",      
                 "pacisl_rate",             "black_rate",              "asian_rate", 
                 "other_rate",              "white_rate",              "nh_white_rate",           
                 "nh_black_rate",           "nh_asian_rate",           "nh_other_rate",           
                 "nh_twoormor_rate",                  
                          
                 "total_pop",               "latino_pop",              "aian_pop",             
                 "pacisl_pop",              "black_pop",                "asian_pop",              
                 "other_pop",               "white_pop",                  
                               
                 "total_rate_cv",           "latino_rate_cv",          "aian_rate_cv",      
                 "pacisl_rate_cv",          "black_rate_cv",           "asian_rate_cv",
                 "other_rate_cv",           "white_rate_cv",         
                 "total_rate_moe",          "latino_rate_moe",         "white_rate_moe",         
                 "black_rate_moe",          "aian_rate_moe",           "asian_rate_moe",         
                 "pacisl_rate_moe",         "other_rate_moe",           "num_nh_asian",           
                 "num_nh_black",            "num_nh_other",            "num_nh_twoormor",        
                 "num_nh_white",            "num_latino",              "num_aian",               
                 "num_pacisl",              "num_total",               "pop_nh_asian",           
                 "pop_nh_black",            "pop_nh_other",            "pop_nh_twoormor",        
                 "pop_nh_white",            "pop_latino",              "pop_aian",               
                 "pop_pacisl",              "pop_total",               "officials_rate_nh_asian",
                 "officials_rate_nh_black", "officials_rate_nh_other", "officials_rate_nh_twoormor",
                 "officials_rate_nh_white", "officials_rate_latino",   "officials_rate_aian",    
                 "officials_rate_pacisl",   "officials_rate_total",    "rate_moe_nh_asian",      
                 "rate_moe_nh_black",       "rate_moe_nh_other",       "rate_moe_nh_twoormor",   
                 "rate_moe_nh_white",       "rate_moe_latino",         "rate_moe_aian",          
                 "rate_moe_pacisl",         "rate_moe_total",          "rate_cv_nh_asian",       
                 "rate_cv_nh_black",        "rate_cv_nh_other",        "rate_cv_nh_twoormor",    
                 "rate_cv_nh_white",        "rate_cv_latino",          "rate_cv_aian",           
                 "rate_cv_pacisl",          "rate_cv_total")
d <- d[,col_order]

############## CALC RACE COUNTS STATS ##############

#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)


#split STATE into separate table and format id, name columns
state_table <- d[d$geolevel == 'State', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% 
  dplyr::rename("state_name" = "geoname", "state_id" = "geoid") %>% 
  select(-c(ends_with("rate_moe"), ends_with("rate_cv"), ends_with("pop"), 
            white_raw, black_raw, asian_raw, other_raw, 
            white_rate, black_rate, asian_rate, other_rate, 
            white_diff, black_diff, asian_diff, other_diff,
            white_disparity_z, black_disparity_z, asian_disparity_z, other_disparity_z, geolevel))
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'County', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")%>% 
  select(-c(ends_with("rate_moe"), ends_with("rate_cv"), ends_with("pop"), 
            white_raw, black_raw, asian_raw, other_raw, 
            white_rate, black_rate, asian_rate, other_rate, 
            white_diff, black_diff, asian_diff, other_diff,
            white_disparity_z, black_disparity_z, asian_disparity_z, other_disparity_z, 
            white_performance_z, black_performance_z, asian_performance_z, other_performance_z,geolevel))
View(county_table)


#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'City', ]

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") %>% 
  select(-c(starts_with("nh_"), starts_with("num_"), starts_with("pop_"), starts_with("rate_"), starts_with("officials_"), starts_with("other_"), geolevel))
View(city_table)

###update info for postgres tables###
county_table_name <- "arei_econ_officials_county_2023"
state_table_name <- "arei_econ_officials_state_2023"
city_table_name <- "arei_econ_officials_city_2023"
indicator <- "Number of Officials & Managers per 1k People by Race. Only people ages 18-64 who are in the labor force are included. We also screened by pop and CV. White, Black, Asian, Other are one race alone and Latinx-exclusive. Two or More is Latinx-exclusive. AIAN and NHPI are Latinx-inclusive so they are also included in Latinx counts. AIAN and NHPI include AIAN and NHPI Alone and in Combo, so non-Latinx AIAN and NHPI in combo are also included in Two or More. This data is"
source <- "ACS EEO (2014-2018), https://www.census.gov/acs/www/data/eeo-data/"
rc_schema <- "v5"


#send tables to postgres
# to_postgres()
# city_to_postgres()

