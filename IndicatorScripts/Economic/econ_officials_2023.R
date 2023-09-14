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


# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")


## Exploring EEOC 2014-18 Place Data csv file ----
### NOTE: Table EEOALL4R is NOT available thru Census API, though other EEOC tables are.
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
# 		## ESTIMATE_6 = Non-Latinx Asian
# 		## ESTIMATE_7 = Non-Latinx PI
# 		## ESTIMATE_8 = Remainder Non-Latinx Pop -- NOTE: We will not use this category bc it is hard to say what it corresponds to. It's sort of a combo Non-Latinx Two+ Races and Non-Latinx Other Race...
# ## UNIVERSE -- See W:\Project\RACE COUNTS\2023_v5\Economic\EEOTabulation2014-2018-Documentation-1.31.2022.xlsx > Universe tab for info on that. The Universe for this table is "Civilian labor force 16 years and over".
# 
# step2 <- step2 %>% dplyr::rename(
#   "Total" = "ESTIMATE_1",
#   "Latinx, Any Race" = "ESTIMATE_2",
#   "Non-Latinx White Alone" = "ESTIMATE_3",
#   "Non-Latinx Black Alone" = "ESTIMATE_4",
#   "Non-Latinx AIAN Alone" = "ESTIMATE_5",
#   "Non-Latinx Asian" = "ESTIMATE_6",
#   "Non-Latinx PI" = "ESTIMATE_7",
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
# table_comment_source <- "officials & Managers"
# table_source <- "The data is from ACS EEO (2014-2018), https://www.census.gov/acs/www/data/eeo-data/."
# dbWriteTable(con2, c(table_schema, table_name), step3, overwrite = TRUE, row.names = FALSE)

########## City Calcs #########
officials <- dbGetQuery(con, "SELECT * FROM economic.acs_eeo_2014_18") %>% select(-c(ends_with("_Percent")))
# View(officials)

#### Please note: the documentation says that its using not hispanic or Latino race alone variables
officials <- officials %>% dplyr::rename("geoid" = "GEOID", "geoname" = "GEONAME", 
                                         "Total_raw" = "Total_Number",
                                         "Latino_raw" = "Latinx, Any Race_Number",
                                         "White_raw" = "Non-Latinx White Alone_Number",
                                         "Black_raw" = "Non-Latinx Black Alone_Number",
                                         "AIAN_raw" = "Non-Latinx AIAN Alone_Number",
                                         "Asian_raw" = "Non-Latinx Asian_Number",
                                         "NHPI_raw" = "Non-Latinx PI_Number",
                                         "Other_raw" = "Remainder Non-Latinx Pop_Number",
                                         
                                         "Total_moe" = "MG_ERROR_Total_Number", 
                                         "Latino_moe" = "MG_ERROR_Latinx, Any Race_Number",
                                         "White_moe" = "MG_ERROR_NH_White_Number",
                                         "Black_moe" = "MG_ERROR_NH_Black_Number",
                                         "AIAN_moe" = "MG_ERROR_NH_AIAN_Number",
                                         "Asian_moe" = "MG_ERROR_NH_Asian_Number",
                                         "NHPI_moe" = "MG_ERROR_NH_PI_Number", 
                                         "Other_moe" = "MG_ERROR_NH_Remainder_Number")
officials_1 <- officials %>% select(-c(ends_with("_moe"))) %>% pivot_longer(cols=c(ends_with("_raw")), names_to="raceeth", values_to='raw')
officials_1$raceeth <- gsub("_raw", "", officials_1$raceeth)

officials_2 <- officials %>% select(-c(ends_with("_raw"))) %>% pivot_longer(cols=c(ends_with("_moe")), names_to="raceeth", values_to='raw_moe')
officials_2$raceeth <- gsub("_moe", "", officials_2$raceeth)
officials_2$raw_moe <- gsub("[^[:alnum:] ]", "", officials_2$raw_moe)

officials_mngrs <- full_join(officials_1, officials_2, by=c("geoid", "geoname", "raceeth"))
View(officials_mngrs)

### Download place data for labor force (used as the pop values) ------
# Please Note: I downloaded the data again b/c economics.acs_5yr_2301_multigeo_2021 in rda_shared does not contain labor force data (data that starts with S2301_C02)
#Get population estimates from Census API
#check race variables
# v18 <- load_variables(2018, "acs5/subject", cache = TRUE)
# View(v18)

# race variables all INCLUDE Hispanic except for White which is non-hispanic

cities <- get_acs(geography = "place",
                  variables = c("S2301_C02_001", "S2301_C02_014", "S2301_C02_013","S2301_C02_019", #,"S2301_C02_012" removed white and just used nh_white
                                "S2301_C02_018", "S2301_C02_017","S2301_C02_016","S2301_C02_015", "S2301_C02_020"), 
                  state = "CA",
                  survey = "acs5",
                  year = 2018, cache_table = TRUE)
##### start cleaning the data ----
cities$NAME <- gsub(" City, California", "", cities$NAME)
cities$NAME <- gsub(" city, California", "", cities$NAME)
cities$NAME <- gsub(" town, California", "", cities$NAME)
cities$NAME <- gsub(" CDP, California", "", cities$NAME)

# cities$variable <- gsub("B01001H_001", "NH_White", cities$variable)
cities$variable <- gsub("S2301_C02_001", "Total", cities$variable) 
# cities$variable <- gsub("S2301_C02_012", "White", cities$variable) #this is white including Hispanic
cities$variable <- gsub("S2301_C02_019", "Latino", cities$variable)
cities$variable <- gsub("S2301_C02_013",  "Black", cities$variable)
cities$variable <- gsub("S2301_C02_014", "AIAN", cities$variable)
cities$variable <- gsub("S2301_C02_015", "Asian", cities$variable)
cities$variable <- gsub("S2301_C02_016", "NHPI", cities$variable)
cities$variable <- gsub("S2301_C02_017", "Other", cities$variable)
cities$variable <- gsub("S2301_C02_018", "Two or More Races", cities$variable)
cities$variable <- gsub("S2301_C02_020", "White", cities$variable) #white used for the calcuation is going to be nh_white


cities <- cities %>% dplyr::rename("geoid" = "GEOID", "geoname" = "NAME", "raceeth" = "variable", "pop" = "estimate", "pop_moe" = "moe")
View(cities)

#combine cities and officials df to calculate rate and rate moe ----

#set population base
pop_base <- 1000

# set thresholds     #methodology only said that it was screened for low reliability but not what number so I might remove this screening

cv_threshold <- 35
pop_threshold <- 150

df <- left_join(officials_mngrs, cities, by=c("geoid", "geoname", "raceeth")) 

# convert to numeric to make sure that it can be used in calculations
df$raw <- as.numeric(df$raw)
df$raw_moe <- as.numeric(df$raw_moe)

df <- df %>% group_by(geoid, geoname, raceeth) %>%
  mutate(rate = ((raw/pop) * pop_base)) 

df <- df %>% 
  mutate(rate_moe = ifelse(is.na(pop_moe), NA, ifelse(df$raw_moe^2 - (df$raw / df$pop)^2 * df$pop_moe^2 < 0, #removed this part  | is.na(raw_moe)
                                                      (df$raw_moe^2 + (df$raw/df$pop)^2 * df$pop_moe^2)^(1/2)/ df$pop * 100,
                                                      (df$raw_moe^2 - (df$raw / df$pop)^2 * df$pop_moe^2)^(1/2) / df$pop * 100)))

df <- df %>% 
  mutate(rate_cv = ifelse(rate==0, NA, rate_moe/1.645/rate*100)) # calculate the coefficient of variation for the rate
View(df)

############## CV CALCS AND EXPORT TO RDA_SHARED_DATA ##############

### calc cv's
## Calculate CV values for all rates - store in columns as cv_[race]_rate

#Screen data: Convert rate to NA if its greater than the cv_threshold or less than the pop_threshold
df_screened <- df %>% 
  mutate(rate = ifelse(rate_cv > cv_threshold, NA, ifelse(pop < pop_threshold, NA, rate)),
         raw = ifelse(rate_cv > cv_threshold, NA, ifelse(pop < pop_threshold, NA, raw))) 

df_wide <- df_screened %>% ungroup() %>% 
  pivot_wider(names_from = raceeth, values_from = c(raw, pop, rate, raw_moe, pop_moe, rate_moe, rate_cv), names_glue = "{raceeth}_{.value}")

d <- select(df_wide, geoid, name, geolevel, ends_with("_pop"), ends_with("_raw"), ends_with("_rate"), everything(), -ends_with("_moe"))

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
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- rename(county_table, "county_id" = "geoid", "county_name" = "geoname")
View(county_table)

#split CITY into separate table and format id, name columns
city_table <- d[d$geolevel == 'city', ] %>% select(-c(geolevel))

#calculate CITY z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname") 
View(city_table)

###update info for postgres tables###
county_table_name <- "arei_econ_officials_county_2023"
state_table_name <- "arei_econ_officials_state_2023"
city_table_name <- "arei_econ_officials_city_2023"
indicator <- "Number of Officials & Managers per 1k People by Race. Only people ages 18-64 who are in the labor force are included. We also screened by pop and CV. White, Black, Asian, Other are one race alone and Latinx-exclusive. Two or More is Latinx-exclusive. AIAN and NHPI are Latinx-inclusive so they are also included in Latinx counts. AIAN and NHPI include AIAN and NHPI Alone and in Combo, so non-Latinx AIAN and NHPI in combo are also included in Two or More. This data is"
source <- "ACS EEO (2014-2018)"
rc_schema <- "v5"


#send tables to postgres
to_postgres()