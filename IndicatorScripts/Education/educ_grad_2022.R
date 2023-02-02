#install packages if not already installed
list.of.packages <- c("readr", "DBI", "tidyr","dplyr","RPostgreSQL","tidycensus")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(readr)
library(dplyr)
library(tidyr)
library(RPostgreSQL)
library(tidycensus)
# setwd("W:/Project/RACE COUNTS/2022_v4/Education/R/")

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

############### PREP DATA ########################

#Get HS Grad, handle nas, ensure DistrictCode reads in right
# Data Dictionary: https://www.cde.ca.gov/ds/ad/filesacgr.asp
filepath = "https://www3.cde.ca.gov/demo-downloads/acgr/cohort2021.txt"
df <- read_delim(file = filepath, delim = "\t", na = c("*", ""),
                  col_types = cols(DistrictCode = col_character()))

#remove special characters from names
names(df) <- gsub("\\(", "", names(df))
names(df) <- gsub("\\)", "", names(df))
names(df) <- gsub(" ", "", names(df))

#filter for county and state rows, all types of schools, and racial categories
df_subset <- df %>% filter(AggregateLevel %in% c("C", "T") & CharterSchool == "All" & DASS == "All" & 
                             ReportingCategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) %>%
  
  #select just fields we need
  select(CountyName, ReportingCategory, CohortStudents, RegularHSDiplomaGraduatesCount, RegularHSDiplomaGraduatesRate)

#format for column headers
df_subset <- rename(df_subset, 
                    raw = "RegularHSDiplomaGraduatesCount",
                    pop = "CohortStudents",
                    rate = "RegularHSDiplomaGraduatesRate")

df_subset$ReportingCategory <- gsub("TA", "total", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RB", "nh_black", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RI", "nh_aian", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RA", "nh_asian", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RF", "nh_filipino", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RH", "latino", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RP", "nh_pacisl", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RT", "nh_twoormor", df_subset$ReportingCategory)
df_subset$ReportingCategory <- gsub("RW", "nh_white", df_subset$ReportingCategory)
df_subset <- rename(df_subset,c("geoname" = "CountyName"))

#pop screen. suppress raw/rate for groups with fewer than 20 graduating students.
threshold = 20
df_subset <- df_subset %>%
              mutate(rate = ifelse(raw < threshold, NA, rate), raw = ifelse(raw < threshold, NA, raw))

#pivot
df_wide <- df_subset %>% pivot_wider(names_from = ReportingCategory, names_glue = "{ReportingCategory}_{.value}", 
                                     values_from = c(raw, pop, rate))
df_wide$geoname[df_wide$geoname =='State'] <- 'California'   # update state rows' geoname field values


# View(df_subset)

#get county Geoids
census_api_key("25fb5e48345b42318ae435e4dcd28ad3f196f2c4", install = TRUE, overwrite = TRUE)
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
#View(ca)


#add county geoids
df_wide <- merge(x=ca,y=df_wide,by="geoname", all=T)
#add state geoid
df_wide <- within(df_wide, geoid[geoname == 'California'] <- '06')


# View(df_wide)
d <- df_wide

####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/2022_v4/RaceCounts/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

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
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
View(county_table)

###update info for postgres tables###
county_table_name <- "arei_educ_hs_grad_county_2022"
state_table_name <- "arei_educ_hs_grad_state_2022"
indicator <- "Four-year adjusted cohort graduation rate"
source <- "CDE 2020-21 https://www.cde.ca.gov/ds/ad/filesacgr.asp "

#send tables to postgres
to_postgres()
