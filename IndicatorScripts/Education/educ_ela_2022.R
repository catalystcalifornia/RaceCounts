# #install packages if not already installed ------------------------------
list.of.packages <- c("readr", "DBI", "tidyr","dplyr","RPostgreSQL","tidycensus")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)
library(readr)
library(DBI)
library(dplyr)
library(tidyr)
library(RPostgreSQL)
library(tidycensus)
# setwd("W:/Project/RACE COUNTS/2022_v4/Education/R/")
# metadata: https://caaspp-elpac.cde.ca.gov/caaspp/research_fixfileformatSB21

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

############### PREP DATA ########################

url = "https://caaspp-elpac.cde.ca.gov/caaspp/researchfiles/sb_ca2021_all_ascii_v2.zip"
zipfile = "W:\\Data\\Education\\CAASPP\\2020-21\\sb_ca2021_all_ascii_v2.zip"
file = "W:\\Data\\Education\\CAASPP\\2020-21\\sb_ca2021_all_ascii_v2.txt"
exdir = "W:\\Data\\Education\\CAASPP\\2020-21"


# #Download and unzip data ------------------------------------------------
if(!file.exists(zipfile)) { download.file(url=url, destfile=zipfile) } 
if(!file.exists(file)) { unzip(zipfile, exdir = exdir) } 

#Read in data
all_student_groups <- read_fwf(file, na = c("*", ""),
                               fwf_widths(c(14,4,4,3,1,7,7,2,2,7,7,6,6,6,6,6,6,7,6,6,6,6,6,6,6,6,6,6,6,6,2),
                                          col_names = c("cdscode","filler","test_year","student_grp_id",
                                                        "test_type","total_tested_at_reporting_level","total_tested_with_scores",	
                                                        "grade","test_id","students_enrolled","students_tested","mean_scale_score",	
                                                        "percentage_standard_exceeded","percentage_standard_met","percentage_standard_met_and_above",
                                                        "percentage_standard_nearly_met","percentage_standard_not_met","students_with_scores",	
                                                        "area_1_percentage_above_standard","area_1_percentage_near_standard","area_1_percentage_below_standard",
                                                        "area_2_percentage_above_standard","area_2_percentage_near_standard","area_2_percentage_below_standard",
                                                        "area_3_percentage_above_standard","area_3_percentage_near_standard","area_3_percentage_below_standard",
                                                        "area_4_percentage_above_standard","area_4_percentage_near_standard","area_4_percentage_below_standard",
                                                        "type_id")))

all_student_groups <- all_student_groups %>% select(-c(filler))

#Download and unzip school entities
url = "https://caaspp-elpac.cde.ca.gov/caaspp/researchfiles/sb_ca2021entities_ascii.zip"
zipfile2 = "W:\\Data\\Education\\CAASPP\\2020-21\\sb_ca2021entities_ascii.zip"
file2 = "W:\\Data\\Education\\CAASPP\\2020-21\\sb_ca2021entities_ascii.txt"

if(!file.exists(zipfile2)) { download.file(url=url, destfile=zipfile2) } 
if(!file.exists(file2)) { unzip(zipfile2, exdir = exdir) } 

#Read in entities
entities <- read_fwf(file2, fwf_widths(c(14,4,4,2,50), col_names = c("cdscode","filler","test_year","type_id", "geoname")))
entities <- entities %>% select(-c(filler))

df <-left_join(x=all_student_groups,y=entities,by= c("cdscode", "test_year", "type_id")) %>%
  select(cdscode, everything())
df <- df %>% dplyr::relocate(geoname, .after = cdscode)

# #join data to entities --------------------------------------------------
df <- rename(df, rate = percentage_standard_met_and_above, pop = students_with_scores, race = student_grp_id)

# Filter for 3rd grade, Math test, race/ethnicity subgroups, county/state level 
df_subset <- df %>% filter(grade == "03" & test_id == "01" & race %in% c("001","074","075","076","077","078","079","080","144")
                           & type_id %in% c("04", "05")) %>%    
  
  
  # # calc raw/rate and screen ---------------------------------------------------------
#calculate raw
mutate(raw = round(pop * rate / 100, 0)) 

#pop screen
threshold = 20
df_subset <- df_subset %>%mutate(raw = ifelse(pop < threshold, NA, raw), rate = ifelse(pop < threshold, NA, rate))

#select just fields we need
df_subset <- df_subset %>% select(geoname, race, rate, raw, pop) 

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
census_api_key("25fb5e48345b42318ae435e4dcd28ad3f196f2c4", overwrite = TRUE)

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
#View(ca)

#add county geoids
df_subset <- merge(x=ca,y=df_subset,by="geoname", all=T)
#add state geoid
df_subset <- within(df_subset, geoid[geoname == 'California'] <- '06')
#View(df_subset)

#pivot
df_wide <- df_subset %>% pivot_wider(names_from = race, names_glue = "{race}_{.value}", values_from = c(raw, pop, rate))

d <- df_wide 

####################################################################################################################################################
############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/2022_v4/RaceCounts/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update $asbest line of code accordingly before running this function.
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
county_table_name <- "arei_educ_gr3_ela_scores_county_2022"
state_table_name <- "arei_educ_gr3_ela_scores_state_2022"

indicator <- "Students scoring proficient or better on 3rd grade English Language Arts (%)"
source <- "CAASPP 2020-21 https://caaspp-elpac.cde.ca.gov/caaspp/ResearchFileListSB?ps=true&lstTestYear=2021&lstTestType=B&lstCounty=00&lstDistrict=00000"

#send tables to postgres
to_postgres()
