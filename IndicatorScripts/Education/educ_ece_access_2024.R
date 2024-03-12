### ECE Access RC v6 ### 

##install packages if not already installed ------------------------------
list.of.packages <- c("dplyr","tidyr","tidycensus","tigris","readxl","sf","tidyverse","usethis","RPostgeSQL")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)
library(dplyr)
library(tidyr)
library(tidycensus)
library(tigris)
library(readxl)
library(sf)
library(tidyverse)
library(usethis)
library(RPostgreSQL)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# define variables used in several places that must be updated each year
curr_yr <- "2020-2021"  # must keep same format
dwnld_url <- "https://rrnetwork.org/ and https://elneedsassessment.org/"
rc_schema <- "v6"
yr <- "2024"

##### 1. Get County IDs 

ca_counties <- counties(state = "06") 
ca_counties <- as.data.frame(ca_counties) %>% select(NAME, GEOID) %>% arrange(NAME) %>% rename(geoname = NAME)


##### 2. AIR IT DATA ####

#get la county infant-toddler (IT) data, converting asterisks to nulls
air_it <- read_xlsx("W:/Data/Education/American Institute for Research/2020/air_it_2020.xlsx", range = "A6:V2568", na = "*")
# air_it_meta <- read_xlsx("W:/Data/Education/American Institute for Research/2020/air_it_2020.xlsx", range = "A2:V7", na = "*") 
# air_it_meta <- air_it_meta[-which(air_it_meta[1]=='California'),] # check against subset below
# View(air_it_meta)


#subset IT data to keep columns we want
air_it <- air_it[c(1:2, 6, 10, 14, 18, 22)]

#rename columns
names(air_it) <- c("geoname", "pct_zip", "it_pop", "it_under_85smi", "it_enrollment", "it_unmet_need", "it_pct_unmet_need")

#join county ids
air_it <- left_join(air_it, ca_counties, by = "geoname") %>% 
  
  #fill in the blanks for county ids, 
  fill(GEOID) %>% 
  
  #filter out county totals
  filter(pct_zip != "Percent of Zip Code Allocation")  %>%
  
  #create county-zip id
  mutate(zip_county_id = paste0(geoname, "-", GEOID))


#### 3. AIR PREK DATA ####

#get prek data, converting asterisks to nulls
air_prek <- read_xlsx("W:/Data/Education/American Institute for Research/2020/air_prek_2020.xlsx", range = "A4:V2566", na = "*")
# air_prek_meta <- read_xlsx("W:/Data/Education/American Institute for Research/2020/air_prek_2020.xlsx", range = "A2:V7", na = "*")
# air_prek_meta <- air_prek_meta[-which(air_prek_meta[1]=='California'),] # check against subset below
# View(air_prek_meta)

#subset prek data to columns we want
air_prek <- air_prek[c(1:2, 3:4, 7:8, 11:12, 15:16, 19:20)]

#rename columns
names(air_prek) <- c("geoname", "pct_zip", 
                     "prek3_pop","prek4_pop", 
                     "prek_under_85smi3","prek_under_85smi4", 
                     "prek_enrollment3","prek_enrollment4", 
                     "prek_unmet_need3", "prek_unmet_need4",
                     "prek_pct_unmet_need3", "prek_pct_unmet_need4")

#add 3 and 4 year olds
air_prek <- air_prek %>% mutate(
  prek_pop = prek3_pop + prek4_pop,
  prek_under_85smi = prek_under_85smi3 + prek_under_85smi4,
  prek_enrollment = prek_enrollment3 + prek_enrollment4,
  prek_unmet_need = prek_unmet_need3 + prek_unmet_need4,
  prek_pct_unmet_need = (prek_unmet_need3 + prek_unmet_need4) / (prek_under_85smi3 + prek_under_85smi4)
) %>% select (geoname, pct_zip, prek_pop, prek_under_85smi, prek_enrollment, prek_unmet_need, prek_pct_unmet_need)

#join county ids
air_prek <- left_join(air_prek, ca_counties, by = "geoname") %>% 
  
  #fill in the blanks for county ids, 
  fill(GEOID) %>% 
  
  #filter out county totals
  filter(pct_zip != "Percent of Zip Code Allocation")  %>%
  
  #create county-zip id
  mutate(zip_county_id = paste0(geoname, "-", GEOID))


#### 4. AIR TK DATA ####

#get tk data
air_tk <- read_xlsx("W:/Data/Education/American Institute for Research/2020/tk.xlsx", range = "A4:F2566", na = "*")
# air_tk_meta <- read_xlsx("W:/Data/Education/American Institute for Research/2020/tk.xlsx", range = "A2:F5", na = "*")
# air_tk_meta <- air_tk_meta[-which(air_tk_meta[1]=='California'),] # check against subset below
# View(air_tk_meta)

names(air_tk) <- c("geoname", "pct_zip", "three", "four", "five", "tk") 

#join county ids
air_tk <- left_join(air_tk, ca_counties, by = "geoname") %>% 
  
  #fill in the blanks for county ids, 
  fill(GEOID) %>% 
  
  #filter out county totals
  filter(pct_zip != "Percent of Zip Code Allocation")  %>%
  
  #create county-zip id
  mutate(zip_county_id = paste0(geoname, "-", GEOID)) %>% 
  
  #remove unnecessary fields
  select(-"three", -"four", -"five")


#### 5. CCCRRN DATA ####

#get CCCRRN data
cccrrn <- read_xlsx("W:/Data/Education/CCCRRN/2021/CatalystCA2021Data_rev.xlsx") %>% rename(geoname = ZIPCODE)

#format columns for join
cccrrn$geoname <- as.character(cccrrn$geoname) 


#### 6. Join AIR IT, PREK, TK, & CCRRN data)

#join it, prek, and tk data (removing duplicate fields)
df <- full_join(air_it, air_prek %>% select(-geoname, -pct_zip, -GEOID), by = "zip_county_id")
df <- full_join(df, air_tk %>% select(-geoname, -pct_zip, -GEOID), by = "zip_county_id")

#this will be a many to many join and we will apply pct_zip next, check 91361
df <- left_join(df, cccrrn, by = "geoname")


#### 7. Calculate indicator ####

#calculating as we did for education.ece_zip_code_enrollment_rate_2018 used in RC v3
#which assumes ccrrn capacity = full enrollment. 
df$pct_zip <- as.numeric(df$pct_zip)/100
df$children <- rowSums(df[,c("it_pop", "prek_pop")] * df$pct_zip, na.rm = TRUE)
df$enrollment <- rowSums(df[,c("INFCAP", "PRECAP", "FCCCAP", "tk")] * df$pct_zip, na.rm = TRUE)
#df$enrollment_rate <- df$enrollment / df$children * 100  # I don't think we want to calc the rate here? Instead do it after we have the pop data?
#df$enrollment_rate[df$enrollment_rate == "Inf"] <- 100   # I don't think we want to calc the rate here? Instead do it after we have the pop data?

#format for WA
ind_df <- df %>% filter(children > 0) %>% rename(target_id = GEOID, sub_id = geoname) %>% #, indicator = enrollment_rate) %>%
  mutate(geoname = "zcta") %>% select(target_id, sub_id, geoname) #, indicator)

# import ZCTA-County Relationship File from Census. https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2020.html#zcta
## Read more: https://www2.census.gov/geo/pdfs/maps-data/data/rel2020/zcta520/explanation_tab20_zcta520_county20_natl.pdf and
         ##   https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/2020-zcta-record-layout.html#county
filepath <- "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"
rel_file <- read_delim(file = filepath, delim = "|", na = c("*", ""))
xwalk <- filter(rel_file, (substr(GEOID_COUNTY_20,1,2) == '06' & !is.na(GEOID_ZCTA5_20))) %>%
              rename(zcta_id = GEOID_ZCTA5_20, county_id = GEOID_COUNTY_20, county_name = NAMELSAD_COUNTY_20) %>%
                select(c(zcta_id, county_id, county_name))

#### 8. Get ZCTA under 5 pop by race ####
#set source for WA Functions script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")

# set values for weighted average functions - You may need to update these
year <- c(2020)                   # define your data vintage
subgeo <- c('zcta')              # define your sub geolevel: can be tract or zcta (zcta may require some additions to the fx since they are mostly for tract)
targetgeolevel <- c('county')     # define your target geolevel
survey <- "census"                # define which Census survey you want
pop_threshold = 50                # define population threshold for screening
census_api_key(census_key1)       # reload census API key
vars_list <- "vars_list_p12"      # pop under 5 by race/eth, the list of variables is in the WA fx script

pop <- update_detailed_table_census(vars = vars_list, yr = year, srvy = survey)  # subgeolevel pop
pop <- lapply(pop, function(x) cbind(x, table = str_extract(x$variable, "[^_]+"))) # create table field used in next step
pop_ <- as.data.frame(pop) %>% select(c(GEOID, table, geolevel, value)) %>% group_by(GEOID, table, geolevel) %>% summarise(value_sum=sum(value))
pop_ <- pop_ %>% left_join(p12_meta, by = c("table" = "p12_table")) # add race field
pop_wide <- pop_ %>% as.data.frame() %>% pivot_wider(id_cols = c(GEOID, geolevel), names_from = p12_race, values_from = value_sum)
pop_wide <- pop_wide %>% right_join(select(xwalk, c(zcta_id, county_id, county_name)), by = c("GEOID" = "zcta_id"))  # join target geoids/names
pop_wide <- pop_wide %>% relocate(county_id, .after = "GEOID") %>% relocate(county_name, .after = "county_id")
colnames(pop_wide)[5:ncol(pop_wide)] <- paste(colnames(pop_wide)[5:ncol(pop_wide)], "sub_pop", sep = "_") # rename sub_pop columns
pop_wide <- dplyr::rename(pop_wide, sub_id = GEOID, target_id = county_id, target_name = county_name) # rename to generic column names for WA functions

##### 9. Make county data from zcta data ####

under5_df_wide_county <- pop_wide %>% group_by(target_id) %>% 
  summarize(
    total_target_pop = sum(total_sub_pop),
    aian_target_pop = sum(aian_sub_pop),
    latino_target_pop = sum(latino_sub_pop),
    nh_asian_target_pop = sum(nh_asian_sub_pop),
    nh_black_target_pop = sum(nh_black_sub_pop),
    nh_twoormor_target_pop = sum(nh_twoormor_sub_pop),
    nh_other_target_pop = sum(nh_other_sub_pop),
    nh_white_target_pop = sum(nh_white_sub_pop),
    pacisl_target_pop = sum(pacisl_sub_pop),
    n=n()
  )

#join zip data to county data and format
pop_df <- left_join(pop_wide, under5_df_wide_county, by="target_id")
pop_df$geolevel = "zcta"
pop_df <- pop_df %>% select(sub_id, target_id, geolevel, everything())


########### 10. Calc weighted average ################
#set source for WA Functions script
source("W:/RDA Team/R/Functions/Cnty_St_Wt_Avg_Functions.R")


##### COUNTY WEIGHTED AVG CALCS ######
wa <- wt_avg(pct_df)        # calc weighted average and apply reliability screens
wa <- wa %>% left_join(targetgeo_names, by = "target_id") %>% mutate(geolevel = 'county')    # add in target geolevel names and geolevel type


pop_threshold = 50 # same threshold as used in previous calcs
# calc pct of target geolevel pop in each sub geolevel
pct_df <- pop_pct_multi(pop_df) # use pop_pct_multi bc a sub_geo can match to more than one target_geo
wa <- wt_avg(pct_df)            # calc weighted average and apply reliability screens





#disconnect
dbDisconnect(con)

