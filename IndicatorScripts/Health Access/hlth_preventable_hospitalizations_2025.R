## Preventable Hospitalizations for RC v7 ##

#install packages if not already installed
packages <- c("tidyr", "stringr", "dplyr", "RPostgres", "usethis", "sf", "data.table", "readxl")  

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
con_shared <- connect_to_db("rda_shared_data")
con_rc <- connect_to_db("racecounts")

# update variables used throughout each year
curr_yr <- '2018-2022'
acs_yr <- 2020   # used to get census geoid/names from census api
rc_yr <- '2025'
rc_schema <- 'v7'
dwnld_url <- "https://hcai.ca.gov/"
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Health Access\\QA_Sheet_Preventable_Hospitalizations.docx"

threshold <- 800   # data for geo+race combos with fewer than threshold # of pop are suppressed

# Get downloaded data and subset
df_multigeo <- read_excel("W:/Data/Health/OSHPD/2018-2022 HCAI Custom Run/_PQI 92 by Race-Ethnicity, 2018-2022_CS2712_Mulholland Graves.xlsx", 
                          sheet = 2, col_types = c(rep("text", 4), rep("numeric", 4))) %>% 
  select(-c(YEARS, `_type_`, `OBSERVED RATE`)) %>% 
  rename("geoname" ="PATIENT COUNTY", 
         "race" = "RACE CATEGORY", 
         "raw" ="PQI 92 DISCHARGES", 
         "pop" = "POPULATION", 
         "rate" = "OBSERVED RATE PER 100,000") %>%
  filter(!is.na(geoname))
  

# rename race function
rename_raceeth <- function(x) {
  x$race <-  gsub(" Total", "", x$race, fixed = TRUE) #fixed = TRUE tells it that there is no regular expression in the pattern / patterns should not be considered a regex
  x$race <-  gsub("All", "total", x$race, fixed = TRUE)
  x$race <-  gsub("Asian/PacIslander", "nh_api", x$race, fixed = TRUE)
  x$race <-  gsub("Native American", "nh_aian", x$race, fixed = TRUE)
  x$race <-  gsub("Hispanic", "latino", x$race, fixed = TRUE)
  x$race <-  gsub("Black", "nh_black", x$race, fixed = TRUE)
  x$race <-  gsub("White", "nh_white", x$race, fixed = TRUE)
  x$race <-  gsub("Other", "nh_other", x$race, fixed = TRUE)
  
  return(x)
}

# rename races
df_multigeo <- rename_raceeth(df_multigeo)


# calc county totals
total_df <- df_multigeo %>% group_by(geoname) %>% 
  filter(geoname!='Statewide') %>% 
  summarize(total_raw=sum(raw, na.rm=TRUE), 
            total_pop = sum(pop, na.rm=TRUE), 
            total_rate=(total_raw/total_pop)*100000)


#pivot_wider to make it readable to the race counts functions
df_multigeo_wide <- pivot_wider(df_multigeo, names_from=race, names_glue = "{race}_{.value}", values_from=c(raw,pop,rate))

df_multigeo_wide <- df_multigeo_wide %>%  # fill in county totals
  filter(is.na(total_rate)) %>% # deal only with NAs
  select(-c(total_rate, total_raw, total_pop)) %>% # remove column to be overwritten
  left_join(total_df, by = "geoname") %>% # get the values from dictionary
  bind_rows(df_multigeo_wide %>%  
              filter(!is.na(total_rate)))


#get geoids
census_api_key(census_key1, overwrite=TRUE) # In practice, may need to include install=TRUE if switching between census api keys
Sys.getenv("CENSUS_API_KEY") # confirms value saved to .renviron

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = acs_yr)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

#add county geoids
df_multigeo_wide <- merge(x=ca,y=df_multigeo_wide,by="geoname", all=T) %>%
  select(geoid, geoname, everything())

#add state geoid, geoname
df_multigeo_wide$geoname <- ifelse(df_multigeo_wide$geoname == 'Statewide', 'California', df_multigeo_wide$geoname)
df_multigeo_wide$geoid <- ifelse(df_multigeo_wide$geoname == 'California', '06', df_multigeo_wide$geoid)

#Now screen out racial groups with fewer than threshold # of people so small numbers did not lead to general conclusions about a racial group in a county or across the state per the methodology https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EbTuOYlkNAxKvtipqwQxTwUBZ3ijcQT5DTu-Xb8uYXl5AQ?e=Qkveug
df_screened <- df_multigeo_wide %>% mutate(nh_api_raw = ifelse(nh_api_pop< threshold, NA, nh_api_raw), 
                                           nh_api_rate=ifelse(nh_api_pop< threshold, NA, nh_api_rate),
                                           nh_black_raw = ifelse(nh_black_pop< threshold, NA, nh_black_raw), 
                                           nh_black_rate = ifelse(nh_black_pop< threshold, NA, nh_black_rate),
                                           latino_raw  = ifelse(latino_pop< threshold, NA, latino_raw), 
                                           latino_rate = ifelse(latino_pop< threshold, NA, latino_rate),
                                           nh_aian_raw = ifelse(nh_aian_pop< threshold, NA, nh_aian_raw), 
                                           nh_aian_rate = ifelse(nh_aian_pop< threshold, NA, nh_aian_rate),
                                           nh_other_raw = ifelse(nh_other_pop< threshold, NA, nh_other_raw), 
                                           nh_other_rate = ifelse(nh_other_pop< threshold, NA, nh_other_rate),
                                           nh_white_raw = ifelse(nh_white_pop< threshold, NA, nh_white_raw), 
                                           nh_white_rate = ifelse(nh_white_pop< threshold, NA, nh_white_rate),
                                           total_raw = ifelse(total_pop< threshold, NA, total_raw),  
                                           total_rate = ifelse(total_pop< threshold, NA, total_rate))

# add geolevel
d <- df_screened %>% mutate(geolevel = ifelse(geoname == "California", "state", "county")) %>%
  select(geoid, geoname, geolevel, everything()) %>% arrange(geoid)

############## CALC RACE COUNTS STATS ##############

# set source for RC Functions script
source("./Functions/RC_Functions.R")

# Adds asbest value for RC Functions
d$asbest = "min"   # min bc minimum rate is 'best' rate

d <- count_values(d)
d <- calc_best(d)
d <- calc_diff(d)
d <- calc_avg_diff(d)
d <- calc_p_var(d) #switch between s_var for sample and p_var for population
d <- calc_id(d)

### Split into geolevel tables
#split into STATE and COUNTY tables
state_table <- d[d$geoname == 'California', ]
county_table <- d[d$geoname != 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table) 
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table)

## Calc county ranks##
county_table <- calc_ranks(county_table) 
View(county_table)

#rename geoid to state_id, county_id
state_table <- rename(state_table, state_id = geoid, state_name = geoname)
county_table <- rename(county_table, county_id = geoid, county_name = geoname)

# ############## SEND COUNTY, STATE, CITY CALCULATIONS TO POSTGRES ##############

### info for postgres tables will auto update ###
county_table_name <- paste0("arei_hlth_preventable_hospitalizations_county_",rc_yr)      
state_table_name <- paste0("arei_hlth_preventable_hospitalizations_state_",rc_yr)      
indicator <- paste0("Preventable Hospitalizations (Rate per 100k). QA doc: ", qa_filepath)                         # See most recent Indicator Methodology for indicator description
source <- paste0("California Department of Health Care Access and Information (", curr_yr, ")", dwnld_url)


# ####### SEND TO POSTGRES #######
to_postgres(county_table,state_table)


# disconnect
dbDisconnect(con_shared)
dbDisconnect(con_rc)

