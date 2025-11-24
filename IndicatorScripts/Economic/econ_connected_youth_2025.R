### Connected Youth RC v7 ###

#install packages if not already installed
packages <- c("data.table","stringr","dplyr","RPostgres","dbplyr","srvyr",
              "tidycensus","tidyr","rpostgis", "here", "sf", "usethis")

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

###### SET UP WORKSPACE #######
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("rda_shared_data")

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Economic\\QA_Connected_Youth.docx"

# define variables used throughout - update each year
curr_yr <- 2023 
rc_yr <- '2025'
rc_schema <- 'v7'

### define common inputs for calc_pums{} and pums_screen{}
indicator <- 'connected'        # update this to the desired ppl$indicator value you are working with
indicator_val = 'connected'     # desired indicator value, eg: 'livable' (not 'not livable') 
weight <- 'PWGTP'               # You must use to WGTP (if you are using psam_h06.csv and want housing units, like for Low Quality Housing) or PWGTP (if you want person units, like for Connected Youth)    
cv_threshold <- 20              # threshold and CV must be displayed as a percentage (not decimal)
raw_rate_threshold <- 0         # data values less than threshold are screened, for RC indicators threshold is 0.
pop_threshold <- 400       

##### GET PUMA CROSSWALKS ######
crosswalk <- dbGetQuery(con, "select county_id AS geoid, county_name AS geoname, geo_id AS puma, num_county, afact, afact2 from crosswalks.puma_2022_county_2020")

## Drop counties that have only 1 PUMA that is shared with another county that also has only 1 PUMA.
dupe_pumas <- crosswalk %>% 
  filter(num_county == 1) %>%   # keep only counties with 1 PUMA
  group_by(puma) %>% 
  summarise(n=n()) %>%   		    # count the # of counties assigned to each PUMA
  filter(n > 1)		   		        # keep only counties that have only 1 PUMA that is shared with another county with only 1 PUMA

county_crosswalk <- crosswalk %>%   
  subset(!(puma %in% dupe_pumas$puma)) # remove only counties that have only 1 PUMA that is shared with another county with only 1 PUMA

# length(unique(county_crosswalk$geoid))  # this number should be lower bc it is filtered xwalk
# length(unique(crosswalk$geoid))         # this number should be higher bc it is unfiltered xwalk

assm_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldl24 AS geoid, num_dist AS num_assm from crosswalks.puma_2020_state_assembly_2024")
sen_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldu24 AS geoid, num_dist AS num_sen from crosswalks.puma_2020_state_senate_2024")


# Get PUMS Data -----------------------------------------------------------
# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf
# path where my data lives (not pulling pums data from the postgres db, takes too long to run calcs that way) 
start_yr <- curr_yr - 4     # autogenerate start yr of 5yr estimates
root <- paste0("W:/Data/Demographics/PUMS/CA_", start_yr, "_", curr_yr, "/")

# Load ONLY the PUMS columns needed for this indicator
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0)) # get all PUMS cols 
cols_ <- grep(paste0("^", weight, "*"), cols, value = TRUE)                                # filter for PUMS weight colnames
ppl <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE, select = c(cols_, "AGEP", "ESR", "SCH", "PUMA", "ANC1P", "ANC2P", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH"),
             colClasses = list(character = c("ESR", "SCH", "PUMA", "ANC1P", "ANC2P", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH")))

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with crosswalks.puma_county_2020
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid, ppl$PUMA)

# create list of replicate weights
repwlist = rep(paste0(weight, 1:80))

# save copy of original data
orig_data <- ppl

############## Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2022.pdf ###############

##### Reclassify Race/Ethnicity ########
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/PUMS_Functions_new.R")
# check how many records there are for RACAIAN (AIAN alone/combo) versus RAC1P (AIAN alone) and same for NHPI
#View(subset(ppl, RACAIAN =="1"))
#View(subset(ppl, RAC1P >= 3 & ppl$RAC1P <=5))
#View(subset(ppl, RACNH =="1" | RACPI =="1"))
#View(subset(ppl, RAC1P == 7))

# latino includes all races. AIAN is AIAN alone/combo latino/non-latino, NHPI is alone/combo latino/non-latino, SWANA includes all races and latino/non-latino
ppl <- race_reclass(orig_data, start_yr, curr_yr)

# review data 
View(ppl[c("HISP","latino","RAC1P","race","ANC1P","ANC2P", "aian", "pacisl", "swana")])
# table(ppl$race, useNA = "always")
# table(ppl$race, ppl$latino, useNA = "always")
# table(ppl$race, ppl$aian, useNA = "always")
# table(ppl$race, ppl$pacisl, useNA = "always")
# table(ppl$race, ppl$swana, useNA = "always")
# table(ppl$aian, useNA = "always")
# table(ppl$pacisl, useNA = "always")
# table(ppl$swana, useNA = "always")

##### Define Connected Youth ###########
# Keep records only for those ages 16-24
ppl <- ppl %>% filter(AGEP >= 16 & AGEP <= 24)

# Filtering for those unemployed or not in the labor force 
ppl$employment <- ifelse(ppl$ESR %in% c(3,6), "not employed", "employed")

# Filtering for those not attending school and in school
ppl$schl_enroll <-  ifelse(ppl$SCH == 1, "not attending school", "in school")

# verify coding 
table(ppl$employment, ppl$ESR, useNA = "always")
table(ppl$SCH, ppl$schl_enroll, useNA = "always")
table(ppl$employment, ppl$schl_enroll, useNA = "always")

# Combine both conditions to create connected/disconnected variable 
ppl$connected <- ifelse(ppl$employment == "not employed" & ppl$schl_enroll =="not attending school", "not connected", "connected")
ppl$connected <- as.factor(ppl$connected)

#review
summary(ppl$indicator)
table(ppl$indicator, useNA = "always")

############### CALC LEG DIST, COUNTY, STATE ESTIMATES/CVS ETC. ############### 

# join county crosswalk to data
ppl_cs <- left_join(ppl, county_crosswalk, by=c("puma_id" = "puma"))   # join FILTERED county-puma crosswalk


# join assm crosswalk to data
ppl_assm <- left_join(ppl, assm_crosswalk, by=c("puma_id" = "puma")) 
## Add geonames
census_api_key(census_key1, overwrite=TRUE)
assm_name <- get_acs(geography = "State Legislative District (Lower Chamber)", 
                     variables = c("B01001_001"), 
                     state = "CA", 
                     year = curr_yr)

assm_name <- assm_name[,1:2]
assm_name$NAME <- str_remove(assm_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
assm_name$NAME <- gsub("; California", "", assm_name$NAME)
names(assm_name) <- c("geoid", "geoname")
# View(assm_name)

# add geonames to data
ppl_assm <- merge(x=assm_name,y=ppl_assm, by="geoid", all=T) #%>% filter(if_all(starts_with("PWGTP"), ~ !is.na(.)))


# join sen crosswalk to data
ppl_sen <- left_join(ppl, sen_crosswalk, by=c("puma_id" = "puma")) #%>% filter(if_all(starts_with("PWGTP"), ~ !is.na(.)))
## Add geonames
# census_api_key(census_key1, overwrite=TRUE)
sen_name <- get_acs(geography = "State Legislative District (Upper Chamber)", 
                    variables = c("B01001_001"), 
                    state = "CA", 
                    year = curr_yr)

sen_name <- sen_name[,1:2]
sen_name$NAME <- str_remove(sen_name$NAME,  "\\s*\\(.*\\)\\s*")  # clean geoname for sldl/sldu
sen_name$NAME <- gsub("; California", "", sen_name$NAME)
names(sen_name) <- c("geoid", "geoname")
# View(sen_name)

# add geonames to WA
ppl_sen <- merge(x=sen_name,y=ppl_sen, by="geoid", all=T)


# prep state df
ppl_state <- ppl %>% rename(geoid = state_geoid) %>% mutate(geoname = 'California')


#### Run PUMS Calcs ####
rc_county <- calc_pums(d = ppl_cs, indicator, indicator_val, weight)   # Calc county
rc_county$geolevel <- 'county'
View(rc_county)

rc_assm <- calc_pums(d = ppl_assm, indicator, indicator_val, weight)    # Calc assembly
rc_assm$geolevel <- 'sldl'
View(rc_assm)

rc_sen <- calc_pums(d = ppl_sen, indicator, indicator_val, weight)      # Calc senate
rc_sen$geolevel <- 'sldu'
rc_sen$geoname <- gsub("State ", "", rc_sen$geoname)  # clean geonames
View(rc_sen)

rc_state <- calc_pums(d = ppl_state, indicator, indicator_val, weight)  # Calc state
rc_state$geolevel <- 'state'
View(rc_state)

############ COMBINE & SCREEN COUNTY/STATE DATA ############# 
rc_all <- rbind(rc_state, rc_county, rc_assm, rc_sen) %>%        # combine all geolevel df's before screening
  select(-c(starts_with("count_moe"), starts_with("count_cv")))  # drop fields not needed for RC tables

colnames(rc_all) <- sub("count", "num", colnames(rc_all))  # rename some cols to RC colnames

screened <- pums_screen(rc_all, cv_threshold, raw_rate_threshold, pop_threshold, indicator_val)
View(screened)

d <- screened


############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)

#split STATE into separate table
state_table <- d[d$geolevel == 'state', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
View(state_table)

#split COUNTY into separate table
county_table <- d[d$geolevel == 'county', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
View(county_table)

#split LEG DIST into separate tables
upper_table <- d[d$geolevel == 'sldu', ]
lower_table <- d[d$geolevel == 'sldl', ]

#calculate SLDU z-scores and ranks
upper_table <- calc_z(upper_table)

upper_table <- calc_ranks(upper_table)
#View(upper_table)

#calculate SLDL z-scores and ranks
lower_table <- calc_z(lower_table)

lower_table <- calc_ranks(lower_table)
#View(lower_table)

## Bind sldu and sldl tables into one leg_table##
leg_table <- rbind(upper_table, lower_table)
View(leg_table)

state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
leg_table <- leg_table %>% dplyr::rename("leg_name" = "geoname", "leg_id" = "geoid")

###update info for postgres tables###
leg_table_name <- paste0("arei_econ_connected_youth_leg_", rc_yr)
county_table_name <- paste0("arei_econ_connected_youth_county_", rc_yr)
state_table_name <- paste0("arei_econ_connected_youth_state_", rc_yr)
indicator <- paste0("Connected Youth out of all Youth (%). Connected Youth are those ages 16-24 who are in school and/or employed. PUMAs are assigned to counties and leg districts based on Geocorr 2022 crosswalks. We screened by pop and CV. White, Black, Asian, Other are one race alone and Latinx-exclusive. Two or More is Latinx-exclusive. AIAN, NHPI, SWANA are Latinx-inclusive so they are also included in Latinx counts. AIAN, NHPI, and SWANA include AIAN, NHPI, and SWANA Alone and in combo, so non-Latinx AIAN, NHPI, SWANA in combo are also included in Two or More. QA Doc: ", qa_filepath, ". This data is")
source <- paste0("ACS PUMS (", start_yr, "-", curr_yr, ")")

#send tables to postgres
to_postgres()
leg_to_postgres()

#close connection
dbDisconnect(con)
