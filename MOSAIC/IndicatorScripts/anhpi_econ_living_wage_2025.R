### AAPI Living Wage RC v7###

# Set up workspace --------------------------------------------------------
# Install packages if not already installed
packages <- c("data.table", "stringr", "dplyr", "RPostgres", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "sf", "DBI", "usethis") 

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
con <- connect_to_db("rda_shared_data")
source("./Functions/pums_fx.R")  # MOSAIC-specific PUMS fx


# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Economic\\QA_Living_Wage_ANHPI.docx"

# define variables used throughout - update each year
curr_yr <- 2023 
rc_yr <- '2025'
rc_schema <- 'v7'
lw <- 15.50    # update living wage value as needed

## CHECK FOR UPDATES EACH YEAR
source("W://RDA Team//R//Github//RDA Functions//LF//RDA-Functions//Asian_NHPI_Ancestry_List.R")
### Ancestries pulled from: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf
# pi_ancestry <- c("Polynesian", "Hawaiian", "Samoan", "Tongan", "Micronesian", "Guamanian", "Chamorro", "Marshallese", "Fijian", "Pacific Islander", "Other Pacific")
# aa_ancestry <- c("")

### define common inputs for calc_pums{} and pums_screen{}
indicator = 'living_wage'         # name of column that contains indicator data, eg: 'living_wage' which contains values 'livable' and 'not livable'
indicator_val = 'livable'         # desired indicator value, eg: 'livable' (not 'not livable')        
weight = 'PWGTP'                  # PWGTP for person-level (psam_p06.csv) or WGTP for housing unit-level (psam_h06.csv) analysis
cv_threshold <- 30                # threshold and CV must be displayed as a percentage not decimal, eg: 30 not .3
raw_rate_threshold <- 0           # data values less than threshold are screened, for RC indicators threshold is 0.
pop_threshold <- 400              # data for geos+race combos with pop smaller than threshold are screened.

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
start_yr <- curr_yr - 4  # autogenerate start yr of 5yr estimates
root <- paste0("W:/Data/Demographics/PUMS/CA_", start_yr, "_", curr_yr, "/")

# Load ONLY the PUMS columns needed for this indicator
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0))    # get all PUMS cols 
cols_wts <- grep("^PWGTP*", cols, value = TRUE)                   # filter for PUMS weight colnames

ppl <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE, select = c(cols_wts, "RT", "SERIALNO", "AGEP", "ESR", "SCH", "PUMA",
                                                                                                   "ANC1P", "ANC2P", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH", 
                                                                                                   "ADJINC", "WAGP", "COW", "WKHP", "WRK", "WKWN"),
             colClasses = list(character = c("PUMA", "ANC1P", "ANC2P", "HISP", "RAC1P", "RACAIAN", "RACPI", "RACNH", 
                                             "ADJINC", "WAGP", "COW", "WKHP", "ESR", "WRK", "WKWN")))

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with puma-county xwalk
ppl$state_geoid <- "06"
ppl$puma_id <- paste0(ppl$state_geoid, ppl$PUMA)

# create list of replicate weights
repwlist = rep(paste0("PWGTP", 1:80))

# save copy of original data
orig_data <- ppl


####### Subset Data for Living Wage ########
# Adjust wage or salary income in past 12 months: WAGP (adjust with ADJINC)----
# trying wages first then will try earnings 
ppl$wages_adj <- (as.numeric(ppl$WAGP)*(as.numeric(ppl$ADJINC)/1000000))

# Filter data for pop of interest  ----
# Keep records only for those ages 18-64
ppl <- ppl %>% filter(AGEP >= 18 & AGEP <= 64)

# Keep records for those with non-zero earnings in past year
ppl <- ppl %>% filter(wages_adj>0)

# Keep records for those who were at work last week OR had a job but were not at work last week
ppl <- ppl %>% filter(WRK=='1' | ESR %in% c(1, 2, 3, 4, 5))

# Filter for those who were not self-employed or unpaid family workers
## 6-8 which are self-employed and then employed in family business do seem to have different average earnings than others
ppl <- ppl %>% filter(!COW %in% c('6','7','8'))

####### Calculate Living Wage #######
# Calculate hourly wage ----
# First calculate number of hours worked based on weekly hours and weeks worked
## convert usual hours worked per week past 12 months: WKHP to integer
ppl$wkly_hrs <- as.integer(ppl$WKHP)

## number of weeks worked in past 12 months: WKWN
ppl$wks_worked <- as.numeric(ppl$WKWN)

# review
table(ppl$wks_worked, ppl$WKWN, useNA = "always")
## View(ppl[c("RT","SERIALNO","wages_adj","WKWN","wks_worked")])

# Then calculate hourly wage
## Used 15.50 since that went into effect January 2023
ppl$hrly_wage <- as.numeric(ppl$wages_adj/(ppl$wks_worked * ppl$wkly_hrs))
## View(ppl[c("RT","SERIALNO","wages_adj","WKHP","WKWN","wks_worked","wkly_hrs","hrly_wage")])

# Code for Living Wage Indicator ----
# When lw or more, code as livable. When less than lw code as not livable. All other values code as NULL.
ppl$living_wage <- case_when(ppl$hrly_wage >= lw ~ "livable", ppl$hrly_wage < lw ~ "not livable", TRUE ~ "NA")
# View(ppl[c("RT","SERIALNO","COW","ESR","wages_adj","WKHP","WKWN","wks_worked","wkly_hrs","hrly_wage","living_wage")])

# Convert to factor for indicator
ppl$indicator <- as.factor(ppl$living_wage)
ppl$living_wage <- as.factor(ppl$living_wage)

#review
#table(ppl$indicator, useNA = "always")
table(ppl$living_wage, useNA = "always")

# Test disparities for state
# install.packages("spatstat")
# library(spatstat)
# median_race<-ppl%>%
#   group_by(race)%>%
#   summarize(median_hrly_wages=weighted.median(hrly_wage,PWGTP, na.rm=TRUE))
# ppl$living_wage_num <- ifelse(ppl$hrly_wage >= lw, 1, 0)
# living_wage_race<-ppl%>%
#   group_by(race)%>%
#   summarize(living_wage=weighted.mean(living_wage_num,PWGTP, na.rm=TRUE))
## looks as expected

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
ppl_assm <- merge(x=assm_name,y=ppl_assm, by="geoid", all=T)

# join sen crosswalk to data
ppl_sen <- left_join(ppl, sen_crosswalk, by=c("puma_id" = "puma")) 
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

# put all geolevel dfs into 1 list
ppl_list <- list(ppl_cs = ppl_cs, ppl_assm = ppl_assm, ppl_sen = ppl_sen, ppl_state = ppl_state)



### CALC NHPI ## -------------------------------------------------------------------------
# reclassify select ancestries
disagg_nhpi <- lapply(ppl_list, anhpi_reclass, curr_yr, nhpi_ancestry)   # in 2023, 11 nhpi ancestries present

# run pums calcs
disagg_nhpi_calc <- lapply(disagg_nhpi, calc_anhpi_pums, indicator, indicator_val, weight)

# combine all geolevel calcs and screen
nhpi_all <- rbindlist(disagg_nhpi_calc) %>%         # combine all geolevel df's before screening
  select(-starts_with("count_moe"), -starts_with("count_cv"))   # drop fields not needed for RC tables

colnames(nhpi_all) <- sub("count", "num", colnames(nhpi_all))         # rename some cols to RC colnames

#nhpi_screened <- pums_screen(nhpi_all, cv_threshold, raw_rate_threshold, pop_threshold, indicator_val)
### insert code to screen nhpi_all

# get list of subgroups in the data
nhpi_grps <- nhpi_all %>%
  select(starts_with("rate"), -contains("moe"), -contains("cv")) %>%
  colnames() %>%
  gsub("rate_", "", .)

nhpi_grps

#View(nhpi_screened)

# reclassify select ancestries
disagg_asian <- lapply(ppl_list, aapi_reclass, curr_yr, asian_ancestry) # in 2023, 29 asian ancestries present

disagg_asian_calc <- lapply(disagg_asian, calc_anhpi_pums, indicator, indicator_val, weight)

asian_all <- rbindlist(disagg_asian_calc) %>%         # combine all geolevel df's before screening
  select(-starts_with("count_moe"), -starts_with("count_cv"))   # drop fields not needed for RC tables

colnames(asian_all) <- sub("count", "num", colnames(asian_all))        # rename some cols to RC colnames

# get list of subgroups in the data
asian_grps <- asian_all %>%
  select(starts_with("rate"), -contains("moe"), -contains("cv")) %>%
  colnames() %>%
  gsub("rate_", "", .)

asian_grps

# aa_screened <- pums_screen(aa_all, cv_threshold, raw_rate_threshold, pop_threshold, indicator_val)
# View(aa_screened)



#d <- screened


# ############## CALC RACE COUNTS STATS ##############
# #set source for RC Functions script
# source("./Functions/RC_Functions.R")
# 
# d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'
# 
# d <- count_values(d) #calculate number of "_rate" values
# d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
# d <- calc_diff(d) #calculate difference from best
# d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
# d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
# d <- calc_id(d) #calculate index of disparity
# View(d)
# 
# #split STATE into separate table
# state_table <- d[d$geolevel == 'state', ]
# 
# #calculate STATE z-scores
# state_table <- calc_state_z(state_table)
# View(state_table)
# 
# #split COUNTY into separate table
# county_table <- d[d$geolevel == 'county', ]
# 
# #calculate COUNTY z-scores
# county_table <- calc_z(county_table)
# county_table <- calc_ranks(county_table)
# View(county_table)
# 
# #split LEG DIST into separate tables
# upper_table <- d[d$geolevel == 'sldu', ]
# lower_table <- d[d$geolevel == 'sldl', ]
# 
# #calculate SLDU z-scores and ranks
# upper_table <- calc_z(upper_table)
# 
# upper_table <- calc_ranks(upper_table)
# #View(upper_table)
# 
# #calculate SLDL z-scores and ranks
# lower_table <- calc_z(lower_table)
# 
# lower_table <- calc_ranks(lower_table)
# #View(lower_table)
# 
# ## Bind sldu and sldl tables into one leg_table##
# leg_table <- rbind(upper_table, lower_table)
# View(leg_table)
# 
# state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
# county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
# leg_table <- leg_table %>% dplyr::rename("leg_name" = "geoname", "leg_id" = "geoid")
# 
# 
# ###update info for postgres tables###
# leg_table_name <- paste0("aapi_econ_living_wage_leg_", rc_yr)
# county_table_name <- paste0("aapi_econ_living_wage_county_", rc_yr)
# state_table_name <- paste0("aapi_econ_living_wage_state_", rc_yr)
# indicator <- paste0("Percent of workers earning above living wage (", lw, "). Includes workers ages 18-64 who were at work last week or were employed but not at work. Excludes those with zero earnings and self-employed or unpaid family workers. PUMAs are assigned to counties and leg districts based on Geocorr 2022 crosswalks. We also screened by pop (400) and CV (30%). Asian is one race alone and Latinx-exclusive. NHPI and all disaggregated subgroups include one race alone (Asian or NHPI) and in combo and Latinx. QA Doc:", qa_filepath, ". This data is")
# source <- paste0("ACS PUMS (", start_yr, "-", curr_yr, ")")
# 
# #send tables to postgres
# ################# INSERT CODE TO SEND TO MOSAIC DB #######################
# con2 <- connect_to_db("mosaic")
# #to_postgres()
# #leg_to_postgres()
# 
# #close connection
# dbDisconnect(con)
# dbDisconnect(con2)
