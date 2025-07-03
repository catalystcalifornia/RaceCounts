### AAPI Living Wage RC v7###

# Set up workspace --------------------------------------------------------
# Install packages if not already installed
packages <- c("data.table", "stringr", "dplyr", "RPostgres", "dbplyr", "srvyr", "tidycensus", "rpostgis",  "tidyr", "here", "sf", "DBI", "usethis") 

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

# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Economic\\QA_Living_Wage_AAPI.docx"

# define variables used throughout - update each year
curr_yr <- 2023 
rc_yr <- '2025'
rc_schema <- 'v7'
lw <- 15.50    # update living wage value as needed

## UPDATE EACH YEAR
### Ancestries pulled from: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf
pi_ancestry <- c("Polynesian", "Hawaiian", "Samoan", "Tongan", "Micronesian", "Guamanian", "Chamorro", "Marshallese", "Fijian", "Pacific Islander", "Other Pacific")
aa_ancestry <- c("")

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

##### Reclassify AA and PI Ancestries ########  Note: Ancestry lists specified at top of script
aapi_reclass <- function(x, acs_yr, ancestry_list) {
  ## import PUMS codes
  url <- paste0("https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_", start_yr, "-", curr_yr, ".csv")
  pums_vars_ <- read.csv(url, header=FALSE, na = "NA")   # read in data dictionary without headers bc some cols do not have names
  head(pums_vars_)
  anc_codes <- pums_vars_ %>% mutate(V6 = ifelse(V6=="", "col_6", V6), V7 = ifelse(V7=="", "val_label", V7))   # manually add names of cols w missing names
  colnames(anc_codes) <- as.character(unlist(anc_codes[1,]))												   # set first row values as col names
  anc_codes <- anc_codes %>% rename(var_code = 'Record Type')
  anc_codes <- anc_codes %>% filter(RT == 'ANC1P' | RT == 'ANC2P')											   # filter RT col for ancestry rows
  anc_codes <- anc_codes %>% select(var_code, val_label) %>% unique()										   # keep only code and label cols, remove dupes
  head(anc_codes)
  print("Added missing column names to anc_codes. Check console to ensure colnames are correct.")
  
  #View(anc_codes)
  
  ## filter PUMS codes for descriptions based on specified ancestry_list
  all_anc_codes <- anc_codes %>% filter(val_label %in% ancestry_list) %>% arrange(val_label)
  print("Displaying ancestry codes filtered by specified ancestry_list as all_anc_codes df.")
  View(all_anc_codes)
  
  aapi_incl1 <- x %>% select(ANC1P) %>% inner_join(all_anc_codes, by =c("ANC1P" = "var_code")) %>% unique() %>% rename(anc = ANC1P) # get list of AA or PI ancestries actually in CA PUMS data
  aapi_incl2 <- x %>% select(ANC2P) %>% inner_join(all_anc_codes, by =c("ANC2P" = "var_code")) %>% unique() %>% rename(anc = ANC2P) # get list of AA or PI ancestries actually in CA PUMS data
  aapi_incl <- rbind(aapi_incl1, aapi_incl2) %>% unique() %>% arrange(val_label)
  print("Displaying all the AA or PI ancestries actually present in the data as aapi_incl df.")
  View(aapi_incl)
  
  
  # code subgroups
  x$var_code = as.factor(ifelse(x$ANC1P == all_anc_codes$var_code[1], all_anc_codes$var_code[1],
                                #ifelse(x$ANC1P == 1 & x$latino =="latino", "white",
                                #      ifelse(x$ANC1P == 2 & x$latino =="not latino", "nh_black",
                                #            ifelse(x$ANC1P == 2 & x$latino =="latino", "black",
                                #                  ifelse(x$ANC1P == 6 & x$latino =="not latino", "nh_asian",
                                #                        ifelse(x$ANC1P == 6 & x$latino =="latino", "asian",
                                #                              ifelse(x$ANC1P == 8 & x$latino =="not latino", "nh_other", 
                                #                                    ifelse(x$ANC1P == 8 & x$latino =="latino", "other",
                                #                                          ifelse(x$ANC1P == 9 & x$latino =="not latino", "nh_twoormor",
                                #                                                ifelse(x$ANC1P == 9 & x$latino =="latino", "twoormor",
                                
                                NA))#))))))))))
  
  
  
  as.data.frame(x)
  
  x <- x %>% right_join(all_anc_codes)
  
  
  return(x)
}

## Run Reclassify Fx ##
disagg_pi <- lapply(ppl_list, aapi_reclass, curr_yr, pi_ancestry)
#disagg_aa <- lapply(ppl_list, aapi_reclass, curr_yr, aa_ancestry)


#### Run PUMS Calcs ####
calc_aapi_pums <- function(d, indicator, indicator_val, weight) {
  # d = PUMS dataframe, must contain geoid, geoname, race, latino, aian, pacisl, and swana fields
  # indicator = name of column that contains indicator data, eg: 'living_wage' which contains values 'livable' and 'not livable'
  # indicator_val = desired indicator value, eg: 'livable' (not 'not livable')        
  # weight = PWGTP for person-level (psam_p06.csv) or WGTP for housing unit-level (psam_h06.csv) analysis
  
  # create survey design  for indicator
  x <- na.omit(d) %>%               
    as_survey_rep(
      variables = c(geoid, geoname, var_code, val_label, !!sym(indicator)),   # select grouping variables
      weights = weight,                       # person or housing unit weight, must be defined in indicator script
      repweights = repwlist,                  # list of replicate weights
      combined_weights = TRUE,                # tells the function that replicate weights are included in the data
      mse = TRUE,                             # tells the function to calc mse
      type="other",                           # statistical method
      scale=4/80,                             # scaling set by ACS
      rscale=rep(1,80)                        # setting specific to ACS-scaling
    ) %>%
    filter(!is.na(indicator))			  	  # drop rows where indicator is null
  
  # calculate by subgroup 
  indicator_subgroup <- x %>%
    group_by(geoid, geoname, var_code, val_label, !!sym(indicator)) %>%
    summarise(
      count = survey_total(na.rm=T),         							  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%            								  # get the (survey weighted) proportion for the numerator
    
    left_join(x %>%                                        				  # left join in the denominators 
                group_by(geoid, geoname, var_code, val_label) %>%                       
                summarise(pop = survey_total(na.rm=T))) %>%               # get the (survey weighted) universe aka denominator
    mutate(rate = rate * 100,                                             # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,                              # calculate the derived margin of error for the rate
           rate_cv = ((rate_moe/1.645)/rate) * 100,                       # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100)                    # calculate cv for numerator count
  
  indicator_subgroup$raceeth <- as.character(indicator_subgroup$val_label)
  print(indicator_subgroup, n = 25)
  
  
  indicator_ <- indicator_subgroup %>% 
    filter(!!sym(indicator) == !!indicator_val) %>%				    # filter only records for desired ppl$indicator value
    filter(!is.na(raceeth)) %>%										# filter out records where raceeth is NULL
    group_by(geoid, geoname) %>%										# group by geo and geoname name
    as.data.frame()
  
  # convert long format to wide
  rc_indicator <- indicator_ %>% 
    pivot_wider(id_cols = c(geoid, geoname),						# convert to wide format
                names_from = raceeth,
                values_from = c("count", "pop", "rate", "rate_moe", "rate_cv", "count_moe", "count_cv")) %>%  
    as.data.frame()
  
  # drop rows where geoid = NA
  rc_indicator <- rc_indicator %>% drop_na(geoid)
  
  return(rc_indicator)
}

# disagg_pi_calc <- lapply(disagg_pi, calc_aapi_pums(indicator, indicator_val, weight)) # fx doesn't work on list for some reason

pi_county <- calc_aapi_pums(disagg_pi$ppl_cs, indicator, indicator_val, weight)        # Calc county
pi_assm <- calc_aapi_pums(disagg_pi$ppl_assm, indicator, indicator_val, weight)        # Calc assembly
pi_sen <- calc_aapi_pums(disagg_pi$ppl_sen, indicator, indicator_val, weight)          # Calc senate
pi_state <- calc_aapi_pums(disagg_pi$ppl_state, indicator, indicator_val, weight)      # Calc state

# aa_county <- calc_aaaa_pums(disagg_aa$ppl_cs, indicator, indicator_val, weight)      # Calc county
# aa_assm <- calc_aaaa_pums(disagg_aa$ppl_assm, indicator, indicator_val, weight)      # Calc assembly
# aa_sen <- calc_aaaa_pums(disagg_aa$ppl_sen, indicator, indicator_val, weight)        # Calc senate
# aa_state <- calc_aaaa_pums(disagg_aa$ppl_state, indicator, indicator_val, weight)    # Calc state


############ COMBINE & SCREEN DATA ############# 
pi_all <- rbind(pi_county, pi_assm, pi_sen, pi_state) %>%         # combine all geolevel df's before screening
  select(-c(starts_with("count_moe"), starts_with("count_cv")))   # drop fields not needed for RC tables

colnames(pi_all) <- sub("count", "num", colnames(pi_all))         # rename some cols to RC colnames

pi_screened <- pums_screen(pi_all, cv_threshold, raw_rate_threshold, pop_threshold, indicator_val)
View(pi_screened)

# aa_all <- rbind(aa_county, aa_assm, aa_sen, aa_state) %>%        # combine all geolevel df's before screening
#   select(-c(starts_with("count_moe"), starts_with("count_cv")))  # drop fields not needed for RC tables
# 
# colnames(aa_all) <- sub("count", "num", colnames(aa_all))        # rename some cols to RC colnames
# 
# aa_screened <- pums_screen(aa_all, cv_threshold, raw_rate_threshold, pop_threshold, indicator_val)
# View(aa_screened)



#d <- screened


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
leg_table_name <- paste0("aapi_econ_living_wage_leg_", rc_yr)
county_table_name <- paste0("aapi_econ_living_wage_county_", rc_yr)
state_table_name <- paste0("aapi_econ_living_wage_state_", rc_yr)
indicator <- paste0("Percent of workers earning above living wage (", lw, "). Includes workers ages 18-64 who were at work last week or were employed but not at work. Excludes those with zero earnings and self-employed or unpaid family workers. PUMAs are assigned to counties and leg districts based on Geocorr 2022 crosswalks. We also screened by pop (400) and CV (30%). Asian is one race alone and Latinx-exclusive. NHPI and all disaggregated subgroups include one race alone (Asian or NHPI) and in combo and Latinx. QA Doc:", qa_filepath, ". This data is")
source <- paste0("ACS PUMS (", start_yr, "-", curr_yr, ")")

#send tables to postgres
################# INSERT CODE TO SEND TO MOSAIC DB #######################
con2 <- connect_to_db("mosaic")
#to_postgres()
#leg_to_postgres()

#close connection
dbDisconnect(con)
dbDisconnect(con2)
