# Disaggregated Asian / NHPI Pop

# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf

# Install packages if not already installed
packages <- c("tidyverse","data.table","readxl","tidycensus","srvyr","stringr","openxlsx","dplyr") 

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(install_packages) > 0) {
  install.packages(install_packages)
} else {
  print("All required packages are already installed.")
}

for(pkg in packages){
  library(pkg, character.only = TRUE)
}

#SOURCE from the script that has: styling, packages, dbconnection, colors
source("W:\\RDA Team\\R\\credentials_source.R")
source("MOSAIC/Functions/pums_fx.R")  # MOSAIC-specific PUMS fx
con <- connect_to_db("rda_shared_data")


# update QA doc filepath
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Economic\\QA_Living_Wage_MOSAIC.docx"

# define variables used throughout - update each year
curr_yr <- 2023 
rc_yr <- '2025'
rc_schema <- 'v7'

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


#### Step 1: load the data ####

# PUMS Data
root <- "W:/Data/Demographics/PUMS/"
indicator_name <- "Disaggregated Asian-NHPI Alone and AOIC"
start_yr <- curr_yr - 4  # autogenerate start yr of 5yr estimates

# Load the people PUMS data
people <- fread(paste0(root, "CA_2023/psam_p06.csv"), header = TRUE, data.table = FALSE,
                colClasses = list(character = c("PUMA", "RACASN", "RACNH", "RACPI", "RAC3P")))


## Select Asian/NHPI people
pums_data <- people %>% 
  
  #filtering for universe
  filter(RACASN == 1 | RACNH == 1 | RACPI == 1)

# Add state_geoid to ppl, add state_geoid to PUMA id, so it aligns with puma-county xwalk
pums_data$state_geoid <- "06"
pums_data$puma_id <- paste0(pums_data$state_geoid, pums_data$PUMA)

#pull in PUMS data dictionary codes for RAC2P
race_codes <- read_excel(paste0("W:/Data/Demographics/PUMS/CA_", start_yr, "_", curr_yr, "/PUMS_Data_Dictionary_", start_yr, "-", curr_yr, "_RAC3P.xlsx")) %>% 
  mutate_all(as.character) # created this excel document separate by opening PUMS Data Dictionary in excel and deleting everything but RAC3P
race_codes$RAC3P <- race_codes$Code_1
race_codes <- race_codes %>% select(RAC3P, Description)

anhpi_pop <- left_join(pums_data, race_codes, by=("RAC3P")) %>% rename(anhpi_subgroup=Description)# clarify column we'll group by and set final dataset

# checking alternate way of recoding subgroups
## also there are many variations that may not be what we're looking for, eg: 'and/or Asian groups' 'Other Asian 
# x <- left_join(pums_data, race_codes, by=("RAC3P")) %>% rename(anhpi_subgroup=Description)# clarify column we'll group by and set final dataset
# x <- x %>%
#   separate(
#     anhpi_subgroup,
#     into = paste0("subgroup", 1:10),  # adjust max number if needed
#     sep = ";",
#     fill = "right",
#     extra = "drop",
#     remove = FALSE
#   ) %>%
#   mutate(across(starts_with("subgroup"), str_trim))
# View(x)

# run reclass fx to create a new col for each asian/nhpi subgroup, assign 1 for yes and 0 for no
anhpi_pop <- anhpi_reclass(anhpi_pop)

# check all subgroup cols have value of 0, meaning each record has a 1 for at least 1 subgroup
# anhpi_pop %>%
#   dplyr::select(chinese:last_col()) %>%
#   summarise(across(everything(), ~ sum(is.na(.))))


#### Step 2: join data to crosswalks ####

# join county crosswalk to data
ppl_cs <- left_join(anhpi_pop, county_crosswalk, by=c("puma_id" = "puma"))   # join FILTERED county-puma crosswalk

# join assm crosswalk to data
ppl_assm <- left_join(anhpi_pop, assm_crosswalk, by=c("puma_id" = "puma")) 
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
ppl_sen <- left_join(anhpi_pop, sen_crosswalk, by=c("puma_id" = "puma")) 
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
ppl_state <- anhpi_pop %>% rename(geoid = state_geoid) %>% mutate(geoname = 'California')

# put all geolevel dfs into 1 list
ppl_list <- list(ppl_cs = ppl_cs, ppl_assm = ppl_assm, ppl_sen = ppl_sen, ppl_state = ppl_state)


#### Step 3: Set up survey ####

weight <- 'PWGTP'  # weight
repwlist = rep(paste0("PWGTP", 1:80)) # replicate weights

# set survey components
anhpi_svry <- anhpi_pop %>%               
  as_survey_rep(
    variables = c(geoid, anhpi_subgroup),   # dplyr::select grouping variables
    weights = weight,                       # person weight
    repweights = repwlist,                  # list of replicate weights
    combined_weights = TRUE,                # tells the function that replicate weights are included in the data
    mse = TRUE,                             # tells the function to calc mse
    type="other",                           # statistical method
    scale=4/80,                             # scaling set by ACS
    rscale=rep(1,80)                        # setting specific to ACS-scaling
  )


###### Asian/NHPI subgroups, alone and AOIC ######
anhpi_subgroups_table <- anhpi_svry %>%
  group_by(geoid, anhpi_subgroup) %>%   # group by asian subgroup description
  summarise(
    num = survey_total(na.rm=T), # get the (survey weighted) count for the numerators
    rate = survey_mean()) %>%        # get the (survey weighted) proportion for the numerator
  left_join(anhpi_svry %>%                                        # left join in the denominators
              group_by(geoid) %>%                                     # group by geo
              summarise(pop = survey_total(na.rm=T))) %>%              # get the weighted total for overall geo
  mutate(rate=rate*100,
         rate_moe = rate_se*1.645*100,    # calculate the margin of error for the rate based on se
         rate_cv = ((rate_moe/1.645)/rate) * 100, # calculate cv for rate
         count_moe = num_se*1.645, # calculate moe for numerator count based on se
         count_cv = ((count_moe/1.645)/num) * 100)  # calculate cv for numerator count

# recode rates under 1% and create a list for recoding
other_list<-anhpi_subgroups_table%>%filter(rate<1)%>%ungroup()%>%select(anhpi_subgroup)
other_list<-other_list$anhpi_subgroup


anhpi_youth<-anhpi_youth%>%
  mutate(anhpi_subgroup=ifelse(anhpi_subgroup %in% other_list, 'Another Asian Identity Alone',anhpi_subgroup)) 

anhpi_svry <- anhpi_youth %>%               
  as_survey_rep(
    variables = c(geoid, anhpi_subgroup),   # dplyr::select grouping variables
    weights = weight,                       # person weight
    repweights = repwlist,                  # list of replicate weights
    combined_weights = TRUE,                # tells the function that replicate weights are included in the data
    mse = TRUE,                             # tells the function to calc mse
    type="other",                           # statistical method
    scale=4/80,                             # scaling set by ACS
    rscale=rep(1,80)                        # setting specific to ACS-scaling
  )


###### Asian subgroups, Asian alone ######
anhpi_subgroups_table_re <- anhpi_svry %>%
  group_by(geoid, anhpi_subgroup) %>%   # group by asian subgroup description
  summarise(
    num = survey_total(na.rm=T), # get the (survey weighted) count for the numerators
    rate = survey_mean()) %>%        # get the (survey weighted) proportion for the numerator
  left_join(anhpi_svry %>%                                        # left join in the denominators
              group_by(geoid) %>%                                     # group by geo
              summarise(pop = survey_total(na.rm=T))) %>%              # get the weighted total for overall geo
  mutate(rate=rate*100,
         rate_moe = rate_se*1.645*100,    # calculate the margin of error for the rate based on se
         rate_cv = ((rate_moe/1.645)/rate) * 100, # calculate cv for rate
         count_moe = num_se*1.645, # calculate moe for numerator count based on se
         count_cv = ((count_moe/1.645)/num) * 100)  # calculate cv for numerator count


#Upload to Postgres  ####
# set the connection to pgadmin
con3 <- connect_to_db("bold_vision")
table_name <- "demo_disaggregated_asian"
schema <- 'bv_2023'

indicator <- "Disaggregated Asian Youth"
source <- "American Community Survey 2017-2021 5-year PUMS estimates. See QA doc for details: W:\\Project\\OSI\\Bold Vision\\BV 2023\\Documentation\\QA_Demo_Asian.docx"

dbWriteTable(con3, c(schema, table_name), anhpi_subgroups_table_re,
             overwrite = TRUE, row.names = FALSE)

#comment on table and columns
comment <- paste0("COMMENT ON TABLE ", schema, ".", table_name,  " IS '", indicator, " from ", source, ".';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".geoid IS 'County fips';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".rate IS 'indicator rate';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".pop IS 'total population';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".num IS 'number of people by disaggregated non-Hispanic Asian Identity';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".rate_cv IS 'cv of indicator rate';")
print(comment)
dbSendQuery(con3, comment)

#disconnect
dbDisconnect(con3)