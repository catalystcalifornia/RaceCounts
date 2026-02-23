# Disaggregated Asian and NHPI from PUMS

# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf

# Install packages if not already installed
packages <- c("tidyverse", "data.table", "readxl","tidycensus", "srvyr","stringr", "openxlsx", "dplyr") 

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
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("mosaic")

qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\QA_Sheet_ANHPI_Pop_PUMS.docx"

#### Step 1: load the data ####

# PUMS Data
root <- "W:/Data/Demographics/PUMS/CA_2019_2023/"
indicator_name <- "Disaggregated Asian"
curr_yr <- 2023
start_yr <- curr_yr - 4
# created this excel document separate by opening PUMS Data Dictionary in excel and deleting everything but RAC3P, 
## updating col names, using text-to-columns to split description into separate cols. Adding binary cols for each subgroup.
data_dict <- paste0(root, "PUMS_Data_Dictionary_2019-2023_RAC3P.xlsx")

# set survey components
weight <- 'PWGTP'  # person weight
repwlist = rep(paste0(weight, 1:80)) # replicate weights


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


assm_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldl24 AS geoid, num_dist AS num_assm from crosswalks.puma_2020_state_assembly_2024")
sen_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldu24 AS geoid, num_dist AS num_sen from crosswalks.puma_2020_state_senate_2024")


##### GET PUMS DATA ######
people <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE,
                colClasses = list(character = c("PUMA", "RAC1P", "RAC3P", "RAC2P19", "RAC2P23", "HISP")))
orig_data <- people
# tmp_file <- tempfile(fileext = ".rds") # generate temporary filepath
# saveRDS(people, file = tmp_file)       # save the df to temporary filepath, in case need original data again
# orig_data <- readRDS(tmp_file)       # load original data if needed

# add nhpi value
people$RACNHPI <- case_when(
  people$RACNH == 1 | people$RACPI ==1 ~ 1,
  TRUE ~ 0
)

#### Step 1: Join subbroup descriptions to data ####

# pull in RAC3P metadata - adapted from W:\Data\Demographics\PUMS\CA_2019_2023\PUMS_Data_Dictionary_2019-2023.csv
race_codes3 <- read_excel(data_dict)  

# get unique list of RAC3P descriptions
# rac3p_unique <- as.data.frame(c(unique(race_codes3$Description1), unique(race_codes3$Description2), unique(race_codes3$Description3), 
#                                 unique(race_codes3$Description4), unique(race_codes3$Description5), unique(race_codes3$Description6))) %>%
#   unique() %>%
#   na.omit()
# View(rac3p_unique)

# join race descriptions to data
people <- left_join(people, race_codes3 %>% select(-var, -starts_with("Description"), "Description"), by="RAC3P") %>%
  #rename(subgroup=Description) %>% # clarify column we'll group by and set final dataset
  select(RAC3P, indian, chinese, filipino, japanese, korean, vietnamese, oth_asian, nat_hawaiian, chamorro, samoan, oth_nhpi, everything())


# check join
View(people[c("HISP","RAC1P","RACNHPI","ANC1P","ANC2P","Description","indian","chinese","filipino","japanese","korean","vietnamese","oth_asian","nat_hawaiian","chamorro","samoan","oth_nhpi")])
# table(people$indian, useNA = "always")
# table(people$chamorro, useNA = "always")
# table(people$oth_asian, useNA = "always")


#### Step 2: Join crosswalks to data ####

# Add state_geoid to people, add state_geoid to PUMA id, so it aligns with same vintage county-puma xwalk
people$state_geoid <- "06"
people$puma_id <- paste0(people$state_geoid, people$PUMA)

# join county crosswalk to data
ppl_cs <- left_join(people, county_crosswalk, by=c("puma_id" = "puma"))   # join FILTERED county-puma crosswalk

# join assm crosswalk to data
ppl_assm <- left_join(people, assm_crosswalk, by=c("puma_id" = "puma")) 
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
ppl_sen <- left_join(people, sen_crosswalk, by=c("puma_id" = "puma")) 
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
ppl_state <- people %>% rename(geoid = state_geoid) %>% mutate(geoname = 'California')

# put all geolevel dfs into 1 list
ppl_list <- list(ppl_cs = ppl_cs, ppl_assm = ppl_assm, ppl_sen = ppl_sen, ppl_state = ppl_state)



#### Step 3: Set up survey ####

anhpi_survey <- x %>%   
  as_survey_rep(
    variables = c(geoid, geoname, RAC3P, subgroup),  # dplyr::select grouping variables
    weights = weight,                       # person weight
    repweights = repwlist,                  # list of replicate weights
    combined_weights = TRUE,                # tells the function that replicate weights are included in the data
    mse = TRUE,                             # tells the function to calc mse
    type="other",                           # statistical method
    scale=4/80,                             # scaling set by ACS
    rscale=rep(1,80)                        # setting specific to ACS-scaling
  )


###### Asian subgroups, Asian alone Latinx-Incl ######




## Select Non-Latinx Asian and Non-Latinx NHPI people -- MOVE TO THE PART WHERE WE CALC THE ASIAN/NHPI STATS
# pums_data <- people %>% 
#   #filter for Asian, Nat Hawaiian, and Pac Islanders
#   filter((RACASN == 1 | RACNH == 1 | RACPI == 1))

###### Asian subgroups, Asian alone ######
asian_alone_subgroups_table <- anhpi_survey %>%
  group_by(geoid, subgroup) %>%   # group by asian subgroup description
  summarise(
    num = survey_total(na.rm=T),  # get the (survey weighted) count for the numerators
    rate = survey_mean()) %>%     # get the (survey weighted) proportion for the numerator
  left_join(anhpi_survey %>%                                        # left join in the denominators
              group_by(geoid) %>%                                   # group by geo
              summarise(pop = survey_total(na.rm=T))) %>%           # get the weighted total for overall geo
  mutate(rate=rate*100,
         rate_moe = rate_se*1.645*100,             # calculate the margin of error for the rate based on se
         rate_cv = ((rate_moe/1.645)/rate) * 100,  # calculate cv for rate
         count_moe = num_se*1.645,                 # calculate moe for numerator count based on se
         count_cv = ((count_moe/1.645)/num) * 100) # calculate cv for numerator count

# recode rates under 1% and create a list for recoding
other_list <- asian_alone_subgroups_table %>%
  filter(rate<1) %>%
  ungroup() %>%
  select(subgroup)
other_list<-other_list$subgroup


anhpi_alone_youth<-anhpi_alone_youth %>%
  mutate(subgroup=ifelse(subgroup %in% other_list, 'Another Asian Identity Alone',subgroup)) 

anhpi_survey <- anhpi_alone_youth %>%               
  as_survey_rep(
    variables = c(geoid, subgroup),   # dplyr::select grouping variables
    weights = weight,                       # person weight
    repweights = repwlist,                  # list of replicate weights
    combined_weights = TRUE,                # tells the function that replicate weights are included in the data
    mse = TRUE,                             # tells the function to calc mse
    type="other",                           # statistical method
    scale=4/80,                             # scaling set by ACS
    rscale=rep(1,80)                        # setting specific to ACS-scaling
  )


###### Asian subgroups, Asian alone ######
asian_alone_subgroups_table_re <- anhpi_survey %>%
  group_by(geoid, subgroup) %>%   # group by asian subgroup description
  summarise(
    num = survey_total(na.rm=T),   # get the (survey weighted) count for the numerators
    rate = survey_mean()) %>%      # get the (survey weighted) proportion for the numerator
  left_join(anhpi_survey %>%                                        # left join in the denominators
              group_by(geoid) %>%                                   # group by geo
              summarise(pop = survey_total(na.rm=T))) %>%           # get the weighted total for overall geo
  mutate(rate=rate*100,
         rate_moe = rate_se*1.645*100,             # calculate the margin of error for the rate based on se
         rate_cv = ((rate_moe/1.645)/rate) * 100,  # calculate cv for rate
         count_moe = num_se*1.645,                 # calculate moe for numerator count based on se
         count_cv = ((count_moe/1.645)/num) * 100) # calculate cv for numerator count


#Upload to Postgres  ####
indicator <- "Disaggregated Asian Subgroup Population"
source <- paste0("American Community Survey 2019-2023 5-year PUMS estimates for Asian Alone, Latinx-Inclusive. QA doc: ", qa_filepath)

dbWriteTable(con2, c(schema, table_name), asian_alone_subgroups_table_re,
             overwrite = TRUE, row.names = FALSE)

#comment on table and columns
comment <- paste0("COMMENT ON TABLE ", schema, ".", table_name,  " IS '", indicator, " from ", source, ".';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".geoid IS 'County fips';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".rate IS 'indicator rate';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".pop IS 'total population';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".num IS 'number of people by disaggregated Asian Identity';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".rate_cv IS 'cv of indicator rate';")
print(comment)
dbSendQuery(con2, comment)

#disconnect
dbDisconnect(con2)