# Disaggregated Asian and NHPI Pop from PUMS

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
source("./MOSAIC/Functions/pums_fx.R")
con <- connect_to_db("rda_shared_data")
con2 <- connect_to_db("mosaic")
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\QA_Sheet_MOSAIC_Pop_PUMS.docx"
ancestry_list <- read_excel("W:\\Project\\RACE COUNTS\\2025_v7\\Demographics\\Asian_NHPI_Ancestry.xlsx", sheet = "ancestry") # list of ANHPI ANC1P/ANC2P codes

#### Step 1: Define Variables ####

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


##### Step 2: GET PUMA CROSSWALKS ######
crosswalk <- dbGetQuery(con, "select county_id AS geoid, county_name AS geoname, geo_id AS puma, geo_name AS puma_name, num_county, afact, afact2 from crosswalks.puma_2022_county_2020")

## Drop counties that have only 1 PUMA that is shared with another county that also has only 1 PUMA.
dupe_pumas <- crosswalk %>% 
  filter(num_county == 1) %>%   # keep only counties with 1 PUMA
  group_by(puma) %>% 
  summarise(n=n()) %>%   		    # count the # of counties assigned to each PUMA
  filter(n > 1)		   		        # keep only counties that have only 1 PUMA that is shared with another county with only 1 PUMA

county_crosswalk <- crosswalk %>%   
  subset(!(puma %in% dupe_pumas$puma)) # remove only counties that have only 1 PUMA that is shared with another county with only 1 PUMA


# assm_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldl24 AS geoid, num_dist AS num_assm from crosswalks.puma_2020_state_assembly_2024")
# sen_crosswalk <- dbGetQuery(con, "select geo_id AS puma, sldu24 AS geoid, num_dist AS num_sen from crosswalks.puma_2020_state_senate_2024")


##### Step 3: GET PUMS DATA ######
# Load ONLY the PUMS columns needed for this indicator
cols <- colnames(fread(paste0(root, "psam_p06.csv"), nrows=0))    # get all PUMS cols 
cols_wts <- grep(paste0("^", weight, "*"), cols, value = TRUE)    # filter for PUMS weight colnames

people <- fread(paste0(root, "psam_p06.csv"), header = TRUE, data.table = FALSE,
                select = c(cols_wts, "RT", "SERIALNO", "PUMA", "RAC1P", "RAC3P", "RAC2P19", "RAC2P23", "HISP", "ANC1P", "ANC2P", "RACASN", "RACNH", "RACPI"),
                colClasses = list(character = c("PUMA", "RAC1P", "RAC3P", "RAC2P19", "RAC2P23", "HISP", "ANC1P", "ANC2P", "RACASN", "RACNH", "RACPI")))
orig_data <- people
# tmp_file <- tempfile(fileext = ".rds") # generate temporary filepath
# saveRDS(people, file = tmp_file)       # save the df to temporary filepath, in case need original data again
# orig_data <- readRDS(tmp_file)         # load original data if needed

# add nhpi value
people$RACNHPI <- case_when(
  people$RACNH == 1 | people$RACPI ==1 ~ 1,
  TRUE ~ 0
)

# Add state_geoid to people, add state_geoid to PUMA id, so it aligns with same vintage county-puma xwalk
people$state_geoid <- "06"
people$puma_id <- paste0(people$state_geoid, people$PUMA)

#### Step 4: Join subgroup descriptions to data ####
people <- anhpi_reclass(people, curr_yr, ancestry_list)  # returns list containing people (reclassified pums data) and aapi_incl
list2env(people, .GlobalEnv)

# Add a new column for each anc_label, populated with 1 or 0
for (label in aapi_incl$anc_label) {
  people[[label]] <- as.integer(people$anc_label.x == label | people$anc_label.y == label)
}

# Add a new column for asian and nhpi, populated with 1 or 0
# Create a named lookup vector: anc_label -> asian value
asian_lookup <- setNames(aapi_incl$asian, aapi_incl$anc_label)

# Populate the new 'asian' column
people$asian <- as.integer(
  (people$anc_label.x %in% names(asian_lookup[asian_lookup == 1])) |
    (people$anc_label.y %in% names(asian_lookup[asian_lookup == 1]))
)

# Create a named lookup vector: anc_label -> nhpi value
nhpi_lookup <- setNames(aapi_incl$nhpi, aapi_incl$anc_label)

# Populate the new 'nhpi' column
people$nhpi <- as.integer(
  (people$anc_label.x %in% names(nhpi_lookup[nhpi_lookup == 1])) |
    (people$anc_label.y %in% names(nhpi_lookup[nhpi_lookup == 1]))
)


## check a few of the new ancestry & asian/nhpi cols
table(chinese = people$chinese, asian_race = people$RACASN)
table(chinese = people$chinese, asian_anc = people$asian)
table(asian_anc = people$asian, asian_race = people$RACASN)  # 4452 people w/ asian ancestry who are not coded race = Asian
#         asian_race
# asian_anc       0       1
#         0 1481976   79536
#         1    4452  287465

table(samoan = people$samoan, nhpi_race = people$RACNHPI)
table(samoan = people$samoan, nhpi_anc = people$nhpi)
table(nhpi_anc = people$nhpi, nhpi_race = people$RACNHPI)    # 941 people w/ nhpi ancestry who are not coded race = NHPI
#       nhpi_race
# nhpi_anc       0       1
#       0 1838025    7536
#       1     941    6927

#### Step 5: Join crosswalks to data ####
# join county crosswalk to data: county, puma, state
ppl_cs <- left_join(people, county_crosswalk, by=c("puma_id" = "puma"))   # join FILTERED county-puma crosswalk

ppl_state <- people %>% rename(geoid = state_geoid) %>% mutate(geoname = 'California')

ppl_puma <- left_join(people, county_crosswalk %>% select(puma, puma_name), by=c("puma_id" = "puma")) %>%  # join puma names
  rename(geoid = puma_id, geoname = puma_name)




#### Step 3: Set up for PUMS calc fx ####
# # list subgroup col names for calcs
# vars <- c("indian","chinese","filipino","japanese","korean","vietnamese","oth_asian","nat_hawaiian","chamorro","samoan","oth_nhpi")

## STATE
  # run fx to create survey and calc pop rate denominators
  state_list <- pums_pop_srvy_denom(ppl_state, weight, repwlist)
  
  # add list elements to Environment - these df's are used in calc_pums_pop fx
  list2env(state_list, envir = .GlobalEnv)
  
  # run PUMS calcs
  pop_table_state <- map_dfr(vars, calc_pums_pop)


## COUNTY
  # run fx to create survey and calc pop rate denominators
  county_list <- pums_pop_srvy_denom(ppl_cs, weight, repwlist)
  
  # add list elements to Environment - these df's are used in calc_pums_pop fx
  list2env(county_list, envir = .GlobalEnv)
  
  # run PUMS calcs
  pop_table_county <- map_dfr(vars, calc_pums_pop)


## PUMA
  # run fx to create survey and calc pop rate denominators
  puma_list <- pums_pop_srvy_denom(ppl_puma, weight, repwlist)
  
  # add list elements to Environment - these df's are used in calc_pums_pop fx
  list2env(puma_list, envir = .GlobalEnv)
  
  # run PUMS calcs
  pop_table_puma <- map_dfr(vars, calc_pums_pop)



asian_pop <- bind_rows(pop_table_state %>% filter(group == 'asian'), pop_table_county %>% filter(group == 'asian'), pop_table_puma %>% filter(group == 'asian'))
nhpi_pop <- bind_rows(pop_table_state %>% filter(group == 'nhpi'), pop_table_county %>% filter(group == 'nhpi'), pop_table_puma %>% filter(group == 'nhpi'))



# ASIAN: Upload to Postgres  ####
table_name <- 'asian_pop'
indicator <- "Disaggregated Asian Subgroup Population"
source <- paste0("American Community Survey 2019-2023 5-year PUMS estimates for Asian & Asian subgroups AOIC, Latinx-Inclusive (RAC3P) at state, county, PUMA level. QA doc: ", qa_filepath)

dbWriteTable(con2, c(schema, table_name), asian_subgroups,
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



# NHPI: Upload to Postgres  ####
table_name <- 'nhpi_pop'
indicator <- "Disaggregated Asian Subgroup Population"
source <- paste0("American Community Survey 2019-2023 5-year PUMS estimates for Asian & Asian subgroups AOIC, Latinx-Inclusive (RAC3P) at state, county, PUMA level. QA doc: ", qa_filepath)

dbWriteTable(con2, c(schema, table_name), nhpi_subgroups,
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