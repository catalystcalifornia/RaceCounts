# Leg to CA Region Crosswalk --------------------------------------------

### Set Up ### ------------------------------------------------------------------

#install packages if not already installed
packages <- c("dplyr", "RPostgres", "janitor", "stringr", "usethis", "DBI")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

#set up Postgres connection but connect and disconnect only when needed later
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")
rda_schema <- "crosswalks"

options(scipen=999)

### Update each year ------------------------------------------------------
rc_schema <- 'v7'
rc_yr <- '2025'
pop_yr <- '2020'  # update, as appropriate, based on which pop vintage was used for pop-weighting on Geocorr
leg_yr <- '2024'  # leg district shape vintage
ct_sen_xwalk <- "tract_2020_state_senate_2024"
ct_assm_xwalk <- "tract_2020_state_assembly_2024"
sldu_id <- 'sldu24'
sldl_id <- 'sldl24'

qa_filepath <- 'W:\\Project\\RACE COUNTS\\2025_v7\\Findings\\Leg_Region_Xwalk.docx'

sen_pop_threshold <- .4    # district is assigned to region containing >.4 of district pop, this means a district can be assigned to 1 or 2 regions
assm_pop_threshold <- .4   # district is assigned to region containing >.4 of district pop, this means a district can be assigned to 1 or 2 regions


### Create crosswalks ------------------------------------------------------

### Import regions
regions <- dbGetQuery(con, paste0("SELECT county_id, county, region, urban_type FROM ", rc_schema, ".arei_county_region_urban_type"))

### Import ct-leg dist xwalks and ct pop
sldu_ct_pop <- dbGetQuery(con2, paste0("SELECT geo_id AS ct_geoid, ", sldu_id, " AS leg_id, pop20, afact2 FROM crosswalks.", ct_sen_xwalk)) %>%
  mutate(geolevel = 'sldu', county_id = str_sub(ct_geoid, 1, 5))

sldl_ct_pop <- dbGetQuery(con2, paste0("SELECT geo_id AS ct_geoid, ", sldl_id, " AS leg_id, pop20, afact2 FROM crosswalks.", ct_assm_xwalk)) %>%
  mutate(geolevel = 'sldl', county_id = str_sub(ct_geoid, 1, 5))

# get sen dist pop by county
sldu_county_pop <- sldu_ct_pop %>% group_by(leg_id, county_id, geolevel) %>% dplyr::summarise(ct_total_pop = sum(pop20))
sldu_county_pop2 <- sldu_county_pop %>% left_join(regions, by = "county_id") %>% filter(geolevel == 'sldu') 
sldu_county_pop3 <- sldu_county_pop2 %>% group_by(leg_id, region, geolevel, urban_type) %>%
  dplyr::summarise(ct_total_pop = sum(ct_total_pop))

# get sen dist pop by region
sldu_region_pop <- sldu_county_pop3 %>% group_by(leg_id) %>%
  dplyr::summarise(total_pop = sum(ct_total_pop))

sldu_county_pop3 <- sldu_county_pop3 %>% left_join(sldu_region_pop, by = "leg_id") %>%
  mutate(pct_total_pop = ct_total_pop / total_pop) %>%
  arrange(leg_id, desc(pct_total_pop))

sldu_region <- sldu_county_pop3 %>% filter(pct_total_pop > sen_pop_threshold) %>%
  group_by(leg_id) %>%
  mutate(rank = dense_rank(desc(pct_total_pop))) %>%
  arrange(leg_id, pct_total_pop)


print(case_when(length(unique(sldu_region$leg_id)) != 40 ~ "There is at least one Senate district with more than 1 region assigned.",
                length(unique(sldu_region$leg_id)) == 40 ~ "There are no Senate districts with more than 1 region assigned."))


# get assm dist pop by county
sldl_county_pop <- sldl_ct_pop %>% group_by(leg_id, county_id, geolevel) %>% dplyr::summarise(ct_total_pop = sum(pop20))
sldl_county_pop2 <- sldl_county_pop %>% left_join(regions, by = "county_id") %>% filter(geolevel == 'sldl') 
sldl_county_pop3 <- sldl_county_pop2 %>% group_by(leg_id, region, geolevel,urban_type) %>%
  dplyr::summarise(ct_total_pop = sum(ct_total_pop))

# get assm dist pop by region
sldl_region_pop <- sldl_county_pop3 %>% group_by(leg_id) %>%
  dplyr::summarise(total_pop = sum(ct_total_pop))

sldl_county_pop3 <- sldl_county_pop3 %>% left_join(sldl_region_pop, by = "leg_id") %>%
  mutate(pct_total_pop = ct_total_pop / total_pop) %>%
  arrange(leg_id, region, desc(pct_total_pop))

sldl_region <- sldl_county_pop3 %>% filter(pct_total_pop > assm_pop_threshold) %>%
  group_by(leg_id) %>%
  mutate(rank = dense_rank(desc(pct_total_pop))) %>%
  arrange(leg_id, desc(pct_total_pop))

print(case_when(length(sldl_region$leg_id) != 80 ~ "There is at least one Assembly district with more than 1 region assigned.",
                length(sldl_region$leg_id) == 80 ~ "There are no Assembly districts with more than 1 region assigned."))


### Export crosswalks ------------------------------------------------------
xwalk_schema <- 'crosswalks'

# # Export Senate-Region xwalk with urban_type
# sen_table <- paste0('state_senate_', leg_yr, "_region_urban")
# 
# # dbWriteTable(con2,
# #              Id(schema = xwalk_schema, table = sen_table),
# #              sldu_region, overwrite = FALSE)
# 
# # comment on table and columns
# ind <- paste0("State Senate (", leg_yr, ") to RC regions crosswalk. Districts are assigned to regions that contain > ", sen_pop_threshold, " of district pop (", pop_yr, "). NOTE: A district may be assigned to 1 or 2 regions. Row with rank = 1 is the region with the highest pct of district population. QA doc: ", qa_filepath)
# source <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\Functions\\leg_dist_crosswalks.R.")
# column_names <- colnames(sldu_region)
# column_comments <- c(
#   'Legislative geoid',
#   'RC region from racecounts.[schema].arei_county_region_urban_type',
#   'sldu = State Senate, sldl = State Assembly',
#   'Leg population within the region based on tract population',
#   'Total Leg population',
#   'Percent of total Leg population within the region', 
#   'Rank = 1 is the region containing the highest percentage of total Leg population') 
# 
# # add_table_comments(con2, xwalk_schema, sen_table, ind, source, qa_filepath, column_names, column_comments)
# 
# 
# # Export Assm-Region xwalk
# assm_table <- paste0('state_assembly_', leg_yr, "_region_urban")
# 
# # dbWriteTable(con2,
# #              Id(schema = xwalk_schema, table = assm_table),
# #              sldl_region, overwrite = FALSE)
# 
# # comment on table and columns
# ind <- paste0("State Assembly (", leg_yr, ") to RC regions crosswalk. Districts are assigned to regions that contain > ", assm_pop_threshold, " of district pop (", pop_yr, "). NOTE: A district may be assigned to 1 or 2 regions. Row with rank = 1 is the region with the highest pct of district population. QA doc: ", qa_filepath)
# source <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\Functions\\leg_dist_crosswalks.R.")
# column_names <- colnames(sldl_region)
# column_comments <- c(
#   'Legislative geoid',
#   'RC region from racecounts.[schema].arei_county_region_urban_type',
#   'sldu = State Senate, sldl = State Assembly',
#   'Leg population within the region based on tract population',
#   'Total Leg population',
#   'Percent of total Leg population within the region', 
#   'Rank = 1 is the region containing the highest percentage of total Leg population') 
# 
# # add_table_comments(con2, xwalk_schema, assm_table, ind, source, qa_filepath, column_names, column_comments)


####### Export Senate-Region xwalk but remove urban type ------------
sldl_region <- sldl_region %>% select(-urban_type)
sldu_region <- sldu_region%>% select(-urban_type)

# Export Senate-Region xwalk
sen_table <- paste0('state_senate_', leg_yr, "_region")

# dbWriteTable(con2,
#              Id(schema = xwalk_schema, table = sen_table),
#              sldu_region, overwrite = FALSE)

# comment on table and columns
ind <- paste0("State Senate (", leg_yr, ") to RC regions crosswalk. Districts are assigned to regions that contain > ", sen_pop_threshold, " of district pop (", pop_yr, "). NOTE: A district may be assigned to 1 or 2 regions. Row with rank = 1 is the region with the highest pct of district population. QA doc: ", qa_filepath)
source <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\Functions\\leg_dist_crosswalks.R.")
column_names <- colnames(sldu_region)
column_comments <- c(
  'Legislative geoid',
  'RC region from racecounts.[schema].arei_county_region_urban_type',
  'sldu = State Senate, sldl = State Assembly',
  'Leg population within the region based on tract population',
  'Total Leg population',
  'Percent of total Leg population within the region', 
  'Rank = 1 is the region containing the highest percentage of total Leg population') 

# add_table_comments(con2, xwalk_schema, sen_table, ind, source, qa_filepath, column_names, column_comments)


# Export Assm-Region xwalk
assm_table <- paste0('state_assembly_', leg_yr, "_region")

# dbWriteTable(con2,
#              Id(schema = xwalk_schema, table = assm_table),
#              sldl_region, overwrite = FALSE)

# comment on table and columns
ind <- paste0("State Assembly (", leg_yr, ") to RC regions crosswalk. Districts are assigned to regions that contain > ", assm_pop_threshold, " of district pop (", pop_yr, "). NOTE: A district may be assigned to 1 or 2 regions. Row with rank = 1 is the region with the highest pct of district population. QA doc: ", qa_filepath)
source <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\Functions\\leg_dist_crosswalks.R.")
column_names <- colnames(sldl_region)
column_comments <- c(
  'Legislative geoid',
  'RC region from racecounts.[schema].arei_county_region_urban_type',
  'sldu = State Senate, sldl = State Assembly',
  'Leg population within the region based on tract population',
  'Total Leg population',
  'Percent of total Leg population within the region', 
  'Rank = 1 is the region containing the highest percentage of total Leg population') 

# add_table_comments(con2, xwalk_schema, assm_table, ind, source, qa_filepath, column_names, column_comments)


# Leg to Census Geo Crosswalks --------------------------------------------

# Script used to prep crosswalk tables and export to postgres database by defining source_geo/source_geo_yr and target_geo/target_geo_yr: 
## 2020 Census Tracts to 2022 and 2024 State Senate & Assembly Districts
## 2020 ZCTAs to 2022 and 2024 State Senate & Assembly Districts
## 2020 PUMAs to 2024 State Senate & Assembly Districts
## 2020 Counties to 2024 State Senate & Assembly Districts
## 2020 Unified, Secondary, and Elementary School Districts to 2024 State Senate & Assembly Districts
## 2022 PUMAs to 2020 Counties

### Set Up ### ------------------------------------------------------------------

#install packages if not already installed
packages <- c("dplyr", "RPostgres", "janitor", "stringr", "usethis", "DBI")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

#set up Postgres connection but connect and disconnect only when needed later
source("W:\\RDA Team\\R\\credentials_source.R")
rda_schema <- "crosswalks"

### Update each year ------------------------------------------------------
rc_schema <- 'v7'
rc_yr <- '2025'
pop_yr <- '20' # 2-digit format, so '20' means '2020'. update, as appropriate, based on which pop vintage was used for pop-weighting on Geocorr

# Note: Crosswalks downloaded from: MO Census Data Center at https://mcdc.missouri.edu/applications/geocorr2022.html
geocorr_yr <- '2022' # version of geocorr used. update, as appropriate.

### Source Geos - Will not need updates unless adding a new source geo. ------------------------------------------------------
# specified by user when calling fx, used in file and table names
geo_names_short <- c("tract", "zcta", "puma", "county", "usd", "esd", "ssd", "assembly", "senate")
# names used in table comments
geo_names_long <- c("Census Tract", "ZCTA", "PUMA", "County", "Unified School District", "Elementary School District", "Secondary School District", "State Assembly", "State Senate")

# create short names to long names table
geo_names <- as.data.frame(geo_names_short) %>% cbind(geo_names_long)
geo_names  # check needed geonames are present, and that all are correct


### Fx's to prep xwalk postgres tables --------------------------------------
prep_tables <- function(target_geo, target_geo_yr, source_geo, source_geo_yr) {  # For Assm/Sen-Based Xwalks
  
  # check if the crosswalk already in pgadmin
  table_name <- paste0(source_geo, "_", source_geo_yr, "_state_", target_geo, "_", target_geo_yr)  # generate postgres table name
  check_tables_sql <- paste0("SELECT * FROM information_schema.tables WHERE table_schema = '",
                             rda_schema, "' AND table_name ='", table_name, "';")
  con <- connect_to_db("rda_shared_data") 
  check_tables <- dbGetQuery(con, check_tables_sql)
  
  if (nrow(check_tables)==1) {          # the crosswalk already exists in db
    print("The crosswalk already exists in pgadmin. Aborting function...")
    dbDisconnect(con)
    
  } else if (nrow(check_tables)==0) {   # the crosswalk does not exist in db
    print("The crosswalk is not already in pgadmin. Creating now...")
    
    # generate filepath to crosswalk csv
    geocorr_filepath <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\Geographies\\geocorr", geocorr_yr, "_", source_geo, "_", source_geo_yr, "_to_", target_geo, "_", target_geo_yr, ".csv")
    
    source_name_long <- filter(geo_names, geo_names_short == source_geo) %>% select(geo_names_long)  # assign source geo long name based on source_geo
    target_name_long <- filter(geo_names, geo_names_short == target_geo) %>% select(geo_names_long)  # assign target geo long name based on target_geo
    
    district_id_col <- if (target_geo == 'assembly') {paste0("sldl", substr(target_geo_yr, 3,4))     # generate leg dist (target) geoid column name
    } else                     {paste0("sldu", substr(target_geo_yr, 3,4))}
    
    # read in tract-leg dist crosswalk, removing metadata row, and keeping character FIPS codes
    all_content = readLines(geocorr_filepath)
    skip_second = all_content[-2]
    if (source_geo == "tract") {
      crosswalk <- read.csv(textConnection(skip_second), 
                            colClasses = c(rep("character", 4), rep("numeric", 5))) %>% clean_names()
      print("Crosswalk csv imported to R.")
    } else if (source_geo == "zcta" | 
               source_geo == "county") {
      crosswalk <- read.csv(textConnection(skip_second), 
                            colClasses = c(rep("character", 3), rep("numeric", 5))) %>% clean_names()
      print("Crosswalk csv imported to R.")
    } else if (source_geo == "puma" | 
               source_geo == "usd" |
               source_geo == "esd" | 
               source_geo == "ssd") {
      crosswalk <- read.csv(textConnection(skip_second), 
                            colClasses = c(rep("character", 5), rep("numeric", 5))) %>% clean_names()
      print("Crosswalk csv imported to R.")
    }
    
    
    # read in metadata
    metadata <- read.csv(geocorr_filepath, nrows = 1) %>% clean_names()
    print("Metadata csv imported to R.")
    if (source_geo == "puma" | 
        source_geo == "unsd" |
        source_geo == "elsd" | 
        source_geo == "scsd") {
      metadata <- metadata %>% select(-state, -stab)
      print("Metadata cleaned.")
    } else if 
    (source_geo == "tract") {
      metadata <- metadata %>% select(-county) %>% mutate(county_name = "Tract End")
      print("Metadata cleaned.")
    }
    
    
    # clean crosswalks
    if (source_geo == "tract") {
      # create full tract id, removing periods
      crosswalk <- crosswalk %>% 
        mutate(geo_id = paste0(county, substr(tract, 1, 4), substr(tract, 6, 7)),
               geo_name = tract)  
    } else if (source_geo == "zcta") {
      # rename geo_id, name, filter out parts of districts not in ZCTAs or out of state
      crosswalk <- crosswalk %>% 
        filter(zip_name != "[not in a ZCTA]") %>%   # drop parts of districts not in ZCTAs
        filter(!str_starts(zcta, "8")) %>%          # drop ZCTAs not really in CA
        rename(geo_id = zcta, geo_name = zip_name) 
    } else if (source_geo == "puma") {
      # rename geo_id, name, and select only columns we want
      crosswalk <- crosswalk %>% 
        rename_with(~'geo_name', ends_with('name')) %>%
        rename_with(~'geo_id', starts_with('puma')) 
    } else if (source_geo == "county") {
      # rename geo_id and select only columns we want
      crosswalk <- crosswalk %>% 
        rename(geo_id = county, 
               geo_name = county_name) 
    } else if (source_geo == "usd") {
      # rename geo_id and select only columns we want
      crosswalk <- crosswalk %>% 
        rename_with(~'geo_name', starts_with('uschlnm')) %>%
        rename_with(~'geo_id', starts_with('sduni')) 
    } else if (source_geo == "esd") {
      # rename geo_id and select only columns we want
      crosswalk <- crosswalk %>% 
        rename_with(~'geo_name', starts_with('eschlnm')) %>%
        rename_with(~'geo_id', starts_with('sdelem')) 
    } else if (source_geo == "ssd") {
      # rename geo_id and select only columns we want
      crosswalk <- crosswalk %>% 
        rename_with(~'geo_name', starts_with('sschlnm')) %>%
        rename_with(~'geo_id', starts_with('sdsec')) 
    }
    
    print("Crosswalk cleaned.")
    
    # rearrange columns and drop unneeded columns
    crosswalk <- crosswalk %>% select(geo_id, geo_name, district_id_col, paste0("pop", pop_yr), int_pt_lat, int_pt_lon, afact2, afact)
    
    # export to Postgres
    dbWriteTable(con, Id(rda_schema, table_name), crosswalk,
                 overwrite=FALSE, row.names=FALSE)
    print("Table exported to postgres.")
    
    # Start a transaction
    dbBegin(con)
    
    # Add table / column comments
    indicator <- paste0("COMMENT ON TABLE ",rda_schema,".",table_name," IS 'Created on ", Sys.Date(), ".")  
    source <- paste0("Geocorr ", source_geo_yr, " ", source_name_long, " to ", target_geo_yr, " ", target_name_long, " crosswalk weighted by population. Allocation Factor columns represent % of source geo in target geo and vice versa.")
    qa_filepath <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\Economic\\QA_Living_Wage.docx")
    column_names <- colnames(crosswalk)
    column_comments <- c(metadata[1,1],
                         metadata[1,3],
                         metadata[1,2],
                         metadata[1,4],
                         metadata[1,5],
                         metadata[1,6],
                         metadata[1,7],
                         metadata[1,8] )
    print(column_comments)     
    
    add_table_comments(con, schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
    
    # Commit the transaction if everything succeeded
    dbCommit(con)
    
    print("Table and column comments sent to postgres.")
    
    dbDisconnect(con)
    return(crosswalk) }
}


prep_tables_county <- function(target_geo, target_geo_yr, source_geo, source_geo_yr) { # For County-Based Xwalks
  
  # check if the crosswalk already in pgadmin
  table_name <- paste0(source_geo, "_", source_geo_yr, "_", target_geo, "_", target_geo_yr)  # generate postgres table name
  check_tables_sql <- paste0("SELECT * FROM information_schema.tables WHERE table_schema = '",
                             rda_schema, "' AND table_name ='", table_name, "';")
  con <- connect_to_db("rda_shared_data") 
  check_tables <- dbGetQuery(con, check_tables_sql)
  
  if (nrow(check_tables)==1) {          # the crosswalk already exists in db
    print("The crosswalk already exists in pgadmin. Aborting function...")
    dbDisconnect(con)
    
  } else if (nrow(check_tables)==0) {   # the crosswalk does not exist in db
    print("The crosswalk is not already in pgadmin. Creating now...")
    
    # generate filepath to crosswalk csv
    geocorr_filepath <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\Geographies\\geocorr", geocorr_yr, "_", source_geo, "_", source_geo_yr, "_to_", target_geo, "_", target_geo_yr, ".csv")
    
    source_name_long <- filter(geo_names, geo_names_short == source_geo) %>% select(geo_names_long)  # assign source geo long name based on source_geo
    target_name_long <- filter(geo_names, geo_names_short == target_geo) %>% select(geo_names_long)  # assign target geo long name based on target_geo
    
    # read in tract-leg dist crosswalk, removing metadata row, and keeping character FIPS codes
    all_content = readLines(geocorr_filepath)
    skip_second = all_content[-2]
    if (source_geo == "zcta") {
      crosswalk <- read.csv(textConnection(skip_second), 
                            colClasses = c(rep("character", 3), rep("numeric", 5))) %>% clean_names()
      print("Crosswalk csv imported to R.")
    } else if (source_geo == "puma") {
      crosswalk <- read.csv(textConnection(skip_second), 
                            colClasses = c(rep("character", 6), rep("numeric", 5))) %>% clean_names()
      print("Crosswalk csv imported to R.")
    }
    
    
    # read in metadata
    metadata <- read.csv(geocorr_filepath, nrows = 1) %>% clean_names()
    print("Metadata csv imported to R.")
    
    metadata <- metadata %>% rename(county_id = county) %>% 
      select(-state, -stab)
    #metadata <- tolower(metadata)
    print("Metadata cleaned.")
    
    
    # clean crosswalks
    if (source_geo == "zcta") {
      # rename geo_id, name, filter out parts of districts not in ZCTAs or out of state
      crosswalk <- crosswalk %>% 
        filter(zip_name != "[not in a ZCTA]") %>%   # drop parts of districts not in ZCTAs
        filter(!str_starts(zcta, "8")) %>%          # drop ZCTAs not really in CA
        rename(geo_id = zcta, geo_name = zip_name) 
    } else if (source_geo == "puma") {
      # rename geo_id, name, and select only columns we want
      crosswalk <- crosswalk %>% 
        rename(geo_name = (paste0('puma',substr(source_geo_yr, 3,4),'name')), 
               geo_id = paste0('puma', substr(source_geo_yr, 3,4))) %>%
        mutate(county_name = gsub(" CA", "", county_name),
               geo_id = paste0("06", geo_id))       # add state fips to puma geoid
    } else if (source_geo == "usd") {
      # rename geo_id and select only columns we want
      crosswalk <- crosswalk %>% 
        rename_with(~'geo_name', starts_with('uschlnm')) %>%
        rename_with(~'geo_id', starts_with('sduni')) 
    } else if (source_geo == "esd") {
      # rename geo_id and select only columns we want
      crosswalk <- crosswalk %>% 
        rename_with(~'geo_name', starts_with('eschlnm')) %>%
        rename_with(~'geo_id', starts_with('sdelem')) 
    } else if (source_geo == "ssd") {
      # rename geo_id and select only columns we want
      crosswalk <- crosswalk %>% 
        rename_with(~'geo_name', starts_with('sschlnm')) %>%
        rename_with(~'geo_id', starts_with('sdsec')) 
    }
    
    crosswalk <- crosswalk %>% rename(county_id = county) %>%
      select(-c(state, stab))
    
    print("Crosswalk cleaned.")
    
    # rearrange columns and drop unneeded columns
    crosswalk <- crosswalk %>% select(geo_id, geo_name, county_id, county_name, paste0("pop", pop_yr), int_pt_lat, int_pt_lon, afact2, afact)
    
    if (source_geo == 'puma') {
      count_pumas <- crosswalk %>% group_by(county_id, county_name) %>% summarise(num_county = n()) %>% select(-county_name)
      crosswalk <- crosswalk %>% left_join(count_pumas, by = 'county_id', relationship = 'many-to-many')
    } else {   }
    
    # export to Postgres
    dbWriteTable(con, Id(rda_schema, table_name), crosswalk,
                 overwrite=FALSE, row.names=FALSE)
    print("Table exported to postgres.")
    
    # Add table / column comments
    indicator <- paste0(source_geo_yr, " ", source_name_long, " to ", target_geo_yr, " ", target_name_long, " crosswalk weighted by population. Allocation Factor columns represent % of source geo in target geo and vice versa")  
    source <- paste0("Geocorr ", geocorr_yr)
    qa_filepath <- paste0("W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\Economic\\QA_Living_Wage.docx")
    column_names <- colnames(crosswalk)
    if (source_geo == 'puma') {
      column_comments <- c(metadata[1,1],
                           metadata[1,4],
                           metadata[1,2],
                           metadata[1,3],
                           metadata[1,5],
                           metadata[1,6],
                           metadata[1,7],
                           metadata[1,8],
                           metadata[1,9],
                           'Number of PUMAs that intersect the county') }
    else {column_comments <- c(metadata[1,1],
                               metadata[1,4],
                               metadata[1,2],
                               metadata[1,3],
                               metadata[1,5],
                               metadata[1,6],
                               metadata[1,7],
                               metadata[1,8],
                               metadata[1,9])}
    print(column_comments)     
    
    add_table_comments(con, rda_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
    print("Table and column comments sent to postgres.")
    
    dbDisconnect(con)
    return(crosswalk) }
}


### COUNTY - PUMA ### ----------------------------------------------------
target_geo <- "county"
target_geo_yr <- "2020" # update data vintage as needed

source_geo <- "puma"
source_geo_yr <- "2022" # update data vintage as needed

puma_county <- prep_tables_county(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE SENATE - TRACT ### ----------------------------------------------------
target_geo <- "senate"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "tract"
source_geo_yr <- "2020" # update data vintage as needed

tract_senate <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE ASSEMBLY - TRACT ### --------------------------------------------------
target_geo <- "assembly"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "tract"
source_geo_yr <- "2020" # update data vintage as needed

tract_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE SENATE - ZCTA ### ----------------------------------------------------
target_geo <- "senate"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "zcta"
source_geo_yr <- "2020" # update data vintage as needed

zcta_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE ASSEMBLY - ZCTA ### --------------------------------------------------
target_geo <- "assembly"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "zcta"
source_geo_yr <- "2020" # update data vintage as needed

zcta_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)



### STATE SENATE - PUMA ### ----------------------------------------------------
target_geo <- "senate"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "puma"
source_geo_yr <- "2020" # update data vintage as needed

puma_senate <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE ASSEMBLY - PUMA ### --------------------------------------------------
target_geo <- "assembly"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "puma"
source_geo_yr <- "2020" # update data vintage as needed

puma_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE SENATE - COUNTY ### ----------------------------------------------------
target_geo <- "senate"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "county"
source_geo_yr <- "2020" # update data vintage as needed

county_senate <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE ASSEMBLY - COUNTY ### --------------------------------------------------
target_geo <- "assembly"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "county"
source_geo_yr <- "2020" # update data vintage as needed

county_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE SENATE - UNIFIED SCHOOL DISTRICT ### ----------------------------------------------------
target_geo <- "senate"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "usd"
source_geo_yr <- "2020" # update data vintage as needed

usd_senate <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)

### STATE ASSEMBLY - UNIFIED SCHOOL DISTRICT ### --------------------------------------------------
target_geo <- "assembly"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "usd"
source_geo_yr <- "2020" # update data vintage as needed

usd_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE SENATE - ELEMENTARY SCHOOL DISTRICT ### ----------------------------------------------------
target_geo <- "senate"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "esd"
source_geo_yr <- "2020" # update data vintage as needed

esd_senate <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE ASSEMBLY - ELEMENTARY SCHOOL DISTRICT ### --------------------------------------------------
target_geo <- "assembly"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "esd"
source_geo_yr <- "2020" # update data vintage as needed

esd_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)


### STATE SENATE - SECONDARY SCHOOL DISTRICT ### ----------------------------------------------------
target_geo <- "senate"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "ssd"
source_geo_yr <- "2020" # update data vintage as needed

ssd_senate <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)



### STATE ASSEMBLY - SECONDARY SCHOOL DISTRICT ### --------------------------------------------------
target_geo <- "assembly"
target_geo_yr <- "2024" # update data vintage as needed

source_geo <- "ssd"
source_geo_yr <- "2020" # update data vintage as needed

ssd_assembly <- prep_tables(target_geo, target_geo_yr, source_geo, source_geo_yr)