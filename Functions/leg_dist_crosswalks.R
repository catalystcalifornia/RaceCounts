# Script used to prep crosswalk tables and export to postgres database by defining source_geo/source_geo_yr and target_geo/target_geo_yr: 
## 2020 Census Tracts to 2022 and 2024 State Senate & Assembly Districts
## 2020 ZCTAs to 2022 and 2024 State Senate & Assembly Districts
## 2020 PUMAs to 2024 State Senate & Assembly Districts
## 2020 Counties to 2024 State Senate & Assembly Districts
## 2020 Unified, Secondary, and Elementary School Districts to 2024 State Senate & Assembly Districts

### Set Up ### ------------------------------------------------------------------

#install packages if not already installed
packages <- c("dplyr", "RPostgreSQL", "janitor", "stringr", "usethis")  

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


### Fx to prep xwalk postgres tables --------------------------------------
prep_tables <- function(target_geo, target_geo_yr, source_geo, source_geo_yr) {

  # check if the crosswalk already in pgadmin
  table_name <- paste0(source_geo, "_", source_geo_yr, "_state_", target_geo, "_", target_geo_yr)  # generate postgres table name
  check_tables_sql <- paste0("SELECT * FROM information_schema.tables WHERE table_schema = '",
                             rda_schema, "' AND table_name ='", table_name, "';")
  conn <- connect_to_db("rda_shared_data") 
  check_tables <- dbGetQuery(conn, check_tables_sql)
  
  con <- connect_to_db("rda_shared_data")
  
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
  dbWriteTable(con, c(rda_schema, table_name), crosswalk,
               overwrite=FALSE, row.names=FALSE)
  print("Table exported to postgres.")

  table_comment <- paste0("COMMENT ON TABLE ",rda_schema,".",table_name," IS 'Created on ", Sys.Date(), ". Geocorr ", source_geo_yr, " ", source_name_long, " to ", target_geo_yr, " ", target_name_long, " crosswalk weighted by population. Allocation Factor columns represent % of source geo in target geo and vice versa. See QA doc for details: W:\\Project\\RACE COUNTS\\", rc_yr, "_", rc_schema, "\\Geographies\\QA_Geographies.docx';")  # print(table_comment)
  print(table_comment) 
  # send table comment
  dbSendQuery(conn = con, table_comment)
  print("Table comment sent to postgres.")

  column_comments <- paste0("COMMENT ON COLUMN ",rda_schema,".",table_name,".geo_id IS '", metadata[1,1] ,"';
                              COMMENT ON COLUMN ",rda_schema,".",table_name,".geo_name IS '", metadata[1,3] ,"';
                              COMMENT ON COLUMN ",rda_schema,".",table_name,".",district_id_col," IS '", metadata[1,2] ,"';
                              COMMENT ON COLUMN ",rda_schema,".",table_name,".pop", pop_yr," IS '", metadata[1,4] ,"';
                              COMMENT ON COLUMN ",rda_schema,".",table_name,".int_pt_lat IS '", metadata[1,5] ,"';
                              COMMENT ON COLUMN ",rda_schema,".",table_name,".int_pt_lon IS '", metadata[1,6] ,"';
                              COMMENT ON COLUMN ",rda_schema,".",table_name,".afact2 IS '", metadata[1,7] ,"';
                              COMMENT ON COLUMN ",rda_schema,".",table_name,".afact IS '", metadata[1,8] ,"';
                         ")
  print(column_comments) 
  # send column comments
  dbSendQuery(conn = con, column_comments)
  print("Column comments sent to postgres.")

  dbDisconnect(con)
  return(crosswalk) }
}


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


