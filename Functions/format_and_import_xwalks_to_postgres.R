# Make crosswalks and export to postgres for
# 2020 Census Tracts to 2022 and 2024 State Senate & Assembly Districts
# 2020 ZCTAs to 2022 and 2024 State Senate & Assembly Districts
# 2020 PUMAs to 2024 State Senate & Assembly Districts
# 2020 Counties to 2024 State Senate & Assembly Districts
# 2020 Unified, Secondary, and Elementary School Districts to 2024 State Senate & Assembly Districts

### Set Up ### ------------------------------------------------------------------

#install packages if not already installed
packages <- c("dplyr", "RPostgreSQL", "janitor", "stringr")  

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

# Fx to prep xwalk postgres tables
prep_tables <- function(filepath, id_col, table_name, dist_name, non_dist_geo) {
  
  # read in tract-leg dist crosswalk, removing metadata row, and keeping character FIPS codes
  all_content = readLines(geocorr_filepath)
  skip_second = all_content[-2]
  if (non_dist_geo == "Census Tract") {
  crosswalk <- read.csv(textConnection(skip_second), 
                        colClasses = c(rep("character", 4), rep("numeric", 5))) %>% clean_names()
  print("Crosswalk imported.")
  } else if (non_dist_geo == "ZCTA" | non_dist_geo == "County") {
    crosswalk <- read.csv(textConnection(skip_second), 
                          colClasses = c(rep("character", 3), rep("numeric", 5))) %>% clean_names()
    print("Crosswalk imported.")
  } else if (non_dist_geo == "PUMA" | non_dist_geo == "Unified School District" |
             non_dist_geo == "Elementary School District" | non_dist_geo == "Secondary School District") {
    crosswalk <- read.csv(textConnection(skip_second), 
                          colClasses = c(rep("character", 5), rep("numeric", 5))) %>% clean_names()
    print("Crosswalk imported.")
  }
  
  
  #read in metadata
  metadata <- read.csv(geocorr_filepath, nrows = 1) %>% clean_names()
  print("Metadata imported.")
  if (non_dist_geo == "PUMA" | non_dist_geo == "Unified School District" |
      non_dist_geo == "Elementary School District" | non_dist_geo == "Secondary School District") {
    metadata <- metadata %>% select(-state, -stab)
    print("metadata cleaned.")
  } else if (non_dist_geo == "Census Tract") {
    metadata <- metadata %>% select(-county) %>% mutate(county_name = "Tract End")
    print("metadata cleaned.")
}
  
  
  #clean crosswalks
  if (non_dist_geo == "Census Tract") {
  #create full tract id, removing periods, and move it to front of the data frame
  crosswalk <- crosswalk %>% 
    mutate(geo_id = paste0(county, substr(tract, 1, 4), substr(tract, 6, 7)),
           geo_name = tract) %>%
    select(geo_id, geo_name, district_id_col, pop20, int_pt_lat, int_pt_lon, afact2, afact)
  print("Crosswalk cleaned.")
  } else if (non_dist_geo == "ZCTA") {
  # rename geo_id, name, filter out parts of districts not in ZCTAs or out of state
    crosswalk <- crosswalk %>% 
      filter(zip_name != "[not in a ZCTA]") %>%
      filter(!str_starts(zcta, "8")) %>%
      rename(geo_id = zcta, geo_name = zip_name) %>%
      select(geo_id, geo_name, district_id_col, pop20, int_pt_lat, int_pt_lon, afact2, afact)
    print("Crosswalk NOT cleaned.")
  } else if (non_dist_geo == "PUMA") {
    #rename geo_id, name, and select only columns we want
    crosswalk <- crosswalk %>% 
      rename(geo_id = puma22, geo_name = puma22name) %>%
      select(geo_id, geo_name, district_id_col, pop20, int_pt_lat, int_pt_lon, afact2, afact)
    print("Crosswalk cleaned.")
  } else if (non_dist_geo == "County") {
    #rename geo_id and select only columns we want
    crosswalk <- crosswalk %>% 
      rename(geo_id = county, geo_name = county_name) %>%
      select(geo_id, geo_name, district_id_col, pop20, int_pt_lat, int_pt_lon, afact2, afact)
    print("Crosswalk cleaned.")
  } else if (non_dist_geo == "Unified School District") {
    #rename geo_id and select only columns we want
    crosswalk <- crosswalk %>% 
      rename(geo_id = sduni20, geo_name = uschlnm20) %>%
      select(geo_id, geo_name, district_id_col, pop20, int_pt_lat, int_pt_lon, afact2, afact)
    print("Crosswalk cleaned.")
  } else if (non_dist_geo == "Elementary School District") {
    #rename geo_id and select only columns we want
    crosswalk <- crosswalk %>% 
      rename(geo_id = sdelem20, geo_name = eschlnm20) %>%
      select(geo_id, geo_name, district_id_col, pop20, int_pt_lat, int_pt_lon, afact2, afact)
    print("Crosswalk cleaned.")
  } else if (non_dist_geo == "Secondary School District") {
    #rename geo_id and select only columns we want
    crosswalk <- crosswalk %>% 
      rename(geo_id = sdsec20, geo_name = sschlnm20) %>%
      select(geo_id, geo_name, district_id_col, pop20, int_pt_lat, int_pt_lon, afact2, afact)
    print("Crosswalk cleaned.")
  }
  
  # export to Postgres
  con <- connect_to_db("rda_shared_data")
  
  dbWriteTable(con, c(rda_schema, table_name_for_postgres), crosswalk,
               overwrite=TRUE, row.names=FALSE)
  print("Table exported.")
  
  table_comment <- paste0("COMMENT ON TABLE ",rda_schema,".",table_name_for_postgres," IS 'Created on ", Sys.Date(), ". Geocorr ",paste(non_dist_geo_year, non_dist_geo)," to ",paste(dist_year, dist_name)," crosswalk weighted by population. Allocation Factor columns represent % of source geo in target geo and vice versa. See QA doc for details: W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\QA_Geographies.docx';")
  print(table_comment)
  
  # send table comment 
  dbSendQuery(conn = con, table_comment)
  print("Table comment applied.")
  
  column_comments <- paste0("COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".geo_id IS '", metadata[1,1] ,"';
                          COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".geo_name IS '", metadata[1,3] ,"';
                          COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".",district_id_col," IS '", metadata[1,2] ,"';
                          COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".pop20 IS '", metadata[1,4] ,"';
                          COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".int_pt_lat IS '", metadata[1,5] ,"';
                          COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".int_pt_lon IS '", metadata[1,6] ,"';
                          COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".afact2 IS '", metadata[1,7] ,"';
                          COMMENT ON COLUMN ",rda_schema,".",table_name_for_postgres,".afact IS '", metadata[1,8] ,"';
                         ")
  
  # send column comments 
  dbSendQuery(conn = con, column_comments)
  print("Column comments applied.")
  
  dbDisconnect(con)
  
}


### 2022 STATE SENATE - TRACT ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_tract_to_senate_2022.csv"
district_id_col <- "sldu22"
table_name_for_postgres <- "tract_state_senate_2022"
dist_name <- "State Senate"
non_dist_geo <- "Census Tract"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2022 STATE ASSEMBLY - TRACT ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_tract_to_assembly_2022.csv"
district_id_col <- "sldl22"
table_name_for_postgres <- "tract_state_assembly_2022"
dist_name <- "State Assembly"
non_dist_geo <- "Census Tract"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE SENATE - TRACT ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_tract_to_senate_2024.csv"
district_id_col <- "sldu24"
table_name_for_postgres <- "tract_state_senate_2024"
dist_name <- "State Senate"
non_dist_geo <- "Census Tract"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE ASSEMBLY - TRACT ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_tract_to_assembly_2024.csv"
district_id_col <- "sldl24"
table_name_for_postgres <- "tract_state_assembly_2024"
dist_name <- "State Assembly"
non_dist_geo <- "Census Tract"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2022 STATE SENATE - ZCTA ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_zcta_to_senate_2022.csv"
district_id_col <- "sldu22"
table_name_for_postgres <- "zcta_state_senate_2022"
dist_name <- "State Senate"
non_dist_geo <- "ZCTA"
dist_year <- "2022"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2022 STATE ASSEMBLY - ZCTA ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_zcta_to_assembly_2022.csv"
district_id_col <- "sldl22"
table_name_for_postgres <- "zcta_state_assembly_2022"
dist_name <- "State Assembly"
non_dist_geo <- "ZCTA"
dist_year <- "2022"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE SENATE - ZCTA ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_zcta_to_senate_2024.csv"
district_id_col <- "sldu24"
table_name_for_postgres <- "zcta_state_senate_2024"
dist_name <- "State Senate"
non_dist_geo <- "ZCTA"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE ASSEMBLY - ZCTA ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_zcta_to_assembly_2024.csv"
district_id_col <- "sldl24"
table_name_for_postgres <- "zcta_state_assembly_2024"
dist_name <- "State Assembly"
non_dist_geo <- "ZCTA"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE SENATE - PUMA ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_puma_to_senate_2024.csv"
district_id_col <- "sldu24"
table_name_for_postgres <- "puma_state_senate_2024"
dist_name <- "State Senate"
non_dist_geo <- "PUMA"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE ASSEMBLY - PUMA ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_puma_to_assembly_2024.csv"
district_id_col <- "sldl24"
table_name_for_postgres <- "puma_state_assembly_2024"
dist_name <- "State Assembly"
non_dist_geo <- "PUMA"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE SENATE - COUNTY ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_county_to_senate_2024.csv"
district_id_col <- "sldu24"
table_name_for_postgres <- "county_state_senate_2024"
dist_name <- "State Senate"
non_dist_geo <- "County"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE ASSEMBLY - COUNTY ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_county_to_assembly_2024.csv"
district_id_col <- "sldl24"
table_name_for_postgres <- "county_state_assembly_2024"
dist_name <- "State Assembly"
non_dist_geo <- "County"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE SENATE - UNIFIED SCHOOL DISTRICT ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_usd_to_senate_2024.csv"
district_id_col <- "sldu24"
table_name_for_postgres <- "usd_state_senate_2024"
dist_name <- "State Senate"
non_dist_geo <- "Unified School District"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE ASSEMBLY - UNIFIED SCHOOL DISTRICT ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_usd_to_assembly_2024.csv"
district_id_col <- "sldl24"
table_name_for_postgres <- "usd_state_assembly_2024"
dist_name <- "State Assembly"
non_dist_geo <- "Unified School District"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE SENATE - ELEMENTARY SCHOOL DISTRICT ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_esd_to_senate_2024.csv"
district_id_col <- "sldu24"
table_name_for_postgres <- "esd_state_senate_2024"
dist_name <- "State Senate"
non_dist_geo <- "Elementary School District"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE ASSEMBLY - ELEMENTARY SCHOOL DISTRICT ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_esd_to_assembly_2024.csv"
district_id_col <- "sldl24"
table_name_for_postgres <- "esd_state_assembly_2024"
dist_name <- "State Assembly"
non_dist_geo <- "Elementary School District"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)



### 2024 STATE SENATE - SECONDARY SCHOOL DISTRICT ### ----------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_ssd_to_senate_2024.csv"
district_id_col <- "sldu24"
table_name_for_postgres <- "ssd_state_senate_2024"
dist_name <- "State Senate"
non_dist_geo <- "Secondary School District"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


### 2024 STATE ASSEMBLY - SECONDARY SCHOOL DISTRICT ### --------------------------------------------------

geocorr_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Geographies\\geocorr2022_ssd_to_assembly_2024.csv"
district_id_col <- "sldl24"
table_name_for_postgres <- "ssd_state_assembly_2024"
dist_name <- "State Assembly"
non_dist_geo <- "Secondary School District"
dist_year <- "2024"
non_dist_geo_year <- "2020"

prep_tables(geocorr_filepath, district_id_col, table_name_for_postgres, dist_name, non_dist_geo)


