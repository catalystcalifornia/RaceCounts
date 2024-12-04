## Functions used for RC Housing indicators ##

## install and load packages ------------------------------
packages <- c("data.table", "stringr", "dplyr", "RPostgreSQL", "dbplyr", 
              "srvyr", "tidycensus", "rpostgis", "tidyr", "readxl", "sf", "tidyverse", "usethis")
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
con <- connect_to_db("rda_shared_data")


# Download and Export CHAS data to postgres -------------------------------------------
## Update variables each year
## To update "_file" variables below, first download data from https://www.huduser.gov/portal/datasets/cp.html
### Select most current data year and download, state, county, census places, and CT files in csv format 
data_yrs <- "2017_21"
table_name <- paste0("hud_chas_cost_burden_multigeo_", data_yrs)
metadata_file <- "CHAS-data-dictionary-17-21.xlsx"
tract_file <- "2017thru2021-140-csv/140/Table9.csv"
place_file <- "2017thru2021-160-csv/160/Table9.csv"
county_file <- "2017thru2021-050-csv/050/Table9.csv"
state_file <- "2017thru2021-040-csv/040/Table9.csv"

# does not need to be updated
data_folder <- paste0("W:/Data/Housing/HUD/CHAS/",substr(data_yrs, 1,4), "-20", substr(data_yrs, 6,7), "/")

# import downloaded data tables
table140 <- read.csv(paste0(data_folder, tract_file)) %>% mutate(geolevel = "census tract", geoid = substr(geoid, 10, 20)) # tract
table160 <- read.csv(paste0(data_folder, place_file)) %>% mutate(geolevel = "city", geoid = substr(geoid, 10, 16))         # place
table050 <- read.csv(paste0(data_folder, county_file)) %>% mutate(geolevel = "county", geoid = substr(geoid, 10, 14))      # county
table040 <- read.csv(paste0(data_folder, state_file)) %>% mutate(geolevel = "state", geoid = substr(geoid, 10, 11))        # state

# View(head(table160))
# View(head(table140))
# View(head(table050))
# View(head(table040))

chas_data <- bind_rows(table140, table160, table050, table040) %>% filter(st==6) %>% select(source, geoid, name, geolevel, st, starts_with("t9_"), cnty, place, tract)
names(chas_data) <- tolower(names(chas_data))

dbWriteTable(con, c("housing", table_name), chas_data,
             overwrite = FALSE, row.names = FALSE)

# create table and column comments
df_metadata <- read_excel(paste0(data_folder, metadata_file), sheet = "Table 9")                   # import downloaded data dictionary
df_metadata <- df_metadata %>% rename(label = "Column Name") %>% mutate(label = tolower(label))

df_metadata$variable <- paste(df_metadata[[3]], df_metadata[[4]], df_metadata[[5]], sep = ", ")    # create column comment content
df_metadata <- df_metadata %>% select(c(label, variable))

# create metadata for cols in chas_data that are not in the metadata file
new_cols <- data.frame(
  label = c("source", "geoid", "name", "geolevel", "st", "cnty", "place", "tract"), 
  variable = c("Data years", "FIPS code", "Geography Name", "Geography level: state, county, place, tract", "State", "County", "Census Place", "Census Tract"),
  stringsAsFactors = FALSE
  )

df_metadata_ <- rbind(new_cols, df_metadata) 

# join metadata with data column names
df_names <- data.frame(names(chas_data))  # pull in df col names 
colcomments <- df_metadata_ %>% right_join(df_names, by =c("label" = "names.chas_data."))
View(colcomments)  # moe columns will NOT have column comments

# make character vectors for column names and descriptions. 
colcomments_charvar <- colcomments$variable
colname_charvar <- colcomments$label

# This loop writes comments for all columns, then sends to the postgres db.
  for (i in seq_along(colname_charvar)){
    sqlcolcomment <-
      paste0("COMMENT ON COLUMN ", "housing.", table_name, ".",
             colname_charvar[[i]], " IS '", colcomments_charvar[[i]], "'; COMMENT ON COLUMN ", "housing.", table_name, ".",
             colname_charvar[[i]], " IS '", colcomments_charvar[[i]], "';" )
    
    # send sql comment to database
    dbSendQuery(conn = con, sqlcolcomment)
  }


# send table comment to postgres
table_comment <- paste0("COMMENT ON TABLE housing.", table_name, " IS 'Created ", Sys.Date(), ". Housing affordability data from HUD CHAS (", data_yrs, "). Downloaded from https://www.huduser.gov/portal/datasets/cp.html';")
dbSendQuery(conn = con, table_comment)    
  
dbDisconnect(con)