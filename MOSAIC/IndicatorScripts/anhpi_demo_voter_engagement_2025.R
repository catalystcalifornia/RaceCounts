### MOSAIC: Voter Engagement RC v7 ### 

#install packages if not already installed
packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "usethis", "openxlsx", "RPostgres", "data.table")  

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

# define variables used in several places that must be updated each year
curr_yr <- "2019_24"  # must keep same format
dwnld_url <- "https://ask.chis.ucla.edu/"
rc_schema <- "v7"
yr <- "2025"
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Democracy\\QA_Voter_Engagement - MOSAIC.docx"

chis_dir <- ("W:/Data/Democracy/CHIS/")

#get data for Total population
total_df = read.xlsx(paste0(chis_dir,"/Voter_Engagement/2019_23/AskCHIStotal.xlsx"), sheet=1, startRow=5, rows=c(5:8)) 

#format row headers
total_df_rownames <- c("measure","total_yes", "total_no")
total_df[1:3,1] <- total_df_rownames[1:3]


#get data for Asian subgroups
asian_df = read.xlsx(paste0(chis_dir,"/Voter_Engagement/",curr_yr,"/Voter_asian7.xlsx"), sheet=1, startRow=8, rows=c(8,10:23))

#format row headers
asian_rownames <- c("chinese_yes", "japanese_yes", "korean_yes", "filipino_yes", "south_asian_yes", "vietnamese_yes", "other_asian_yes", 
                    "chinese_no", "japanese_no", "korean_no", "filipino_no", "south_asian_no", "vietnamese_no", "other_asian_no")
asian_df[1:14,1] <- asian_rownames[1:14]

#asian column names come in different for some reason so updating
names(asian_df) <- names(total_df)

#combine
df <- rbind(total_df, asian_df)

# run the rest of CHIS prep -----------------------------------------------
##### including formatting column names, screen using flags, adding geonames, etc.
source("./MOSAIC/Functions/CHIS_Functions.R")

df <- fix_colnames(df) # this wasn't necessary in other CHIS scripts but is necessary here for some reason
df_subset <- prep_chis(df)
View(df_subset)

d <- df_subset


#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'
d$geolevel = case_when(d$geoname == "California" ~ "state", .default = "county")

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update $asbest line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
View(d)


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)

state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

# DROP TOTAL AND TOTAL RATE-DERIVED COLS
county_table <- county_table %>% select(-c(starts_with("performance"), "quadrant"))
state_table <- state_table


###info for postgres tables - automatically updates###
county_table_name <- paste0("asian_demo_voter_engagement_county_",yr)
state_table_name <- paste0("asian_demo_voter_engagement_state_",yr)
indicator <- "Voter engagement in national, state, and local elections - US Citizens (%) Asian Ethnic Groups ONLY"
source <- paste0("AskCHIS ", curr_yr, " Pooled Estimates. ", dwnld_url, " QA doc: ", qa_filepath)

#send tables to postgres
to_postgres(county_table,state_table,"mosaic")


# Prep Total and Raced Data ------------------------------------------------
##### Ran this after the original Asian disagg postgres tables had been created.
con <- connect_to_db("mosaic")
source("./Functions/CHIS_Functions.R")  # regular RC CHIS fx
new_chis_dir <- "W:/Data/Democracy/CHIS/Voter_Engagement/2019_24"

asian_df <- dbGetQuery(con, "SELECT * FROM v7.asian_demo_voter_engagement_state_2025")

#get data for Total population
total_df = read.xlsx(paste0(new_chis_dir, "/Voter_Total.xlsx"), sheet=1, startRow=5, rows=c(5:8)) 

#format row headers
total_df_rownames <- c("measure","total_yes", "total_no")
total_df[1:3,1] <- total_df_rownames[1:3]

#get data for Hispanic/non-Hispanic races, excluding AIAN, NHPI, and SWANA
races_df = read.xlsx(paste0(new_chis_dir, "/Voter_race.xlsx"), sheet=1, startRow=8, rows=c(8,10:12,14,16:19,21,23))

#format row headers
races_rownames <- c("latino_no", "nh_white_no", "nh_black_no", "nh_asian_no", "nh_twoormor_no", 
                    "latino_yes", "nh_white_yes", "nh_black_yes", "nh_asian_yes", "nh_twoormor_yes")
races_df[1:10,1] <- races_rownames[1:10]


#get data for ALL-AIAN
aian_df = read.xlsx(paste0(new_chis_dir, "/Voter_aian.xlsx"), sheet=1, startRow=8, rows=c(8,10,12))

#format row headers
aian_rownames <- c("aian_no", "aian_yes")
aian_df[1:2,1] <- aian_rownames[1:2]


#get data for ALL-NHPI
pacisl_df = read.xlsx(paste0(new_chis_dir, "/Voter_nhpi.xlsx"), sheet=1, startRow=8, rows=c(8,10,12))

#format row headers
pacisl_rownames <- c("pacisl_no", "pacisl_yes")
pacisl_df[1:2,1] <- pacisl_rownames[1:2]


#get data for ALL-SWANA -- Note this is 2022-24 ONLY
swana_df = read.xlsx(paste0(new_chis_dir, "/Voter_mena.xlsx"), sheet=1, startRow=8, rows=c(8,10,12))

#format row headers
swana_rownames <- c("swana_no", "swana_yes")
swana_df[1:2,1] <- swana_rownames[1:2]


#combine & prep
df <- rbind(total_df, races_df, aian_df, pacisl_df, swana_df)
df_subset <- prep_chis(df)

df_county <- df_subset %>% filter(geoid !='06') %>% rename(county_id = geoid) %>% select(-geoname)
df_state <- df_subset %>% filter(geoid =='06') %>% select(-geoid, -geoname)

# Append Total and Raced Data to Existing Postgres Tables ------------------------------------------------
# 1. Write data frame to temp table
dbWriteTable(con, "temp_data", df_state, temporary = TRUE, overwrite = TRUE)

# Append state dataframe to the existing state table
# Map R types to Postgres types
r_to_pg_type <- function(x) {
  if (is.integer(x))        return("INTEGER")
  if (is.numeric(x))        return("DOUBLE PRECISION")
  if (is.logical(x))        return("BOOLEAN")
  if (inherits(x, "Date"))  return("DATE")
  return("TEXT")
}

# Add each new column and update its value from df_state
state_table_name <- "asian_demo_voter_engagement_state_2025"

for (col in names(df_state)) {
  pg_type <- r_to_pg_type(df_state[[col]])
  val     <- df_state[[col]][1]
  
  # 1. Add the column
  dbExecute(con, sprintf(
    'ALTER TABLE %.% ADD COLUMN IF NOT EXISTS "%s" %s',
    rc_schema, state_table_name, col, pg_type
  ))
  
  # 2. Update the single row with the value from df_state
  dbExecute(con,
            sprintf('UPDATE %.% SET "%s" = $1', col),
            rc_schema, state_table_name, list(val)
  )
}


# Append county dataframe to the existing county table
# Identify new columns to append (excluding county_id and anything already in PG)
county_table_name <- "asian_demo_voter_engagement_county_2025"
existing_cols <- dbListFields(con, Id(schema = rc_schema, table = county_table_name))
new_cols <- setdiff(names(df_county), c(existing_cols))  # county_id already in existing_cols, so it's excluded automatically

# Add each new column then update row-by-row, matched on county_id
for (col in new_cols) {
  pg_type <- r_to_pg_type(df_county[[col]])
  
  # 1. Add the column
  dbExecute(con, sprintf(
    'ALTER TABLE %s.% ADD COLUMN IF NOT EXISTS "%s" %s',
    rc_schema, county_table_name, col, pg_type
  ))
}

# Write df_county to a temp table, then join once per column -- or better, join all at once:

# Upload df_county as a temporary table
dbWriteTable(con, "df_county_tmp", df_county, overwrite = TRUE, temporary = TRUE)

# Build SET clause for all new columns
set_clause <- paste(
  sprintf('"%s" = tmp."%s"', new_cols, new_cols),
  collapse = ", "
)

# Single UPDATE joining on county_id
update_sql <- sprintf(
  'UPDATE %.% AS tgt
   SET %s
   FROM pg_temp.df_county_tmp AS tmp
   WHERE tgt.county_id = tmp.county_id',
  rc_schema, county_table_name, set_clause
)

# send new cols to postgres
dbExecute(con, update_sql)


## Check that old columns in updated tables match columns in _orig tables. _orig tables are copies of the postgres tables w/ only Asian subgroups.
county_orig <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".", county_table_name, "_orig"))
state_orig <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".", state_table_name, "_orig"))

county <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".", county_table_name))
state <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".", state_table_name))

existing_cols <- dbListFields(con, Id(schema = rc_schema, table = paste0(county_table_name, "_orig")))
county <- county %>% select(existing_cols)

all.equal(county_orig %>% arrange(county_id), county %>% arrange(county_id))  # should be TRUE

existing_cols <- dbListFields(con, Id(schema = rc_schema, table = paste0(state_table_name, "_orig")))
state <- state %>% select(existing_cols)

all.equal(state_orig, state)  # should be TRUE

dbDisconnect(con)













# 2. Update the existing table from temp table
dbExecute(con, "
  UPDATE v7.asian_demo_voter_engagement_state_2025 
  SET new_col_name = temp_data.new_col_name 
  FROM temp_data 
  WHERE v7.asian_demo_voter_engagement_state_2025.state_id = temp_data.state_id")





# Identify which columns to append (exclude any that already exist in the PG table)
existing_cols <- dbListFields(con, Id(schema = "v7", table = "asian_demo_voter_engagement_county_2025"))
new_cols <- setdiff(names(df_county), existing_cols)