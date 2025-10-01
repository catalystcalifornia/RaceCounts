### Teacher & Staff Diversity RC v7 ### 
## Analyst: Maria Khan

###### STEP 0: set up -----
#install packages if not already installed
packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "rpostgis", "usethis")
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
source("./Functions/rdashared_functions.R") #source functions used later
con <- connect_to_db("rda_shared_data")


# update each year
curr_yr <- '2023_24' 
acs_yr <- 2023  # used to get census geonames
rc_yr <- '2025'
rc_schema <- 'v7'
pop_screen <- 100   # geo + race combos with fewer than threshold # of students are suppressed

qa_filepath <-  "W:\\Project\\RACE COUNTS\\2025_v7\\Education\\QA_Sheet_Staff_Diversity.docx"
filepath = "https://www3.cde.ca.gov/demo-downloads/staff/stre2324.txt"  # CDE Staff Demographics data download URL
filepath2 = "https://www3.cde.ca.gov/demo-downloads/ce/cenroll2324.txt" # CDE Student Demographics data download URL


#Pull in xwalks first for leg dist to use later
## Update if newer xwalk available
xwalk_school_sen <- dbGetQuery(con, paste0("SELECT cdscode, ca_senate_district FROM crosswalks.cde_school_leg_districts_2022_23")) %>%
  mutate(leg_id = paste0('060', ca_senate_district),
         geolevel = 'sldu')

xwalk_school_assm <- dbGetQuery(con, paste0("SELECT cdscode, ca_assembly_district FROM crosswalks.cde_school_leg_districts_2022_23")) %>%
  mutate(leg_id = paste0('060', ca_assembly_district),
         geolevel = 'sldl')

#get county geoids to use later
census_api_key(census_key1, install = TRUE, overwrite = TRUE)
ca <- get_acs(geography = "county",
              variables = c("B01001_001"),
              state = "CA",
              year = acs_yr)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")
# View(ca)

###### STEP 1: get STAFF data and save it in pgadmin -----
fieldtype = 1:14 # specify which cols should be varchar, the rest will be assigned numeric

## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "education"
table_name <- paste0("cde_", curr_yr, "_staff_demo")
table_comment_source <- paste0("QA DOC: ", qa_filepath)
table_source <- "Downloaded from https://www.cde.ca.gov/ds/ad/filesstre.asp. Headers were cleaned of characters like /, ., ), and (. Cells with values of * were nullified. Created cdscode by concatenating county, district, and school codes"

## Run function to prep and export rda_shared_data table
#df <- get_cde_data(filepath, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db

## Get staff data for population values
staff <- dbGetQuery(con, paste0("select * from education.cde_", curr_yr, "_staff_demo"))
#View(staff)
staff <- staff %>% 
  mutate(districtcode = ifelse(!is.na(districtcode),paste0(staff$countycode,staff$districtcode), NA))

staff_df <- staff %>% filter(
  (aggregatelevel == "S" &
     stafftype == "ALL" &
     staffgender == "ALL") |
    
  (aggregatelevel != "S" &
     charterschool == "ALL" &
     stafftype == "ALL" &
     staffgender == "ALL" &
     schoolgradespan == "ALL" &
     dass == "ALL")
) %>%
  #select just fields we need
  select(-academicyear,-countycode, -charterschool, -dass, -stafftype, -staffgender, -schoolgradespan,
         -notreported) %>% #note that not reported data is excluded for the purpose of this project focusing on race. 
  #rename fields to column names we use 
  rename(total = totalstaffcount,
         nh_black = africanamerican, 
         nh_aian =  americanindianoralaskanative,
         nh_asian = asian, 
         nh_filipino = filipino,
         latino = hispanicorlatino,
         nh_pacisl = pacificislander,
         nh_white = white,
         nh_twoormor = twoormoreraces
         )

#add county geoids
staff_df <- left_join(x=staff_df,y=ca,by=c("countyname"="geoname")) %>%
  mutate(geoid = ifelse(aggregatelevel=="D", NA, geoid))

# add state geoid
staff_df <- within(staff_df, geoid[cdscode == '00000000000000'] <- '06') %>%
  select(geoid, everything())

#fix column names 
staff_df <- staff_df %>%
  mutate(geolevel = aggregatelevel,
         geoname = ifelse(aggregatelevel == 'D', districtname,
                   ifelse(aggregatelevel == 'C', countyname,
                   ifelse(aggregatelevel == 'T', "California",
                   ifelse(aggregatelevel == "S", schoolname,
                                               'NA')))),
         geoid = ifelse(geolevel == 'S' | geolevel == 'D', cdscode,
                 ifelse(geolevel == 'C', geoid,
                 ifelse(geolevel == 'T', '06', 'NA')))
         ) %>%
  select(geoid, geolevel, geoname, total:nh_twoormor)

  
###### STEP 2: get STUDENT ENROLLMENT data and save it in pgadmin -----
fieldtype = 1:12 # specify which cols should be varchar, the rest will be assigned numeric

## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
table_schema <- "education"
table_name <- paste0("cde_multigeo_enrollment_", curr_yr)
table_comment_source <- paste0("QA DOC: ", qa_filepath)
table_source <- "Downloaded from https://www.cde.ca.gov/ds/ad/filesenrcum.asp. Headers were cleaned of characters like /, ., ), and (. Cells with values of * were nullified. Created cdscode by concatenating county, district, and school codes"

## Run function to prep and export rda_shared_data table
#df <- get_cde_data(filepath2, fieldtype, table_schema, table_name, table_comment_source, table_source) # function to create and export rda_shared_table to postgres db

## Get student cumulative enrollment data for population values
enrollment <- dbGetQuery(con, paste0("select * from education.cde_multigeo_enrollment_", curr_yr))
#View(enrollment)
enrollment <- enrollment %>% 
  mutate(districtcode = ifelse(!is.na(districtcode),paste0(enrollment$countycode, enrollment$districtcode), NA))

enrollment_df <- enrollment %>% 
  filter(  
       (aggregatelevel == "S" & 
       reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW")) | 
       (aggregatelevel %in% c("C", "T", "D") & 
       charter == "All" & 
       reportingcategory %in% c("TA", "RB", "RI", "RA", "RF", "RH", "RP", "RT", "RW"))) %>%
  #select just fields we need
  select(aggregatelevel, cdscode, countyname, districtname, districtcode, schoolcode, schoolname, reportingcategory, cumulativeenrollment) 
  enrollment_df$cumulativeenrollment <- as.numeric(enrollment_df$cumulativeenrollment)
# View(enrollment_df)

#rename to the race/ethnicity codes
enrollment_df$reportingcategory <- gsub("TA", "total", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RB", "nh_black", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RI", "nh_aian", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RA", "nh_asian", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RF", "nh_filipino", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RH", "latino", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RP", "nh_pacisl", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RT", "nh_twoormor", enrollment_df$reportingcategory)
enrollment_df$reportingcategory <- gsub("RW", "nh_white", enrollment_df$reportingcategory)

#pivot wider
enrollment_wide <- enrollment_df %>% pivot_wider(names_from = reportingcategory, names_glue = "{reportingcategory}_pop", values_from = cumulativeenrollment)
enrollment_wide$countyname[enrollment_wide$countyname =='State'] <- 'California'   # update state row's countyname field values
#add county geoids
enrollment_wide <- left_join(x=enrollment_wide,y=ca,by=c("countyname"="geoname")) %>% mutate(geoid = ifelse(aggregatelevel=="D", NA, geoid))
# add state geoid
enrollment_wide <- within(enrollment_wide, geoid[countyname == 'California'] <- '06') %>%
  select(geoid, everything())
#fix column names 
enrollment_wide <- enrollment_wide %>%
  mutate(geolevel = aggregatelevel,
         geoname = ifelse(aggregatelevel == 'D', districtname,
                          ifelse(aggregatelevel == 'C', countyname,
                                 ifelse(aggregatelevel == 'T', "California",
                                        ifelse(aggregatelevel == "S", schoolname,
                                               'NA')))),
         geoid = ifelse(geolevel == 'S' | geolevel == 'D', cdscode,
                        ifelse(geolevel == 'C', geoid,
                               ifelse(geolevel == 'T', '06', 'NA')))
  ) %>%
  select(geoid, geolevel, geoname, nh_asian_pop:total_pop)

###### STEP 3: Calculate student to staff ratios -----
#join together to calculate rate
df_enroll_staff <- left_join(enrollment_wide, staff_df, 
                             by= c("geoid","geoname","geolevel")) 
#NOTE THAT staff data had more schools (about 341) than enrollment data, not sure why? You can check that by running table()

df_enroll_staff_senate <- df_enroll_staff %>%
  filter(geolevel == "S") %>%
  inner_join(xwalk_school_sen, by = c("geoid" = "cdscode")) %>%
  group_by(leg_id) %>%
  summarise(across(nh_asian_pop:nh_twoormor,\(x) sum(x, na.rm = TRUE)), .groups = "drop") %>%
  mutate(geolevel = "sldu",
         geoname = paste0("Senate District ", as.numeric(str_sub(leg_id, -2))),
         geoid = leg_id) %>%
  select(geoid, geolevel, geoname, nh_asian_pop:nh_twoormor)

df_enroll_staff_assembly <-  df_enroll_staff %>%
  filter(geolevel == "S") %>%
  inner_join(xwalk_school_assm,  by = c("geoid" = "cdscode")) %>%
  group_by(leg_id) %>%
  summarise(across(nh_asian_pop:nh_twoormor,\(x) sum(x, na.rm = TRUE)), .groups = "drop") %>%
  mutate(geolevel = "sldl",
         geoname = paste0("Assembly District ", as.numeric(str_sub(leg_id, -2))),
         geoid = leg_id) %>%
  select(geoid, geolevel, geoname, nh_asian_pop:nh_twoormor)


df_enroll_staff_final <- bind_rows(df_enroll_staff %>% filter(geolevel != "S"), df_enroll_staff_senate, df_enroll_staff_assembly) 


#screen data and set population screen threshold
df_enroll_staff_final <- df_enroll_staff_final %>% mutate(
  total_raw = ifelse(total_pop < pop_screen, NA, total),
  nh_black_raw = ifelse(nh_black_pop < pop_screen, NA, nh_black),
  nh_aian_raw = ifelse(nh_aian_pop < pop_screen, NA, nh_aian),
  nh_asian_raw = ifelse(nh_asian_pop < pop_screen, NA, nh_asian),
  nh_filipino_raw = ifelse(nh_filipino_pop < pop_screen, NA, nh_filipino),
  latino_raw =   ifelse(latino_pop < pop_screen, NA, latino),
  nh_pacisl_raw =  ifelse(nh_pacisl_pop < pop_screen, NA, nh_pacisl),
  nh_white_raw =  ifelse(nh_white_pop < pop_screen, NA, nh_white),
  nh_twoormor_raw =  ifelse(nh_twoormor_pop < pop_screen, NA, nh_twoormor),
  
  total_rate = ifelse(is.na(total_raw) | is.na(total_pop) | total_pop == 0, NA, (total_raw / total_pop)*100),
  nh_black_rate = ifelse(is.na(nh_black_raw) | is.na(nh_black_pop) | nh_black_pop == 0, NA, (nh_black_raw / nh_black_pop)*100),
  nh_aian_rate = ifelse(is.na(nh_aian_raw) | is.na(nh_aian_pop) |  nh_aian_pop == 0, NA, (nh_aian_raw / nh_aian_pop)*100),
  nh_asian_rate = ifelse(is.na(nh_asian_raw) | is.na(nh_asian_pop) | nh_asian_pop == 0, NA, (nh_asian_raw / nh_asian_pop)*100),
  nh_filipino_rate = ifelse(is.na(nh_filipino_raw) | is.na(nh_filipino_pop) | nh_filipino_pop == 0, NA, (nh_filipino_raw / nh_filipino_pop)*100),
  latino_rate = ifelse(is.na(latino_raw) | is.na(latino_pop) | latino_pop == 0, NA, (latino_raw / latino_pop)*100),
  nh_pacisl_rate = ifelse(is.na(nh_pacisl_raw) | is.na(nh_pacisl_pop) | nh_pacisl_pop == 0, NA, (nh_pacisl_raw / nh_pacisl_pop)*100),
  nh_white_rate = ifelse(is.na(nh_white_raw) | is.na(nh_white_pop) | nh_white_pop == 0, NA, (nh_white_raw / nh_white_pop)*100),
  nh_twoormor_rate = ifelse(is.na(nh_twoormor_raw) | is.na(nh_twoormor_pop) | nh_twoormor_pop == 0, NA, (nh_twoormor_raw / nh_twoormor_pop)*100)) %>%
  select(-all_of(c(
    "total", "nh_black", "nh_aian", "nh_asian", "nh_filipino", 
    "latino", "nh_pacisl", "nh_white", "nh_twoormor"
  )))
    
# get school district geoids (NCES District ID) - pull in active district records w/ geoids and names from CDE schools' list
districts <- dbGetQuery(con, paste0("SELECT cdscode, ncesdist AS geoid FROM education.cde_public_schools_",curr_yr," WHERE ncesdist <> '' AND right(cdscode,7) = '0000000' AND statustype = 'Active'")) # district,

df_final <- left_join(df_enroll_staff_final, districts, by = c('geoid' = 'cdscode')) %>% 
  mutate(cdscode = geoid,
  geoid=ifelse(geolevel == "D", geoid.y, geoid)) %>%
  select(-c(geoid.y)) %>% 
  distinct() %>%  # combine distinct county and district geoid matched df's
  relocate(geoid, geoname, cdscode, geolevel)

df_final <- filter(df_final, !is.na(geoid) & geoid != "No Data") # remove records without fips codes
# View(df_final)

d <- df_final

###### STEP 4: Calculate Race Counts Stats -----
#set source for RC Functions script
source("./Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS APPROPRIATE: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update d$asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity
# View(d)

#split STATE into separate table and format id, name columns 
state_table <- d[d$geolevel == 'T', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_id" = "geoid", "state_name" = "geoname") %>%
  select(-cdscode)
View(state_table)

#remove state from county table
county_table <- d[d$geolevel == 'C', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_id" = "geoid", "county_name" = "geoname") %>%
  select(-cdscode)
View(county_table)

#remove county/state from place table 
city_table <- d[d$geolevel == 'D', ] 

#calculate DISTRICT z-scores
city_table <- calc_z(city_table)
city_table <- calc_ranks(city_table)
city_table <- city_table %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname")
View(city_table)

#remove county/state/district from place table 
upper_leg_table <- d[d$geolevel == 'sldu', ]
lower_leg_table <- d[d$geolevel == 'sldl', ]

#calculate LEGISLATIVE DISTRICTS z-scores and bind
upper_leg_table <- calc_z(upper_leg_table)
upper_leg_table <- calc_ranks(upper_leg_table)
upper_leg_table <- upper_leg_table
#View(upper_leg_table)

lower_leg_table <- calc_z(lower_leg_table)
lower_leg_table <- calc_ranks(lower_leg_table)
lower_leg_table <- lower_leg_table
#View(lower_leg_table)

leg_table <- rbind(upper_leg_table, lower_leg_table) %>% dplyr::rename("leg_id" = "geoid", "leg_name" = "geoname") %>%
  select(-cdscode)
View(leg_table)

###update info for postgres tables###
county_table_name <- paste0("arei_educ_staff_diversity_county_", rc_yr)
state_table_name <- paste0("arei_educ_staff_diversity_state_", rc_yr)
city_table_name <- paste0("arei_educ_staff_diversity_district_", rc_yr)
leg_table_name <- paste0("arei_educ_staff_diversity_leg_", rc_yr)

indicator <- "Staff and Teacher Diversity Count and Rate. This data is"
source <- paste0("CDE ", curr_yr, " from https://www.cde.ca.gov/ds/ad/fsstre.asp. QA doc: ", qa_filepath)

# send tables to postgres
to_postgres(county_table,state_table)
city_to_postgres(city_table)
leg_to_postgres(leg_table)
