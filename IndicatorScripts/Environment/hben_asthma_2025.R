### Asthma RC v6 ### 

#install packages if not already installed
packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "usethis", "openxlsx", "RPostgreSQL")  

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

# define variables used in several places that must be updated each year
curr_yr <- "2011_23"  # must keep same format
dwnld_url <- "https://ask.chis.ucla.edu/"
rc_schema <- "v7"
yr <- "2025"

setwd("W:/Data/Health/CHIS/")


#get data for Total population
total_df = read.xlsx(paste0("Asthma/",curr_yr,"/Asthma_total.xlsx"), sheet=1, startRow=5, rows=c(5:8))

#format row headers
total_df_rownames <- c("measure","total_yes", "total_no")
total_df[1:3,1] <- total_df_rownames[1:3]


#get data for Hispanic/non-Hispanic races, excluding AIAN, NHPI, and SWANA
races_df = read.xlsx(paste0("Asthma/",curr_yr,"/Asthma_race.xlsx"), sheet=1, startRow=8, rows=c(8,10:12,14,16:19,21,23))

#format row headers
races_rownames <- c("latino_yes", "nh_white_yes", "nh_black_yes", "nh_asian_yes", "nh_twoormor_yes", 
                    "latino_no", "nh_white_no", "nh_black_no", "nh_asian_no", "nh_twoormor_no")
races_df[1:10,1] <- races_rownames[1:10]


#get data for ALL-AIAN
aian_df = read.xlsx(paste0("Asthma/",curr_yr,"/Asthma_aian.xlsx"), sheet=1, startRow=8, rows=c(8,10,12))

#format row headers
aian_rownames <- c("aian_yes", "aian_no")
aian_df[1:2,1] <- aian_rownames[1:2]


#get data for ALL-NHPI
pacisl_df = read.xlsx(paste0("Asthma/",curr_yr,"/Asthma_nhpi.xlsx"), sheet=1, startRow=8, rows=c(8,10,12))

#format row headers
pacisl_rownames <- c("pacisl_yes", "pacisl_no")
pacisl_df[1:2,1] <- pacisl_rownames[1:2]


#get data for ALL-SWANA
swana_df = read.xlsx(paste0("Asthma/",curr_yr,"/Asthma_mena.xlsx"), sheet=1, startRow=8, rows=c(8,10,12))

#format row headers
swana_rownames <- c("swana_yes", "swana_no")
swana_df[1:2,1] <- swana_rownames[1:2]


#combine
df <- rbind(total_df, races_df, aian_df, pacisl_df, swana_df)

#run the rest of CHIS prep including formatting column names, screen using flags, adding geonames, etc.
source("W:/Project/RACE COUNTS/Functions/CHIS_Functions.R")
df_subset <- prep_chis(df)
View(df_subset)

d <- df_subset


#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

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

###info for postgres tables - automatically updates###
county_table_name <- paste0("arei_hben_asthma_county_",yr)
state_table_name <- paste0("arei_hben_asthma_state_",yr)
indicator <- paste0("Created on ", Sys.Date(), ". People ever Diagnosed with Asthma (%)")
source <- paste0("AskCHIS ", curr_yr, " Pooled Estimates ", dwnld_url)

#send tables to postgres
#to_postgres(county_table,state_table)

dbDisconnect(con)