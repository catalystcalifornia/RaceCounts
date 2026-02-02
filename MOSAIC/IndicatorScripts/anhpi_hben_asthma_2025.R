### Asthma for MOSAIC### 

#install packages if not already installed
packages <- c("tidyr", "dplyr", "sf", "tidycensus", "tidyverse", "usethis", "openxlsx", "RPostgres")  

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
curr_yr <- "2017_24"  # must keep same format
dwnld_url <- "https://ask.chis.ucla.edu/"
rc_schema <- "v7"
yr <- "2025"
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Environment\\QA_Sheet_Asthma - MOSAIC.docx"

chis_dir <- ("W:/Data/Health/CHIS/")


#get data for Total population - NOTE: We only pull in this data to make CHIS fx work, we drop this data at the end
total_df = read.xlsx(paste0(chis_dir, "Asthma/2011_23/Asthma_total.xlsx"), sheet=1, startRow=5, rows=c(5:8))

#format row headers
total_df_rownames <- c("measure","total_yes", "total_no")
total_df[1:3,1] <- total_df_rownames[1:3]


#get data for Asian subgroups
asian_df = read.xlsx(paste0(chis_dir, "Asthma/",curr_yr,"/AsianEthnicityGroups.xlsx"), sheet=1, startRow=8, rows=c(8,10:23))

#format row headers
asian_rownames <- c("chinese_yes", "japanese_yes", "korean_yes", "filipino_yes", "south_asian_yes", "vietnamese_yes", "other_asian_yes", 
                    "chinese_no", "japanese_no", "korean_no", "filipino_no", "south_asian_no", "vietnamese_no", "other_asian_no")
asian_df[1:14,1] <- asian_rownames[1:14]

#asian column names come in different for some reason so updating
names(asian_df) <- names(total_df)

#combine
df <- rbind(total_df, asian_df)

#run the rest of CHIS prep including formatting column names, screen using flags, adding geonames, etc.
source("./MOSAIC/Functions/CHIS_Functions.R")
df_subset <- prep_chis(df)
View(df_subset)

d <- df_subset

#set source for RC Functions script
source("./MOSAIC/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'
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
county_table <- county_table %>% select(-c(starts_with("total"), starts_with("performance"), "quadrant"))
state_table <- state_table %>% select(-c(starts_with("total")))


###info for postgres tables - automatically updates###
county_table_name <- paste0("asian_hben_asthma_county_",yr)
state_table_name <- paste0("asian_hben_asthma_state_",yr)
indicator <- paste0("People ever Diagnosed with Asthma (%) Asian Ethnic Groups ONLY.")
source <- paste0("AskCHIS ", curr_yr, " Pooled Estimates ", dwnld_url, ". QA doc: ", qa_filepath)

#send tables to postgres
to_postgres(county_table,state_table,"mosaic")
