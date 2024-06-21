## Student Homelessness for RC v5 ##

#install packages if not already installed
list.of.packages <- c("openxlsx","tidyr","dplyr","stringr","RPostgreSQL","data.table","tidycensus","sf","tigris")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(openxlsx)
library(stringr)
library(tidyr)
library(dplyr)
library(RPostgreSQL)
library(data.table)
library(tidycensus)
library(tigris)
library(sf)
options(scipen = 999) # disable scientific notation

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")

############### PREP COUNTY/STATE DATA ########################

#get data for student homelessness by race/ethnicity and county and state
county_enrollment_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/enrollment_total_by_county_2021_22.xlsx", sheet=1, startRow=1, rows=c(1:59), cols=c(1:11))
# head(county_enrollment_df)
# colnames(county_enrollment_df)

county_enrollment_df <- dplyr::rename(county_enrollment_df, c("geoname" = "Name",                             
                                                        "enroll_total" = "Total",                            
                                                       "enroll_nh_black" = "African.American",            
                                                       "enroll_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                       "enroll_nh_asian" = "Asian",                            
                                                       "enroll_nh_filipino" = "Filipino",                        
                                                       "enroll_latino" = "Hispanic.or.Latino",               
                                                       "enroll_nh_nhpi" = "Pacific.Islander",                 
                                                       "enroll_nh_white" = "White",                           
                                                       "enroll_nh_twoormor" = "Two.or.More.Races",                
                                                       "enroll_not_reported" = "Not.Reported"))  


state_enrollment_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/enrollment_total_by_county_2021_22.xlsx", sheet=1, startRow=61, rows=c(61:62), cols=c(1:11))
# head(state_enrollment_df)
state_enrollment_df <- dplyr::rename(state_enrollment_df, c("geoname" = "Name",                             
                                                     "enroll_total" = "Total",                            
                                                     "enroll_nh_black" = "African.American",            
                                                     "enroll_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                     "enroll_nh_asian" = "Asian",                            
                                                     "enroll_nh_filipino" = "Filipino",                        
                                                     "enroll_latino" = "Hispanic.or.Latino",               
                                                     "enroll_nh_nhpi" = "Pacific.Islander",                 
                                                     "enroll_nh_white" = "White",                           
                                                     "enroll_nh_twoormor" = "Two.or.More.Races",                
                                                     "enroll_not_reported" = "Not.Reported"))  
state_enrollment_df$geoname[state_enrollment_df$geoname =='Statewide'] <- 'California'

# head(state_enrollment_df)

county_homeless_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/homeless_enrollment_by_county_2021_22.xlsx", sheet=1, startRow=1, rows=c(1:59), cols=c(1:11))
# head(county_homeless_df)
county_homeless_df <- dplyr::rename(county_homeless_df, c("geoname" = "Name",                             
                                                   "homeless_total" = "Total",                            
                                                   "homeless_nh_black" = "African.American",            
                                                   "homeless_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                   "homeless_nh_asian" = "Asian",                            
                                                   "homeless_nh_filipino" = "Filipino",                        
                                                   "homeless_latino" = "Hispanic.or.Latino",               
                                                   "homeless_nh_nhpi" = "Pacific.Islander",                 
                                                   "homeless_nh_white" = "White",                           
                                                   "homeless_nh_twoormor" = "Two.or.More.Races",                
                                                   "homeless_not_reported" = "Not.Reported"))  

state_homeless_df = read.xlsx("W:/Data/Education/California Department of Education/Student_Homelessness/2021-22/homeless_enrollment_by_county_2021_22.xlsx", sheet=1, startRow=61, rows=c(61:62), cols=c(1:11))

state_homeless_df <- dplyr::rename(state_homeless_df, c("geoname" = "Name",                             
                                                 "homeless_total" = "Total",                            
                                                 "homeless_nh_black" = "African.American",            
                                                 "homeless_nh_aian" = "American.Indian.or.Alaska.Native", 
                                                 "homeless_nh_asian" = "Asian",                            
                                                 "homeless_nh_filipino" = "Filipino",                        
                                                 "homeless_latino" = "Hispanic.or.Latino",               
                                                 "homeless_nh_nhpi" = "Pacific.Islander",                 
                                                 "homeless_nh_white" = "White",                           
                                                 "homeless_nh_twoormor" = "Two.or.More.Races",                
                                                 "homeless_not_reported" = "Not.Reported"))  
state_homeless_df$geoname[state_homeless_df$geoname =='Statewide'] <- 'California'
# head(state_homeless_df)

######### Bind and merge dataframes to create one final df ##########

enrollment <- rbind(county_enrollment_df,state_enrollment_df)
# View(enrollment)

homelessness <- rbind(county_homeless_df,state_homeless_df)
# View(homelessness)

df <- enrollment %>% left_join(homelessness, by = "geoname")


#get census geoids

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

#add county geoids
df <- merge(x=ca,y=df,by="geoname", all=T)
#add state geoid
df <- within(df, geoid[geoname == 'California'] <- '06')


#View(df)

####### clean and transform the raw csv data #######
threshold <- 50
df <- df %>% 
  mutate( total_raw = ifelse(enroll_total < threshold, NA, homeless_total),   #pop screen for _raw columns where a group's enrollment is less than 50
          nh_black_raw = ifelse(enroll_nh_black < threshold, NA, homeless_nh_black),
          nh_aian_raw = ifelse(enroll_nh_aian < threshold, NA, homeless_nh_aian),
          nh_asian_raw = ifelse(enroll_nh_asian < threshold, NA, homeless_nh_asian),
          nh_filipino_raw = ifelse(enroll_nh_filipino < threshold, NA, homeless_nh_filipino),
          latino_raw = ifelse(enroll_latino < threshold, NA, homeless_latino),
          nh_pacisl_raw = ifelse(enroll_nh_nhpi < threshold, NA, homeless_nh_nhpi),
          nh_white_raw = ifelse(enroll_nh_white < threshold, NA, homeless_nh_white),
          nh_twoormor_raw = ifelse(enroll_nh_twoormor < threshold, NA, homeless_nh_twoormor),
          
          # calculate _rate column if _raw column does not equal NA
          total_rate = ifelse(is.na(total_raw), NA, (total_raw / enroll_total)*100),
          nh_black_rate = ifelse(is.na(nh_black_raw), NA, (nh_black_raw / enroll_nh_black)*100),
          nh_aian_rate = ifelse(is.na(nh_aian_raw), NA, (nh_aian_raw / enroll_nh_aian)*100),
          nh_asian_rate = ifelse(is.na(nh_asian_raw), NA, (nh_asian_raw / enroll_nh_asian)*100),
          nh_filipino_rate = ifelse(is.na(nh_filipino_raw), NA, (nh_filipino_raw / enroll_nh_filipino)*100),
          latino_rate = ifelse(is.na(latino_raw), NA, (latino_raw / enroll_latino)*100),
          nh_pacisl_rate = ifelse(is.na(nh_pacisl_raw), NA, (nh_pacisl_raw / enroll_nh_nhpi)*100),
          nh_white_rate = ifelse(is.na(nh_white_raw), NA, (nh_white_raw / enroll_nh_white)*100),
          nh_twoormor_rate = ifelse(is.na(nh_twoormor_raw), NA, (nh_twoormor_raw / enroll_nh_twoormor)*100),
          
          # create pop columns
          total_pop = enroll_total,
          nh_black_pop = enroll_nh_black,
          nh_aian_pop = enroll_nh_aian,
          nh_asian_pop = enroll_nh_asian,
          nh_filipino_pop = enroll_nh_filipino,
          latino_pop = enroll_latino,
          nh_pacisl_pop = enroll_nh_nhpi,
          nh_white_pop = enroll_nh_white,
          nh_twoormor_pop = enroll_nh_twoormor
          )
# View(df)   #This line is used to see a pop-out view of the df. Un-comment this line if you intend to use it
# colnames(df) This line is used to view column names so that you can select the appropriate column names when creating the df_subset. Un-comment this line if you intend to use it

########## create a df with only the newly calculated columns #######
df_subset <- select(df, geoid, geoname, ends_with("_pop"), ends_with("_raw"), ends_with("_rate")) %>% mutate(geolevel = ifelse(geoid == '06', 'state', 'county'))
View(df_subset)

d <- df_subset

############## CALC RACE COUNTS STATS ##############
#set source for RC Functions script
source("W:/Project/RACE COUNTS/Functions/RC_Functions.R")

d$asbest = 'min'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update previous line of code accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_p_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geolevel == 'state', ]
county_table <- d[d$geolevel == 'county', ]
city_table <- d[d$geolevel == 'place', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)
state_table <- state_table %>% dplyr::rename("state_name" = "geoname", "state_id" = "geoid")
View(state_table)

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)
county_table <- county_table %>% dplyr::rename("county_name" = "geoname", "county_id" = "geoid")
View(county_table)


###update info for postgres tables###
county_table_name <- "arei_hous_student_homelessness_county_2023"
state_table_name <- "arei_hous_student_homelessness_state_2023"
indicator <- "Student homelessness rates. Data for groups with enrollment under 50 are excluded from the calculations. Homelessness rate calculated as a percent of enrollment for each group"
source <- "CDE 2021-22 https://dq.cde.ca.gov/dataquest/"
rc_schema <- 'v5'



#send tables to postgres
#to_postgres(county_table, state_table)
#city_to_postgres(city_table)

