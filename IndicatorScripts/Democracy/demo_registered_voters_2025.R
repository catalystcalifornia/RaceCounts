#install packages if not already installed
list.of.packages <- c("RPostgres","DBI","tidyverse","tidycensus","usethis","httr","janitor","hablar")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

### load packages
library(RPostgres)
library(DBI)
library(tidyverse)
library(tidycensus)
library(usethis)
library(httr) # connect to CPS API
library(janitor) # set row 1 to colnames
library(hablar) # sum_() returns NA when all NA, ignores NA when at least 1 non-NA value

# Set Sources --------------------------------------------------------------
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/democracy_functions.R")
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")
census_api_key(census_key1)

# Update variables used throughout each year --------------------------------------------------------------
cps_yr <- c('2012', '2014', '2016', '2018', '2020', '2022', '2024')
rc_yr <- 2025
rc_schema <- 'v7'

# Get Latest Data: Comment out after data has been exported to postgres ####
 # metadata <-"https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov24.pdf"           # update each year
 # filepath = "https://www2.census.gov/programs-surveys/cps/datasets/2024/supp/nov24pub.csv" # update each year
 # fieldtype = 1  # confirm using metadata link
 # ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
 # table_schema <- "democracy"
 # table_name <- paste0("cps_voting_supplement_", tail(cps_yr, n=1))
 # 
 # table_comment_source <- "NOTE: Geoid fields (gestfips, gtcbsa, gtcco, tco, gtcsa) are missing leading zeroes"
 # table_source <- paste0("CPS Voting Supplement data downloaded ", Sys.Date(), " from https://www.census.gov/data/datasets/time-series/demo/cps/cps-supp_cps-repwgt.html. Metadata here: ", metadata)
 # 
 # df <- read_csv(file = filepath, na = c("*", "")) %>% filter(GESTFIPS == 6)
 # names(df) <- tolower(names(df)) # make col names lowercase
 # 
 # ##  WRITE TABLE TO POSTGRES DB ##
 # # make character vector for field types in postgres table
 # charvect = rep('numeric', dim(df)[2])
 # charvect[fieldtype] <- "varchar" # specify which cols are varchar, the rest will be numeric
 # 
 # # add names to the character vector
 # names(charvect) <- colnames(df)
 # 
 # # write table
 # dbWriteTable(con2, Id(table_schema, table_name), df, overwrite = FALSE, row.names = FALSE)
 # 
 # # write comment to table, and the first three fields that won't change.
 # table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
 # 
 # # send table comment to database
 # dbBegin(con2)
 # dbExecute(con2, table_comment)
 # dbCommit(con2)
 
# Pull in all data years from postgres --------------------------------------------------------------
## Pre-2020 data
table_list <- dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_schema='data'")
cps_list <- table_list %>% filter(grepl("cps_",table_name)) %>% filter(grepl("voting_supplement",table_name)) %>% filter(!grepl("2010",table_name)) 
cps_list <- cps_list[order(cps_list$table_name), ]  # alphabetize list of cps_list tables, needed to format list correctly for next step
# import all tables on cps_list
cps_tables <- lapply(setNames(paste0("select gtco,gestfips,gtcbsa,gtcsa,hrintsta,pes1,pes2,prpertyp,prtage,prcitshp,ptdtrace,pehspnon,pwsswgt from data.", cps_list), cps_list), DBI::dbGetQuery, conn = con)
cps_tables <- Map(cbind, cps_tables, year = names(cps_tables)) # add year column, populated by table names
cps_tables <- lapply(cps_tables, transform, year=str_sub(year,5,8)) # update year column values to year only

## 2020 data and newer
table_list2 <- dbGetQuery(con2, "SELECT table_name FROM information_schema.tables WHERE table_schema='democracy'")
cps_list2 <- filter(table_list2, grepl("cps_",table_name)) %>% filter(grepl("voting",table_name)) 
cps_list2 <- cps_list2[order(cps_list2$table_name), ]  # alphabetize list of cps_list tables, needed to format list correctly for next step
# import all tables on cps_list2
cps_tables2 <- lapply(setNames(paste0("select gtco,gestfips,gtcbsa,gtcsa,hrintsta,pes1,pes2,prpertyp,prtage,prcitshp,ptdtrace,pehspnon,pwsswgt from democracy.", cps_list2), cps_list2), DBI::dbGetQuery, conn = con2)
cps_tables2 <- Map(cbind, cps_tables2, year = names(cps_tables2)) # add year column, populated by table names
cps_tables2 <- lapply(cps_tables2, transform, year=str_sub(year,-4,-1)) # update year column values to year only
cps_tables2 <- lapply(cps_tables2, function(i) {i[] <- lapply(i, as.character); i}) # convert all cols to char

combo_list <- list(c(cps_tables, cps_tables2)) # combine all data years into 1 list
combo_list <- combo_list[[1]]  # unnest the list

# create new list element names
new_names <- list() # create empty list for loop below
for (i in cps_yr) {
  temp <- paste0("df_",i)
  new_names[[i]] <- temp   
}  

names(combo_list) <- new_names # rename list elements

combo_list <- lapply(combo_list, function(x) clean_cps(x)) # clean geoid codes and create numeric wgt column


## REGISTERED VOTER CALCS  --------------------------------------------
county_reg <- lapply(combo_list, function(x) registered_by_county(x)) # calc registered voters by race/total
state_reg <- lapply(combo_list, function(x) registered_by_state(x))   # calc registered voters by race/total

county_vap <- lapply(combo_list, function(x) voting_age_county(x)) # calc voting age pop by race/total
state_vap <- lapply(combo_list, function(x) voting_age_state(x))   # calc voting age pop by race/total

## combine county and summarize datasets together, combine and summarize state datasets together
county_data_list <- lapply(1:length(county_reg), 
                           function(x) merge(county_reg[[x]], 
                                             county_vap[[x]], 
                                             by = "gtco",
                                             all = TRUE))

county_data_df_ <- Reduce(full_join,county_data_list)  # combine data years into 1 list 

county_data_num <- county_data_df_ %>% group_by(gtco) %>% select(c(starts_with("num_"))) %>% summarise_all(., mean, na.rm=TRUE)  # summarize (average) all number data years
county_data_count <- county_data_df_ %>% group_by(gtco) %>% select(c(starts_with("count_"))) %>% summarise_all(., sum_)          # summarize (sum_) all count data years

county_data_df <- county_data_num %>% full_join(county_data_count, by = "gtco") %>% rename(geoid = gtco)   # join avg numbers and counts

state_data_list <- lapply(1:length(state_reg), 
                          function(x) merge(state_reg[[x]], 
                                            state_vap[[x]], 
                                            by = "gestfips",
                                            all = TRUE))

state_data_df_ <- Reduce(full_join,state_data_list)  # combine data years into 1 list 

state_data_num <- state_data_df_ %>% group_by(gestfips) %>% select(c(starts_with("num_"))) %>% summarise_all(., mean, na.rm=TRUE)  # summarize (average) all number data years
state_data_count <- state_data_df_ %>% group_by(gestfips) %>% select(c(starts_with("count_"))) %>% summarise_all(., sum_)          # summarize (sum_) all count data years

state_data_df <- state_data_num %>% full_join(state_data_count, by = "gestfips") %>% rename(geoid = gestfips)   # join avg numbers and counts	


## join county and state data together
final_data_df <- county_data_df %>% rbind(state_data_df)


## count number of data years per county
temp_list <- list()
for(i in 1:length(county_reg)) {
  temp <- do.call(data.frame, county_reg[[i]][1])
  temp_list[[i]] <- temp
}

num_data_yrs <- list_c(temp_list)
num_data_yrs <- num_data_yrs %>% count(gtco) %>% rename(num_yrs = n)

## join data and data yrs
final_df <- final_data_df %>% full_join(num_data_yrs, by = c('geoid' = 'gtco')) %>% mutate(num_yrs = ifelse(geoid == '06', length(unique(cps_yr)), num_yrs)) 


# Screening and calculate raw/rate ---------------------------------------------------------------
threshold = 10

final_df_screened <- final_df %>%
  mutate(total_raw = ifelse(count_total_reg < threshold, NA, round(num_total_reg, 0)),
         
         latino_raw = ifelse(count_latino_reg < threshold, NA, round(num_latino_reg, 0)),
         
         nh_white_raw = ifelse(count_nh_white_reg < threshold, NA, round(num_nh_white_reg, 0)),
         
         nh_black_raw = ifelse(count_nh_black_reg < threshold, NA, round(num_nh_black_reg, 0)),
         
         aian_raw = ifelse(count_aian_reg < 10, NA, round(num_aian_reg, 0)),
         
         nh_asian_raw = ifelse(count_nh_asian_reg < threshold, NA, round(num_nh_asian_reg, 0)),
         
         pacisl_raw = ifelse(count_pacisl_reg < threshold, NA, round(num_pacisl_reg, 0)),
         
         nh_twoormor_raw = ifelse(count_nh_twoormor_reg < threshold, NA, round(num_nh_twoormor_reg, 0)),
         
         total_rate = ifelse(count_total_reg < threshold, NA, (num_total_reg) / num_total_va_pop * 100),
         
         latino_rate = ifelse(count_latino_reg < threshold, NA,  (num_latino_reg) / num_latino_va_pop * 100),
         
         nh_white_rate = ifelse(count_nh_white_reg < threshold, NA,  (num_nh_white_reg) / num_nh_white_va_pop * 100),
         
         nh_black_rate = ifelse(count_nh_black_reg < threshold, NA, (num_nh_black_reg) / num_nh_black_va_pop * 100),
         
         aian_rate = ifelse(count_aian_reg < 10, NA, (num_aian_reg) / num_aian_va_pop * 100),
         
         nh_asian_rate = ifelse(count_nh_asian_reg < 10, NA,(num_nh_asian_reg) / num_nh_asian_va_pop * 100),
         
         pacisl_rate = ifelse(count_pacisl_reg < threshold, NA, (num_pacisl_reg) / num_pacisl_va_pop * 100),
         
         nh_twoormor_rate = ifelse(count_nh_twoormor_reg < threshold, NA, (num_nh_twoormor_reg) / num_nh_twoormor_va_pop * 100)
         
  ) %>%
  
  
  select(geoid, ends_with("_reg"), ends_with("_va_pop"), ends_with("_raw"), ends_with("_rate"), num_yrs
         
  )  


# Convert any NaN values to NA
final_df_screened <- final_df_screened %>% mutate(across(everything(), gsub, pattern = NaN, replacement = NA))

# #get census geoids ------------------------------------------------------

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

#add county geonames
df <- merge(x=ca,y=final_df_screened,by="geoid", all=T)
#add state geoname
df$geoname[is.na(df$geoname)] <- "California"

# make d 
d <- df %>% mutate(across(-c(geoid, geoname), as.numeric))

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


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

# remove columns not going on API
county_table<- county_table %>% select(-ends_with("_reg")) %>% select(-ends_with("_va_pop"))
state_table<- state_table %>% select(-ends_with("_reg")) %>% select(-ends_with("_va_pop"))

###update info for postgres tables###
county_table_name <- paste0("arei_demo_registered_voters_county_", rc_yr)
state_table_name <- paste0("arei_demo_registered_voters_state_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Annual average percent of registered voters among the citizen voting age population.")

source <- paste0("CPS (", paste(cps_yr, collapse = ", "), ") average https://www.census.gov/topics/public-sector/voting/data.html")


#send tables to postgres
#to_postgres(county_table, state_table)

dbDisconnect(con)
dbDisconnect(con2)



