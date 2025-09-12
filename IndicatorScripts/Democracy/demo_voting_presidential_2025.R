### Voting in Presidential Elections RC v7 ### 

#### Set up ####
#install packages if not already installed
packages <- c("DBI", "RPostgres", "tidyverse", "tidycensus", "usethis")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

# create connection for rda database and census API
source("W:\\RDA Team\\R\\credentials_source.R")
# create connection
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")
census_api_key(census_key1)

# define variables used in several places that must be updated each year
cps_yrs <- c("2012", "2016", "2020", "2024")  # must keep same format
dwnld_url <- "https://www.census.gov/topics/public-sector/voting/data.html"
rc_schema <- "v7"
rc_yr <- "2025"
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\Democracy\\QA_Voting_Presidential.docx"
threshold = 10   # geo+race combos with < threshold voters who voted are suppressed  

# set source fx
source("./Functions/democracy_functions.R")

# Get 2024 Data --------------------------------------------------

# fx will download file and/or export postgres table and/or import postgres table as needed
metadata = "W:\\Data\\Democracy\\Current Population Survey Voting and Registration\\2024\\cpsnov24.pdf"
url <- "https://www2.census.gov/programs-surveys/cps/datasets/2024/supp/nov24pub.csv"
file <- "W:/Data/Democracy/Current Population Survey Voting and Registration/2024/nov24pub.csv"
varchar_cols <- c("hrhhid", "pes1", "hrintsta", "prpertyp")       # Use metadata to update as needed

df_2024 <- get_cps_supp(metadata, url, '2024', varchar_cols)

# Get 2020 Data --------------------------------------------------

# fx will download file and/or export postgres table and/or import postgres table as needed
metadata = "W:\\Data\\Democracy\\Current Population Survey Voting and Registration\\2020\\cpsnov20.pdf"
url = "https://www2.census.gov/programs-surveys/cps/datasets/2020/supp/nov20pub.csv"
file <- "W:/Data/Democracy/Current Population Survey Voting and Registration/2020/nov20pub.csv"
varchar_cols <- c("hrhhid", "pes1", "hrintsta", "prpertyp")         # Use metadata to update as needed

df_2020 <- get_cps_supp(metadata, url, '2020', varchar_cols)

# Get 2012, 2016 Data --------------------------------------------------
#2016 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov16.pdf
#2012 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov12.pdf

# load data (in RC db, not rda_shared_data)
df_2016 <- dbGetQuery(con, "SELECT * FROM data.cps_2016_voting_supplement")
df_2012 <- dbGetQuery(con, "SELECT * FROM data.cps_2012_voting_supplement")

# close the connection to rda_shared_data
dbDisconnect(con2)


# Summarize the data ----------------------------------------
combo_list <- list(df_2012, df_2016, df_2020, df_2024) # combine all data years into 1 list

# create new list element names
new_names <- list() # create empty list for loop below
for (i in cps_yrs) {
  temp <- paste0("df_",i)
  new_names[[i]] <- temp   
}  

names(combo_list) <- new_names # rename list elements

# Clean Data
combo_list <- lapply(combo_list, function(x) clean_cps(x)) # clean geoid codes and create numeric wgt column


# Run Calcs
county_voter <- lapply(combo_list, function(x) voted_by_county(x)) # calc voters by race/total
state_voter <- lapply(combo_list, function(x) voted_by_state(x))   # calc voters by race/total

county_vap <- lapply(combo_list, function(x) voting_age_county(x)) # calc voting age pop by race/total
state_vap <- lapply(combo_list, function(x) voting_age_state(x))   # calc voting age pop by race/total

## combine county and summarize datasets together, combine and summarize state datasets together
county_data_list <- lapply(1:length(county_voter), 
                           function(x) merge(county_voter[[x]], 
                                             county_vap[[x]], 
                                             by = "gtco",
                                             all = TRUE))

county_data_df_ <- Reduce(full_join, county_data_list)             # combine county data into 1 df 

county_data_num <- county_data_df_ %>% group_by(gtco) %>% select(c(starts_with("num_"))) %>% summarise_all(., mean, na.rm=TRUE)     # summarize (average) all number data years
county_data_count <- county_data_df_ %>% group_by(gtco) %>% select(c(starts_with("count_"))) %>% summarise_all(., sum, na.rm=TRUE)  # summarize (sum_) all count data years

county_data_df <- county_data_num %>% full_join(county_data_count, by = "gtco") %>% rename(geoid = gtco)   # join avg numbers and counts

state_data_list <- lapply(1:length(state_voter), 
                          function(x) merge(state_voter[[x]], 
                                            state_vap[[x]], 
                                            by = "gestfips",
                                            all = TRUE))

state_data_df_ <- Reduce(full_join,state_data_list)  # combine state data into 1 df

state_data_num <- state_data_df_ %>% group_by(gestfips) %>% select(c(starts_with("num_"))) %>% summarise_all(., mean, na.rm=TRUE)       # summarize (average) all number data years
state_data_count <- state_data_df_ %>% group_by(gestfips) %>% select(c(starts_with("count_"))) %>% summarise_all(., sum, na.rm=TRUE)    # summarize (sum) all count data years

state_data_df <- state_data_num %>% full_join(state_data_count, by = "gestfips") %>% rename(geoid = gestfips)   # join avg numbers and counts	

## join county and state data together
final_data_df <- county_data_df %>% rbind(state_data_df)

## count number of data years per county
temp_list <- list()
for(i in 1:length(county_voter)) {
  temp <- do.call(data.frame, county_voter[[i]][1])
  temp_list[[i]] <- temp
}

num_data_yrs <- list_c(temp_list)
num_data_yrs <- num_data_yrs %>% count(gtco) %>% rename(num_yrs = n)

## join data and data yrs
final_df <- final_data_df %>% full_join(num_data_yrs, by = c('geoid' = 'gtco')) %>% mutate(num_yrs = ifelse(geoid == '06', length(unique(cps_yrs)), num_yrs)) 


# Screening and calculate raw/rate ---------------------------------------------------------------

final_df_screened <- final_df %>%
  mutate(total_raw = ifelse(count_total_voted < threshold, NA, round(num_total_voted, 0)),
         
         latino_raw = ifelse(count_latino_voted < threshold, NA, round(num_latino_voted, 0)),
         
         nh_white_raw = ifelse(count_nh_white_voted < threshold, NA, round(num_nh_white_voted, 0)),
         
         nh_black_raw = ifelse(count_nh_black_voted < threshold, NA, round(num_nh_black_voted, 0)),
         
         aian_raw = ifelse(count_aian_voted < threshold, NA, round(num_aian_voted, 0)),
         
         nh_asian_raw = ifelse(count_nh_asian_voted < threshold, NA, round(num_nh_asian_voted, 0)),
         
         pacisl_raw = ifelse(count_pacisl_voted < threshold, NA, round(num_pacisl_voted, 0)),
         
         nh_twoormor_raw = ifelse(count_nh_twoormor_voted < threshold, NA, round(num_nh_twoormor_voted, 0)),
         
         total_rate = ifelse(count_total_voted < threshold, NA, (num_total_voted) / num_total_va_pop * 100),
         
         latino_rate = ifelse(count_latino_voted < threshold, NA,  (num_latino_voted) / num_latino_va_pop * 100),
         
         nh_white_rate = ifelse(count_nh_white_voted < threshold, NA,  (num_nh_white_voted) / num_nh_white_va_pop * 100),
         
         nh_black_rate = ifelse(count_nh_black_voted < threshold, NA, (num_nh_black_voted) / num_nh_black_va_pop * 100),
         
         aian_rate = ifelse(count_aian_voted < threshold, NA, (num_aian_voted) / num_aian_va_pop * 100),
         
         nh_asian_rate = ifelse(count_nh_asian_voted < threshold, NA,(num_nh_asian_voted) / num_nh_asian_va_pop * 100),
         
         pacisl_rate = ifelse(count_pacisl_voted < threshold, NA, (num_pacisl_voted) / num_pacisl_va_pop * 100),
         
         nh_twoormor_rate = ifelse(count_nh_twoormor_voted < threshold, NA, (num_nh_twoormor_voted) / num_nh_twoormor_va_pop * 100)
         
  ) %>%
  
  
  select(geoid, ends_with("_voted"), ends_with("_va_pop"), ends_with("_raw"), ends_with("_rate"), num_yrs
         
  )  


# Convert any NaN values to NA
final_df_screened <- final_df_screened %>% mutate(across(everything(), gsub, pattern = NaN, replacement = NA)) %>%
  filter(geoid != '06000') %>%    # filter out row summarizing where county is not specified
  mutate(across(-1, as.numeric))  # convert all cols except geoid to numeric


## get census geoids ------------------------------------------------------
census_api_key(census_key1, overwrite=TRUE)
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

# add geolevel
df$geolevel <- ifelse(df$geoname == 'California', 'state', 'county')

# make d 
d <- df

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
source("./Functions/RC_Functions.R")

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

state_table <- rename(state_table, state_id = geoid, state_name = geoname) %>%
  select(-c(geolevel))
#View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname) %>%
  select(-c(geolevel))
#View(county_table)

###update info for postgres tables###
county_table_name <- paste0("arei_demo_voting_presidential_county_", rc_yr)
state_table_name <- paste0("arei_demo_voting_presidential_state_", rc_yr)

indicator <- paste0("Annual average percent of voters voting in presidential elections among eligible voting age population. This data is")
source <- paste0("CPS (", paste(cps_yrs, collapse=', '), ") average. ", dwnld_url)

#send tables to postgres
#to_postgres(county_table, state_table)

dbDisconnect(con)


