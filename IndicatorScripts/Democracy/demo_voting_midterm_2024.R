#install packages if not already installed
list.of.packages <- c("RPostgreSQL","DBI","tidyverse","tidycensus","usethis","httr","janitor","hablar")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

### load packages
library(RPostgreSQL)
library(DBI)
library(tidyverse)
library(tidycensus)
library(usethis)
library(httr) # connect to CPS API
library(janitor) # set row 1 to colnames
library(hablar) # sum_() returns NA when all NA, ignores NA when at least 1 non-NA value

# Set Sources --------------------------------------------------------------
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/staging/Functions/democracy_functions.R")
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("rda_shared_data")
census_api_key(census_key1)

# Update variables used throughout each year --------------------------------------------------------------
cps_yr <- c('2010', '2014', '2018', '2022')
rc_yr <- 2024
rc_schema <- 'v6'
threshold = 10   # geo+race combos with < threshold voters who voted are suppressed 

# Get Latest Data: Comment out after data has been exported to postgres ####
# metadata <-"https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov22.pdf"           # update each year
# filepath = "https://www2.census.gov/programs-surveys/cps/datasets/2022/supp/nov22pub.csv" # update each year
# fieldtype = 1  # confirm using metadata link
# ## Manually define postgres schema, table name, table comment, data source for rda_shared_data table
# table_schema <- "democracy"
# table_name <- paste0("cps_voting_supplement_", tail(cps_yr, n=1))

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
# dbWriteTable(con2, c(table_schema, table_name), df,
#              overwrite = FALSE, row.names = FALSE,
#              field.types = charvect)
# 
# # write comment to table, and the first three fields that won't change.
# table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ". ", table_source, ".';")
# 
# # send table comment to database
# dbSendQuery(conn = con2, table_comment)

# Pull in all data years from postgres --------------------------------------------------------------
## 2010 data
table_list <- dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_schema='data'")
cps_list_ <- table_list %>% filter(grepl("cps_",table_name)) %>% filter(grepl("voting_supplement",table_name)) %>% filter(grepl("2010", table_name))
cps_list_ <- cps_list_[order(cps_list_$table_name), ]  # alphabetize list of cps_list tables, needed to format list correctly for next step
# import all tables on cps_list
cps_tables_ <- lapply(setNames(paste0("select gtco,gestfips,gtcbsa,gtcsa,hrintsta,pes1,pes2,prpertyp,peage,prcitshp,ptdtrace,pehspnon,pwsswgt from data.", cps_list_), cps_list_), DBI::dbGetQuery, conn = con)
cps_tables_ <- Map(cbind, cps_tables_, year = names(cps_tables_)) # add year column, populated by table names
cps_tables_ <- lapply(cps_tables_, transform, year=str_sub(year,5,8)) # update year column values to year only
cps_tables_ <- lapply(cps_tables_, function(x) x %>% rename(prtage = peage)) # update col name to match rest of data yrs
cps_tables_ <- lapply(cps_tables_, function(x) { x$pwsswgt <- as.numeric(x$pwsswgt); x }) # make sure its the correct type
## 2012-2018 data
table_list <- dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_schema='data'")
cps_list <- table_list %>% filter(grepl("cps_",table_name)) %>% filter(grepl("voting_supplement",table_name)) %>% filter(grepl(paste(cps_yr, collapse="|"), table_name)) %>% filter(!grepl("2010", table_name))
cps_list <- cps_list[order(cps_list$table_name), ]  # alphabetize list of cps_list tables, needed to format list correctly for next step
# import all tables on cps_list
cps_tables <- lapply(setNames(paste0("select gtco,gestfips,gtcbsa,gtcsa,hrintsta,pes1,pes2,prpertyp,prtage,prcitshp,ptdtrace,pehspnon,pwsswgt from data.", cps_list), cps_list), DBI::dbGetQuery, conn = con)
cps_tables <- Map(cbind, cps_tables, year = names(cps_tables)) # add year column, populated by table names
cps_tables <- lapply(cps_tables, transform, year=str_sub(year,5,8)) # update year column values to year only
cps_tables <- lapply(cps_tables, function(x) { x$pwsswgt <- as.numeric(x$pwsswgt); x }) #make sure its the right type
## 2022 data and newer
table_list2 <- dbGetQuery(con2, "SELECT table_name FROM information_schema.tables WHERE table_schema='democracy'")
# This code was breaking for me 
cps_list2 <- filter(table_list2, grepl("cps_",table_name)) %>% 
  filter(grepl("voting_supplement",table_name)) %>%
  filter(grepl(paste(cps_yr, collapse="|"), table_name)) # alphabetize list of cps_list tables, needed to format list correctly for next step
# import all tables on cps_list2
cps_tables2 <- lapply(setNames(paste0("select gtco,gestfips,gtcbsa,gtcsa,hrintsta,pes1,pes2,prpertyp,prtage,prcitshp,ptdtrace,pehspnon,pwsswgt from democracy.", cps_list2), cps_list2), DBI::dbGetQuery, conn = con2)
cps_tables2 <- Map(cbind, cps_tables2, year = names(cps_tables2))
cps_tables2 <- lapply(cps_tables2, transform, year=str_sub(year,-4,-1))
cps_tables2 <- lapply(cps_tables2, function(i) {i[] <- lapply(i, as.character); i}) #this line diagnosis the type issue w/ the weight
cps_tables2 <- lapply(cps_tables2, function(i) {i$pwsswgt <- as.numeric(i$pwsswgt); i}) # this line applies a fix for the type issue for the weight

combo_list <- list(c(cps_tables_, cps_tables, cps_tables2)) # combine all data years into 1 list
combo_list <- combo_list[[1]]  # unnest the list

#check the types because I keep getting a type error
sapply(combo_list, function(x) class(x$pwsswgt))
# df_2010     df_2014     df_2018     df_2022 
# "character" "character" "character"   "numeric" 
#earlier fix did fix one part of the list but not the whole thing so go back and fix the type 
# create new list element names
new_names <- list() # create empty list for loop below
for (i in cps_yr) {
  temp <- paste0("df_",i)
  new_names[[i]] <- temp   
}  

names(combo_list) <- new_names # rename list elements

combo_list <- lapply(combo_list, function(x) clean_cps(x)) # clean geoid codes and create numeric wgt column


## MIDTERM VOTER CALCS  --------------------------------------------
county_voter <- lapply(combo_list, function(x) voted_by_county(x)) # calc midterm voters by race/total
state_voter <- lapply(combo_list, function(x) voted_by_state(x))   # calc midterm voters by race/total

county_vap <- lapply(combo_list, function(x) voting_age_county(x)) # calc voting age pop by race/total
state_vap <- lapply(combo_list, function(x) voting_age_state(x))   # calc voting age pop by race/total

## combine county and summarize datasets together, combine and summarize state datasets together
county_data_list <- lapply(1:length(county_voter), 
                           function(x) merge(county_voter[[x]], 
                                             county_vap[[x]], 
                                             by = "gtco",
                                             all = TRUE))

race_groups <- c("total", "latino", "nh_white", "nh_black", "aian", "nh_asian", "pacisl", "nh_twoormor")
county_data_list <- lapply(county_data_list, sync_voted_vap_na, race_groups = race_groups)  # add this

county_data_df_ <- Reduce(full_join,county_data_list)  # combine data years into 1 list 

county_data_num <- county_data_df_ %>% group_by(gtco) %>% select(c(starts_with("num_"))) %>% summarise_all(., mean, na.rm=TRUE)  # summarize (average) all number data years
county_data_count <- county_data_df_ %>% group_by(gtco) %>% select(c(starts_with("count_"))) %>% summarise_all(., sum_)          # summarize (sum_) all count data years

county_data_df <- county_data_num %>% full_join(county_data_count, by = "gtco") %>% rename(geoid = gtco)   # join avg numbers and counts

state_data_list <- lapply(1:length(state_voter), 
                          function(x) merge(state_voter[[x]], 
                                            state_vap[[x]], 
                                            by = "gestfips",
                                            all = TRUE))

state_data_list <- lapply(state_data_list, sync_voted_vap_na, race_groups = race_groups)  # add this

state_data_df_ <- Reduce(full_join,state_data_list)  # combine data years into 1 list 

state_data_num <- state_data_df_ %>% group_by(gestfips) %>% select(c(starts_with("num_"))) %>% summarise_all(., mean, na.rm=TRUE)  # summarize (average) all number data years
state_data_count <- state_data_df_ %>% group_by(gestfips) %>% select(c(starts_with("count_"))) %>% summarise_all(., sum_)          # summarize (sum_) all count data years

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
final_df <- final_data_df %>% full_join(num_data_yrs, by = c('geoid' = 'gtco')) %>% mutate(num_yrs = ifelse(geoid == '06', length(unique(cps_yr)), num_yrs)) 


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
final_df_screened <- final_df_screened %>% mutate(across(everything(), gsub, pattern = NaN, replacement = NA))


##get census geoids ------------------------------------------------------
ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")

#add county geonames
df <- merge(x=ca,y=final_df_screened,by="geoid", all=T)
df$geoname[is.na(df$geoname)] <- "California"

# add geolevel #since this was an older script; it didn't have this column but it should now that its being rerun w/ the newer versions of the RC functions
df$geolevel <- ifelse(df$geoname == 'California', 'state', 'county')

# make d 
d <- df %>% mutate(across(-c(geoid, geoname, geolevel), as.numeric))

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
county_table_name <- paste0("arei_demo_voting_midterm_county_", rc_yr)
state_table_name <- paste0("arei_demo_voting_midterm_state_", rc_yr)

indicator <- paste0("Created on ", Sys.Date(), ". Annual average percent of voters voting in midterm elections among eligible voting age population. This data is")
source <- paste0("CPS (", paste(cps_yr, collapse = ", "), ") average https://www.census.gov/topics/public-sector/voting/data.html")

to_postgres(county_table, state_table)


dbDisconnect(con)

### v2 check the final tables
con <- connect_to_db("racecounts")
old_county_df <- dbGetQuery(con, "SELECT * FROM v6.arei_demo_voting_midterm_county_2024")
new_county_df <- dbGetQuery(con, "SELECT * FROM v6.arei_demo_voting_midterm_county_2024_v2")
old_state_df <- dbGetQuery(con, "SELECT * FROM v6.arei_demo_voting_midterm_state_2024")
new_state_df <- dbGetQuery(con, "SELECT * FROM v6.arei_demo_voting_midterm_state_2024_v2")
dbDisconnect(con)

#check the differences
nrow(old_county_df); nrow(new_county_df) #58 and 58 so they're the same
setdiff(old_county_df$county_id, new_county_df$county_id) #0
setdiff(new_county_df$county_id, old_county_df$county_id) #0 so all the same geoids

# difference for every rate
rate_cols <- names(old_county_df)[grepl("_rate$", names(old_county_df))] # diff in every rate col

rate_diffs <- map_dfr(rate_cols, function(col) {
  old_county_df %>%
    select(county_id, county_name, old_val = all_of(col)) %>%
    left_join(new_county_df %>% select(county_id, new_val = all_of(col)), by = "county_id") %>%
    mutate(race = col, diff = new_val - old_val) %>%
    filter(!is.na(diff), diff != 0)
})

rate_diffs %>% arrange(desc(abs(diff)))
# # output
# county_id   county_name  old_val  new_val          race                      diff
# 1      06077   San Joaquin 83.17468 65.85088 nh_black_rate -17.323800649718094746277
# 2      06073     San Diego 71.28864 56.72137   pacisl_rate -14.567270004211202660827
# 3      06083 Santa Barbara 73.29596 60.46377 nh_asian_rate -12.832187217502699638771
# 4      06001       Alameda 72.94303 62.86210     aian_rate -10.080930877952994251245
# 5      06001       Alameda 75.29834 65.36018   pacisl_rate  -9.938162507835897940822
# 6      06053      Monterey 53.49198 45.34338   latino_rate  -8.148593736463496384204
# 7      06095        Solano 33.10227 29.60050   latino_rate  -3.501778477860696625612
# 8      06113          Yolo 75.36849 76.87041 nh_asian_rate   1.501923546857909741448
# 9      06081     San Mateo 56.34450 54.94349   latino_rate  -1.401009173616401426443
# 10     06095        Solano 32.21906 30.95365 nh_asian_rate  -1.265411444513400596179
# 11     06029          Kern 58.77638 59.70650 nh_asian_rate   0.930122242399100684906
# 12     06097        Sonoma 60.30933 60.30933   latino_rate  -0.000000000000007105427

# see if previous issue is fixed too
races <- c("total", "latino", "nh_white", "nh_black", "aian", "nh_asian", "pacisl", "nh_twoormor")

for (r in races) {
  voted_col <- paste0(r, "_raw")
  vap_col <- paste0("num_", r, "_va_pop")
  
  bad_old <- old_county_df %>% filter(.data[[voted_col]] > .data[[vap_col]])
  bad_new <- new_county_df %>% filter(.data[[voted_col]] > .data[[vap_col]])
  cat(r, "- old bad rows:", nrow(bad_old), " | new bad rows:", nrow(bad_new), "\n")
}
# # output shows no more cases where the race_raw would be bigger than the total raw
# total - old bad rows: 0  | new bad rows: 0 
# latino - old bad rows: 0  | new bad rows: 0 
# nh_white - old bad rows: 0  | new bad rows: 0 
# nh_black - old bad rows: 0  | new bad rows: 0 
# aian - old bad rows: 0  | new bad rows: 0 
# nh_asian - old bad rows: 0  | new bad rows: 0 
# pacisl - old bad rows: 0  | new bad rows: 0 
# nh_twoormor - old bad rows: 0  | new bad rows: 0 
# although that was not an issue with this script where the numerator was explicitly bigger than the denominator, I think its worth keeping the methodology change for all of the CPS scripts
# look at quadrants too to see if there were any notable diff there that would change the other visuals
old_county_df %>% select(county_id, county_name, old_rank = performance_rank, old_quad = quadrant) %>%
  left_join(new_county_df %>% select(county_id, new_rank = performance_rank, new_quad = quadrant), by = "county_id") %>%
  filter(old_rank != new_rank | old_quad != new_quad)
# output # results in 3 counties changing quadrants
# county_id    county_name old_rank old_quad new_rank new_quad
# 1     06067     Sacramento       16   yellow       16      red
# 2     06071 San Bernardino       27   yellow       27      red
# 3     06073      San Diego       11   orange       11   purple
## output show that one county did change from purple to orange based on this new methodology
# county_id     county_name old_rank old_quad new_rank new_quad
# 1     06079 San Luis Obispo        1   purple        1   orange

# lastly just spot check by changing the gtco to diff geoids
lapply(county_voter, function(df) df[df$gtco == "06019", c("gtco", "num_aian_voted", "count_aian_voted")])
#$df_2010
# A tibble: 1 × 3
# gtco  num_aian_voted count_aian_voted
# <chr>          <dbl>            <int>
#   1 06019             NA               NA
# 
# $df_2014
# # A tibble: 1 × 3
# gtco  num_aian_voted count_aian_voted
# <chr>          <dbl>            <int>
#   1 06019          5757.                2
# 
# $df_2018
# # A tibble: 1 × 3
# gtco  num_aian_voted count_aian_voted
# <chr>          <dbl>            <int>
#   1 06019             NA               NA
# 
# $df_2022
# # A tibble: 1 × 3
# gtco  num_aian_voted count_aian_voted
# <chr>          <dbl>            <int>
#   1 06019         16586.                3
lapply(county_vap, function(df) df[df$gtco == "06019", c("gtco", "num_aian_va_pop")])
# #
# $df_2010
# # A tibble: 1 × 2
# gtco  num_aian_va_pop
# <chr>           <dbl>
#   1 06019          36707.
# 
# $df_2014
# # A tibble: 1 × 2
# gtco  num_aian_va_pop
# <chr>           <dbl>
#   1 06019           5757.
# 
# $df_2018
# # A tibble: 1 × 2
# gtco  num_aian_va_pop
# <chr>           <dbl>
#   1 06019           4073.
# 
# $df_2022
# # A tibble: 1 × 2
# gtco  num_aian_va_pop
# <chr>           <dbl>
#   1 06019          74716.
# lets run another check just to be sure
combo_list$df_2010 %>%
  mutate(prtage = as.numeric(prtage)) %>%
  filter(prtage >= 18, prcitshp != "5", ptdtrace %in% all_aian, gtco == "06019") %>%
  nrow() # sample size of 10 in 2010

combo_list$df_2014 %>%
  mutate(prtage = as.numeric(prtage)) %>%
  filter(prtage >= 18, prcitshp != "5", ptdtrace %in% all_aian, gtco == "06019") %>%
  nrow() # sample size of 2 in 2014 # a history of very small sample sizes

combo_list$df_2018 %>%
  mutate(prtage = as.numeric(prtage)) %>%
  filter(prtage >= 18, prcitshp != "5", ptdtrace %in% all_aian, gtco == "06019") %>%
  nrow() # sample size of 1 in 2018

combo_list$df_2022 %>%
  mutate(prtage = as.numeric(prtage)) %>%
  filter(prtage >= 18, prcitshp != "5", ptdtrace %in% all_aian, gtco == "06019") %>%
  nrow() # a sample size of 12 2022
# Not a calc error just a data limitation of CPS 
# new methodology looks fine
