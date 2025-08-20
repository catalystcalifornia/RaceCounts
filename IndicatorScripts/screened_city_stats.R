#### Screened City Indicator Z-Scores, Ranks etc. for RC v7 ####
## These tables are used for the website and city index

#install packages if not already installed
packages <- c("tidyverse","RPostgres","sf","usethis","dplyr","DBI")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen=999) # disable scientific notation

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
source("./Functions/rdashared_functions.R")
con <- connect_to_db("racecounts")

# update variables each year
curr_yr <- '2025'
rc_schema <- "v7"
qa_filepath <- 'W:\\Project\\RACE COUNTS\\2025_v7\\Composite Index\\QA_api_screened_city.docx'

# set city population threshold
pop_threshold <- 10000


# pull in city-district crosswalk and city population data
crosswalk <- dbGetQuery(con, paste0("SELECT  city_id, city_name, dist_id, district_name, cdscode FROM ", rc_schema, ".arei_city_county_district_table"))
pop_df <- dbGetQuery(con, paste0("SELECT geoid AS city_id, name AS city_name, total_pop AS city_total_pop FROM ", rc_schema, ".arei_race_multigeo WHERE geolevel = 'place'"))

# Pull in Education city indicators -------------------------------------------------------
## These indicators require extra prep bc they are at school district not city level
# pull in list of tables in racecounts current schema
city_table_query <- paste0("SELECT table_catalog, table_schema, table_name as table, table_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND table_catalog = 'racecounts' AND table_schema = '", rc_schema, "' AND table_name like 'arei%' AND (table_name like '%_city_", curr_yr, "' OR table_name like '%_district_", curr_yr, "') order by table_name;")

rc_list = dbGetQuery(con, city_table_query) %>%
  select(table)

## Pull in city (or district) level education indicators
education_list <- filter(rc_list, grepl(paste0("_district_", curr_yr),table))
education_list <- education_list [order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on education_list
education_tables_list <- lapply(setNames(paste0("select * from ", rc_schema, ".", education_list), education_list), DBI::dbGetQuery, conn = con)

# drop unneeded cols, all cols after ID
education_tables_list_ <- lapply(education_tables_list, function(x) x %>% select(dist_id, district_name, ends_with("_raw"), ends_with("_rate"), ends_with("_pop"), asbest, values_count, best, ends_with("_diff"), avg, variance, index_of_disparity))

# screen tables using city pop threshold and prep for RC Functions
## first, join city_id's to education tables (one-to-many)
education_tables_list_join <- lapply(education_tables_list_, function(x) x %>% right_join(crosswalk, by = "dist_id")) # add city_id's
education_tables_list_join <- lapply(education_tables_list_join, function(x) x %>% mutate(district_name.x = ifelse(!is.na(district_name.x), district_name.x, district_name.y))) # fill in missing dist names using xwalk dist names
education_tables_list_join <- lapply(education_tables_list_join, function(x) x %>% right_join(pop_df, by = "city_id")) # add city pop data
education_tables_list_screened <- lapply(education_tables_list_join, function(x) x %>% filter(city_total_pop >= pop_threshold)) # apply pop threshold screen
education_tables_list_screened <- lapply(education_tables_list_screened, function(x) x %>% rename(geoid = dist_id, geoname = district_name.x)) # rename fields for RC functions
education_tables_list_final <- lapply(education_tables_list_screened, function(x) x %>% select(-c(city_id, city_name.x, city_name.y, city_total_pop, district_name.y))) # drop city info and dupe dist name
education_tables_list_final <- lapply(education_tables_list_final, function(x) unique(x)) # keep unique rows (1 per district)


## Pull in the non-Education city indicators  ------------------------------------------------------
# filter for only city level indicator tables
city_list <- filter(rc_list, grepl(paste0("_city_", curr_yr),table))
city_list <- filter(city_list, !grepl("index", table)) # filter out index in case there is a prev version in postgres
city_list <- city_list[order(city_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step

# import all tables on city_list
city_tables_list <- lapply(setNames(paste0("select * from ", rc_schema, ".", city_list), city_list), DBI::dbGetQuery, conn = con)

# drop unneeded cols, all cols after ID
city_tables_list_ <- lapply(city_tables_list, function(x) x %>% 
                              select(city_id, city_name, 
                                     ends_with("_raw"), 
                                     ends_with("_rate"), 
                                     ends_with("_pop"), 
                                     ends_with("_diff"),
                                     asbest, values_count, best,  
                                     avg, variance, index_of_disparity))

# remove cities that are actually universities/colleges: RC v5 there are 6 of them
city_tables_list_ <- lapply(city_tables_list_, function(x) x %>% filter(!grepl('University', city_name)))

# screen tables using city pop threshold and prep for RC Functions
city_tables_list_join <- lapply(city_tables_list_, function(x) x %>% right_join(pop_df, by = "city_id")) # add city pop data
city_tables_list_screened <- lapply(city_tables_list_join, function(x) x %>% filter(city_total_pop >= pop_threshold)) # apply pop threshold screen
city_tables_list_screened <- lapply(city_tables_list_screened, function(x) x %>% mutate(city_name.x = ifelse(!is.na(city_name.x), city_name.x, city_name.y))) # fill in missing city names using pop_df city names
city_tables_list_screened <- lapply(city_tables_list_screened, function(x) x %>% rename(geoid = city_id, geoname = city_name.x)) # rename fields for RC functions
city_tables_list_final <- lapply(city_tables_list_screened, function(x) x %>% select(-c(city_total_pop, city_name.y))) # drop city pop and dupe city name, add geolevel
city_tables_list_final <- lapply(city_tables_list_final, function(x) x %>% clean_geo_names()) # clean city names

# Edited RC Functions -------------------------------------------------------------------------
dist_list <- education_tables_list_final
city_list <- city_tables_list_final

# edited calc_z function for this script only
calc_z <- function(x) {
  #####calculate city disparity z-scores ----
  ## Total/Overall disparity_z score ##
  id_table <- dplyr::select(x, geoid, index_of_disparity)
  avg_id = mean(id_table$index_of_disparity, na.rm = TRUE) #calc avg id and std dev of id
  sd_id = sd((id_table$index_of_disparity), na.rm = TRUE)
  id_table$disparity_z <- (id_table$index_of_disparity - avg_id) / sd_id      #note the disp_z results are slightly different than pgadmin, must be due to slight methodology differences
  x$disparity_z = id_table$disparity_z                                   #add disparity_z to original table
  
  ## Raced disparity_z scores ##
  diff <- dplyr::select(x, geoid, avg, index_of_disparity, variance, ends_with("_diff"))          #get geoid, avg, variance, and raced diff columns
  diff <- diff[!is.na(diff$index_of_disparity),]                                           #exclude rows with 2+ raced values, min is best, and lowest rate is 0
  diff_long <- pivot_longer(diff, cols = ends_with("_diff"), names_to="measure_diff", values_to="diff") %>%   #pivot wide table to long on geoid & variance cols
    mutate(dispz=(diff - avg) / sqrt(variance), na.rm = TRUE) %>%                                   #calc disparity z-scores
    mutate(measure_diff=sub("_diff", "_disparity_z", measure_diff))                                #create new column names for disparity z-scores
  diff_wide <- diff_long %>% dplyr::select(geoid, measure_diff, dispz) %>%      #pivot long table back to wide keeping only geoid and new columns
    pivot_wider(names_from=measure_diff, values_from=dispz)
  x <- x %>% left_join(diff_wide, by="geoid")                           #join new columns back to original table
  
  #####calculate city performance z-scores
  ## Total/Overall performance z_scores 
  tot_table <- dplyr::select(x, geoid, total_rate)
  avg_tot = mean(tot_table$total_rate, na.rm = TRUE)      #calc avg total_rate and std dev of total_rate
  sd_tot = sd(tot_table$total_rate, na.rm = TRUE)
  asbest_ = min(x$asbest, na.rm=TRUE)
  
  if (asbest_ == 'max') {
    tot_table$performance_z <- (tot_table$total_rate - avg_tot) / sd_tot          #calc perf_z scores if MAX is best
  } else
  {
    tot_table$performance_z <- ((tot_table$total_rate - avg_tot) / sd_tot) * -1   #calc perf_z scores if MIN is best
  }
  
  tot_table <- tot_table %>% select(-c(total_rate))
  x <- x %>% left_join(tot_table, by="geoid")                           #join new columns back to original table
  
  ## Raced performance z_scores ##
  rates <- dplyr::select(x, geoid, ends_with("_rate"), -ends_with("_no_rate"), -ends_with("_moe_rate"), -total_rate)  #get geoid, avg, variance, and raced diff columns
  avg_rates <- colMeans(rates[, endsWith(colnames(rates), "_rate")], na.rm = TRUE)                                        #calc average rates for each raced rate
  a <- as.data.frame(avg_rates)                                                                     #convert to data frame
  a$measure_rate  <- c(names(avg_rates))                                                            #create join field
  sd_rates <- sapply(rates[, endsWith(colnames(rates), "_rate")], sd, na.rm = TRUE)                                       #calc std dev for each raced rate
  s <- as.data.frame(sd_rates)                                                                      #convert to data frame
  s$measure_rate  <- c(names(sd_rates))                                                             #create join field
  rates_long <- pivot_longer(rates,  cols = ends_with("_rate"), names_to="measure_rate", values_to="rate")           #pivot wide table to long on geoid & variance cols
  rates_long <- left_join(rates_long, a, by="measure_rate")                                             #join avg rates for each raced rate
  rates_long <- left_join(rates_long, s, by="measure_rate")                                             #join std dev for each raced rate
  if (asbest_ == 'max') {
    rates_long <- rates_long %>% mutate(perf=(rate - avg_rates) / sd_rates, na.rm = TRUE) %>%         #calc perf_z scores if MAX is best
      mutate(measure_perf=sub("_rate", "_performance_z", measure_rate))                   #create new column names for performance z-scores
  } else
  {
    rates_long <- rates_long %>% mutate(perf=((rate - avg_rates) / sd_rates) *-1, na.rm = TRUE) %>%   #calc perf_z scores if MIN is best
      mutate(measure_perf=sub("_rate", "_performance_z", measure_rate))                   #create new column names for performance z-scores
  }
  
  rates_wide <- rates_long %>% dplyr::select(geoid, measure_perf, perf) %>%          #pivot long table back to wide keeping only geoid and new columns
    pivot_wider(names_from=measure_perf, values_from=perf)
  
  x <- x %>% left_join(rates_wide, by = "geoid")                                #join new columns back to original table
  
  return(x)
}

# edited calc_ranks function for this script only
calc_ranks <- function(x) {
  ranks_table <- dplyr::select(x, geoid, total_rate, index_of_disparity, disparity_z, performance_z)
  asbest_ = min(x$asbest, na.rm=TRUE)

  #performance_rank: rank of 1 = best performance
  #if max is best then rank DESC / if min is best, then rank ASC. exclude NULLS.
  if(asbest_ == 'max'){
    ranks_table$performance_rank = rank(desc(ranks_table$total_rate), na.last = "keep", ties.method = "min")
  } else
  {
    ranks_table$performance_rank = rank(ranks_table$total_rate, na.last = "keep", ties.method = "min")
  }
  
  #disparity_rank: rank of 1 = worst disparity
  #max is worst, rank DESC. exclude NULLS.
  ranks_table$disparity_rank = rank(desc(ranks_table$index_of_disparity), na.last = "keep", ties.method = "min")
  
  #quadrants
  #if perf_z below avg and disp_z above avg, then red / perf_z above or avg and disp_z above or avg, then orange /
  #perf_z above or avg and disp_z below avg, then purple / perf_z below avg and disp_z below or avg, then yellow
  ranks_table$quadrant =
    ifelse(ranks_table$performance_z < 0 & ranks_table$disparity_z > 0, 'red',
           ifelse(ranks_table$performance_z >= 0 & ranks_table$disparity_z > 0, 'orange',
                  ifelse(ranks_table$performance_z >= 0 & ranks_table$disparity_z <= 0, 'purple',
                         ifelse(ranks_table$performance_z < 0 & ranks_table$disparity_z <= 0, 'yellow', NA))))

  ranks_table <- ranks_table %>% dplyr::select(geoid, disparity_rank, performance_rank, quadrant)
  x <- x %>% left_join(ranks_table , by = "geoid")

  performance_z_breaks <- quantile(x$performance_z, probs=seq(0,1, by=0.25), na.rm=TRUE)
  performance_z_breaks[[1]] <- performance_z_breaks[[1]]-1
  performance_z_breaks[[5]] <- performance_z_breaks[[5]]+1
  
  disparity_z_breaks <- quantile(x$disparity_z, probs=seq(0,1, by=0.25), na.rm=TRUE)
  disparity_z_breaks[[1]] <-disparity_z_breaks[[1]]-1
  disparity_z_breaks[[5]] <-disparity_z_breaks[[5]]+1
  
  x$performance_z_quartile <- cut(x$performance_z, 
                                  breaks=performance_z_breaks, 
                                  include.lowest=TRUE,
                                  labels=c("lowest", "low", "high", "highest"))
  x$disparity_z_quartile <- cut(x$disparity_z, 
                                breaks=disparity_z_breaks, 
                                include.lowest=TRUE,
                                labels=c("lowest", "low", "high", "highest"))  
    
  return(x)
}


# RC Calcs ----------------------------------------------------------------
#calculate DISTRICT z-scores
dist_tables <- lapply(dist_list, function(x) calc_z(x))
dist_tables <- lapply(dist_tables, function(x) calc_ranks(x))

dist_tables <- lapply(dist_tables, function(x) x %>% dplyr::rename("dist_id" = "geoid", "district_name" = "geoname")) 
View(dist_tables)

#calculate CITY z-scores
city_tables <- lapply(city_list, function(x) calc_z(x))
city_tables <- lapply(city_tables, function(x) calc_ranks(x))

city_tables <- lapply(city_tables, function(x) x %>% dplyr::rename("city_id" = "geoid", "city_name" = "geoname"))
View(city_tables)


##### Export "api_" district indicator tables ##### --------------
source <- paste0("Created on ", Sys.Date(), ". This is the screened city or district indicator table used by the API/website. It is based on the arei_ version of the table and contains data for the screened cities only including updated comparative calcs (everything after ID) based on the screened list of cities. Script: W:\\Project\\RACE COUNTS\\", curr_yr, "_", rc_schema, "\\RC_Github\\RaceCounts\\IndicatorScripts\\screened_city_stats.R. QA doc: ", qa_filepath, ".")

# Make list of api district table names
table_names_d <- as.data.frame(names(dist_list)) %>% rename('table_name' = 1) %>% mutate(table_name = gsub("arei_", "api_", table_name))
table_names_d_ = table_names_d[['table_name']]  # convert to character vector

# loop to export individual tables
for (i in 1:length(dist_tables)) {

  dbWriteTable(con,
               Id(schema = rc_schema, table_name = table_names_d_[i]), 
               dist_tables[[i]], overwrite = FALSE, row.names = FALSE)
  comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", table_names_d_[i],  " IS '", source, "';")
  dbBegin(con)
  dbExecute(con, comment)
  dbCommit(con)
  
}

##### Export "api_" city indicator tables ##### --------------
# Make list of api city table names
table_names_c <- as.data.frame(names(city_list)) %>% rename('table_name' = 1) %>% mutate(table_name = gsub("arei_", "api_", table_name))
table_names_c_ = table_names_c[['table_name']]  # convert to character vector

# loop to export individual tables
for (i in 1:length(city_tables)) {
  
  dbWriteTable(con, 
               Id(schema = rc_schema, table_schema = table_names_c_[i]),
               city_tables[[i]], overwrite = FALSE, row.names = FALSE)
  comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", table_names_c_[i],  " IS '", source, "';")
  dbBegin(con)
  dbExecute(con, comment)
  dbCommit(con)
  
}


# dbDisconnect(con)

