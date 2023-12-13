#### Screened City Indicator Z-Scores, Ranks etc. for RC v5 ####

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgreSQL","sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# Packages ----------------------------------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(sf)


# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# set city population threshold
pop_threshold <- 10000

# pull in city-district crosswalk and city population data
crosswalk <- dbGetQuery(con, "SELECT  city_id, city_name, dist_id FROM v5.arei_city_county_district_table")
pop_df <- dbGetQuery(con, "SELECT geoid AS city_id, total_pop AS city_total_pop FROM v5.arei_multigeo_list WHERE geolevel = 'place'")

# Pull in Education city indicators -------------------------------------------------------
## These indicators require extra prep bc they are at school district not city level
# pull in list of tables in racecounts.v5
rc_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v5"))$table, function(x) slot(x, 'name'))))

## Pull in city (or district) level education indicators
education_list <- filter(rc_list, grepl("_district_2023",table))
education_list <- education_list [order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on education_list
education_tables_list <- lapply(setNames(paste0("select * from v5.", education_list), education_list), DBI::dbGetQuery, conn = con)

# drop unneeded cols, all cols after ID
education_tables_list_ <- lapply(education_tables_list, function(x) x %>% select(dist_id, district_name, ends_with("_raw"), ends_with("_rate"), ends_with("_pop"), asbest, values_count, best, ends_with("_diff"), avg, variance, index_of_disparity))

# screen tables using city pop threshold and prep for RC Functions
## first, join city_id's to education tables (one-to-many)
education_tables_list_join <- lapply(education_tables_list_, function(x) x %>% right_join(crosswalk, by = "dist_id")) # add city_id's
education_tables_list_join <- lapply(education_tables_list_join, function(x) x %>% right_join(pop_df, by = "city_id")) # add city pop data
education_tables_list_screened <- lapply(education_tables_list_join, function(x) x %>% filter(city_total_pop >= pop_threshold)) # apply pop threshold screen
education_tables_list_screened <- lapply(education_tables_list_screened, function(x) x %>% rename(geoid = dist_id, geoname = district_name)) # rename fields for RC functions
education_tables_list_final <- lapply(education_tables_list_screened, function(x) x %>% select(-c(city_id, city_name, city_total_pop))) # drop city info
education_tables_list_final <- lapply(education_tables_list_final, function(x) unique(x)) # keep unique rows (1 per district)


## Pull in the non-Education city indicators  ------------------------------------------------------
# filter for only city level indicator tables
city_list <- filter(rc_list, grepl("_city_2023",table))
city_list <- filter(city_list, !grepl("index", table)) # filter out index in case there is a prev version in postgres
city_list <- city_list[order(city_list$table), ] # alphabetize list of tables, changes df to list the needed format for next step

# import all tables on city_list
city_tables_list <- lapply(setNames(paste0("select * from v5.", city_list), city_list), DBI::dbGetQuery, conn = con)

# drop unneeded cols, all cols after ID
city_tables_list_ <- lapply(city_tables_list, function(x) x %>% select(city_id, city_name, ends_with("_raw"), ends_with("_rate"), ends_with("_pop"), asbest, values_count, best, ends_with("_diff"), avg, variance, index_of_disparity))

# remove cities that are actually universities/colleges: RC v5 there are 6 of them
city_tables_list_ <- lapply(city_tables_list_, function(x) x %>% filter(!grepl('University', city_name)))

# screen tables using city pop threshold and prep for RC Functions
city_tables_list_join <- lapply(city_tables_list_, function(x) x %>% right_join(pop_df, by = "city_id")) # add city pop data
city_tables_list_screened <- lapply(city_tables_list_join, function(x) x %>% filter(city_total_pop >= pop_threshold)) # apply pop threshold screen
city_tables_list_screened <- lapply(city_tables_list_screened, function(x) x %>% rename(geoid = city_id, geoname = city_name)) # rename fields for RC functions
city_tables_list_final <- lapply(city_tables_list_screened, function(x) x %>% select(-c(city_total_pop))) # drop city pop and add geolevel


# Edited RC Functions -------------------------------------------------------------------------
dist_list <- education_tables_list_final
city_list <- city_tables_list_final

# edited calc_z function for this script only
calc_z <- function(x) {
  #####calculate county disparity z-scores ----
  ## Total/Overall disparity_z score ##
  id_table <- dplyr::select(x, geoid, index_of_disparity)
  avg_id = mean(id_table$index_of_disparity, na.rm = TRUE) #calc avg id and std dev of id
  sd_id = sd((id_table$index_of_disparity), na.rm = TRUE)
  id_table$disparity_z <- (id_table$index_of_disparity - avg_id) / sd_id      #note the disp_z results are slightly different than pgadmin, must be due to slight methodology differences
  x$disparity_z = id_table$disparity_z                                   #add disparity_z to original table
  
  ## Raced disparity_z scores ##
  diff <- dplyr::select(x, geoid, avg, index_of_disparity, variance, ends_with("_diff"))          #get geoid, avg, variance, and raced diff columns
  diff <- diff[!is.na(diff$index_of_disparity),]                                           #exclude rows with 2+ raced values, min is best, and lowest rate is 0
  diff_long <- pivot_longer(diff, 5:ncol(diff), names_to="measure_diff", values_to="diff") %>%   #pivot wide table to long on geoid & variance cols
    mutate(dispz=(diff - avg) / sqrt(variance), na.rm = TRUE) %>%                                   #calc disparity z-scores
    mutate(measure_diff=sub("_diff", "_disparity_z", measure_diff))                                #create new column names for disparity z-scores
  diff_wide <- diff_long %>% dplyr::select(geoid, measure_diff, dispz) %>%      #pivot long table back to wide keeping only geoid and new columns
    pivot_wider(names_from=measure_diff, values_from=dispz)
  x <- x %>% left_join(diff_wide, by="geoid")                           #join new columns back to original table
  
  #####calculate county performance z-scores
  ## Total/Overall performance z_scores ## Note the perf_z results are slightly different than pgadmin, must be due to slight methodology differences
  #tot_table <- dplyr::select(x, geoid, asbest, total_rate)
  #new chunk#
  tot_table <- dplyr::select(x, geoid, total_rate)
  #end new chunk#
  avg_tot = mean(tot_table$total_rate, na.rm = TRUE)      #calc avg total_rate and std dev of total_rate
  sd_tot = sd(tot_table$total_rate, na.rm = TRUE)
  
  #new chunk#
  asbest_ = min(x$asbest, na.rm=TRUE)
  #end new chunk# 
  
  if (asbest_ == 'max') {
    tot_table$performance_z <- (tot_table$total_rate - avg_tot) / sd_tot          #calc perf_z scores if MAX is best
  } else
  {
    tot_table$performance_z <- ((tot_table$total_rate - avg_tot) / sd_tot) *-1   #calc perf_z scores if MIN is best
  }
  #x$performance_z = tot_table$performance_z    #add performance_z to original table
  
  tot_table <- tot_table %>% select(-c(total_rate))
  x <- x %>% left_join(tot_table, by="geoid")                           #join new columns back to original table
  
  ## Raced performance z_scores ##
  #rates <- dplyr::select(x, geoid, asbest, ends_with("_rate"), -ends_with("_no_rate"), -ends_with("_moe_rate"), -total_rate)  #get geoid, avg, variance, and raced diff columns
  #new chunk#
  rates <- dplyr::select(x, geoid, ends_with("_rate"), -ends_with("_no_rate"), -ends_with("_moe_rate"), -total_rate)  #get geoid, avg, variance, and raced diff columns
  #end new chunk#
  avg_rates <- colMeans(rates[,3:ncol(rates)], na.rm = TRUE)                                        #calc average rates for each raced rate
  a <- as.data.frame(avg_rates)                                                                     #convert to data frame
  a$measure_rate  <- c(names(avg_rates))                                                            #create join field
  sd_rates <- sapply(rates[,3:ncol(rates)], sd, na.rm = TRUE)                                       #calc std dev for each raced rate
  s <- as.data.frame(sd_rates)                                                                      #convert to data frame
  s$measure_rate  <- c(names(sd_rates))                                                             #create join field
  rates_long <- pivot_longer(rates, 3:ncol(rates), names_to="measure_rate", values_to="rate")           #pivot wide table to long on geoid & variance cols
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
  #ranks_table <- dplyr::select(x, geoid, asbest, total_rate, index_of_disparity, disparity_z, performance_z)
  #new chunk#
  ranks_table <- dplyr::select(x, geoid, total_rate, index_of_disparity, disparity_z, performance_z)
  asbest_ = min(x$asbest, na.rm=TRUE)
  #end new chunk#
  
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
           ifelse(ranks_table$performance_z > 0 & ranks_table$disparity_z >= 0, 'orange',
                  ifelse(ranks_table$performance_z >= 0 & ranks_table$disparity_z < 0, 'purple',
                         ifelse(ranks_table$performance_z < 0 & ranks_table$disparity_z <= 0, 'yellow', NA))))
  
  ranks_table <- ranks_table %>% dplyr::select(geoid, disparity_rank, performance_rank, quadrant)
  x <- x %>% left_join(ranks_table , by = "geoid")
  
  return(x)
}


# RC Calcs ----------------------------------------------------------------
### Will add in disp and perf quartile calcs in later. ### Ref: W:\Project\RACE COUNTS\2023_v5\API\quartiles_quadrants\add_api_table_quadrants_quartiles.R (lines 146-225 and 229-255)
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
# Make list of api district table names
table_names_d <- as.data.frame(names(dist_list)) %>% rename('table_name' = 1) %>% mutate(table_name = gsub("arei_", "api_", table_name))
table_names_d_ = table_names_d[['table_name']]  # convert to character vector
rc_schema <- "v5"

# loop to export individual tables
for (i in 1:length(dist_tables)) {

  dbWriteTable(con, c(rc_schema, table_names_d_[i]), dist_tables[[i]], overwrite = FALSE, row.names = FALSE)
 
}

##### Export "api_" city indicator tables ##### --------------
# Make list of api city table names
table_names_c <- as.data.frame(names(city_list)) %>% rename('table_name' = 1) %>% mutate(table_name = gsub("arei_", "api_", table_name))
table_names_c_ = table_names_c[['table_name']]  # convert to character vector
rc_schema <- "v5"

# loop to export individual tables
for (i in 1:length(city_tables)) {
  
  dbWriteTable(con, c(rc_schema, table_names_c_[i]), city_tables[[i]], overwrite = FALSE, row.names = FALSE)
  
}


# dbDisconnect(con)

