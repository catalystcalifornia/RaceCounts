#### Composite Index Index (z-score) for RC v5 ####

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


# Education Section -------------------------------------------------------

# pull in list of tables in racecounts.v5
rc_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v5"))$table, function(x) slot(x, 'name'))))

## Pull in education indicators (ends with school district)

# filter for only city level indicator tables
education_list <- filter(rc_list, grepl("_district_2023",table))
education_list <- education_list [order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step



# import all tables on city_list
education_tables <- lapply(setNames(paste0("select * from v5.", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables  <- map2(education_tables, names(education_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name



# rename column 
education_tables_short <- lapply(education_tables, function(x) x%>% select(dist_geoid, district_name, disparity_z, performance_z, indicator))


education_tables_updated <-
  lapply(names(education_tables_short), function(i){
    x <- education_tables_short[[ i ]]
    # set 2nd column to a new name
    names(x)[3] <- paste0(i, "_disp_z")
    names(x)[4] <- paste0(i, "_perf_z")
    # return
    x
  })


# only select columns we want
education_tables_updated <- lapply(education_tables_updated, function(x) x%>% select(dist_geoid, district_name, ends_with("disp_z"), ends_with("perf_z")))


# make into df
education_tables_df <- education_tables_updated  %>% reduce(full_join) %>% arrange(district_name)


# pull in cross-walk
crosswalk <- dbGetQuery(con, "SELECT * FROM v5.arei_city_county_district_table")

# merge cross-walk
education_tables_df_crosswalk <- crosswalk %>% select(city_id, city_name, dist_geoid, district_name) %>% left_join(education_tables_df, by = "dist_geoid") %>% select(city_id, city_name, ends_with("z"))

# pivot longer and wider
education_tables_avg <- education_tables_df_crosswalk  %>% pivot_longer(
  cols = ends_with("z"),
  names_to = "indicator",
  values_to = "value"
) %>% group_by(city_id, city_name, indicator)  %>% mutate(mean = mean(value, na.rm=TRUE)) %>% select(-value) %>% ungroup() %>% distinct(city_id, indicator, mean) %>% pivot_wider(names_from = "indicator", values_from = "mean") %>% mutate_all(~ifelse(is.nan(.), NA, .))





## Add the rest of the indicators  ------------------------------------------------------

# pull in list of tables in racecounts.v5

# filter for only city level indicator tables
city_list <- filter(rc_list, grepl("_city_2023",table))
city_list <- city_list[order(city_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step


# import all tables on city_list
city_tables <- lapply(setNames(paste0("select * from v5.", city_list), city_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
city_tables <- map2(city_tables, names(city_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name


# rename column 
city_tables_short <- lapply(city_tables, function(x) x%>% select(city_id, city_name, disparity_z, performance_z, indicator))

city_tables_updated <-
  lapply(names(city_tables_short), function(i){
    x <- city_tables_short[[ i ]]
    # set 2nd column to a new name
    names(x)[3] <- paste0(i, "_disp_z")
    names(x)[4] <- paste0(i, "_perf_z")
    # return
    x
  })

# only select columns we want
city_tables_updated <- lapply(city_tables_updated, function(x) x%>% select(city_id, ends_with("disp_z"), ends_with("perf_z")))


# make into df
arei_race_multigeo <- dbGetQuery(con, "SELECT geoid, name, geolevel  FROM v5.arei_race_multigeo") %>% filter(geolevel == "place") %>% rename(city_id = geoid, city_name = name) %>% select(-geolevel)

# merge to get city names and education table

city_tables_df <- city_tables_updated  %>% reduce(full_join) %>% arrange(city_id) %>% distinct(city_id, .keep_all = TRUE) %>% left_join(education_tables_avg) %>% left_join(arei_race_multigeo) %>% select(city_id, city_name, everything())


# 6 indicators don't have a city name
#arei_race_multigeo %>% filter(
#city_id %in% c("0607379", "0610559", "0633633", "0662868", "0675168", "0675588")
#)

## pull in city screen table from Leila's script to compare 

city_screen <- dbGetQuery(con, "SELECT * FROM v5.api_city_list")

# merge- there are 654 cities
# city_tables_screened <- city_screen %>% left_join(city_tables_df, by = c("city_id"))

# Look at perf_na and disp_na across all indicators. We will need this later
all_indicators_perf_count<- city_tables_df %>% select(city_id, ends_with("perf_z")) %>%
  mutate(all_indicators_perf_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_perf_count)

all_indicators_disp_count <- city_tables_df %>% select(city_id, ends_with("disp_z")) %>%
  mutate(all_indicators_disp_count = rowSums(!is.na(.))) %>% select(city_id, all_indicators_disp_count)


# count number of indicators per issue area
indicators <- names(city_tables) %>% as.data.frame()
educ_indicators <- names(education_tables) %>% as.data.frame()

crim_count <- length(grep("crim", indicators$.))
demo_count <- length(grep("demo", indicators$.))
econ_count <- length(grep("econ", indicators$.))
hben_count <- length(grep("hben", indicators$.))
hlth_count <- length(grep("hlth", indicators$.))
hous_count <- length(grep("hous", indicators$.))
educ_count <- length(grep("educ", educ_indicators$.))



# make separate data-frames for all
crim <- city_tables_df %>% select(city_id, city_name, starts_with("arei_crim"), perf_na, disp_na) %>% mutate(crim_count = crim_count) 
demo <- city_tables_df %>% select(city_id, city_name, starts_with("arei_demo"), perf_na, disp_na) %>% mutate(demo_count = demo_count) 
econ <- city_tables_df %>% select(city_id, city_name, starts_with("arei_econ"), perf_na, disp_na) %>% mutate(econ_count = econ_count) 
hben <- city_tables_df %>% select(city_id, city_name, starts_with("arei_hben"), perf_na, disp_na) %>% mutate(hben_count = hben_count) 
hlth <- city_tables_df %>% select(city_id, city_name, starts_with("arei_hlth"), perf_na, disp_na) %>% mutate(hlth_count = hlth_count) 
hous <- city_tables_df %>% select(city_id, city_name, starts_with("arei_hous"), perf_na, disp_na) %>% mutate(hous_count = hous_count) 
educ <- city_tables_df %>% select(city_id, city_name, starts_with("arei_educ"), perf_na, disp_na)   %>% mutate(educ_count = educ_count) 

# now calculate average disparity/performance z-score and use weight


# Method: average, then weigh overall average z-score

## Function

## issue area threshold
crim_threshold <- 1
demo_threshold <- 1
econ_threshold <- 2 # could be 3
educ_threshold <- 3
hlth_threshold <- 1
hben_threshold <- 2
hous_threshold <- 5



calculate_city_weighted_z <- function(x,y,z) {
  
  ##  for city issue areas with only 1 indicator
  if (y == "1") {
    x <- x %>% select(city_id, city_name, ends_with("z")) %>% rename(
      disp_z =  ends_with("disp_z"),
      perf_z = ends_with("perf_z")
    )
    
    return(x)
  }
  
  
  # count performance z-scores
  rates_performance <- select(x, ends_with("perf_z"))
  rates_performance$perf_values_count <- rowSums(!is.na(rates_performance))
  
  x$perf_values_count <- rates_performance$perf_values_count
  
  # count disparity z-scores
  rates_disparity <- select(x, ends_with("disp_z"))
  rates_disparity$values_count <- rowSums(!is.na(rates_disparity))
  
  x$disp_values_count <- rates_disparity$values_count
  
  # calculate avg disparity z-scores
  disp_avg <- select(x, city_id, grep("disp_z", colnames(x)))
  disp_avg$disp_avg <- rowMeans(disp_avg[,-1], na.rm = TRUE)
  disp_avg <- select(disp_avg, city_id, disp_avg) 
  disp_avg$disp_avg[is.nan(disp_avg$disp_avg)] <- NA
  
  x <- x %>% left_join(disp_avg, by="city_id")   
  
  # calculate average performance z scores
  perf_avg <- select(x, city_id, grep("perf_z", colnames(x)))                         
  perf_avg$perf_avg <- rowMeans(perf_avg[,-1], na.rm = TRUE)                           
  perf_avg <- select(perf_avg, city_id, perf_avg)                    
  perf_avg$perf_avg[is.nan(perf_avg$perf_avg)] <- NA
  
  x <- x %>% left_join(perf_avg, by="city_id")
  
  ## IF THERE IS AN INDICATOR THRESHOLD, APPL THIS HERE ( USE IFELSE STATEMENT: LOOK AT LINE 162 OF RC_INDEX_FUNCTIONS)
  x <-  x %>% mutate(
    disp_z =  ifelse(disp_values_count < z, NA, disp_avg * (1/y)),
    perf_z = ifelse(perf_values_count < z, NA,perf_avg * (1/y)),
    
  )
  
  # select columns with average weighted z-score, then rename to reflect issue area
  x %>% select(city_id, ends_with("count"), disp_z, perf_z) 
  
}


crim_index <-  calculate_city_weighted_z(crim, crim_count, crim_threshold) %>% rename_with(~ paste0('crime_and_justice', "_", .x),ends_with("z"))
demo_index <-  calculate_city_weighted_z(demo, demo_count, demo_threshold) %>% rename_with(~ paste0('democracy', "_", .x),ends_with("z"))
econ_index <-  calculate_city_weighted_z(econ, econ_count, econ_threshold) %>% rename_with(~ paste0('economic_opportunity', "_", .x),ends_with("z"))
hben_index <-  calculate_city_weighted_z(hben, hben_count, hben_threshold) %>% rename_with(~ paste0('healthy_built_environment', "_", .x),ends_with("z"))
hlth_index <-  calculate_city_weighted_z(hlth, hlth_count, hlth_threshold) %>% rename_with(~ paste0('health_care_access', "_", .x),ends_with("z"))
hous_index <-  calculate_city_weighted_z(hous,hous_count, hous_threshold) %>% rename_with(~ paste0('housing', "_", .x),ends_with("z"))
educ_index <-  calculate_city_weighted_z(educ,educ_count, educ_threshold) %>% rename_with(~ paste0('education', "_", .x),ends_with("z"))


# merge all index tables together


all_index <- crim_index %>% select(city_id, ends_with("z")) %>% left_join(
  demo_index %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  econ_index %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  hben_index %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  hlth_index %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  hous_index %>% select(city_id, ends_with("z"))   
)   %>% left_join(
  educ_index %>% select(city_id, ends_with("z"))   
)  %>% left_join(arei_race_multigeo) %>% mutate(
  all_indicators_disp_count = all_indicators_disp_count$all_indicators_disp_count,
  all_indicators_perf_count = all_indicators_perf_count$all_indicators_perf_count,
) %>% select(
  city_id, city_name, ends_with("count"), everything()
)  



# Final function: overall index
issue_area_threshold <- 3 # place must have at least 3 perf/disp z-scores or it will be supressed
indicator_threshold <- 12

calculate_city_index <- function(x,y,z) {
  
  # count performance z-scores
  rates_performance <- select(x, ends_with("perf_z"))
  rates_performance$perf_values_count <- rowSums(!is.na(rates_performance))
  
  x$perf_values_count <- rates_performance$perf_values_count
  
  # count disparity z-scores
  rates_disparity <- select(x, ends_with("disp_z"))
  rates_disparity$values_count <- rowSums(!is.na(rates_disparity))
  
  x$disp_values_count <- rates_disparity$values_count
  
  # calculate avg disparity z-scores
  disp_avg <- select(x, city_id, grep("disp_z", colnames(x)))
  disp_avg$disp_avg <- rowMeans(disp_avg[,-1], na.rm = TRUE)
  disp_avg <- select(disp_avg, city_id, disp_avg) 
  disp_avg$disp_avg[is.nan(disp_avg$disp_avg)] <- NA
  
  x <- x %>% left_join(disp_avg, by="city_id")   
  
  # calculate average performance z scores
  perf_avg <- select(x, city_id, grep("perf_z", colnames(x)))                         
  perf_avg$perf_avg <- rowMeans(perf_avg[,-1], na.rm = TRUE)                           
  perf_avg <- select(perf_avg, city_id, perf_avg)                    
  perf_avg$perf_avg[is.nan(perf_avg$perf_avg)] <- NA
  
  x <- x %>% left_join(perf_avg, by="city_id")
  
  ## IF THERE IS AN INDICATOR THRESHOLD, APPLY THIS HERE ( USE IFELSE STATEMENT: LOOK AT LINE 162 OF RC_INDEX_FUNCTIONS)
  x <-  x %>% mutate(
    disparity_z =  ifelse(disp_values_count < y, NA, disp_avg),
    performance_z = ifelse(perf_values_count < y, NA, perf_avg),
    screening =  ifelse(all_indicators_disp_count >=z, 1, 0) 
    
  )
  
  # select columns with average weighted z-score, then rename to reflect issue area
  x %>% select(city_id, city_name, screening, ends_with("count"), disparity_z, performance_z, ends_with("disp_z"), ends_with("perf_z")) 
  
}

index_table_final_screen <-  calculate_city_index(all_index, issue_area_threshold, indicator_threshold) 

`

# Export to postgres ------------------------------------------------------
table_name <- "arei_composite_index_city_2023"
table_schema <- "v5"
table_comment_source <- "This is the official index table for v5 City calculations.Cities have a flag column if they don't have a disparity_z column for the minimum indicator threshold (out of 30)
R script used to calculate rates and import table: W:/Project/RACE COUNTS/2023_v5/RC_Github/RaceCounts/IndexScripts/composite_index_2023.R 
QA document:  W:/Project/RACE COUNTS/2023_v5/Composite Index/Documentation/QA_sheet_Composite_Index.docx';" 

# make character vector for field types in postgresql db
charvect = rep('numeric', dim(index_table_final_screen)[2])

# change data type for columns
charvect[c(1:2)] <- "varchar" # Define which cols are character for the geoid and names etc

# add names to the character vector
names(charvect) <- colnames(index_table_final_screen)

# dbWriteTable(con, c(table_schema, table_name), index_table_final_screen,
#           overwrite = FALSE, row.names = FALSE,
#            field.types = charvect)

# write comment to table, and the first three fields that won't change.
table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")

## send table comment to database
#dbSendQuery(conn = con, table_comment)      		

