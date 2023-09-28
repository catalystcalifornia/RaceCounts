#### Composite Index (z-score) for RC v5 ####

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

# pull in cross-walk to go from district to city
crosswalk <- dbGetQuery(con, "SELECT  city_id, city_name, dist_id, total_enroll FROM v5.arei_city_county_district_table")


# Education Section -------------------------------------------------------
## These indicators require extra prep bc they are at school district not city level
# pull in list of tables in racecounts.v5
rc_list = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v5"))$table, function(x) slot(x, 'name'))))

## Pull in city (or district) level education indicators (ends with school district)
education_list <- filter(rc_list, grepl("_district_2023",table))
education_list <- education_list [order(education_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on education_list
education_tables <- lapply(setNames(paste0("select * from v5.", education_list), education_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
education_tables  <- map2(education_tables, names(education_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

education_tables_disparity <- lapply(education_tables, function(x) x%>% select(dist_id, district_name, disparity_z, indicator))

education_disparity <- imap_dfr(education_tables_disparity, ~
               .x %>% 
                   pivot_longer(cols = disparity_z,
                 names_to = "measure",
                 values_to = "disparity_z_score_unweighted")) %>% select(-measure)



education_tables_performance <- lapply(education_tables, function(x) x%>% select(dist_id,district_name, performance_z, indicator))


education_performance <- imap_dfr(education_tables_performance, ~
               .x %>% 
                   pivot_longer(cols = performance_z,
                 names_to = "measure",
                 values_to = "performance_z_score_unweighted")) %>% select(-measure)


df_education_district <- education_disparity %>% full_join(education_performance)  %>% left_join(crosswalk, by = "dist_id")

## extra step: weighted averages by enrollment size per indicator ( some cities have multiple districts but not every indicator will have multiple districts per city).

enrollment_percentages <- df_education_district %>% select(city_id, dist_id, total_enroll, indicator) %>% unique() %>% group_by(city_id,  indicator) %>% mutate(sum_total_enroll = sum(total_enroll, na.rm = T), 
                                                             percent_total_enroll = (total_enroll/sum_total_enroll)) %>% ungroup() %>% select(city_id, dist_id, indicator, total_enroll, sum_total_enroll, percent_total_enroll)

df_education_district_weighted <- df_education_district %>% left_join(enrollment_percentages, by = c("city_id","dist_id","indicator", "total_enroll")) %>%
  mutate(
disparity_z_score = disparity_z_score_unweighted * percent_total_enroll,
performance_z_score = performance_z_score_unweighted * percent_total_enroll
)

df_education_city <- df_education_district_weighted %>% group_by(city_id, city_name, indicator) %>% summarize(disp_z = sum(disparity_z_score), perf_z = sum(performance_z_score))


education_tables_agg <- df_education_city %>% pivot_wider(
  names_from = indicator,
  values_from = c(disp_z, perf_z),
  names_glue = "{indicator}_{.value}",
)

### commenting out previous code where I just averaged
# rename column 
#education_tables_short <- lapply(education_tables, function(x) x%>% select(dist_id, district_name, disparity_z, performance_z, indicator))

#education_tables_updated <-
#  lapply(names(education_tables_short), function(i){
#   x <- education_tables_short[[ i ]]
#    # set 2nd column to a new name
#    names(x)[3] <- paste0(i, "_disp_z")
#    names(x)[4] <- paste0(i, "_perf_z")
#    # return
#    x
#  })
 

# only select columns we want
#education_tables_updated <- lapply(education_tables_updated, function(x) x%>% select(dist_id, district_name, ends_with("disp_z"), ends_with("perf_z")))

# make into df
#education_tables_df <- education_tables_updated  %>% reduce(full_join) %>% arrange(district_name)


#
# merge cross-walk
#education_tables_df_crosswalk <- crosswalk %>% select(city_id, city_name, dist_id, district_name) %>% left_join(education_tables_df, by = "dist_id") %>% # select(city_id, city_name, ends_with("z"))



# pivot longer and wider
#education_tables_agg<- education_tables_df_crosswalk %>% pivot_longer(
#                                                                        cols = ends_with("z"),
#                                                                        names_to = "indicator",
#                                                                        values_to = "value" ) %>%
#                        group_by(city_id, city_name, indicator) %>% mutate(mean = mean(value, na.rm=TRUE)) %>% 
#                        select(-value) %>% ungroup() %>% distinct(city_id, city_name, indicator, mean) %>% 
#                       pivot_wider(names_from = "indicator", values_from = "mean") %>% mutate_all(~ifelse(is.nan(.), NA, .))


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

city_tables_df <- city_tables_updated  %>% reduce(full_join) %>% arrange(city_id) %>% distinct(city_id, .keep_all = TRUE) %>% left_join(education_tables_agg) %>% left_join(arei_race_multigeo) %>% select(city_id, city_name, everything())

# remove cities: there are 6 of them
city_tables_df <- city_tables_df %>% filter(!grepl('University', city_name))


# cap perf_z and disp_z values  at 3.5 
city_tables_capped <- city_tables_df %>% mutate(across(ends_with("disp_z"), 
             ~ case_when(. > 3.5 ~ 3.5, 
                       . < -3.5 ~ -3.5,
                       TRUE ~ .))) %>% 
  
  mutate(across(ends_with("perf_z"), 
             ~ case_when(. > 3.5 ~ 3.5, 
                       . < -3.5 ~ -3.5,
                       TRUE ~ .)))

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
crim <- city_tables_capped %>% select(city_id, city_name, starts_with("arei_crim")) %>% mutate(crim_count = crim_count) 
demo <- city_tables_capped %>% select(city_id, city_name, starts_with("arei_demo")) %>% mutate(demo_count = demo_count) 
econ <- city_tables_capped %>% select(city_id, city_name, starts_with("arei_econ")) %>% mutate(econ_count = econ_count) 
hben <- city_tables_capped %>% select(city_id, city_name, starts_with("arei_hben")) %>% mutate(hben_count = hben_count) 
hlth <- city_tables_capped %>% select(city_id, city_name, starts_with("arei_hlth")) %>% mutate(hlth_count = hlth_count) 
hous <- city_tables_capped %>% select(city_id, city_name, starts_with("arei_hous")) %>% mutate(hous_count = hous_count) 
educ <- city_tables_capped %>% select(city_id, city_name, starts_with("arei_educ"))   %>% mutate(educ_count = educ_count) 

# now calculate average disparity/performance z-score and use weight


# Method: average, then weigh overall average z-score

## Function

## issue area threshold
crim_threshold <- 1
demo_threshold <- 1
econ_threshold <- 2 # could be 3
educ_threshold <- 2
hlth_threshold <- 1
hben_threshold <- 2
hous_threshold <- 3



calculate_city_weighted_z <- function(x,y,z) {
  
  ##  for city issue areas with only 1 indicator
  if (y == "1") {
    x <- x %>% select(city_id, city_name, ends_with("z")) %>% rename(
      disp_z =  ends_with("disp_z"),
      perf_z = ends_with("perf_z")
    )
    
     
  # Cap Issue Index z-scores at |2| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% mutate(
          disp_z = case_when(
            disp_z > 2 ~ 2,
            disp_z < -2 ~ -2,
            TRUE ~ disp_z)
        ) %>%
        mutate (
          perf_z = case_when(
            perf_z > 2 ~ 2,
            perf_z < -2 ~ -2,
            TRUE ~ perf_z)
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
  
  ##ISSUE THRESHOLD
  x <-  x %>% mutate(
    disp_z =  ifelse(disp_values_count < z, NA, disp_avg),
    perf_z = ifelse(perf_values_count < z, NA,perf_avg),
    
  )
  
  # Cap Issue Index z-scores at |2| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% mutate(
          disp_z = case_when(
            disp_z > 2 ~ 2,
            disp_z < -2 ~ -2,
            TRUE ~ disp_z)
        ) %>%
        mutate (
          perf_z = case_when(
            perf_z > 2 ~ 2,
            perf_z < -2 ~ -2,
            TRUE ~ perf_z)
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
  
  ## ISSUE AREA THRESHOLD, INDICATOR THRESHOLD AND cap
  x <-  x %>% mutate(
    disparity_z =  ifelse(disp_values_count < y, NA, disp_avg),
    performance_z = ifelse(perf_values_count < y, NA, perf_avg)) %>%
     mutate(
    disparity_z =  ifelse(all_indicators_disp_count <=z, NA, disp_avg),
    performance_z = ifelse(all_indicators_disp_count <=z, NA, perf_avg)
) %>%
    mutate(
          disparity_z = case_when(
            disparity_z > 1 ~ 1,
            disparity_z < -1 ~ -1,
            TRUE ~ disparity_z)
        ) %>%
        mutate (
          performance_z = case_when(
             performance_z > 1 ~ 1,
             performance_z < -1 ~ -1,
            TRUE ~  performance_z)
        )

  
 
  
  
  # select columns with average weighted z-score, then rename to reflect issue area
  x %>% select(city_id, city_name, ends_with("count"), disparity_z, performance_z, ends_with("disp_z"), ends_with("perf_z")) 
  
}


issue_area_threshold <- 3 # place must have at least 3 perf/disp z-scores or it will be supressed
indicator_threshold <- 12

index_table_final_screen <-  calculate_city_index(all_index, issue_area_threshold, indicator_threshold) 

# Export to postgres ------------------------------------------------------
table_name <- "arei_composite_index_city_2023"
table_schema <- "v5"
table_comment_source <- "This is the official index table for v5 City calculations including threshold for representation across all issue areas and indicators)
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



