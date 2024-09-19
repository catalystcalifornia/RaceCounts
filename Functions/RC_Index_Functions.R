# RC Index Functions

## To use these functions, your input tables need to have performance_z and disparity_z columns
## as well as the county_id and county_name columns (city_id and city_name for city index fx). 
## These functions cap z-scores according to: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=QlUysV
## as well as calculate issue and composite indexes and ranks


############# COUNTY INDEX FUNCTIONS ############# --------------------------------------------------

# Cap INDICATOR Z-Scores ---------------------------------------------------
clean_data_z <- function(x, y) {

  # Cap Indicator z-scores at |3.5| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% select(county_id, county_name, performance_z, disparity_z) %>%  
    mutate(
    disparity_z = case_when(
      disparity_z > 3.5 ~ 3.5,
      disparity_z < -3.5 ~ -3.5,
      TRUE ~ disparity_z)
  ) %>%
    mutate (
      performance_z = case_when(
        performance_z > 3.5 ~ 3.5,
        performance_z < -3.5 ~ -3.5,
        TRUE ~ performance_z))

  x <- x %>% rename_with(~ paste0(y, "_", .x), ends_with("_z"))
  
  
return(x)
}

# Cap ISSUE INDEX Z-Scores ---------------------------------------------------
clean_index_data_z <- function(x, y) {
  
  # Cap Issue z-scores at |2| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  clean <- x %>% select(county_id, county_name, ends_with("performance_z"), ends_with("disparity_z"))   
  clean <- clean %>% mutate(performance_z = clean[, c(3)], disparity_z = clean[, c(4)]) %>%   
    mutate(
      disparity_z = case_when(
        disparity_z > 2 ~ 2,
        disparity_z < -2 ~ -2,
        TRUE ~ disparity_z)
    ) %>%
    mutate(
      performance_z = case_when(
        performance_z > 2 ~ 2,
        performance_z < -2 ~ -2,
        TRUE ~ performance_z))
  
  x <- clean %>% rename_with(~ paste0(y, "_", .x), ends_with("_z")) %>% select(-c(3,4))
  
  return(x)
}

# Calculate and cap ISSUE INDEX ---------------
calculate_z <- function(x) {
# count performance z-scores
  rates_performance <- select(x, ends_with("perf_z"))
  rates_performance$perf_values_count <- rowSums(!is.na(rates_performance))
  
  x$perf_values_count <- rates_performance$perf_values_count

  # count disparity z-scores
  rates_disparity <- select(x, ends_with("disp_z"))
  rates_disparity$values_count <- rowSums(!is.na(rates_disparity))

  x$disp_values_count <- rates_disparity$values_count
  
  # calculate avg disparity z-scores
  disp_avg <- select(x, county_id, grep("disp_z", colnames(x)))
  disp_avg$disp_avg <- rowMeans(disp_avg[,-1], na.rm = TRUE)
  disp_avg <- select(disp_avg, county_id, disp_avg) 
  disp_avg$disp_avg[is.nan(disp_avg$disp_avg)] <- NA
  
  x <- x %>% left_join(disp_avg, by="county_id")   
  
  # calculate average performance z scores
  perf_avg <- select(x, county_id, grep("perf_z", colnames(x)))                         
  perf_avg$perf_avg <- rowMeans(perf_avg[,-1], na.rm = TRUE)                           
  perf_avg <- select(perf_avg, county_id, perf_avg)                    
  perf_avg$perf_avg[is.nan(perf_avg$perf_avg)] <- NA
  
  x <- x %>% left_join(perf_avg, by="county_id") 
  
  
  # FOR COUNTIES WHERE # OF INDICATOR VALUES >= THRESHOLD, GET INDEX DISP_Z AND PERF_Z: threshold will change across issue areas. Users manually update threshold in index script.
  x <- x %>% mutate(
    performance_z =  ifelse(perf_values_count < ind_threshold, NA, perf_avg),
    disparity_z =    ifelse(disp_values_count < ind_threshold, NA, disp_avg)
    )
    
  # Cap Issue Index z-scores at |2| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% mutate(
          disparity_z = case_when(
            disparity_z > 2 ~ 2,
            disparity_z < -2 ~ -2,
            TRUE ~ disparity_z)
        ) %>%
        mutate (
          performance_z = case_when(
            performance_z > 2 ~ 2,
            performance_z < -2 ~ -2,
            TRUE ~ performance_z)
        )
 
 
  # calculate quadrant
  x <- x %>% mutate(
    quadrant = ifelse(performance_z < 0 & disparity_z > 0, 'red', 
                      ifelse(performance_z >= 0 & disparity_z > 0, 'orange', 
                             ifelse(performance_z  >= 0 & disparity_z <= 0, 'purple', 
                                    ifelse(performance_z < 0 & disparity_z <= 0, 'yellow', NA))))
  )
  
  
  # calculate disparity rank
    x$disparity_rank <-  rank(desc(x$disparity_z), na.last = "keep", ties.method = "min")
  
  # calculate performance rank
    x$performance_rank <-  rank(desc(x$performance_z), na.last = "keep", ties.method = "min")
  
  # calc disp_z and perf_z quartiles
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

# Calculate and cap COMPOSITE INDEX  ---------------
calculate_index_z <- function(x) {
  # count performance z-scores
  rates_performance <- select(x, ends_with("perf_z"))
  rates_performance$perf_values_count <- rowSums(!is.na(rates_performance))
  
  x$perf_values_count <- rates_performance$perf_values_count
  
  # count disparity z-scores
  rates_disparity <- select(x, ends_with("disp_z"))
  rates_disparity$values_count <- rowSums(!is.na(rates_disparity))
  
  x$disp_values_count <- rates_disparity$values_count
  
  # calculate avg disparity z-scores
  disp_avg <- select(x, county_id, grep("disp_z", colnames(x)))
  disp_avg$disp_avg <- rowMeans(disp_avg[,-1], na.rm = TRUE)
  disp_avg <- select(disp_avg, county_id, disp_avg) 
  disp_avg$disp_avg[is.nan(disp_avg$disp_avg)] <- NA
  
  x <- x %>% left_join(disp_avg, by="county_id")   
  
  # calculate average performance z scores
  perf_avg <- select(x, county_id, grep("perf_z", colnames(x)))                         
  perf_avg$perf_avg <- rowMeans(perf_avg[,-1], na.rm = TRUE)                           
  perf_avg <- select(perf_avg, county_id, perf_avg)                    
  perf_avg$perf_avg[is.nan(perf_avg$perf_avg)] <- NA
  
  x <- x %>% left_join(perf_avg, by="county_id") 
  
  
  # FOR COUNTIES WHERE # OF ISSUE INDEX VALUES >= THRESHOLD, GET INDEX DISP_Z AND PERF_Z. Users manually update threshold in index script.
  x <- x %>% mutate(
    performance_z =  ifelse(perf_values_count < ind_threshold, NA, perf_avg),
    disparity_z =    ifelse(disp_values_count < ind_threshold, NA, disp_avg)
  )
  
  # Cap Composite Index z-scores at |1| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% mutate(
    disparity_z = case_when(
      disparity_z > 1 ~ 1,
      disparity_z < -1 ~ -1,
      TRUE ~ disparity_z)
  ) %>%
    mutate (
      performance_z = case_when(
        performance_z > 1 ~ 1,
        performance_z < -1 ~ -1,
        TRUE ~ performance_z)
    )
  
  
  # calculate quadrant
  x <- x %>% mutate(
    quadrant = ifelse(performance_z < 0 & disparity_z > 0, 'red', 
                      ifelse(performance_z >= 0 & disparity_z > 0, 'orange', 
                             ifelse(performance_z >= 0 & disparity_z <= 0, 'purple', 
                                    ifelse(performance_z < 0 & disparity_z <= 0, 'yellow', NA))))
  )
  
  
  # calculate disparity rank
  x$disparity_rank <-  rank(desc(x$disparity_z), na.last = "keep", ties.method = "min")
  
  # calculate performance rank
  x$performance_rank <-  rank(desc(x$performance_z), na.last = "keep", ties.method = "min")
  
  # calc disp_z and perf_z quartiles
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

#####send index table to postgres
index_to_postgres <- function(x, y) {
  
  # create connection for rda database
  source("W:\\RDA Team\\R\\credentials_source.R")
  con <- connect_to_db("racecounts")                    
  
  #INDEX TABLE
  index_table <- as.data.frame(index_table)
  
  # make character vector for field types in postgresql db
  charvect = rep('numeric', dim(index_table)[2])
  
  # change data type for first four columns
  charvect[1:4] <- "varchar" # first two are characters for the geoid and names
  
  # add names to the character vector
  names(charvect) <- colnames(index_table)
  
  dbWriteTable(con, c(rc_schema, index_table_name), index_table,
               overwrite = FALSE, row.names = FALSE)
  
  comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", index_table_name, " IS '", index, " from ", source, ".';
                 COMMENT ON COLUMN ",  rc_schema, ".", index_table_name, ".county_id IS 'County fips';")  
  
  
  print(comment)
  
  dbSendQuery(con, comment)
  dbDisconnect(con)  
  
}


############# CITY INDEX FUNCTIONS ############# ----------------------------------------------------

# Aggregate DISTRICT data to CITIES ---------------------------------------
dist_data_to_city <- function(x) {

    ### weighted averages by enrollment size per indicator (some cities have multiple districts but not every indicator will have multiple districts per city).
    enrollment_percentages <- x %>% select(city_id, dist_id, total_enroll, indicator) %>% unique() %>% group_by(city_id, indicator) %>% 
                                    mutate(sum_total_enroll = sum(total_enroll, na.rm = T), 
                                           percent_total_enroll = (total_enroll/sum_total_enroll)) %>% ungroup() %>% 
                                    select(city_id, dist_id, indicator, total_enroll, sum_total_enroll, percent_total_enroll)
                            
    df_education_district_weighted <- x %>% left_join(enrollment_percentages, by = c("city_id","dist_id","indicator","total_enroll")) %>%
                                      mutate(disparity_z_score = disparity_z_score_unweighted * percent_total_enroll,
                                             performance_z_score = performance_z_score_unweighted * percent_total_enroll)
    
    df_education_city <- df_education_district_weighted %>% group_by(city_id, city_name, indicator) %>% summarize(disp_z = sum(disparity_z_score, na.rm = T), perf_z = sum(performance_z_score, na.rm = T), disp_count = sum(!is.na(disparity_z_score)), perf_count = sum(!is.na(performance_z_score)))    
    df_education_city$disp_z <- ifelse(df_education_city$disp_count == 0, NA, df_education_city$disp_z) # change disp_z back to NA when all disparity_z's are NA
    df_education_city$perf_z <- ifelse(df_education_city$perf_count == 0, NA, df_education_city$perf_z) # change perf_z back to NA when all performance_z's are NA
    
    education_tables_agg <- df_education_city %>% select(-c(disp_count, perf_count)) %>% pivot_wider(
                                                                                                      names_from = indicator,
                                                                                                      values_from = c(disp_z, perf_z),
                                                                                                      names_glue = "{indicator}_{.value}",
                                                                                                    )

return(education_tables_agg)
}

# Cap INDICATOR Z-scores ---------------------------------------------------------------------
clean_city_indicator_data_z <- function(x, y) {
  
  # Cap Indicator z-scores at |3.5| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
  x <- x %>% mutate(across(ends_with("disp_z"), 
                           ~ case_when(. > 3.5 ~ 3.5, 
                                       . < -3.5 ~ -3.5,
                                       TRUE ~ .))) %>% 
    
    mutate(across(ends_with("perf_z"), 
                  ~ case_when(. > 3.5 ~ 3.5, 
                              . < -3.5 ~ -3.5,
                              TRUE ~ .)))
  return(x)
}

# Calculate and cap ISSUE Z-scores ---------------------------------------------------
calculate_city_issue <- function(x, y, z) {
  
  ##  for city issue areas with only 1 indicator
  if (y == 1) {
    x <- x %>% select(city_id, city_name, ends_with("disp_z"), ends_with("perf_z")) %>% mutate(
      disp_avg = x[,3],
      perf_avg = x[,4]
    )    
    
    # Cap Issue Index z-scores at |2| More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
    x <- x %>% mutate(disparity_z = ifelse(x$disp_avg > 2, 2, 
                                      ifelse(x$disp_avg < -2, -2,
                                             x$disp_avg)),
                      performance_z = ifelse(x$perf_avg > 2, 2, 
                                      ifelse(x$perf_avg < -2, -2,
                                             x$perf_avg))					  
    )	  
  } else {  
    
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
    
    ## ISSUE THRESHOLD
    x <- x %>% mutate(
      disparity_z = ifelse(disp_values_count < z, NA, disp_avg),
      performance_z = ifelse(perf_values_count < z, NA, perf_avg),
      
    )
    
    # Cap Issue Index z-scores. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
    x <- x %>% mutate(
      disparity_z = case_when(
        disparity_z > 2 ~ 2,
        disparity_z < -2 ~ -2,
        TRUE ~ disparity_z)
    ) %>%
      mutate (
        performance_z = case_when(
          performance_z > 2 ~ 2,
          performance_z < -2 ~ -2,
          TRUE ~ performance_z)
      )
    
    # select columns with average weighted z-score, then rename to reflect issue area
    x %>% select(city_id, ends_with("count"), disparity_z, performance_z) 
  }
  
  return(x)
}

# Calculate and cap COMPOSITE INDEX Z-scores ---------------------------------------------------
calculate_city_index <- function(x, y, z) {
  
  # count issue area performance z-scores
  rates_performance <- select(x, ends_with("performance_z"))
  rates_performance$perf_values_count <- rowSums(!is.na(rates_performance))
  
  x$issue_perf_count <- rates_performance$perf_values_count
  
  # count issue area disparity z-scores
  rates_disparity <- select(x, ends_with("disparity_z"))
  rates_disparity$values_count <- rowSums(!is.na(rates_disparity))
  
  x$issue_disp_count <- rates_disparity$values_count
  
  # calculate avg disparity z-scores
  disp_avg <- select(x, city_id, grep("disparity_z", colnames(x)))
  disp_avg$disp_avg <- rowMeans(disp_avg[,-1], na.rm = TRUE)
  disp_avg <- select(disp_avg, city_id, disp_avg) 
  disp_avg$disp_avg[is.nan(disp_avg$disp_avg)] <- NA
  
  x <- x %>% left_join(disp_avg, by="city_id")   
  
  # calculate average performance z scores
  perf_avg <- select(x, city_id, grep("performance_z", colnames(x)))                         
  perf_avg$perf_avg <- rowMeans(perf_avg[,-1], na.rm = TRUE)                           
  perf_avg <- select(perf_avg, city_id, perf_avg)                    
  perf_avg$perf_avg[is.nan(perf_avg$perf_avg)] <- NA
  
  x <- x %>% left_join(perf_avg, by="city_id")
  
  # Shorten issue disp/perf z colnames
  colnames(x) = gsub("_disparity_z", "_disp_z", colnames(x))
  colnames(x) = gsub("_performance_z", "_perf_z", colnames(x))
  
  ## SCREEN USING ISSUE AREA and ALL INDICATORS THRESHOLDS. Users manually update threshold in index script.
  x <- x %>% mutate(
    disparity_z =  ifelse((issue_disp_count < y | all_indicators_disp_count < z), NA, disp_avg),
    performance_z = ifelse((issue_perf_count < y | all_indicators_perf_count < z), NA, perf_avg),
  ) %>%
    ## Cap Overall Index Z-score at |1|. More info: https://catalystcalifornia.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=feyI80
    mutate(
      disparity_z = case_when(
        disparity_z > 1 ~ 1,
        disparity_z < -1 ~ -1,
        TRUE ~ disparity_z)
    ) %>%
    mutate(
      performance_z = case_when(
        performance_z > 1 ~ 1,
        performance_z < -1 ~ -1,
        TRUE ~ performance_z))
  
  # calculate quadrant
  x <- x %>% mutate(
    quadrant = ifelse(x$performance_z < 0 & x$disparity_z > 0, 'red', 
                     ifelse(x$performance_z >= 0 & x$disparity_z > 0, 'orange', 
                            ifelse(x$performance_z >= 0 & x$disparity_z <= 0, 'purple', 
                                  ifelse(x$performance_z < 0 & x$disparity_z <= 0, 'yellow', NA)))) 
  )

  # calculate disparity rank
  x$disparity_rank <- dense_rank(-x$disparity_z)

  # calculate performance rank
  x$performance_rank <- dense_rank(-x$performance_z)

  # calc disp_z and perf_z quartiles
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


# Export city index -------------------------------------------------------
city_index_to_postgres <- function(x) {
      table_schema <- rc_schema
      # make character vector for field types in postgresql db
      charvect = rep('numeric', dim(x)[2])
      
      # change data type for columns
      charvect[c(1:2,9,26:28,30:31)] <- "varchar" # Define which cols are character for the geoid and names etc
      
      # add names to the character vector
      names(charvect) <- colnames(x)
      
      # write table to postgres
      dbWriteTable(con, c(table_schema, table_name), x, overwrite = FALSE, row.names = FALSE, field.types = charvect)
      
      # write comment to table, and the first three fields that won't change.
      table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS '", table_comment_source, ".", "';")
      
      ## send table comment to database
      dbSendQuery(conn = con, table_comment)     

}


