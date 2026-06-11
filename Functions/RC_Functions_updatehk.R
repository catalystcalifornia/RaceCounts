# Common RACE COUNTS Functions

# user-defined functions for things we do repeatedly in RC data prep
############ To use the following RC Functions, 'd' will need the following columns at minimum:
############ geoid and total and raced "_rate" and total and raced "_raw" (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.
############ Any pop screens or data reliability screens (like CV) should be complete before you get to RC Functions.
############ Your dataframe must also contain both county and state (eventually city where available) data.
############ Your final table export(s) must have either county_id/county_name, state_id/state_name, or city_id/city_name fields.
############ Your final table export(s) should contain cv values if you used a cv screen, same for _pop columns if you used a pop screen.

options(scipen = 999) # disable scientific notation


#####calculate rates#####
calc_rates_100k <- function(x) {
  pop <- dplyr::select(x, geoid, ends_with("_pop"))  #get geoid, raced rate and best columns
  pop_long <- pivot_longer(pop, 2:ncol(pop), names_to="measure_rate", values_to="pop") #%>%   #pivot wide table to long on geoid & best cols
  pop_long$measure_rate <- sub("pop", "_", pop_long$measure_rate)
  raw <- dplyr::select(x, geoid, ends_with("_raw"))  #get geoid, raced rate and best columns
  raw_long <- pivot_longer(raw, 2:ncol(raw), names_to="measure_rate", values_to="raw") #%>%   #pivot wide table to long on geoid & best cols
  raw_long$measure_rate <- sub("raw", "_", raw_long$measure_rate)
  calc_long <- pop_long %>% left_join(raw_long, by=c("geoid", "measure_rate"))
  calc_long <- calc_long %>%
    dplyr::mutate(rate=raw/pop * 100000) %>%                                             #calc rates
    dplyr::mutate(measure_rate=sub("__", "_rate", measure_rate))                         #create new column names for diffs from best
  calc_wide <- calc_long %>% dplyr::select(geoid, measure_rate, rate) %>%      #pivot long table back to wide
    pivot_wider(names_from=measure_rate, values_from=rate)
  x <- x %>% left_join(calc_wide, by="geoid")                           #join new diff from best columns back to original table
  
  return(x)
}


#####calculate number of non-NA raced "_rate" values#####
count_values <- function(x) {
  rates <- x %>%
    dplyr::select(-ends_with("_no_rate"), -total_rate) %>%
    dplyr::mutate(values_count = rowSums(!is.na(select(., ends_with("_rate"))))) 
  
  x <- x %>%
    left_join(rates)
  
  return(x)
}


#####calculate best rate#####
calc_best <- function(x) {
  # disable scientific notation
  options(scipen = 999) 
  
  rates <- x %>%
    dplyr::select(-c(ends_with("_no_rate"), total_rate)) %>%
    
    # Prep: Replace 0 rates with NA 
    # Excludes superficially low rates where asbest is "min"
    dplyr::mutate(across(ends_with("_rate"), ~if_else(. == 0, NA, .))) %>%
    
    # Calculation
    rowwise() %>%
    dplyr::mutate(best = case_when(
      asbest == "max" ~ max(c_across(ends_with("_rate")), na.rm = TRUE),
      asbest == "min" ~ min(c_across(ends_with("_rate")), na.rm = TRUE),
      .default = NA
    )) %>%
    ungroup() %>%
    
    # Clean: If all rates in a row are NA, max()/min() will return "Inf" values
    # Replace Inf with NA 
    dplyr::mutate(best = if_else(is.infinite(best), NA, best)) 
  
  x <- x %>% 
    left_join(rates)
  
  return(x)
}


#####calculate difference from best#####
calc_diff <- function(x) {
  # Prep
  rates <- x %>%
    dplyr::select(-c(starts_with("total_"), -ends_with("_no_rate"))) %>%
    # remove duplicates from overcrowding
    unique()
  
  # Calculation: pivot to long on _rate cols to simplify diff calc 
  # then pivot back to wide to join to original dataframe
  diff <- rates %>%
    pivot_longer(ends_with("_rate"), names_to="measure_rate", values_to="rate") %>%   
    dplyr::mutate(diff=abs(best-rate)) %>%
    dplyr::mutate(measure_diff=sub("_rate", "_diff", measure_rate)) %>%
    select(-c(measure_rate, rate)) %>%
    pivot_wider(names_from=measure_diff, values_from=diff)
  
  # join _diff columns back to original table
  x <- x %>% 
    left_join(diff)                     
  
  return(x)
}


#####calculate (row wise) mean difference from best#####
calc_avg_diff <- function(x) {
  diffs <- x %>%
    # Prep: only run calc when 2 or more (non-NA) raced diff values are present
    filter(values_count > 1) %>%
    
    # Calculation: average absolute difference from best 
    dplyr::mutate(avg = rowMeans(across(ends_with("_diff")), na.rm = TRUE)) 
  
  # join avg diff column back to original table
  x <- x %>% 
    left_join(diffs)  
  
  return(x)
}


#####calculate (row wise) SAMPLE variance of differences from best - use for sample data like ACS or CHIS#####
calc_s_var <- function(x) {
  diffs <- x %>%
    # Prep
    filter(values_count > 1) %>%
    dplyr::select(geoid, geolevel, ends_with("_diff")) %>%
    
    # Calculation 
    dplyr::mutate(variance = apply(dplyr::select(., ends_with("_diff")), 1, var, na.rm = TRUE)) %>%
    dplyr::select(geoid, geolevel, variance)
  
  # Join variance column back to original table
  x <- x %>% 
    left_join(diffs, by=c("geoid","geolevel"))    
  
  return(x)
}


#####calculate (row wise) POPULATION variance of differences from best - use for non-sample data like Decennial Census, CDPH Births, or CADOJ Incarceration#####
calc_p_var <- function(x) {
  diffs <- x %>% 
    # Prep
    filter(values_count > 1) %>%
    
    # Calculation
    dplyr::mutate(svar = apply(dplyr::select(., ends_with("_diff")), 1, var, na.rm = TRUE)) %>%   
    
    # Convert sample variance to population variance. Checked that the svar and variance results match VARS.S/VARS.P Excel results.
    #See more: https://stackoverflow.com/questions/37733239/population-variance-in-r
    dplyr::mutate(variance = svar * (values_count - 1) / values_count) %>%
    dplyr::select(-svar)                               
  
  # Join variance column back to original table
  x <- x %>% 
    left_join(diffs)    
  
  return(x)
}


#####calculate index of disparity#####
calc_id <- function(x) {
  diffs <- x %>%
    filter(values_count>0) %>%
    
    # Calculation 1: sum of difference from best (needed for ID calc)
    dplyr::mutate(sumdiff = rowSums(select(., ends_with("_diff")), na.rm=TRUE)) %>%
    
    # Calculation 2: Index of Disparity (ID)
    # Returns NA when (there are <2 raced values) OR (there are 2 raced values AND MIN is best AND the sum of diffs = best)
    # Example: The second condition is where MIN is best, a geo has only 2 rates and one of them is 0.
    dplyr::mutate(index_of_disparity = ifelse((values_count < 2) | 
                                                (values_count == 2 & asbest == 'min' & sumdiff == best),
                                              NA, 
                                              (((sumdiff / best) / (values_count - 1)) * 100))) %>%
    dplyr::select(-sumdiff)
  
  # Join ID column back to original df
  x <- x %>%
    left_join(diffs) 
  
  # Can add here? calc 'times findings', eg: The Latinx rate is X times the White rate.
  
  return(x)
}


#####calculate state z-scores#####
calc_state_z <- function(x) {
  ## Raced disparity z-scores ##
  diff <- x %>%
    # exclude rows with 2+ raced values, min is best, and lowest rate is 0 (i.e., where index_of_disparity is NOT NA)
    filter(!is.na(index_of_disparity))
  
  diff_long <- diff %>%
    pivot_longer(ends_with("_diff"), names_to="measure_rate", values_to="rate") %>%
    # calc z scores
    dplyr::mutate(diff=(rate - avg) / sqrt(variance),
                  measure_diff=sub("_diff", "_disparity_z", measure_rate)) %>%
    select(-c(measure_rate, rate))
  
  # create new column names for disparity z-scores
  diff_wide <- diff_long %>% 
    pivot_wider(names_from=measure_diff, values_from=diff)
  
  # join new columns back to original table
  x <- x %>% left_join(diff_wide)                           
  
  return(x)
}


#####calculate county disparity and performance z-scores ----
calc_z <- function(x) {
  ## Total/Overall disparity_z score ##
  id_table <- x
  avg_id = mean(id_table$index_of_disparity, na.rm = TRUE) #calc avg id and std dev of id
  sd_id = sd((id_table$index_of_disparity), na.rm = TRUE)
  
  id_table <- id_table %>%
    #note the disp_z results are slightly different than pgadmin, must be due to slight methodology differences
    mutate(disparity_z=(index_of_disparity - avg_id) / sd_id) 
  
  ## Raced disparity_z scores ##
  diff <- id_table %>%
    # exclude rows with 2+ raced values, min is best, and lowest rate is 0 (i.e., where index_of_disparity is NOT NA)
    filter(!is.na(index_of_disparity))
  
  diff_long <- diff %>%
    pivot_longer(ends_with("_diff"), names_to="measure_diff", values_to="diff") %>%  
    dplyr::mutate(
      #calc disparity z-scores
      dispz=(diff - avg) / sqrt(variance),
      measure_diff=sub("_diff", "_disparity_z", measure_diff)) %>%
    select(-c(diff))
  
  diff_wide <- diff_long %>%
    pivot_wider(names_from=measure_diff, values_from=dispz)
  
  #join new columns back to original table
  x <- x %>% left_join(diff_wide)                           
  
  ## Total/Overall performance z_scores 
  ## Note the perf_z results are slightly different than pgadmin, must be due to slight methodology differences
  tot_table <- x
  
  #calc avg total_rate and std dev of total_rate
  avg_tot = mean(tot_table$total_rate, na.rm = TRUE)      
  sd_tot = sd(tot_table$total_rate, na.rm = TRUE)
  
  tot_table <- tot_table %>%
    mutate(performance_z = case_when(
      min(asbest) == 'max'~ (total_rate - avg_tot) / sd_tot,
      min(asbest) == 'min'~ ((total_rate - avg_tot) / sd_tot) *-1
    ))
  
  ## Raced performance z_scores ##
  rates <- tot_table %>%
    dplyr::select(-c(ends_with("_no_rate"), ends_with("_moe_rate"), total_rate))
  
  rates_long <- rates %>%
    # Pivot to long format first
    pivot_longer(cols = ends_with("_rate"), names_to = "measure_rate", values_to = "rate") %>%
    # Group by measure_rate to calculate across each race group
    group_by(measure_rate) %>%
    # Calc average and standard deviation for each raced rate
    mutate(avg_rates = mean(rate, na.rm = TRUE),
           sd_rates = sd(rate, na.rm = TRUE)) %>%
    # Ungroup to return to regular data frame
    ungroup() %>%
    mutate(
      perf=case_when(
        min(asbest) == 'max'~ (rate - avg_rates) / sd_rates,
        min(asbest) == 'min'~ ((rate - avg_rates) / sd_rates) *-1),
      measure_perf=sub("_rate", "_performance_z", measure_rate)) %>%
    select(-c(measure_rate, rate, avg_rates, sd_rates))
  
  rates_wide <- rates_long %>%
    pivot_wider(names_from=measure_perf, values_from=perf)
  
  #join new columns back to original table
  x <- x %>% left_join(rates_wide)                                
  
  return(x)
}


#####calculate disparity and performance ranks, quadrants, disparity and performance quartile labels#####
calc_ranks <- function(x) {
  ranks_table <- x %>%
    mutate(
      #performance_rank: rank of 1 = best performance
      #if max is best then rank DESC / if min is best, then rank ASC. exclude NULLS.
      performance_rank = case_when(
        min(asbest) == 'max'~ rank(desc(total_rate), na.last = "keep", ties.method = "min"),
        min(asbest) == 'min'~ rank(total_rate, na.last = "keep", ties.method = "min")),
      
      #disparity_rank: rank of 1 = worst disparity
      #max is worst, rank DESC. exclude NULLS.
      disparity_rank = rank(desc(index_of_disparity), na.last = "keep", ties.method = "min"),
      
      # quadrants (updated 2023)
      #if perf_z below avg and disp_z above avg, then red / perf_z above or avg and disp_z above or avg, then orange /
      #perf_z above or avg and disp_z below avg, then purple / perf_z below avg and disp_z below or avg, then yellow
      quadrant = case_when(
        performance_z < 0 & disparity_z > 0 ~ 'red',
        performance_z >= 0 & disparity_z > 0 ~ 'orange',
        performance_z >= 0 & disparity_z <= 0 ~ 'purple',
        performance_z < 0 & disparity_z <= 0 ~ 'yellow',
        .default=NA))
  
  x <- x %>% left_join(ranks_table)
  
  # calculate disparity and performance quartiles from z scores
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



#####send city, county, state and leg district tables to postgres#####
to_postgres <- function(x,y) {
  # create connection for rda database
  source("W:\\RDA Team\\R\\credentials_source.R")
  con <- connect_to_db("racecounts")
  
  #STATE TABLE
  state_table <- as.data.frame(state_table)
  
  # make character vector for field types in postgresql db
  charvect = rep('numeric', dim(state_table)[2])
  
  # change data type for first two columns
  charvect[1:2] <- "varchar" # first two cols are characters for the geoid and names
  
  # add names to the character vector
  names(charvect) <- colnames(state_table)
  
  dbWriteTable(con,
               Id(schema = rc_schema, table = state_table_name),
               state_table, overwrite = FALSE)
  
  # Start a transaction
  dbBegin(con)
  
  #comment on table and columns
  comment <- paste0("COMMENT ON TABLE ", "\"", rc_schema, "\"", ".", "\"", state_table_name, "\"", " IS 'Table created on ", Sys.Date(), ". ", indicator, " from ", source, ".';")
  print(comment)
  dbExecute(con, comment)
  
  col_comment <- paste0("COMMENT ON COLUMN ", "\"", rc_schema, "\"", ".", "\"", state_table_name, "\"", ".state_id IS 'State fips';")
  print(col_comment)                  
  dbExecute(con, col_comment)
  
  # Commit the transaction if everything succeeded
  dbCommit(con)
  
  #COUNTY TABLE
  county_table <- as.data.frame(county_table)
  
  # make character vector for field types in postgresql db
  charvect = rep('numeric', dim(county_table)[2])
  
  # change data type for first two columns
  charvect[1:2] <- "varchar" # first two are characters for the geoid and names
  
  # add names to the character vector
  names(charvect) <- colnames(county_table)
  
  dbWriteTable(con,
               Id(schema = rc_schema, table = county_table_name),
               county_table, overwrite = FALSE)
  
  # Start a transaction
  dbBegin(con)
  
  #comment on table and columns
  comment <- paste0("COMMENT ON TABLE ", "\"", rc_schema, "\"", ".", "\"", county_table_name, "\"", " IS 'Table created on ", Sys.Date(), ". ", indicator, " from ", source, ".';")
  print(comment)
  dbExecute(con, comment)
  
  col_comment <- paste0("COMMENT ON COLUMN ", "\"", rc_schema, "\"", ".", "\"", county_table_name, "\"", ".county_id IS 'County fips';")
  print(col_comment)
  dbExecute(con, col_comment)
  
  # Commit the transaction if everything succeeded
  dbCommit(con)
  return("Table and columns comments added to table!")
  
  dbDisconnect(con)
  
  return(x)
}

city_to_postgres <- function(x) {
  
  # create connection for rda database
  source("W:\\RDA Team\\R\\credentials_source.R")
  con <- connect_to_db("racecounts")
  
  #CITY TABLE
  city_table <- as.data.frame(city_table)
  
  # make character vector for field types in postgresql db
  charvect = rep('numeric', dim(city_table)[2])
  
  # change data type for first two columns
  charvect[1:2] <- "varchar" # first two cols are characters for the geoid and names
  
  # add names to the character vector
  names(charvect) <- colnames(city_table)
  
  dbWriteTable(con,
               Id(schema = rc_schema, table = city_table_name),
               city_table, overwrite = FALSE)
  
  # Start a transaction
  dbBegin(con)
  
  #comment on table and columns
  comment <- paste0("COMMENT ON TABLE ", "\"", rc_schema, "\"", ".", "\"", city_table_name, "\"", " IS 'Table created on ", Sys.Date(), ". ", indicator, " from ", source, ".';")
  print(comment)
  
  dbExecute(con, comment)
  
  # Commit the transaction if everything succeeded
  dbCommit(con)
  return("Table and columns comments added to table!")
  
  dbDisconnect(con)
  
  return(x)
}

leg_to_postgres <- function(x) {
  # create connection for rda database
  source("W:\\RDA Team\\R\\credentials_source.R")
  con <- connect_to_db("racecounts")
  
  #STATE TABLE
  leg_table <- as.data.frame(leg_table)
  
  # make character vector for field types in postgresql db
  charvect = rep('numeric', dim(leg_table)[2])
  
  # change data type for first two columns
  charvect[1:3] <- "varchar" # first two cols are characters for the geoid and names
  
  # add names to the character vector
  names(charvect) <- colnames(leg_table)
  
  dbWriteTable(con,
               Id(schema = rc_schema, table = leg_table_name),
               leg_table, overwrite = FALSE)
  
  # Start a transaction
  dbBegin(con)
  
  #comment on table and columns
  comment <- paste0("COMMENT ON TABLE ", "\"", rc_schema, "\"", ".", "\"", leg_table_name, "\"", " IS 'Table created on ", Sys.Date(), ". ", indicator, " from ", source, ".';")
  print(comment)
  dbExecute(con, comment)
  
  col_comment <- paste0("COMMENT ON COLUMN ", "\"", rc_schema, "\"", ".", "\"", leg_table_name, "\"", ".leg_id IS 'Legislative District fips - note Assm and Sen fips are NOT unique. You must use combination of leg_id and geolevel to identify';")
  print(col_comment)
  dbExecute(con, col_comment)
  
  # Commit the transaction if everything succeeded
  dbCommit(con)
  return("Table and columns comments added to table!")
  
  dbDisconnect(con)
  
  return(x)
}
