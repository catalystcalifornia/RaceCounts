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
                                 mutate(rate=raw/pop * 100000) %>%                                             #calc rates
                                 mutate(measure_rate=sub("__", "_rate", measure_rate))                         #create new column names for diffs from best
                    calc_wide <- calc_long %>% dplyr::select(geoid, measure_rate, rate) %>%      #pivot long table back to wide
                                 pivot_wider(names_from=measure_rate, values_from=rate)
                    x <- x %>% left_join(calc_wide, by="geoid")                           #join new diff from best columns back to original table

return(x)
}


#####calculate number of non-NA raced "_rate" values#####
count_values <- function(x) {
  rates <- dplyr::select(x, geoid, geolevel, ends_with("_rate"), -ends_with("_no_rate"), -total_rate) %>%
    mutate(values_count = rowSums(!is.na(select(., ends_with("_rate"))))) %>%
    dplyr::select(geoid, geolevel, values_count)
  
  x <- x %>%
    left_join(rates, by=c("geoid","geolevel"))
  
  return(x)
}


#####calculate best rate#####
calc_best <- function(x) {
  # disable scientific notation
  options(scipen = 999) 
  
  rates <- x %>%
    dplyr::select(geoid, geolevel, asbest, ends_with("_rate"), 
                  -ends_with("_no_rate"), -total_rate) %>%
    
    # Prep: Replace 0 rates with NA 
    # Excludes superficially low rates where asbest is "min"
    mutate(across(ends_with("_rate"), ~if_else(. == 0, NA, .))) %>%
    
    # Calculation
    rowwise() %>%
    mutate(best = case_when(
      asbest == "max" ~ max(c_across(ends_with("_rate")), na.rm = TRUE),
      asbest == "min" ~ min(c_across(ends_with("_rate")), na.rm = TRUE),
      .default = NA
    )) %>%
    ungroup() %>%
    
    # Clean: If all rates in a row are NA, max()/min() will return "Inf" values
    # Replace Inf with NA 
    mutate(best = if_else(is.infinite(best), NA, best)) %>%
    dplyr::select(geoid, geolevel, best)
  
  x <- x %>% 
    left_join(rates, by = c("geoid", "geolevel"))
  
  return(x)
}


#####calculate difference from best#####
calc_diff <- function(x) {
  # get geoid, raced rate and best columns
  rates <- dplyr::select(x, geoid, geolevel, best, ends_with("_rate"), -starts_with("total_"), -ends_with("_no_rate"))  
  # 2/21/23 added due to prior step resulting in dupes for overcrowding
  rates <- unique(rates) 
  
  # pivot wide table to long on geoid & best cols
  diff_long <- pivot_longer(rates, 4:ncol(rates), names_to="measure_rate", values_to="rate") %>%   
    # calc diff from best
    mutate(diff=abs(best-rate)) %>%
    #create new column names for diffs from best
    mutate(measure_diff=sub("_rate", "_diff", measure_rate))      
  
  # pivot long table back to wide
  diff_wide <- diff_long %>% dplyr::select(geoid, geolevel, measure_diff, diff) %>%     
    pivot_wider(names_from=measure_diff, values_from=diff)
  
  # join new diff from best columns back to original table
  x <- x %>% left_join(diff_wide, by=c("geoid","geolevel"))                     
  
  return(x)
}


#####calculate (row wise) mean difference from best#####
calc_avg_diff <- function(x) {
                diffs <- dplyr::select(x, geoid, geolevel, values_count, ends_with("_diff"))
                counts <- diffs %>% 
                  filter(values_count > 1) %>% 
                  mutate(avg = rowMeans(across(ends_with("_diff")), na.rm = TRUE)) %>%
                  #remove values_count, _diff cols before join
                  dplyr::select(-c(values_count, ends_with("_diff")))   
                #join new avg diff column back to original table
                x <- x %>% left_join(counts, by=c("geoid","geolevel"))                               

return(x)
}


#####calculate (row wise) SAMPLE variance of differences from best - use for sample data like ACS or CHIS#####
calc_s_var <- function(x) {
                suppressWarnings(rm(var))   #removes object 'var' if had been previously defined
                diffs <- dplyr::select(x, geoid, geolevel, values_count, grep("_diff", colnames(x)))
                counts <- diffs %>% filter(values_count > 1) %>% dplyr::select(-c(values_count))       #filter for counts >1
                counts$variance <- apply(counts[,-(1:2)], 1, var, na.rm = T)                        #calc sample variance
                counts <- dplyr::select(counts, -grep("_diff", colnames(counts)))                      #remove _diff cols before join
                x <- x %>% left_join(counts, by=c("geoid","geolevel"))                                 #join new variance column back to original table

return(x)
}


#####calculate (row wise) POPULATION variance of differences from best - use for non-sample data like Decennial Census, CDPH Births, or CADOJ Incarceration#####
calc_p_var <- function(x) {
                suppressWarnings(rm(var))   #removes object 'var' if had been previously defined
                diffs <- dplyr::select(x, geoid, geolevel, values_count, ends_with("_diff"))
                counts <- diffs %>% filter(values_count > 1) #%>% dplyr::select(-c(values_count))       #filter for counts >1
                counts$svar <- apply(counts[,4:ncol(counts)], 1, var, na.rm = T)                        #calc population variance
                
                #convert sample variance to population variance. checked that the svar and variance results match VARS.S/VARS.P Excel results.
                #See more: https://stackoverflow.com/questions/37733239/population-variance-in-r
                counts$variance <- counts$svar * (counts$values_count - 1) / counts$values_count
                counts <- dplyr::select(counts, geoid, geolevel, variance)                               #remove extra cols before join
                x <- x %>% left_join(counts, by=c("geoid","geolevel"))                                   #join new variance column back to original table

return(x)
}


#####calculate index of disparity#####
calc_id <- function(x) {
                diffs <- dplyr::select(x, geoid, best, asbest, values_count, ends_with("_diff"))
                diffs$sumdiff <- ifelse(diffs$values_count==0, NA, rowSums(diffs[,-c(1:4)], na.rm=TRUE))    #calc sum of diff from best
                #ID calc returns NA when there are <2 raced values OR where there are 2 raced values, MIN is best, and the sum of diffs = best.
                #The second condition is where MIN is best, a geo has only 2 rates and one of them is 0.
                diffs$index_of_disparity <- ifelse(diffs$values_count < 2 | diffs$values_count == 2 & diffs$asbest == 'min' & diffs$sumdiff == diffs$best, NA, (((diffs$sumdiff / diffs$best) / (diffs$values_count - 1)) * 100))
                x$index_of_disparity <- diffs$index_of_disparity

                # Can add here? calc 'times findings', eg: The Latinx rate is X times the White rate.
                
return(x)
}


#####calculate state z-scores#####
calc_state_z <- function(x) {
        ## Raced disparity z-scores ##
                diff <- dplyr::select(x, geoid, avg, index_of_disparity, variance, ends_with("_diff"))          #get geoid, avg, variance, and raced diff columns
                diff <- diff[!is.na(diff$index_of_disparity),]                                           #exclude rows with 2+ raced values, min is best, and lowest rate is 0
                diff_long <- pivot_longer(diff, 5:ncol(diff), names_to="measure_rate", values_to="rate") %>%   #pivot wide table to long on geoid & variance cols
                  mutate(diff=(rate - avg) / sqrt(variance)) %>%                                               #calc disparity z-scores
                  mutate(measure_diff=sub("_diff", "_disparity_z", measure_rate))                              #create new column names for disparity z-scores
                diff_wide <- diff_long %>% dplyr::select(geoid, measure_diff, diff) %>%      #pivot long table back to wide keeping only geoid and new columns
                  pivot_wider(names_from=measure_diff, values_from=diff)
                x <- x %>% left_join(diff_wide, by="geoid")                           #join new columns back to original table

return(x)
}


#####calculate county disparity z-scores ----
calc_z <- function(x) {
        ## Total/Overall disparity_z score ##
                id_table <- dplyr::select(x, geoid, index_of_disparity)
                avg_id = mean(id_table$index_of_disparity, na.rm = TRUE) #calc avg id and std dev of id
                sd_id = sd((id_table$index_of_disparity), na.rm = TRUE)
                #mutate(sd_id = sd(unlist(id_table$index_of_disparity)))                    #calc avg id and std dev of id with unlist()
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
                tot_table <- dplyr::select(x, geoid, asbest, total_rate)
                avg_tot = mean(tot_table$total_rate, na.rm = TRUE)      #calc avg total_rate and std dev of total_rate
                sd_tot = sd(tot_table$total_rate, na.rm = TRUE)
                if (min(tot_table$asbest) == 'max') {
                            tot_table$performance_z <- (tot_table$total_rate - avg_tot) / sd_tot          #calc perf_z scores if MAX is best
                } else
                if (min(tot_table$asbest) == 'min') {
                            tot_table$performance_z <-  ((tot_table$total_rate - avg_tot) / sd_tot) *-1   #calc perf_z scores if MIN is best
                }
                x$performance_z = tot_table$performance_z    #add performance_z to original table

        ## Raced performance z_scores ##
                rates <- dplyr::select(x, geoid, asbest, ends_with("_rate"), -ends_with("_no_rate"), -ends_with("_moe_rate"), -total_rate)  #get geoid, avg, variance, and raced diff columns
                avg_rates <- colMeans(rates[,3:ncol(rates)], na.rm = TRUE)                                        #calc average rates for each raced rate
                a <- as.data.frame(avg_rates)                                                                     #convert to data frame
                a$measure_rate  <- c(names(avg_rates))                                                            #create join field
                sd_rates <- sapply(rates[,3:ncol(rates)], sd, na.rm = TRUE)                                       #calc std dev for each raced rate
                s <- as.data.frame(sd_rates)                                                                      #convert to data frame
                s$measure_rate  <- c(names(sd_rates))                                                             #create join field
                rates_long <- pivot_longer(rates, 3:ncol(rates), names_to="measure_rate", values_to="rate")           #pivot wide table to long on geoid & variance cols
                rates_long <- left_join(rates_long, a, by="measure_rate")                                             #join avg rates for each raced rate
                rates_long <- left_join(rates_long, s, by="measure_rate")                                             #join std dev for each raced rate

                if (min(rates$asbest) == 'max') {
                rates_long <- rates_long %>% mutate(perf=(rate - avg_rates) / sd_rates, na.rm = TRUE) %>%         #calc perf_z scores if MAX is best
                              mutate(measure_perf=sub("_rate", "_performance_z", measure_rate))                   #create new column names for performance z-scores
                } else
                if (min(rates$asbest) == 'min') {
                rates_long <- rates_long %>% mutate(perf=((rate - avg_rates) / sd_rates) *-1, na.rm = TRUE) %>%   #calc perf_z scores if MIN is best
                              mutate(measure_perf=sub("_rate", "_performance_z", measure_rate))                   #create new column names for performance z-scores
                }

                rates_wide <- rates_long %>% dplyr::select(geoid, measure_perf, perf) %>%          #pivot long table back to wide keeping only geoid and new columns
                              pivot_wider(names_from=measure_perf, values_from=perf)

                x <- x %>% left_join(rates_wide, by = "geoid")                                #join new columns back to original table

return(x)
}


#####calculate disparity and performance ranks, quadrants, disparity and performance quartile labels#####
calc_ranks <- function(x) {
  ranks_table <- dplyr::select(x, geoid, asbest, total_rate, index_of_disparity, disparity_z, performance_z)
  
  #performance_rank: rank of 1 = best performance
  #if max is best then rank DESC / if min is best, then rank ASC. exclude NULLS.
  if(min(ranks_table$asbest) == 'max'){
    ranks_table$performance_rank = rank(desc(ranks_table$total_rate), na.last = "keep", ties.method = "min")
  } else
    if(min(ranks_table$asbest) == 'min'){
      ranks_table$performance_rank = rank(ranks_table$total_rate, na.last = "keep", ties.method = "min")
    }
  
  #disparity_rank: rank of 1 = worst disparity
  #max is worst, rank DESC. exclude NULLS.
  ranks_table$disparity_rank = rank(desc(ranks_table$index_of_disparity), na.last = "keep", ties.method = "min")
  
  #quadrants (updated 2023)
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

                      dbWriteTable(con, c(rc_schema, state_table_name), state_table,
                                   overwrite = FALSE, row.names = FALSE)

                      #comment on table and columns
                      comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", state_table_name,  " IS '", indicator, " from ", source, ".';
                                                                        COMMENT ON COLUMN ", rc_schema, ".", state_table_name, ".state_id IS 'State fips';")
                      print(comment)
                      dbSendQuery(con, comment)

                      #COUNTY TABLE
                      county_table <- as.data.frame(county_table)

                      # make character vector for field types in postgresql db
                      charvect = rep('numeric', dim(county_table)[2])

                      # change data type for first two columns
                      charvect[1:2] <- "varchar" # first two are characters for the geoid and names

                      # add names to the character vector
                      names(charvect) <- colnames(county_table)

                      dbWriteTable(con, c(rc_schema, county_table_name), county_table,
                                   overwrite = FALSE, row.names = FALSE)

                      #comment on table and columns
                      comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", county_table_name,  " IS '", indicator, " from ", source, ".';
                                         COMMENT ON COLUMN ", rc_schema, ".", county_table_name, ".county_id IS 'County fips';")
                      print(comment)
                      dbSendQuery(con, comment)

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

  dbWriteTable(con, c(rc_schema, city_table_name), city_table,
               overwrite = FALSE, row.names = FALSE)

  #comment on table and columns
  comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", city_table_name,  " IS '", indicator, " from ", source, ".';")
  print(comment)
  dbSendQuery(con, comment)
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
  
  dbWriteTable(con, c(rc_schema, leg_table_name), leg_table,
               overwrite = FALSE, row.names = FALSE)
  
  #comment on table and columns
  comment <- paste0("COMMENT ON TABLE ", rc_schema, ".", leg_table_name,  " IS '", indicator, " from ", source, ".';
                                                                        COMMENT ON COLUMN ", rc_schema, ".", leg_table_name, ".leg_id IS 'Legislative District fips - note Assm and Sen fips are NOT unique. You must use combination of leg_id and geolevel to identify';")
  print(comment)
  dbSendQuery(con, comment)
  
  dbDisconnect(con)
  
  return(x)
}
