##### Reclassify Asian and NHPI Ancestries ########  Note: Ancestry lists sourced at top of indicator script

##### Reclassify Asian and NHPI RAC3P ########
anhpi_reclass <- function(x) {
  # x = df with PUMS data, eg: anhpi_pop
  # separate asian and nhpi data
  asian_pop <- x %>% filter(RACASN == 1)    # Asian alone or AOIC
  nhpi_pop <- x %>% filter(RACNH == 1) %>%  # Nat Haw alone or AOIC
    rbind(x %>% filter(RACPI == 1))         # Pac Isl alone or AOIC
  
  # get unique asian and nhpi subgroup codes/descr.
  asian_subgroups <- unique(asian_pop[c("RAC3P", "anhpi_subgroup")])
  nhpi_subgroups <- unique(nhpi_pop[c("RAC3P", "anhpi_subgroup")])
  print("Displaying all the Asian or NHPI subgroups actually present in the data as asian_subgroups and nhpi_subgroups dfs.")
  View(asian_subgroups)
  View(nhpi_subgroups)
  
  # code subgroups - Pulled Asian/NHPI subgroups from RAC2P
  subgroups <- c(# Asian
                 "Chinese", "Hmong", "Japanese", "Korean", "Mongolian", "Taiwanese", "Burmese", "Cambodian", "Filipino", "Indonesian", "Laotian", 
                 "Malaysian", "Mien", "Thai", "Vietnamese", "Asian Indian", "Bangladeshi", "Bhutanese", "Nepalese", "Pakistani", "Sikh", "Sri Lankan",
                 "Kazakh", "Uzbek", "Other Asian",
                 # NHPI
                 "Native Hawaiian", "Samoan", "Tongan", "Chamorro", "Chuukese", "Guamanian", "Marshallese", "Fijian", "Other Pacific Islander")
  
  for (sg in subgroups) {
    x[[tolower(gsub(" ", "_", sg))]] <- ifelse(
      grepl(sg, x$anhpi_subgroup),
      1, 0
    )
  }
  
 return(x)
}




anhpi_reclass_v1 <- function(x, acs_yr, ancestry_list) {  # used in MOSAIC Living Wage test
  ## import PUMS codes
  url <- paste0("https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_", start_yr, "-", curr_yr, ".csv")
  pums_vars_ <- read.csv(url, header=FALSE, na = "NA")   # read in data dictionary without headers bc some cols do not have names
  head(pums_vars_)
  anc_codes <- pums_vars_ %>% mutate(V6 = ifelse(V6=="", "col_6", V6), V7 = ifelse(V7=="", "val_label", V7))   # manually add names of cols w missing names
  colnames(anc_codes) <- as.character(unlist(anc_codes[1,]))												       # set first row values as col names
  anc_codes <- anc_codes %>% rename(var_code = 'Record Type')
  anc_codes <- anc_codes %>% filter(RT == 'ANC1P' | RT == 'ANC2P')											   # filter RT col for ancestry rows
  anc_codes <- anc_codes %>% select(var_code, val_label) %>% unique()										   # keep only code and label cols, remove dupes
  head(anc_codes)
  print("Added missing column names to anc_codes. Check console to ensure colnames are correct.")
  
  #View(anc_codes)
  
  ## filter PUMS codes for descriptions based on specified ancestry_list
  all_anc_codes <- anc_codes %>% filter(val_label %in% ancestry_list$anc) %>% arrange(val_label) %>% mutate(val_label = gsub(" ", "_", tolower(val_label)))
  print("Displaying ancestry codes filtered by specified ancestry_list as all_anc_codes df.")
  View(all_anc_codes)
  
  aapi_incl1 <- x %>% select(ANC1P) %>% inner_join(all_anc_codes, by =c("ANC1P" = "var_code")) %>% unique() %>% rename(anc = ANC1P) # get list of AA or PI ancestries actually in CA PUMS data
  aapi_incl2 <- x %>% select(ANC2P) %>% inner_join(all_anc_codes, by =c("ANC2P" = "var_code")) %>% unique() %>% rename(anc = ANC2P) # get list of AA or PI ancestries actually in CA PUMS data
  aapi_incl <- rbind(aapi_incl1, aapi_incl2) %>% unique() %>% arrange(val_label)
  print("Displaying all the AA or PI ancestries actually present in the data as aapi_incl df.")
  View(aapi_incl)
  
  
  # code subgroups
  x$var_code = as.factor(ifelse(x$ANC1P == all_anc_codes$var_code[1], all_anc_codes$var_code[1],
                                  NA))
  
  x <- x %>% right_join(all_anc_codes)
  
  return(x)
}







pums_pop_srvy_denom <- function(d, weight, repwlist){
  # d = pums dataframe, e.g. ppl_state
  # weight and repwlist are defined at top of script
  anhpi_svry <- d %>%      
    as_survey_rep(
      variables = c(geoid, geoname, RACASN, RACNHPI, indian, chinese, filipino, japanese, korean, vietnamese, oth_asian, nat_hawaiian, chamorro, samoan, oth_nhpi),   # dplyr::select grouping variables
      weights = weight,                       # person weight
      repweights = repwlist,                  # list of replicate weights
      combined_weights = TRUE,                # tells the function that replicate weights are included in the data
      mse = TRUE,                             # tells the function to calc mse
      type="other",                           # statistical method
      scale=4/80,                             # scaling set by ACS
      rscale=rep(1,80)                        # setting specific to ACS-scaling
    )
  
  # pre-calc pop denominators to speed up subgroup calc fx
  den_asian <- anhpi_svry %>%
    filter(RACASN == 1) %>%
    group_by(geoid, geoname) %>%
    summarise(pop = survey_total(na.rm = TRUE), .groups = "drop")
  
  den_nhpi <- anhpi_svry %>%
    filter(RACNHPI == 1) %>%
    group_by(geoid, geoname) %>%
    summarise(pop = survey_total(na.rm = TRUE), .groups = "drop")
  
  den_total <- anhpi_svry %>%
    group_by(geoid, geoname) %>%
    summarise(pop = survey_total(na.rm = TRUE), .groups = "drop")
  
return(list(anhpi_svry = anhpi_svry, den_asian = den_asian, den_nhpi = den_nhpi, den_total = den_total))
  
}

calc_pums_pop <- function(var_name) {
# Adapted somewhat from: https://github.com/catalystcalifornia/boldvision_2023/blob/main/demographics/demo_asian_disagg.R
# Must declare vector of variable names in your script, e.g. vars = c("indian", "chamorro")
# You must also calc the denominators in your script. See: W:/Project/RACE COUNTS/2025_v7/RC_Github/LF/RaceCounts/MOSAIC/IndicatorScripts/anhpi_pop_pums.R
### Then run something like this: pop_table <- map_dfr(vars, calc_pums_pop)

  # Determine race group
  den_group <- case_when( # used to determine which pop denominator to use for rate calcs
    var_name %in% c("nat_hawaiian","chamorro","samoan","oth_nhpi") ~ "RACNHPI",
    var_name %in% c("indian","chinese","filipino","japanese",
                    "korean","vietnamese","oth_asian") ~ "RACASN"
  )
  
  race_group <- case_when( # used to differentiate asian / nhpi rows
    var_name %in% c("nat_hawaiian","chamorro","samoan","oth_nhpi") ~ "nhpi",
    var_name %in% c("indian","chinese","filipino","japanese",
                    "korean","vietnamese","oth_asian") ~ "asian"
  )
  
  # Select appropriate denominator (already computed in indicator script!)
  pop_df <- switch(
    den_group,
    "RACASN" = den_asian,
    "RACNHPI"  = den_nhpi
  )
  
  # Numerator
  num_df <- anhpi_svry %>%
    filter(.data[[den_group]] == 1) %>%
    group_by(geoid, geoname) %>%
    summarise(
      num  = survey_total(.data[[var_name]] == 1, na.rm = TRUE),
      rate = survey_mean(.data[[var_name]] == 1, na.rm = TRUE),
      #num  = survey_total(na.rm = TRUE),
      #rate = survey_mean(na.rm = TRUE),
      .groups = "drop"
    )
  
  # Join + metrics
  num_df %>%
    left_join(pop_df, by = c("geoid", "geoname")) %>%
    mutate(
      subgroup  = var_name,
      group     = race_group,
      rate      = rate * 100,
      #rate_se   = (num_se / pop),              # delta method (approx)
      rate_moe  = rate_se * 1.645 * 100,
      rate_cv   = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
      count_moe = num_se * 1.645,
      count_cv  = ifelse(num > 0, (num_se / num) * 100, NA_real_)
    )
}













############### PUMS COUNT/RATE CALCS ################ Adapted from: W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/PUMS_Functions_new.R
calc_pums <- function(d, indicator, indicator_val, weight) {
  # d = PUMS dataframe, must contain geoid, geoname, RAC2P, subgroup fields
  # indicator = name of column that contains indicator data, eg: 'living_wage' which contains values 'livable' and 'not livable'
  # indicator_val = desired indicator value, eg: 'livable' (not 'not livable')        
  # weight = PWGTP for person-level (psam_p06.csv) or WGTP for housing unit-level (psam_h06.csv) analysis
  
  # create survey design  for indicator by county
  x <- d %>%               
    as_survey_rep(
      variables = c(geoid, geoname, race, latino, aian, pacisl, swana, !!sym(indicator)),   # select grouping variables
      weights = weight,                       # person or housing unit weight, must be defined in indicator script
      repweights = repwlist,                  # list of replicate weights
      combined_weights = TRUE,                # tells the function that replicate weights are included in the data
      mse =TRUE,                              # tells the function to calc mse
      type="other",                           # statistical method
      scale=4/80,                             # scaling set by ACS
      rscale=rep(1,80)                        # setting specific to ACS-scaling
    ) %>%
    filter(!is.na(indicator))			  	  # drop rows where indicator is null
  
  
  ###### Summarize Data
  
  # calculate by latino 
  indicator_latino <- x %>%
    group_by(geoid, geoname, latino, !!sym(indicator)) %>%
    summarise(
      count = survey_total(na.rm=T),         							  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%            								  # get the (survey weighted) proportion for the numerator
    
    left_join(x %>%                                        				  # left join in the denominators 
                group_by(geoid, geoname, latino) %>%                       
                summarise(pop = survey_total(na.rm=T))) %>%               # get the (survey weighted) universe aka denominator
    mutate(rate = rate * 100,                                             # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,                              # calculate the derived margin of error for the rate
           rate_cv = ((rate_moe/1.645)/rate) * 100,                       # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100)                    # calculate cv for numerator count
  
  print(head(indicator_latino))
  
  # calculate by aian 
  indicator_aian <- x %>%
    group_by(geoid, geoname, aian, !!sym(indicator)) %>%
    summarise(
      count = survey_total(na.rm=T),         							  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%            								  # get the (survey weighted) proportion for the numerator
    
    left_join(x %>%                                        				  # left join in the denominators 
                group_by(geoid, geoname, aian) %>%                       
                summarise(pop = survey_total(na.rm=T))) %>%               # get the (survey weighted) universe aka denominator
    mutate(rate = rate * 100,                                             # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,                              # calculate the derived margin of error for the rate
           rate_cv = ((rate_moe/1.645)/rate) * 100,                       # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100)                    # calculate cv for numerator count
  
  print(head(indicator_aian))
  
  # calculate by pacisl 
  indicator_pacisl <- x %>%
    group_by(geoid, geoname, pacisl, !!sym(indicator)) %>%
    summarise(
      count = survey_total(na.rm=T),         							  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%            								  # get the (survey weighted) proportion for the numerator
    
    left_join(x %>%                                        				  # left join in the denominators 
                group_by(geoid, geoname, pacisl) %>%                       
                summarise(pop = survey_total(na.rm=T))) %>%               # get the (survey weighted) universe aka denominator
    mutate(rate = rate * 100,                                             # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,                              # calculate the derived margin of error for the rate
           rate_cv = ((rate_moe/1.645)/rate) * 100,                       # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100)                    # calculate cv for numerator count
  
  print(head(indicator_pacisl))
  
  # calculate by swana 
  indicator_swana <- x %>%
    group_by(geoid, geoname, swana, !!sym(indicator)) %>%
    summarise(
      count = survey_total(na.rm=T),         							  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%            								  # get the (survey weighted) proportion for the numerator
    
    left_join(x %>%                                        				  # left join in the denominators 
                group_by(geoid, geoname, swana) %>%                       
                summarise(pop = survey_total(na.rm=T))) %>%               # get the (survey weighted) universe aka denominator
    mutate(rate = rate * 100,                                             # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,                              # calculate the derived margin of error for the rate
           rate_cv = ((rate_moe/1.645)/rate) * 100,                       # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100)                    # calculate cv for numerator count
  
  
  #swana_geos <- distinct(d, geoid, geoname)							  # get geonames for SWANA only
  #indicator_swana <- indicator_swana %>%
  # left_join(swana_geos, by = 'geoid')								      # join geonames to SWANA data only  
  
  print(head(indicator_swana))
  
  ### calc by non-latinx race
  indicator_race <- x %>%
    group_by(geoid, geoname, race, !!sym(indicator)) %>%
    summarise(
      count = survey_total(na.rm=T),         							  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%            								  # get the (survey weighted) proportion for the numerator
    
    left_join(x %>%                                        				  # left join in the denominators 
                group_by(geoid, geoname, race) %>%                       
                summarise(pop = survey_total(na.rm=T))) %>%               # get the (survey weighted) universe aka denominator
    mutate(rate = rate * 100,                                             # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,                              # calculate the derived margin of error for the rate
           rate_cv = ((rate_moe/1.645)/rate) * 100,                       # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100)                    # calculate cv for numerator count
  
  print(head(indicator_race))
  
  ###### calc for total
  indicator_total <- x %>%
    group_by(geoid, geoname, !!sym(indicator)) %>%  
    summarise(
      count = survey_total(na.rm=T),										  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%											  # get the (survey weighted) proportion for the numerator      
    
    left_join(x %>%														  # left join in the denominators													
                group_by(geoid, geoname) %>%                            
                summarise(pop = survey_total(na.rm=T))) %>%						  # get the (survey weighted) universe aka denominator 
    mutate(rate = rate * 100,                                      		  # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,							  # calculate the derived margin of error for the rate   
           rate_cv = ((rate_moe/1.645)/rate) * 100,						  # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100,                    # calculate cv for numerator count
           raceeth = "total")											  # add raceeth col, set value to 'total'
  
  print(head(indicator_total))
  
  
  ##### Combine the data frames into one
  # First code consistent race/eth col to prep for join
  indicator_race$raceeth <- as.character(indicator_race$race) 			  			 	# add new raceeth col, convert factor to character
  indicator_latino$raceeth <- as.character(indicator_latino$latino)
  indicator_aian$raceeth <- as.character(indicator_aian$aian)
  indicator_pacisl$raceeth <- as.character(indicator_pacisl$pacisl)
  indicator_swana$raceeth <- as.character(indicator_swana$swana)
  
  
  indicator_df <- 												
    bind_rows(
      indicator_race %>% 
        filter(!raceeth %in% c("white", "asian", "black", "other", "twoormor")),      	# keep only non-latinx/aian/pacisl/swana race groups
      indicator_latino %>% 
        filter(raceeth =="latino"), 
      indicator_aian %>% 
        filter(raceeth =="aian"), 
      indicator_pacisl %>% 
        filter(raceeth =="pacisl"), 
      indicator_swana %>% 
        select(-swana) %>% 
        filter(raceeth =="swana"),
      indicator_total)
  
  indicator_df <- data.frame(indicator_df) %>% select(-race, -latino, -aian, -pacisl, -swana)  # drop unneeded columns, for some reason doing in the prev. step like in state_pums didn't work
  
  # review combined table
  print(head(indicator_df, 10))
  
  
  ########4. Prepare final df
  ### get the count of non-NA values
  indicator_ <- indicator_df %>% 
    filter(!!sym(indicator) == !!indicator_val) %>%				    # filter only records for desired ppl$indicator value
    filter(!is.na(raceeth)) %>%										# filter out records where raceeth is NULL
    group_by(geoid, geoname) %>%									# group by county and county name
    as.data.frame()
  
  # convert long format to wide
  rc_indicator <- indicator_ %>% 
    pivot_wider(id_cols = c(geoid, geoname),						# convert to wide format
                names_from = raceeth,
                values_from = c("count", "pop", "rate", "rate_moe", "rate_cv", "count_moe", "count_cv")) %>%  
    as.data.frame()
  
  # drop rows where geoid = NA
  rc_indicator <- rc_indicator %>% drop_na(geoid)
  
  return(rc_indicator)
}








#### Run PUMS Calcs ####
calc_anhpi_pums <- function(d, indicator, indicator_val, weight) {
  # d = PUMS dataframe, must contain geoid, geoname, var_code, val_label, indicator fields
  # indicator = name of column that contains indicator data, eg: 'living_wage' which contains values 'livable' and 'not livable'
  # indicator_val = desired indicator value, eg: 'livable' (not 'not livable')        
  # weight = PWGTP for person-level (psam_p06.csv) or WGTP for housing unit-level (psam_h06.csv) analysis
  
  # create survey design  for indicator
  x <- na.omit(d) %>%               
    as_survey_rep(
      variables = c(geoid, geoname, var_code, val_label, !!sym(indicator)),   # select grouping variables
      weights = weight,                       # person or housing unit weight, must be defined in indicator script
      repweights = repwlist,                  # list of replicate weights
      combined_weights = TRUE,                # tells the function that replicate weights are included in the data
      mse = TRUE,                             # tells the function to calc mse
      type="other",                           # statistical method
      scale=4/80,                             # scaling set by ACS
      rscale=rep(1,80)                        # setting specific to ACS-scaling
    ) %>%
    filter(!is.na(indicator))			  	        # drop rows where indicator is null
  
  # calculate by subgroup 
  indicator_subgroup <- x %>%
    group_by(geoid, geoname, var_code, val_label, !!sym(indicator)) %>%
    summarise(
      count = survey_total(na.rm=T),         							  # get the (survey weighted) count for the numerator
      rate = survey_mean()) %>%            								  # get the (survey weighted) proportion for the numerator
    
    left_join(x %>%                                        				        # left join in the denominators 
                group_by(geoid, geoname, var_code, val_label) %>%                       
                summarise(pop = survey_total(na.rm=T))) %>%               # get the (survey weighted) universe aka denominator
    mutate(rate = rate * 100,                                             # get the rate as a %
           rate_moe = rate_se * 1.645 * 100,                              # calculate the derived margin of error for the rate
           rate_cv = ((rate_moe/1.645)/rate) * 100,                       # calculate the coefficient of variation for the rate
           count_moe = count_se*1.645,                                    # calculate moe for numerator count based on se provided by the output  
           count_cv = ((count_moe/1.645)/count) * 100)                    # calculate cv for numerator count
  
  indicator_subgroup$raceeth <- as.character(indicator_subgroup$val_label)
  print(indicator_subgroup, n = 25)
  
  
  indicator_ <- indicator_subgroup %>% 
    filter(!!sym(indicator) == !!indicator_val) %>%				    # filter only records for desired ppl$indicator value
    filter(!is.na(raceeth)) %>%										            # filter out records where raceeth is NULL
    group_by(geoid, geoname) %>%										          # group by geo and geoname name
    as.data.frame()
  
  # convert long format to wide
  rc_indicator <- indicator_ %>% 
    pivot_wider(id_cols = c(geoid, geoname),						# convert to wide format
                names_from = raceeth,
                values_from = c("count", "pop", "rate", "rate_moe", "rate_cv", "count_moe", "count_cv")) %>%  
    as.data.frame()
  
  # drop rows where geoid = NA
  rc_indicator <- rc_indicator %>% drop_na(geoid)
  
  return(rc_indicator)
}


############### SCREEN DATA: Designed for MOSAIC RC, you can also screen within your script and skip this function ############### 
# based on: W:/RDA Team/R/Github/RDA Functions/LF/RDA-Functions/PUMS_Functions_new.R
# d = PUMS dataframe which can have one or more geolevels within it, it must use RC race suffixes on column names/race groups
# cv_threshold = Coefficient of Variation threshold, data with CVs higher than threshold are screened. Must be defined as pct not decimal, eg: 30 not .3
# raw_rate_threshold = Data values less than threshold are screened, for RC indicators threshold is 0.
# pop_threshold = Data for geos+race combos with pop smaller than threshold are screened.
# pums_screen <- function(d, cv_threshold, raw_rate_threshold, pop_threshold, indicator_val) {
#   
#   screened <- d %>%             
#     mutate(d, total_rate = ifelse(d$rate_cv_total > cv_threshold | d$rate_total < raw_rate_threshold | d$pop_total < pop_threshold, NA, d$rate_total),
#            latino_rate = ifelse(d$rate_cv_latino > cv_threshold | d$rate_latino < raw_rate_threshold | d$pop_latino < pop_threshold, NA, d$rate_latino),
#            nh_white_rate = ifelse(d$rate_cv_nh_white > cv_threshold | d$rate_nh_white < raw_rate_threshold | d$pop_nh_white < pop_threshold, NA, d$rate_nh_white),
#            nh_black_rate = ifelse(d$rate_cv_nh_black > cv_threshold | d$rate_nh_black < raw_rate_threshold | d$pop_nh_black < pop_threshold, NA, d$rate_nh_black),
#            nh_asian_rate = ifelse(d$rate_cv_nh_asian > cv_threshold | d$rate_nh_asian < raw_rate_threshold | d$pop_nh_asian < pop_threshold, NA, d$rate_nh_asian),
#            aian_rate = ifelse(d$rate_cv_aian > cv_threshold | d$rate_aian < raw_rate_threshold | d$pop_aian < pop_threshold, NA, d$rate_aian),
#            pacisl_rate = ifelse(d$rate_cv_pacisl > cv_threshold | d$rate_pacisl < raw_rate_threshold | d$pop_pacisl < pop_threshold, NA, d$rate_pacisl),
#            swana_rate = ifelse(d$rate_cv_swana > cv_threshold | d$rate_swana < raw_rate_threshold | d$pop_swana < pop_threshold, NA, d$rate_swana),
#            nh_other_rate = ifelse(d$rate_cv_nh_other > cv_threshold | d$rate_nh_other < raw_rate_threshold | d$pop_nh_other < pop_threshold, NA, d$rate_nh_other),
#            nh_twoormor_rate = ifelse(d$rate_cv_nh_twoormor > cv_threshold | d$rate_nh_twoormor < raw_rate_threshold | d$pop_nh_twoormor < pop_threshold, NA, d$rate_nh_twoormor),
#            
#            total_raw = ifelse(d$rate_cv_total > cv_threshold | d$num_total < raw_rate_threshold | d$pop_total < pop_threshold, NA, d$num_total),
#            latino_raw = ifelse(d$rate_cv_latino > cv_threshold | d$num_latino < raw_rate_threshold | d$pop_latino < pop_threshold, NA, d$num_latino),
#            nh_white_raw = ifelse(d$rate_cv_nh_white > cv_threshold | d$num_nh_white < raw_rate_threshold | d$pop_nh_white < pop_threshold, NA, d$num_nh_white),
#            nh_black_raw = ifelse(d$rate_cv_nh_black > cv_threshold | d$num_nh_black < raw_rate_threshold | d$pop_nh_black < pop_threshold, NA, d$num_nh_black),
#            nh_asian_raw = ifelse(d$rate_cv_nh_asian > cv_threshold | d$num_nh_asian < raw_rate_threshold | d$pop_nh_asian < pop_threshold, NA, d$num_nh_asian),
#            aian_raw = ifelse(d$rate_cv_aian > cv_threshold | d$num_aian < raw_rate_threshold | d$pop_aian < pop_threshold, NA, d$num_aian),
#            pacisl_raw = ifelse(d$rate_cv_pacisl > cv_threshold | d$num_pacisl < raw_rate_threshold | d$pop_pacisl < pop_threshold, NA, d$num_pacisl),
#            swana_raw = ifelse(d$rate_cv_swana > cv_threshold | d$num_swana < raw_rate_threshold | d$pop_swana < pop_threshold, NA, d$num_swana),
#            nh_other_raw = ifelse(d$rate_cv_nh_other > cv_threshold | d$num_nh_other < raw_rate_threshold | d$pop_nh_other < pop_threshold, NA, d$num_nh_other),
#            nh_twoormor_raw = ifelse(d$rate_cv_nh_twoormor > cv_threshold | d$num_nh_twoormor < raw_rate_threshold | d$pop_nh_twoormor < pop_threshold, NA, d$num_nh_twoormor))
#   
#   colnames(screened) <- gsub("rate_", paste0((sym(indicator_val)), "_rate_"), colnames(screened))
#   
#   return(screened)
#   
# }
