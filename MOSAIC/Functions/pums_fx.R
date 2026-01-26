##### Reclassify Asian and NHPI Ancestries ########  Note: Ancestry lists sourced at top of indicator script
anhpi_reclass <- function(x, acs_yr, ancestry_list) {
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
