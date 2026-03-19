##### Reclassify Asian and NHPI Ancestries ######## 
anhpi_reclass <- function(x, acs_yr, ancestry_list) {  # used in MOSAIC Living Wage test
  # x = pums dataframe
  # acs_yr = last year of ACS 5y estimates
  # ancestry_list = dataframe containing all PUMS ANC1P/ANC2P codes and labels, plus 1 binary column for asian and 1 for nhpi
  ## import ANC1P/ANC2P codes/labels pulled from https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf
  print(paste0("Adding ancestry labels from:", ancestry_list))
  anc_codes <- ancestry_list %>%
    mutate(anc_label = stringr::str_to_lower(stringr::str_replace_all(anc_label, "\\s+", "_")))
  print(head(anc_codes %>% filter(asian==1)))
  # get list of AA or PI ancestries actually in CA PUMS data
  aapi_incl <- x %>% select(ANC1P) %>% 
    unique() %>%
    inner_join(anc_codes %>% filter((asian == 1 | nhpi ==1)), by ="ANC1P") %>%
    arrange(anc_label)
  print("Displaying all the AA or PI ancestries actually present in the data as aapi_incl df.")
  View(aapi_incl)

  x <- x %>% left_join(anc_codes %>% select(ANC1P, anc_label))
  x <- x %>% left_join(anc_codes %>% select(ANC1P, anc_label), by = c("ANC2P" = "ANC1P"))
  reclass_list <- list(people = x, aapi_incl = aapi_incl)
  
return(reclass_list)
}



##### Prep denominators & survey for ANHPI PUMS pop calcs ######## 
pums_pop_srvy_denom <- function(d, weight, repwlist, vars){
  # d = pums dataframe, e.g. ppl_state
  # weight and repwlist are defined at top of script
  # vars is the list of asian and nhpi subgroups
  # After running this and creating resulting list elements as df's in your environment, run calc_pums_pop{} below.
  
  start_time <- Sys.time()  # start timer
  
  message("Building survey object...")
  anhpi_srvy <- d %>%      
    as_survey_rep(
      variables = c(geoid, geoname, asian, nhpi, vars),   # dplyr::select grouping variables. any asian ancestry, any nhpi ancestry, vars = asian and nhpi subgroups
      weights = weight,                       # person weight
      repweights = all_of(repwlist),                  # list of replicate weights
      combined_weights = TRUE,                # tells the function that replicate weights are included in the data
      mse = TRUE,                             # tells the function to calc mse
      type="other",                           # statistical method
      scale=4/80,                             # scaling set by ACS
      rscale=rep(1,80)                        # setting specific to ACS-scaling
    )
  
  message("Calculating total, Asian, NHPI denominators...")
  den_total <- bind_rows(
    anhpi_srvy %>%     # total denominator
      group_by(geoid, geoname) %>%
      summarise(pop = survey_total(na.rm = TRUE), .groups = "drop") %>%
      mutate(pop_group = "total"),
    
    anhpi_srvy %>%     # asian denominator
      filter(asian == 1) %>%
      group_by(geoid, geoname) %>%
      summarise(pop = survey_total(na.rm = TRUE), .groups = "drop") %>%
      mutate(pop_group = "asian"),
    
    anhpi_srvy %>%     # nhpi denominator
      filter(nhpi == 1) %>%
      group_by(geoid, geoname) %>%
      summarise(pop = survey_total(na.rm = TRUE), .groups = "drop") %>%
      mutate(pop_group = "nhpi")
  )
  
  message("Calculating any Asian ancestry group counts and rates...")
  num_df_asian <- anhpi_srvy %>%
    filter(asian == 1) %>%
    group_by(geoid, geoname) %>%
    summarise(
      num = survey_total(na.rm = TRUE),
      .groups = "drop"
    ) %>%
    left_join(
      den_total %>% filter(pop_group == "total"),
      by = c("geoid","geoname")
    ) %>%
    mutate(
      rate = num / pop,
      subgroup = "asian",
      group = "asian",
      rate_se = num_se / pop,
      rate_moe = rate_se * 1.645 * 100,
      rate_cv = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
      rate = rate * 100,
      count_moe = num_se * 1.645,
      count_cv = ifelse(num > 0, (num_se / num) * 100, NA_real_)
    )
  
  message("Calculating any NHPI ancestry group counts and rates...")
  num_df_nhpi <- anhpi_srvy %>%
    filter(nhpi == 1) %>%
    group_by(geoid, geoname) %>%
    summarise(
      num = survey_total(na.rm = TRUE),
      .groups = "drop"
    ) %>%
    left_join(
      den_total %>% filter(pop_group == "total"),
      by = c("geoid","geoname")
    ) %>%
    mutate(
      rate = num / pop,
      subgroup = "nhpi",
      group = "nhpi",
      rate_se = num_se / pop,
      rate_moe = rate_se * 1.645 * 100,
      rate_cv = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
      rate = rate * 100,
      count_moe = num_se * 1.645,
      count_cv = ifelse(num > 0, (num_se / num) * 100, NA_real_)
    )
  
  # Combine subgroup and group dfs
  num_df_group <- bind_rows(num_df_asian, num_df_nhpi)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "mins"), 2)
  message(paste0("Done! pums_pop_srvy_denom completed in ", elapsed, " minutes."))
  
  return(list(num_df_group = num_df_group, anhpi_srvy = anhpi_srvy, den_total = den_total))
}

##### Calc ANHPI PUMS pop ######## 
calc_pums_pop <- function(var_name) {
  # Adapted somewhat from: https://github.com/catalystcalifornia/boldvision_2023/blob/main/demographics/demo_asian_disagg.R
  # You must first calc the denominators in your script using pums_pop_srvy_denom{} above. See: W:/Project/RACE COUNTS/2025_v7/RC_Github/LF/RaceCounts/MOSAIC/IndicatorScripts/anhpi_pop_pums.R
  ### Inputs used here, created by pums_pop_srvy_denom: num_df_group, den_total, den_asian, den_nhpi, total_srvy, asian_srvy, nhpi_srvy
  # var_name is the list of asian and nhpi subgroups
  ### Then run something like this: pop_table <- map(vars, calc_pums_pop) |> list_rbind()
  
  message(paste0(var_name, ": Assigning subgroup survey and denominator (asian , nhpi)..."))
  
  # Assign ancestry group, used to determine which pop denominator to use for rate calcs
  den_group <- case_when( 
    var_name %in% (aapi_incl %>% filter(nhpi == 1) %>% pull(anc_label)) ~ "nhpi",
    var_name %in% (aapi_incl %>% filter(asian == 1) %>% pull(anc_label)) ~ "asian"
  )
  
  # Select appropriate denominator & survey (already computed in indicator script!)
  pop_df <- switch(
    den_group,
    "asian" = den_total %>% filter(pop_group == 'asian'),
    "nhpi"  = den_total %>% filter(pop_group == 'nhpi')
  )
  
  srvy_subset <- switch(den_group,
                        "asian" = anhpi_srvy %>% filter(asian == 1),
                        "nhpi"  = anhpi_srvy %>% filter(nhpi == 1)
  )
  
  message(paste0(var_name, ": Calculating subgroup counts and rates..."))
  # Numerator
  num_df <- srvy_subset %>%
    group_by(geoid, geoname) %>%
    summarise(
      num  = survey_total(.data[[var_name]] == 1, na.rm = TRUE),
      rate = survey_mean(.data[[var_name]] == 1, na.rm = TRUE),
      .groups = "drop"
    ) %>%
  
  # Join + metrics
  left_join(pop_df, by = c("geoid", "geoname")) %>%
    mutate(
      subgroup  = var_name,
      group     = den_group,
      rate_moe  = rate_se * 1.645 * 100,
      rate_cv   = ifelse(rate > 0, (rate_se / rate) * 100, NA_real_),
      rate      = rate * 100,
      count_moe = num_se * 1.645,
      count_cv  = ifelse(num > 0, (num_se / num) * 100, NA_real_)
    ) %>%
    filter(!is.na(geoid))
  
return(num_df)  
}


