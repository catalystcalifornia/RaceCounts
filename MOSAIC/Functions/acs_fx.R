### MOSAIC-related functions for ACS data ###

#install packages if not already installed
packages <- c("readr", "tidyr", "dplyr", "DBI", "RPostgres", "tidycensus", "tidyverse", "stringr", "usethis", "httr", "rlang")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 


#### Automate writing API calls and pull ACS SPT detailed Asian & NHPI race tables for city/county/state ####
get_detailed_race <- function(table, race, year = 2021) {
  # race = for MOSAIC, either 'asian' or 'nhpi', case-insensitive
  # table = ACS table name, eg: "B25003" or "S2701", case-insensitive
  # year = ACS data year, defaults to 2021 if none specified
  
  race_code <- case_when(
    str_detect(race, regex('asian', ignore_case = TRUE)) ~ '-04',
    str_detect(race, regex('nhpi', ignore_case = TRUE)) ~ '-05',
    .default = ''
    )

  # return error if race is not already included in available race_code list
    if (!(race_code %in% c('-04','-05'))) {
       return(print("This function doesn't pull data for the race you selected. Please select either Asian or NHPI or talk to Leila about adding an additional race."))
   }

  table_name <- toupper(table)
  
  city_api_call <- sprintf(
    "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&POPGROUP=pseudo(%s)&ucgid=pseudo(0400000US06$1600000)",
    year, table_name, race_code)
  
  county_api_call <- sprintf(
    "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&POPGROUP=pseudo(%s)&ucgid=pseudo(0400000US06$0500000)",
    year, table_name, race_code)
  
  state_api_call <- sprintf(
    "https://api.census.gov/data/%s/acs/acs5/spt?get=group(%s)&POPGROUP=pseudo(%s)&ucgid=0400000US06",
    year, table_name, race_code)

api_call_list <- c(city_api_call, county_api_call, state_api_call)  
  
# Loop through API calls
parsed_list <- list()    # create empty list for loop results

for (i in api_call_list) {
  
  response <- GET(i)
  status_code(response)
  
  # Check if the request was successful
  if (status_code(response) == 200) {
    print("API request successful.")
  } else {
    stop("API request failed with status code: ", status_code(response))
  }
  
  raw_content <- content(response, "text")   # returns the response body as text
  parsed_data <- fromJSON(raw_content)
  
  parsed_list[[i]] <- parsed_data               # put loop results into a list
  
}

# clean up data
parsed_data <- do.call(rbind, parsed_list) %>% as.data.frame()
clean_data <- parsed_data
colnames(clean_data) <- clean_data[1, ]  # replace col names with first row values
clean_data <- clean_data[, !duplicated(names(clean_data))] # drop any duplicate cols, eg: POPGROUP
clean_data <- clean_data %>%
  mutate(across(contains(table_name), as.numeric))         # assign numeric cols to numeric type
clean_data <- clean_data %>%
  filter(!if_all(where(is.numeric), is.na))                # drop rows where all numeric values are NA, this also removes the extra 'header' rows
clean_data$geoid <- str_replace(clean_data$GEO_ID, ".*US", "")  # clean geoids
clean_data$geolevel <- case_when(                                # add geolevel bc it's a multigeo table
  nchar(clean_data$geoid) == 2 ~ 'state',
  nchar(clean_data$geoid) == 5 ~ 'county',
  .default = 'place'
)

  clean_data <- clean_data %>%
    select(where(~!all(is.na(.))))         # drop cols where all vals are NA, eg: X_EA and X_MA the annotation cols

  # reformat clean data
  df_wide <- clean_data %>%
    pivot_longer(
      cols = starts_with("B25003"),
      names_to = "orig_col",
      values_to = "value"
    ) %>%
    mutate(
      suffix = str_remove(orig_col, "^B25003_?"),
      new_col = paste0("B25003_", POPGROUP, "_", suffix)
    ) %>%
    select(NAME, geoid, geolevel, new_col, value) %>%
    pivot_wider(
      names_from = new_col,
      values_from = value
    )
  
  colnames(df_wide) <- tolower(colnames(df_wide))
  
  source(".//Functions//rdashared_functions.R")
  df_wide <- df_wide %>% 
    mutate(geoname = name) %>%
    clean_geo_names() %>%
    mutate(name = geoname) %>%
    select(-geoname)

  # prep metadata
  metadata <- clean_data %>%
    pivot_longer(cols = starts_with("B"),
                 names_to = "var",
                 values_to = "raw")
  
  metadata <- metadata %>%
    select(POPGROUP, POPGROUP_LABEL, var) %>%
    unique() %>%
    mutate(var_suff = sub(".*_", "", var),
           generic_var = gsub(("E|M"), "", var),
           new_var = tolower(paste0(table_code, "_", POPGROUP, "_", var_suff)))
                                              
  # load variable names
  v21 <- load_variables(2021, "acs5", cache = TRUE)
  table_vars <- v21 %>% filter(grepl(table_name, name))
  
  # join variable names to metadata
  metadata <- metadata %>% 
    left_join(table_vars %>% select(name, label), by = c("generic_var" = "name")) %>%
    mutate(new_label = paste0(label, " ", POPGROUP_LABEL)) %>%
    mutate(new_label = case_when(
      grepl("M", var) == TRUE ~ gsub("Estimate", "MOE", new_label),
      .default = new_label))
  
  metadata_ <- metadata %>%
    select(new_var, new_label) %>%
    arrange(new_var)
  
  new_rows <- data.frame(
    new_var = c("name", "geoid", "geolevel"),
    new_label = c("","fips code","city, county, state"))
  
  metadata_final <- rbind(new_rows, metadata_)

data_list <- list(df_wide, metadata_final)

names(data_list) <- c(paste0(race,"_df"), "metadata")

return(data_list)
}
  

#### Send raw detailed tables to postgres - info for postgres tables automatically updates ####
send_to_mosaic <- function(acs_table, df_list, table_schema){
  
  name_as_string <- names(df_list)[1]
  
  table_name <- sprintf("%s_acs_5yr_%s_multigeo_%s",
                        str_replace(name_as_string, "_df*.", ""), tolower(table_code), curr_yr)
  print(table_name)
  
  # generate table comment
  indicator <-  paste0("Disaggregated ", str_replace(name_as_string, "_df*.", ""), " detailed race alone and alone or in combination with another race by City, County, and State")       
  source <- paste0("ACS ", curr_yr - 4, "-", curr_yr, ", Table ", toupper(acs_table)) 
  print(indicator)
  print(source)
  
  column_names <- df_list[[2]]$new_var
  column_comments <- df_list[[2]]$new_label
  
  # send table to postgres
  dbWriteTable(con, 
               Id(schema = table_schema, table = table_name), 
               df_list[[1]], overwrite = FALSE)
  
  # comment on table and columns
  add_table_comments(con, table_schema, table_name, indicator, source, qa_filepath, column_names, column_comments)
  
}



#### Prep ACS data for RC Fx ####
### For prep_acs: Check table variables each year as they may change. Modify the URLs below by year and table. ###
# Subject Tables: https://api.census.gov/data/2022/acs/acs5/subject/groups/S2701.html
# Detailed Tables: https://api.census.gov/data/2022/acs/acs5/groups/B19301.html
# Profile Tables: https://api.census.gov/data/2022/acs/acs5/profile/groups/DP05.html


##### Prep ACS tables for RC fx #####
prep_acs <- function(x, race, table_code, cv_threshold, pop_threshold) {
  
  # SQL query to retrieve column names and comments: The query uses the pg_catalog.col_description function
  
  # Define the schema and table name
  table_name <- sprintf("%s_acs_5yr_%s_multigeo_%s",
                        tolower(race), tolower(table_code), curr_yr)
  
  #	query <- sprintf("
  #	SELECT
  #		c.column_name,
  #		pg_catalog.col_description(
  #			(c.table_schema || '.' || c.table_name)::regclass::oid,
  #			c.ordinal_position
  #		) AS column_comment
  #	FROM
  #		information_schema.columns c
  #	WHERE
  #		c.table_schema = '%s'
  #		AND c.table_name = '%s'
  #	ORDER BY
  #		c.ordinal_position;",
  #		rc_schema, table_name)
  
  # Execute the query and store the results in an R data frame
  #	column_metadata <- dbGetQuery(con, query) %>% filter(grepl("e",column_name)) %>% filter(grepl("001",column_name)) # get unique codes, eg: 051, 052
  #	print(column_metadata)  # this df was used to create the renaming rules below
  
  # renaming rules will change depending on type of census table
  if (startsWith(table_code, "b") && startsWith(table_name, "nhpi")) {
    table_051_code = paste0(table_code, "_051_")
    table_052_code = paste0(table_code, "_052_")
    table_053_code = paste0(table_code, "_053_")
    table_054_code = paste0(table_code, "_054_")
    table_055_code = paste0(table_code, "_055_")
    table_056_code = paste0(table_code, "_056_")
    table_057_code = paste0(table_code, "_057_")
    table_058_code = paste0(table_code, "_058_")
    table_061_code = paste0(table_code, "_061_")
    table_062_code = paste0(table_code, "_062_")
    table_063_code = paste0(table_code, "_063_")
    table_064_code = paste0(table_code, "_064_")
    table_065_code = paste0(table_code, "_065_")
    table_066_code = paste0(table_code, "_066_")
    table_067_code = paste0(table_code, "_067_")
    table_068_code = paste0(table_code, "_068_")
    table_9z8_code = paste0(table_code, "_9z8_")
    table_9z9_code = paste0(table_code, "_9z9_")
    table_096_code = paste0(table_code, "_096_")
    table_176_code = paste0(table_code, "_176_")
    table_177_code = paste0(table_code, "_177_")
    
    names(x) <- gsub(table_051_code, "polynesian", names(x))
    names(x) <- gsub(table_052_code, "nat_hawaii", names(x))
    names(x) <- gsub(table_053_code, "samoan", names(x))
    names(x) <- gsub(table_054_code, "tongan", names(x))
    names(x) <- gsub(table_055_code, "micronesian", names(x))
    names(x) <- gsub(table_056_code, "guam_chamorro", names(x))
    names(x) <- gsub(table_057_code, "melanesian", names(x))
    names(x) <- gsub(table_058_code, "fijian", names(x))	
    names(x) <- gsub(table_061_code, "polynesian_aoic", names(x))
    names(x) <- gsub(table_062_code, "nat_hawaii_aoic", names(x))
    names(x) <- gsub(table_063_code, "samoan_aoic", names(x))
    names(x) <- gsub(table_064_code, "tongan_aoic", names(x))
    names(x) <- gsub(table_065_code, "micronesian_aoic", names(x))
    names(x) <- gsub(table_066_code, "guam_chamorro_aoic", names(x))
    names(x) <- gsub(table_067_code, "melanesian_aoic", names(x))
    names(x) <- gsub(table_068_code, "fijian_aoic", names(x))	
    names(x) <- gsub(table_9z8_code, "chamorro", names(x))
    names(x) <- gsub(table_9z9_code, "chamorro_aoic", names(x))
    names(x) <- gsub(table_096_code, "marshallese", names(x))
    names(x) <- gsub(table_176_code, "marshallese_aoic", names(x))
    names(x) <- gsub(table_177_code, "palauan_aoic", names(x))
    
  } else if (startsWith(table_code, "b") && startsWith(table_name, "asian")) {
    table_013_code = paste0(table_code, "_051_")
    table_014_code = paste0(table_code, "_052_")
    table_015_code = paste0(table_code, "_053_")
    table_016_code = paste0(table_code, "_054_")
    table_017_code = paste0(table_code, "_055_")
    table_019_code = paste0(table_code, "_056_")
    table_020_code = paste0(table_code, "_057_")
    table_021_code = paste0(table_code, "_058_")
    table_022_code = paste0(table_code, "_061_")
    table_023_code = paste0(table_code, "_062_")
    table_024_code = paste0(table_code, "_063_")
    table_026_code = paste0(table_code, "_064_")
    table_027_code = paste0(table_code, "_065_")
    table_028_code = paste0(table_code, "_066_")
    table_029_code = paste0(table_code, "_067_")
    table_032_code = paste0(table_code, "_068_")
    table_033_code = paste0(table_code, "_9z8_")
    table_034_code = paste0(table_code, "_9z9_")
    table_035_code = paste0(table_code, "_176_")
    table_036_code = paste0(table_code, "_177_")
    table_037_code = paste0(table_code, "_037_")
    table_038_code = paste0(table_code, "_038_")
    table_039_code = paste0(table_code, "_039_")
    table_040_code = paste0(table_code, "_040_")
    table_041_code = paste0(table_code, "_041_")
    table_042_code = paste0(table_code, "_042_")
    table_043_code = paste0(table_code, "_043_")
    table_045_code = paste0(table_code, "_045_")
    table_046_code = paste0(table_code, "_046_")
    table_047_code = paste0(table_code, "_047_")
    table_048_code = paste0(table_code, "_048_")
    table_072_code = paste0(table_code, "_072_")
    table_073_code = paste0(table_code, "_073_")
    table_075_code = paste0(table_code, "_075_")
    table_076_code = paste0(table_code, "_076_")
    table_081_code = paste0(table_code, "_081_")
    table_083_code = paste0(table_code, "_083_")
    table_084_code = paste0(table_code, "_084_")
    table_025_code = paste0(table_code, "_025_")
    table_044_code = paste0(table_code, "_044_")
    table_085_code = paste0(table_code, "_085_")	
    
    names(x) <- gsub(table_013_code, "indian", names(x))
    names(x) <- gsub(table_014_code, "bangladeshi", names(x))
    names(x) <- gsub(table_015_code, "cambodian", names(x))
    names(x) <- gsub(table_016_code, "chinese", names(x))
    names(x) <- gsub(table_017_code, "chinese_no_taiwan", names(x))
    names(x) <- gsub(table_019_code, "taiwanese", names(x))
    names(x) <- gsub(table_020_code, "filipino", names(x))
    names(x) <- gsub(table_021_code, "indonesian", names(x))	
    names(x) <- gsub(table_022_code, "japanese", names(x))
    names(x) <- gsub(table_023_code, "korean", names(x))
    names(x) <- gsub(table_024_code, "laotian", names(x))
    names(x) <- gsub(table_026_code, "pakistani", names(x))
    names(x) <- gsub(table_027_code, "sri_lankan", names(x))
    names(x) <- gsub(table_028_code, "thai", names(x))
    names(x) <- gsub(table_029_code, "vietnamese", names(x))
    names(x) <- gsub(table_032_code, "indian_aoic", names(x))	
    names(x) <- gsub(table_033_code, "bangladeshi_aoic", names(x))
    names(x) <- gsub(table_034_code, "cambodian_aoic", names(x))
    names(x) <- gsub(table_035_code, "chinese_aoic", names(x))
    names(x) <- gsub(table_036_code, "chinese_no_taiwan_aoic", names(x))
    names(x) <- gsub(table_037_code, "taiwanese_aoic", names(x))
    names(x) <- gsub(table_038_code, "filipino_aoic", names(x))
    names(x) <- gsub(table_039_code, "hmong_aoic", names(x))
    names(x) <- gsub(table_040_code, "indonesian_aoic", names(x))
    names(x) <- gsub(table_041_code, "japanese_aoic", names(x))
    names(x) <- gsub(table_042_code, "korean_aoic", names(x))
    names(x) <- gsub(table_043_code, "laotian_aoic", names(x))
    names(x) <- gsub(table_045_code, "pakistani_aoic", names(x))	
    names(x) <- gsub(table_046_code, "sri_lankan_aoic", names(x))
    names(x) <- gsub(table_047_code, "thai_aoic", names(x))
    names(x) <- gsub(table_048_code, "vietnamese_aoic", names(x))
    names(x) <- gsub(table_072_code, "bhutanese", names(x))
    names(x) <- gsub(table_073_code, "burmese", names(x))
    names(x) <- gsub(table_075_code, "mongolian", names(x))
    names(x) <- gsub(table_076_code, "nepalese", names(x))
    names(x) <- gsub(table_081_code, "burmese_aoic", names(x))
    names(x) <- gsub(table_083_code, "mongolian_aoic", names(x))	
    names(x) <- gsub(table_084_code, "nepalese_aoic", names(x))
    names(x) <- gsub(table_025_code, "malaysian", names(x))
    names(x) <- gsub(table_044_code, "malaysian_aoic", names(x))
    names(x) <- gsub(table_085_code, "okinawan_aoic", names(x))
    
  } else {
    stop('The column renaming function did not work for the table you have submitted. Please check your table.')
  }
  
  
  if(endsWith(table_code, "b25014")) {
    # Overcrowded Housing #
    ## Occupants per Room
    names(x) <- gsub("001e", "_pop", names(x))
    names(x) <- gsub("001m", "_pop_moe", names(x))
    
    names(x) <- gsub("003e", "_raw", names(x))
    names(x) <- gsub("003m", "_raw_moe", names(x))
    
    ## total data (more disaggregated than raced values so different prep needed)
    
    ### Extract total values to perform the various calculations needed
    totals <- x %>%
      select(geoid, geolevel, starts_with("total"))
    
    totals <- totals %>% pivot_longer(total005e:total013e, names_to="var_name", values_to = "estimate")
    totals <- totals %>% pivot_longer(total005m:total013m, names_to="var_name2", values_to = "moe")
    totals$var_name <- substr(totals$var_name, 1, nchar(totals$var_name)-1)
    totals$var_name2 <- substr(totals$var_name2, 1, nchar(totals$var_name2)-1)
    totals <- totals[totals$var_name == totals$var_name2, ]
    totals <- select(totals, -c(var_name, var_name2))
    
    ### sum the numerator columns 005e-013e (total_raw):
    total_raw_values <- totals %>%
      select(geoid, geolevel, estimate) %>%
      group_by(geoid, geolevel) %>%
      summarise(total_raw = sum(estimate))
    
    #### join these calculations back to x
    x <- left_join(x, total_raw_values, by = c("geoid", "geolevel"))
    
    ### calculate the total_raw_moe using moe_sum (need to sort MOE values first to make sure highest MOE is used in case of multiple zero estimates)
    ### methodology source is text under table on slide 52 here: https://www.census.gov/content/dam/Census/programs-surveys/acs/guidance/training-presentations/20180418_MOE.pdf
    total_raw_moes <- totals %>%
      select(geoid, geolevel, estimate, moe) %>%
      group_by(geoid, geolevel) %>%
      arrange(desc(moe), .by_group = TRUE) %>%
      summarise(total_raw_moe = moe_sum(moe, estimate, na.rm=TRUE))   # https://walker-data.com/tidycensus/reference/moe_sum.html
    
    #### join these calculations back to x
    x <- left_join(x, total_raw_moes, by = c("geoid", "geolevel"))
    
    ### calculate total_rate
    total_rates <- left_join(total_raw_values, totals[, 1:3])
    total_rates$total_rate <- total_rates$total_raw/total_rates$total_pop*100
    total_rates <- total_rates %>%
      select(geoid, geolevel, total_rate) %>%
      distinct()
    
    #### join these calculations back to x
    x <- left_join(x, total_rates, by = c("geoid", "geolevel"))
    
    ### calculate the moe for total_rate
    total_pop_data <- totals %>%
      select(geoid, geolevel, total_pop, total_pop_moe) %>%
      distinct()
    total_rate_moes <- left_join(total_raw_values, total_raw_moes, by = c("geoid", "geolevel")) %>%
      left_join(., total_pop_data, by = c("geoid", "geolevel"))
    total_rate_moes$total_rate_moe <- moe_prop(total_rate_moes$total_raw,    # https://walker-data.com/tidycensus/reference/moe_prop.html
                                               total_rate_moes$total_pop, 
                                               total_rate_moes$total_raw_moe, 
                                               total_rate_moes$total_pop_moe)*100
    total_rate_moes <- total_rate_moes %>%
      select(geoid, geolevel, total_rate_moe)
    
    #### join these calculations back to x
    x <- left_join(x, total_rate_moes, by = c("geoid", "geolevel"))
    
    ## raced data (raw values don't need aggregation like total values do)
    
    ### calculate raced rates
    x$asian_rate <- ifelse(x$asian_pop <= 0, NA, x$asian_raw/x$asian_pop*100)
    x$black_rate <- ifelse(x$black_pop <= 0, NA, x$black_raw/x$black_pop*100)
    x$nh_white_rate <- ifelse(x$nh_white_pop <= 0, NA, x$nh_white_raw/x$nh_white_pop*100)
    x$latino_rate <- ifelse(x$latino_pop <= 0, NA, x$latino_raw/x$latino_pop*100)
    x$other_rate <- ifelse(x$other_pop <= 0, NA, x$other_raw/x$other_pop*100)
    x$pacisl_rate <- ifelse(x$pacisl_pop <= 0, NA, x$pacisl_raw/x$pacisl_pop*100)
    x$twoormor_rate <- ifelse(x$twoormor_pop <= 0, NA, x$twoormor_raw/x$twoormor_pop*100)
    x$aian_rate <- ifelse(x$aian_pop <= 0, NA, x$aian_raw/x$aian_pop*100)
    
    
    ### calculate moes for raced rates
    x$asian_rate_moe <- moe_prop(x$asian_raw,
                                 x$asian_pop,
                                 x$asian_raw_moe,
                                 x$asian_pop_moe)*100
    
    x$black_rate_moe <- moe_prop(x$black_raw,
                                 x$black_pop,
                                 x$black_raw_moe,
                                 x$black_pop_moe)*100
    
    x$nh_white_rate_moe <- moe_prop(x$nh_white_raw,
                                    x$nh_white_pop,
                                    x$nh_white_raw_moe,
                                    x$nh_white_pop_moe)*100
    
    x$latino_rate_moe <- moe_prop(x$latino_raw,
                                  x$latino_pop,
                                  x$latino_raw_moe,
                                  x$latino_pop_moe)*100
    
    x$other_rate_moe <- moe_prop(x$other_raw,
                                 x$other_pop,
                                 x$other_raw_moe,
                                 x$other_pop_moe)*100
    
    x$pacisl_rate_moe <- moe_prop(x$pacisl_raw,
                                  x$pacisl_pop,
                                  x$pacisl_raw_moe,
                                  x$pacisl_pop_moe)*100
    
    x$twoormor_rate_moe <- moe_prop(x$twoormor_raw,
                                    x$twoormor_pop,
                                    x$twoormor_raw_moe,
                                    x$twoormor_pop_moe)*100
    
    x$aian_rate_moe <- moe_prop(x$aian_raw,
                                x$aian_pop,
                                x$aian_raw_moe,
                                x$aian_pop_moe)*100
    
    
    ### Convert any NaN to NA
    x <- x %>% 
      mutate_all(function(x) ifelse(is.nan(x), NA, x))
    
    ### drop the total006-013 e and m columns and pop_moe cols
    x <- x %>%
      select(-starts_with("total0"), -ends_with("_pop_moe"))
    
  }
  
  if(endsWith(table_code, "b19301")) {
    
    names(x) <- gsub("001e", "_rate", names(x))
    names(x) <- gsub("001m", "_rate_moe", names(x))
  }
  
  if(endsWith(table_code, "b25003")) {  # HAVE ONLY EDITED THIS TABLE SO FAR
    # names(x) <- gsub("001e", "_pop", names(x))
    # names(x) <- gsub("001m", "_pop_moe", names(x))
    # 
    # names(x) <- gsub("002e", "_raw", names(x))
    # names(x) <- gsub("002m", "_raw_moe", names(x))
    
    x <- x %>% select(-contains("003")) %>%   # drop cols for renter hh's
      select(geoid, name, geolevel, everything())
    
    # pivot longer
    x_long <- x %>%
      pivot_longer(
        cols = -c(geoid, name, geolevel),
        names_to = c("ethnic_group", "line", "stat"),
        names_pattern = "^(.*?)(001|002)(e|m)$",
        values_to = "value"
      ) %>%
      mutate(
        measure = case_when(
          line == "001" & stat == "e" ~ "pop",
          line == "001" & stat == "m" ~ "pop_moe",
          line == "002" & stat == "e" ~ "raw",
          line == "002" & stat == "m" ~ "raw_moe"
        )
      ) %>%
      select(-line, -stat) %>%
      pivot_wider(
        names_from = measure,
        values_from = value
      )
    
    # calc raced rates
    x_long <- x_long %>%
      mutate(rate = ifelse(pop <= 0, NA, raw / pop * 100),
             rate_moe = moe_prop(raw, pop, raw_moe, pop_moe)*100)
    
  }
  
  if (startsWith(table_code, "s2802") | startsWith(table_code, "s2701")) {
    old_names <- colnames(x)[-(1:3)]
    new_names <- c("total_pop", "black_pop", "aian_pop", "asian_pop", "pacisl_pop",
                   "other_pop", "twoormor_pop", "latino_pop", "nh_white_pop", 
                   "total_raw", "black_raw", "aian_raw", "asian_raw", "pacisl_raw",
                   "other_raw", "twoormor_raw", "latino_raw", "nh_white_raw", 
                   "total_rate", "black_rate", "aian_rate", "asian_rate", "pacisl_rate",
                   "other_rate", "twoormor_rate", "latino_rate", "nh_white_rate", 
                   "total_pop_moe", "black_pop_moe", "aian_pop_moe", "asian_pop_moe", "pacisl_pop_moe",
                   "other_pop_moe", "twoormor_pop_moe", "latino_pop_moe", "nh_white_pop_moe",
                   "total_raw_moe", "black_raw_moe", "aian_raw_moe", "asian_raw_moe",
                   "pacisl_raw_moe", "other_raw_moe", "twoormor_raw_moe", "latino_raw_moe",
                   "nh_white_raw_moe", 
                   "total_rate_moe", "black_rate_moe", "aian_rate_moe", "asian_rate_moe",
                   "pacisl_rate_moe", "other_rate_moe", "twoormor_rate_moe", "latino_rate_moe",
                   "nh_white_rate_moe")
    x <- x %>%
      rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
  }
  
  if (startsWith(table_code, "s2301")) {
    old_names <- colnames(x)[-(1:3)]
    new_names <- c("total_pop", "black_pop", "aian_pop", "asian_pop", "pacisl_pop",
                   "other_pop", "twoormor_pop", "latino_pop", "nh_white_pop", "total_rate", 
                   "black_rate", "aian_rate", "asian_rate", "pacisl_rate", "other_rate", 
                   "twoormor_rate", "latino_rate", "nh_white_rate", "total_pop_moe", 
                   "black_pop_moe", "aian_pop_moe", "asian_pop_moe", "pacisl_pop_moe", 
                   "other_pop_moe", "twoormor_pop_moe", "latino_pop_moe", "nh_white_pop_moe", 
                   "total_rate_moe", "black_rate_moe", "aian_rate_moe", "asian_rate_moe", 
                   "pacisl_rate_moe", "other_rate_moe", "twoormor_rate_moe", "latino_rate_moe", 
                   "nh_white_rate_moe")
    x <- x %>%
      rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
    
    # Employment data doesn't include _raw or _raw_moe values - adding here using _pop * _rate columns
    x$total_raw <- ifelse(x$total_pop <= 0, NA, x$total_pop*x$total_rate/100)
    x$asian_raw <- ifelse(x$asian_pop <= 0, NA, x$asian_pop*x$asian_rate/100)
    x$black_raw <- ifelse(x$black_pop <= 0, NA, x$black_pop*x$black_rate/100)
    x$nh_white_raw <- ifelse(x$nh_white_pop <= 0, NA, x$nh_white_pop*x$nh_white_rate/100)
    x$latino_raw <- ifelse(x$latino_pop <= 0, NA, x$latino_pop*x$latino_rate/100)
    x$other_raw <- ifelse(x$other_pop <= 0, NA, x$other_pop*x$other_rate/100)
    x$pacisl_raw <- ifelse(x$pacisl_pop <= 0, NA, x$pacisl_pop*x$pacisl_rate/100)
    x$twoormor_raw <- ifelse(x$twoormor_pop <= 0, NA, x$twoormor_pop*x$twoormor_rate/100)
    x$aian_raw <- ifelse(x$aian_pop <= 0, NA, x$aian_pop*x$aian_rate/100)
    
    x$total_raw_moe <- sqrt(x$total_pop^2 * (x$total_rate_moe/100)^2 + (x$total_rate/100)^2 * x$total_pop_moe^2)
    
    x$asian_raw_moe <- sqrt(x$asian_pop^2 * (x$asian_rate_moe/100)^2 + (x$asian_rate/100)^2 * x$asian_pop_moe^2)
    
    x$black_raw_moe <- sqrt(x$black_pop^2 * (x$black_rate_moe/100)^2 + (x$black_rate/100)^2 * x$black_pop_moe^2)
    
    x$nh_white_raw_moe <- sqrt(x$nh_white_pop^2 * (x$nh_white_rate_moe/100)^2 + (x$nh_white_rate/100)^2 * x$nh_white_pop_moe^2)
    
    x$latino_raw_moe <- sqrt(x$latino_pop^2 * (x$latino_rate_moe/100)^2 + (x$latino_rate/100)^2 * x$latino_pop_moe^2)
    
    x$other_raw_moe <- sqrt(x$other_pop^2 * (x$other_rate_moe/100)^2 + (x$other_rate/100)^2 * x$other_pop_moe^2)
    
    x$pacisl_raw_moe <- sqrt(x$pacisl_pop^2 * (x$pacisl_rate_moe/100)^2 + (x$pacisl_rate/100)^2 * x$pacisl_pop_moe^2)
    
    x$twoormor_raw_moe <- sqrt(x$twoormor_pop^2 * (x$twoormor_rate_moe/100)^2 + (x$twoormor_rate/100)^2 * x$twoormor_pop_moe^2)
    
    x$aian_raw_moe <- sqrt(x$aian_pop^2 * (x$aian_rate_moe/100)^2 + (x$aian_rate/100)^2 * x$aian_pop_moe^2)
  }
  
  
  if (startsWith(table_code, "dp05")) {
    old_names <- colnames(x)[-(1:3)]
    new_names <- c("total_pop", "total_rate", "aian_pop", "pct_aian_pop", "pacisl_pop",
                   "pct_pacisl_pop", "latino_pop", "pct_latino_pop", "nh_white_pop", "pct_nh_white_pop",
                   "black_pop", "pct_black_pop", "asian_pop", "pct_asian_pop", "other_pop", "pct_other_pop",
                   "twoormor_pop", "pct_twoormor_pop", "total_pop_moe", "pct_total_pop_moe", "aian_pop_moe", "pct_aian_pop_moe", "pacisl_pop_moe",
                   "pct_pacisl_pop_moe", "latino_pop_moe", "pct_latino_pop_moe", "nh_white_pop_moe", "pct_nh_white_pop_moe",
                   "black_pop_moe", "pct_black_pop_moe", "asian_pop_moe", "pct_asian_pop_moe", "other_pop_moe", "pct_other_pop_moe",
                   "twoormor_pop_moe", "pct_twoormor_pop_moe")
    x <- x %>%
      rename_with(~ new_names[which(old_names == .x)], .cols = old_names)
    
    # drop total_rate - is just a copy of total_pop & drop _moe values (get clarification: aren't included in arei_race_county_2021)
    x <- dplyr::select(x, -ends_with("_moe"), -ends_with("_rate"))
  }
  
  # Finish up data cleaning
    # make colnames lower case
    colnames(x_long) <- tolower(colnames(x_long))
    
    # Clean geo names
    x_long$name <- gsub(", California", "", x_long$name)
    x_long$name <- gsub(" County", "", x_long$name)
    x_long$name <- gsub(" city", "", x_long$name)
    x_long$name <- gsub(" town", "", x_long$name)
    x_long$name <- gsub(" CDP", "", x_long$name)
    x_long$name <- str_remove(x_long$name,  "\\s*\\(.*\\)\\s*")
    x_long$name <- gsub("; California", "", x_long$name)
  
  ### Coefficient of Variation (CV) CALCS ###

  ### calc cv's
  ## Calculate CV values for all rates - store in columns as cv_[race]_rate
  if (!is.na(cv_threshold)){
    x_long$rate_cv <- ifelse(x_long$rate==0, NA, x_long$rate_moe/1.645/x_long$rate*100)
  }
  
  df <- x_long %>%
    filter(!if_all(where(is.numeric), is.na))  # drop rows where ALL numeric values are NA
  
  ############## PRE-CALCULATION POPULATION AND/OR CV CHECKS ##############
  ############## PRE-CALCULATION POPULATION AND/OR CV CHECKS ##############
  if (!is.na(pop_threshold) & is.na(cv_threshold)) {
    # if pop_threshold ex_longists and cv_threshold is NA, do pop check but no CV check (doesn't apply to any at this time, may need to add _raw screens later.)
    ## Screen out low populations
    df$rate <- ifelse(df$pop < pop_threshold, NA, df$rate)
    
  } else if (is.na(pop_threshold) & !is.na(cv_threshold)){
    # if pop_threshold is NA and cv_threshold ex_longists, check cv only (i.e. only B19301). As of now, the only table that uses this does not have _raw values, may need to add _raw screens later.
    ## Screen out rates with high CVs
    df$rate <- ifelse(df$rate_cv > cv_threshold, NA, df$rate)
    
  } else if (!is.na(pop_threshold) & !is.na(cv_threshold)){
    # if pop_threshold ex_longists and cv_threshold ex_longists, check population and cv (i.e. B25003, S2301, S2802, S2701, B25014)
    ## Screen out rates with high CVs and low populations
    df$rate <- ifelse(df$rate_cv > cv_threshold, NA, ifelse(df$pop < pop_threshold, NA, df$rate))
    df$raw <- ifelse(df$rate_cv > cv_threshold, NA, ifelse(df$pop < pop_threshold, NA, df$raw))
    
  } else {
    # Only DP05 should hit this condition
    # Will use to change population values < 0 to NA (negative values are Census annotations)
    pop_columns <- colnames(dplyr::select(df, ends_with("_pop")))
    df[,pop_columns] <- sapply(df[,pop_columns], function(x_long) ifelse(x_long<0, NA, x_long))
    
  }
  
  df_wide <- pivot_wider(df,
                         names_from = ethnic_group,
                         values_from = c(pop, pop_moe, raw, raw_moe, rate, rate_moe, rate_cv),
                         names_glue = "{ethnic_group}_{.value}")
  
  df_wide$total_rate <- NA   # add dummy total_rate col so RC_Functions work as-is
  
  return(df_wide)
}

