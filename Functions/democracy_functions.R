## Functions used for RC Democracy indicators ##

# metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov22.pdf


# race/ethnicity codes

# All AIAN
all_aian <- c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24")

# All NHPI
all_nhpi <- c("5", "9", "12", "14", "15", "18", "20", "21", "24")

# NH Two or More
nh_twoormor <-c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26")



# Download CPS Voting Supplement and Sent to Postgres ---------------------
get_cps_supp <- function(metadata, url, cps_yr, varchar_cols) {
  if(!file.exists(file)) {
    print("Downloading data to W drive now.")
    download.file(url=url, destfile=file) 
    df <- read_csv(file, 
                   col_types = cols(
                     gestfips = "character",
                     gtcbsa = "character",
                     gtco = "character",
                     gtcsa = "character"))
    
    print("Raw data saved to W:\\Data\\Democracy\\Current Population Survey Voting and Registration\\")
  } else { 
    print("Data already downloaded to W drive.") }
    
    ## Define postgres schema, table name, table comment, data source for rda_shared_data table
    table_schema <- "democracy"
    table_name <- paste0("cps_voting_supplement_", tail(cps_yr, n=1))
    
    table_comment_source <- "NOTE: Geoid fields (gestfips, gtcbsa, gtcco, tco, gtcsa) are missing leading zeroes"
    table_source <- paste0("CPS Voting Supplement data downloaded from https://www.census.gov/data/datasets/time-series/demo/cps/cps-supp_cps-repwgt.html. Metadata here: ", metadata)
    
    print("Prepping data for postgres table now.")
    df <- read_csv(file = url, na = c("*", ""), show_col_types = FALSE)
    names(df) <- tolower(names(df)) # make col names lowercase
    df <- df %>% filter(gestfips == 6)
    
    ##  WRITE TABLE TO POSTGRES DB ##
    # make character vector for field types in postgres table
    charvect = rep('numeric', dim(df)[2])

    # add names to the character vector - confirm using metadata
    names(charvect) <- colnames(df)
    charvect[varchar_cols] <- "varchar"  # set user-selected cols as varchar, not numeric
   
    
    # send table to postgres
    dbWriteTable(con2, Id(schema = table_schema, table = table_name), df,
                 overwrite = FALSE, row.names = FALSE,
                 field.types = charvect)
    
    # add index
    idx_query <- paste0("CREATE index cps_voting_supplement_", tail(cps_yr, n=1), "_hrhhid_idx on democracy.cps_voting_supplement_", tail(cps_yr, n=1), "(hrhhid);")
    dbSendQuery(con2, idx_query)
    
    # write comment to table, and the first three fields that won't change.
    table_comment <- paste0("COMMENT ON TABLE ", table_schema, ".", table_name, " IS 'Created on ", Sys.Date(), ".", table_comment_source, ". ", table_source, ".';")
    dbSendQuery(con2, table_comment)
    print(paste0("Data exported to postgres as ", table_schema, ".", table_name, "."))
  
}


##DATA CLEANING FUNCTION: Clean geoids and create numeric wgt column -------------------------------------------------------------------------
##### see p31-33 of metadata for more: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov20.pdf
clean_cps <- function(x) {
  
  #make column names lowercase
  colnames(x) <- tolower(colnames(x))

  #filter for just CA data
  x <- filter(x, gestfips %in% c('6', '06'))
  
  x$gestfips <- ifelse (x$gestfips == '6', paste("0",x$gestfips, sep = ""), x$gestfips)
  x$gtcbsa <- ifelse (nchar(x$gtcbsa) == 1, paste("0000",x$gtcbsa, sep = ""), x$gtcbsa)
  x$gtcco <- ifelse (x$gtco == "0", NA, x$gtco) 
  x$gtco <- ifelse (nchar(x$gtco) == 1, paste("00",x$gtco, sep = ""), x$gtco)
  x$gtco <- ifelse (nchar(x$gtco) == 2, paste("0",x$gtco, sep = ""), x$gtco)
  x$gtco <- ifelse(x$gtco != '06', paste0("06",x$gtco), x$gtco)
  x$gtcsa <- ifelse (nchar(x$gtcsa) == 1, paste("00",x$gtcsa, sep = ""), x$gtcsa)
  
  ## divide PWSSWGT by 1,000 because the data dictionary specifies this is 4 implied decimal places. Chris already did this for 2012 and 2016 data
  if(!"pwsswgtnum" %in% colnames(x))
  {
    x$pwsswgtnum <- x$pwsswgt/10000 
  }
  
    #correct/make column types consistent across data years
  x$pes1 <- as.character(x$pes1)
  x$hrintsta <- as.character(x$hrintsta)
  x$prpertyp <- as.character(x$prpertyp)
  x$prtage <- as.double(x$prtage)
  
return(x)  
}


## Registered by County -----------------
registered_by_county <- function(data) {       # pes1 == 1 did vote / pes1 == 2 did NOT vote / pes2 == 1 -> did not vote but is registered to vote #
  # total
  data %>% 
    filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", !is.na(gtco)
    ) %>%
    group_by(
      gtco # group by county code
    ) %>%
    summarize(num_total_reg = sum(pwsswgtnum), count_total_reg = n() ) %>%
    
    ### merge with Latino
    left_join(
      
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1", !is.na(gtco) # Hispanic or Latino 
        ) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_latino_reg = sum(pwsswgtnum), count_latino_reg = n() )) %>%
    
    ### merge with NH White
    left_join(
      
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1", !is.na(gtco) # non-hispanic White
        ) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_white_reg = sum(pwsswgtnum), count_nh_white_reg = n() )) %>%
    
    ### merge with NH Black
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2", !is.na(gtco) # non-hispanic Black
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_black_reg = sum(pwsswgtnum), count_nh_black_reg = n() )) %>%
    
    ### merge with all AIAN
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_aian), !is.na(gtco) # AIAN
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_aian_reg = sum(pwsswgtnum), count_aian_reg = n() )) %>%
    
    ### merge with NH Asian
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4", !is.na(gtco) # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_asian_reg = sum(pwsswgtnum), count_nh_asian_reg = n() )) %>%
    
    ### merge with all PACISL
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_nhpi), !is.na(gtco) # PACISL
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_pacisl_reg = sum(pwsswgtnum), count_pacisl_reg = n() )) %>%
    
    ### merge with NH Two or more races
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace %in% c(nh_twoormor), !is.na(gtco) # NH Two or More
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_twoormor_reg = sum(pwsswgtnum), count_nh_twoormor_reg = n() ))   
  
}

## Registered by State ---------------------------------------------
registered_by_state <- function(data) {       # pes2 == 1 -> registered to vote #
  # total
  data %>% 
    filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18"
    ) %>%
    group_by(
      gestfips # group by state
    ) %>%
    summarize(num_total_reg = sum(pwsswgtnum), count_total_reg = n() ) %>%
    
    ###### merge with Latino
    left_join(
      
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1" # Hispanic or Latino 
        ) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_latino_reg = sum(pwsswgtnum), count_latino_reg = n() )) %>%
    
    ### merge with NH white
    left_join(
      
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1" # non-hispanic White
        ) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_white_reg = sum(pwsswgtnum), count_nh_white_reg = n() )) %>%
    
    #### merge with NH black
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2"# non-hispanic Black
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_black_reg = sum(pwsswgtnum), count_nh_black_reg = n() )) %>%
    
    #### merge with All AIAN
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_aian) # AIAN
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_aian_reg = sum(pwsswgtnum), count_aian_reg = n() )) %>%
    
    ### merge with NH Asian
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4" # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_asian_reg = sum(pwsswgtnum), count_nh_asian_reg = n() )) %>%
    
    ### merge with all PACISL
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_nhpi) # PACISL
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_pacisl_reg = sum(pwsswgtnum), count_pacisl_reg = n() )) %>%
    
    ### merge with nh two or more races
    left_join(
      data %>% 
        filter( (pes1 == "1" | (pes1 == "2" & pes2 == "1")), hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace %in% c(nh_twoormor) # Two or More
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_twoormor_reg = sum(pwsswgtnum), count_nh_twoormor_reg = n() )) 
  
  
}

## Voted by County ---------------------------------------------
voted_by_county <- function(data) {     # pes1 == 1 -> DID vote / pes1 == 2 -> Did NOT vote #
  # total
  data %>% 
    filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", !is.na(gtco)
    ) %>%
    group_by(
      gtco # group by county code
    ) %>%
    summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
    
    ### merge with Latino
    left_join(
      
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1", !is.na(gtco) # Hispanic or Latino 
        ) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
    
    ### merge with nh White
    left_join(
      
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1", !is.na(gtco) # non-hispanic White
        ) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
    
    ### merge with nh Black
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2", !is.na(gtco) # non-hispanic Black
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
    
    ### merge with all AIAN
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_aian), !is.na(gtco) # AIAN
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
    
    ### merge with nh Asian
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4", !is.na(gtco) # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
    
    
    ### merge with all PacIsl
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_nhpi), !is.na(gtco) # PACISL
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
    
    ### merge with nh Two or more races
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c(nh_twoormor), !is.na(gtco) # Two or More
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))
  
}

## Voted by State -------------------------------------------
voted_by_state <- function(data, y) {       # pes1 == 1 -> DID vote / pes1 == 2 -> Did NOT vote #
  # total
  data %>% 
    filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18"
    ) %>%
    group_by(
      gestfips # group by state
    ) %>%
    summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
    
    
    ###### merge with Latino
    left_join(
      
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1" # Hispanic or Latino 
        ) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
    
    ### merge with nh white
    left_join(
      
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1" # non-hispanic White
        ) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
    
    #### merge with nh black
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2"# non-hispanic Black
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
    
    #### merge with All AIAN
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_aian)# AIAN
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
    
    ### merge with nh Asian
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4" # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
    
    ### merge with all PACISL
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c(all_nhpi) # PACISL
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
    
    ### merge with nh two or more races
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c(nh_twoormor) # Two or More
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))
  
}

## Voting Age by County -------------------------------------
voting_age_county <- function(data) {
  # Total
  data %>%
    mutate(prtage = as.numeric(prtage)) %>%
    filter( prtage >=18, prcitshp != "5", !is.na(gtco)) %>%
    group_by(
      gtco #group by county code
    ) %>%
    summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
    
    
    # Hispanic/Latino
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "1", !is.na(gtco)) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
    
    
    # NH WHITE
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1", !is.na(gtco)) %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Black
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2", !is.na(gtco)) %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### All AIAN
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c(all_aian), !is.na(gtco)) # AIAN
      %>% 
        group_by(gtco) %>% #group by county code
        summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
    
    ### NH Asian
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4", !is.na(gtco)) %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### All PacIsl
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c(all_nhpi), !is.na(gtco)) %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Two or more
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c(nh_twoormor), !is.na(gtco)) %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
  
}

## Voting Age by State --------------------------------------
voting_age_state <- function(data) {
  
  ### Total
  data %>%
    mutate(prtage = as.numeric(prtage)) %>%
    filter( prtage >=18, prcitshp != "5") %>%
    group_by(
      gestfips #group by state
    ) %>%
    summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
    
    
    # Hispanic/Latino
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "1") %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
    
    
    # NH WHITE
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1") %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Black
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2") %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### All AIAN
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c(all_aian)) # AIAN
      %>% 
        group_by(gestfips) %>% #group by state
        summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Asian
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4") %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### ALL PacIsl
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c(all_nhpi)) %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Two or more
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c(nh_twoormor)) %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
  
}
