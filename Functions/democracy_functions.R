## Functions used for RC Democracy indicators ##

# metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov22.pdf


# race/ethnicity codes

# All AIAN
all_aian <- c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24")

# All NHPI
all_nhpi <- c("5", "9", "12", "14", "15", "18", "20", "21", "24")

# NH Two or More
nh_twoormor <-c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26")


##DATA CLEANING FUNCTION: Clean geoids and create numeric wgt column -------------------------------------------------------------------------
##### see p31-33 of metadata for more: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov20.pdf
clean_cps <- function(x) {
  ## use code like unique(cps_list[[3]][[1]]) or unique(cps_list2[[1]][[1]]) to ensure all $gtco fixes etc. are covered in this fx
  x$gestfips <- ifelse(x$gestfips == '6', paste0("0",x$gestfips), x$gestfips)
  x$gtcbsa <- ifelse(nchar(x$gtcbsa) == 1, paste0("0000",x$gtcbsa), x$gtcbsa)
  x$gtco <- ifelse((x$gtco == "0" | x$gtco == "000"), NA, str_pad(x$gtco, 3, pad = "0"))  # make all county codes 3 digits by padding with leading zeroes
  x$gtco <- ifelse(!is.na(x$gtco), paste0("06",x$gtco), x$gtco)
  x$gtcsa <- ifelse(nchar(x$gtcsa) == 1, paste0("00",x$gtcsa), x$gtcsa)
  
  ## divide PWSSWGT by 1,000 because the data dictionary specifies this is 4 implied decimal places
  x$pwsswgtnum <- as.numeric(x$pwsswgt)/10000
  
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
