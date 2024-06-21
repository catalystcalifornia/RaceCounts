## Functions used for RC Crime & Justice indicators ##


# RIPA race-ethnicity codes. source: https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-01/RIPA%20Dataset%20Read%20Me%202022.pdf 
latino_code <- "1"
nh_code <- "0"
nh_asian_code <- "1"
nh_black_code <- "2"
nh_white_code <- "7"
nh_multiracial_code <- "8"
aian_pacisl_swanasa_code <- "1"


## RIPA Stops by State -----------------
stops_by_state <- function(x) {     
  # total
  x %>% 
    group_by(state_id) %>%
    dplyr::summarize(total_stops = n()) %>%
    
    ### merge with Latino
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == latino_code) %>% # Hispanic or Latino 
        group_by(state_id) %>%
        dplyr::summarize(latino_stops = n())) %>%
    
    ### merge with NH Asian
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_asian_code) %>%
        group_by(state_id) %>%
        dplyr::summarize(nh_asian_stops = n())) %>%
    
    ### merge with NH Black
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_black_code) %>%
        group_by(state_id) %>%
        dplyr::summarize(nh_black_stops = n() )) %>%
    
    ### merge with NH White
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_white_code) %>%
        group_by(state_id) %>%
        dplyr::summarize(nh_white_stops = n() )) %>%
    
    ### merge with AIAN
    left_join(      
      x %>% 
        filter(rae_native_american == aian_pacisl_swanasa_code) %>%
        group_by(state_id) %>%
        dplyr::summarize(aian_stops = n())) %>%
    
    ### merge with PACISL
    left_join(      
      x %>% 
        filter(rae_pacific_islander == aian_pacisl_swanasa_code) %>%
        group_by(state_id) %>%
        dplyr::summarize(pacisl_stops = n())) %>%
    
    ### merge with SWANASA
    left_join(      
      x %>% 
        filter(rae_middle_eastern_south_asian == aian_pacisl_swanasa_code) %>%
        group_by(state_id) %>%
        dplyr::summarize(swanasa_stops = n())) %>%

  ### merge with NH Multiracial
  left_join(      
    x %>% 
      filter(rae_hispanic_latino == nh_code & rae_full == nh_multiracial_code) %>%
      group_by(state_id) %>%
      dplyr::summarize(nh_twoormor_stops = n())) 		
    
}


## RIPA Stops by County -----------------
stops_by_county <- function(x) {     
  # total
  x %>% 
    group_by(county) %>%
    dplyr::summarize(total_stops = n()) %>%
    
    ### merge with Latino
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == latino_code) %>% # Hispanic or Latino 
        group_by(county) %>%
        dplyr::summarize(latino_stops = n())) %>%
    
    ### merge with NH Asian
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_asian_code) %>%
        group_by(county) %>%
        dplyr::summarize(nh_asian_stops = n())) %>%
    
    ### merge with NH Black
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_black_code) %>%
        group_by(county) %>%
        dplyr::summarize(nh_black_stops = n() )) %>%
    
    ### merge with NH White
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_white_code) %>%
        group_by(county) %>%
        dplyr::summarize(nh_white_stops = n() )) %>%
    
    ### merge with AIAN
    left_join(      
      x %>% 
        filter(rae_native_american == aian_pacisl_swanasa_code) %>%
        group_by(county) %>%
        dplyr::summarize(aian_stops = n())) %>%
    
    ### merge with PACISL
    left_join(      
      x %>% 
        filter(rae_pacific_islander == aian_pacisl_swanasa_code) %>%
        group_by(county) %>%
        dplyr::summarize(pacisl_stops = n())) %>%
    
    ### merge with SWANASA
    left_join(      
      x %>% 
        filter(rae_middle_eastern_south_asian == aian_pacisl_swanasa_code) %>%
        group_by(county) %>%
        dplyr::summarize(swanasa_stops = n())) %>%

        ### merge with NH Multiracial
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_multiracial_code) %>%
        group_by(county) %>%
        dplyr::summarize(nh_twoormor_stops = n())) 	
  
}


## RIPA Stops by Agency (city) -----------------
stops_by_agency <- function(x) {     
  # total
  x %>% 
    group_by(agency_name_new) %>%
    dplyr::summarize(total_stops = n()) %>%
    
    ### merge with Latino
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == latino_code) %>% # Hispanic or Latino 
        group_by(agency_name_new) %>%
        dplyr::summarize(latino_stops = n())) %>%
    
    ### merge with NH Asian
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_asian_code) %>%
        group_by(agency_name_new) %>%
        dplyr::summarize(nh_asian_stops = n())) %>%
    
    ### merge with NH Black
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_black_code) %>%
        group_by(agency_name_new) %>%
        dplyr::summarize(nh_black_stops = n() )) %>%
    
    ### merge with NH White
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_white_code) %>%
        group_by(agency_name_new) %>%
        dplyr::summarize(nh_white_stops = n() )) %>%
    
    ### merge with AIAN
    left_join(      
      x %>% 
        filter(rae_native_american == aian_pacisl_swanasa_code) %>%
        group_by(agency_name_new) %>%
        dplyr::summarize(aian_stops = n())) %>%
    
    ### merge with PACISL
    left_join(      
      x %>% 
        filter(rae_pacific_islander == aian_pacisl_swanasa_code) %>%
        group_by(agency_name_new) %>%
        dplyr::summarize(pacisl_stops = n())) %>%
    
    ### merge with SWANASA
    left_join(      
      x %>% 
        filter(rae_middle_eastern_south_asian == aian_pacisl_swanasa_code) %>%
        group_by(agency_name_new) %>%
        dplyr::summarize(swanasa_stops = n())) %>%
    
    ### merge with NH Multiracial
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_multiracial_code) %>%
        group_by(agency_name_new) %>%
        dplyr::summarize(nh_twoormor_stops = n())) 		
  
}