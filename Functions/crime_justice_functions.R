## Functions used for RC Crime & Justice indicators ##


# RIPA race-ethnicity codes. source: W:\Data\Crime and Justice\CA DOJ\RIPA Stop Data\RIPA Dataset Read Me 2022 - 20241112.pdf
latino_code <- "1"
nh_code <- "0"
nh_asian_code <- "1"
nh_black_code <- "2"
nh_white_code <- "7"
nh_multiracial_code <- "8"
aian_pacisl_swanasa_code <- "1"
nh_aian_code<-"5"
nh_nhpi_code<-"6"


## RIPA Stops by Race -----------------
# geoid: name of column in df you want to group_by, do not use ""
stops_by_race <- function(x, geoid, geoid_col) {     
  # total
y <- x %>% 
    group_by({{geoid}}, data_yrs) %>%
    dplyr::summarise(total_stops = n()) %>%
    mutate(total_stops_wt1 = total_stops/data_yrs) %>%
    group_by({{geoid}}) %>% 
    dplyr::summarize(total_stops_wt = sum(total_stops_wt1),
                     total_stops = sum(total_stops)) %>%
    
    ### merge with Latino
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == latino_code) %>% # Hispanic or Latino 
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(latino_stops = n()) %>%
        mutate(latino_stops_wt1 = latino_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(latino_stops_wt = sum(latino_stops_wt1),
                         latino_stops = sum(latino_stops))) %>%
    
    ### merge with NH Asian
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_asian_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(nh_asian_stops = n()) %>%
        mutate(nh_asian_stops_wt1 = nh_asian_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(nh_asian_stops_wt = sum(nh_asian_stops_wt1),
                         nh_asian_stops = sum(nh_asian_stops))) %>%
      
    ### merge with NH Black
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_black_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(nh_black_stops = n()) %>%
        mutate(nh_black_stops_wt1 = nh_black_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(nh_black_stops_wt = sum(nh_black_stops_wt1),
                         nh_black_stops = sum(nh_black_stops))) %>%
      
    ### merge with NH White
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_white_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(nh_white_stops = n()) %>%
        mutate(nh_white_stops_wt1 = nh_white_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(nh_white_stops_wt = sum(nh_white_stops_wt1),
                         nh_white_stops = sum(nh_white_stops))) %>%
      
    ### merge with AIAN
    left_join(      
      x %>% 
        filter(rae_native_american == aian_pacisl_swanasa_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(aian_stops = n()) %>%
        mutate(aian_stops_wt1 = aian_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(aian_stops_wt = sum(aian_stops_wt1),
                         aian_stops = sum(aian_stops))) %>%
      
    ### merge with PACISL
    left_join(      
      x %>% 
        filter(rae_pacific_islander == aian_pacisl_swanasa_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(pacisl_stops = n()) %>%
        mutate(pacisl_stops_wt1 = pacisl_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(pacisl_stops_wt = sum(pacisl_stops_wt1),
                         pacisl_stops = sum(pacisl_stops))) %>%
      
    ### merge with NH-AIAN
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_aian_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(nh_aian_stops = n()) %>%
        mutate(nh_aian_stops_wt1 = nh_aian_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(nh_aian_stops_wt = sum(nh_aian_stops_wt1),
                         nh_aian_stops = sum(nh_aian_stops))) %>%
      
    ### merge with NH-PACISL
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_nhpi_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(nh_pacisl_stops = n()) %>%
        mutate(nh_pacisl_stops_wt1 = nh_pacisl_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(nh_pacisl_stops_wt = sum(nh_pacisl_stops_wt1),
                         nh_pacisl_stops = sum(nh_pacisl_stops))) %>%
      
    ### merge with SWANASA
    left_join(      
      x %>% 
        filter(rae_middle_eastern_south_asian == aian_pacisl_swanasa_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(swanasa_stops = n()) %>%
        mutate(swanasa_stops_wt1 = swanasa_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(swanasa_stops_wt = sum(swanasa_stops_wt1),
                         swanasa_stops = sum(swanasa_stops))) %>%
      
        ### merge with NH Multiracial
    left_join(      
      x %>% 
        filter(rae_hispanic_latino == nh_code & rae_full == nh_multiracial_code) %>%
        group_by({{geoid}}, data_yrs) %>%
        dplyr::summarise(nh_twoormor_stops = n()) %>%
        mutate(nh_twoormor_stops_wt1 = nh_twoormor_stops/data_yrs) %>%
        group_by({{geoid}}) %>% 
        dplyr::summarize(nh_twoormor_stops_wt = sum(nh_twoormor_stops_wt1),
                         nh_twoormor_stops = sum(nh_twoormor_stops)))

return (y)  
}


