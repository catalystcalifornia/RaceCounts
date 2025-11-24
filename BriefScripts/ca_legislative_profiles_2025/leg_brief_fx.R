### Functions used in get_briefs_data.R


prep_dist_descr1 <- function(x, subgeo_name, overlap) {  
  # Groups geos overlapping districts into 'all of' or 'portion of' based on pct of overlap, then orders/ranks each group alphabetically
  # x is the dataframe containing district geoids and overlapping counties (must have a leg_id and geolevel cols) 
  # subgeo_name is the col containing names of counties that overlap the districts, do NOT use quotation marks
  # overlap is the col containing the % (decimal form) of county that overlaps district, do NOT use quotation marks
  dist_descr2 <- x %>%
    arrange({{subgeo_name}}) %>%       # order subgeo_name alphabetically
    mutate(portion = case_when({{overlap}} == 1 ~ 'all of',     # when district contains 100% of geo, then assign 'all of'
                               {{overlap}} < 1 ~ 'portion of',  # when district contains less than 100% of geo, then assign 'portion of'
                               .default = 'na')) %>%
    group_by(leg_id, portion) %>%
    mutate(group_order = paste0("group_", rank({{subgeo_name}}, ties.method = "first")), # number the counties grouped by district and portion
           count = n()) %>%  														 # count of counties grouped by district+portion
    ungroup() %>%
    mutate(num_count = str_count(group_order, "[0-9]"),								 # count of numeric characters in group_order string
           grp1 = str_sub(group_order,1,6),
           grp2 = str_sub(group_order,7,7)) %>%
    mutate(group_order_ = case_when(num_count == 1 ~ paste0(grp1,"0",grp2),			 # add leading zeroes, so can correctly group and sort county names
                                    .default = group_order)) %>%
    arrange(leg_id, portion, group_order_)
  
  return(dist_descr2)
}


prep_dist_descr2 <- function(x, subgeo_name) {  
  # Generates 1 geographic description for 'all of' counties per district and 1 for 'portion of' counties per district
  # Use for COUNTIES only!
  # x is the dataframe
  # subgeo_name is the col containing names of counties that overlap the districts, DO use quotation marks
  descr_wide <- x %>% 
    select(leg_id, subgeo_name, geolevel, portion, group_order_) %>%      	   # pivot long table back to wide
    pivot_wider(names_from=c(group_order_), values_from=subgeo_name) %>%
    unite(subgeo_names, starts_with("group"), na.rm = TRUE, sep=", ") %>%	   # concatenate county names grouped by district+portion
    # format 'Portion(s) of' descr
    mutate(clean_names = sub(",([^,]*)$", " and\\1", subgeo_names),   # sub 'and' for the comma where there are 2 counties intersecting the district
           descr = case_when(portion == 'portion of' & !grepl("and", clean_names, ignore.case = FALSE) ~ paste0("a ", portion, " ", clean_names, " County"), # add 'a' when there is only 1 partial county
                            (portion == 'portion of' & grepl("and", clean_names, ignore.case = FALSE)) ~ paste0("portions of ", clean_names, " counties"),   # sub 'portions of' for 'portion of' when there are 2+ partial counties
                            .default = paste0(portion, " ", clean_names))) %>%			# else, concatenate portion and subgeo_names fields
    # format 'All of' descr
    mutate(descr = case_when(portion == 'all of' & !grepl("and", clean_names, ignore.case = FALSE) ~ paste0(descr, " County"),		# 
                             (portion == 'all of' & grepl("and", clean_names, ignore.case = FALSE)) ~ paste0(descr, " counties"),
                             .default = descr))
  
  return(descr_wide)
}


prep_dist_descr3 <- function(x) {  
  # Creates 1 geographic description per district (final_descr col)
  # Does NOT include the more detailed descriptions of urban Senate Districts. Those are added after this step.
  # final_descr is used to build df_final$Characteristics
  descr_final <- x %>%
    group_by(leg_id, geolevel) %>%
    summarise(final_descr = paste(descr, collapse = " and "))
  
  return(descr_final)
}