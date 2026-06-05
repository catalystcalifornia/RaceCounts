#### MOSAIC LAC Summary Table: Indicator for Report Page ####

#install packages if not already installed
list.of.packages <- c("tidyverse","RPostgres","sf","dplyr","plyr","usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

for(pkg in list.of.packages){ 
  library(pkg, character.only = TRUE) 
} 

# remove exponentiation
options(scipen = 100) 

# Load PostgreSQL driver and databases --------------------------------------------------
# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")
con2 <- connect_to_db("mosaic")


# Update each year --------------------------------------------------------
curr_schema <- 'v7'
rc_yr <- '2025'
ind_name <- 'overcrowded'
county_ <- '06037'
qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\MOSAIC\\QA_Summary_Table.docx"

# used later to populate 'type' column
races <- c('latino', 'aian', 'black', 'white', 'twoormor', 'other', 'swana', 'nh_aian', 'nh_black', 'nh_white', 'nh_twoormor', 'nh_other', 'nh_swana')

## get MOSAIC pop data #####
asian_pop <- dbGetQuery(con2, paste0("select * from ", curr_schema, ".aa_pop_b02018 where geolevel IN ('county')")) # import county & state records only
nhpi_pop <- dbGetQuery(con2, paste0("select * from ", curr_schema, ".nhpi_pop_b02019 where geolevel IN ('county')")) # import county & state records only

## get MOSAIC county indicator tables #####
table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_county_%';")
m_list <- dbGetQuery(con2, table_list) %>% dplyr::rename('table' = 'table_name') %>%
  filter(grepl(ind_name, table))

indicator_list <- m_list[order(m_list$table), ] # alphabetize list of index tables which transforms into character from list, needed to format list correctly for next steps
indicator_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", indicator_list), indicator_list), DBI::dbGetQuery, conn = con2) # import tables from postgres

# filter for LAC
indicator_tables <- lapply(indicator_tables, function(x) x %>% filter(county_id == county_))
                           
## split into asian and nhpi ##
asian_tables <- indicator_tables[grepl("asian", names(indicator_tables))]

nhpi_tables <- indicator_tables[grepl("nhpi", names(indicator_tables))]

## get RC state indicator tables #####
table_list_rc <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_county_%' AND table_name LIKE '%", rc_yr, "a';")
rc_list <- dbGetQuery(con, table_list_rc) %>% dplyr::rename('table' = 'table_name') %>%
  filter(grepl(ind_name, table))

indicator_list_rc <- rc_list[order(rc_list$table), ] # alphabetize list of index tables which transforms into character from list, needed to format list correctly for next steps
indicator_tables_rc <- lapply(setNames(paste0("select * from ", curr_schema, ".", indicator_list_rc), indicator_list_rc), DBI::dbGetQuery, conn = con) # import tables from postgres

# filter for LAC
indicator_tables_rc <- lapply(indicator_tables_rc, function(x) x %>% filter(county_id == county_))

# format and clean tables####
clean_tables <- function(data_list, race){
  
  indicator_tables_clean <- lapply(data_list, function(x) x %>%
                                     select(county_id, county_name, ends_with(c("raw", "rate"))))
  
  indicator_tables_clean <- map2(indicator_tables_clean, names(indicator_tables_clean), ~ mutate(.x, indicator_ = .y)) # create column with indicator name
  # indicator_tables_clean <- lapply(indicator_tables_clean, function(x) x %>%
  # mutate(indicator1 = gsub("asian_|nhpi_", '', indicator_),  # step 1
  #        indicator2 = substring(indicator1, 6),              # step 2
  #        indicator = gsub(paste0("_state_",rc_yr), '', indicator2)) %>% # step 3
  # select(-c(indicator_, indicator1, indicator2)))  # drop interim cleaning steps
  
  data_df <- as.data.frame(do.call(cbind, indicator_tables_clean))  # convert list to df
  
  # clean col names #
  data_df <- data_df %>% 
    rename_with(~ sub(paste0("^",race,"_"), "", .x))   # ^ anchors to start of string only          # step 1
  colnames(data_df) <- substring(colnames(data_df), 6)                         # step 2
  colnames(data_df) <- gsub(paste0("_state_",rc_yr), '', colnames(data_df))    # step 3
  
  # step 4 - drop dupe id/name cols
  colnames(data_df) <- ifelse(grepl("_id|_name", colnames(data_df)), sub("^.*\\.", "", colnames(data_df)), colnames(data_df))
  data_df = data_df[,!duplicated(names(data_df))]
  data_df = data_df %>% select(-contains('indicator_'))  # drop indicator name columns
  
  # step 5 - pivot_longer
  df_long <- data_df %>%
    tidyr::pivot_longer(
      cols = -c(county_id, county_name),
      names_to = "name",
      values_to = "value") %>%
    tidyr::separate(name, 
                    into = c("topic", "group_metric"), 
                    sep = "\\.") %>%
    tidyr::separate(group_metric, 
                    into = c("subgroup", "metric"), 
                    sep = "_(?=rate$|raw$)") %>%
    tidyr::pivot_wider(
      names_from = metric,
      values_from = value
    )
  
  return(df_long)
}

# clean mosaic data
asian_clean <- clean_tables(asian_tables, 'asian') %>%
  mutate(topic = ind_name,
         original_group = subgroup) %>%
  mutate(
    type = ifelse(original_group %in% c("asian", "nh_asian"), 'group', 'subgroup')) %>%
  mutate(
    type = case_when(
      original_group %in% races ~ 'race',
      original_group %in% c("pacisl", "nh_pacisl") ~ 'race',
      original_group == 'total' ~ 'total',
      TRUE ~ type))

# check for race groups with aoic data - where aoic and alone exist for the same race and indicator, keep only the aoic data
asian_clean <- asian_clean %>%
  dplyr::group_by(topic, type) %>%
  # check if any indicators use aoic
  dplyr::mutate(
    has_aoic = any(grepl("aoic", original_group))) %>%
  dplyr::ungroup() %>%
  # keep only 'aoic' rows when an indicator has alone and aoic rows for same group
  dplyr::filter(
    type != "subgroup" |
      (type == "subgroup" & (!has_aoic | grepl("aoic", original_group)))) %>%
  dplyr::select(-has_aoic)


#clean mosaic data
nhpi_clean <- clean_tables(nhpi_tables, 'nhpi') %>%
  mutate(topic = ind_name,
         original_group = subgroup) %>%
  filter(original_group != 'nhpi') %>%  # screen out, will use RC pacisl as group value
  filter(original_group != 'total') %>%
  mutate(
    type = ifelse(original_group %in% c("nhpi", "nh_nhpi"), 'group', 'subgroup')) %>%
  mutate(
    type = case_when(
      original_group %in% races ~ 'race',
      original_group %in% c("asian", "nh_asian") ~ 'race',
      original_group == 'total' ~ 'total',
      TRUE ~ type))

# check for race groups with aoic data - where aoic and alone exist for the same race and indicator, keep only the aoic data
nhpi_clean <- nhpi_clean %>%
  dplyr::group_by(topic, type) %>%
  # check if any indicators use aoic
  dplyr::mutate(has_aoic = any(grepl("aoic", original_group))) %>%
  dplyr::ungroup() %>%
  # keep only 'aoic' rows when an indicator has alone and aoic rows for same group
  dplyr::filter(
    type != "subgroup" |
      (type == "subgroup" & (!has_aoic | grepl("aoic", original_group)))) %>%
  dplyr::select(-has_aoic)


#clean rc data
asian_clean_rc <- clean_tables(indicator_tables_rc, 'asian') %>%
  mutate(original_group = subgroup,
         topic = ind_name,
         type = case_when(
           grepl('asian|nh_asian', original_group) ~ 'group',
           original_group %in% races ~ 'race',
           original_group %in% c("pacisl", "nh_pacisl") ~ 'race',
           original_group == 'total' ~ 'total',
           TRUE ~ NA))

#clean rc data
nhpi_clean_rc <- clean_tables(indicator_tables_rc, 'asian') %>%  # use 'asian' here bc it is the correct character count for the fx
  mutate(original_group = subgroup,
         topic = ind_name,
         type = case_when(
           grepl('pacisl|nh_pacisl', original_group) ~ 'group',
           original_group %in% races ~ 'race',
           original_group %in% c("asian", "nh_asian") ~ 'race',
           original_group == 'total' ~ 'total',
           TRUE ~ NA))

# checks for NA type - there are none
nhpi_clean_rc %>% filter(is.na(type))

#bind mosaic and rc data ####
asian_df <- rbind(asian_clean, asian_clean_rc)
nhpi_df <- rbind(nhpi_clean, nhpi_clean_rc)

# check all rows have type assigned
asian_df %>% filter(is.na(type)) # 0
nhpi_df %>% filter(is.na(type)) # 0

#add chart labels ####
asian_final <- asian_df %>%
  mutate(
    label = str_to_title(gsub("_", " ", original_group)))
# check which labels need to be edited
unique(asian_final$label)

asian_final <- asian_final %>%
  mutate(label = gsub(" Aoic|Nh ", "", label)) %>%
  mutate(label = case_when(
    label %in% c('Swana', 'Aian') ~ toupper(label),
    label == 'Latino' ~ 'Latinx',
    label == 'Pacisl' ~ 'Pacific Islander',
    label == 'Other' ~ 'Another Race',
    label == 'Twoormor' ~ 'Multiracial',
    label == 'Chinese No Taiwan' ~ 'Chinese excl. Taiwan',
    label == 'Asian Generic' ~ 'Asian not specified',
    TRUE ~ label
  ))

unique(asian_final$label)


#add chart labels ####
nhpi_final <- nhpi_df %>%
  mutate(label = str_to_title(gsub("_", " ", original_group)))
# check which labels need to be edited
unique(nhpi_final$label)

nhpi_final <- nhpi_final %>%
  mutate(label = gsub(" Aoic|Nh ", "", label)) %>%
  mutate(label = case_when(
    label %in% c('Swana', 'Aian') ~ toupper(label),
    label == 'Latino' ~ 'Latinx',
    label == 'Pacisl' ~ 'Pacific Islander',
    label == 'Other' ~ 'Another Race',
    label == 'Twoormor' ~ 'Multiracial',
    label == 'Nat Hawaii' ~ 'Native Hawaiian',
    label == 'Guam Chamorro' ~ 'Guam - Chamorro',
    TRUE ~ label
  ))
unique(nhpi_final$label)

# Edits to align with chart templates
asian_final2 <- asian_final %>%
  mutate(
    group = case_when(
      type == "subgroup" ~ "asian subgroups",
      type == "group" ~ "asian",
      type == "race" ~ "other major race groups",
      .default = type
    )
  ) %>%
  mutate(
    subgroup = ifelse(group == "asian", "asian total", subgroup)
  )

nhpi_final2 <- nhpi_final %>%
  mutate(
    group = case_when(
      type == "subgroup" ~ "pacisl subgroups",
      type == "group" ~ "pacisl",
      type == "race" ~ "other major race groups",
      .default = type
    )
  ) %>%
  mutate(
    subgroup = ifelse(group == "pacisl", "pacisl total", subgroup)
  )

asian_final <- asian_final2
nhpi_final <- nhpi_final2



# Export Asian Summary table to Postgres ------------------------------------------------------

table_name <- "asian_summary_for_charts_lac"
table_comment_source <- paste0("Created ", Sys.Date(), ". ONLY includes LAC indicator selected for the MOSAIC disaggregation report. Subgroup values come from MOSAIC indicator tables. Group, race, and total values come from RC indicator tables. QA doc: ", qa_filepath)
table_comment <- paste0("COMMENT ON TABLE ", curr_schema, ".", table_name, " IS '", table_comment_source, ".';")
column_comment <- paste0("COMMENT ON COLUMN ", curr_schema, ".", table_name, ".type IS 'group = asian, subgroup = asian subgroups, race = main rc races except for asian, total = total rate';")

#define col types
charvect <- c("text", "text", "text", "text", "integer", "numeric", "text", "text", "text", "text")
# add names to the character vector
names(charvect) <- colnames(asian_final)
charvect # check col types before exporting table to database

dbWriteTable(con2,
             Id(schema = curr_schema, table = table_name), asian_final,
             overwrite = FALSE, row.names = FALSE, field.types = charvect)

# send table and column comments to database
# Start a transaction
dbBegin(con2)
dbExecute(con2, table_comment)
dbExecute(con2, column_comment)

# Commit the transaction if everything succeeded
dbCommit(con2)



# Export NHPI Summary table to Postgres ------------------------------------------------------

table_name <- "nhpi_summary_for_charts_lac"
table_comment_source <- paste0("Created ", Sys.Date(), ". ONLY includes LAC indicator selected for the MOSAIC disaggregation report. Subgroup values come from MOSAIC indicator tables. Group, race, and total values come from RC indicator tables. QA doc: ", qa_filepath)
table_comment <- paste0("COMMENT ON TABLE ", curr_schema, ".", table_name, " IS '", table_comment_source, ".';")
column_comment <- paste0("COMMENT ON COLUMN ", curr_schema, ".", table_name, ".type IS 'group = nhpi, subgroup = nhpi subgroups, race = main rc races except for nhpi, total = total rate';")

#define col types
charvect <- c("text", "text", "text", "text", "integer", "numeric", "text", "text", "text", "text")
# add names to the character vector
names(charvect) <- colnames(nhpi_final)
charvect # check col types before exporting table to database

dbWriteTable(con2,
             Id(schema = curr_schema, table = table_name), nhpi_final,
             overwrite = FALSE, row.names = FALSE, field.types = charvect)

# send table and column comments to database
# Start a transaction
dbBegin(con2)
dbExecute(con2, table_comment)
dbExecute(con2, column_comment)

# Commit the transaction if everything succeeded
dbCommit(con2)


dbDisconnect(con)
dbDisconnect(con2)