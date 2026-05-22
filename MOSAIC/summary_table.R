#### MOSAIC Summary Table: Indicators for Report Page ####

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

qa_filepath <- "W:\\Project\\RACE COUNTS\\2025_v7\\MOSAIC\\QA_Summary_Table.docx"

# used later to populate 'type' column
races <- c('latino', 'aian', 'black', 'white', 'twoormor', 'other', 'swana', 'nh_aian', 'nh_black', 'nh_white', 'nh_twoormor', 'nh_other', 'nh_swana')

## get MOSAIC pop data #####
asian_pop <- dbGetQuery(con2, paste0("select * from ", curr_schema, ".aa_pop_b02018 where geolevel IN ('county','state')")) # import county & state records only
nhpi_pop <- dbGetQuery(con2, paste0("select * from ", curr_schema, ".nhpi_pop_b02019 where geolevel IN ('county','state')")) # import county & state records only

## get MOSAIC state indicator tables #####
table_list <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_state_%';")
m_list <- dbGetQuery(con2, table_list) %>% dplyr::rename('table' = 'table_name')

indicator_list <- m_list[order(m_list$table), ] # alphabetize list of index tables which transforms into character from list, needed to format list correctly for next steps
indicator_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", indicator_list), indicator_list), DBI::dbGetQuery, conn = con2) # import tables from postgres

## filter for report page indicators & split into asian and nhpi ##
asian_tables <- indicator_tables[grepl("asian", names(indicator_tables))]
asian_tables <- asian_tables[grepl("overcrowd|officials|insurance|voter", names(asian_tables))]

nhpi_tables <- indicator_tables[grepl("nhpi", names(indicator_tables))]
nhpi_tables <- nhpi_tables[grepl("overcrowd|youth|insurance|wage", names(nhpi_tables))]

## get RC state indicator tables #####
table_list_rc <- paste0("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", curr_schema, "' AND table_name LIKE '%_state_%' AND table_name LIKE '%", rc_yr, "a';")
rc_list <- dbGetQuery(con, table_list_rc) %>% dplyr::rename('table' = 'table_name')

indicator_list_rc <- rc_list[order(rc_list$table), ] # alphabetize list of index tables which transforms into character from list, needed to format list correctly for next steps
indicator_tables_rc <- lapply(setNames(paste0("select * from ", curr_schema, ".", indicator_list_rc), indicator_list_rc), DBI::dbGetQuery, conn = con) # import tables from postgres

## filter for report page indicators & split into asian and nhpi ####
asian_tables_rc <- indicator_tables_rc[grepl("overcrowd|officials|insurance|voter", names(indicator_tables_rc))]
nhpi_tables_rc <- indicator_tables_rc[grepl("overcrowd|youth|insurance|wage", names(indicator_tables_rc))]


# format and clean tables####
clean_tables <- function(data_list, race){

indicator_tables_clean <- lapply(data_list, function(x) x %>%
  select(state_id, state_name, ends_with(c("raw", "rate"))))
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
    cols = -c(state_id, state_name),
    names_to = "name",
    values_to = "value"
  ) %>%
  tidyr::separate(name, 
                  into = c("topic", "group_metric"), 
                  sep = "\\.") %>%
  tidyr::separate(group_metric, 
                  into = c("group", "metric"), 
                  sep = "_(?=rate$|raw$)") %>%
  tidyr::pivot_wider(
    names_from = metric,
    values_from = value
  )

return(df_long)
}

#clean mosaic data
asian_clean <- clean_tables(asian_tables, 'asian') %>%
  filter(!(group == 'asian' & topic =='officials')) %>% # screen out, will use RC nh_asian as group value
  filter(!(group == 'total' & topic == 'officials')) %>%
  mutate(type = ifelse(group %in% c("asian", "nh_asian"), 'group', 'subgroup')) %>%
  mutate(type = case_when(
    group %in% races ~ 'race',
    group %in% c("pacisl", "nh_pacisl") ~ 'race',
    group == 'total' ~ 'total',
    TRUE ~ type))

#keep only 'aoic' rows when an indicator has alone and aoic rows for same group
asian_clean <- asian_clean %>%
  dplyr::group_by(topic, type) %>%
  dplyr::filter(
    type != "subgroup" |
      (type == "subgroup" & (
        if (any(grepl("aoic", group))) grepl("aoic", group) else TRUE
      ))
  ) %>%
  dplyr::ungroup()


#clean mosaic data
nhpi_clean <- clean_tables(nhpi_tables, 'nhpi') %>%
  filter(group != 'nhpi') %>%  # screen out, will use RC pacisl as group value
  filter(group != 'total') %>%
  mutate(type = ifelse(group %in% c("nhpi", "nh_nhpi"), 'group', 'subgroup')) %>%
  mutate(type = case_when(
    group %in% races ~ 'race',
    group %in% c("asian", "nh_asian") ~ 'race',
    group == 'total' ~ 'total',
    TRUE ~ type))

#keep only 'aoic' rows when an indicator has alone and aoic rows for same group
nhpi_clean <- nhpi_clean %>%
  dplyr::group_by(topic, type) %>%
  dplyr::filter(
    type != "subgroup" |
      (type == "subgroup" & (
        if (any(grepl("aoic", group))) grepl("aoic", group) else TRUE
      ))
  ) %>%
  dplyr::ungroup()


#clean rc data
asian_clean_rc <- clean_tables(asian_tables_rc, 'asian') %>%
  mutate(topic = str_sub(topic, end = -2)) %>%
  mutate(topic = str_sub(topic, start = 6),
  type = case_when(
    grepl('asian|nh_asian', group) ~ 'group',
    group %in% races ~ 'race',
    group %in% c("pacisl", "nh_pacisl") ~ 'race',
    group == 'total' ~ 'total',
    TRUE ~ NA))

#clean rc data
nhpi_clean_rc <- clean_tables(nhpi_tables_rc, 'asian') %>%  # use 'asian' here bc it is the correct character count for the fx
  mutate(topic = str_sub(topic, end = -2)) %>%
  mutate(topic = str_sub(topic, start = 6),
         type = case_when(
           grepl('pacisl|nh_pacisl', group) ~ 'group',
           group %in% races ~ 'race',
           group %in% c("asian", "nh_asian") ~ 'race',
           group == 'total' ~ 'total',
           TRUE ~ NA))
nhpi_clean_rc %>% filter(is.na(type))

#bind mosaic and rc data ####
asian_df <- rbind(asian_clean, asian_clean_rc)
nhpi_df <- rbind(nhpi_clean, nhpi_clean_rc)

# check all rows have type assigned
asian_df %>% filter(is.na(type))
nhpi_df %>% filter(is.na(type))

#add chart labels ####
asian_final <- asian_df %>%
  mutate(label = str_to_title(gsub("_", " ", group)))
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
  mutate(label = str_to_title(gsub("_", " ", group)))
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





# Export Asian Summary table to Postgres ------------------------------------------------------

table_name <- "asian_summary_for_charts"
table_comment_source <- paste0("Created ", Sys.Date(), ". ONLY includes indicators selected for the MOSAIC disaggregation report. Subgroup values come from MOSAIC indicator tables. Group, race, and total values come from RC indicator tables. QA doc: ", qa_filepath)
table_comment <- paste0("COMMENT ON TABLE ", curr_schema, ".", table_name, " IS '", table_comment_source, ".';")
column_comment <- paste0("COMMENT ON COLUMN ", curr_schema, ".", table_name, ".type IS 'group = asian, subgroup = asian subgroups, race = main rc races except for asian, total = total rate';")

#define col types
charvect <- c("text", "text", "text", "text", "integer", "numeric", "text", "text")
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

table_name <- "nhpi_summary_for_charts"
table_comment_source <- paste0("Created ", Sys.Date(), ". ONLY includes indicators selected for the MOSAIC disaggregation report. Subgroup values come from MOSAIC indicator tables. Group, race, and total values come from RC indicator tables. QA doc: ", qa_filepath)
table_comment <- paste0("COMMENT ON TABLE ", curr_schema, ".", table_name, " IS '", table_comment_source, ".';")
column_comment <- paste0("COMMENT ON COLUMN ", curr_schema, ".", table_name, ".type IS 'group = nhpi, subgroup = nhpi subgroups, race = main rc races except for nhpi, total = total rate';")

#define col types
charvect <- c("text", "text", "text", "text", "integer", "numeric", "text", "text")
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
