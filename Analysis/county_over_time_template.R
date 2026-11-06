# create connection for DR databases
source("W:\\RDA Team\\R\\credentials_source.R")
source("Analysis\\get_county_data_functions.R")
library(purrr)
con_rc <- connect_to_db("racecounts")

options(scipen = 999) # disable scientific notation


##### Set parameters for reporting: county name, the curr(ent) and prev(ious) schemas/years for comparison #####
county <- "Marin"
curr_schema = "v7"
curr_year = "2025"
prev_schema = "v5"
prev_year = "2023"

##### Review composite index changes first to see if there are changes in disparity and/or performance #####
composite <- compare_index(
  con=con_rc,
  idx="composite",
  county=county,
  schema_a = prev_schema,
  year_a = prev_year,
  schema_b = curr_schema,
  year_b = curr_year) 

# can filter for only disparity or performance, for rank or values count

##### Next review each issue area index and see if larger shifts happened in one issue versus another
results <- lapply(issue_area_list, function(issue_area) {
  compare_index(
    con=con_rc,
    idx=issue_area,
    county=county,
    schema_a = prev_schema,
    year_a = prev_year,
    schema_b = curr_schema,
    year_b = curr_year) 
})

# see where bigger rankings shifts happened across all issue area indexes
# also includes value_counts in to help flag changes in data availability
county_issues_overview <- bind_rows(results) %>%
  arrange(desc(change)) %>%
  arrange(issue_area, dimension, statistic)

# based on above we see improvements across all issue areas (except housing)
# should identify what explains improvements or regression - taking note that
# methods changes or new data could also help explain changes for specific indicators
# do we see improvement in disparity in Marin or just other counties regressing more? both? something else?

table_list <- lapply(issue_area_list, function(issue) {
  dbGetQuery(con_rc, build_table_query(issue, curr_schema, curr_year)) |>
    mutate(
      issue_code = issue,
      table = gsub(paste0("_", curr_year, "$"), "", table_name)
    ) |>
    select(issue_code, table)
}) |>
  bind_rows()

results <- purrr::map2(table_list$table, table_list$issue_code, function(ind_, issue) {
  compare_indicators(
    con = con_rc,
    ind_table = ind_,
    county = county,
    schema_a = prev_schema,
    year_a = prev_year,
    schema_b = curr_schema,
    year_b = curr_year) |>
  mutate(issue_code = issue, .before = 1)}) |>
  bind_rows()

issue_dfs <- split(results, results$issue_code)

# Can view individual issue areas - since the overview indicated large shifts in 
# Safety & Justice and Economic Opportunity I'll review those

# These tables can be filtered to only show overall ranks for disp/perf and also 
# z scores (overall and for each race/ethn group) which helps clarify if data 
# availability impacted the county's rankings. 
# Note: minor shifts in z scores over time are not reliable
View(issue_dfs$crim) 
View(issue_dfs$crim %>% filter(statistic=="rank"))
View(issue_dfs$crim %>% filter(statistic=="z"))


View(issue_dfs$econ)

# Housing was the only issue area to worsen is now the worst
View(issue_dfs$hous)

dbDisconnect(con_rc)