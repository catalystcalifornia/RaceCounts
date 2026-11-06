##### Create a couple functions
source("W:\\RDA Team\\R\\credentials_source.R")
library(stringr)
library(bit64) # some pg columns are bigint and need specific handling

##### define static variables for lapply)
issue_area_list <- c("demo", "econ", "educ", "hlth", "hben", "hous", "crim")


##### define functions #####
build_table_query <- function(issue_code, curr_schema, curr_year){
  table_query <- paste0(
    "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND 
    table_schema='", curr_schema, "' AND 
    table_name LIKE 'arei_", issue_code, "_%' AND 
    table_name LIKE '%_county_%' AND 
    table_name NOT LIKE '%index%'AND 
    table_name LIKE '%_", curr_year, "';")
}

# helper function for compare_index() to pull a given index for a given county for 2 periods of time
get_indicator <- function(con, ind_table, schema_, year, county) {
  tbl_id <- DBI::Id(schema = schema_, table = sprintf("%s_%s", ind_table, year))
  
  tryCatch(
    dplyr::tbl(con, tbl_id) |>
      dplyr::filter(county_name == !!county) |>
      dplyr::collect(),
    error = function(e) {
      message("Skipping ", schema_, ".", ind_table, "_", year, ": ", conditionMessage(e))
      NULL
    }
  )
}

get_index <- function(con, idx, schema_, year, county) {
  tbl <- DBI::Id(schema = schema_, table = sprintf("arei_%s_index_%s", idx, year))
  tbl(con, tbl) |> filter(county_name == !!county) |> collect()
}

# function to compare same index over two years
compare_index <- function(con, idx, county, schema_a, year_a, schema_b, year_b) {
  a <- get_index(con, idx, schema_a, year_a, county)
  b <- get_index(con, idx, schema_b, year_b, county)
  # keep only numeric columns, pivot each to long format
  to_long <- function(df, value_name) {
    df |>
      mutate(across(where(is.integer64), as.numeric)) |>
      select(county_name, where(is.numeric)) |>
      pivot_longer(-county_name, names_to = "metric", values_to = value_name)
  }
  
  full_join(
    to_long(a, "period_a"),
    to_long(b, "period_b"),
    by = c("county_name", "metric")
  ) |>
    mutate(
      change = period_b - period_a,
      dimension = case_when(
        str_detect(metric, "(^|_)disp(arity)?(_|$)") ~ "disparity",
        str_detect(metric, "(^|_)perf(ormance)?(_|$)") ~ "performance",
        TRUE ~ "other"),
      # everything before _disp_ / _perf_; NA when there is no prefix
      issue_area = idx,
      statistic = str_extract(metric, "(rank|z|avg|values_count)$"),
      .after = metric
    ) %>%
    select(-metric) %>%
    filter(statistic %in% c("rank", "values_count")) %>% # can edit this list if we want to include other stats likes z score, etc.
    rename("{year_a}" := period_a, "{year_b}" := period_b) %>%
    arrange(desc(change), dimension)
}

compare_indicators <- function(con, ind_table, county, schema_a, year_a, schema_b, year_b) {
  # get indicator name from ind_table
  indicator <- str_match(ind_table, "^arei_[^_]{4}_(.+?)_county")[, 2]
  if (is.na(indicator)) {
    stop("Can't parse indicator from table name: ", ind_table, call. = FALSE)
  }
  
  a <- get_indicator(con, ind_table, schema_a, year_a, county)
  b <- get_indicator(con, ind_table, schema_b, year_b, county)
  
  to_long <- function(df, value_name) {
    if (is.null(df)) {
      # empty tibble with the right columns/types; the join fills in NAs
      return(tibble::tibble(
        county_name = character(),
        metric      = character(),
        "{value_name}" := numeric()
      ))
    }
    
    df |>
      mutate(across(where(is.integer64), as.numeric)) |>
      select(county_name, matches("disp|perf")) |>
      select(county_name, where(is.numeric)) |>
      pivot_longer(-county_name, names_to = "metric", values_to = value_name)
  }
  
  full_join(
    to_long(a, "period_a"),
    to_long(b, "period_b"),
    by = c("county_name", "metric")
  ) |>
    mutate(
      change = period_b - period_a,
      dimension = case_when(
        str_detect(metric, "(^|_)disp(arity)?(_|$)") ~ "disparity",
        str_detect(metric, "(^|_)perf(ormance)?(_|$)") ~ "performance",
        TRUE ~ "other"
      ),
      race_ethn  = str_match(metric, "^(.+?)_(?:disp|perf)")[, 2] |>
        replace_na("overall"),
      indicator = indicator,
      statistic = str_extract(metric, "(rank|z|avg|values_count|index_of)$"),
      .after = metric
    ) |>
    select(-metric) |>
    filter(statistic %in% c("rank", "z", "index_of")) |> # can edit this list if we want to include other stats likes z score, etc.
    rename("{year_a}" := period_a, "{year_b}" := period_b) |>
    mutate(across(where(is.numeric), \(x) round(x, 2))) |>
    arrange(desc(change), dimension) 
    
}
