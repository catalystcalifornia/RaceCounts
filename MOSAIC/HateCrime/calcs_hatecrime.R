## MOSAIC: Hate Crime Data Analysis ###

##### 1. set up the environment ##### 

#install packages if not already installed
packages <- c("openxlsx", "tidyr", "dplyr", "DBI", "RPostgres", "tidyverse", "stringr", "usethis", "rlang", "purrr")
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 

options(scipen=999)

# create connection for rda database
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("mosaic")

##### 2. get downloaded data ##### 
filepath <- "W:\\Data\\Crime and Justice\\HateCrimes\\CHIS\\"
omb_pre2020 <- read.xlsx(paste0(filepath, "pre_2020\\pre2020_omb.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:10,14:16,23:25))
aian_pre2020 <- read.xlsx(paste0(filepath, "pre_2020\\pre2020_aian.xlsx"), sheet=1, startRow=6, rows=c(6,8:9), cols=c(1:4))
nhpi_pre2020 <- read.xlsx(paste0(filepath, "pre_2020\\pre2020_nhpi.xlsx"), sheet=1, startRow=6, rows=c(6,8:9), cols=c(1:4))
swana_pre2020 <- read.xlsx(paste0(filepath, "pre_2020\\pre2020_swana.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:4))  # unstable

omb_2020 <- read.xlsx(paste0(filepath, "2020\\2020_omb.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:10,14:16,23:25))
aian_2020 <- read.xlsx(paste0(filepath, "2020\\2020_aian.xlsx"), sheet=1, startRow=6, rows=c(6,8:9), cols=c(1:4))
nhpi_2020 <- read.xlsx(paste0(filepath, "2020\\2020_nhpi.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:4))   # unstable
swana_2020 <- read.xlsx(paste0(filepath, "2020\\2020_swana.xlsx"), sheet=1, startRow=6, rows=c(6,8:9), cols=c(1:4))

omb_2021 <- read.xlsx(paste0(filepath, "2021\\2021_omb.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:10,14:16,23:25))
aian_2021 <- read.xlsx(paste0(filepath, "2021\\2021_aian.xlsx"), sheet=1, startRow=6, rows=c(6,8:9), cols=c(1:4))
nhpi_2021 <- read.xlsx(paste0(filepath, "2021\\2021_nhpi.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:4))    # unstable
swana_2021 <- read.xlsx(paste0(filepath, "2021\\2021_swana.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:4))  # unstable

omb_2022 <- read.xlsx(paste0(filepath, "2022\\2022_omb.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:10,14:16,23:25))
aian_2022 <- read.xlsx(paste0(filepath, "2022\\2022_aian.xlsx"), sheet=1, startRow=6, rows=c(6,8:9), cols=c(1:4))
nhpi_2022 <- read.xlsx(paste0(filepath, "2022\\2022_nhpi.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:4))    # unstable
swana_2022 <- read.xlsx(paste0(filepath, "2022\\2022_swana.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:4))  # unstable



##### 3. clean data ##### 
collect_dfs <- function(..., envir = .GlobalEnv) {
  # put all df's with specified prefix(es) into a nested list
  prefixes <- c(...) # allows the fx to accept unlimited # of prefixes (strings)
  
  nested_list <- setNames(
    lapply(prefixes, function(prefix) {
      all_names <- ls(envir = envir)
      matching_names <- all_names[startsWith(all_names, prefix)]
      
      df_names <- matching_names[sapply(matching_names, function(n) is.data.frame(get(n, envir = envir)))]
      
      setNames(
        lapply(df_names, function(n) get(n, envir = envir)),
        df_names
      )
    }),
    paste0(prefixes, "_data")
  )
  
  nested_list
}


# put each group's data into a list
data_list <- collect_dfs("omb", "nhpi", "aian", "swana")

# Fill in missing col names, Drop confidence interval columns (containing "-" in any row) and 'no' rows, Drop first col
clean_list <- function(d) {
  # temporarily name empty cols by position
  names(d) <- ifelse(is.na(names(d)) | trimws(names(d)) == "",
                     paste0("empty_", seq_along(names(d))),
                     names(d))
  
  d <- d %>%
    select(where(~ !any(grepl("-", ., fixed = TRUE), na.rm = TRUE))) %>%  # drop cols containing "-"
    filter(.[, 1] != "No") %>%    # drop 'No' rows
    rename_with(~ gsub("empty", "raw", .x), starts_with("empty")) %>% # rename raw cols
    select(-1)
    
  return(d)
}

clean_list <- lapply(data_list, function(sublist) {
  lapply(sublist, clean_list)
})

# View(clean_list[[4]][[1]])
# View(clean_list[[1]][[1]])
# View(clean_list[[2]][[1]])
# View(clean_list[[3]][[1]])

# clean SWANA, NHPI, AIAN
rename_nested_col <- function(nested_list, prefix) {
  # nested_list is the list containing nested elements for each race group, eg: clean_data
  # prefix is the prefix on each group's nested elements names, eg: 'swana'
  element_name <- paste0(prefix, "_data")  # get the nested list element name
  element <- nested_list[[element_name]]   # get the nested list element
  
  updated <- lapply(element, function(x) {
    x %>%   # rename columns to rate and raw
      rename(!!paste0(prefix, "_rate") := 1) %>%
      rename(!!paste0(prefix, "_raw") := 2)
  })
  
  nested_list[[element_name]] <- updated  # put updated list elements back into nested list
  return(nested_list)
}

clean_list <- rename_nested_col(clean_list, "swana")
clean_list <- rename_nested_col(clean_list, "nhpi")
clean_list <- rename_nested_col(clean_list, "aian")


# clean OMB
# 1. Extract the specific element
omb_ <- clean_list$omb_data

#View(omb_[[1]]) # use to create omb_colnames
omb_colnames <- c("latino_rate", "latino_raw", "nh_white_rate", "nh_white_raw", "nh_black_rate", "nh_black_raw",
                  "nh_asian_rate", "nh_asian_raw", "total_rate", "total_raw")

# 2. Run R code on it 
code_to_run <- lapply(omb_, setNames, omb_colnames)
result <- eval(code_to_run)

# 3. Insert the result back or keep the element updated
clean_list$omb_data <- result 
      
                
# flag unstable estimates
clean_list2 <- lapply(clean_list, function(sublist) {
  lapply(sublist, function(d) {
    rate_cols <- which(grepl("_rate$", names(d)))   # identify rate cols
    
    for (i in rev(rate_cols)) {
      d <- d %>%
        # add rate_flag column indicating unstable estimates as identified by CHIS
        mutate(rate_flag = ifelse(grepl("*", .[[i]], fixed = TRUE), "unstable", NA)) %>%
        relocate(rate_flag, .after = i) %>%
        # drop * from unstable estimate rate values, make all _rate cols type double
        mutate(!!names(.)[i] := as.double(gsub("*", "", .[[i]], fixed = TRUE)))
    }
    return(d)
  })
})


#### Convert to one longform dataframe ####

extract_year_label <- function(key) {
  if (str_detect(key, "pre")) {
    return("pre-2020")
  }
  match <- str_extract(key, "\\d{4}")
  if (!is.na(match)) return(match)
  return(key)
}

process_race_specific <- function(df, race, year_label) {
  rate_col  <- paste0(race, "_rate")
  raw_col   <- paste0(race, "_raw")
  flag_col  <- "rate_flag"
  
  id_cols <- setdiff(names(df), c(rate_col, raw_col, flag_col))
  
  df[id_cols] |>
    mutate(
      race      = race,
      year_     = year_label,
      rate      = if (rate_col %in% names(df)) df[[rate_col]] else NA,
      raw       = if (raw_col  %in% names(df)) df[[raw_col]]  else NA,
      rate_flag = if (flag_col %in% names(df)) df[[flag_col]] else NA
    )
}

process_omb <- function(group_data) {
  # Detect race prefixes from columns ending in '_rate'
  sample_df <- group_data[[1]]
  races <- names(sample_df) |>
    keep(~ str_ends(.x, "_rate")) |>
    str_remove("_rate$")
  
  imap(group_data, function(df, year_key) {
    year_label <- extract_year_label(year_key)
    id_cols <- setdiff(names(df), unlist(map(races, ~ c(paste0(.x, "_rate"), paste0(.x, "_raw")))))
    
    map(races, function(race) {
      df[id_cols] |>
        mutate(
          race      = race,
          year_     = year_label,
          rate      = if (paste0(race, "_rate") %in% names(df)) df[[paste0(race, "_rate")]] else NA,
          raw       = if (paste0(race, "_raw")  %in% names(df)) df[[paste0(race, "_raw")]]  else NA,
          rate_flag = NA  # omb data does not have any unstable estimates
        )
    }) |> bind_rows()
  }) |> bind_rows()
}

flatten_clean_list2 <- function(clean_list2) {
  imap(clean_list2, function(group_data, group_name) {
    if (group_name == "omb_data") {
      process_omb(group_data)
    } else {
      race <- str_remove(group_name, "_data$")  # 'swana_data' -> 'swana'
      
      imap(group_data, function(df, year_key) {
        year_label <- extract_year_label(year_key)
        process_race_specific(df, race, year_label)
      }) |> bind_rows()
    }
  }) |> bind_rows()
}

# Run it
final_df <- flatten_clean_list2(clean_list2) %>%
  # add col to order chronologically
  mutate(chrono = case_when(
    year_ == "pre-2020" ~ 1,
    year_ == "2020"     ~ 2,
    year_ == "2021"     ~ 3,
    year_ == "2022"     ~ 4
  ))