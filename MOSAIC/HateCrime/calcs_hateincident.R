## MOSAIC: Hate Incident Data Analysis ###

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
omb_data <- read.xlsx(paste0(filepath, "2023_24\\2023_24_omb.xlsx"), sheet=1, startRow=5, rows=c(5,7), cols=c(1:10,14:16,23:25))
aian_data <- read.xlsx(paste0(filepath, "2023_24\\2023_24_aian.xlsx"), sheet=1, startRow=5, rows=c(5,7), cols=c(1:4))
nhpi_data <- read.xlsx(paste0(filepath, "2023_24\\2023_24_nhpi.xlsx"), sheet=1, startRow=5, rows=c(5,7), cols=c(1:4)) 
swana_data <- read.xlsx(paste0(filepath, "2023_24\\2023_24_swana.xlsx"), sheet=1, startRow=5, rows=c(5,7), cols=c(1:4))


##### 3. clean data ##### 

# put each group's data into a list
data_list <- list(omb_data, nhpi_data, aian_data, swana_data)
names(data_list) <- c("omb", "nhpi", "aian", "swana")

# Fill in missing col names, Drop confidence interval columns (containing "-" in any row) and 'no' rows, Drop first col
clean_list <- function(d) {
  # temporarily name empty cols by position
  names(d) <- ifelse(is.na(names(d)) | trimws(names(d)) == "",
                     paste0("empty_", seq_along(names(d))),
                     names(d))
  
  d <- d %>%
    select(where(~ !any(grepl("-", ., fixed = TRUE), na.rm = TRUE))) %>%  # drop cols containing "-"
    rename_with(~ gsub("empty", "raw", .x), starts_with("empty")) %>% # rename raw cols
    select(-1)
    
  return(d)
}

clean_data <- lapply(data_list, clean_list)

# View(clean_list[[4]])
# View(clean_list[[1]])
# View(clean_list[[2]])
# View(clean_list[[3]])

# clean SWANA, NHPI, AIAN
rename_col <- function(y, prefix) {
  # x is the list containing elements for each race group, eg: clean_data
  # prefix is the prefix on each group's nested elements names, eg: 'swana'
  element <- y[[prefix]]  # get the specific list element
  
  updated <- element %>%
      rename(!!paste0(prefix, "_rate") := 1) %>%
      rename(!!paste0(prefix, "_raw") := 2)

  
  y[[prefix]] <- updated  # put updated element back into list
  return(y)
}

clean_data <- rename_col(clean_data, "swana")
clean_data <- rename_col(clean_data, "nhpi")
clean_data <- rename_col(clean_data, "aian")


# clean OMB
# 1. Extract the specific element
omb_ <- clean_data$omb

#View(omb_[[1]]) # use to create omb_colnames
omb_colnames <- c("latino_rate", "latino_raw", "nh_white_rate", "nh_white_raw", "nh_black_rate", "nh_black_raw",
                  "nh_asian_rate", "nh_asian_raw", "total_rate", "total_raw")
# 2. Rename cols
colnames(omb_)
colnames(omb_) <- omb_colnames
colnames(omb_)

# 3. Insert the result back (keep the updated element)
clean_data$omb <- omb_ 
      
                
##### 4. Convert to long form df ##### 
convert_to_long <- function(data_list) {
  # data_list is a list of dataframes with columns like 'swana_rate', 'latino_raw'
  
  long_list <- lapply(data_list, function(df) {
    
    # get all prefixes from column names
    prefixes <- unique(gsub("_rate$|_raw$", "", grep("_rate$|_raw$", names(df), value = TRUE)))
    
    # pivot each prefix and stack
    prefix_list <- lapply(prefixes, function(prefix) {
      df %>%
        select(matches(paste0("^", prefix, "_rate$|^", prefix, "_raw$"))) %>%
        rename(rate = paste0(prefix, "_rate"),
               raw = paste0(prefix, "_raw")) %>%
        mutate(race = prefix)
    })
    
    bind_rows(prefix_list)
  })
  
  bind_rows(long_list) %>%
    select(race, rate, raw)
}

final_df <- convert_to_long(clean_data) 
