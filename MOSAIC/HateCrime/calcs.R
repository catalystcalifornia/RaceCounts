## MOSAIC: Hate Crime Data Analysis ###

##### 1. set up the environment ##### 

#install packages if not already installed
packages <- c("openxlsx", "tidyr", "dplyr", "DBI", "RPostgres", "tidyverse", "stringr", "usethis", "rlang")
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
nhpi_2020 <- read.xlsx(paste0(filepath, "2020\\2020_nhpi.xlsx"), sheet=1, startRow=8, rows=c(8,10:11), cols=c(1:4))  # unstable
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


##### 4. analyze data ##### 





