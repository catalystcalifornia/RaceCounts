# Script to produce list of ACS SWANA ancestries for RC Race-Ethnicity Readme

#install packages if not already installed
list.of.packages <- c("DBI", "qpcR", "writexl", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(DBI)
library(qpcR)
library(writexl)
library(usethis)


# Variables - Update each yr
acs_yr = 2023
curr_yr = '2025'
curr_schema = 'v7'

# connect to source for swana/soasian fx
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/SWANA_Ancestry_List.R") 
survey = "acs5"

# get full ACS SWANA ancestry list
swana_ancestry <- as.data.frame(swana_ancestry)

# get full ACS S. Asian ancestry list
soasian_ancestry <- as.data.frame(soasian_ancestry)

# combine swana and soasian lists
ancestry_list <- c(swana_ancestry, soasian_ancestry)
ancestry <- do.call(qpcR:::cbind.na, ancestry_list) %>% as.data.frame()
colnames(ancestry) <- c("Southwest Asian / N. African Ancestry", "S. Asian Ancestry")

# export combined ancestry list as excel doc
write_xlsx(ancestry, paste0("W:\\Project\\RACE COUNTS\\", curr_yr, "_", curr_schema, "\\RC_Github\\RaceCounts\\Methodology\\SWANA_SoAsian_Ancestry.xlsx"))

