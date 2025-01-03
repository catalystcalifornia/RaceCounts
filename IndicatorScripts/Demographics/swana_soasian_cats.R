# Script to produce list of ACS SWANA ancestries for RC Race-Ethnicity Readme

#install packages if not already installed
list.of.packages <- c("readr","tidyr","dplyr","DBI","tidycensus", "rvest", "tidyverse", "stringr", "qpcR", "writexl", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(tidyr)
library(stringr)
library(tidycensus)
library(dplyr)
library(DBI)
library(qpcR)
library(writexl)
library(usethis)


# Variables - Update each yr
acs_yr = 2023

# connect to source for swana/soasian fx
source("W:/RDA Team/R/Github/RDA Functions/main/RDA-Functions/SWANA_Ancestry_List.R") # swana_ancestry() list
survey = "acs5"

# get full ACS SWANA ancestry list
swana_ancestry <- as.data.frame(swana_ancestry)

# get full ACS S. Asian ancestry list
soasian_ancestry <- as.data.frame(soasian_ancestry)

ancestry_list <- c(swana_ancestry, soasian_ancestry)
ancestry <- do.call(qpcR:::cbind.na, ancestry_list)
colnames(ancestry) <- c("Southwest Asian / N. African Ancestry", "S. Asian Ancestry")



# get ACS SWANA ancestries list that actually appears in B04006 - adapted from get_swana_var{} in SWANA_Ancestry_List.R
b04006_vars <- load_variables(acs_yr, survey, cache = TRUE) %>% filter(grepl("B04006", name)) # get all ancestry vars
swana_vars <- b04006_vars %>% filter(str_detect(label, str_c(swana_ancestry, collapse="|"))) # keep only SWANA ancestry vars
swana_vars <- swana_vars %>% filter(!label %in% c("Estimate!!Total:!!Arab:", "Estimate!!Total!!Arab"))  # filter out Arab category, keep Arab sub-categories
print.data.frame(swana_vars, max = NULL) # swana variables present in specified b04006 table which may/may not include all ancestries in swana_ancestry()

# get ACS S. Asian ancestries list that actually appears in B02018 - adapted from get_soasian_var{} in SWANA_Ancestry_List.R
soasian_ancestry
b02018_vars <- load_variables(acs_yr, survey, cache = TRUE) %>% filter(grepl("B02018", name)) # get all ancestry vars
soasian_vars <- b02018_vars %>% filter(str_detect(label, str_c(soasian_ancestry, collapse="|"))) # keep only asian 
print.data.frame(soasian_vars, max = NULL) # soasian variables present in specified b02018 table which may/may not include all ancestries in soasian_ancestry()



