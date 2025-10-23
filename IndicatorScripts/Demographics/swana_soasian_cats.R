# Script to produce list of ACS SWANA ancestries for RC Race-Ethnicity Readme

#install packages if not already installed
packages <- c("DBI", "qpcR", "writexl", "usethis") 
install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 



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
write_xlsx(ancestry, ".\\Methodology\\SWANA_SoAsian_Ancestry.xlsx")

