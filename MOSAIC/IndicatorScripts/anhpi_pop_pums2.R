# Disaggregated Asian and NHPI from PUMS

# Data Dictionary: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.pdf

# Install packages if not already installed
packages <- c("tidyverse", "data.table", "readxl","tidycensus", "srvyr","stringr", "openxlsx", "dplyr") 

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(install_packages) > 0) {
  install.packages(install_packages)
} else {
  print("All required packages are already installed.")
}

for(pkg in packages){
  library(pkg, character.only = TRUE)
}

#SOURCE from the script that has: styling, packages, dbconnection, colors
source("W:\\RDA Team\\R\\credentials_source.R")

#### Step 1: load the data ####

# PUMS Data
root <- "W:/Data/Demographics/PUMS/"
indicator_name <- "Disaggregated Asian"


# Load the people PUMS data
people <- fread(paste0(root, "CA_2023/psam_p06.csv"), header = TRUE, data.table = FALSE,
                colClasses = list(character = c("PUMA", "RAC1P", "RAC2P", "HISP")))


## Select Non-Latinx Asian and Non-Latinx NHPI people
pums_data <- people %>% 
  
  #filtering for universe and LA county
  filter((RACASN == 1 | RACNH == 1 | RACPI == 1) & HISP == "01") 

#pull in PUMS data dictionary codes for RAC2P
race_codes <- read_excel("W:/Data/Demographics/PUMS/CA_2019_2023/PUMS_Data_Dictionary_2019-2023_RAC2P.xlsx") %>% 
  # created this excel document separate by opening PUMS Data Dictionary in excel and deleting everything but both vintages of RAC2P, deleting old header rows, adding new header row at top
  mutate_all(as.character) 

anhpi_alone_youth <- left_join(pums_data, race_codes, by=("RAC2P")) %>% rename(anhpi_subgroup=Description)# clarify column we'll group by and set final dataset




weight <- 'PWGTP'  # weight
repwlist = rep(paste0("PWGTP", 1:80)) # replicate weights

# set survey components
asian_youth_svry <- asian_alone_youth %>%               
  as_survey_rep(
    variables = c(geoid, asian_subgroup),   # dplyr::select grouping variables
    weights = weight,                       # person weight
    repweights = repwlist,                  # list of replicate weights
    combined_weights = TRUE,                # tells the function that replicate weights are included in the data
    mse = TRUE,                             # tells the function to calc mse
    type="other",                           # statistical method
    scale=4/80,                             # scaling set by ACS
    rscale=rep(1,80)                        # setting specific to ACS-scaling
  )


###### Asian subgroups, Asian alone ######
asian_alone_subgroups_table <- asian_youth_svry %>%
  group_by(geoid, asian_subgroup) %>%   # group by asian subgroup description
  summarise(
    num = survey_total(na.rm=T), # get the (survey weighted) count for the numerators
    rate = survey_mean()) %>%        # get the (survey weighted) proportion for the numerator
  left_join(asian_youth_svry %>%                                        # left join in the denominators
              group_by(geoid) %>%                                     # group by geo
              summarise(pop = survey_total(na.rm=T))) %>%              # get the weighted total for overall geo
  mutate(rate=rate*100,
         rate_moe = rate_se*1.645*100,    # calculate the margin of error for the rate based on se
         rate_cv = ((rate_moe/1.645)/rate) * 100, # calculate cv for rate
         count_moe = num_se*1.645, # calculate moe for numerator count based on se
         count_cv = ((count_moe/1.645)/num) * 100)  # calculate cv for numerator count

# recode rates under 1% and create a list for recoding
other_list<-asian_alone_subgroups_table%>%filter(rate<1)%>%ungroup()%>%select(asian_subgroup)
other_list<-other_list$asian_subgroup


asian_alone_youth<-asian_alone_youth%>%
  mutate(asian_subgroup=ifelse(asian_subgroup %in% other_list, 'Another Asian Identity Alone',asian_subgroup)) 

asian_youth_svry <- asian_alone_youth %>%               
  as_survey_rep(
    variables = c(geoid, asian_subgroup),   # dplyr::select grouping variables
    weights = weight,                       # person weight
    repweights = repwlist,                  # list of replicate weights
    combined_weights = TRUE,                # tells the function that replicate weights are included in the data
    mse = TRUE,                             # tells the function to calc mse
    type="other",                           # statistical method
    scale=4/80,                             # scaling set by ACS
    rscale=rep(1,80)                        # setting specific to ACS-scaling
  )


###### Asian subgroups, Asian alone ######
asian_alone_subgroups_table_re <- asian_youth_svry %>%
  group_by(geoid, asian_subgroup) %>%   # group by asian subgroup description
  summarise(
    num = survey_total(na.rm=T), # get the (survey weighted) count for the numerators
    rate = survey_mean()) %>%        # get the (survey weighted) proportion for the numerator
  left_join(asian_youth_svry %>%                                        # left join in the denominators
              group_by(geoid) %>%                                     # group by geo
              summarise(pop = survey_total(na.rm=T))) %>%              # get the weighted total for overall geo
  mutate(rate=rate*100,
         rate_moe = rate_se*1.645*100,    # calculate the margin of error for the rate based on se
         rate_cv = ((rate_moe/1.645)/rate) * 100, # calculate cv for rate
         count_moe = num_se*1.645, # calculate moe for numerator count based on se
         count_cv = ((count_moe/1.645)/num) * 100)  # calculate cv for numerator count


#Upload to Postgres  ####
# set the connection to pgadmin
con3 <- connect_to_db("bold_vision")
table_name <- "demo_disaggregated_asian"
schema <- 'bv_2023'

indicator <- "Disaggregated Asian Youth"
source <- "American Community Survey 2017-2021 5-year PUMS estimates. See QA doc for details: W:\\Project\\OSI\\Bold Vision\\BV 2023\\Documentation\\QA_Demo_Asian.docx"

dbWriteTable(con3, c(schema, table_name), asian_alone_subgroups_table_re,
             overwrite = TRUE, row.names = FALSE)

#comment on table and columns
comment <- paste0("COMMENT ON TABLE ", schema, ".", table_name,  " IS '", indicator, " from ", source, ".';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".geoid IS 'County fips';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".rate IS 'indicator rate';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".pop IS 'total population';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".num IS 'number of people by disaggregated non-Hispanic Asian Identity';
                  COMMENT ON COLUMN ", schema, ".", table_name, ".rate_cv IS 'cv of indicator rate';")
print(comment)
dbSendQuery(con3, comment)

#disconnect
dbDisconnect(con3)