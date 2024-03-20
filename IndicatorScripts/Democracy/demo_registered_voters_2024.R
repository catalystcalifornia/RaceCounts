#install packages if not already installed
list.of.packages <- c("RPostgreSQL","DBI","tidyverse","tidycensus")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

### load packages
library(RPostgreSQL)
library(DBI)
library(tidyverse)
library(tidycensus)


# Set Source --------------------------------------------------------------
##set source for Voting Presidential Script to get clean_cps, voted_by_county, voted_by_state, voting_age_county, voting_age_state functions
source("W:/Project/RACE COUNTS/2022_v4/Democracy/R/demo_voting_presidential_2022.R")


# Get Data ----------------------------------------------------------------

source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

#get already QA'd 2012, 2014, 2016, and 2018 data 
df_2012 <- dbGetQuery(con, "SELECT * FROM data.cps_2012_voting_supplement") # 12,675 x 84
#url <- "https://www2.census.gov/programs-surveys/cps/datasets/2012/supp/nov12pub.csv"
#destfile <- "W:/Data/Democracy/Current Population Survey Voting and Registration/2012/nov20pub.csv"
# 2012 metadata:  https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov12.pdf

df_2014 <- dbGetQuery(con, "SELECT * FROM data.cps_2014_voting_supplement") #12, 747 x 89
# 2014 metadata:  https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov14.pdf

df_2016 <- dbGetQuery(con, "SELECT * FROM data.cps_2016_voting_supplement") #12, 299 x 85
# 2016 metadata:  https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov16.pdf

df_2018 <- dbGetQuery(con, "SELECT * FROM data.cps_2018_voting_supplement") # 11,700 x 85
# 2018 metadata:  https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov18.pdf

dbDisconnect(con)

#2020 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov20.pdf
# ALREADY QA'D THRU PRESIDENTIAL VOTING INDICATOR
df_2020 <-  read_csv(file = "W:/Data/Democracy/Current Population Survey Voting and Registration/2020/nov20pub.csv", # 134, 122 x 400
                     col_types = cols(
                       gestfips = "character",
                       gtcbsa = "character",
                       gtco = "character",
                       gtcsa = "character"))

#2022 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov22.pdf
df_2022 <-  read_csv(file = "W:/Data/Democracy/Current Population Survey Voting and Registration/2022/nov22pub.csv", # 134, 122 x 400
                     col_types = cols(
                       GESTFIPS = "character",
                       GTCBSA = "character",
                       GTCO = "character",
                       GTCSA = "character")) %>% 
  rename(gestfips = GESTFIPS, gtcbsa = GTCBSA, gtco = GTCO, gtcsa = GTCSA)


# Clean data using clean_cps function created in voting_president --------
df_2012 <- clean_cps(df_2012)

df_2014 <- clean_cps(df_2014)

df_2016 <- clean_cps(df_2016)

df_2018 <- clean_cps(df_2018)

df_2020 <- clean_cps(df_2020)

df_2022 <- clean_cps(df_2022)

# REGISTERED NON-VOTERS Functions --------------------------------------------

## By county -- pes2 universe EXCLUDES those who voted.

registered_by_county <- function(data) {
  # total
  data %>% 
    filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", gtco != "06000"
    ) %>%
    group_by(
      gtco # group by county code
    ) %>%
    summarize(num_total_reg_nv = sum(pwsswgtnum), count_total_reg_nv = n() ) %>%
    
    ### merge with Latino
    left_join(
      
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1", gtco != "06000" # Hispanic or Latino 
        ) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_latino_reg_nv = sum(pwsswgtnum), count_latino_reg_nv = n() )) %>%
    
    ### merge with NH White
    left_join(
      
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1", gtco != "06000" # non-hispanic White
        ) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_white_reg_nv = sum(pwsswgtnum), count_nh_white_reg_nv = n() )) %>%
    
    ### merge with NH Black
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2", gtco != "06000" # non-hispanic Black
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_black_reg_nv = sum(pwsswgtnum), count_nh_black_reg_nv = n() )) %>%
    
    ### merge with all AIAN
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24"), gtco != "06000" # AIAN
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_aian_reg_nv = sum(pwsswgtnum), count_aian_reg_nv = n() )) %>%
    
    ### merge with NH Asian
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4", gtco != "06000" # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_asian_reg_nv = sum(pwsswgtnum), count_nh_asian_reg_nv = n() )) %>%
    
    ### merge with all PACISL
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24"), gtco != "06000" # PACISL
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_pacisl_reg_nv = sum(pwsswgtnum), count_pacisl_reg_nv = n() )) %>%
    
    ### merge with NH Two or more races
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26"), gtco != "06000" # NH Two or More
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_twoormor_reg_nv = sum(pwsswgtnum), count_nh_twoormor_reg_nv = n() ))   
  
}


## By State -- pes2 universe EXCLUDES those who voted

registered_by_state <- function(data) {
  # total
  data %>% 
    filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18"
    ) %>%
    group_by(
      gestfips # group by state
    ) %>%
    summarize(num_total_reg_nv = sum(pwsswgtnum), count_total_reg_nv = n() ) %>%
    
    ###### merge with Latino
    left_join(
      
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1" # Hispanic or Latino 
        ) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_latino_reg_nv = sum(pwsswgtnum), count_latino_reg_nv = n() )) %>%
    
    ### merge with NH white
    left_join(
      
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1" # non-hispanic White
        ) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_white_reg_nv = sum(pwsswgtnum), count_nh_white_reg_nv = n() )) %>%
    
    #### merge with NH black
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2"# non-hispanic Black
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_black_reg_nv = sum(pwsswgtnum), count_nh_black_reg_nv = n() )) %>%
    
    #### merge with All AIAN
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24") # AIAN
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_aian_reg_nv = sum(pwsswgtnum), count_aian_reg_nv = n() )) %>%
    
    ### merge with NH Asian
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4" # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_asian_reg_nv = sum(pwsswgtnum), count_nh_asian_reg_nv = n() )) %>%
    
    ### merge with all PACISL
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24") # PACISL
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_pacisl_reg_nv = sum(pwsswgtnum), count_pacisl_reg_nv = n() )) %>%
    
    ### merge with nh two or more races
    left_join(
      data %>% 
        filter( pes1 == "2", pes2 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26") # Two or More
        ) %>%
        select(gtco, gestfips, pes2, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_twoormor_reg_nv = sum(pwsswgtnum), count_nh_twoormor_reg_nv = n() )) 
  
  
}


# 2012 data ---------------------------------------------------------------

### Calc voted ###
# county
voted_county_2012 <- voted_by_county(df_2012)
names(voted_county_2012) <- ifelse(names(voted_county_2012) == "gtco", "geoid",  paste0(names(voted_county_2012),"_12")) 

# state 
voted_state_2012 <- voted_by_state(df_2012)
names(voted_state_2012) <- ifelse(names(voted_state_2012) == "gestfips", "geoid",  paste0(names(voted_state_2012),"_12"))

## merge voting
final_voted2012 <- rbind(voted_county_2012, voted_state_2012) #30 x 17

### Calc eligible voters ##
# county
eligible_county_2012 <- voting_age_county(df_2012)
names(eligible_county_2012) <- ifelse(names(eligible_county_2012) == "gtco", "geoid",  paste0(names(eligible_county_2012),"_12"))

# state 
eligible_state_2012 <- voting_age_state(df_2012)
names(eligible_state_2012) <- ifelse(names(eligible_state_2012) == "gestfips", "geoid",  paste0(names(eligible_state_2012),"_12"))

## merge eligible voters
final_eligible2012 <- rbind(eligible_county_2012, eligible_state_2012)



## Calc Registered to Vote ## 
# county
registered_county_2012 <- registered_by_county(df_2012)
names(registered_county_2012) <- ifelse(names(registered_county_2012) == "gtco", "geoid",  paste0(names(registered_county_2012),"_12"))

# state
registered_state_2012 <- registered_by_state(df_2012)
names(registered_state_2012) <- ifelse(names(registered_state_2012) == "gestfips", "geoid",  paste0(names(registered_state_2012),"_12"))

# merge registered to vote
final_registered2012 <- rbind(registered_county_2012, registered_state_2012)


# Merge voting & citizen voting age pop
final_2012 <- full_join(final_voted2012, final_eligible2012, by = "geoid")
final_2012 <- full_join(final_2012, final_registered2012, by = "geoid")



# 2014 data ---------------------------------------------------------------

### Calc voted ###
# county
voted_county_2014 <- voted_by_county(df_2014)
names(voted_county_2014) <- ifelse(names(voted_county_2014) == "gtco", "geoid",  paste0(names(voted_county_2014),"_14")) 

# state 
voted_state_2014 <- voted_by_state(df_2014)
names(voted_state_2014) <- ifelse(names(voted_state_2014) == "gestfips", "geoid",  paste0(names(voted_state_2014),"_14"))

## merge voting
final_voted2014 <- rbind(voted_county_2014, voted_state_2014)


### Calc eligible voters ##
# county
eligible_county_2014 <- voting_age_county(df_2014)
names(eligible_county_2014) <- ifelse(names(eligible_county_2014) == "gtco", "geoid",  paste0(names(eligible_county_2014),"_14"))

# state 
eligible_state_2014 <- voting_age_state(df_2014)
names(eligible_state_2014) <- ifelse(names(eligible_state_2014) == "gestfips", "geoid",  paste0(names(eligible_state_2014),"_14"))

## merge eligible voters
final_eligible2014 <- rbind(eligible_county_2014, eligible_state_2014)



## Calc Registered to Vote ## 
# county
registered_county_2014 <- registered_by_county(df_2014)
names(registered_county_2014) <- ifelse(names(registered_county_2014) == "gtco", "geoid",  paste0(names(registered_county_2014),"_14"))

# state
registered_state_2014 <- registered_by_state(df_2014)
names(registered_state_2014) <- ifelse(names(registered_state_2014) == "gestfips", "geoid",  paste0(names(registered_state_2014),"_14"))

# merge registered to vote
final_registered2014 <- rbind(registered_county_2014, registered_state_2014)


# Merge voting & citizen voting age pop
final_2014 <-  full_join(final_voted2014, final_eligible2014, by = "geoid")
final_2014 <-  full_join(final_2014, final_registered2014)


# 2016 data ---------------------------------------------------------------

### Calc voted ###
# county
voted_county_2016 <- voted_by_county(df_2016)
names(voted_county_2016) <- ifelse(names(voted_county_2016) == "gtco", "geoid",  paste0(names(voted_county_2016),"_16")) 

# state 
voted_state_2016 <- voted_by_state(df_2016)
names(voted_state_2016) <- ifelse(names(voted_state_2016) == "gestfips", "geoid",  paste0(names(voted_state_2016),"_16"))

## merge voting
final_voted2016 <- rbind(voted_county_2016, voted_state_2016)


### Calc eligible voters ##
# county
eligible_county_2016 <- voting_age_county(df_2016)
names(eligible_county_2016) <- ifelse(names(eligible_county_2016) == "gtco", "geoid",  paste0(names(eligible_county_2016),"_16"))

# state 
eligible_state_2016 <- voting_age_state(df_2016)
names(eligible_state_2016) <- ifelse(names(eligible_state_2016) == "gestfips", "geoid",  paste0(names(eligible_state_2016),"_16"))

## merge eligible voters
final_eligible2016 <- rbind(eligible_county_2016, eligible_state_2016)


## Calc Registered to Vote ## 
# county
registered_county_2016 <- registered_by_county(df_2016)
names(registered_county_2016) <- ifelse(names(registered_county_2016) == "gtco", "geoid",  paste0(names(registered_county_2016),"_16"))

# state
registered_state_2016 <- registered_by_state(df_2016)
names(registered_state_2016) <- ifelse(names(registered_state_2016) == "gestfips", "geoid",  paste0(names(registered_state_2016),"_16"))

# merge registered to vote
final_registered2016 <- rbind(registered_county_2016, registered_state_2016)


# Merge voting & citizen voting age pop
final_2016 <-  full_join(final_voted2016, final_eligible2016, by = "geoid")
final_2016 <- full_join(final_2016, final_registered2016)


# 2018 data ---------------------------------------------------------------

### Calc voted ###
# county
voted_county_2018 <- voted_by_county(df_2018)
names(voted_county_2018) <- ifelse(names(voted_county_2018) == "gtco", "geoid",  paste0(names(voted_county_2018),"_18")) 

# state 
voted_state_2018 <- voted_by_state(df_2018)
names(voted_state_2018) <- ifelse(names(voted_state_2018) == "gestfips", "geoid",  paste0(names(voted_state_2018),"_18"))

## merge voting
final_voted2018 <- rbind(voted_county_2018, voted_state_2018)


### Calc eligible voters ##
# county
eligible_county_2018 <- voting_age_county(df_2018)
names(eligible_county_2018) <- ifelse(names(eligible_county_2018) == "gtco", "geoid",  paste0(names(eligible_county_2018),"_18"))

# state 
eligible_state_2018 <- voting_age_state(df_2018)
names(eligible_state_2018) <- ifelse(names(eligible_state_2018) == "gestfips", "geoid",  paste0(names(eligible_state_2018),"_18"))

## merge eligible voters
final_eligible2018 <- rbind(eligible_county_2018, eligible_state_2018)


## Calc Registered to Vote ## 
# county
registered_county_2018 <- registered_by_county(df_2018)
names(registered_county_2018) <- ifelse(names(registered_county_2018) == "gtco", "geoid",  paste0(names(registered_county_2018),"_18"))

# state
registered_state_2018 <- registered_by_state(df_2018)
names(registered_state_2018) <- ifelse(names(registered_state_2018) == "gestfips", "geoid",  paste0(names(registered_state_2018),"_18"))

# merge registered to vote
final_registered2018 <- rbind(registered_county_2018, registered_state_2018)


# Merge voting & citizen voting age pop
final_2018 <- full_join(final_voted2018, final_eligible2018, by = "geoid")
final_2018 <-  full_join(final_2018, final_registered2018)



# 2020 data ---------------------------------------------------------------

### Calc voted ###
# county
voted_county_2020 <- voted_by_county(df_2020)
names(voted_county_2020) <- ifelse(names(voted_county_2020) == "gtco", "geoid",  paste0(names(voted_county_2020),"_20")) 

# state 
voted_state_2020 <- voted_by_state(df_2020)
names(voted_state_2020) <- ifelse(names(voted_state_2020) == "gestfips", "geoid",  paste0(names(voted_state_2020),"_20"))

## merge voting
final_voted2020 <- rbind(voted_county_2020, voted_state_2020)


### Calc eligible voters ##
# county
eligible_county_2020 <- voting_age_county(df_2020)
names(eligible_county_2020) <- ifelse(names(eligible_county_2020) == "gtco", "geoid",  paste0(names(eligible_county_2020),"_20"))

# state 
eligible_state_2020 <- voting_age_state(df_2020)
names(eligible_state_2020) <- ifelse(names(eligible_state_2020) == "gestfips", "geoid",  paste0(names(eligible_state_2020),"_20"))

## merge eligible voters
final_eligible2020 <- rbind(eligible_county_2020, eligible_state_2020)


## Calc Registered to Vote ## 
# county
registered_county_2020 <- registered_by_county(df_2020)
names(registered_county_2020) <- ifelse(names(registered_county_2020) == "gtco", "geoid",  paste0(names(registered_county_2020),"_20"))

# state
registered_state_2020 <- registered_by_state(df_2020)
names(registered_state_2020) <- ifelse(names(registered_state_2020) == "gestfips", "geoid",  paste0(names(registered_state_2020),"_20"))

# merge registered to vote
final_registered2020 <- rbind(registered_county_2020, registered_state_2020)


# Merge voting & citizen voting age pop
final_2020 <-  full_join(final_voted2020, final_eligible2020, by = "geoid")
final_2020 <-  full_join(final_2020, final_registered2020)


# 2022 data ---------------------------------------------------------------

### Calc voted ###
# county
voted_county_2022 <- voted_by_county(df_2022)
names(voted_county_2022) <- ifelse(names(voted_county_2022) == "gtco", "geoid",  paste0(names(voted_county_2022),"_22")) 

# state 
voted_state_2022 <- voted_by_state(df_2022)
names(voted_state_2022) <- ifelse(names(voted_state_2022) == "gestfips", "geoid",  paste0(names(voted_state_2022),"_22"))

## merge voting
final_voted2022 <- rbind(voted_county_2022, voted_state_2022)


### Calc eligible voters ##
# county
eligible_county_2022 <- voting_age_county(df_2022)
names(eligible_county_2022) <- ifelse(names(eligible_county_2022) == "gtco", "geoid",  paste0(names(eligible_county_2022),"_22"))

# state 
eligible_state_2022 <- voting_age_state(df_2022)
names(eligible_state_2022) <- ifelse(names(eligible_state_2022) == "gestfips", "geoid",  paste0(names(eligible_state_2022),"_22"))

## merge eligible voters
final_eligible2022 <- rbind(eligible_county_2022, eligible_state_2022)


## Calc Registered to Vote ## 
# county
registered_county_2022 <- registered_by_county(df_2022)
names(registered_county_2022) <- ifelse(names(registered_county_2022) == "gtco", "geoid",  paste0(names(registered_county_2022),"_22"))

# state
registered_state_2022 <- registered_by_state(df_2022)
names(registered_state_2022) <- ifelse(names(registered_state_2022) == "gestfips", "geoid",  paste0(names(registered_state_2022),"_22"))

# merge registered to vote
final_registered2022 <- rbind(registered_county_2022, registered_state_2022)


# Merge voting & citizen voting age pop
final_2022 <-  full_join(final_voted2022, final_eligible2022, by = "geoid")
final_2022 <-  full_join(final_2022, final_registered2022)


# merge all years together
final <- full_join(final_2012, final_2014, by = "geoid")
final <- full_join(final, final_2016, by = "geoid")
final <- full_join(final, final_2018, by = "geoid")
final <- full_join(final, final_2020, by = "geoid")
final <- full_join(final, final_2022, by = "geoid")


# Get count of years available by Geoid -----------------------------------

# ### get count of data years available by geoid --------------------------
names(final_2012) <- gsub(x = names(final_2012), pattern = "\\_12", replacement = "")  
final_2012$year <- "2012"

names(final_2014) <- gsub(x = names(final_2014), pattern = "\\_14", replacement = "")  
final_2014$year <- "2014"

names(final_2016) <- gsub(x = names(final_2016), pattern = "\\_16", replacement = "")  
final_2016$year <- "2016"

names(final_2018) <- gsub(x = names(final_2018), pattern = "\\_18", replacement = "")  
final_2018$year <- "2018"

names(final_2020) <- gsub(x = names(final_2020), pattern = "\\_20", replacement = "")  
final_2020$year <- "2020"

names(final_2022) <- gsub(x = names(final_2022), pattern = "\\_22", replacement = "")  
final_2022$year <- "2022"

geoid_counts <- rbind(final_2012, final_2014)
geoid_counts <- rbind(geoid_counts, final_2016)
geoid_counts <- rbind(geoid_counts, final_2018)
geoid_counts <- rbind(geoid_counts, final_2020)
geoid_counts <- rbind(geoid_counts, final_2022)

geoid_counts <- geoid_counts %>% group_by(geoid) %>% summarize(num_yrs = n())

# merge with number of years
final <- final %>% left_join(geoid_counts, by = "geoid")



# Calculate averages and total count ------------------------------------------------------

final_df <-  final %>% mutate(
  
  ## average total voting age population ##
  num_total_va_pop = rowSums(select(.,num_total_va_pop_12, num_total_va_pop_14, num_total_va_pop_16, num_total_va_pop_18, num_total_va_pop_20, num_total_va_pop_22), na.rm = TRUE)/num_yrs,
  
  num_latino_va_pop = rowSums(select(.,num_latino_va_pop_12, num_latino_va_pop_14, num_latino_va_pop_16, num_latino_va_pop_18, num_latino_va_pop_20, num_latino_va_pop_22), na.rm = TRUE)/num_yrs,
  
  num_nh_white_va_pop = rowSums(select(.,num_nh_white_va_pop_12, num_nh_white_va_pop_14, num_nh_white_va_pop_16,num_nh_white_va_pop_18, num_nh_white_va_pop_20, num_nh_white_va_pop_22), na.rm = TRUE)/num_yrs,
  
  num_nh_black_va_pop = rowSums(select(.,num_nh_black_va_pop_12, num_nh_black_va_pop_14, num_nh_black_va_pop_16, num_nh_black_va_pop_18, num_nh_black_va_pop_20, num_nh_black_va_pop_22), na.rm = TRUE)/num_yrs,
  
  num_aian_va_pop =  rowSums(select(.,num_aian_va_pop_12, num_aian_va_pop_14, num_aian_va_pop_16, num_aian_va_pop_18, num_aian_va_pop_20, num_aian_va_pop_22), na.rm = TRUE)/num_yrs,
  
  num_nh_asian_va_pop = rowSums(select(.,num_nh_asian_va_pop_12, num_nh_asian_va_pop_14, num_nh_asian_va_pop_16, num_nh_asian_va_pop_18, num_nh_asian_va_pop_20, num_nh_asian_va_pop_22), na.rm = TRUE)/num_yrs,
  
  num_pacisl_va_pop = rowSums(select(.,num_pacisl_va_pop_12, num_pacisl_va_pop_14, num_pacisl_va_pop_16, num_pacisl_va_pop_18, num_pacisl_va_pop_20, num_pacisl_va_pop_22), na.rm = TRUE)/num_yrs,
  
  num_nh_twoormor_va_pop = rowSums(select(.,num_nh_twoormor_va_pop_12, num_nh_twoormor_va_pop_14, num_nh_twoormor_va_pop_16, num_nh_twoormor_va_pop_18, num_nh_twoormor_va_pop_20, num_nh_twoormor_va_pop_22), na.rm = TRUE)/num_yrs,
  
  
  ## sum registered non-voters count ##
  count_total_reg_nv = rowSums(select(.,count_total_reg_nv_12, count_total_reg_nv_14, count_total_reg_nv_16, count_total_reg_nv_18, count_total_reg_nv_20, count_total_reg_nv_22), na.rm = TRUE),
  
  count_latino_reg_nv = rowSums(select(.,count_latino_reg_nv_12, count_latino_reg_nv_14, count_latino_reg_nv_16, count_latino_reg_nv_18, count_latino_reg_nv_20, count_latino_reg_nv_22), na.rm = TRUE),
  
  count_nh_white_reg_nv = rowSums(select(.,count_nh_white_reg_nv_12, count_nh_white_reg_nv_14, count_nh_white_reg_nv_16, count_nh_white_reg_nv_18, count_nh_white_reg_nv_20, count_nh_white_reg_nv_22), na.rm = TRUE),
  
  count_nh_black_reg_nv = rowSums(select(.,count_nh_black_reg_nv_12, count_nh_black_reg_nv_14, count_nh_black_reg_nv_16, count_nh_black_reg_nv_18, count_nh_black_reg_nv_20, count_nh_black_reg_nv_22), na.rm = TRUE), 
  
  count_aian_reg_nv = rowSums(select(.,count_aian_reg_nv_12, count_aian_reg_nv_14, count_aian_reg_nv_16, count_aian_reg_nv_18, count_aian_reg_nv_20, count_aian_reg_nv_22), na.rm = TRUE), 
  
  count_nh_asian_reg_nv = rowSums(select(.,count_nh_asian_reg_nv_12, count_nh_asian_reg_nv_14, count_nh_asian_reg_nv_16, count_nh_asian_reg_nv_18, count_nh_asian_reg_nv_20, count_nh_asian_reg_nv_22), na.rm = TRUE), 
  
  count_pacisl_reg_nv = rowSums(select(.,count_pacisl_reg_nv_12, count_pacisl_reg_nv_14, count_pacisl_reg_nv_16, count_pacisl_reg_nv_18, count_pacisl_reg_nv_20, count_pacisl_reg_nv_22), na.rm = TRUE), 
  
  count_nh_twoormor_reg_nv = rowSums(select(.,count_nh_twoormor_reg_nv_12, count_nh_twoormor_reg_nv_14, count_nh_twoormor_reg_nv_16, count_nh_twoormor_reg_nv_18, count_nh_twoormor_reg_nv_20, count_nh_twoormor_reg_nv_22), na.rm = TRUE), 
  
  
  ## sum voted count
  count_total_voted = rowSums(select(.,count_total_voted_12, count_total_voted_14, count_total_voted_16,count_total_voted_18, count_total_voted_20, count_total_voted_22), na.rm = TRUE),
  
  count_latino_voted = rowSums(select(.,count_latino_voted_12, count_latino_voted_14, count_latino_voted_16, count_latino_voted_18, count_latino_voted_20, count_latino_voted_22), na.rm = TRUE),
  
  count_nh_white_voted = rowSums(select(.,count_nh_white_voted_12, count_nh_white_voted_14, count_nh_white_voted_16, count_nh_white_voted_18, count_nh_white_voted_20, count_nh_white_voted_22), na.rm = TRUE),
  
  count_nh_black_voted = rowSums(select(.,count_nh_black_voted_12, count_nh_black_voted_14, count_nh_black_voted_16, count_nh_black_voted_18, count_nh_black_voted_20, count_nh_black_voted_22), na.rm = TRUE), 
  
  count_aian_voted = rowSums(select(.,count_aian_voted_12, count_aian_voted_14, count_aian_voted_16, count_aian_voted_18, count_aian_voted_20, count_aian_voted_22), na.rm = TRUE), 
  
  count_nh_asian_voted = rowSums(select(.,count_nh_asian_voted_12, count_nh_asian_voted_14, count_nh_asian_voted_16, count_nh_asian_voted_18, count_nh_asian_voted_20, count_nh_asian_voted_22), na.rm = TRUE), 
  
  count_pacisl_voted = rowSums(select(.,count_pacisl_voted_12, count_pacisl_voted_14, count_pacisl_voted_16, count_pacisl_voted_18, count_pacisl_voted_20, count_pacisl_voted_22), na.rm = TRUE), 
  
  count_nh_twoormor_voted = rowSums(select(.,count_nh_twoormor_voted_12, count_nh_twoormor_voted_14,  count_nh_twoormor_voted_16, count_nh_twoormor_voted_18, count_nh_twoormor_voted_20, count_nh_twoormor_voted_22), na.rm = TRUE),
  
  
  ## average num registered non-voters ##
  num_total_reg_nv = rowSums(select(.,num_total_reg_nv_12, num_total_reg_nv_14, num_total_reg_nv_16, num_total_reg_nv_18, num_total_reg_nv_20, num_total_reg_nv_22), na.rm = TRUE)/num_yrs,
  
  num_latino_reg_nv = rowSums(select(.,num_latino_reg_nv_12, num_latino_reg_nv_14, num_latino_reg_nv_16, num_latino_reg_nv_18, num_latino_reg_nv_20, num_latino_reg_nv_22), na.rm = TRUE)/num_yrs,
  
  num_nh_white_reg_nv = rowSums(select(.,num_nh_white_reg_nv_12, num_nh_white_reg_nv_14, num_nh_white_reg_nv_16, num_nh_white_reg_nv_18, num_nh_white_reg_nv_20, num_nh_white_reg_nv_22), na.rm = TRUE)/num_yrs,
  
  num_nh_black_reg_nv = rowSums(select(.,num_nh_black_reg_nv_12, num_nh_black_reg_nv_14, num_nh_black_reg_nv_16, num_nh_black_reg_nv_18, num_nh_black_reg_nv_20, num_nh_black_reg_nv_22), na.rm = TRUE)/num_yrs,
  
  num_aian_reg_nv = rowSums(select(.,num_aian_reg_nv_12, num_aian_reg_nv_14, num_aian_reg_nv_16, num_aian_reg_nv_18, num_aian_reg_nv_20, num_aian_reg_nv_22), na.rm = TRUE)/num_yrs,
  
  num_nh_asian_reg_nv = rowSums(select(.,num_nh_asian_reg_nv_12, num_nh_asian_reg_nv_14, num_nh_asian_reg_nv_16, num_nh_asian_reg_nv_18, num_nh_asian_reg_nv_20, num_nh_asian_reg_nv_22), na.rm = TRUE)/num_yrs,
  
  num_pacisl_reg_nv = rowSums(select(.,num_pacisl_reg_nv_12, num_pacisl_reg_nv_14, num_pacisl_reg_nv_16, num_pacisl_reg_nv_18, num_pacisl_reg_nv_20, num_pacisl_reg_nv_22), na.rm = TRUE)/num_yrs, 
  
  num_nh_twoormor_reg_nv = rowSums(select(.,num_nh_twoormor_reg_nv_12, num_nh_twoormor_reg_nv_14, num_nh_twoormor_reg_nv_16, num_nh_twoormor_reg_nv_18, num_nh_twoormor_reg_nv_20, num_nh_twoormor_reg_nv_22), na.rm = TRUE)/num_yrs, 
  
  
  ## average num voted ##
  num_total_voted = rowSums(select(.,num_total_voted_12, num_total_voted_14,  num_total_voted_16, num_total_voted_18, num_total_voted_20, num_total_voted_22), na.rm = TRUE)/num_yrs,
  
  num_latino_voted = rowSums(select(.,num_latino_voted_12, num_latino_voted_14, num_latino_voted_16, num_latino_voted_18, num_latino_voted_20, num_latino_voted_22), na.rm = TRUE)/num_yrs,
  
  num_nh_white_voted = rowSums(select(.,num_nh_white_voted_12, num_nh_white_voted_14, num_nh_white_voted_16, num_nh_white_voted_18, num_nh_white_voted_20, num_nh_white_voted_22), na.rm = TRUE)/num_yrs,
  
  num_nh_black_voted = rowSums(select(.,num_nh_black_voted_12, num_nh_black_voted_14, num_nh_black_voted_16, num_nh_black_voted_18, num_nh_black_voted_20, num_nh_black_voted_22), na.rm = TRUE)/num_yrs,
  
  num_aian_voted =  rowSums(select(.,num_aian_voted_12, num_aian_voted_14, num_aian_voted_16, num_aian_voted_18, num_aian_voted_20, num_aian_voted_22), na.rm = TRUE)/num_yrs,
  
  num_nh_asian_voted = rowSums(select(.,num_nh_asian_voted_12, num_nh_asian_voted_14, num_nh_asian_voted_16, num_nh_asian_voted_18, num_nh_asian_voted_20, num_nh_asian_voted_22), na.rm = TRUE)/num_yrs,
  
  num_pacisl_voted = rowSums(select(.,num_pacisl_voted_12, num_pacisl_voted_14, num_pacisl_voted_16, num_pacisl_voted_18, num_pacisl_voted_20, num_pacisl_voted_22), na.rm = TRUE)/num_yrs,
  
  num_nh_twoormor_voted = rowSums(select(.,num_nh_twoormor_voted_12, num_nh_twoormor_voted_14, num_nh_twoormor_voted_16, num_nh_twoormor_voted_18, num_nh_twoormor_voted_20, num_nh_twoormor_voted_22), na.rm = TRUE)/num_yrs
  
  
) %>% select( geoid, num_total_va_pop,num_latino_va_pop,num_nh_white_va_pop,num_nh_black_va_pop, num_aian_va_pop, num_nh_asian_va_pop, num_pacisl_va_pop, num_nh_twoormor_va_pop,
              count_total_reg_nv, count_latino_reg_nv, count_nh_white_reg_nv, count_nh_black_reg_nv, count_aian_reg_nv, count_nh_asian_reg_nv, count_pacisl_reg_nv, 
              count_nh_twoormor_reg_nv,
              count_total_voted,count_latino_voted,count_nh_white_voted,count_nh_black_voted,count_aian_voted, count_nh_asian_voted, count_pacisl_voted, count_nh_twoormor_voted,
              num_total_reg_nv, num_latino_reg_nv, num_nh_white_reg_nv, num_nh_black_reg_nv, num_aian_reg_nv, num_nh_asian_reg_nv, num_pacisl_reg_nv, num_nh_twoormor_reg_nv,
              num_total_voted, num_latino_voted, num_nh_white_voted, num_nh_black_voted, num_aian_voted, num_nh_asian_voted, num_pacisl_voted, num_nh_twoormor_voted, num_yrs      
              
)


# Screening and calculate raw/rate ---------------------------------------------------------------
threshold = 10

final_df_screened <- final_df %>%
  mutate(total_reg_voters = round(num_total_voted + num_total_reg_nv, 0),
         
         latino_reg_voters = round(num_latino_voted + num_latino_reg_nv, 0),
         
         nh_white_reg_voters = round(num_nh_white_voted + num_nh_white_reg_nv, 0),
         
         nh_black_reg_voters = round(num_nh_black_voted + num_nh_black_reg_nv, 0),
         
         aian_reg_voters = round(num_aian_voted + num_aian_reg_nv, 0),
         
         nh_asian_reg_voters = round(num_nh_asian_voted + num_nh_asian_reg_nv, 0),
         
         pacisl_reg_voters = round(num_pacisl_voted + num_pacisl_reg_nv, 0),
         
         nh_twoormor_reg_voters = round(num_nh_twoormor_voted + num_nh_twoormor_reg_nv, 0),
         
         total_raw = ifelse(count_total_voted + count_total_reg_nv < threshold, NA, round(num_total_voted + num_total_reg_nv, 0)),
         
         latino_raw = ifelse(count_latino_voted + count_latino_reg_nv < threshold, NA, round(num_latino_voted + num_latino_reg_nv, 0)),
         
         nh_white_raw = ifelse(count_nh_white_voted + count_nh_white_reg_nv < threshold, NA, round(num_nh_white_voted + num_nh_white_reg_nv, 0)),
         
         nh_black_raw = ifelse(count_nh_black_voted + count_nh_black_reg_nv < threshold, NA, round(num_nh_black_voted + num_nh_black_reg_nv, 0)),
         
         aian_raw = ifelse(count_aian_voted + count_aian_reg_nv < 10, NA, round(num_aian_voted + num_aian_reg_nv, 0)),
         
         nh_asian_raw = ifelse(count_nh_asian_voted + count_nh_asian_reg_nv < threshold, NA, round(num_nh_asian_voted + num_nh_asian_reg_nv, 0)),
         
         pacisl_raw = ifelse(count_pacisl_voted + count_pacisl_reg_nv < threshold, NA, round(num_pacisl_voted + num_pacisl_reg_nv, 0)),
         
         nh_twoormor_raw = ifelse(count_nh_twoormor_voted + count_nh_twoormor_reg_nv < threshold, NA, round(num_nh_twoormor_voted + num_nh_twoormor_reg_nv, 0)),
         
         total_rate = ifelse(count_total_voted + count_total_reg_nv < threshold, NA, (num_total_voted + num_total_reg_nv) / num_total_va_pop * 100),
         
         latino_rate = ifelse(count_latino_voted + count_latino_reg_nv < threshold, NA,  (num_latino_voted + num_latino_reg_nv) / num_latino_va_pop * 100),
         
         nh_white_rate = ifelse(count_nh_white_voted + count_nh_white_reg_nv < threshold, NA,  (num_nh_white_voted + num_nh_white_reg_nv) / num_nh_white_va_pop * 100),
         
         nh_black_rate = ifelse(count_nh_black_voted + count_nh_black_reg_nv < threshold, NA, (num_nh_black_voted + num_nh_black_reg_nv) / num_nh_black_va_pop * 100),
         
         aian_rate = ifelse(count_aian_voted + count_aian_reg_nv < 10, NA, (num_aian_voted + num_aian_reg_nv) / num_aian_va_pop * 100),
         
         nh_asian_rate = ifelse(count_nh_asian_voted + count_nh_asian_reg_nv < 10, NA,(num_nh_asian_voted + num_nh_asian_reg_nv) / num_nh_asian_va_pop * 100),
         
         pacisl_rate = ifelse(count_pacisl_voted + count_pacisl_reg_nv < threshold, NA, (num_pacisl_voted + num_pacisl_reg_nv) / num_pacisl_va_pop * 100),
         
         nh_twoormor_rate = ifelse(count_nh_twoormor_voted + count_nh_twoormor_reg_nv < threshold, NA, (num_nh_twoormor_voted + num_nh_twoormor_reg_nv) / num_nh_twoormor_va_pop * 100)
         
  ) %>%
  
  
  select( geoid, total_reg_voters, latino_reg_voters, nh_white_reg_voters, nh_black_reg_voters, aian_reg_voters, nh_asian_reg_voters, pacisl_reg_voters, nh_twoormor_reg_voters,
          
          count_total_voted, count_latino_voted, count_nh_white_voted, count_nh_black_voted, count_aian_voted, count_nh_asian_voted, count_pacisl_voted, count_nh_twoormor_voted,
          
          count_total_reg_nv, count_latino_reg_nv, count_nh_white_reg_nv, count_nh_black_reg_nv, count_aian_reg_nv, count_nh_asian_reg_nv, count_pacisl_reg_nv, count_nh_twoormor_reg_nv,
          
          num_total_va_pop, num_latino_va_pop, num_nh_white_va_pop, num_nh_black_va_pop, num_aian_va_pop, num_nh_asian_va_pop,num_pacisl_va_pop, num_nh_twoormor_va_pop,
          
          total_raw, latino_raw, nh_white_raw, nh_black_raw, aian_raw,nh_asian_raw, pacisl_raw, nh_twoormor_raw,
          
          total_rate,  latino_rate, nh_white_rate,  nh_black_rate, aian_rate,  nh_asian_rate,  pacisl_rate, nh_twoormor_rate, num_yrs      
          
  )


# Calcs for _reg_voters / count_x_voted / num_x_reg_nv that had all NA values returned zeroes. Changing these to NA. Does not impact _raw or _rate columns.
final_df_screened[final_df_screened == 0] <- NA


# #get census geoids ------------------------------------------------------
#census_api_key("25fb5e48345b42318ae435e4dcd28ad3f196f2c4", overwrite = TRUE)
census_api_key(census_key1)

ca <- get_acs(geography = "county", 
              variables = c("B01001_001"), 
              state = "CA", 
              year = 2020)

ca <- ca[,1:2]
ca$NAME <- gsub(" County, California", "", ca$NAME)
names(ca) <- c("geoid", "geoname")



#add county geonames
df <- merge(x=ca,y=final_df_screened,by="geoid", all=T)
#add state geoname
df$geoname[is.na(df$geoname)] <- "California"

# make d 
d <- df

############## CALC RACE COUNTS STATS ##############
############ To use the following RC Functions, 'd' will need the following columns at minimum: 
############ geoid and total and raced _rate (following RC naming conventions) columns. If you use a rate calc function, you will need _pop and _raw columns as well.

#set source for RC Functions script
#source("W:/Project/RACE COUNTS/2022_v4/RaceCounts/RC_Functions.R")
source("https://raw.githubusercontent.com/catalystcalifornia/RaceCounts/main/Functions/RC_Functions.R")

d$asbest = 'max'    #YOU MUST UPDATE THIS FIELD AS NECESSARY: assign 'min' or 'max'

d <- count_values(d) #calculate number of "_rate" values
d <- calc_best(d) #calculate best rates -- be sure to update asbest accordingly before running this function.
d <- calc_diff(d) #calculate difference from best
d <- calc_avg_diff(d) #calculate (row wise) mean difference from best
d <- calc_s_var(d) #calculate (row wise) population or sample variance. be sure to use calc_s_var for sample data or calc_p_var for population data.
d <- calc_id(d) #calculate index of disparity


#split STATE into separate table and format id, name columns
state_table <- d[d$geoname == 'California', ]

#calculate STATE z-scores
state_table <- calc_state_z(state_table)

state_table <- rename(state_table, state_id = geoid, state_name = geoname)
View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
View(county_table)

###update info for postgres tables###
county_table_name <- "arei_demo_registered_voters_county_2024"
state_table_name <- "arei_demo_registered_voters_state_2024"

indicator <- "Annual average percent of registered voters among the citizen voting age population."

source <- "CPS 2012, 2014, 2016, 2018, 2020, and 2022 averages https://www.census.gov/topics/public-sector/voting/data.html"

rc_schema <- "v6"

#send tables to postgres
#to_postgres(county_table, state_table)

dbDisconnect(con)



