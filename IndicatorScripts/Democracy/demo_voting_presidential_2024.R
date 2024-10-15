#install packages if not already installed
list.of.packages <- c("DBI", "tidyverse","RPostgreSQL", "tidycensus")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

### load packages
library(RPostgreSQL)
library(DBI)
library(tidyverse)
library(tidycensus)


# get data: download and read file or just read file if already downloaded----------------------------------------------------------------
 url <- "https://www2.census.gov/programs-surveys/cps/datasets/2020/supp/nov20pub.csv"
 file <- "W:/Data/Democracy/Current Population Survey Voting and Registration/2020/nov20pub.csv"

  if(!file.exists(file)) {
    download.file(url=url, destfile=file) 
    df_2020 <- read_csv(file, 
                        col_types = cols(
                          gestfips = "character",
                          gtcbsa = "character",
                          gtco = "character",
                          gtcsa = "character"))
   } else { 
     df_2020 <- read_csv(file, 
                        col_types = cols(
                           gestfips = "character",
                           gtcbsa = "character",
                           gtco = "character",
                           gtcsa = "character"))
   }


#2020 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov20.pdf

#DATA CLEANING FUNCTION: add missing leading zeroes to geoids. add state fips (06) to CA county gtco's.
#see p31-33 of metadata for more: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov20.pdf
clean_cps <- function(x) {
  x$gestfips <- ifelse (x$gestfips == '6', paste("0",x$gestfips, sep = ""), x$gestfips)
  x$gtcbsa <- ifelse (nchar(x$gtcbsa) == 1, paste("0000",x$gtcbsa, sep = ""), x$gtcbsa)
  x$gtcco <- ifelse (x$gtco == "0", NA, x$gtco) 
  x$gtco <- ifelse (nchar(x$gtco) == 1, paste("00",x$gtco, sep = ""), x$gtco)
  x$gtco <- ifelse (nchar(x$gtco) == 2, paste("0",x$gtco, sep = ""), x$gtco)
  x$gtco <- ifelse(x$gtco != '06', paste0("06",x$gtco), x$gtco)
  x$gtcsa <- ifelse (nchar(x$gtcsa) == 1, paste("00",x$gtcsa, sep = ""), x$gtcsa)
  
  #filter for just CA data
  x <- filter(x, gestfips == '06')
  ## divide PWSSWGT by 1,000 because the data dictionary specifies this is 4 implied decimal places. Chris already did this for 2012 and 2016 data
  if(!"pwsswgtnum" %in% colnames(x))
  {
    x$pwsswgtnum <- x$PWSSWGT/10000
  }
  
  #make column names lowercase
  colnames(x) <- tolower(colnames(x))  
  
  #correct/make column types consistent across data years
  x$pes1 <- as.character(x$pes1)
  x$hrintsta <- as.character(x$hrintsta)
  x$prpertyp <- as.character(x$prpertyp)
  x$prtage <- as.double(x$prtage)
  
  return(x)  
}

df_2020 <- clean_cps(df_2020)


#2016 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov16.pdf
#2012 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov12.pdf


#get already QA'd 2016 and 2012 data 
source("W:\\RDA Team\\R\\credentials_source.R")
# create connection
con <- connect_to_db("racecounts")
census_api_key(census_key1)


# load RC view 
df_2016 <- dbGetQuery(con, "SELECT * FROM data.cps_2016_voting_supplement")
df_2012 <- dbGetQuery(con, "SELECT * FROM data.cps_2012_voting_supplement")
# close the connection
dbDisconnect(con)


# Functions to summarize the 3 DFs ----------------------------------------
## Function 1: Voted by County ---------------------------------------------

voted_by_county <- function(data) {
  # total
  data %>% 
    filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18",  gtco != "06000"
    ) %>%
    group_by(
      gtco # group by county code
    ) %>%
    summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
    
    ### merge with Latino
    left_join(

      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1",  gtco != "06000" # Hispanic or Latino 
        ) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
    
    ### merge with nh White
    left_join(
      
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1",  gtco != "06000" # non-hispanic White
        ) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
    
    ### merge with nh Black
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2",  gtco != "06000" # non-hispanic Black
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
    
    ### merge with all AIAN
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24"),  gtco != "06000" # AIAN
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
    
    ### merge with nh Asian
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4",  gtco != "06000" # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
    
    
    ### merge with all PacIsl
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24"),  gtco != "06000" # PACISL
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
    
    ### merge with nh Two or more races
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26"),  gtco != "06000" # Two or More
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gtco # group by county code
        ) %>%
        summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))
  
}



## Function 2: Voted by State -------------------------------------------

voted_by_state <- function(data) {
  # total
  data %>% 
    filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18"
    ) %>%
    group_by(
      gestfips # group by state
    ) %>%
    summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
    
    
    ###### merge with Latino
    left_join(
      
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1" # Hispanic or Latino 
        ) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
    
    ### merge with nh white
    left_join(
      
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1" # non-hispanic White
        ) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
    
    #### merge with nh black
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2"# non-hispanic Black
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
    
    #### merge with All AIAN
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24")# AIAN
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
    
    ### merge with nh Asian
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4" # non-hispanic Asian
        ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
    
    ### merge with all PACISL
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24")# PACISL
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips# group by state
        ) %>%
        summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
    
    ### merge with nh two or more races
    left_join(
      data %>% 
        filter( pes1 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26") # Two or More
        ) %>%
        select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))

}



## Function 3: Voting Age by County -------------------------------------

### Total
voting_age_county <- function(data) {
  # Total
  data %>%
    mutate(prtage = as.numeric(prtage)) %>%
    filter( prtage >=18, prcitshp != "5",  gtco != "06000") %>%
    group_by(
      gtco #group by county code
    ) %>%
    summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
    
    
    # Hispanic/Latino
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "1",  gtco != "06000") %>%
        group_by(
          gtco# group by county code
        ) %>%
        summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
    
    
    # NH WHITE
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1",  gtco != "06000") %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Black
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2",  gtco != "06000") %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### All AIAN
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24"),  gtco != "06000") # AIAN
      %>% 
        group_by(gtco) %>% #group by county code
        summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
    
    ### NH Asian
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4",  gtco != "06000") %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### All PacIsl
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24"),  gtco != "06000") %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Two or more
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26"),  gtco != "06000") %>%
        group_by(
          gtco #group by county code
        ) %>%
        summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
  
}


## Function 4: Voting Age by State --------------------------------------

voting_age_state <- function(data) {
  
  ### Total
  data %>%
    mutate(prtage = as.numeric(prtage)) %>%
    filter( prtage >=18, prcitshp != "5") %>%
    group_by(
      gestfips #group by state
    ) %>%
    summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
    
    
    # Hispanic/Latino
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "1") %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
    
    
    # NH WHITE
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1") %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Black
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2") %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### All AIAN
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24")) # AIAN
      %>% 
        group_by(gestfips) %>% #group by state
        summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Asian
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4") %>%
        group_by(
          gestfips # group by state
        ) %>%
        summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### ALL PacIsl
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24")) %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
    
    
    ### NH Two or more
    left_join(
      data %>%
        mutate(prtage = as.numeric(prtage)) %>%
        filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26")) %>%
        group_by(
          gestfips #group by state
        ) %>%
        summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
  
}


# End FUNCTIONS ----------------------------------------------------------


# 2012 data ---------------------------------------------------------------

# Calc voted
voted_county_2012 <- voted_by_county(df_2012)
names(voted_county_2012) <- ifelse(names(voted_county_2012) == "gtco", "geoid",  paste0(names(voted_county_2012),"_12")) 

voted_state_2012 <- voted_by_state(df_2012)
names(voted_state_2012) <- ifelse(names(voted_state_2012) == "gestfips", "geoid",  paste0(names(voted_state_2012),"_12"))

  ## merge voting
  final_voted2012 <- rbind(voted_county_2012, voted_state_2012)

# Calc eligible voters
eligible_county_2012 <- voting_age_county(df_2012)
names(eligible_county_2012) <- ifelse(names(eligible_county_2012) == "gtco", "geoid",  paste0(names(eligible_county_2012),"_12"))

eligible_state_2012 <- voting_age_state(df_2012)
names(eligible_state_2012) <- ifelse(names(eligible_state_2012) == "gestfips", "geoid",  paste0(names(eligible_state_2012),"_12"))

  ## merge eligible voters
  final_eligible2012 <- rbind(eligible_county_2012, eligible_state_2012)

# Merge voting & citizen voting age pop
final_2012 <- merge(final_voted2012, final_eligible2012, by = "geoid")



# 2016 data  --------------------------------------------------------------

# Calc voted
voted_county_2016 <- voted_by_county(df_2016)
names(voted_county_2016) <- ifelse(names(voted_county_2016) == "gtco", "geoid",  paste0(names(voted_county_2016),"_16"))

voted_state_2016 <- voted_by_state(df_2016)
names(voted_state_2016) <- ifelse(names(voted_state_2016) == "gestfips", "geoid",  paste0(names(voted_state_2016),"_16"))

  ## merge voting
  final_voted2016 <- rbind(voted_county_2016, voted_state_2016)

# Calc eligible voters
eligible_county_2016<- voting_age_county(df_2016)
names(eligible_county_2016) <- ifelse(names(eligible_county_2016) == "gtco", "geoid",  paste0(names(eligible_county_2016),"_16"))

eligible_state_2016 <- voting_age_state(df_2016)
names(eligible_state_2016) <- ifelse(names(eligible_state_2016) == "gestfips", "geoid", paste0(names(eligible_state_2016),"_16"))

## merge eligible voters
final_eligible2016 <- rbind(eligible_county_2016, eligible_state_2016)

# Merge voting & citizen voting age pop
final_2016 <- merge(final_voted2016, final_eligible2016, by = "geoid")


# 2020 data ---------------------------------------------------------------

# Calc voting 
voted_county_2020 <- voted_by_county(df_2020)
names(voted_county_2020) <- ifelse(names(voted_county_2020) == "gtco", "geoid", paste0(names(voted_county_2020),"_20"))

voted_state_2020 <- voted_by_state(df_2020)
names(voted_state_2020) <- ifelse(names(voted_state_2020) == "gestfips", "geoid",  paste0(names(voted_state_2020),"_20"))

  ### merge voting
  final_voted2020 <- rbind(voted_county_2020, voted_state_2020)

# Calc citizen voting age pop
eligible_county_2020<- voting_age_county(df_2020)
names(eligible_county_2020) <- ifelse(names(eligible_county_2020) == "gtco", "geoid",  paste0(names(eligible_county_2020),"_20"))

eligible_state_2020 <- voting_age_state(df_2020)
names(eligible_state_2020) <- ifelse(names(eligible_state_2020) == "gestfips", "geoid",  paste0(names(eligible_state_2020),"_20"))

  ### merge eligible voters
  final_eligible2020 <- rbind(eligible_county_2020, eligible_state_2020)

# merge voting & citizen voting age pop
final_2020 <- merge(final_voted2020, final_eligible2020, by = "geoid")

# Merge all data years together
final <- full_join(final_2012, final_2016, by = "geoid")
final <- full_join(final, final_2020, by = "geoid")


## get count of data years available by geoid --------------------------
names(final_2012) <- gsub(x = names(final_2012), pattern = "\\_12", replacement = "")  
final_2012$year <- "2012"
names(final_2016) <- gsub(x = names(final_2016), pattern = "\\_16", replacement = "")  
final_2016$year <- "2016"
names(final_2020) <- gsub(x = names(final_2020), pattern = "\\_20", replacement = "")  
final_2020$year <- "2020"
geoid_counts <- rbind(final_2012, final_2016)
geoid_counts <- rbind(geoid_counts, final_2020)
geoid_counts <- geoid_counts %>% group_by(geoid) %>% summarize(num_yrs = n())

# merge with number of years
final <- final %>% left_join(geoid_counts, by = "geoid")

## calculate averages -----------------------------------------------
##calculate averages using # of data yrs available per geo as denominator (total voting age and total voted) and total counts

final_df <-  final %>% mutate(
  num_total_va_pop = rowSums(select(.,num_total_va_pop_12, num_total_va_pop_16, num_total_va_pop_20), na.rm = TRUE)/num_yrs,
  num_latino_va_pop = rowSums(select(.,num_latino_va_pop_12, num_latino_va_pop_16, num_latino_va_pop_20), na.rm = TRUE)/num_yrs,
  num_nh_white_va_pop = rowSums(select(.,num_nh_white_va_pop_12, num_nh_white_va_pop_16, num_nh_white_va_pop_20), na.rm = TRUE)/num_yrs,
  num_nh_black_va_pop = rowSums(select(.,num_nh_black_va_pop_12, num_nh_black_va_pop_16, num_nh_black_va_pop_20), na.rm = TRUE)/num_yrs,
  num_aian_va_pop =  rowSums(select(.,num_aian_va_pop_12, num_aian_va_pop_16, num_aian_va_pop_20), na.rm = TRUE)/num_yrs,
  num_nh_asian_va_pop = rowSums(select(.,num_nh_asian_va_pop_12, num_nh_asian_va_pop_16, num_nh_asian_va_pop_20), na.rm = TRUE)/num_yrs,
  num_pacisl_va_pop = rowSums(select(.,num_pacisl_va_pop_12, num_pacisl_va_pop_16, num_pacisl_va_pop_20), na.rm = TRUE)/num_yrs,
  num_nh_twoormor_va_pop = rowSums(select(.,num_nh_twoormor_va_pop_12, num_nh_twoormor_va_pop_16, num_nh_twoormor_va_pop_20), na.rm = TRUE)/num_yrs,
  
  count_total_voted = rowSums(select(.,count_total_voted_12, count_total_voted_16, count_total_voted_20), na.rm = TRUE),
  count_latino_voted = rowSums(select(.,count_latino_voted_12, count_latino_voted_16, count_latino_voted_20), na.rm = TRUE),
  count_nh_white_voted = rowSums(select(.,count_nh_white_voted_12, count_nh_white_voted_16, count_nh_white_voted_20), na.rm = TRUE),
  count_nh_black_voted = rowSums(select(.,count_nh_black_voted_12, count_nh_black_voted_16, count_nh_black_voted_20), na.rm = TRUE), 
  count_aian_voted = rowSums(select(.,count_aian_voted_12, count_aian_voted_16, count_aian_voted_20), na.rm = TRUE), 
  count_nh_asian_voted = rowSums(select(.,count_nh_asian_voted_12, count_nh_asian_voted_16, count_nh_asian_voted_20), na.rm = TRUE), 
  count_pacisl_voted = rowSums(select(.,count_pacisl_voted_12, count_pacisl_voted_16, count_pacisl_voted_20), na.rm = TRUE), 
  count_nh_twoormor_voted = rowSums(select(.,count_nh_twoormor_voted_12, count_nh_twoormor_voted_16, count_nh_twoormor_voted_20), na.rm = TRUE), 
  
  num_total_voted = rowSums(select(.,num_total_voted_12, num_total_voted_16, num_total_voted_20), na.rm = TRUE)/num_yrs,
  num_latino_voted = rowSums(select(.,num_latino_voted_12, num_latino_voted_16, num_latino_voted_20), na.rm = TRUE)/num_yrs,
  num_nh_white_voted = rowSums(select(.,num_nh_white_voted_12, num_nh_white_voted_16, num_nh_white_voted_20), na.rm = TRUE)/num_yrs,
  num_nh_black_voted = rowSums(select(.,num_nh_black_voted_12, num_nh_black_voted_16, num_nh_black_voted_20), na.rm = TRUE)/num_yrs,
  num_aian_voted =  rowSums(select(.,num_aian_voted_12, num_aian_voted_16, num_aian_voted_20), na.rm = TRUE)/num_yrs,
  num_nh_asian_voted = rowSums(select(.,num_nh_asian_voted_12, num_nh_asian_voted_16, num_nh_asian_voted_20), na.rm = TRUE)/num_yrs,
  num_pacisl_voted = rowSums(select(.,num_pacisl_voted_12, num_pacisl_voted_16, num_pacisl_voted_20), na.rm = TRUE)/num_yrs,
  num_nh_twoormor_voted = rowSums(select(.,num_nh_twoormor_voted_12, num_nh_twoormor_voted_16, num_nh_twoormor_voted_20), na.rm = TRUE)/num_yrs
) %>% 
  
  select(geoid, num_total_voted, num_latino_voted, num_nh_white_voted, num_nh_black_voted, num_aian_voted, num_nh_asian_voted, num_pacisl_voted,num_nh_twoormor_voted, num_total_va_pop, num_latino_va_pop, num_nh_white_va_pop, num_nh_black_va_pop, num_aian_va_pop, num_nh_asian_va_pop, num_pacisl_va_pop, num_nh_twoormor_va_pop,
         count_total_voted, count_latino_voted, count_nh_white_voted, count_nh_black_voted, count_aian_voted, count_nh_asian_voted, count_pacisl_voted, count_nh_twoormor_voted, num_yrs)


## screen according to data methodology ------------------------------

final_df_screened <- final_df %>%
  mutate(total_raw = ifelse(count_total_voted < 10, NA, round(num_total_voted, 0)),
         latino_raw = ifelse(count_latino_voted < 10, NA, round(num_latino_voted, 0)),
         nh_white_raw = ifelse(count_nh_white_voted < 10, NA, round(num_nh_white_voted, 0)),
         nh_black_raw = ifelse(count_nh_black_voted < 10, NA, round(num_nh_black_voted, 0)),
         aian_raw = ifelse(count_aian_voted  < 10, NA, round(num_aian_voted, 0)),
         nh_asian_raw =  ifelse(count_nh_asian_voted  < 10, NA, round(num_nh_asian_voted, 0)),
         pacisl_raw = ifelse(count_pacisl_voted < 10, NA, round(num_pacisl_voted, 0)),
         nh_twoormor_raw = ifelse(count_nh_twoormor_voted < 10, NA, round(num_nh_twoormor_voted, 0)),
         
         total_rate = ifelse(count_total_voted < 10, NA, (num_total_voted / num_total_va_pop * 100)),
         latino_rate = ifelse(count_latino_voted < 10, NA, (num_latino_voted / num_latino_va_pop * 100)),
         nh_white_rate = ifelse(count_nh_white_voted < 10, NA,(num_nh_white_voted / num_nh_white_va_pop * 100)),
         nh_black_rate = ifelse(count_nh_black_voted < 10, NA, (num_nh_black_voted / num_nh_black_va_pop * 100)),
         aian_rate = ifelse(count_aian_voted  < 10, NA,(num_aian_voted / num_aian_va_pop * 100)),
         nh_asian_rate =  ifelse(count_nh_asian_voted  < 10, NA,(num_nh_asian_voted / num_nh_asian_va_pop * 100)),
         pacisl_rate = ifelse(count_pacisl_voted < 10, NA, (num_pacisl_voted / num_pacisl_va_pop * 100)),
         nh_twoormor_rate = ifelse(count_nh_twoormor_voted < 10, NA, (num_nh_twoormor_voted / num_nh_twoormor_va_pop * 100))
  ) %>%
  
  select(
    geoid, num_total_voted, num_latino_voted, num_nh_white_voted, num_nh_black_voted, num_aian_voted, num_nh_asian_voted, num_pacisl_voted, num_nh_twoormor_voted,
    count_total_voted, count_latino_voted, count_nh_white_voted, count_nh_black_voted, count_aian_voted, count_nh_asian_voted,count_pacisl_voted, count_nh_twoormor_voted, 
    num_total_va_pop, num_latino_va_pop, num_nh_white_va_pop,num_nh_black_va_pop, num_aian_va_pop, num_nh_asian_va_pop, num_pacisl_va_pop, num_nh_twoormor_va_pop,
    total_raw, latino_raw, nh_white_raw, nh_black_raw, aian_raw,nh_asian_raw,  pacisl_raw, nh_twoormor_raw,
    total_rate, latino_rate, nh_white_rate, nh_black_rate, aian_rate, nh_asian_rate, pacisl_rate, nh_twoormor_rate, num_yrs
  )


## get census geoids ------------------------------------------------------


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
source("W:/Project/RACE COUNTS/2022_v4/RaceCounts/RC_Functions.R")

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
#View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
#View(county_table)

###update info for postgres tables###
county_table_name <- "arei_demo_voting_presidential_county_2022"
state_table_name <- "arei_demo_voting_presidential_state_2022"

indicator <- paste0("Created on ", Sys.Date(), ". Annual average percent of voters voting in presidential elections among eligible voting age population. This data is")

source <- "CPS 2012, 2016, and 2020 average https://www.census.gov/topics/public-sector/voting/data.html"

#send tables to postgres
#to_postgres()


