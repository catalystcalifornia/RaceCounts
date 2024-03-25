#install packages if not already installed
list.of.packages <- c("RPostgreSQL", "DBI", "tidyverse", "tidycensus", "sf")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)


### load packages
library(RPostgreSQL)
library(DBI)
library(tidyverse)
library(tidycensus)
library(sf)



# Get Data ----------------------------------------------------------------

#DATA CLEANING FUNCTION: add missing leading zeroes to geoids. add state fips (06) to CA county gtco's.
#see p31-33 of metadata for more: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov22.pdf
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
  
  if("peage" %in% colnames(x))
  {
    x <- x %>% rename(prtage = peage) 
  }
  
  x$prtage <- as.double(x$prtage)
  
  return(x)  
}


#2010 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov10.pdf
#2014 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov14.pdf
#2018 metadata: https://www2.census.gov/programs-surveys/cps/techdocs/cpsnov18.pdf


#get already QA'd 2010, 2014, and 2018 data 
source("W:\\RDA Team\\R\\credentials_source.R")
con <- connect_to_db("racecounts")

# load RC view 
df_2010 <- dbGetQuery(con, "SELECT * FROM data.cps_2010_voting_supplement")
df_2014 <- dbGetQuery(con, "SELECT * FROM data.cps_2014_voting_supplement")
df_2018 <- dbGetQuery(con, "SELECT * FROM data.cps_2018_voting_supplement")

# close the connection
#dbDisconnect(con)


# read 2022 data
df_2022 <-  read_csv(file = "W:/Data/Democracy/Current Population Survey Voting and Registration/2022/nov22pub.csv",
                     col_types = cols(
                       GESTFIPS = "character",
                       GTCBSA = "character",
                       GTCO = "character",
                       GTCSA = "character")) %>% 
  rename(gestfips = GESTFIPS, gtcbsa = GTCBSA, gtco = GTCO, gtcsa = GTCSA)


# clean data
df_2010 <- clean_cps(df_2010)
df_2014 <- clean_cps(df_2014)
df_2018 <- clean_cps(df_2018)
df_2022 <- clean_cps(df_2022)


# Functions to summarize the 4 DFs ----------------------------------------
## Function 1: Voted by County ---------------------------------------------

voted_by_county <- function(x) {
  
  if (min(x$hryear4) == '2010') {
    # total
    x <-  x %>% 
      filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18",  gtco != "06000"
      ) %>%
      group_by(
        gtco # group by county code
      ) %>%
      summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
      
      ### merge with Latino
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1",  gtco != "06000" # Hispanic or Latino 
          ) %>%
          group_by(
            gtco # group by county code
          ) %>%
          summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
      
      ### merge with nh White
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1",  gtco != "06000" # NH White
          ) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
      
      ### merge with nh Black
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2",  gtco != "06000" # NH Black
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
      
      ### merge with all AIAN
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "15", "17", "19"),  gtco != "06000" # All AIAN
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
      
      ### merge with nh Asian
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4",  gtco != "06000" # NH Asian
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco # group by county code
          ) %>%
          summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
      
      
      ### merge with all PacIsl
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "18"),  gtco != "06000" # All PACISL
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
      
      ### merge with nh Two or more races
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"),  gtco != "06000" # NH Two or More
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco # group by county code
          ) %>%
          summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))
    
    return(x)
  } 
  
  
  else {
    
    # total
    x <- x %>% 
      filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18",  gtco != "06000"
      ) %>%
      group_by(
        gtco # group by county code
      ) %>%
      summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
      
      ### merge with Latino
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1",  gtco != "06000" # Hispanic or Latino 
          ) %>%
          group_by(
            gtco # group by county code
          ) %>%
          summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
      
      ### merge with nh White
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1",  gtco != "06000" # non-hispanic White
          ) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
      
      ### merge with nh Black
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2",  gtco != "06000" # non-hispanic Black
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
      
      ### merge with all AIAN
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24"),  gtco != "06000" # All AIAN
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
      
      ### merge with nh Asian
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4",  gtco != "06000" # non-hispanic Asian
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco # group by county code
          ) %>%
          summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
      
      
      ### merge with all PacIsl
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24"),  gtco != "06000" # All PACISL
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
      
      ### merge with nh Two or more races
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26"),  gtco != "06000" # Two or More
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gtco # group by county code
          ) %>%
          summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))
    
    
    return(x)
    
  }
  
  
}



## Function 2: Voted by State -------------------------------------------

voted_by_state <- function(x) {
  if (min(x$hryear4) == '2010') {
    
    
    # total
    x <- x %>% 
      filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18"
      ) %>%
      group_by(
        gestfips # group by state
      ) %>%
      summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
      
      
      ###### merge with Latino
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1" # Hispanic or Latino 
          ) %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
      
      ### merge with nh white
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1" # non-hispanic White
          ) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
      
      #### merge with nh black
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2"# non-hispanic Black
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
      
      #### merge with All AIAN
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "15", "17", "19") # AIAN
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
      
      ### merge with nh Asian
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4" # non-hispanic Asian
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
      
      ### merge with all PACISL
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "18") # PACISL
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
      
      ### merge with nh two or more races
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"))  %>% 
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))
    
    
    
    return(x)
  }
  
  else {
    
    
    # total
    x <-  x %>% 
      filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18"
      ) %>%
      group_by(
        gestfips # group by state
      ) %>%
      summarize(num_total_voted = sum(pwsswgtnum), count_total_voted = n() ) %>%
      
      
      ###### merge with Latino
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "1" # Hispanic or Latino 
          ) %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_latino_voted = sum(pwsswgtnum), count_latino_voted = n() )) %>%
      
      ### merge with nh white
      left_join(
        
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "1" # non-hispanic White
          ) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_nh_white_voted = sum(pwsswgtnum), count_nh_white_voted = n() )) %>%
      
      #### merge with nh black
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "2"# non-hispanic Black
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_nh_black_voted = sum(pwsswgtnum), count_nh_black_voted = n() )) %>%
      
      #### merge with All AIAN
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24") # All AIAN
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_aian_voted = sum(pwsswgtnum), count_aian_voted = n() )) %>%
      
      ### merge with nh Asian
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", pehspnon == "2", ptdtrace == "4" # non-hispanic Asian
          ) %>% select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_nh_asian_voted = sum(pwsswgtnum), count_nh_asian_voted = n() )) %>%
      
      ### merge with all PACISL
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", prtage >="18", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24") # All PACISL
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips# group by state
          ) %>%
          summarize(num_pacisl_voted = sum(pwsswgtnum), count_pacisl_voted = n() )) %>%
      
      ### merge with nh two or more races
      left_join(
        x %>% 
          filter( pes1 == "1", hrintsta == "1", prpertyp == "2", pehspnon == "2", prtage >="18", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26") # Two or More
          ) %>%
          select(gtco, gestfips, pes1, hrintsta, prtage, ptdtrace, pwsswgtnum) %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_nh_twoormor_voted = sum(pwsswgtnum), count_nh_twoormor_voted = n() ))
    
    return(x)
    
  }
}


## Function 3: Voting Age by County -------------------------------------

### Total
voting_age_county <- function(x) {
  
  
  if (min(x$hryear4) == '2010') {
    
    # Total
    x <- x %>%
      mutate(prtage = as.numeric(prtage)) %>%
      filter( prtage >=18, prcitshp != "5",  gtco != "06000") %>%
      group_by(
        gtco #group by county code
      ) %>%
      summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
      
      
      # Hispanic/Latino
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "1",  gtco != "06000") %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
      
      
      # NH WHITE
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1",  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Black
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2",  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### All AIAN
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("3", "7", "10", "13", "15", "17", "19"),  gtco != "06000") # All AIAN
        %>% 
          group_by(gtco) %>% #group by county code
          summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
      
      ### NH Asian
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4",  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### All PacIsl
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("5", "9", "12", "14", "18"),  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Two or more
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"),  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
    
    
    return(x)
  }
  
  else{
    
    
    # Total
    x <-  x %>%
      mutate(prtage = as.numeric(prtage)) %>%
      filter( prtage >=18, prcitshp != "5",  gtco != "06000") %>%
      group_by(
        gtco #group by county code
      ) %>%
      summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
      
      
      # Hispanic/Latino
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "1",  gtco != "06000") %>%
          group_by(
            gtco# group by county code
          ) %>%
          summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
      
      
      # NH WHITE
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1",  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Black
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2",  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### All AIAN
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24"),  gtco != "06000") # AIAN
        %>% 
          group_by(gtco) %>% #group by county code
          summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
      
      ### NH Asian
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4",  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### All PacIsl
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24"),  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Two or more
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26"),  gtco != "06000") %>%
          group_by(
            gtco #group by county code
          ) %>%
          summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
    
    return(x)
    
  }
  
  
}

## Function 4: Voting Age by State --------------------------------------

voting_age_state <- function(x) {
  
  if (min(x$hryear4) == '2010') {
    
    ### Total
    x <-  x %>%
      mutate(prtage = as.numeric(prtage)) %>%
      filter( prtage >=18, prcitshp != "5") %>%
      group_by(
        gestfips #group by state
      ) %>%
      summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
      
      
      # Hispanic/Latino
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "1") %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
      
      
      # NH WHITE
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1") %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Black
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2") %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### All AIAN
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("3", "7", "10", "13", "15", "17", "19")) # AIAN
        %>% 
          group_by(gestfips) %>% #group by state
          summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Asian
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4") %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### ALL PacIsl
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("5", "9", "12", "14", "18")) %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Two or more
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21")) %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
    
    
    
    return(x)
  }
  
  else{
    
    
    
    ### Total
    x <- x %>%
      mutate(prtage = as.numeric(prtage)) %>%
      filter( prtage >=18, prcitshp != "5") %>%
      group_by(
        gestfips #group by state
      ) %>%
      summarize(num_total_va_pop = sum(pwsswgtnum)) %>%
      
      
      # Hispanic/Latino
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "1") %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_latino_va_pop = sum(pwsswgtnum))) %>%
      
      
      # NH WHITE
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "1") %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_nh_white_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Black
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "2") %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_nh_black_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### All AIAN
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("3", "7", "10", "13", "14", "16", "19", "20", "22", "23", "24")) # AIAN
        %>% 
          group_by(gestfips) %>% #group by state
          summarize(num_aian_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Asian
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon == "2", ptdtrace == "4") %>%
          group_by(
            gestfips # group by state
          ) %>%
          summarize(num_nh_asian_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### ALL PacIsl
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", ptdtrace %in% c("5", "9", "12", "14", "15", "18", "20", "21", "24")) %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_pacisl_va_pop = sum(pwsswgtnum))) %>%
      
      
      ### NH Two or more
      left_join(
        x %>%
          mutate(prtage = as.numeric(prtage)) %>%
          filter( prtage >=18, prcitshp != "5", pehspnon =="2", ptdtrace %in% c("6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26")) %>%
          group_by(
            gestfips #group by state
          ) %>%
          summarize(num_nh_twoormor_va_pop = sum(pwsswgtnum)))
    
    return(x)
    
  }
}

# End FUNCTIONS ----------------------------------------------------------


# 2010 data ---------------------------------------------------------------

# Calc voted
voted_county_2010 <- voted_by_county(df_2010)
names(voted_county_2010) <- ifelse(names(voted_county_2010) == "gtco", "geoid",  paste0(names(voted_county_2010),"_10")) 

voted_state_2010 <- voted_by_state(df_2010)
names(voted_state_2010) <- ifelse(names(voted_state_2010) == "gestfips", "geoid",  paste0(names(voted_state_2010),"_10"))

## merge voting
final_voted2010 <- rbind(voted_county_2010, voted_state_2010)

# Calc eligible voters
eligible_county_2010 <- voting_age_county(df_2010)
names(eligible_county_2010) <- ifelse(names(eligible_county_2010) == "gtco", "geoid",  paste0(names(eligible_county_2010),"_10"))

eligible_state_2010 <- voting_age_state(df_2010)
names(eligible_state_2010) <- ifelse(names(eligible_state_2010) == "gestfips", "geoid",  paste0(names(eligible_state_2010),"_10"))

## merge eligible voters
final_eligible2010 <- rbind(eligible_county_2010, eligible_state_2010)

# Merge voting & citizen voting age pop
final_2010 <- merge(final_voted2010, final_eligible2010, by = "geoid")


# 2014 data  --------------------------------------------------------------

# Calc voted
voted_county_2014 <- voted_by_county(df_2014)
names(voted_county_2014) <- ifelse(names(voted_county_2014) == "gtco", "geoid",  paste0(names(voted_county_2014),"_14"))

voted_state_2014 <- voted_by_state(df_2014)
names(voted_state_2014) <- ifelse(names(voted_state_2014) == "gestfips", "geoid",  paste0(names(voted_state_2014),"_14"))

## merge voting
final_voted2014 <- rbind(voted_county_2014, voted_state_2014)

# Calc eligible voters
eligible_county_2014<- voting_age_county(df_2014)
names(eligible_county_2014) <- ifelse(names(eligible_county_2014) == "gtco", "geoid",  paste0(names(eligible_county_2014),"_14"))

eligible_state_2014 <- voting_age_state(df_2014)
names(eligible_state_2014) <- ifelse(names(eligible_state_2014) == "gestfips", "geoid", paste0(names(eligible_state_2014),"_14"))

## merge eligible voters
final_eligible2014 <- rbind(eligible_county_2014, eligible_state_2014)

# Merge voting & citizen voting age pop
final_2014 <- merge(final_voted2014, final_eligible2014, by = "geoid")


# 2018 data ---------------------------------------------------------------

# Calc voting 
voted_county_2018 <- voted_by_county(df_2018)
names(voted_county_2018) <- ifelse(names(voted_county_2018) == "gtco", "geoid", paste0(names(voted_county_2018),"_18"))

voted_state_2018 <- voted_by_state(df_2018)
names(voted_state_2018) <- ifelse(names(voted_state_2018) == "gestfips", "geoid",  paste0(names(voted_state_2018),"_18"))

### merge voting
final_voted2018 <- rbind(voted_county_2018, voted_state_2018)

# Calc citizen voting age pop
eligible_county_2018<- voting_age_county(df_2018)
names(eligible_county_2018) <- ifelse(names(eligible_county_2018) == "gtco", "geoid",  paste0(names(eligible_county_2018),"_18"))

eligible_state_2018 <- voting_age_state(df_2018)
names(eligible_state_2018) <- ifelse(names(eligible_state_2018) == "gestfips", "geoid",  paste0(names(eligible_state_2018),"_18"))

### merge eligible voters
final_eligible2018 <- rbind(eligible_county_2018, eligible_state_2018)

# merge voting & citizen voting age pop
final_2018 <- merge(final_voted2018, final_eligible2018, by = "geoid")


# 2022 data ---------------------------------------------------------------

# Calc voting 
voted_county_2022 <- voted_by_county(df_2022)
names(voted_county_2022) <- ifelse(names(voted_county_2022) == "gtco", "geoid", paste0(names(voted_county_2022),"_22"))

voted_state_2022 <- voted_by_state(df_2022)
names(voted_state_2022) <- ifelse(names(voted_state_2022) == "gestfips", "geoid",  paste0(names(voted_state_2022),"_22"))

### merge voting
final_voted2022 <- rbind(voted_county_2022, voted_state_2022)

# Calc citizen voting age pop
eligible_county_2022<- voting_age_county(df_2022)
names(eligible_county_2022) <- ifelse(names(eligible_county_2022) == "gtco", "geoid",  paste0(names(eligible_county_2022),"_22"))

eligible_state_2022 <- voting_age_state(df_2022)
names(eligible_state_2022) <- ifelse(names(eligible_state_2022) == "gestfips", "geoid",  paste0(names(eligible_state_2022),"_22"))

### merge eligible voters
final_eligible2022 <- rbind(eligible_county_2022, eligible_state_2022)

# merge voting & citizen voting age pop
final_2022 <- merge(final_voted2022, final_eligible2022, by = "geoid")


# Merge all data years together
final <- full_join(final_2010, final_2014, by = "geoid")
final <- full_join(final, final_2018, by = "geoid")
final <- full_join(final, final_2022, by = "geoid")


## get count of data years available by geoid --------------------------
names(final_2010) <- gsub(x = names(final_2010), pattern = "\\_10", replacement = "")  
final_2010$year <- "2010"
names(final_2014) <- gsub(x = names(final_2014), pattern = "\\_14", replacement = "")  
final_2014$year <- "2014"
names(final_2018) <- gsub(x = names(final_2018), pattern = "\\_18", replacement = "")  
final_2018$year <- "2018"
names(final_2022) <- gsub(x = names(final_2022), pattern = "\\_22", replacement = "")  
final_2022$year <- "2022"
geoid_counts <- rbind(final_2010, final_2014)
geoid_counts <- rbind(geoid_counts, final_2018)
geoid_counts <- rbind(geoid_counts, final_2022)
geoid_counts <- geoid_counts %>% group_by(geoid) %>% summarize(num_yrs = n())

# merge with number of years
final <- final %>% left_join(geoid_counts, by = "geoid")

## calculate averages -----------------------------------------------
##calculate averages using # of data yrs available per geo as denominator (total voting age and total voted) and total counts

final_df <-  final %>% mutate(
  num_total_va_pop = rowSums(select(.,num_total_va_pop_10, num_total_va_pop_14, num_total_va_pop_18, num_total_va_pop_22), na.rm = TRUE)/num_yrs,
  num_latino_va_pop = rowSums(select(.,num_latino_va_pop_10, num_latino_va_pop_14, num_latino_va_pop_18, num_latino_va_pop_22), na.rm = TRUE)/num_yrs,
  num_nh_white_va_pop = rowSums(select(.,num_nh_white_va_pop_10, num_nh_white_va_pop_14, num_nh_white_va_pop_18, num_nh_white_va_pop_22), na.rm = TRUE)/num_yrs,
  num_nh_black_va_pop = rowSums(select(.,num_nh_black_va_pop_10, num_nh_black_va_pop_14, num_nh_black_va_pop_18, num_nh_black_va_pop_22), na.rm = TRUE)/num_yrs,
  num_aian_va_pop =  rowSums(select(.,num_aian_va_pop_10, num_aian_va_pop_14, num_aian_va_pop_18, num_aian_va_pop_22), na.rm = TRUE)/num_yrs,
  num_nh_asian_va_pop = rowSums(select(.,num_nh_asian_va_pop_10, num_nh_asian_va_pop_14, num_nh_asian_va_pop_18, num_nh_asian_va_pop_22), na.rm = TRUE)/num_yrs,
  num_pacisl_va_pop = rowSums(select(.,num_pacisl_va_pop_10, num_pacisl_va_pop_14, num_pacisl_va_pop_18, num_pacisl_va_pop_22), na.rm = TRUE)/num_yrs,
  num_nh_twoormor_va_pop = rowSums(select(.,num_nh_twoormor_va_pop_10, num_nh_twoormor_va_pop_14, num_nh_twoormor_va_pop_18, num_nh_twoormor_va_pop_22), na.rm = TRUE)/num_yrs,
  
  count_total_voted = rowSums(select(.,count_total_voted_10, count_total_voted_14, count_total_voted_18, count_total_voted_22), na.rm = TRUE),
  count_latino_voted = rowSums(select(.,count_latino_voted_10, count_latino_voted_14, count_latino_voted_18, count_latino_voted_22), na.rm = TRUE),
  count_nh_white_voted = rowSums(select(.,count_nh_white_voted_10, count_nh_white_voted_14, count_nh_white_voted_18, count_nh_white_voted_22), na.rm = TRUE),
  count_nh_black_voted = rowSums(select(.,count_nh_black_voted_10, count_nh_black_voted_14, count_nh_black_voted_18, count_nh_black_voted_22), na.rm = TRUE), 
  count_aian_voted = rowSums(select(.,count_aian_voted_10, count_aian_voted_14, count_aian_voted_18, count_aian_voted_22), na.rm = TRUE), 
  count_nh_asian_voted = rowSums(select(.,count_nh_asian_voted_10, count_nh_asian_voted_14, count_nh_asian_voted_18, count_nh_asian_voted_22), na.rm = TRUE), 
  count_pacisl_voted = rowSums(select(.,count_pacisl_voted_10, count_pacisl_voted_14, count_pacisl_voted_18, count_pacisl_voted_22), na.rm = TRUE), 
  count_nh_twoormor_voted = rowSums(select(.,count_nh_twoormor_voted_10, count_nh_twoormor_voted_14, count_nh_twoormor_voted_18, count_nh_twoormor_voted_22), na.rm = TRUE), 
  
  num_total_voted = rowSums(select(.,num_total_voted_10, num_total_voted_14, num_total_voted_18, num_total_voted_22), na.rm = TRUE)/num_yrs,
  num_latino_voted = rowSums(select(.,num_latino_voted_10, num_latino_voted_14, num_latino_voted_18, num_latino_voted_22), na.rm = TRUE)/num_yrs,
  num_nh_white_voted = rowSums(select(.,num_nh_white_voted_10, num_nh_white_voted_14, num_nh_white_voted_18, num_nh_white_voted_22), na.rm = TRUE)/num_yrs,
  num_nh_black_voted = rowSums(select(.,num_nh_black_voted_10, num_nh_black_voted_14, num_nh_black_voted_18, num_nh_black_voted_22), na.rm = TRUE)/num_yrs,
  num_aian_voted =  rowSums(select(.,num_aian_voted_10, num_aian_voted_14, num_aian_voted_18, num_aian_voted_22), na.rm = TRUE)/num_yrs,
  num_nh_asian_voted = rowSums(select(.,num_nh_asian_voted_10, num_nh_asian_voted_14, num_nh_asian_voted_18, num_nh_asian_voted_22), na.rm = TRUE)/num_yrs,
  num_pacisl_voted = rowSums(select(.,num_pacisl_voted_10, num_pacisl_voted_14, num_pacisl_voted_18, num_pacisl_voted_22), na.rm = TRUE)/num_yrs,
  num_nh_twoormor_voted = rowSums(select(.,num_nh_twoormor_voted_10, num_nh_twoormor_voted_14, num_nh_twoormor_voted_18, num_nh_twoormor_voted_22), na.rm = TRUE)/num_yrs
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

# v3 filters out 4 counties with only 1 yr of data: Imperial, Madera, Merced, and Napa
# final_df_screened <- final_df_screened %>% filter(num_yrs >1)





## get census geoids ------------------------------------------------------
census_api_key("25fb5e48345b42318ae435e4dcd28ad3f196f2c4", overwrite = TRUE)

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
#View(state_table)

#remove state from county table
county_table <- d[d$geoname != 'California', ]

#calculate COUNTY z-scores
county_table <- calc_z(county_table)
county_table <- calc_ranks(county_table)

county_table <- rename(county_table, county_id = geoid, county_name = geoname)
#View(county_table)

###update info for postgres tables###
county_table_name <- "arei_demo_voting_midterm_county_2024"
state_table_name <- "arei_demo_voting_midterm_state_2024"

indicator <- "Annual average percent of voters voting in midterm elections among eligible voting age population. This data is"

source <- "CPS 2010, 2014, 2018, and 2022 average https://www.census.gov/topics/public-sector/voting/data.html"

rc_schema <- "v6"

#to_postgres(county_table, state_table)



# Compare with V3
# v3_county <- st_read(con, query = "SELECT * FROM v3.arei_demo_voting_midterm_county_2021") %>% arrange(county_name)
# v3_state <- st_read(con, query = "SELECT * FROM v3.arei_demo_voting_midterm_state_2021") 



### difference in num_pacisl_voted for Los Angeles due to 1 extra count. But quadrants, performance_z, and disparity z scores are the same. Calculated wrong in SQL calculations.  With state table, anything with pacisl. Because of this, avg, variance, and index of disparity are slightly different as well as the race disparity z-scores for state. 


#county_table %>% select(county_name, pacisl_raw, num_pacisl_voted, count_pacisl_voted, performance_z, disparity_z)
#v3_county %>% select(county_name, pacisl_raw, num_pacisl_voted, count_pacisl_voted, performance_z, disparity_z)


dbDisconnect(con)

