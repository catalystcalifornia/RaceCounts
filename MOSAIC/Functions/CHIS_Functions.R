# Fx to prep CHIS data for RC functions #

source("W:\\RDA Team\\R\\credentials_source.R")


# Update columns with names starting with "X", to blank.

fix_colnames <- function(x) {

names(x) <- sub("^X", "", names(x)) # Remove X from column names
names(x) <- sub("[0-9]+", "", names(x)) # Remove numbers from column names

return(x)
}


# Finish cleaning, formatting CHIS data
prep_chis <- function(x, yesno) { 
#x is df / yesno is either yes or no depending on which CHIS answer you want
  df1 <- x
  #format column headers
  #create new object colnames
  colnames <- names(df1)
  #remove blank column names from colnames
  colnames <- colnames[colnames != ""]
  #temporarily put indicator name into a new object called variable
  variable <- colnames[1]
  #temporarily remove indicator name from colnames
  colnames <- colnames[-1]
  #make all column names in triplicate (to account for the 2 blank column names per geography)
  colnames <- rep(colnames,each=3)
  #re-add indicator name to colnames
  colnames <- c(variable, colnames)
  #sub space for "." in colnames
  colnames <- gsub("\\.", " ", colnames)
  #rename 'All' columns to 'California'
  colnames <- gsub("All", "California", colnames)
  #rename column 1 name to 'variable' from specific indicator name
  colnames[1] <- "variable"
  #rename columns in df to those in colnames
  names(df1) <- colnames


  #format values
  df1[df1=="-"]<-NA

  
  #format PERCENTS
  df_pct <- df1[, which(df1[1,]=='%' | df1[1,]=='measure')]
  df_pct <- df_pct[-1,]
  
  #filter for yes or no values
  df_pct <- df_pct %>%
    filter(grepl(paste0("measure|_",yesno), variable))
  
  #pivot
  df_pct <- pivot_longer(df_pct, 
                         cols = Butte:California, # columns that should pivot from wide to long (unquoted)
                         names_to = "geoname", # name of the new category column as a quoted string
                         values_to = yesno # name of the new value column as a quoted string
  )
  
  df_pct <- pivot_wider(df_pct, 
                        id_cols = geoname, # optional vector of columns you do not want affected
                        names_from = variable, # category column(s) to pivot from long to wide
                        values_from = yesno # value columns(s) that hold data for each category column
                        #names_sep # optional string separator for category-value columns
  )

  #rename
  names(df_pct) <- gsub(paste0("_",yesno), "_rate", names(df_pct))

  #format CIs
  df_ci <- df1[, which(df1[1,]=='95% CI' | df1[1,]=='measure')]
  df_ci <- df_ci[-1,]
  
  #filter for yes or no values
  df_ci <- df_ci %>%
    filter(grepl(paste0("_", yesno), variable))
  
  
  #pivot
  df_ci <- pivot_longer(df_ci,
                        cols = Butte:California, # columns that should pivot from wide to long (unquoted)
                        names_to = "geoname", # name of the new category column as a quoted string
                        values_to = yesno # name of the new value column as a quoted string
  )
  
  df_ci <- pivot_wider(df_ci,
                       id_cols = geoname, # optional vector of columns you do not want affected
                       names_from = variable, # category column(s) to pivot from long to wide
                       values_from = yesno # value columns(s) that hold data for each category column
                       #names_sep # optional string separator for category-value columns
  )
  
  #rename
  names(df_ci) <- gsub(paste0("_",yesno), "_ci", names(df_ci))

  
  #format Populations
  df_pop <- df1[, which(df1[1,]=='Population' | df1[1,]=='measure')]
  df_pop <- df_pop[-1,]
  
  #filter for yes or no values
  df_pop <- df_pop %>%
    filter(grepl(paste0("_", yesno), variable))
  
  #pivot
  df_pop <- pivot_longer(df_pop,
                         cols = Butte:California, # columns that should pivot from wide to long (unquoted)
                         names_to = "geoname", # name of the new category column as a quoted string
                         values_to = yesno # name of the new value column as a quoted string
  )
  
  df_pop <- pivot_wider(df_pop,
                        id_cols = geoname, # optional vector of columns you do not want affected
                        names_from = variable, # category column(s) to pivot from long to wide
                        values_from = yesno # value columns(s) that hold data for each category column
                        #names_sep # optional string separator for category-value columns
  )
  
  #rename
  names(df_pop) <- gsub(paste0("_",yesno), "_raw", names(df_pop))

  
  #bring back together
  df_wide <- merge(x=df_pct, y=df_pop, by="geoname")
  df_wide <- merge(x=df_wide, y=df_ci, by="geoname")
  
  #add flag fields
  df_wide <- mutate(df_wide,
                    total_rate_flag = ifelse(grepl("\\*", total_rate), 1,0),
                    chinese_rate_flag = ifelse(grepl("\\*", chinese_rate), 1,0),
                    japanese_rate_flag = ifelse(grepl("\\*", japanese_rate), 1,0),
                    korean_rate_flag = ifelse(grepl("\\*", korean_rate), 1,0),
                    filipino_rate_flag = ifelse(grepl("\\*", filipino_rate), 1,0),
                    south_asian_rate_flag = ifelse(grepl("\\*", south_asian_rate), 1,0),
                    vietnamese_rate_flag = ifelse(grepl("\\*", vietnamese_rate), 1,0),
                    other_asian_rate_flag = ifelse(grepl("\\*", other_asian_rate), 1,0)
                    
  )
  
  #screen raw and rates by BOTH flag fields
  df_wide <- mutate(df_wide,
                    total_rate = ifelse (total_rate_flag == 1, "NA", total_rate),
                    chinese_rate = ifelse (chinese_rate_flag == 1, "NA", chinese_rate),
                    japanese_rate = ifelse (japanese_rate_flag == 1, "NA", japanese_rate),
                    korean_rate = ifelse (korean_rate_flag == 1, "NA", korean_rate),
                    filipino_rate = ifelse (filipino_rate_flag == 1, "NA", filipino_rate),
                    south_asian_rate = ifelse (south_asian_rate_flag == 1, "NA", south_asian_rate),
                    vietnamese_rate = ifelse (vietnamese_rate_flag == 1, "NA", vietnamese_rate),
                    other_asian_rate = ifelse (other_asian_rate_flag == 1, "NA", other_asian_rate)

 )
  
  #remove asterisks
  df_wide <- as.data.frame(sapply(df_wide,sub,pattern='\\*',replacement=NA))
  
  #format numeric
  cols.num <- c("total_rate",
                "chinese_rate", "japanese_rate", "korean_rate", "filipino_rate", "south_asian_rate", "vietnamese_rate", "other_asian_rate",
                "total_raw",
                "chinese_raw", "japanese_raw", "korean_raw", "filipino_raw", "south_asian_raw", "vietnamese_raw", "other_asian_raw")
  df_wide[cols.num] <- sapply(df_wide[cols.num],as.numeric)
  
  
  #get census geoids
  library(tidycensus)
  
  census_api_key(census_key1, overwrite = TRUE)
  
  ca <- get_acs(geography = "county",
                variables = c("B01001_001"),
                state = "CA",
                year = 2020)
  
  ca <- ca[,1:2]
  ca$NAME <- gsub(" County, California", "", ca$NAME)
  names(ca) <- c("geoid", "geoname")
  
  #add county geoids
  df_wide <- merge(x=ca,y=df_wide,by="geoname", all=T)
  #add state geoid
  df_wide <- within(df_wide, geoid[geoname == 'California'] <- '06')
  #remove CHIS region rows
  df_subset <- df_wide %>% drop_na(geoid)


return(df_subset)
}
