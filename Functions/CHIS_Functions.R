# Fx to prep CHIS data for RC functions #

source("W:\\RDA Team\\R\\credentials_source.R")


# Update columns with names starting with "X", to blank.

fix_colnames <- function(x) {
  
  names(x) <- sub("^X", "", names(x)) # Remove X from column names
  names(x) <- sub("[0-9]+", "", names(x)) # Remove numbers from column names
  
  return(x)
}


# Finish cleaning, formatting CHIS data
prep_chis <- function(x) { 
  
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
  
  #pivot
  df_pct <- pivot_longer(df_pct, 
                         cols = Butte:California, # columns that should pivot from wide to long (unquoted)
                         names_to = "geoname", # name of the new category column as a quoted string
                         values_to = "yes" # name of the new value column as a quoted string
  )
  
  df_pct <- pivot_wider(df_pct, 
                        id_cols = geoname, # optional vector of columns you do not want affected
                        names_from = variable, # category column(s) to pivot from long to wide
                        values_from = yes # value columns(s) that hold data for each category column
                        #names_sep # optional string separator for category-value columns
  )
  
  #rename
  names(df_pct) <- gsub("yes", "rate", names(df_pct))
  names(df_pct) <- gsub("_no", "_no_rate", names(df_pct))
  
  #format CIs
  df_ci <- df1[, which(df1[1,]=='95% CI' | df1[1,]=='measure')]
  df_ci <- df_ci[-1,]
  
  #pivot
  df_ci <- pivot_longer(df_ci,
                        cols = Butte:California, # columns that should pivot from wide to long (unquoted)
                        names_to = "geoname", # name of the new category column as a quoted string
                        values_to = "yes" # name of the new value column as a quoted string
  )
  
  df_ci <- pivot_wider(df_ci,
                       id_cols = geoname, # optional vector of columns you do not want affected
                       names_from = variable, # category column(s) to pivot from long to wide
                       values_from = yes # value columns(s) that hold data for each category column
                       #names_sep # optional string separator for category-value columns
  )
  
  #rename
  names(df_ci) <- gsub("yes", "ci", names(df_ci))
  names(df_ci) <- gsub("_no", "_no_ci", names(df_ci))
  
  
  #format Populations
  df_pop <- df1[, which(df1[1,]=='Population' | df1[1,]=='measure')]
  df_pop <- df_pop[-1,]
  
  #pivot
  df_pop <- pivot_longer(df_pop,
                         cols = Butte:California, # columns that should pivot from wide to long (unquoted)
                         names_to = "geoname", # name of the new category column as a quoted string
                         values_to = "yes" # name of the new value column as a quoted string
  )
  
  df_pop <- pivot_wider(df_pop,
                        id_cols = geoname, # optional vector of columns you do not want affected
                        names_from = variable, # category column(s) to pivot from long to wide
                        values_from = yes # value columns(s) that hold data for each category column
                        #names_sep # optional string separator for category-value columns
  )
  
  #rename
  names(df_pop) <- gsub("yes", "raw", names(df_pop))
  names(df_pop) <- gsub("_no", "_no_raw", names(df_pop))
  
  
  #bring back together
  df_wide <- merge(x=df_pct, y=df_pop, by="geoname")
  df_wide <- merge(x=df_wide, y=df_ci, by="geoname")
  
  #add flag fields
  df_wide <- mutate(df_wide,
                    total_rate_flag = ifelse(grepl("\\*", total_rate), 1,0),
                    latino_rate_flag = ifelse(grepl("\\*", latino_rate), 1,0),
                    nh_white_rate_flag = ifelse(grepl("\\*", nh_white_rate), 1,0),
                    nh_black_rate_flag = ifelse(grepl("\\*", nh_black_rate), 1,0),
                    nh_asian_rate_flag = ifelse(grepl("\\*", nh_asian_rate), 1,0),
                    nh_twoormor_rate_flag = ifelse(grepl("\\*", nh_twoormor_rate), 1,0),
                    aian_rate_flag = ifelse(grepl("\\*", aian_rate), 1,0),
                    pacisl_rate_flag = ifelse(grepl("\\*", pacisl_rate), 1,0),
                    swana_rate_flag = ifelse(grepl("\\*", swana_rate), 1,0),
                    
                    total_no_rate_flag = ifelse(grepl("\\*", total_no_rate), 1,0),
                    latino_no_rate_flag = ifelse(grepl("\\*", latino_no_rate), 1,0),
                    nh_white_no_rate_flag = ifelse(grepl("\\*", nh_white_no_rate), 1,0),
                    nh_black_no_rate_flag = ifelse(grepl("\\*", nh_black_no_rate), 1,0),
                    nh_asian_no_rate_flag = ifelse(grepl("\\*", nh_asian_no_rate), 1,0),
                    nh_twoormor_no_rate_flag = ifelse(grepl("\\*", nh_twoormor_no_rate), 1,0),
                    aian_no_rate_flag = ifelse(grepl("\\*", aian_no_rate), 1,0),
                    pacisl_no_rate_flag = ifelse(grepl("\\*", pacisl_no_rate), 1,0),
                    swana_no_rate_flag = ifelse(grepl("\\*", swana_no_rate), 1,0)
                    
  )
  
  #screen raw and rates by BOTH flag fields
  df_wide <- mutate(df_wide,
                    total_rate = ifelse (total_rate_flag == 1 | total_no_rate_flag == 1, "NA", total_rate),
                    total_no_rate = ifelse (total_rate_flag == 1 | total_no_rate_flag == 1, "NA", total_no_rate),
                    latino_rate = ifelse (latino_rate_flag == 1 | latino_no_rate_flag == 1, "NA", latino_rate),
                    latino_no_rate = ifelse (latino_rate_flag == 1 | latino_no_rate_flag == 1, "NA", latino_no_rate),
                    nh_white_rate = ifelse (nh_white_rate_flag == 1 | nh_white_no_rate_flag == 1, "NA", nh_white_rate),
                    nh_white_no_rate = ifelse (nh_white_rate_flag == 1 | nh_white_no_rate_flag == 1, "NA", nh_white_no_rate),
                    nh_black_rate = ifelse (nh_black_rate_flag == 1 | nh_black_no_rate_flag == 1, "NA", nh_black_rate),
                    nh_black_no_rate = ifelse (nh_black_rate_flag == 1 | nh_black_no_rate_flag == 1, "NA", nh_black_no_rate),
                    nh_asian_rate = ifelse (nh_asian_rate_flag == 1 | nh_asian_no_rate_flag == 1, "NA", nh_asian_rate),
                    nh_asian_no_rate = ifelse (nh_asian_rate_flag == 1 | nh_asian_no_rate_flag == 1, "NA", nh_asian_no_rate),
                    nh_twoormor_rate = ifelse (nh_twoormor_rate_flag == 1 | nh_twoormor_no_rate_flag == 1, "NA", nh_twoormor_rate),
                    nh_twoormor_no_rate = ifelse (nh_twoormor_rate_flag == 1 | nh_twoormor_no_rate_flag == 1, "NA", nh_twoormor_no_rate),
                    aian_rate = ifelse (aian_rate_flag == 1 | aian_no_rate_flag == 1, "NA", aian_rate),
                    aian_no_rate = ifelse (aian_rate_flag == 1 | aian_no_rate_flag == 1, "NA", aian_no_rate),
                    pacisl_rate = ifelse (pacisl_rate_flag == 1 | pacisl_no_rate_flag == 1, "NA", pacisl_rate),
                    pacisl_no_rate = ifelse (pacisl_rate_flag == 1 | pacisl_no_rate_flag == 1, "NA", pacisl_no_rate),
                    swana_rate = ifelse (swana_rate_flag == 1 | swana_no_rate_flag == 1, "NA", swana_rate),
                    swana_no_rate = ifelse (swana_rate_flag == 1 | swana_no_rate_flag == 1, "NA", swana_no_rate),
                    
                    total_raw = ifelse (total_rate_flag == 1 | total_no_rate_flag == 1, "NA", total_raw),
                    total_no_raw = ifelse (total_rate_flag == 1 | total_no_rate_flag == 1, "NA", total_no_raw),
                    latino_raw = ifelse (latino_rate_flag == 1 | latino_no_rate_flag == 1, "NA", latino_raw),
                    latino_no_raw = ifelse (latino_rate_flag == 1 | latino_no_rate_flag == 1, "NA", latino_no_raw),
                    nh_white_raw = ifelse (nh_white_rate_flag == 1 | nh_white_no_rate_flag == 1, "NA", nh_white_raw),
                    nh_white_no_raw = ifelse (nh_white_rate_flag == 1 | nh_white_no_rate_flag == 1, "NA", nh_white_no_raw),
                    nh_black_raw = ifelse (nh_black_rate_flag == 1 | nh_black_no_rate_flag == 1, "NA", nh_black_raw),
                    nh_black_no_raw = ifelse (nh_black_rate_flag == 1 | nh_black_no_rate_flag == 1, "NA", nh_black_no_raw),
                    nh_asian_raw = ifelse (nh_asian_rate_flag == 1 | nh_asian_no_rate_flag == 1, "NA", nh_asian_raw),
                    nh_asian_no_raw = ifelse (nh_asian_rate_flag == 1 | nh_asian_no_rate_flag == 1, "NA", nh_asian_no_raw),
                    nh_twoormor_raw = ifelse (nh_twoormor_rate_flag == 1 | nh_twoormor_no_rate_flag == 1, "NA", nh_twoormor_raw),
                    nh_twoormor_no_raw = ifelse (nh_twoormor_rate_flag == 1 | nh_twoormor_no_rate_flag == 1, "NA", nh_twoormor_no_raw),
                    aian_raw = ifelse (aian_rate_flag == 1 | aian_no_rate_flag == 1, "NA", aian_raw),
                    aian_no_raw = ifelse (aian_rate_flag == 1 | aian_no_rate_flag == 1, "NA", aian_no_raw),
                    pacisl_raw = ifelse (pacisl_rate_flag == 1 | pacisl_no_rate_flag == 1, "NA", pacisl_raw),
                    pacisl_no_raw = ifelse (pacisl_rate_flag == 1 | pacisl_no_rate_flag == 1, "NA", pacisl_no_raw),
                    swana_raw = ifelse (swana_rate_flag == 1 | swana_no_rate_flag == 1, "NA", swana_raw),
                    swana_no_raw = ifelse (swana_rate_flag == 1 | swana_no_rate_flag == 1, "NA", swana_no_raw)
                    
  )
  
  #remove asterisks
  df_wide <- as.data.frame(sapply(df_wide,sub,pattern='\\*',replacement=NA))
  
  #format numeric
  cols.num <- c("total_no_rate","latino_no_rate","nh_white_no_rate","nh_black_no_rate","nh_asian_no_rate","nh_twoormor_no_rate","aian_no_rate","pacisl_no_rate","swana_no_rate",
                "total_rate","latino_rate","nh_white_rate","nh_black_rate","nh_asian_rate","nh_twoormor_rate","aian_rate","pacisl_rate","swana_rate",
                "total_no_raw","latino_no_raw","nh_white_no_raw","nh_black_no_raw","nh_asian_no_raw","nh_twoormor_no_raw","aian_no_raw","pacisl_no_raw","swana_no_raw",
                "total_raw","latino_raw","nh_white_raw","nh_black_raw","nh_asian_raw","nh_twoormor_raw","aian_raw","pacisl_raw","swana_raw")
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
  df_subset <- df_subset %>% select(-c(ends_with("_no_rate"), ends_with("_no_raw"), ends_with("_no_ci")))
  
  return(df_subset)
}