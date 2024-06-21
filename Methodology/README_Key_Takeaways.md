# RACE COUNTS
### January 2024

<base target="_blank">


<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/rc_homepage.PNG" alt="RACE COUNTS homepage">




<details>
  <summary>Table of Contents</summary>
  <ol>
       <li>
      <a href="#intro">Introduction</a></li>
    <li><a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#generate-key-findings">Generate Key Findings</a></li>
    <li><a href="#contact-us">Contact Us</a></li>
    <li><a href="#github-link">Github Link</a></li>
    <li><a href="#citation">Citation</a></li>
    <li><a href="#license">License</a></li>
  </ol>
</details>

## Introduction

This is an explanation of how we create the Key Takeaways found on RACECOUNTS.org. This document is a work in progress and will continue to be updated and expanded. 

Note: The code below does not include lines relating to importing of the data. We pull tables from our private PostgreSQL database using credentials accessed through a separate script before running any of the code below. The database is accessible only by our Research & Data Analysis team. However, we do plan to share a public file with the complete data for each RACE COUNTS indicator, where possible, on this repo soon.

## Getting Started

To get a local copy up and running follow these simple steps. 

### Prerequisites

We completed the data cleaning, analysis, and visualization using the following software. 
* [R](https://cran.rstudio.com/)
* [RStudio](https://posit.co/download/rstudio-desktop)

We used several R packages to analyze data and perform different functions, including the following.
* data.table 
* dplyr
* sf
* tidyr
* tidyverse 
* RPostgreSQL
* usethis

```
# Install packages if not already installed
list.of.packages <- c("data.table", "dplyr", "sf", "tidyr", "tidyverse", "RPostgreSQL", "usethis")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

#Load libraries
library(data.table) 
library(dplyr)
library(sf)
library(tidyr)
library(tidyverse) 
library(RPostgreSQL) 
library(usethis)
options(scipen=999)
```


### Installation

Clone the repo
   ```sh
   git clone https://github.com/github_username/repo_name.git
   ```

<p align="right">(<a href="#top">back to top</a>)</p>



## Generate Key Findings

<!-- Will need to update to say key_findings_2023.R-->
**This section is an explanation of the key_findings_2023.R script.**

### Data Loading and Set Up
Import the cleaned and standardized data for each indicator separately for each geography level (city, county, state) from our private database into RStudio and format it. In the script, you will see that city-level Education data requires additional processing. This is because Education data is collected at school district, not city level. 

The data import process is time-consuming because of the amount of data pulled in from the database and the amount of reformatting done. Then combine into one massive dataframe (df) and clean the data. Once you have this large dataframe, it is a good idea to work with a copy of it. That way, you will not have to re-import and clean the data again if you need to change the code that comes after these steps. As mentioned above, we plan to make a file with complete clean indicator data, where possible, available here soon. 


<details>
<summary>Code Explanation</summary>

```
# Pull list of indicator tables, then import data. County example: ----------------------------------
rc_list_ = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = curr_schema))$table, function(x) slot(x, 'name'))))

# filter for only county level indicator tables, drop all others including api_*_county_ curren year tables
county_list <- filter(rc_list_, grepl(paste0("^arei_.*\\county_", curr_yr, "$"), table))
county_list <- county_list[order(county_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on county_list
county_tables <- lapply(setNames(paste0("select * from ", curr_schema, ".", county_list), county_list), DBI::dbGetQuery, conn = con)

# create column with indicator name
county_tables <- map2(county_tables, names(county_tables), ~ mutate(.x, indicator = .y)) # create column with indicator name

# call columns we want and pivot wider
county_tables_disparity <- lapply(county_tables, function(x) x %>% select(county_id, asbest, ends_with("disparity_z"), indicator, values_count))

county_disparity <- imap_dfr(county_tables_disparity, ~
                               .x %>% 
                               pivot_longer(cols = ends_with("disparity_z"),
                                            names_to = "race",
                                            values_to = "disparity_z_score")) %>% mutate(
                                              race = (ifelse(race == 'disparity_z', 'total', race)),
                                              race = gsub('_disparity_z', '', race))

county_tables_performance <- lapply(county_tables, function(x) x %>% select(county_id, asbest, ends_with("performance_z"), indicator, values_count))

county_performance <- imap_dfr(county_tables_performance, ~
                                 .x %>% 
                                 pivot_longer(cols = ends_with("performance_z"),
                                              names_to = "race",
                                              values_to = "performance_z_score")) %>% mutate(
                                                race = (ifelse(race == 'performance_z', 'total', race)),
                                                race = gsub('_performance_z', '', race))


county_tables_rate <- lapply(county_tables, function(x) x %>% select(county_id, asbest, ends_with("_rate"), indicator, values_count))

county_rate <- imap_dfr(county_tables_rate , ~
                          .x %>% 
                          pivot_longer(cols = ends_with("_rate"),
                                       names_to = "race",
                                       values_to = "rate")) %>% mutate(
                                         race = (ifelse(race == 'rate', 'total', race)),
                                         race = gsub('_rate', '', race))

# merge all 3 
df_merged_county <- county_disparity %>% full_join(county_performance) %>% full_join(county_rate)

# create issue, indicator, geo_level, race generic columns for issue tables except for education
df_county <- df_merged_county %>% mutate(
        issue = substring(indicator, 6,9),  
        indicator = substring(indicator, 11),
        indicator = gsub(paste0('_county_', curr_yr), '', indicator),
        geo_level = "county",
        race_generic = gsub('nh_', '', race)) %>% # create 'generic' race name column, drop nh_ prefixes to help generate counts by race later
            left_join(arei_race_multigeo_county) %>% 
                rename(geoid = county_id, geo_name = county_name)  %>%  mutate(geo_name = paste0(geo_name, " County"))

# merge city, county, state data
df <- bind_rows(df_city, df_county, df_state) %>% select(
                    geoid, geo_name, issue, indicator, race, asbest, rate, disparity_z_score, performance_z_score, values_count, geo_level, race_generic)


# remove records where city name is actually a university: there are 6 'cities' like this making up 898 rows
final_df <- df %>% filter(!grepl('University', geo_name))

```

Create short form to long form race and indicator label crosswalks that will be used to generate key findings text.

```
# Create race name crosswalk ------------------------------------------------
race_generic <- unique(df$race_generic)
long_name <- c("Total", "API", "Black", "Latinx", "American Indian / Alaska Native", "White", "Asian", "Two or More Races", "Native Hawaiian / Pacific Islander", "Other Race", "Filipinx")
race_names <- data.frame(race_generic, long_name)

# Create indicator long name df -------------------------------------------
### NOTE: This list may need to be updated or re-ordered. ###
indicator <- st_read(con, query = "SELECT arei_indicator AS indicator, api_name AS indicator_short, arei_issue_area FROM v5.arei_indicator_list_cntyst")

# unique education indicators at school district level, for city key takeaways analysis later
educ_indicators <- filter(indicator, arei_issue_area == 'Education')

```
</details>

### Count of Best and Worst Rates by Race Findings

Create findings identifying the number of times each race has the worst and best rates in each geography. These findings are found on Race pages such as the [Alameda County: Asian Race Page](https://www.racecounts.org/county_races/alameda/?race=asian).

<details>
<summary>Code Explanation</summary>

* Step 1: Identify the most disparate district for each Education indicator by city, then join only that district's data to the rest of city-level data.

```
# rank overall district disparity z-scores for each city+indicator combo
df_education_district_disparate <- df_education_district %>% filter(!is.na(geoid)) %>% group_by(geoid, indicator, race) %>% 
                                                                          mutate(rk = ifelse(race == 'total', dense_rank(-disparity_z_score), NA))

# checked for districts tied for rk 1, but there are none. if there are ties, will need to add tiebreaker code similar to what's in the best outcomes code
#temp <- filter(df_education_district_disparate, rk=='1')
#temp <- temp %>% select(geoid, geo_name, indicator, rate) %>% group_by(geoid, indicator) %>% count(rate)

# keep indicator data for the most disparate district for each city+indicator combo only
df_education_district_disparate <- df_education_district_disparate %>% group_by(geoid, dist_id, indicator) %>% fill(rk, .direction = "downup") %>%
                                                                          filter(rk == 1) %>% select (-c(rk))

# bind most disparate district with main df
df_lf <- bind_rows(final_df, df_education_district_disparate) 
```

* Step 2: Remove the total or overall rates, keeping only rates by race. Not all data sources report Asian and Native Hawaiian / Pacific Islander data separately. Duplicate and reclassify combined Asian-Pacific Islander (API) data into the separate Asian and Native Hawaiian / Pacific Islander groups.

```
df_lf <- filter(df, race != 'total')

# duplicate API rows, assigning one set race_generic Asian and the other set PacIsl
api_split <- function(x) {
  
    api_asian <- filter(x, race_generic == 'api') %>% mutate(race_generic = 'asian')
    api_pacisl <- filter(x, race_generic == 'api') %>% mutate(race_generic = 'pacisl')
    temp <- filter(x, race_generic != 'api')       # remove api rows
    x <- bind_rows(temp, api_asian, api_pacisl)    # add back api rows as asian AND pacisl rows

  return(x)
}

df_lf <- api_split(df_lf) # duplicate/split api rates as asian and pacisl
```

* Step 3: Count the number of non-null rates for each race and geography combo to screen key findings later. 

```
bestworst_screen <- df_lf %>% group_by(geoid, race_generic) %>% summarise(rate_count = sum(!is.na(rate)))  # count number of non-null rates for each race+geo combo
```

* Step 4: Count number of times each race has the worst rate (highest disparity z-score) for each geography. Screen out observations where fewer than 2 races in a geography have non-null values for an indicator. Finally, clean city names.

```
worst_table <- df_lf %>% 
  group_by(geoid, geo_level, indicator) %>% top_n(1, disparity_z_score) %>% # get worst raced disparity z-score by geo+indicator combo
  rename(worst_rate = race_generic) %>% filter(values_count > 1) # filter out geo+indicator combos with only 1 raced rate


worst_table2 <- df_lf %>% 
  left_join(select(worst_table, geoid, indicator, worst_rate, geo_level), by = c("geoid", "indicator", "geo_level")) %>%
  mutate(worst = ifelse((race_generic == worst_rate), 1, 0)) %>% # worst = binary indicating whether the race+geo combo is the worst rate             
  group_by(geoid, geo_name, geo_level, race_generic) %>% summarise(count = sum(worst, na.rm = TRUE)) %>% # count = num of worst rates for race+geo combo
  left_join(race_names, by = "race_generic") %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic")) 
  
  # Clean geo_names where 'City' isn't part of city's name and fix geo_names that include "City City"
clean_city_names <- function(x) {
    clean_city_names1 <- x %>% filter(!grepl('City City', geo_name) & grepl(' City', geo_name)) %>%
                                mutate(geo_name = gsub(' City', '', geo_name))
    
    clean_city_names2 <- x %>% filter(grepl('City City', geo_name)) %>%
                                mutate(geo_name = gsub('City City', 'City', geo_name))		

    clean_city_names_ <- rbind(clean_city_names1, clean_city_names2) %>% ungroup() %>% select(geoid, geo_name) %>% unique()

    library(easyr)
    x <- jrepl(
      x,
      clean_city_names_,
      by = c('geoid' = 'geoid'),
      replace.cols = c('geo_name' = 'geo_name'),
      na.only = FALSE,
      only.rows = NULL,
      verbose = FALSE)

  return(x)
}

worst_table2 <- clean_city_names(worst_table2)
                
```                

* Step 5: Create the "shells" used to create the best/worst rate count key finding sentences. Use the variables created above to generate key findings.

``` 
# NOTE: This df does include findings for non-RC race pg grps, however they won't appear on the site
wb_rate_threshold <- 5  # suppress findings for race+geo combos with data for fewer than 6 indicators
worst_rate_count <- filter(worst_table2, !is.na(rate_count)) %>% mutate(finding_type = 'worst count', findings_pos = 2) %>% 
  mutate(finding = ifelse(rate_count > wb_rate_threshold, paste0(geo_name, "'s ", long_name, " residents have the worst rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis.")))

```

* Step 6: Apply code similar to Worst Rate findings to generate Best Rate key findings. First, find the school district with the best total_rate (overall outcome) per city-indicator combination and add only that district's data to the rest of city-level data.
  
```
df_education_district_best_outcome <- df_education_district %>% filter(values_count > 1 & !is.na(geoid)) %>% group_by(geoid, indicator, race) %>% 
                                        mutate(rk = ifelse(asbest == 'min' & race == 'total', dense_rank(rate), 
                                                    ifelse(asbest == 'max' & race == 'total', dense_rank(-rate), NA))) # using dense_rank means there can be ties, use enr as tie-breaker

# tie-breaker when 2+ districts tie for best overall outcome for a city+indicator combo
tiebreaker <- df_education_district_best_outcome %>% group_by(geoid, indicator, rk) %>% mutate(ties = ifelse(rk == '1', sum(rk), NA)) # if ties is >1 then there is a tie
tiebreaker <- filter(tiebreaker, ties > 1) %>% group_by(geoid, indicator) %>% mutate(rk2 = ifelse(ties > 2, rank(-total_enroll), rk)) # break tie based on largest total_enrollment                                                                                     
df_education_district_best_outcome <- df_education_district_best_outcome %>% mutate(old_rk = rk) %>% # preserve original ranks with ties
                                 left_join(select(tiebreaker, geoid, indicator, dist_id, race, rk2), by = c("geoid", "indicator", "dist_id", "race"))
df_education_district_best_outcome <- df_education_district_best_outcome %>% mutate(rk = ifelse(!is.na(rk2), rk2, rk)) %>% select(-c(rk2)) # update rk to reflect tiebreaker

# keep indicator data for the best overall outcome district for each city+indicator combo only
df_education_district_best_outcome <- df_education_district_best_outcome %>% group_by(geoid, dist_id, indicator) %>% fill(rk, .direction = "downup") %>%
  filter(rk == 1) %>% select (-c(rk, old_rk))
        
## Now, bind this back with the main df
df_lf2 <- bind_rows(final_df, df_education_district_best_outcome) 
```

Step 7: Remove the total or overall rates, keeping only rates by race. Then, reclassify API rates as Asian and Native Hawaiian / Pacific Islander rates.

```
df_lf2 <- filter(df_lf2, race != 'total')   # remove total rates bc all findings in this section are raced

df_lf2 <- api_split(df_lf2) # duplicate api rates as asian and pacisl

```

Step 8: Count number of times each race has the best rate for each geography. Screen out observations where fewer than 2 races in a geography have non-null values for an indicator. Finally, clean city names.

```
#### Note: Code differs from Worst rates to account for when min is best and there is raced rate = 0, so we cannot use disparity_z for indicators where the minimum rate is best, like suspensions

best_table <- df_lf2 %>%  
  group_by(geoid, geo_level, indicator) %>% 
  mutate(rk = ifelse(asbest == 'min', dense_rank(rate), ifelse(asbest == 'max', dense_rank(-rate), NA))) %>%  # rank based on which race has best outcome
  mutate(best_rate = ifelse(rk == 1, race_generic, ""))   # identify race with best rate using rk (ties are ok)

best_table2 <- subset(df_lf2, values_count > 1) %>%  # filter out indicators with only 1 raced rate 
  left_join(select(best_table, geoid, indicator, best_rate, geo_level), by = c("geoid", "indicator", "geo_level")) %>%
  mutate(best = ifelse((race_generic == best_rate), 1, 0)) %>%             
  group_by(geoid, geo_name, geo_level, race_generic) %>% summarise(count = sum(best, na.rm = TRUE)) %>%
  left_join(race_names, by = c("race_generic")) %>%
  left_join(bestworst_screen, by = c("geoid", "race_generic"))

best_table2 <- clean_city_names(best_table2)
```

Step 9: Create the "shells" used to create the Best Rate Count key findings. Use the variables created above to generate key findings. Then, join the Best and Worst Rate Count key findings.

```
best_rate_count <- filter(best_table2, !is.na(rate_count)) %>% mutate(finding_type = 'best count', findings_pos = 1) %>%
  mutate(finding = ifelse(rate_count > wb_rate_threshold, paste0(geo_name, "'s ", long_name, " residents have the best rate for ", count, " of the ", rate_count, " RACE COUNTS indicators with data for them."), paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis.")))



## Bind worst and best tables - RACE PAGE ## ----------------------------------------------
worst_best_counts <- bind_rows(worst_rate_count, best_rate_count)
worst_best_counts <- rename(worst_best_counts, race = race_generic) %>% select(-long_name, -rate_count, -count)
```

</details>

<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/finding_best_rate.PNG" alt="Counts of Best and Worst Rates Findings">

### Most Impacted Race by Place Finding

Create findings identifying the race most impacted by racial disparity in each geography using counts from the Count of Worst Rate Findings. The most impacted race in a geography is the group that has the highest count of worst rates. These findings are found on Place pages, such as the [Bakersfield Place Page](https://www.racecounts.org/city/bakersfield/).

<details>
<summary>Code Explanation</summary>
This finding is generated with the following steps:

  
* Step 1: Count the number of indicators with Index of Disparity scores per geography, used for screening later. Then use the Count of Worst Rates data produced earlier to identify the Most Impacted group including handling of ties.

```
impact_screen <- df_lf %>% group_by(geoid, geo_name, indicator) %>% summarise(rate_count = sum(!is.na(disparity_z_score))) 
impact_screen <- filter(impact_screen, rate_count > 1) %>% group_by(geoid, geo_name) %>% summarise(id_count = n())Create a table counting number of indicators with overall disparity z-scores (requires two or more groups with non-null rates) per geography.

impact_table <- worst_table2 %>% select(-rate_count) %>% group_by(geoid, geo_name) %>% top_n(1, count) %>% # get race most impacted by racial disparity by geo
                left_join(select(impact_screen, geoid, id_count), by = "geoid")

## the next few lines concatenate the names of the tied groups to prep for findings
impact_table2 <- filter(impact_table, !is.na(id_count)) %>% 
  group_by(geoid, geo_name, count) %>%
  mutate(race_count = n()) # count the number of most impacted groups

impact_table2 <- impact_table2[order(impact_table2$long_name),] # order long race name alphabetically

impact_table2 <- impact_table2 %>% mutate(group_order = paste0("group_", rank(long_name, ties.method = "first"))) # number the most impacted groups grouped by geo

impact_table_wide <- impact_table2 %>% dplyr::select(geoid, geo_name, geo_level, id_count, race_count, group_order, long_name) %>%      #pivot long table back to wide
  pivot_wider(names_from=group_order, values_from=long_name)
```

* Step 2: Suppress findings for places with too many groups tied for Most Impacted which makes findings less useful. Create the "shells" used to create the key finding sentences. Use the variables created above to generate key findings. Finally, clean place names.
  
```
most_impacted <- impact_table_wide %>% mutate(finding_type = 'most impacted', 
                                              finding = ifelse(id_count > 4 & long_name2 != '99999', 
                                                          paste0("Across indicators, ", geo_name, " ", long_name2, " residents are most impacted by racial disparity."), 
                                                            ifelse(id_count > 4 & long_name2 == '99999', 
                                                              paste0('There are more than three groups tied for most impacted in ', geo_name, "."), 
                                                                paste0("Data for residents of ", geo_name, " is too limited for this analysis."))),
                                              findings_pos = 1)  


most_impacted <- most_impacted %>% select(c(geoid, geo_name, geo_level, finding_type, finding, findings_pos))
most_impacted$geo_name <- gsub(" County", "", most_impacted$geo_name) 
most_impacted$geo_name <- gsub(" City", "", most_impacted$geo_name) 
  
```
 
</details>

<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/finding_most_impacted.PNG" alt="Most Impacted Race by Place Finding">


### Most Disparate Indicator by Race & Place

Create findings identifying the indicator with the most racial disparity (highest disparity z-score) for each race in each geography. These findings are found on Race pages, such as the [Tulare: Black Race Page](https://www.racecounts.org/county_races/tulare/?race=black).

<details>
<summary>Code Explanation</summary>
This finding is generated with the following steps:

* Step 1: Create a functions that will generate findings for each race (one at a time), including nested function to identify the largest disparity z-score.
```
# Function to prep raced most_disparate tables
most_disp_by_race <- function(x, y) {
  # Nested function to pull the column with the max disp_z value ----------------------
  find_first_max_index_na <- function(row) {
    
    head(which(row == max(row, na.rm=TRUE)), 1)[1]
  }  
```

* Step 2: First, filter for the specified race, then pivot data to wide format. Count indicators with non-null values for each race-place combination and identify the most disparate indicator (highest disparity_z). Finally, generate a key finding based on the most disparate indicator by race for each geography.  

```
  # filter by race, pivot_wider, select the columns we want, get race long_name
  z <- x %>% filter(race_generic == y) %>% mutate(indicator = paste0(indicator, "_ind")) %>% pivot_wider(names_from = indicator, values_from = disparity_z_score) %>% group_by(geoid, geo_name) %>%  
    fill(ends_with("ind"), dist_id, district_name, total_enroll, .direction = 'updown') %>% 
    filter(!duplicated(geo_name)) %>% select(-race) %>% rename(race = race_generic) %>% select(geoid, geo_name, race, dist_id, district_name, total_enroll, ends_with("ind"), geo_level)
  z <- z %>% inner_join(race_names, by = c('race' = 'race_generic')) %>% select(geoid, geo_name, race,  dist_id, district_name, total_enroll, long_name, everything()) # add race long names
  
  # count non-null indicators by race/place
  indicator_count_ <- z %>% ungroup %>% select(-geo_name:-total_enroll)
  indicator_count_ <- indicator_count_ %>% mutate(indicator_count = rowSums(!is.na(select(., 3:ncol(indicator_count_)))))
  
  z$indicator_count <- indicator_count_$indicator_count # add indicator counts by race/place to original df
  
  # select columns we need
  z <- z %>% select(geoid, geo_name, dist_id, district_name, total_enroll, race, long_name, indicator_count, everything()) 
  
  # unique indicators that apply to race
  indicator_col <- z %>% ungroup %>% select(ends_with("_ind"))
  indicator_col <- names(indicator_col)
  
  # pull the column name with the maximum value
  z$max_col <- colnames(z[indicator_col]) [
    apply(
      z[indicator_col],
      MARGIN = 1,
      find_first_max_index_na )
  ]
 
  z$max_col <- gsub("_ind", "", z$max_col)
  
  ## merge with indicator
  z <- left_join(z, indicator, by = c("max_col"="indicator_short"))
  
return(z)
  
}
```
* Step 3: Prepare data for the function as follows. Add city-level education data to the rest of the indicator data. Then, remove the total_rates, keeping only rates by race. Finally, reclassify API data as both Asian and Native Hawaiian / Pacific Islander data.

```
df_ds <- bind_rows(final_df,df_education_district_disparate)
# df_ds %>% filter(is.na(geo_name)) # why do some geo_names in housing don't have a geo_name? Some of them belong to census designated places with very low pop counts-- we'll filter this out later
df_ds <- filter(df_ds, race != 'total')    # remove total rates bc all findings in this section are raced
df_ds <- api_split(df_ds) # duplicate api rates as asian and pacisl
```

* Step 4: Apply the function to the data by race, then bind results together into one dataframe.

```
aian_ <- most_disp_by_race(df_ds, 'aian')

asian_ <- most_disp_by_race(df_ds, 'asian')

black_ <- most_disp_by_race(df_ds, 'black')

latinx_ <- most_disp_by_race(df_ds, 'latino')

pacisl_ <- most_disp_by_race(df_ds, 'pacisl')

white_ <- most_disp_by_race(df_ds, 'white')

most_disp <- bind_rows(aian_, asian_, black_, latinx_, pacisl_, white_) %>%
                   select(geoid, geo_name, dist_id, district_name, total_enroll, race, long_name, indicator_count, ends_with("_ind"), everything())
``` 

* Step 5: Clean city names. Suppress findings for race-place combinations with too few indicator disparity_z scores. Create the "shells" used to create the key finding sentences. Use the variables created above to generate key findings. Note, we add school district names to education-related findings at city level.

```
n = 5 # indicator_count threshold

most_disp_final <- most_disp %>% mutate(
  finding = ifelse(indicator_count <= n,     ## Suppress finding if race+geo combo has 5 or fewer indicator disparity_z scores
                   paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."), 
                   paste0(long_name, " residents face the most disparity with ", indicator, " in ", geo_name, "."))
  ) %>% mutate(
  
  # add school district name to city education-related findings
  finding = ifelse(
    arei_issue_area == 'Education' & !grepl('too limited', finding) & !is.na(district_name), 
    paste0(long_name, " residents face the most disparity with ", indicator, " (", district_name, ") in ", geo_name, "."),
    finding),
  
  finding_type = 'most disparate', findings_pos = 3) %>%
  select(geoid, geo_name, geo_level, dist_id, district_name, total_enroll, long_name, race, indicator, indicator_count, finding_type, findings_pos, finding) %>% filter(!is.na(geo_name)) # some geoids don't have geo_names. all of them belong in housing-- for example: Camp Pendleton North. They don't pass the indicator count threshold either way to be included in the finding, so we will remove these. 
```

* Step 6: Bind Most Disparate Indicator and Counts of Best and Worst Rates findings together. Export table to postgres database with metadata.

```
rda_race_findings <- bind_rows(most_disp_final, worst_best_counts)
rda_race_findings <- rda_race_findings %>% relocate(geo_level, .after = geo_name) %>% relocate(finding_type, .after = race) %>% mutate(src = 'rda', citations = '') %>%
  mutate(race = ifelse(race == 'pacisl', 'nhpi', race))  # rename pacisl to nhpi to feed API - In all other tables we use 'pacisl'
  
## Export postgres table
dbWriteTable(con, c(curr_schema, "arei_findings_races_multigeo"), rda_race_findings, overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE v5.arei_findings_races_multigeo IS 'findings for Race pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaways\\key_findings_2023.R.';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo.finding_type
                         IS 'Categorizes findings: count of best and worst rates by race/geo combo, most disparate indicator by race/geo combo';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo.src
                         IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo.citations
                         IS 'External v5.citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
dbSendQuery(con, comment)  
```

</details>



<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/finding_most_disparate_by_race.PNG" alt="Most Disparate">


### Most Disparate and Worst Outcome Indicators by Place Findings

Create findings identifying the indicator with most racial disparity (highest disparity z-score) in each geography. These findings are found on Place pages, such as the [Anaheim Place Page](https://www.racecounts.org/city/anaheim/).

<details>
<summary>Code Explanation</summary>
This finding is generated with the following steps:

* Step 1: As Education data is collected at school district level, not city level, additional processing is needed. Merge the most disparate school district per city's data (identified in Count of Worst Rates code) with the rest of data. Then, keep only total or overall rates, dropping raced data.

```
df_3 <- bind_rows(final_df, df_education_district_disparate) 

disp_long <- df_3 %>% filter(race == "total" & geo_level %in% c("county", "city")) %>% select(geoid, geo_name, dist_id, district_name, total_enroll, indicator, disparity_z_score, geo_level) %>% 
                      rename(variable = indicator, value = disparity_z_score)
```

* Step 2: Group data by place, then rank to find the most disparate indicator by place. Filter to keep only the most disparate indicators by place.

```
#### Rank indicators by disp_z with worst/highest disp_z = 1
disp_final <- disp_long %>%
  group_by(geoid, geo_name) %>%
  mutate(rk = min_rank(-value))

##### Select only worst/highest disparity indicator per county
disp_final <- disp_final %>% filter(rk == 1) %>% arrange(geoid) 
```

* Step 3: Add long variables names used to generate findings later and adjust for indicators that tie for most disparate.

```
#### Rename variable and value fields
names(disp_final)[names(disp_final) == 'value'] <- 'worst_disp_z'
names(disp_final)[names(disp_final) == 'variable'] <- 'worst_disp_indicator'

# join to get long indicator names for findings
worst_disp <- select(disp_final, -c(rk, worst_disp_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_disp_indicator" = "indicator_short")) %>% rename(long_disp_indicator = indicator)

# Adjust for geos with tied indicators
worst_disp2 <- worst_disp %>% 
  group_by(geoid,geo_name) %>% 
  mutate(disp_ties = n()) %>%
  mutate(long_disp_indicator = paste0(long_disp_indicator, collapse = " and ")) %>% select(-c(worst_disp_indicator)) %>% unique() # RC v5: no ties

```

* Step 4: Clean city names. Create the "shells" used to create the key finding sentences. Use the variables created above to generate key findings. 

```
worst_disp2 <- clean_city_names(worst_disp2) 

# Write findings using ifelse statements
worst_disp3 <- subset(worst_disp2, !is.na(geo_name)) %>%
  mutate(finding_type = 'worst disparity', finding = paste0(long_disp_indicator, " is the most disparate indicator in ", geo_name, "."), 
         findings_pos = 4) %>% mutate(
           finding = ifelse(
             arei_issue_area == 'Education' & geo_level == "city",
             paste0(long_disp_indicator, " (", district_name, ") is the most disparate indicator in ", geo_name, "."),
             finding
           )
         ) %>% select(geoid, geo_name, dist_id, district_name, total_enroll, geo_level, finding_type,finding, findings_pos)

```

* Step 5: As Education data is collected at school district level, not city level, additional processing is needed. Identify the worst outcome district for each Education indicator by city. Merge the worst outcome school district data with the rest of data. Finally, keep only total rate data, dropping raced rates.

```
# rank overall district disparity z-scores for each city+indicator combo
df_education_district_worst_outcome <- df_education_district %>% filter(!is.na(geoid)) %>% group_by(geoid, race_generic) %>%
                                                                 mutate(rk = dense_rank(performance_z_score)) %>% filter(rk == "1") %>% select(-rk) # RC v5 no ties

df_4 <- bind_rows(final_df, df_education_district_worst_outcome) 

outc_long <- df_4 %>% filter(race == "total" & geo_level %in% c("county", "city")) %>% select(geoid, geo_name, dist_id, district_name, total_enroll, indicator, performance_z_score, geo_level) %>%
                      rename(variable = indicator, value = performance_z_score)
```

* Step 6: Add long variables names used to generate findings later and adjust for indicators that tie for worst outcome.

```
outc_final <- outc_long %>%
  group_by(geoid, geo_name) %>%
  mutate(rk = min_rank(value))

##### Select only worst/lowest outcome indicator per county
outc_final <- outc_final %>% filter(rk == 1) %>% arrange(geoid)

##### Rename variable and value fields
names(outc_final)[names(outc_final) == 'value'] <- 'worst_perf_z'
names(outc_final)[names(outc_final) == 'variable'] <- 'worst_perf_indicator'    

# join to get long indicator names for findings
worst_outc <- select(outc_final, -c(rk, worst_perf_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_perf_indicator" = "indicator_short")) %>% rename(long_perf_indicator = indicator)

# Adjust for counties with tied indicators
worst_outc2 <- worst_outc %>% 
  group_by(geoid, geo_name) %>% 
  mutate(perf_ties = n()) %>%
  mutate(long_perf_indicator = paste0(long_perf_indicator, collapse = " and ")) %>% select(-c(worst_perf_indicator)) %>% unique() # RC v5: no ties
```

* Step 7: Clean city names. Create the "shells" used to create the key finding sentences. Use the variables created above to generate key findings. Combine Most Disparate and Worst Outcome Indicator Findings.

```
worst_outc2 <- clean_city_names(worst_outc2) 

# Write Findings using ifelse statements
worst_outc3 <- subset(worst_outc2, !is.na(geo_name)) %>%
               mutate(finding_type = 'worst overall outcome', finding = ifelse(geo_level == "county", 
                                                            paste0(long_perf_indicator, " has the worst overall outcome in ", geo_name, "."), 
                                                            paste0(long_perf_indicator, " has the worst overall outcome in ", geo_name, ".") 
                                                            ), 
         findings_pos = 5) %>% mutate(
           finding = ifelse(
             arei_issue_area == 'Education' & geo_level == "city",
             paste0(long_perf_indicator, " (", district_name, ") has the worst overall outcome in ", geo_name, "."),
             finding
           )
         ) %>% select(geoid, geo_name, dist_id, district_name, total_enroll, geo_level, finding_type, finding, findings_pos)


# Combine findings into one final df
worst_disp_outc <- union(worst_disp3, worst_outc3)
```


</details>

<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/finding_most_disparate_ind.PNG" alt="Most Disparate Indicator by Place">


### Above or Below Average Racial Disparity and Outcomes by Place

Create key findings identifying whether a geography has above or below average disparity and outcomes as compared to other geographies of the same type. These findings are based on the average disparity z-score and average outcome z-score across all indicators for each geography. When the average disparity or outcome z-score across all indicators is below zero, we say that the disparity is or outcomes are below average. If the average disparity or outcome z-score is above zero, we say that the disparity is or outcomes are above above average. These findings are found on Place pages, such as the [Eureka Place Page](https://www.racecounts.org/city/eureka/).

<details>
<summary>Code Explanation</summary>
This finding is generated with the following steps:

* Step 1: Import county and city Racial Equity Index tables from the postgres database, select only needed fields. Then, reclassify outcome and disparity z-scores as above or below average.

```
## pull composite disparity/outcome z-scores for city and county; add urban type = NA for cities.
index_county <- st_read(con, query = paste0("SELECT * FROM ", curr_schema, ".arei_composite_index_", curr_yr)) %>% select(county_id, county_name, urban_type, disparity_z, performance_z) %>% rename(geoid = county_id, geo_name = county_name) %>% mutate(geo_level = "county")

index_city <- st_read(con, query = paste0("SELECT * FROM ", curr_schema, ".arei_composite_index_city_", curr_yr)) %>% select(city_id, city_name, disparity_z, performance_z) %>% rename(geoid = city_id, geo_name = city_name) %>% mutate(urban_type = NA, geo_level = "city")

index_county_city <- rbind(index_county, index_city)       
index_county_city$geo_name <- ifelse(index_county_city$geo_level == 'county', paste0(index_county_city$geo_name, ' County'), index_county_city$geo_name)

# Above/Below Avg Disp/Outcome
avg_statement_df <- index_county_city %>% 
  mutate(pop_type = ifelse(urban_type == 'Urban', 'more', 'less'), # used for more/less populous finding
         outc_type = ifelse(performance_z < 0, 'below', 'above'),
         disp_type = ifelse(disparity_z < 0, 'below', 'worse than')) 
```

* Step 2: Create the "shells" used to create the key finding sentences. Use the variables created above to generate key findings. Combine Above and Below Average Disparity and Outcomes Findings. Then, bind those findings together. At this point, we do not export the combined findings in rda_places_findings.

```
disp_avg_statement <- avg_statement_df %>%
  mutate(finding_type = 'disparity', finding = ifelse(geo_level == "county", paste0(geo_name, "'s racial disparity across indicators is ", disp_type, " average for California counties."), 
                                                                             paste0(geo_name, "'s racial disparity across indicators is ", disp_type, " average for California cities.")),
         finding = ifelse(is.na(disp_type), paste0("Data for ", geo_name, " is too limited for this analysis."), finding),
         findings_pos = 2) %>% 
  select(geoid, geo_name, geo_level, finding_type, finding, findings_pos)

outc_avg_statement <- avg_statement_df %>% 
  mutate(finding_type = 'outcomes', finding = ifelse(geo_level == "county", paste0(geo_name, "'s overall outcomes across indicators are ", outc_type, " average for California counties."), 
                                                                            paste0(geo_name, "'s overall outcomes across indicators are ", outc_type, " average for California cities.")),
         finding = ifelse(is.na(outc_type), paste0("Data for ", geo_name, " is too limited for this analysis."), finding),
         findings_pos = 3) %>% 
  select(geoid, geo_name, geo_level, finding_type, finding, findings_pos) 

## bind everything together
worst_disp_outc_ <- worst_disp_outc %>% select(-c(dist_id, district_name, total_enroll))
rda_places_findings <- rbind(most_impacted, disp_avg_statement, outc_avg_statement, worst_disp_outc_) %>%
                              mutate(src = 'rda', citations = '') %>%
                              relocate(geo_level, .after = geo_name)
```

</details>

<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/finding_average_outcomes.PNG" alt="Average Outcomes Finding">


<p align="right">(<a href="#top">back to top</a>)</p>



### Issue-based Findings at State Level

Import key findings drafted by the Research & Data Analysis team offline. There are three findings per issue at the state level. These findings are found on the [California Place Page](https://www.racecounts.org/state/california/) and on California Issue Pages, such as the [California Democracy Page](https://www.racecounts.org/issue/democracy/).

<details>
<summary>Code Explanation</summary>
This finding is generated with the following steps:

* Step 1: Import findings drafted by the Research & Data Analysis team offline. Add additional fields required for display on state-level Issue Pages, such as finding_type.

```
issue_area_findings <- read.csv(paste0("W:/Project/RACE COUNTS/", curr_yr, "_", curr_schema, "/RC_Github/RaceCounts/KeyTakeaways/manual_findings_", curr_schema, "_", curr_yr, ".csv"), encoding = "UTF-8", check.names = FALSE)
colnames(issue_area_findings) <- c("issue_area", "finding", "findings_pos")

issue_area_findings_type_dict <- list(economy = "Economic Opportunity",
                                      education = "Education",
                                      housing = "Housing",
                                      health = "Health Care Access",
                                      democracy = "Democracy",
                                      crime = "Crime and Justice",
                                      hbe = "Healthy Built Environment")

issue_area_findings$finding_type <- ifelse(issue_area_findings$issue_area == 'economy', "Economic Opportunity",
                                           ifelse(issue_area_findings$issue_area == 'health', "Health Care Access",
                                                  ifelse(issue_area_findings$issue_area == 'crime', "Crime and Justice",
                                                         ifelse(issue_area_findings$issue_area == 'hbe', "Healthy Built Environment",
                                                                str_to_title(issue_area_findings$issue_area)))))
issue_area_findings$src <- "rda"

issue_area_findings$citations <- ""
```

* Step 2: Export table to postgres database with metadata.

```
#dbWriteTable(con, c(curr_schema, "arei_findings_issues"), issue_area_findings, overwrite = FALSE, row.names = FALSE)

# comment on table and columns
 comment <- paste0("COMMENT ON TABLE v5.arei_findings_issues IS 'findings for Issue Area pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaways\\key_findings_2023.R.';",
                  "COMMENT ON COLUMN v5.arei_findings_issues.finding_type
                       IS 'Categorizes findings: race most impacted by inequities in a geo, above/below avg disp, above/below perf, most disp indicator, worst perf indicator';",
                  "COMMENT ON COLUMN v5.arei_findings_issues.src
                       IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN v5.arei_findings_issues.citations
                       IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN v5.arei_findings_issues.findings_pos
                       IS 'Used to determine the order a set of findings should appear in on RC.org';")
# print(comment)
# dbSendQuery(con, comment)
```

* Step 3: Take the same findings from Step 2 and add additional fields required for display on the state-level Place Page, such as geo_name.

```
# prep issues table for addition to places_findings_table
state_issue_area_findings <- issue_area_findings

state_issue_area_findings <- state_issue_area_findings %>% select(-issue_area)

state_issue_area_findings$geoid <- "06"
state_issue_area_findings$geo_name <- "California"
state_issue_area_findings$geo_level <- "state"

# reorder findings position for places positions
state_issue_area_findings <- state_issue_area_findings %>%
                                mutate(findings_pos = row_number() + 1) 
```

* Step 4: Bind other Place Page findings from earlier (rda_places_findings) with state-level Place Page findings from Step 3. Export table to postgres database with metadata.

```
findings_places_multigeo <- rbind(rda_places_findings, state_issue_area_findings)

## Create postgres table
# dbWriteTable(con, c(curr_schema, "arei_findings_places_multigeo_update"), findings_places_multigeo, overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE v5.arei_findings_places_multigeo_update IS 'findings for Place pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaways\\key_findings_2023.R.';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_update.finding_type
                        IS 'Categorizes findings: race most impacted by inequities in a geo, above/below avg disp, above/below outcome, most disp indicator, worst outcome indicator';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_update.src
                        IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_update.citations
                        IS 'External citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN v5.arei_findings_places_multigeo_update.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
#print(comment)
#dbSendQuery(con, comment)

#dbDisconnect(con) 
```

</details>

<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/finding_state_issue.PNG" alt="State-level Issue Finding">


<p align="right">(<a href="#top">back to top</a>)</p>

## Data Methodology

[RACE COUNTS: Indicator Methodology for County and State  (2023)](https://github.com/catalystcalifornia/RaceCounts/blob/main/Methodology/IndicatorMethodology_CountyState.pdf) <br>
[RACE COUNTS: Indicator Methodology for City (2024)](https://github.com/catalystcalifornia/RaceCounts/blob/main/Methodology/IndicatorMethodology_City.pdf) <br> 
 
## Contributors

* [Alexandra Baker, Research & Data Analyst I](https://github.com/bakeralexan)
* [Chris Ringewald, Senior Director of Research & Data Analysis](https://github.com/cringewald)
* [David Segovia, Research & Data Analyst I](https://github.com/davidseg1997)
* [Elycia Graves, Associate Director of Research & Data Analysis](https://github.com/elyciamg)
* [Hillary Khan, Database Architect](https://github.com/hillarykhan)
* [Jennifer Zhang, Senior Research & Data Analyst](https://github.com/jzhang514)
* [Leila Forouzan, Senior Manager of Research & Data Analysis](https://github.com/lforouzan)
* [Maria Khan, Research & Data Analyst II](https://github.com/mariatkhan)


<p align="right">(<a href="#top">back to top</a>)</p>



## Contact Us

Send an email to [Chris Ringewald](https://www.linkedin.com/in/chris-ringewald-6766369/) and [Leila Forouzan](https://www.linkedin.com/in/leilaforouzan/) at rda[at]catalystcalifornia.org

## Github Link

[Click here to view the RACE COUNTS Github Repo](https://github.com/catalystcalifornia/RaceCounts)

<p align="right">(<a href="#top">back to top</a>)</p>


## Citation
To cite RACE COUNTS, please use the following:

Catalyst California; RACE COUNTS, racecounts.org, [current year].


## License

[License](License.md)

<p align="right">(<a href="#top">back to top</a>)</p>


## RACE COUNTS Partners

* [CALIFORNIA CALLS](https://www.cacalls.org/)
* [USC Dornsife](https://dornsife.usc.edu/)
* [PICO California](http://www.picocalifornia.org/)

<p align="right">(<a href="#top">back to top</a>)</p>

