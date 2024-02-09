# RACE COUNTS
### January 2024

<base target="_blank">
<!-- NOTE: Need to replace homepage image, findings images
-->

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
rc_list_ = as.data.frame(do.call(rbind, lapply(DBI::dbListObjects(con, DBI::Id(schema = "v5"))$table, function(x) slot(x, 'name'))))

# filter for only county level indicator tables, drop all others including api_*_county_2023 tables
county_list <- filter(rc_list_, grepl("^arei_.*\\county_2023$", table))
county_list <- county_list[order(county_list$table), ] # alphabetize list of state tables, changes df to list the needed format for next step

# import all tables on county_list
county_tables <- lapply(setNames(paste0("select * from v5.", county_list), county_list), DBI::dbGetQuery, conn = con)

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
        indicator = gsub('_county_2023', '', indicator),
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
    fill(ends_with("ind"), dist_id, district_name, total_enroll, .direction = 'updown')  %>% 
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
* Step 4: Prepare data for the function as follows. Add city-level education data to the rest of the indicator data. Then, remove the total_rates, keeping only rates by race. Finally, reclassify API data as both Asian and Native Hawaiian / Pacific Islander data.

```
df_ds <- bind_rows(final_df,df_education_district_disparate)
# df_ds %>% filter(is.na(geo_name)) # why do some geo_names in housing don't have a geo_name? Some of them belong to census designated places with very low pop counts-- we'll filter this out later
df_ds <- filter(df_ds, race != 'total')    # remove total rates bc all findings in this section are raced
df_ds <- api_split(df_ds) # duplicate api rates as asian and pacisl
```

* Step 5: Apply the function to the data by race, then bind results together into one dataframe.

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

* Step 6: Clean city names. Suppress findings for race-place combinations with too few indicator disparity_z scores. Create the "shells" used to create the key finding sentences. Use the variables created above to generate key findings. Note, we add school district names to education-related findings at city level.


```
n = 5 # indicator_count threshold

most_disp_final <- most_disp %>% mutate(
  finding = ifelse(indicator_count <= n,     ## Suppress finding if race+geo combo has 5 or fewer indicator disparity_z scores
                   paste0("Data for ", long_name, " residents of ", geo_name, " is too limited for this analysis."), 
                   paste0(long_name, " residents face the most disparity with ", indicator, " in ", geo_name, "."))
  ) %>% mutate(
  
  # add school district name to city education-related findings
  finding = ifelse(
    #max_col %in% educ_indicators & geo_level == "city" 
    arei_issue_area == 'Education' & !grepl('too limited', finding), 
    paste0(long_name, " residents face the most disparity with ", indicator, " (", district_name, ") in ", geo_name, "."),
    finding),
  
  # remove school district info from city non-education-related rows
  dist_id =
    ifelse(
      !arei_issue_area == 'Education' & geo_level == "city", NA, dist_id ## remove district ids for non-education indicators
    ),
  district_name =
    ifelse(
      !arei_issue_area == 'Education' & geo_level == "city", NA, district_name ## remove district names for non-education indicators
    ),
  total_enroll = 
    ifelse(
      !arei_issue_area == 'Education' & geo_level == "city", NA, total_enroll ## remove total enroll for non-education indicators
    ), 
  
  finding_type = 'most disparate', findings_pos = 3) %>%
  select(geoid, geo_name, geo_level, dist_id, district_name, total_enroll, long_name, race, indicator, indicator_count, finding_type, findings_pos, finding) %>% filter(!is.na(geo_name)) # some geoids don't have geo_names. all of them belong in housing-- for example: Camp Pendleton North. They don't pass the indicator count threshold either way to be included in the finding, so we will remove these.   

```

* Step 7: Bind Most Disparate Indicator and Counts of Best and Worst Rates findings together. Export table to postgres database with metadata.

```
rda_race_findings <- bind_rows(most_disp_final, worst_best_counts)
rda_race_findings <- rda_race_findings %>% relocate(geo_level, .after = geo_name) %>% relocate(finding_type, .after = race) %>% mutate(src = 'rda', citations = '') %>%
  mutate(race = ifelse(race == 'latino', 'latinx', ifelse(race == 'pacisl', 'nhpi', race)))  # rename latino to latinx, and pacisl to nhpi to feed API - will change API later so we can use RC standard latino/pacisl
  
## Export postgres table
dbWriteTable(con, c("v5", "arei_findings_races_multigeo_update"), rda_race_findings, overwrite = FALSE, row.names = FALSE)

# comment on table and columns
comment <- paste0("COMMENT ON TABLE v5.arei_findings_races_multigeo_update IS 'findings for Race pages (API) created using W:\\Project\\RACE COUNTS\\2023_v5\\RC_Github\\RaceCounts\\KeyTakeaway\\key_findings_2023_city.R.';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo_update.finding_type
                         IS 'Categorizes findings: count of best and worst rates by race/geo combo, most disparate indicator by race/geo combo';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo_update.src
                         IS 'Categorizes source of finding as either rda or program area';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo_update.citations
                         IS 'External v5.citations for findings are stored here. Null values mean there are no citations, all else are stored as a string with &&& acting as a delimiter between multiple citations';",
                  "COMMENT ON COLUMN v5.arei_findings_races_multigeo_update.findings_pos
                        IS 'Used to determine the order a set of findings should appear in on RC.org';")
dbSendQuery(con, comment)  

  
```

</details>



<img src="images/Most Disparate.png" alt="Most Disparate">


### Lowest Outcome Indicator by Place

Create findings identifying the indicator with the worst outcomes (lowest overall outcome z-score) in each geography. These findings are found on Place pages.

<details>

<summary>Code Explanation</summary>

The code below pulls tables from our private PostgreSQL database using credentials accessed through a separate script. This database is accessible only by our Research & Data Analysis team. However, we do plan to share a public file with the complete data for each RACE COUNTS indicator, where possible, here soon.

* Step 1: Put dataframes containing issue area index data imported from private database into a list.

```

data_list <- list(c_1, c_2, c_3, c_4, c_5, c_6, c_7)

```

* Step 2: Select just the necessary columns including the county name and overall outcome z-scores. Then merge into one matrix, removing duplicated county_id columns. Convert new matrix from 'wide' to 'long' format.

```

# Keep only perf_z column
select_cols <- lapply(data_list, select, "county_name", ends_with(c("perf_z")))
merged_perf <- reduce(select_cols, inner_join, by = "county_name")
perf_long <- melt(merged_perf, id.vars=c("county_name"))

```

* Step 3: Rank indicators with worst outcomes (lowest outcome z-score) equal to a rank of 1. Select the indicator with the worst outcome in each geography. Rename variable and value fields.

```

perf_final <- perf_long %>%
  group_by(county_name) %>%
  mutate(rk = min_rank(-value))
  
perf_final <- perf_final %>% filter(rk == 1) %>%
  arrange(county_name) 
  
names(perf_final)[names(perf_final) == 'value'] <- 'worst_perf_z'
names(perf_final)[names(perf_final) == 'variable'] <- 'worst_perf_indicator'

```

* Step 4: Clean variable names, then join with indicator name crosswalk to get long indicator names for findings.

```

perf_final$worst_perf_indicator <- gsub('_perf_z', '', perf_final$worst_perf_indicator)

worst_perf <- select(perf_final, -c(rk, worst_perf_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_perf_indicator" = "indicator_short")) %>% rename(long_perf_indicator = indicator)

```

* Step 5: Make adjustments for geographies with two or more indicators tied for Worst Outcome.

```

worst_perf2 <- worst_perf %>% 
  group_by(county_name) %>% 
  mutate(perf_ties = n()) %>%
  mutate(long_perf_indicator = paste0(long_perf_indicator, collapse = " and ")) %>% select(-c(worst_perf_indicator)) %>% unique()

```

* Step 6: Generate Worst Outcome key findings.

```

worst_perf2 <- worst_perf2 %>% 
  mutate(Lowest_Performing_Indicator = ifelse(perf_ties > 1, 
                                      paste0(county_name, " County's low overall outcomes in ", long_perf_indicator," stand out most compared to other counties."),
                                      paste0(county_name, " County's low overall outcome in ", long_perf_indicator," stands out most compared to other counties."))) %>% 
  select(county_name, Lowest_Performing_Indicator)

```


</details>

<img src="images/Lowest Performance.png" alt="Lowest Performance">

### Highest Disparity Indicator by Place

Create key findings identifying the indicator with the largest racial disparities in each geography. These findings are found on Place pages.

<details>

<summary>Code Explanation</summary>

The code below pulls tables from our private PostgreSQL database using credentials accessed through a separate script. This database is accessible only by our Research & Data Analysis team. However, we do plan to share a public file with the complete data for each RACE COUNTS indicator, where possible, here soon.

* Step 1: Put dataframes containing issue area index data imported from private database into a list.

```

data_list <- list(c_1, c_2, c_3, c_4, c_5, c_6, c_7)

```

* Step 2: Select just the necessary columns including the county name and overall disparity z-scores. Then merge into one matrix, removing duplicate county_id columns. Convert new matrix from 'wide' to 'long' format.

```

# Keep only disp_z column
select_cols <- lapply(data_list, select, 
                      "county_name",
                      ends_with(c("disp_z")))
merged_disp <- reduce(select_cols, inner_join, by = "county_name")
disp_long <- melt(merged_disp, id.vars=c("county_name"))

```

* Step 3: Rank indicators with most disparity (highest overall disparity z-score) equal to a rank of 1. Select the indicator with the worst disparity in each geography. Rename variable and value fields.

```

disp_final <- disp_long %>%
  group_by(county_name) %>%
  mutate(rk = min_rank(-value))
  
disp_final <- disp_final %>% filter(rk == 1) %>%
  arrange(county_name) 

names(disp_final)[names(disp_final) == 'value'] <- 'worst_disp_z'
names(disp_final)[names(disp_final) == 'variable'] <- 'worst_disp_indicator'    

```

* Step 4: Clean variable names, then join with indicator name crosswalk to get long indicator names for findings.

```

disp_final$worst_disp_indicator <- gsub('_disp_z', '', disp_final$worst_disp_indicator)

worst_disp <- select(disp_final, -c(rk, worst_disp_z)) %>%   # drop rank and z-score fields, join to indicator name equivalency table
  left_join(indicator, by = c("worst_disp_indicator" = "indicator_short")) %>% rename(long_disp_indicator = indicator)

```

* Step 5: Make adjustments for geographies with two or more indicators tied for worst disparity.

```

worst_disp2 <- worst_disp %>% 
  group_by(county_name) %>% 
  mutate(disp_ties = n()) %>%
  mutate(long_disp_indicator = paste0(long_disp_indicator, collapse = " and ")) %>% select(-c(worst_disp_indicator)) %>% unique()

```

* Step 6: Generate Highest Disparity key findings.

```

worst_disp2 <- worst_disp2 %>% 
  mutate(Most_Disp_Indicator = ifelse(disp_ties > 1, 
                                      paste0(county_name, " County's high racial disparity in ", long_disp_indicator," stand out most compared to other counties."),
                                      paste0(county_name, " County's high racial disparity in ", long_disp_indicator," stands out most compared to other counties."))) %>% 
  select(county_name, Most_Disp_Indicator)

```

</details>

<img src="images/Highest Disparity.png" alt="Highest Disparity">

### Above or Below Average Racial Disparity and Outcomes by Place

Create key findings identifying whether a geography has above or below average disparity and outcomes as compared to other geographies of the same type. These findings are based on the average disparity z-score and average outcome z-score across all indicators for each geography. When the average disparity or outcome z-score across all indicators is below zero, we say that the disparity is or outcomes are below average. If the average disparity or outcome z-score is above zero, we say that the disparity is or outcomes are above above average. These findings are found on Place pages.

<details>

<summary>Code Explanation</summary>
The code below pulls tables from our private PostgreSQL database using credentials accessed through a separate script. This database is accessible only by our Research & Data Analysis team. However, we do plan to share a public file with the complete data for each RACE COUNTS indicator, where possible, here soon.

* Step 1: After importing dataframe containing composite index data imported from private database, select only needed fields. Then reclassify outcome and disparity z-scores as above or below average.

```

sum_statement_df <- c_1 %>% 
  select(county_id, county_name, urban_type, disparity_z, performance_z) %>%
  mutate(perf_type = ifelse(performance_z < 0, 'below', 'above'),
         disp_type = ifelse(disparity_z < 0, 'below', 'above'))

```

* Step 2: Generate Above and Below Average key findings for disparity and outcomes.

```
sum_statement_df <- sum_statement_df  %>% 
  mutate(Perf_Level_Statement = ifelse(is.na(perf_type), NA, paste0(county_name, " County's outcomes across indicators are ", perf_type, " average for California counties.")),
                      Disp_Level_Statement = ifelse(is.na(disp_type), NA, paste0(county_name, " County's racial disparity across indicators is ", disp_type, " average for California counties."))) %>% 
  select(county_name, Perf_Level_Statement, Disp_Level_Statement) 

sum_statement_df <- sum_statement_df[order(sum_statement_df$county_name),]

```

</details>

<img src="images/Disparity Finding.png" alt="Disparity Finding">

<img src="images/Performance Finding.png" alt="Performance Finding">


<p align="right">(<a href="#top">back to top</a>)</p>



## Data Methodology

[RACE COUNTS: Indicator Methodology for County and State (2023)](https://github.com/catalystcalifornia/RaceCounts/blob/main/Methodology/IndicatorMethodology_CountyState.pdf) <br>
<!-- [RACE COUNTS: Indicator Methodology for City (2023)](https://github.com/catalystcalifornia/RaceCounts/blob/main/Methodology/IndicatorMethodology_City.pdf) <br> -->
 
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

[Chris Ringewald](https://www.linkedin.com/in/chris-ringewald-6766369/) - cringewald[at]catalystcalifornia.org  <br>


[Leila Forouzan](https://www.linkedin.com/in/leilaforouzan/) - lforouzan[at]catalystcalifornia.org

## Github Link

[Click here to view the RACE COUNTS Github Repo](https://github.com/catalystcalifornia/RaceCounts)

<p align="right">(<a href="#top">back to top</a>)</p>


## Citation
To cite RACE COUNTS, please use the following:

Catalyst California; RACE COUNTS, racecounts.org, 2022.


## License

[License](License.md)

<p align="right">(<a href="#top">back to top</a>)</p>


## RACE COUNTS Partners

* [CALIFORNIA CALLS](https://www.cacalls.org/)
* [USC Dornsife](https://dornsife.usc.edu/)
* [PICO California](http://www.picocalifornia.org/)

<p align="right">(<a href="#top">back to top</a>)</p>

