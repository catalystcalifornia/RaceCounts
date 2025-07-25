#### This script was a start at getting all the params/vars needed for each
# district. Can develop further (with real or fake data) to test automation.
# For organization can update this script and source the final df into 
# run_briefs.R

library(stringr)
library(scales)

source("W:\\RDA Team\\R\\credentials_source.R")

conn <- connect_to_db("racecounts")
schema <- "v6" # substitute until v7 done
yr <- "2024" # substitute until v7 done
geolevel <- "county" #substitute until leg data collection complete

indicators <- dbGetQuery(conn, statement=paste0("SELECT arei_indicator, arei_issue_area, arei_issue_area_id, arei_best, api_name FROM ", schema,".","arei_indicator_list_cntyst;"))
issues <- dbGetQuery(conn, statement=paste0("SELECT arei_issue_area_id, api_name_short, api_name_v2 FROM ", schema,".","arei_issue_list;")) %>%
  mutate(index_table_name = paste0(schema, ".",paste("arei", api_name_short, "index", yr,sep="_")))

combined <- indicators %>%
  left_join(issues, by = "arei_issue_area_id") %>%
  mutate(table_name = paste0(schema,".", paste("arei", api_name_short, api_name, geolevel, yr, sep="_")))

# Get composite rank in disparity and outcomes
composite_index <- dbGetQuery(conn, 
                              statement=paste0("SELECT county_id, county_name, disparity_rank, performance_rank FROM ", schema,".","arei_composite_index_", yr, ";"))

# get each issue area index from pg
issue_indexes <- list()
for(i in 1:nrow(issues)) {
  issue_name <- issues[i, "api_name_short"]
  api_name_v2 <- issues[i, "api_name_v2"]
  index_table_name <- issues [i, "index_table_name"]
  sql_query <- paste0("SELECT * FROM ", index_table_name, ";")
  x <- dbGetQuery(conn, sql_query) 
  x_transform <- x %>%
    select(county_id, county_name, ends_with("_rank"), ends_with("_quadrant"), ends_with("_perf_z"), ends_with("_disp_z")) %>%
    pivot_longer(
      cols = ends_with(c("_perf_z", "_disp_z")),
      names_to = c("indicator", "metric"),
      names_pattern = "(.*)_(perf_z|disp_z)",
      values_to = "value"
    ) %>%
    pivot_wider(
      names_from = metric,
      values_from = value
    )
  
  df <- x_transform %>%
    # select(county_id, county_name, ends_with("_rank"), ends_with("_quadrant")) %>%
    # left_join(x_transform, by="county_id") %>%
    mutate(issue_area=issue_name)
  
  colnames(df) <- gsub(paste0(api_name_v2,"_"), "", colnames(df))
    
  issue_indexes[[issue_name]] <- df

}

dbDisconnect(conn)
list2env(issue_indexes, .GlobalEnv)

# Combine all issue area dfs into 1 df
all_indicators <- rbind(crim, demo, econ, educ, hben, hlth, hous)

# Get summaries of each issue area based on quadrant
all_issues <- all_indicators %>%
  select(county_id, county_name, quadrant, issue_area) %>%
  distinct() %>%
  complete(county_id, issue_area,
           fill = list(disparity_rank = NA, 
                       performance_rank = NA, 
                       quadrant = NA)) %>%
  group_by(county_id) %>%
  fill(county_name, .direction = "downup") %>%
  ungroup() %>%
  mutate(summary=case_when(
    quadrant=="red" ~ "Worse outcomes and higher disparity",
    quadrant=="orange" ~ "Better outcomes and higher disparity",
    quadrant=="yellow" ~ "Worse outcomes and lower disparity",
    quadrant=="purple" ~ "Better outcomes and lower disparity",
    .default = "Not enough available data for comparison"
  )) %>%
  select(-quadrant)%>%
  pivot_wider(names_from = issue_area,
              values_from = summary,
              names_glue = "{issue_area}_summary")

# indicator codes from index table columns do not match arei_indicator_list names
# creating a table to recode
indicator_codes <- all_indicators %>% select(indicator) %>% distinct() %>%
  left_join(indicators %>% select(arei_indicator, api_name), by=c("indicator"="api_name")) %>%
  mutate(arei_indicator=case_when(
    indicator=="safety"~"Perception of Safety",
    indicator=="offenses"~"Arrests for Status Offenses",
    indicator=="force"~"Use of Force",
    indicator=="stops"~"Officer-Initiated Stops",
    indicator=="census"~"Census Participation",
    indicator=="candidate"~"Diversity of Candidates",
    indicator=="elected"~"Diversity of Elected Officials",
    indicator=="voter"~"Registered Voters",
    indicator=="midterm"~"Voting in Midterm Elections",
    indicator=="president"~"Voting in Presidential Elections",
    indicator=="connected"~"Connected Youth",
    indicator=="employ"~"Employment",
    indicator=="percap"~"Per Capita Income",
    indicator=="realcost"~"Cost-of-Living Adjusted Poverty",
    indicator=="living wage"~"Living Wage",
    indicator=="abst"~"Chronic Absenteeism",
    indicator=="grad"~"High School Graduation",
    indicator=="ela"~"3rd Grade English Proficiency",
    indicator=="math"~"3rd Grade Math Proficiency",
    indicator=="susp"~"Suspensions",
    indicator=="ece"~"Early Childhood Education Access",
    indicator=="diver"~"Teacher & Staff Diversity",
    indicator=="water"~"Drinking Water Contaminants",
    indicator=="food"~"Food Access",
    indicator=="hazard"~"Proximity to Hazards",
    indicator=="toxic"~"Toxic Releases from Facilities",
    indicator=="green"~"Lack of Greenspace",
    indicator=="help"~"Got Help",
    indicator=="insur"~"Health Insurance",
    indicator=="life"~"Life Expectancy",
    indicator=="bwt"~"Low Birthweight",
    indicator=="usoc"~"Usual Source of Care",
    indicator=="hosp"~"Preventable Hospitalizations",
    indicator=="burden_own"~"Housing Cost Burden (Owner)",
    indicator=="burden_rent"~"Housing Cost Burden (Renter)",
    indicator=="denied"~"Denied Mortgage Applications",
    indicator=="eviction"~"Evictions",
    indicator=="forecl"~"Foreclosure",
    indicator=="homeown"~"Homeownership",
    indicator=="quality"~"Housing Quality",
    indicator=="homeless"~"Student Homelessness",
    .default = arei_indicator
  ))


# Get 5 indicators with worst outcomes (lowest z?)
worst_5_outcomes <- all_indicators %>%
  select(county_id, county_name, indicator, perf_z) %>%
  group_by(county_id, county_name) %>%
  slice_min(perf_z, n=5) %>%
  mutate(rank=row_number()) %>%
  ungroup() %>%
  left_join(indicator_codes, by="indicator") %>% 
  select(county_id, county_name, rank, arei_indicator) %>%
  pivot_wider(names_from=rank,
              values_from = arei_indicator,
              names_glue = "worst_outcome_{rank}")

# Get 5 indicators with most disparity (highest z?)
worst_5_disparity <- all_indicators %>%
  select(county_id, county_name, indicator, disp_z) %>%
  group_by(county_id, county_name) %>%
  slice_max(disp_z, n=5) %>%
  mutate(rank=row_number()) %>%
  ungroup() %>%
  left_join(indicator_codes, by="indicator") %>% 
  select(county_id, county_name, rank, arei_indicator) %>%
  pivot_wider(names_from=rank,
              values_from = arei_indicator,
              names_glue = "worst_disparity_{rank}")
  
# create final df: composite ranks, issue area summaries, and worst outcome and 
# disparity indicators
geolevels <- c("Assembly", "Senate")

final_df <- composite_index %>%
  rename(composite_disparity_rank=disparity_rank,
         composite_performance_rank=performance_rank) %>%
  mutate(composite_disparity_rank=scales::label_ordinal()(composite_disparity_rank),
         composite_performance_rank=scales::label_ordinal()(composite_performance_rank)) %>%
  left_join(all_issues, by=c("county_id","county_name")) %>%
  left_join(worst_5_outcomes, by=c("county_id","county_name")) %>%
  left_join(worst_5_disparity, by=c("county_id","county_name")) %>%
  # cosmetic changes to emulate leg district df 
  rename(district_number=county_id,
         rep_name=county_name) %>%
  # get two-digit district number, fake rep names
  mutate(district_number=str_sub(district_number,-2,-1),
         rep_name=paste(rep_name, "Surname"),
         geolevel=sample(geolevels, size=nrow(.), replace=T))
  
  