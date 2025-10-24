#### This script was a start at getting all the params/vars needed for each
# district. Can develop further (with real or fake data) to test automation.
# For organization can update this script and source the final df into 
# run_briefs.R

library(stringr)
library(scales)

source("W:\\RDA Team\\R\\credentials_source.R")

conn <- connect_to_db("racecounts")
conn_mosaic <- connect_to_db("mosaic")
schema <- "v7"
yr <- "2025" 
geolevel <- "leg" 

leg_indicator_list <- c("incarceration",
                    "officer_initiated_stops",
                    "use_of_force",
                    "census_participation",
                    "connected_youth",
                    "employment",
                    "internet",
                    "living_wage",
                    "officials",
                    "per_capita_income",
                    "real_cost_measure",
                    "chronic_absenteeism",
                    "ece_access",
                    "gr3_ela_scores",
                    "gr3_math_scores",
                    "hs_grad",
                    "staff_diversity",
                    "suspension",
                    "drinking_water",
                    "haz_weighted_avg",
                    "lack_of_greenspace",
                    "toxic_release",
                    "health_insurance",
                    "cost_burden_renter",
                    "denied_mortgages",
                    "eviction_filing_rate",
                    "foreclosure",
                    "homeownership",
                    "housing_quality",
                    "overcrowded",
                    "subprime")

issues <- dbGetQuery(conn, 
                     statement=paste0("SELECT arei_issue_area_id, api_name_short, api_name_v2 FROM ", 
                                      schema,".","arei_issue_list;")) %>%
  # filter out demo and htlh indexes (only 1 indicator)
  # filter(!(api_name_v2 %in% c("democracy", "health_care_access"))) %>%
  mutate(index_table_name = case_when(
    api_name_v2 == "democracy" ~ paste0(schema, ".",paste("arei", api_name_short, "census_participation", geolevel, yr,sep="_")),
    api_name_v2 == "health_care_access" ~ paste0(schema, ".",paste("arei", api_name_short, "health_insurance", geolevel, yr,sep="_")),
    .default = paste0(schema, ".",paste("arei", api_name_short, "index", geolevel, yr,sep="_"))))

cntyst_indicators <- dbGetQuery(conn, 
                         statement=paste0("SELECT arei_indicator, arei_issue_area, arei_issue_area_id, arei_best, api_name FROM ", 
                                          schema,".","arei_indicator_list_cntyst;")) 
leg_indicators <- cntyst_indicators %>%
  filter(api_name %in% leg_indicator_list)

combined <- leg_indicators %>%
  left_join(issues, by = "arei_issue_area_id") %>%
  mutate(table_name = paste0(schema,".", paste("arei", api_name_short, api_name, geolevel, yr, sep="_")))

# Get composite rank in disparity and outcomes
composite_index <- dbGetQuery(conn, 
                              statement=paste0("SELECT leg_id, leg_name, geolevel, disparity_rank, performance_rank FROM ", 
                                               schema,".","arei_composite_index_leg_", yr, ";"))

# get each issue area index from pg
issue_indexes <- list()
for(i in 1:nrow(issues)) {
  issue_name <- issues[i, "api_name_short"]
  api_name_v2 <- issues[i, "api_name_v2"]
  index_table_name <- issues [i, "index_table_name"]
  sql_query <- paste0("SELECT * FROM ", index_table_name, ";")
  x <- dbGetQuery(conn, sql_query) 
  if(issue_name=="demo"){
    x <- x %>%
      rename(demo_disp_z = disparity_z,
             demo_perf_z = performance_z)
  }
  if(issue_name=="hlth"){
    x <- x %>%
      rename(hlth_disp_z = disparity_z,
             hlth_perf_z = performance_z)
  }
  x_transform <- x %>%
    select(leg_id, leg_name, geolevel, ends_with("_rank"), ends_with("quadrant"), ends_with("_perf_z"), ends_with("_disp_z")) %>%
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


list2env(issue_indexes, .GlobalEnv)

# Combine all issue area dfs into 1 df
all_indicators <- rbind(crim, demo, econ, educ, hben, hlth, hous)

# Get summaries of each issue area based on quadrant
all_issues <- all_indicators %>%
  select(leg_id, leg_name, geolevel, quadrant, issue_area) %>%
  distinct() %>%
  complete(leg_id, issue_area,
           fill = list(disparity_rank = NA, 
                       performance_rank = NA, 
                       quadrant = NA)) %>%
  group_by(leg_id) %>%
  fill(leg_name, .direction = "downup") %>%
  ungroup() %>%
  mutate(outcome_summary=case_when(
    quadrant=="red" ~ "Worse",
    quadrant=="orange" ~ "Better",
    quadrant=="yellow" ~ "Worse",
    quadrant=="purple" ~ "Better",
    .default = "Not enough available data for comparison"
  ),
  disparity_summary=case_when(
    quadrant=="red" ~ "Worse",
    quadrant=="orange" ~ "Worse",
    quadrant=="yellow" ~ "Better",
    quadrant=="purple" ~ "Better",
    .default = "Not enough available data for comparison"
  )) %>%
  select(-c(quadrant, leg_name))%>%
  pivot_wider(names_from = issue_area,
              values_from = c(outcome_summary, disparity_summary),
              names_glue = "{issue_area}_{.value}") 

# Get 5 indicators with worst outcomes 
worst_outcomes <- dbGetQuery(conn = conn_mosaic,
                             statement = "SELECT leg_id, geolevel, variable, rk FROM v7.indicator_outc_rk WHERE rk <=5;") %>%
  left_join(leg_indicators, by=c("variable"="api_name")) %>%
  select(leg_id, geolevel, rk, arei_indicator) %>%
  pivot_wider(names_prefix = "worst_outcome_",
              names_from = rk,
              values_from = arei_indicator)

# Get 5 indicators with most disparity 
worst_disparity <- dbGetQuery(conn = conn_mosaic,
                                 statement = "SELECT leg_id, geolevel, variable, rk FROM v7.indicator_disp_rk WHERE rk <=5;") %>%
  left_join(leg_indicators, by=c("variable"="api_name")) %>%
  select(leg_id, geolevel, rk, arei_indicator) %>%
  pivot_wider(names_prefix = "worst_disparity_",
              names_from = rk,
              values_from = arei_indicator)

dbDisconnect(conn_mosaic)

# findings
place_findings <- dbGetQuery(conn=conn,
                              statement = "SELECT geoid as leg_id, geo_level as geolevel, finding_type, finding as most_impacted_part1 FROM v7.arei_findings_places_multigeo where finding_type='most impacted' and (geo_level='sldu' or geo_level = 'sldl');") %>%
  mutate(most_impacted_part1 = gsub("\\.", ",", most_impacted_part1))

race_findings <- dbGetQuery(conn=conn,
                            statement = "SELECT geoid as leg_id, geo_level as geolevel, race, finding_type, finding  FROM v7.arei_findings_races_multigeo where finding_type='worst count' and (geo_level='sldu' or geo_level = 'sldl');")  %>%
  mutate(worst_count = as.numeric(str_extract(finding, "\\d+(?=\\s+of\\s+the)")),
         total_count = as.numeric(str_extract(finding, "(?<=of\\sthe\\s)\\d+")),
         race = gsub("latino", "latinx", race),
         race = gsub("nhpi", "native hawaiian / pacific islander", race),
         race = gsub("aian", "american indian / alaska native", race),
         race = gsub("other", "another race", race),
         race = gsub("swana", "southwest asian / north african", race),
         race = gsub("filipino", "filipinx", race),
         race = gsub("twoormor", "multiracial", race)
         ) %>%
  group_by(leg_id, geolevel) %>%
  filter(worst_count == max(worst_count, na.rm = TRUE)) %>%
  summarise(
    race = paste(str_to_title(race), collapse = " and "),
    finding_type = first(finding_type),
    worst_count = first(worst_count),
    total_count = max(total_count),
    most_impacted_part2 = paste0("with the worst rates for ", worst_count, " out of ",
                                 total_count, 
                                 " indicators with available data. All races lose when systemic racism remains unchecked though."),
    .groups = "drop") %>%
  select(leg_id, geolevel, most_impacted_part2) 

most_impacted_race_findings <- place_findings %>%
  left_join(race_findings) %>%
  mutate(most_impacted_race_finding = paste(most_impacted_part1, most_impacted_part2)) %>%
  select(leg_id, geolevel, most_impacted_race_finding)

dbDisconnect(conn)

# Add leg member names
ad_members <- read.csv("W:\\Project\\RACE COUNTS\\2025_v7\\Leg_Dist_PDFs\\ca_assembly_members_2025.csv") %>%
  separate_wider_delim(cols=Name,
                       delim=", ",
                       names = c("last_name", "first_name")) %>%
  mutate(District = gsub("District: ", "060", District),
         rep_name = paste(first_name, last_name),
         geolevel="sldl") %>%
  select(-c(first_name, last_name))

sd_members <- read.csv("W:\\Project\\RACE COUNTS\\2025_v7\\Leg_Dist_PDFs\\ca_senate_members_2025.csv") %>%
  mutate(Name = str_sub(Name, end=-(4+1)),
         District = gsub("District ", "060", District),
         geolevel="sldu") %>%
  rename(rep_name=Name)

# replace senate descriptions:
senate_geo_descriptions <- read.csv("W:\\Project\\RACE COUNTS\\2025_v7\\Leg_Dist_PDFs\\ca_senate_members_2025_sub_county_descriptors.csv") %>%
  mutate(District=gsub("District ", "060", District))

sd_members <- sd_members %>% left_join(senate_geo_descriptions,by="District",suffix=c("", "_updated")) %>%
  select(District, rep_name, Party, Characteristics_updated, geolevel) %>%
  rename(Characteristics=Characteristics_updated)

all_members <- rbind(ad_members, sd_members) %>%
  rename(leg_id=District)



# create final df: composite ranks, issue area summaries, and worst outcome and disparity indicators
final_df <- composite_index %>%
  rename(composite_disparity_rank=disparity_rank,
         composite_performance_rank=performance_rank) %>%
  mutate(composite_disparity_rank=scales::label_ordinal()(composite_disparity_rank),
         composite_performance_rank=scales::label_ordinal()(composite_performance_rank)) %>%
  left_join(all_issues, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(worst_outcomes, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(worst_disparity, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(most_impacted_race_findings, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(all_members, by=c("leg_id","geolevel"), keep = FALSE) %>%
  mutate(district_number=str_sub(leg_id,-2,-1)) %>%
  # format text so special characters don't break latex
  mutate(across(where(is.character), ~str_replace_all(., "&", "\\\\&")))

  