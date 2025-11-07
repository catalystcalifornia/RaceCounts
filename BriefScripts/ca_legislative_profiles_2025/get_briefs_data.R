#### This script pulls in all the needed data for the Leg Profiles and puts into 'final_df'.
# The run_briefs.R script runs this script. So we do not run this script directly.

# Set up ---------------------------------------------------

library(stringr)
library(scales)

source("W:\\RDA Team\\R\\credentials_source.R")
source("./BriefScripts/ca_legislative_profiles_2025/leg_brief_fx.R")

conn <- connect_to_db("racecounts")
conn_mosaic <- connect_to_db("mosaic")
conn_xwalk <- connect_to_db("rda_shared_data")
schema <- "v7"
yr <- "2025" 
geolevel <- "leg" 
num_ind <- 5    # pull top "num_ind" most disparate/worst outcome indicators per district

# Set source for district members+party affiliations
sen_members <- "W:\\Project\\RACE COUNTS\\2025_v7\\Leg_Dist_PDFs\\ca_senate_members_2025.csv"
assm_members <- "W:\\Project\\RACE COUNTS\\2025_v7\\Leg_Dist_PDFs\\ca_assembly_members_2025.csv"

# Set source for more detailed descriptions of urban Senate districts
sen_descr_detail <- "W:\\Project\\RACE COUNTS\\2025_v7\\Leg_Dist_PDFs\\ca_senate_members_2025_sub_county_descriptors.csv"  # more detailed for urban districts

# Prep District Geographic Descriptions ---------------------------------------------------
# Import district-county xwalks
sen_descr <- dbGetQuery(conn_xwalk, "SELECT sldu24 AS leg_id, geo_name AS county_name, afact AS pct_in_dist, num_dist AS count FROM crosswalks.county_2020_state_senate_2024")
sen_descr <- sen_descr %>% 
  mutate(county_name = gsub(" CA", "", sen_descr$county_name),
         geolevel = 'sldu')

assm_descr <- dbGetQuery(conn_xwalk, "SELECT sldl24 AS leg_id, geo_name AS county_name, afact AS pct_in_dist, num_dist AS count FROM crosswalks.county_2020_state_assembly_2024")
assm_descr <- assm_descr %>% 
  mutate(county_name = gsub(" CA", "", assm_descr$county_name),
         geolevel = 'sldl')

# Prep district geographic descriptions (Characteristics)
sen_descr_grp <- prep_dist_descr1(sen_descr, county_name, pct_in_dist)
sen_descr_wide <- prep_dist_descr2(sen_descr_grp, "county_name")
sen_descr_final <- prep_dist_descr3(sen_descr_wide)

assm_descr_grp <- prep_dist_descr1(assm_descr, county_name, pct_in_dist)
assm_descr_wide <- prep_dist_descr2(assm_descr_grp, "county_name")
assm_descr_final <- prep_dist_descr3(assm_descr_wide)

# Update descriptions for urban Senate districts
urban_senate_descr <- read.csv(sen_descr_detail) %>%
  mutate(District=gsub("District ", "060", District)) %>%
  filter(Characteristics !='') %>%  # keep only districts with detailed descriptions
  select(-c(Name, Party))           # drop rep name and party

sen_descr_final <- sen_descr_final %>% left_join(urban_senate_descr,by=c(leg_id="District")) %>%
  rename(old_final_descr = final_descr) %>%
  mutate(final_descr = ifelse(is.na(Characteristics), old_final_descr, Characteristics)) %>%
  select(-Characteristics, -old_final_descr)

all_descr <- rbind(assm_descr_final, sen_descr_final)   # combine Assm/Sen geographic descriptions
all_descr$Characteristics_final <- toupper(str_sub(all_descr$final_descr, 1, 1))
all_descr$Characteristics_final <- paste0("District includes: ", all_descr$Characteristics_final, str_sub(all_descr$final_descr, 2, nchar(all_descr$final_descr)))
all_descr <- all_descr %>% 
  select(-final_descr) %>%
  rename(Characteristics = Characteristics_final)

dbDisconnect(conn_xwalk)

# Prep Assm/Sen Rep Names + Party ---------------------------------------------------
# Get leg member names
ad_members <- read.csv(assm_members) %>%
  separate_wider_delim(cols=Name,
                       delim=", ",
                       names = c("last_name", "first_name")) %>%
  mutate(District = gsub("District: ", "060", District),
         rep_name = paste(first_name, last_name),
         geolevel="sldl") %>%
  select(-c(first_name, last_name, Characteristics))

sd_members <- read.csv(sen_members) %>%
  mutate(Name = str_sub(Name, end=-(4+1)),
         District = gsub("District ", "060", District),
         geolevel="sldu") %>%
  rename(rep_name=Name) %>%
  select(-Characteristics)

all_members <- rbind(ad_members, sd_members) %>%  # combine Assm/Sen Rep+Party info
  rename(leg_id=District)

# combine Leg geographic descriptions and rep info
all_dist <- all_descr %>% 
  left_join(all_members, by = c("leg_id", "geolevel")) %>%
  mutate(rep_name = sub("Dr. ", "", rep_name))   # Drop "Dr." title bc inconsistently applied


# Import RC issue names ---------------------------------------------------
issues <- dbGetQuery(conn,
                     statement=paste0("SELECT arei_issue_area_id, api_name_short, api_name_v2 FROM ",
                                      schema,".","arei_issue_list;")) %>%
  # filter out demo and htlh indexes (only 1 indicator)
  # filter(!(api_name_v2 %in% c("democracy", "health_care_access"))) %>%
  mutate(index_table_name = case_when(
    api_name_v2 == "democracy" ~ paste0(schema, ".",paste("arei", api_name_short, "census_participation", geolevel, yr,sep="_")),
    api_name_v2 == "health_care_access" ~ paste0(schema, ".",paste("arei", api_name_short, "health_insurance", geolevel, yr,sep="_")),
    .default = paste0(schema, ".",paste("arei", api_name_short, "index", geolevel, yr,sep="_"))))


# Import & clean indicator names ---------------------------------------------------
leg_indicators <- dbGetQuery(conn, 
                         statement=paste0("SELECT arei_indicator, arei_issue_area, arei_issue_area_id, arei_best, api_name FROM ", 
                                          schema,".","arei_indicator_list_", geolevel, ";")) 

leg_indicators <- leg_indicators %>%
  rename(old_arei_indicator = arei_indicator) %>%
  mutate(arei_indicator = case_when
    (old_arei_indicator == 'Officials and Managers' ~ 'Employment as Officials & Managers',
     old_arei_indicator == 'Proximity to Hazards' ~ 'Proximity to Environmental Hazards',
     old_arei_indicator == 'Suspensions' ~ 'School Suspensions',
     old_arei_indicator == 'Use of Force' ~ 'Police Use of Force',
     TRUE ~ old_arei_indicator)) %>%
  relocate(arei_indicator, .after = old_arei_indicator)


combined <- leg_indicators %>%
  left_join(issues, by = "arei_issue_area_id") %>%
  mutate(table_name = paste0(schema,".", paste("arei", api_name_short, api_name, geolevel, yr, sep="_")))



# Prep Index Data ---------------------------------------------------
# Get composite rank in disparity and outcomes
composite_index <- dbGetQuery(conn, 
                              statement=paste0("SELECT leg_id, leg_name, geolevel, disparity_rank, performance_rank FROM ", 
                                               schema,".","arei_composite_index_", geolevel, "_", yr, ";"))

# Get each issue area index from pg (Democracy & Health are indicators, not indexes)
issue_indexes <- list()
for(i in 1:nrow(issues)) {
  issue_name <- issues[i, "api_name_short"]
  api_name_v2 <- issues[i, "api_name_v2"]
  index_table_name <- issues [i, "index_table_name"]
  sql_query <- paste0("SELECT * FROM ", index_table_name, ";")
  x <- dbGetQuery(conn, sql_query) 
  if(issue_name=="demo"){     # rename disp/perf z cols to match issue index disp/perf z cols
    x <- x %>%
      rename(democracy_disparity_z = disparity_z,
             democracy_performance_z = performance_z)
  }
  if(issue_name=="hlth"){     # rename disp/perf z cols to match issue index disp/perf z cols
    x <- x %>%
      rename(health_care_access_disparity_z = disparity_z,
             health_care_access_performance_z = performance_z)
  }
  x_transform <- x %>%
    select(leg_id, leg_name, geolevel, ends_with("_rank"), ends_with("quadrant"), ends_with("_performance_z"), ends_with("_disparity_z")) %>%
    pivot_longer(
      cols = ends_with(c("_performance_z", "_disparity_z")),
      names_to = c("indicator", "metric"),
      names_pattern = "(.*)_(performance_z|disparity_z)",
      values_to = "value"
    ) %>%
    pivot_wider(
      names_from = metric,
      values_from = value
    ) %>% 
    filter(indicator %in% c(issues$api_name_v2))   # keep only issue area index rows
  
  df <- x_transform %>%
    mutate(issue_area=issue_name)
  
  colnames(df) <- gsub(paste0(api_name_v2,"_"), "", colnames(df))
    
  issue_indexes[[issue_name]] <- df

}

# Add df's from "loop" above to environment
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


# Prep Worst Disparity/Outcomes Indicators ---------------------------------------------------
# Get 5 indicators with worst outcomes 
worst_outcomes <- dbGetQuery(conn = conn_mosaic,
                             statement = paste0("SELECT leg_id, geolevel, variable, rk FROM ", schema, ".indicator_outc_rk WHERE rk <=", num_ind, ";")) %>%
  left_join(leg_indicators, by=c("variable"="api_name")) %>%
  select(leg_id, geolevel, rk, arei_indicator) %>%
  pivot_wider(names_prefix = "worst_outcome_",
              names_from = rk,
              values_from = arei_indicator)

# Get 5 indicators with most disparity 
worst_disparity <- dbGetQuery(conn = conn_mosaic,
                                 statement = paste0("SELECT leg_id, geolevel, variable, rk FROM ", schema, ".indicator_disp_rk WHERE rk <=", num_ind, ";")) %>%
  left_join(leg_indicators, by=c("variable"="api_name")) %>%
  select(leg_id, geolevel, rk, arei_indicator) %>%
  pivot_wider(names_prefix = "worst_disparity_",
              names_from = rk,
              values_from = arei_indicator)

dbDisconnect(conn_mosaic)

# Prep Most Impacted Race Finding ---------------------------------------------------
# get findings
place_findings <- dbGetQuery(conn=conn,
                             statement = paste0("SELECT geoid as leg_id, geo_level as geolevel, finding_type, finding as most_impacted_part1 FROM ", schema, ".arei_findings_places_multigeo where finding_type='most impacted' and (geo_level='sldu' or geo_level = 'sldl');")) %>%
  mutate(most_impacted_part1 = sub(",", " in", most_impacted_part1),     # replace first comma with "in"
         most_impacted_part1 = sub("(\\d)(?!.*\\d)", "\\1,", most_impacted_part1, perl = TRUE),   # add comma after last numeric char
         most_impacted_part1 = sub("\\.", ",", most_impacted_part1))     # replace first period with comma

race_findings <- dbGetQuery(conn=conn,
                            statement = paste0("SELECT geoid as leg_id, geo_level as geolevel, race, finding_type, finding FROM ", schema, ".arei_findings_races_multigeo where finding_type='worst count' and (geo_level='sldu' or geo_level = 'sldl');"))  %>%
  mutate(worst_count = as.numeric(str_extract(finding, "\\d+(?=\\s+of\\s+the)")),
         total_count = as.numeric(str_extract(finding, "(?<=of\\sthe\\s)\\d+")),
         ) %>%
  group_by(leg_id, geolevel) %>%
  filter(worst_count == max(worst_count, na.rm = TRUE)) %>%
  summarise(
    race = paste(race, collapse = " and "),
    finding_type = first(finding_type),
    worst_count = first(worst_count),
    total_count = max(total_count),
    most_impacted_part2 = paste0("with the worst rates for ", worst_count, " out of ",
                                 total_count, 
                                 " indicators with available data. All races lose when systemic racism remains unchecked."),
    .groups = "drop") %>%
  select(leg_id, geolevel, most_impacted_part2) 

most_impacted_race_findings <- place_findings %>%
  left_join(race_findings, by = c("leg_id", "geolevel")) %>%
  mutate(most_impacted_race_finding = paste(most_impacted_part1, most_impacted_part2)) %>%
  select(leg_id, geolevel, most_impacted_race_finding)

dbDisconnect(conn)


# Prep Final Dataframe used in profile RMD ---------------------------------------------------
# create final df: geographic descriptions, rep name+party, composite ranks, issue area summaries,  worst outcome and disparity indicators, and most impacted race
final_df <- composite_index %>%
  rename(composite_disparity_rank=disparity_rank,
         composite_outcome_rank=performance_rank) %>%
  mutate(composite_disparity_rank=scales::label_ordinal()(composite_disparity_rank),
         composite_outcome_rank=scales::label_ordinal()(composite_outcome_rank)) %>%
  left_join(all_issues, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(worst_outcomes, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(worst_disparity, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(most_impacted_race_findings, by=c("leg_id","geolevel"), keep = FALSE) %>%
  left_join(all_dist, by=c("leg_id","geolevel"), keep = FALSE) %>%
  mutate(district_number=str_sub(leg_id,-2,-1)) %>%
  # format text so special characters don't break latex
  mutate(across(where(is.character), ~str_replace_all(., "&", "\\\\&"))) %>%
  mutate(leg_type = case_when(geolevel == 'sldl' ~ 'AD',
                              geolevel == 'sldu' ~ 'SD'))

  