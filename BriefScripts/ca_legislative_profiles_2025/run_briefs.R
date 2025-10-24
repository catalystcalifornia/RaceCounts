# This file runs the test_brief.Rmd 
# File is temporarily created here and then moved to outside of the repo
# We do not want to store 80 pdfs on GitHub because it will eventually
# overwhelm our storage.

# This script still needs the following:
# A df with all our params for each district 
# A for loop to create each district profile using the df
# An S3 bucket on AWS and code that will export the final pdfs to it


library(here)

# get final_df
source(here("BriefScripts", "ca_legislative_profiles_2025", "get_briefs_data.R"))

# add column to final df to get filename of assembly or senate logo
final_df <- final_df %>%
  mutate(geolevel = case_when(geolevel=="sldl"~"assembly",
                              geolevel=="sldu"~"senate",
                              .default = "error")) %>%
  mutate(png_filename = case_when(geolevel=="assembly"~"assm_logo.png",
                                  geolevel=="senate"~"senate_logo.png",
                                  .default = "error")) 

# Render in the source directory first
original_wd <- getwd()
setwd(here("BriefScripts", "ca_legislative_profiles_2025"))

for(i in 1:nrow(final_df)) {
  print(paste("Starting on PDF", i, " of ", nrow(final_df)))
  geolevel <- str_to_title(final_df[i, "geolevel"])
  rep_name <- final_df[i, "rep_name"]
  party <- final_df[i, "Party"]
  district_number <- final_df[i, "district_number"]
  leg_name <- final_df[i, "leg_name"] ### added
  characteristics <- final_df[i, "Characteristics"] ### added
  composite_disparity_rank <- final_df[i, "composite_disparity_rank"]
  composite_outcome_rank <- final_df[i, "composite_performance_rank"]
  crim_outcome_summary <- final_df[i, "crim_outcome_summary"]
  # demo_outcome_summary <- final_df[i, "demo_outcome_summary"]
  econ_outcome_summary <- final_df[i, "econ_outcome_summary"]
  educ_outcome_summary <- final_df[i, "educ_outcome_summary"]
  hben_outcome_summary <- final_df[i, "hben_outcome_summary"]
  # hlth_outcome_summary <- final_df[i, "hlth_outcome_summary"]
  hous_outcome_summary <- final_df[i, "hous_outcome_summary"]
  crim_disparity_summary <- final_df[i, "crim_disparity_summary"]
  # demo_disparity_summary <- final_df[i, "demo_disparity_summary"]
  econ_disparity_summary <- final_df[i, "econ_disparity_summary"]
  educ_disparity_summary <- final_df[i, "educ_disparity_summary"]
  hben_disparity_summary <- final_df[i, "hben_disparity_summary"]
  # hlth_disparity_summary <- final_df[i, "hlth_disparity_summary"]
  hous_disparity_summary <- final_df[i, "hous_disparity_summary"]
  worst_outcome_1 <- final_df[i, "worst_outcome_1"]
  worst_outcome_2 <- final_df[i, "worst_outcome_2"]
  worst_outcome_3 <- final_df[i, "worst_outcome_3"]
  worst_outcome_4 <- final_df[i, "worst_outcome_4"]
  worst_outcome_5 <- final_df[i, "worst_outcome_5"]
  worst_disparity_1 <- final_df[i, "worst_disparity_1"]
  worst_disparity_2 <- final_df[i, "worst_disparity_2"]
  worst_disparity_3 <- final_df[i, "worst_disparity_3"]
  worst_disparity_4 <- final_df[i, "worst_disparity_4"]
  worst_disparity_5 <- final_df[i, "worst_disparity_5"]
  most_impacted_race_finding <- final_df[i, "most_impacted_race_finding"]
  png_filename <- final_df[i, "png_filename"]
  
  if (geolevel=="Senate") {
    total_districts<-"40"
  } else if (geolevel=="Assembly") {
    total_districts<-"80"
  }
  
  # for file creation and storage
  pdf_name <- paste0(paste(geolevel, "District", district_number, sep = "_"), ".pdf")
  output_directory <- paste0("W:/Project/RACE COUNTS/2025_v7/Leg_Dist_PDFs/Deliverables/", geolevel)
  
  # Create the directory if it doesn't exist
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = FALSE)
  }

  # Render without specifying output_dir
  rmarkdown::render(input = "test_brief.Rmd", 
                    output_dir = output_directory,
                    output_file = pdf_name,
                    params = list(
                      geolevel=geolevel,
                      rep_name=rep_name,
                      leg_name=leg_name,
                      leg_name_caps=toupper(leg_name),
                      party=party,
                      characteristics=paste0(toupper(substr(characteristics, 1, 1)),substr(characteristics, 2, nchar(characteristics))),
                      district_number=str_remove(district_number, "^0+"),
                      disparity_rank=composite_disparity_rank,
                      outcome_rank=composite_outcome_rank,
                      total_districts=total_districts,
                      profiles_url_message=paste0("Compare ", leg_name, " to all districts at www.RaceCounts.org/report/",
                                                  tolower(geolevel), "-profiles or via QR Code:"),
                      crim_outcome_summary=crim_outcome_summary,
                      demo_outcome_summary=demo_outcome_summary,
                      econ_outcome_summary=econ_outcome_summary,
                      educ_outcome_summary=educ_outcome_summary,
                      hben_outcome_summary=hben_outcome_summary,
                      hlth_outcome_summary=hlth_outcome_summary,
                      hous_outcome_summary=hous_outcome_summary,
                      crim_disparity_summary=crim_disparity_summary,
                      demo_disparity_summary=demo_disparity_summary,
                      econ_disparity_summary=econ_disparity_summary,
                      educ_disparity_summary=educ_disparity_summary,
                      hben_disparity_summary=hben_disparity_summary,
                      hlth_disparity_summary=hlth_disparity_summary,
                      hous_disparity_summary=hous_disparity_summary,
                      worst_outcome_1=worst_outcome_1,
                      worst_outcome_2=worst_outcome_2,
                      worst_outcome_3=worst_outcome_3,
                      worst_outcome_4=worst_outcome_4,
                      worst_outcome_5=worst_outcome_5,
                      worst_disparity_1=worst_disparity_1,
                      worst_disparity_2=worst_disparity_2,
                      worst_disparity_3=worst_disparity_3,
                      worst_disparity_4=worst_disparity_4,
                      worst_disparity_5=worst_disparity_5,
                      most_impacted_race_finding=most_impacted_race_finding,
                      logo_filename=png_filename))
  
  print(paste("FINISHED PDF", i, "-", leg_name))
  
}

setwd(original_wd)

# Add export to S3?