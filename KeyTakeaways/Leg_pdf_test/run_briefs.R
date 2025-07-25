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
source(here("KeyTakeaways", "Leg_pdf_test", "get_briefs_data.R"))

# add column to final df to get filename of assembly or senate logo
final_df <- final_df %>%
  mutate(png_filename = case_when(geolevel=="assembly"~"assm_logo.png",
                                  geolevel=="senate"~"senate_logo.png",
                                  .default = "error")) %>%
  
  head(5)

# Render in the source directory first
original_wd <- getwd()
setwd(here("KeyTakeaways", "Leg_pdf_test"))

for(i in 1:nrow(final_df)) {
  print(paste("Starting on PDF", i, " of ", nrow(final_df)))
  geolevel <- str_to_title(final_df[i, "geolevel"])
  rep_name <- final_df[i, "rep_name"]
  district_number <- final_df[i, "district_number"]
  composite_disparity_rank <- final_df[i, "composite_disparity_rank"]
  composite_outcome_rank <- final_df[i, "composite_performance_rank"]
  crim_summary <- final_df[i, "crim_summary"]
  demo_summary <- final_df[i, "demo_summary"]
  econ_summary <- final_df[i, "econ_summary"]
  educ_summary <- final_df[i, "educ_summary"]
  hben_summary <- final_df[i, "hben_summary"]
  hlth_summary <- final_df[i, "hlth_summary"]
  hous_summary <- final_df[i, "hous_summary"]
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
  most_disparate_race <- final_df[i, "most_disparate_race"]
  most_disparate_count <- final_df[i, "most_disparate_count"]
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
                      district_number=district_number,
                      disparity_rank=composite_disparity_rank,
                      outcome_rank=composite_outcome_rank,
                      total_districts=total_districts,
                      crim_summary=crim_summary,
                      demo_summary=demo_summary,
                      econ_summary=econ_summary,
                      educ_summary=educ_summary,
                      hben_summary=hben_summary,
                      hlth_summary=hlth_summary,
                      hous_summary=hous_summary,
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
                      most_disparate_race=most_disparate_race,
                      most_disparate_count=most_disparate_count,
                      logo_filename=png_filename))
  
  print(paste("FINISHED PDF", i))
  
}




setwd(original_wd)

# Add export to S3?