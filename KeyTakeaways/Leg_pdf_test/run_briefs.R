# This file runs the test_brief.Rmd 
# File is temporarily created here and then moved to outside of the repo
# We do not want to store 80 pdfs on GitHub because it will eventually
# overwhelm our storage.

# This script still needs the following:
# A df with all our params for each district 
# A for loop to create each district profile using the df
# An S3 bucket on AWS and code that will export the final pdfs to it


library(here)

# Render in the source directory first
original_wd <- getwd()
setwd(here("KeyTakeaways", "Leg_pdf_test"))

# Render without specifying output_dir
rmarkdown::render("test_brief.Rmd", params = list(
  brief_title="Test Brief Title",
  brief_subtitle="Test Subtitle",
  geolevel="Senate",
  rep_name="Firstname Lastname",
  district_name="Senate District XX",
  district_number="XX",
  disparity_rank="3rd"
))

# Then move the PDF to your desired location
file.copy("test_brief.pdf", 
          "W:/Project/RACE COUNTS/2025_v7/Leg_Dist_PDFs/Deliverables/test_brief.pdf",
          overwrite = TRUE)

# Clean up
file.remove("test_brief.pdf")
setwd(original_wd)

# Add export to S3?