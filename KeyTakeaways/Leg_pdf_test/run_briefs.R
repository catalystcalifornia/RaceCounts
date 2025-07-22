library(here)

# Render in the source directory first
original_wd <- getwd()
setwd(here("KeyTakeaways", "Leg_pdf_test"))

# Render without specifying output_dir
rmarkdown::render("test_brief.Rmd", params = list(
  brief_title = "Test Brief Title",
  brief_subtitle = "Test Subtitle"
))

# Then move the PDF to your desired location
file.copy("test_brief.pdf", 
          "W:/Project/RACE COUNTS/2025_v7/Leg_Dist_PDFs/Deliverables/test_brief.pdf",
          overwrite = TRUE)

# Clean up
file.remove("test_brief.pdf")
setwd(original_wd)