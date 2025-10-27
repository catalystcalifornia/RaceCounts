# This file runs the leg_brief.Rmd for 120 legislative districts and
# exports to an AWS S3 bucket for storage and hosting. S3 links will be 
# used on racecounts.org

library(here)

# get final_df
source(here("BriefScripts", "ca_legislative_profiles_2025", "get_briefs_data.R"))

# deliverable output folder
output_folder <- "W:/Project/RACE COUNTS/2025_v7/Leg_Dist_PDFs/Deliverables/"

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
  composite_outcome_rank <- final_df[i, "composite_outcome_rank"]
  crim_outcome_summary <- final_df[i, "crim_outcome_summary"]
  demo_outcome_summary <- final_df[i, "demo_outcome_summary"]
  econ_outcome_summary <- final_df[i, "econ_outcome_summary"]
  educ_outcome_summary <- final_df[i, "educ_outcome_summary"]
  hben_outcome_summary <- final_df[i, "hben_outcome_summary"]
  hlth_outcome_summary <- final_df[i, "hlth_outcome_summary"]
  hous_outcome_summary <- final_df[i, "hous_outcome_summary"]
  crim_disparity_summary <- final_df[i, "crim_disparity_summary"]
  demo_disparity_summary <- final_df[i, "demo_disparity_summary"]
  econ_disparity_summary <- final_df[i, "econ_disparity_summary"]
  educ_disparity_summary <- final_df[i, "educ_disparity_summary"]
  hben_disparity_summary <- final_df[i, "hben_disparity_summary"]
  hlth_disparity_summary <- final_df[i, "hlth_disparity_summary"]
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
  output_directory <- paste0(output_folder, geolevel)
  
  # Create the directory if it doesn't exist
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = FALSE)
  }

  # Render without specifying output_dir
  rmarkdown::render(input = "leg_brief.Rmd", 
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
                      profiles_url_message=paste0("Compare ", leg_name, " to all districts here: \\href{https://bit.ly/StateLegislatureData}{\\textcolor{blue}{https://bit.ly/StateLegislatureData}} or via QR Code"),
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

library(aws.s3)

# Load AWS credentials
access <- read.csv("D:\\Users\\hkhan\\credentials\\rc-legislative-profiles-uploader_accessKeys.csv")
access_id <- access$ï..Access.key.ID
access_key <- access$Secret.access.key

Sys.setenv(
  "AWS_ACCESS_KEY_ID" = access_id,
  "AWS_SECRET_ACCESS_KEY" = access_key,
  "AWS_DEFAULT_REGION" = "us-west-2"
)

# AWS S3 Configuration
S3_BUCKET_NAME <- "rc-ca-legislative-profiles"
S3_FOLDER_PREFIX <- "2025/"

# Function to upload a file to S3 with proper headers for in-browser viewing
upload_to_s3 <- function(local_file_path, s3_key, bucket = S3_BUCKET_NAME) {
  tryCatch({
    put_object(
      file = local_file_path,
      object = s3_key,
      bucket = bucket,
      headers = list(
        "Content-Type" = "application/pdf",
        "Content-Disposition" = "inline"
      )
    )
    cat("Successfully uploaded:", s3_key, "\n")
    public_url <- paste0("https://", bucket, ".s3.amazonaws.com/", s3_key)
    return(public_url)
  }, error = function(e) {
    cat("Error uploading", s3_key, ":", conditionMessage(e), "\n")
    return(NULL)
  })
}

##### S3 Upload #####
# =============================================================================
# STEP 1: Upload all PDFs to S3
# =============================================================================
cat("\n=== Starting S3 Upload Process ===\n")

uploaded_files <- list()

for(i in 1:nrow(final_df)) {
  geolevel <- str_to_title(final_df[i, "geolevel"])
  district_number <- final_df[i, "district_number"]
  
  pdf_name <- paste0(paste(geolevel, "District", district_number, sep = "_"), ".pdf")
  output_directory <- paste0("W:/Project/RACE COUNTS/2025_v7/Leg_Dist_PDFs/Deliverables/", geolevel)
  local_path <- file.path(output_directory, pdf_name)
  
  s3_key <- paste0(S3_FOLDER_PREFIX, geolevel, "/", pdf_name)
  
  if (file.exists(local_path)) {
    public_url <- upload_to_s3(local_path, s3_key)
    if (!is.null(public_url)) {
      uploaded_files[[i]] <- list(
        district = paste(geolevel, district_number),
        url = public_url
      )
    }
  } else {
    cat("Warning: File not found -", local_path, "\n")
  }
}

# Save upload summary
upload_summary <- do.call(rbind, lapply(uploaded_files, function(x) {
  data.frame(district = x$district, url = x$url, stringsAsFactors = FALSE)
}))

write.csv(upload_summary, 
          "W:/Project/RACE COUNTS/2025_v7/Leg_Dist_PDFs/s3_upload_summary.csv",
          row.names = FALSE)

cat("\n=== Upload Complete ===\n")
cat("Total files uploaded:", length(uploaded_files), "\n")

# =============================================================================
# STEP 2: Create Assembly District Index (Minimal)
# =============================================================================
cat("\n=== Creating Assembly Index ===\n")

assembly_files <- get_bucket_df(S3_BUCKET_NAME, prefix = "2025/Assembly/")
assembly_files <- assembly_files[grepl("\\.pdf$", assembly_files$Key), ]
assembly_files$filename <- basename(assembly_files$Key)
assembly_files <- assembly_files[order(assembly_files$filename), ]

html <- '<!DOCTYPE html>
<html>
<head>
    <title>Assembly Districts - 2025</title>
</head>
<body>
    <h1>California Assembly Districts - 2025</h1>
    <p><a href="../index.html">← Back to Main Index</a></p>
    <ul>'

for (i in 1:nrow(assembly_files)) {
  filename <- assembly_files$filename[i]
  display_name <- gsub("_", " ", gsub("\\.pdf$", "", filename))
  html <- paste0(html, sprintf('\n        <li><a href="%s">%s</a></li>', filename, display_name))
}

html <- paste0(html, '
    </ul>
</body>
</html>')

temp_file <- tempfile(fileext = ".html")
writeLines(html, temp_file)

put_object(
  file = temp_file,
  object = "2025/Assembly/index.html",
  bucket = S3_BUCKET_NAME,
  headers = list(
    "Content-Type" = "text/html",
    "Content-Disposition" = "inline"
  )
)

unlink(temp_file)
cat("Assembly index created!\n")

# =============================================================================
# STEP 3: Create Senate District Index (Minimal)
# =============================================================================
cat("\n=== Creating Senate Index ===\n")

senate_files <- get_bucket_df(S3_BUCKET_NAME, prefix = "2025/Senate/")
senate_files <- senate_files[grepl("\\.pdf$", senate_files$Key), ]
senate_files$filename <- basename(senate_files$Key)
senate_files <- senate_files[order(senate_files$filename), ]

html <- '<!DOCTYPE html>
<html>
<head>
    <title>Senate Districts - 2025</title>
</head>
<body>
    <h1>California Senate Districts - 2025</h1>
    <p><a href="../index.html">← Back to Main Index</a></p>
    <ul>'

for (i in 1:nrow(senate_files)) {
  filename <- senate_files$filename[i]
  display_name <- gsub("_", " ", gsub("\\.pdf$", "", filename))
  html <- paste0(html, sprintf('\n        <li><a href="%s">%s</a></li>', filename, display_name))
}

html <- paste0(html, '
    </ul>
</body>
</html>')

temp_file <- tempfile(fileext = ".html")
writeLines(html, temp_file)

put_object(
  file = temp_file,
  object = "2025/Senate/index.html",
  bucket = S3_BUCKET_NAME,
  headers = list(
    "Content-Type" = "text/html",
    "Content-Disposition" = "inline"
  )
)

unlink(temp_file)
cat("Senate index created!\n")

# =============================================================================
# STEP 4: Create Main Landing Page (Minimal)
# =============================================================================
cat("\n=== Creating Main Landing Page ===\n")

main_html <- '<!DOCTYPE html>
<html>
<head>
    <title>California Legislative Profiles - 2025</title>
</head>
<body>
    <h1>California Legislative Profiles - 2025</h1>
    <ul>
        <li><a href="Senate/index.html">Senate Districts (40 districts)</a></li>
        <li><a href="Assembly/index.html">Assembly Districts (80 districts)</a></li>
    </ul>
</body>
</html>'

temp_file <- tempfile(fileext = ".html")
writeLines(main_html, temp_file)

put_object(
  file = temp_file,
  object = "2025/index.html",
  bucket = S3_BUCKET_NAME,
  headers = list(
    "Content-Type" = "text/html",
    "Content-Disposition" = "inline"
  )
)

unlink(temp_file)
cat("\n=== ALL INDEX PAGES CREATED ===\n")
cat("Main landing page: https://", S3_BUCKET_NAME, ".s3-us-west-2.amazonaws.com/2025/index.html\n", sep="")