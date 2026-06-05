# Load packages
library(rmarkdown)
library(here)

## Uncomment out Asian OR PacIsl charts, then run loop.

###### ASIAN CHART ######
# grp <- "asian"



###### NHPI CHART ######
# grp <- "nhpi"



###### CHART LOOP ######
render(
    input = "./MOSAIC/charts/county_tall_bar.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(grp, "_overcrowded_bar_lac.html")
)
