library(rmarkdown)

## Uncomment out Asian OR PacIsl charts, then run loop.

###### ASIAN CHARTS ######
# indicator_list <- c("voter_engagement", "overcrowded", "health_insurance", "officials")
# grp <- "asian"



###### NHPI CHARTS ######
# indicator_list <- c("connected_youth", "overcrowded", "health_insurance", "living_wage")
# grp <- "nhpi"



###### CHART LOOP ######
for (r in indicator_list) {
  render(
    input = "./MOSAIC/charts/templates/tall_bar.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(grp, "_", r, "_bar.html"),
    params = list(indicator_list = r)
  )
}