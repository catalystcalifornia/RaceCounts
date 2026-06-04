# Load packages
library(rmarkdown)
library(here)

## Uncomment out Asian OR PacIsl 'grp' and either 'bar' or 'drilld' indicators, then run loop.

###### ASIAN CHARTS ######
# grp <- "asian"
# bar_indicators <- c("overcrowded", "officials")
# drilld_indicators <- c("voter_engagement", "health_insurance")


###### NHPI CHARTS ######
# grp <- "nhpi"
# bar_indicators <- c("health_insurance", "living_wage")
# drilld_indicators <- c("connected_youth", "overcrowded")


###### DRILLDOWN CHART LOOP ######
for (r in drilld_indicators) {
  render(
    input = "./MOSAIC/charts/templates/drilldown.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(grp, "_", r, "_drilldown.html"),
    params = list(drilld_indicators = r)
  )
}


###### TALL BAR CHART LOOP ######
for (r in bar_indicators) {
  render(
    input = "./MOSAIC/charts/templates/tall_bar.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(grp, "_", r, "_bar.html"),
    params = list(bar_indicators = r)
  )
}