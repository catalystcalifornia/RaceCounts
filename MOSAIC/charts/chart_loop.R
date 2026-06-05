# Load packages
library(rmarkdown)
library(here)
library(stringr)

## Uncomment out Asian OR PacIsl 'grp' and either 'bar' or 'drilld' indicators, then run loop.

###### ASIAN CHARTS ######
# grp <- "asian"
# bar_indicators <- c("overcrowded", "officials")
# drilld_indicators <- c("voter_engagement", "health_insurance")
# pop_table <- 'aa_pop_b02018'

# Asian POP BAR CHART ######
# render(
#   input = "./MOSAIC/charts/pop_tall_bar.Rmd",
#   output_dir = paste0(getwd(),"/MOSAIC/charts"),
#   output_file = paste0(grp, "_pop_bar.html"))

###### NHPI CHARTS ######
# grp <- "nhpi"
# bar_indicators <- c("health_insurance", "living_wage")
# drilld_indicators <- c("connected_youth", "overcrowded")
# pop_table <- 'nhpi_pop_b02019'


# PacIsl POP BAR CHART ######
# render(
#   input = "./MOSAIC/charts/pop_tall_bar.Rmd",
#   output_dir = paste0(getwd(),"/MOSAIC/charts"),
#   output_file = paste0(grp, "_pop_bar.html"))



###### TALL BAR CHART LOOP ######
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