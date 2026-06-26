# Load packages
library(rmarkdown)
library(here)
library(stringr)

## Uncomment out Asian OR PacIsl 'grp' and either 'bar' or 'drilld' indicators, then run loop.

###### ASIAN CHARTS ######
race_grp <- "asian"
bar_indicators <- c("overcrowded", "officials")
drilld_indicators <- c("voter_engagement", "health_insurance")
pop_table <- 'aa_pop_b02018'

### Asian POP BAR CHART ###
render(
  input = "./MOSAIC/charts/pop_tall_bar.Rmd",
  output_dir = paste0(getwd(),"/MOSAIC/charts"),
  output_file = paste0(race_grp, "_pop_bar.html"))

### ASIAN DRILLDOWN IND CHART LOOP ###
for (ind_ in drilld_indicators) {
  ind <- ind_
  geolevel_table_suffix <-""
  render(
    input = "./MOSAIC/charts/templates/drilldown.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(race_grp, "_", ind, "_drilldown.html")
  )
}

### ASIAN TALL BAR IND CHART LOOP ###
for (ind_ in bar_indicators) {
  
  ind <- ind_
  geolevel <- "STATE"
  geolevel_table_suffix <- ""
  
  render(
    input = "./MOSAIC/charts/templates/tall_bar.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(race_grp, "_", ind_, "_bar.html")
  )
}

### ASIAN LAC IND CHART ###

ind <- "overcrowded"
geolevel <- "COUNTY"
geolevel_table_suffix <- "_lac"

render(
  input = "./MOSAIC/charts/templates/tall_bar.Rmd",
  output_dir = paste0(getwd(),"/MOSAIC/charts"),
  output_file = paste0(race_grp, "_overcrowded_bar_lac.html")
)


###### NHPI CHARTS ######
race_grp <- "nhpi"
bar_indicators <- c("connected_youth", "overcrowded")
drilld_indicators <- c("health_insurance", "living_wage")
pop_table <- 'nhpi_pop_b02019'

### NHPI POP BAR CHART ###
render(
  input = "./MOSAIC/charts/pop_tall_bar.Rmd",
  output_dir = paste0(getwd(),"/MOSAIC/charts"),
  output_file = paste0(race_grp, "_pop_bar.html"))

### NHPI DRILLDOWN IND CHART LOOP ###
for (ind_ in drilld_indicators) {
  ind <- ind_
  geolevel_table_suffix <- ""
  render(
    input = "./MOSAIC/charts/templates/drilldown.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(race_grp, "_", ind, "_drilldown.html")
  )
}

### NHPI TALL BAR IND CHART LOOP ###
for (ind_ in bar_indicators) {
  
  ind <- ind_
  geolevel <- "STATE"
  geolevel_table_suffix <- ""
  
  render(
    input = "./MOSAIC/charts/templates/tall_bar.Rmd",
    output_dir = paste0(getwd(),"/MOSAIC/charts"),
    output_file = paste0(race_grp, "_", ind_, "_bar.html")
  )
}

### NHPI LAC IND CHART ###

ind <- "overcrowded"
geolevel <- "COUNTY"
geolevel_table_suffix <- "_lac"

render(
  input = "./MOSAIC/charts/templates/tall_bar.Rmd",
  output_dir = paste0(getwd(),"/MOSAIC/charts"),
  output_file = paste0(race_grp, "_overcrowded_bar_lac.html")
)

