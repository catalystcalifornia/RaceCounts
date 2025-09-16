### install version 1.1.1 of Crosstalk -- LATER VERSIONS OF CROSSTALK WILL NOT WORK ####
# remotes::install_version("crosstalk", version = "1.1.1", repos = "http://cran.us.r-project.org") 
# library(crosstalk)

#Set up workspace
packages <- c("dplyr","crosstalk", "RPostgres", "sf", "htmltools", "tidyr", "reshape2", "plotly", "tidyverse", "highcharter", "rlang", "tidyselect")  

install_packages <- packages[!(packages %in% installed.packages()[,"Package"])] 

if(length(install_packages) > 0) { 
  install.packages(install_packages) 
  
} else { 
  
  print("All required packages are already installed.") 
} 

for(pkg in packages){ 
  library(pkg, character.only = TRUE) 
} 
options(scipen=999)


source("W:\\RDA Team\\R\\credentials_source.R") 
con <- connect_to_db("racecounts")

source("W:\\RDA Team\\R\\credentials_source.R") 
con2 <- connect_to_db("rda_shared_data")

rc_theme <- hc_theme(
    # race counts colors
    colors = c("orange", "purple", "red", "yellow"),
    chart = list(
      backgroundColor = "#FFFFFF",
      borderColor = "black",
      style = list(
        fontFamily = "Rubik") 
    ),
    title = list(
      style = list(
        color = "#070024",
        fontFamily = "Rubik")
    ),
    subtitle = list(
      style = list(
        color = "#8E8C8F", # medium grey
        fontFamily = "Rubik")
    ),
    caption = list(
      style = list(
        color = "#8E8C8F",
        fontFamily = "Rubik")
    ),
    axis = list(
      style = list(
        color = "#8E8C8F",
        fontFamily = "Rubik")
    ),
    legend = list(
      itemStyle = list(
        fontFamily = "Rubik",
        color = "#070024"),
      itemHoverStyle = list(
        fontFamily = "Rubik",
        color = "#070024")
    ))
  
  
  lang <- getOption("highcharter.lang")
  lang$thousandsSep <- ","
  options(highcharter.lang = lang)

  
  # just a note of reference to inserting text #https://monashbioinformaticsplatform.github.io/2017-11-16-open-science-training/topics/rmarkdown.html
  #https://rstudio.com/wp-content/uploads/2015/02/rmarkdown-cheatsheet.pdf


# different caps for the scatterplot, take out the cas then have it be user-defined
#composite inex needs the same naming comvention ex: performance_z as opposed to perf_z
index_scatterplot <- function(x, threshold){
  # Remove geos without quadrant values. Cap index perf/disp z-scores at 2 and -2. More info: https://advancementproject.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=bGyEaZ
  
  # Dynamically find relevant column names
  disp_col <- names(select(x, ends_with("_disparity_z")))
  perf_col <- names(select(x, ends_with("_performance_z")))
  quad_col <- names(select(x, ends_with("_quadrant")))
  rank_disp_col <- names(select(x, ends_with("_disparity_rank")))
  rank_perf_col <- names(select(x, ends_with("_performance_rank")))
  
  # Cap the z-scores
  c_index_chart <- x %>% 
    mutate(
      disp_z_cap = case_when(
        .data[[disp_col]] > threshold ~ threshold,
        .data[[disp_col]] < -threshold ~ -threshold,
        TRUE ~ .data[[disp_col]]
      ),
      perf_z_cap = case_when(
        .data[[perf_col]] > threshold ~ threshold,
        .data[[perf_col]] < -threshold ~ -threshold,
        TRUE ~ .data[[perf_col]]
      )
    ) %>%
    filter(.data[[quad_col]] %in% c("purple", "yellow", "orange", "red"))
  
  # Create the plot
  c_index_chart %>% 
    hchart("scatter", 
           hcaes(
             x = disp_z_cap,
             y = perf_z_cap,
             size = total_pop,
             group = .data[[quad_col]]
           ),
           tooltip = list(
             pointFormat = paste0(
               "<strong>Name: </strong>{point.geo_name} <br>",
               "<strong>Outcome Rank: </strong>{point.", rank_perf_col, "} <br>",
               "<strong>Disparity Rank: </strong>{point.", rank_disp_col, "} <br>",
               "<strong>Universe: </strong>{point.total_pop:,.0f}"
             )
           )) %>%
    hc_title(text = x$arei_issue_area[1]) %>%
    hc_xAxis(title = list(text = "Disparity (Low -> High)"), max = threshold, min = -threshold) %>%
    hc_yAxis(title = list(text = "Outcome (Low -> High)"), max = threshold, min = -threshold) %>%
    hc_caption(
      text = "Data source: Various. <br>A Disparity Rank of 1 represents the most disparate. A Outcome Rank of 1 represents the best outcomes.",
      align = "center"
    ) %>%
    hc_legend(enabled = FALSE) %>%
    hc_add_theme(rc_theme)
}

county_scatterplot<- function(x) {
  # Be sure to update title above depending on if indicator has been updated since RC v3
  # Remove geos without quadrant values. Cap indicator perf/disp z-scores at 3.5 and -3.5. More info: https://advancementproject.sharepoint.com/:w:/s/Portal/EX59kBOn8iRNrLuY1Sfk3JABT34dO3sj1j9fwkuUxLqUgQ?e=bGyEaZ
  # Find all columns ending in "_rate"
  race_rate_cols <- names(select(x, ends_with("_rate")))
  
  # Build dynamic tooltip lines for each race_rate column
  race_tooltip_lines <- paste0(
    "<strong>", 
    gsub("_rate", "", race_rate_cols), 
    " Rate: </strong>{point.", 
    race_rate_cols, 
    ":.1f}"
  )
  
  # Combine all tooltip lines into one string (with <br>)
  race_tooltip_string <- paste(race_tooltip_lines, collapse = " <br>")
  
  # Prepare data
  x_chart <- x %>% 
    mutate(
      disp_z_cap = case_when(
        disparity_z > 3.5 ~ 3.5,
        disparity_z < -3.5 ~ -3.5,
        TRUE ~ disparity_z
      ),
      perf_z_cap = case_when(
        performance_z > 3.5 ~ 3.5,
        performance_z < -3.5 ~ -3.5,
        TRUE ~ performance_z
      )
    ) %>%
    filter(quadrant %in% c("purple", "yellow", "orange", "red"))
  
  # Create plot
  x_chart %>% 
    hchart("scatter", 
           hcaes(x = disp_z_cap, y = perf_z_cap, size = total_pop, group = quadrant),
           tooltip = list(
             pointFormat = paste0(
               "<strong>Name: </strong>{point.geo_name} <br>",
               "<strong>Outcome Rank: </strong>{point.performance_rank} <br>",
               "<strong>Disparity Rank: </strong>{point.disparity_rank} <br>",
               "<strong>Total Population: </strong>{point.total_pop:,.0f} <br>",
               race_tooltip_string
             )
           )
    ) %>%
    hc_title(text = x_chart$bar_chart_header[1]) %>%
    hc_xAxis(title = list(text = "Disparity (Low -> High)"), max = 4, min = -4) %>% 
    hc_yAxis(title = list(text = "Outcome (Low -> High)"), max = 4, min = -4) %>% 
    hc_caption(
      text = paste0(
        "Data source: ", 
        gsub(";", ";<br>", x_chart$data_source[1]), 
        "<br> ", x_chart$description[1], 
        "<br> A Disparity Rank of 1 represents the most disparate. A Outcome Rank of 1 represents the best outcomes."
      ),
      align = "center"
    ) %>% 
    hc_legend(element_blank) %>% 
    hc_add_theme(rc_theme)
}

state_barchart <- function(x) {
  # Be sure to update title above depending on if indicator has been updated since RC v3
  # Be sure to order bars in ascending or descending depending on whether MIN or MAX is best rate
title = list(text=x$bar_chart_header[1])
  x_ <- x %>%
  select(c(state_name, ends_with('_rate'), -total_rate))
x_long <- melt(x_, id.vars=c("state_name"))
x_long <- x_long[order(-x_long$value),]
x_long <- x_long %>% mutate_if(is.double, ~round(., 1))

x_long %>% 
  hchart("column", 
         hcaes(x='variable', y='value'), color = "yellow", borderColor = "black") %>%
  hc_yAxis(title = title,  
           plotLines = list(
             list(
               value = x$total_rate,
               color = "black",
               width = 2,
               zIndex = 20,  # the higher number means will be top/most visible layer
               label = list(text = paste0("Total Rate: ",round(x$total_rate, 1), "%"), style = list(color = "black", fontSize = 16), align = "right")))) %>%
  hc_tooltip(followPointer = TRUE) %>%  # extra line so that tooltip isn't hidden behind total_rate line
  hc_xAxis(title = list(text="Race/Ethnicity"), labels = (list(rotation = -45))) %>%
  hc_size(height=400, width=700) %>% hc_add_theme(rc_theme)
}

county_barchart <- function(x) {
  x_long <- x %>%
    select(c(geo_name, ends_with('_rate')))
  
  x_long <- melt(x_long, id.vars=c("geo_name"))
  x_long <- x_long[order(x_long$value),]
  # Round 'value' field to 1 decimal
  x_long <- x_long %>% mutate(value = round(value, 1))
  
  
  # drop down selection menu for counties: check out this helpful resource https://rstudio.github.io/crosstalk/using.html
  
  # create a shared data object from the df and assign a key for the unique observation. County/Assm/Sen is fine since each race/group has one unique geo. from now on, we will be using the shareddata object and not the df_long
  
  sd2 <- SharedData$new(x_long, key = ~ geo_name)
  title = list(text=x$bar_chart_header[1])
  
  #
  bscols(
    widths = 7, 
    
    filter_select("geo_name",  ## this is the name of column we want to select. 
                  "Name:", # this is what we want to see in the selection menu
                  sd2,
                  ~ geo_name, multiple = FALSE),
    plot_ly(sd2) %>%
      add_trace(x = ~ variable, y = ~ value, type = "bar", color = I("yellow"), 
                marker = list(line = list(color = "black", width = 1)) ## add order t
      ) %>%
      layout(barmode = "stack",
             xaxis = list(title = 'Race/Ethnicity',
                          tickangle=-45,
                          categoryorder = "total ascending"),   # Change to "total ascending" when MIN is best or "total descending" when MAX is best
             
             yaxis = list(title = title,
                          titlefont = list(size = 11)
             ),
             
             
             width = 600,
             paper_bgcolor = "#FFFFFF",
             plot_bgcolor = "#FFFFFF",
             
             font = list(
               family = "Rubik",
               size = 12,
               color = '#8E8C8F'
             ),
             hoverlabel = list(bgcolor = "FFFFFF")
      ) # end layout
  ) # end bscols

  
}