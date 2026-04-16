### These are fx used to create MOSAIC bar charts ###

### Function 1: generate custom metadata ###
metadata_fx <- function(issue, ind, race, meta, tot_yr, geo = "state") {
  # issue: demo, econ, hlth, hben, hous, crim
  # ind: metadata$api_name
  # race: asian, nhpi (mosaic only covers subgroups w/i these 2 races)
  # meta: df with metadata
  # geo: geolevel: state, county, etc.
  
  # Filter metadata to selected indicator before pulling in metadata details
  meta_ <- metadata %>%
    filter(api_name == ind)      # filter for selected indicator
  
  # Set table name
  table_name <- sprintf("%s_%s_%s_%s_%s", race, issue, ind, geo, curr_yr)
  tot_table_name <- sprintf("arei_%s_%s_%s_%s", issue, ind, geo, tot_yr)
  
  # Title of chart here: bar_chart_header from metadata df is default, update as needed
  title_text <- meta_ %>%
    select(api_name, bar_chart_header) %>%  
    pull(bar_chart_header) %>%
    as.character()                # convert to char vector
  
  #Subtitle text if necessary: methodology in metadata df is default, update as needed
  subtitle_text <- meta_ %>%
    select(api_name, methodology) %>%  
    pull(methodology) %>%
    as.character()                # convert to char vector
  
  # Set your data source here: data_source in metadata df is being used as dummy data, WILL NEED TO BE REPLACED WITH MOSAIC INDICATOR METADATA TABLE
  datasource <- meta_ %>%
    select(api_name, data_source) %>%  
    pull(data_source) %>%
    as.character()                # convert to char vector
  
  # Set asbest: arei_best in metadata df, WILL NEED TO BE REPLACED WITH MOSAIC INDICATOR METADATA TABLE
  asbest <- meta_ %>%
    select(api_name, arei_best) %>%  
    pull(arei_best) %>%
    as.character()                # convert to char vector
  
  # put new metadata variables together
  meta2 <- tibble(table_name, tot_table_name, title_text, subtitle_text, datasource, asbest)
  
  # append new metadata variables to existing metadata
  meta_ <- cbind(meta_, meta2)
  
  return(meta_)
}


### Function 2: generate custom indicator dataframe ###
data_fx <- function(meta, race, tot_schema) {
  # meta: indicator+geolevel metadata df with 1 row
  # race: asian, nhpi (mosaic only covers subgroups w/i these 2 races)
  
  tot_yr <- case_when(
              tot_schema == 'v5' ~ '2023',
              tot_schema == 'v6' ~ '2024',
              tot_schema == 'v7' ~ '2025',
              TRUE ~ NA)
  
  # get total, asian, nhpi rates -- dummy data is v7 which is not an exact data match
  tot_df <- dbGetQuery(con2, paste0("SELECT * FROM ", tot_schema, ".", meta$tot_table_name)) %>% 
    select(total_rate, ends_with("asian_rate"), ends_with("pacisl_rate")) %>%
    rename_with(~ "asian_rate", matches("^.*asian.*$")[1]) %>%    # rename asian rate col to generic
    rename_with(~ "pacisl_rate", matches("^.*pacisl.*$")[1])      # rename nhpi rate col to generic
  
  df_wide <- dbGetQuery(con, paste0("SELECT * FROM ", rc_schema, ".", meta$table_name)) %>%
    select(-starts_with("na_"), -starts_with("unknown_")) %>%
    { # Keep only "aoic" cols for ACS indicators. Keep only geoname, raw, rate cols for all indicators.
      if (grepl("American Community Survey", meta$datasource[1]) & !grepl("PUMS", meta$datasource[1])) {
        select(., ends_with("_name"), (ends_with("_raw") & contains("aoic")), (ends_with("_rate") & contains("aoic"))) %>%
          rename_with(~ "geoname", matches("^.*name.*$")[1])
      } else {
        select(., ends_with("_name"), ends_with("_raw"), ends_with("_rate"), -contains("total")) %>%
          rename_with(~ "geoname", matches("^.*name.*$")[1])
      }
    }
  
  # convert data to long form
  df <- df_wide %>%
    pivot_longer(
      cols = -geoname,
      names_to = c("subgroup", ".value"),
      names_pattern = "^(.+)_(raw|rate)$"
    ) %>%
    # make clean subgroup labels
    mutate(subgroup_label = gsub("_aoic", "", subgroup)) %>%
    mutate(subgroup_label = str_to_title(gsub("_", " ", subgroup_label))) %>%
    filter(!is.na(rate) & !is.na(raw))
  
  data_list <- list(df = df, tot_df = tot_df
  )
  return(data_list)
}


### Function 3: generate custom indicator horizontal barchart with subgroup data, total line, and Asian or NHPI line ###
chart_fx <- function(data_list, meta, race, racenote) {
  # data_list: list containing indicator data in element called "df" and total/asian/nhpi data in element called "tot_df"
  # meta: indicator+geolevel metadata df with 1 row
  # race: asian, nhpi (mosaic only covers subgroups w/i these 2 races)
  # racenote: notes on definition of subgroups
  
  # select Asian or NHPI race rate as comparison
  race_ <- ifelse(race == 'nhpi', 'pacisl', 'asian')
  
  race_rate <- data_list$tot_df %>%
    select(contains(tolower(race_))) %>%
    as.numeric()
  
  # generate race rate label
  race_label <- case_when(
    race %in% c('asian','Asian') ~ "Asian",
    TRUE ~ "Native Hawaiian or Pacific Islander"
  )
  
  # Calculate height based on number of rows (e.g., 40px per bar + 150px for headers/labels)
  dynamic_height <- (nrow(data_list$df) * 40) + 150
  # Set constraints so it doesn't get too small or too huge
  chart_height <- pmax(350, pmin(dynamic_height, 800))
  
  # build chart
  b_chart <- hchart(
    data_list$df %>% arrange(if (meta$asbest == 'max') rate else -rate),   # sort asc/desc depending on asbest value
    type = "bar", 
    hcaes(y = round(rate, 1), x = subgroup_label), 
    color = mainblue,
    tooltip = list(   # Add % when max value <= 100 (aka values are percents)
      pointFormat = paste0(
        "Rate: {point.rate:.1f}", if (max(data_list$df$rate, na.rm = TRUE) > 100) "" else "%",
        "<br>", str_to_sentence(meta$raw_descriptor), ": {point.raw:,.0f}"
      )
    ),
  ) %>% 
    
    hc_tooltip(crosshairs = TRUE) %>% 
    
    hc_xAxis(title = list(text = ""),
             labels=list(style=list(fontSize='15px'))
    ) %>% 
    
    hc_yAxis(title = list(text = ""),
             labels = list(  # # Add % when max value <= 100 (aka values are percents)
               format = if (max(race_rate, na.rm = TRUE) > 100) "{value}" else "{value}%"
             ), list(step = 1), padding = 0,    # this is for rate indicators only adding % signs
             max = min(1000, max(data_list$df$rate, na.rm = TRUE) * 1.1),        # data max + 10% buffer, capped at 100
             plotLines = list(
               list(
                 value = data_list$tot_df$total_rate,   # total value
                 color = "red",
                 width = 2,
                 dashStyle = "Solid",     # "Solid", "Dash", "Dot", "DashDot"
                 zIndex = 5,              # draw on top of bars
                 label = list(
                   text = paste0(
                     "Total Rate: ",  # Add % when max value <= 100 (aka values are percents)
                     round(data_list$tot_df$total_rate, 1),
                     if (max(data_list$tot_df$total_rate, na.rm = TRUE) > 100) "" else "%"
                   ),
                   align = "right",       # place label to the left of the line
                   verticalAlign = "bottom",
                   rotation = 0,          # make labels horizontal
                   y = -15,
                   style = list(color = "red", fontWeight = "bold")
                 )
               ),
               list(
                 value = race_rate,   # race group value
                 color = "black",
                 width = 2,
                 dashStyle = "Solid",     # "Solid", "Dash", "Dot", "DashDot"
                 zIndex = 5,              # draw on top of bars
                 label = list(
                   text = paste0(
                     race_label, " Rate: ",   # Add % when max value <= 100 (aka values are percents)
                     round(race_rate, 1),
                     if (max(race_rate, na.rm = TRUE) > 100) "" else "%"
                   ),
                   align = "right",       # place label to the left of the line
                   verticalAlign = "bottom",
                   rotation = 0,          # make labels horizontal
                   y = 5,
                   style = list(color = "black", fontWeight = "bold")
                 )
               )  
             )
    ) %>%
    
    hc_plotOptions(
      bar = list(
        groupPadding = 0.05,   # space between groups (default 0.2)
        pointPadding = 0.01    # width of bars
      )
    ) %>%
    
    # title elements
    hc_title(
      text = paste0(meta$title_text),
      #margin = 20,
      align = "left", 
      style = list(useHTML = TRUE, fontWeight = "bold", fontSize='30px')
    ) %>% 
    
    hc_subtitle(
      text = paste0(meta$subtitle_text),
      align = "left", y = 50
    ) %>% 
    
    hc_caption(
      text = paste0("<br>Data Source: ", meta$datasource,
                    "<br>",racenote),
      style=list(fontSize='15px')
      #align = "left"
    ) %>% 
    
    hc_size(height = chart_height) %>%
    
    hc_add_theme(tnp_theme) #%>%
    
    # hc_exporting(
    #   enabled = TRUE, sourceWidth=1000, sourceHeight=chart_height, 
    #   chartOptions=list(plotOptions=list(series=list(dataLabels=list(enabled=TRUE, format='{y} %')))),
    #   filename = "plot"
    # )
  
  return(b_chart)
}
