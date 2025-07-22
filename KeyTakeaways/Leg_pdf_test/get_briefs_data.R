source("W:\\RDA Team\\R\\credentials_source.R")

conn <- connect_to_db("racecounts")
schema <- "v6" # substitute until v7 done
yr <- "2024" # substitute until v7 done
geolevel <- "county" #substitute until leg data collection complete

indicators <- dbGetQuery(conn, statement=paste0("SELECT arei_indicator, arei_issue_area, arei_issue_area_id, arei_best, api_name FROM ", schema,".","arei_indicator_list_cntyst;"))
issues <- dbGetQuery(conn, statement=paste0("SELECT arei_issue_area_id, api_name_short, api_name_v2 FROM ", schema,".","arei_issue_list;")) %>%
  mutate(index_table_name = paste0(schema, ".",paste("arei", api_name_short, "index", yr,sep="_")))

combined <- indicators %>%
  left_join(issues, by = "arei_issue_area_id") %>%
  mutate(table_name = paste0(schema,".", paste("arei", api_name_short, api_name, geolevel, yr, sep="_")))


# get each issue area index from pg
issue_indexes <- list()
for(i in 1:nrow(issues)) {
  issue_name <- issues[i, "api_name_short"]
  api_name_v2 <- issues[i, "api_name_v2"]
  index_table_name <- issues [i, "index_table_name"]
  sql_query <- paste0("SELECT * FROM ", index_table_name, " LIMIT 40;")
  x <- dbGetQuery(conn, sql_query) 
  x_transform <- x %>%
    select(county_id, county_name, ends_with("_rank"), ends_with("_quartile"), ends_with("_perf_z"), ends_with("_disp_z")) %>%
    pivot_longer(
      cols = ends_with(c("_perf_z", "_disp_z")),
      names_to = c("indicator", "metric"),
      names_pattern = "(.*)_(perf_z|disp_z)",
      values_to = "value"
    ) %>%
    group_by(county_id, metric) %>%
    mutate(rank = min_rank(value)) %>%  # Handle ties by giving them the same rank
    filter(rank <= 5) %>%
    slice_head(n = 5) %>%  # Ensure exactly 5 rows even with ties
    mutate(rank = row_number()) %>%  # Re-rank 1-5
    ungroup() %>%
    select(county_id, metric, rank, indicator) %>%
    pivot_wider(
      names_from = c(metric, rank),
      values_from = indicator,
      names_prefix = "lowest_",
      names_sep = "_ind"
    )
  
  df <- x %>%
    select(county_id, county_name, ends_with("_rank"), ends_with("_quartile")) %>%
    left_join(x_transform, by="county_id") %>%
    mutate(issue_area=issue_name)
  
  colnames(df) <- gsub(paste0(api_name_v2,"_"), "", colnames(df))
    
  issue_indexes[[issue_name]] <- df

}

list2env(issue_indexes, .GlobalEnv)



dbDisconnect(conn)