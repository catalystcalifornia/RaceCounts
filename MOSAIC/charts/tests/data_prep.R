# load libraries and test data
library(here)
library(dplyr)
library(purrr)
library(htmltools)
library(highcharter)

# get data
source("W:\\RDA Team\\R\\credentials_source.R")
source(here("MOSAIC", "charts", "chart_style_vars.R"))

con <- connect_to_db("mosaic")
con_rc <- connect_to_db("racecounts")

low_birthweight_asian<- dbGetQuery(con, "SELECT * FROM v7.asian_hlth_low_birthweight_state_2025;") %>%
  select(-starts_with("total_")) %>%
  select(ends_with("_raw"), ends_with("_rate"))
# low_birthweight_nhpi <- dbGetQuery(con, "SELECT * FROM v7.nhpi_hlth_low_birthweight_state_2025;") %>%
#   select(ends_with("_raw"), ends_with("_rate"))
low_birthweight_rc <- dbGetQuery(con_rc, "SELECT * FROM v7.arei_hlth_low_birthweight_state_2025;") 

low_birthweight_asbest <- low_birthweight_rc %>% pull(asbest)

low_birthweight_rc <- low_birthweight_rc %>%
  select(ends_with("_raw"), ends_with("_rate"))

dbDisconnect(con)
dbDisconnect(con_rc)

# get data in right shape
# should have at minimum: group (asian, nhpi, total); subgroup (bangladesh, philippines, etc); count, rate

# colnames(low_birthweight_asian)

low_bw_asian_reshape <- low_birthweight_asian %>%
  pivot_longer(
    cols = matches("_raw$|_rate$"),
    names_to = c("subgroup", ".value"),
    names_pattern = "^(.+)_(raw|rate)$") %>%
  rename(count = raw) %>%
  mutate(group = "asian") %>%
  select(group, subgroup, count, rate) 

# low_bw_nhpi_reshape <- low_birthweight_nhpi %>%
#    pivot_longer(
#     cols = matches("_raw$|_rate$"),
#     names_to = c("subgroup", ".value"),
#     names_pattern = "^(.+)_(raw|rate)$") %>%
#   rename(count = raw) %>%
#   mutate(group = "nhpi") %>%
#   select(group, subgroup, count, rate)

low_bw_rc_reshape <- low_birthweight_rc %>%
  pivot_longer(
    cols = matches("_raw$|_rate$"),
    names_to = c("subgroup", ".value"),
    names_pattern = "^(.+)_(raw|rate)$") %>%
  rename(count = raw) %>%
  mutate(group = "total") %>%
  select(group, subgroup, count, rate)  

low_birthweight <- rbind(low_bw_asian_reshape, low_bw_rc_reshape) 

low_birthweight <- low_birthweight %>%
  mutate(
    # round rates
    rate=round(rate, 1)) %>%
  # remove nphi for now
  filter(group!="nhpi") %>%
  # mutate(subgroup = ifelse(group=="nhpi", sprintf("%s (nhpi)", subgroup), subgroup)) %>%
  # mutate(group= ifelse(group=="nhpi", "ASIAN", group)) 
  mutate(
    group = case_when(
      group=="total" & subgroup=="nh_asian" ~ "asian",
      group=="total" & subgroup != "total" ~ "other major race groups",
      group=="asian" & subgroup != "total" ~ "asian subgroups",
      .default=group),
    subgroup = case_when(group=="asian" ~ "asian total",
                         .default=subgroup))


# A bit more indicator prep for chart mock ups
# Test highcharter

low_birthweight_chart1 <- low_birthweight %>%
  mutate(subgroup = ifelse(subgroup == "asian total", "total", subgroup)) %>%
  # Will want to include asian in total and as its own bar
  bind_rows(low_birthweight %>% filter(group=="asian") %>% mutate(group="other major race groups", subgroup="nh_asian")) 

low_birthweight_chart2 <- low_birthweight %>%
  filter(group!="total" & subgroup!="total") %>%
  mutate(group = ifelse(group=="asian", "asian total", group))


# use indicator best direction to arrange "shared struggle" groups at top
if(low_birthweight_asbest=="min") {
  low_birthweight_chart2 <- low_birthweight_chart2 %>%
    arrange(desc(rate))
  low_birthweight_chart1 <- low_birthweight_chart1 %>%
    arrange(desc(rate))
  
} else{
  low_birthweight_chart2 <- low_birthweight_chart2 %>%
    arrange(rate)
  low_birthweight_chart1 <- low_birthweight_chart1 %>%
    arrange(desc(rate))
}

# make groups all caps per RC style
low_birthweight_chart1$group <- toupper(low_birthweight_chart1$group)
low_birthweight_chart1$subgroup <- toupper(low_birthweight_chart1$subgroup)

low_birthweight_chart2$group <- toupper(low_birthweight_chart2$group)
low_birthweight_chart2$subgroup <- toupper(low_birthweight_chart2$subgroup)

# define df initial chart of drilldown (asian and total bars only)
base_bw_df <- low_birthweight_chart1 %>%
  filter(subgroup=="TOTAL") %>%
  select(-subgroup) %>%
  mutate(
    color = case_when(
      group == "ASIAN" ~ base_colors[["asian"]],
      group == "TOTAL" ~ base_colors[["total"]],
      TRUE ~ base_colors[["default"]]))

# define df for drilldown charts (asian subgroups and total subgroups)
drilldown_bw_df <- low_birthweight_chart1 %>%
  filter(subgroup!="TOTAL") %>%
  group_nest(group) %>%
  mutate(
    id = case_when(
      group == "ASIAN SUBGROUPS" ~ "ASIAN",
      group == "OTHER MAJOR RACE GROUPS" ~ "TOTAL",
      .default = group
    ),
    type = "bar",
    color = case_when(
      id == "ASIAN" ~ base_colors[["asian"]],
      id == "TOTAL" ~ base_colors[["total"]],
      .default = base_colors[["default"]]),
    # Store both rate-sorted and count-sorted versions
    data = map(data, function(df) {
      df %>%
        arrange(desc(rate)) %>%
        mutate(name = subgroup, y = rate, y2 = count) %>%
        select(name, y, y2) %>%
        list_parse()}),
    data_count_sorted = map2(data, group, function(df, grp) {
      low_birthweight_chart1 %>%
        filter(group == grp, subgroup != "TOTAL") %>%
        arrange(desc(count)) %>%
        mutate(name = subgroup, y = count, y2 = rate) %>%
        select(name, y, y2) %>%
        list_parse()})) %>%
  select(id, type, color, data, data_count_sorted)

total_rate_bw <- base_bw_df %>%
  # Filter out state total (will add as a vertical line)
  filter(group=="TOTAL") %>%
  pull(rate) %>%
  round(2)