# define global styling elements
# define rc colors we want to use
rc_blue <-  "#070024" # primary
rc_orange <- "#FF6B02" # accent
rc_magenta <- "#a63273" # using slightly desaturated version to help plotline rendering "#ac0068" # accent
rc_grey <- "#cccccc" # neutral
rc_white <- "#ffffff" # neutral
rc_black <- "#333333" # neutral
rc_purple <- "#362178" # accent


# Set colors for bars
base_colors <- c(
  "asian" = rc_orange,
  "asian subgroups" = rc_orange,
  "asian total"= rc_blue,
  "total" = rc_magenta,
  "other" = rc_magenta,
  "default" = rc_grey)

custom_theme <- hc_theme(
  chart = list(
    style = list(
      fontFamily = "Roboto", 
      color = rc_black,
      fontSize="1em")),
  title = list(
    style = list(
      color = rc_black,
      fontSize ="1.2em",
      fontWeight="bold")),
  subtitle = list(
    style = list(
      color = rc_black)),
  legend = list(itemStyle = list(color = rc_black)),
  xAxis = list(
    lineColor = rc_black,
    lineWidth = 2,
    labels = list(
      style = list(
        color = rc_black,
        fontSize ="0.8em",
        fontWeight="bold"))),
  yAxis = list(
    lineColor = rc_black,
    lineWidth = 1,      # Keep the main baseline
    tickWidth = 0,       # Remove the small tick marks
    title = list(
      style=list(
        color = rc_black,
        fontSize="0.8em",
        fontWeight="bold"))),
  plotOptions = list(
    bar = list(
      dataLabels = list(
        enabled=TRUE,
        allowOverlap=TRUE,
        crop=FALSE,
        overflow="justify",
        style = list(
          color = rc_black,
          textOutline = "1px contrast",
          fontWeight = "bold"))))
)
