```{r setup, include = FALSE}

knitr::opts_chunk$set(comment = FALSE, message = FALSE, warning = FALSE, echo = FALSE)

#Set up workspace
library(RPostgreSQL)
library(sf)
library(htmltools)
library(tidyr)
#library(rgdal)
library(reshape2)
library(plotly)
library(tidyverse)
library(highcharter)

```

# RACE COUNTS: County & State Indicator Methodology
### October 2024

<base target="_blank">


<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/rc_homepage.PNG" alt="RACE COUNTS Homepage">

The website [RACECOUNTS.org](https://www.racecounts.org) is one part of the larger RACE COUNTS initiative created by [Catalyst California](https://www.catalystcalifornia.org/) (formerly Advancement Project California) and partners. The RACE COUNTS website includes an analysis of racial disparity, overall outcomes, and impact based on population size. This repo is meant to make the methods we use more transparent and duplicable. For more information about the project overall, please see the overall [RACE COUNTS README](https://github.com/catalystcalifornia/RaceCounts/blob/main/README.md).

A note on Racial/Ethnic Categories: Race is a social construct and a biological construct, neither of which are discussed in depth in this analysis. Thus, we use a simplified view of race to portray racial data to a broader audience. Ethnicity is also treated lightly, and while Latinx is an ethnicity, it is generally treated and spoken about as a race, even though Latinxs can be of any race. Additionally, each data source uses slightly different terminology and categorization. Considering all these factors, we use one simplified set of labels for the RACE COUNTS website and publications for consistency while maintaining fidelity to the data sources as much as possible. To find out more, please see our [Race & Ethnicity Methodology documentation](https://github.com/catalystcalifornia/RaceCounts/blob/main/Methodology/README_Race_Ethnicity.md).


<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#safety-and-justice">Crime and Justice</a></li>
    # <li><a href="#race-and-ethnicity">Race and Ethnicity</a></li>
    # <li><a href="#labels-and-definitions-used-in-race-counts">Labels and Definitions Used in RACE COUNTS</a></li>
    # <li><a href="#data-source-limitations">Data Source Limitations</a></li>
    # <li><a href="#race-pages-on-racecounts-website">Race Pages on RACECOUNTS Website</a></li>
  </ol>
</details>


## Crime and Justice



<p align="right">(<a href="#top">back to top</a>)</p>


## Race and Ethnicity

In RACE COUNTS, we measure outcomes, racial disparity, and population impacts for indicators across seven issue areas. Race is a social and a biological construct, neither of which are discussed in depth in our analysis. Thus, we use a simplified view of race to portray data to a broader audience. Ethnicity is also treated lightly, and while Latinx is an ethnicity, it is generally treated and spoken about as a race, even though Latinxs can be of any race. 

<p align="right">(<a href="#top">back to top</a>)</p>


## Labels and Definitions Used in RACE COUNTS

We use one simplified set of terms for the RACE COUNTS website and publications, with a few exceptions. We have standard labels and definitions for each of those groups that we use in our calculations, or match as closely as possible given data limitations. We selected the terms and definitions listed below as our standard based on conversations with our partners and advocates from these communities.

| Race  | One Race or In Combination | Latinx Inclusion |
| :---------- | :------------ | :-------- |
| American Indian / Alaska Native | Alone or in combination with another race  | Includes |
| Asian | Alone | Excludes |
| Black | Alone | Excludes |
| Filipinx | Alone | Excludes |
| Latinx | Alone or in combination with another race  | Includes |
| Native Hawaiian / Pacific Islander | Alone or in combination with another race  | Includes |
| Southwest Asian / North African | Alone or in combination with another race  | Includes |
| White | Alone | Excludes |
| Another Race | Alone | Excludes |
| Multiracial | Alone or in combination with another race  | Excludes |

There are instances where we use different terms, or definitions, due to considerations like the preferences of our partners on a specific project or data considerations.

The specific definition of each race / ethnicity for each indicator can be found in the [Indicator Methodology documents](https://github.com/catalystcalifornia/RaceCounts/tree/main/Methodology).

<p align="right">(<a href="#top">back to top</a>)</p>


## Data Source Limitations

There are three main data source limitations in relation to data cut by race: variation in how groups are defined, which groups are included in the data, and reliability of statistics. Each data source uses different definitions and includes statistics for a different list of groups. This becomes especially challenging when datasets include a category named "Other", "Some Other Race", "Another Race". Often metadata is lacking so it is not possible to fully understand who is included in that group. That group includes vastly different people depending on the data source. When data include a very broad group with varied experiences such as "Another Race", it can render statistics for that group less meaningful. 

Examples:
* American Community Survey tables do include many that are cut by race/ethnicity. However, while these tables provide separate statistics for non-Latinx White residents and all White residents (Latinx and non-Latinx combined) more often than for they do not do the same for other groups.

* California Department of Education data includes statistics for Filipinx students, while most other data sources do not include disaggregated data for Filipinx people. At the same time, their data includes a much more limited definition of American Indian / Alaska Natives and Native Hawaiian / Pacific Islanders than our standard RACE COUNTS definitions do. They include only those who identify as either of those two groups alone, not in combination with another group, and also exclude Latinxs from those groups.

* California Department of Justice and others include statistics for a combined Asian and Pacific Islander group that masks varied experiences.

* Public data collection systematically erases Southwest Asian / North African (SWANA) people by grouping them as White, though many do not identify that way. In addition, inconsistent definitions of SWANA lead to questions around data reliability and comparisons.

* Other datasets, such as American Community Survey Public Use Microdata (PUMS) include rich detail allowing us to calculate statistics for our standard RACE COUNTS race/ethnicity groupings.

Reliability of data is often low for groups that have smaller populations such as American Indian / Alaska Natives due to data collectors having inadequate sample sizes. As a result, we do our best to strike a balance between including data for as many groups as possible, while also only presenting statistics that we are confident are strong enough for advocacy and policymaking. At times this may mean that we suppress data for one or more groups.

<p align="right">(<a href="#top">back to top</a>)</p>


## Race Pages on RACECOUNTS Website
We currently have Race pages for Asian, American Indian / Alaska Native, Black, Latinx, and Native Hawaiian / Pacific Islander Californians on the website. These groups have data for most RACE COUNTS indicators for many places. We do not have pages for other groups including Southwest Asian / North African, Another Race, Multiracial, Filipinx and more. The Another Race and Multiracial groups can be hard to define and often include people with very different lived experiences, making it less meaningful to compare across indicators. Additional groups with unique experiences, such as Filipinx Californians or those with origins in the Middle East, Southwest Asia and North Africa (MENA or SWANA) are not represented on a race page due to data availability issues. At the same time, we are always working to expand our methodologies and include data for some of the groups without Race pages on the Place and Issue pages on the website.

<p align="right">(<a href="#top">back to top</a>)</p>

