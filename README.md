# RACE COUNTS
### November 2025

<base target="_blank">

<img src="https://github.com/catalystcalifornia/RaceCounts/blob/main/images/rc_homepage.PNG" alt="RACE COUNTS Homepage">




<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a></li>
    <li><a href="#about-the-data">About The Data</a></li>
     <ul>
        <li><a href="#indicators">Indicators</a></li>
        <li><a href="#indexes">Indexes</a></li>
        <li><a href="#data-methodology">Data Methodology</a></li>
        <li><a href="#measuring-outcomes,-impact-&-racial-disparity">Measuring Outcomes, Impact & Disparity</a></li>
        <li><a href="#summarizing-racial-disparity">Summarizing Racial Disparity</a></li>
        <li><a href="#key-limitations">Key Limitations</a></li>
        <li><a href="#new-this-year">New This Year</a></li>
      </ul>
    <li><a href="#contributors">Contributors</a></li>
    <li><a href="#contact-us">Contact Us</a></li>
    <li><a href="#citation">Citation</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#race-counts-partners">RACE COUNTS Key Partners</a></li>
  </ol>
</details>


## About The Project

The website [RACECOUNTS.org](https://www.racecounts.org?target=_blank) is one part of the larger RACE COUNTS initiative created by [Catalyst California](https://www.catalystcalifornia.org/) (formerly Advancement Project California) and partners. At Catalyst California, we strategize with community partners to identify funding, services and opportunities in our public systems that can be redistributed for more just outcomes for all. Our goal is to promote racial equity and build a foundation so that every Californian may thrive. The RACE COUNTS website includes an analysis of racial disparity, overall outcomes, and impact based on population size. This repo is meant to make the methods we use more transparent and duplicable. The repo is a work in progress and we will continue to add more documentation around indicators, indexes, and more as we continue to update the website.


# About The Data

## Indicators
RACE COUNTS includes 47 county/state indicators, 29 city indicators, and 31 state legislative district indicators, across seven issues. The issues include: Democracy, Economic Opportunity, Education, Health Care Access, Healthy Built Environment, Housing, and Safety & Justice. These issues and indicators were selected through a collaborative process with our partners. Find out more about our partners here: [https://www.racecounts.org/about/](https://www.racecounts.org/about/#key-partners). For each indicator, counties, cities, and legislative districts receive a rank for racial disparity and a rank for overall outcomes. The city, county, or legislative district ranked one for disparity is the most racially disparate, while the geography ranked one for outcomes has the best outcomes. 

## Indexes
At county level, we calculate one index for each of the seven issue areas that includes all indicators in that issue. For state legislative districts, we calculate one index for five of the seven issue areas. We do not calculate legislative district indexes for Democracy or Health Care Access because those issue areas have only one indicator each. At city, county, and legislative district levels, we calculate a composite Racial Equity Index combining all indicators. In all indexes, each city, county, and legislative district receives a rank for racial disparity and a rank for overall outcomes. The city, county or legislative district ranked one for disparity is the most racially disparate, while the geography ranked one for outcomes has the best outcomes. 

## Data Methodology
The following links contain detailed information about our indicator methodology. For each indicator, we provide the data source and year with a link, detailed race/ethnicity definitions, and our methodology including calculations and data screening process.

[RACE COUNTS: Indicator Methodology for County and State Data](https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_CountyState.html) <br>
[RACE COUNTS: Indicator Methodology for City Data](https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_City.html) <br> 
[RACE COUNTS: Indicator Methodology for Legislative District Data](https://catalystcalifornia.github.io/RaceCounts/Methodology/Indicator_Methodology_Leg_District.html) <br> 

The following links contain detailed information about our composite and issue area index methodologies. For each index, we provide our methodology including calculations and data screening process.

[RACE COUNTS: Index Methodology for County Data](/Methodology/IndexMethodology_County.pdf) <br>
[RACE COUNTS: Index Methodology for City Data](/Methodology/IndexMethodology_City.pdf) <br>
[RACE COUNTS: Index Methodology for Legislative District Data](/Methodology/IndexMethodology_Leg_District.pdf) <br>

The following link contains detailed information about how we think about race/ethnicity and the standard definitions and labels we use when possible.

[RACE COUNTS: Race & Ethnicity Methodology](/Methodology/README_Race_Ethnicity.md) <br>

<p align="right">(<a href="#RACE-COUNTS">back to top</a>)</p>

<!--[RACE COUNTS: Key Takeaways Methodology (2023)](/KeyTakeaways/README_Key_Takeaways.md) <br>-->

## Measuring Outcomes, Impact & Racial Disparity
Measuring outcomes and impact are straightforward. An outcome is the rate for the total population on an indicator, across an issue, or across all issues. For example, when we compare outcomes in high school graduation rates between Los Angeles and Orange counties, we are comparing their overall graduation rates. Impact is the size of the total population. Following this example, Los Angeles has a population of nearly 10 million people, more than three times the size of Orange, with a population of nearly 3.1 million people. All else being equal, expected impacts of disparities are thus expected to be larger in Los Angeles than Orange county based on population size.

Racial disparity is more complicated. We calculate disparity in RACE COUNTS for two main reasons: to compare racial groups directly to one another (e.g., life expectancy of Latinx vs. Whites) and to summarize the overall level of disparity across all races for comparison across counties (e.g., disparity in high school graduation rates in Los Angeles County vs. Orange County). The overall disparity measure summarizes all of the individual racial group comparisons.

Racial groups are directly compared with a straightforward rate difference. To compare high school graduation rates of Latinx and Whites in a county would simply be subtracting the Latinx high school graduation rate from the White high school graduation rate, with a result of 0 implying total equity. In Figure 1, the rate difference between Latinx and Whites is 4.7% in Los Angeles County (89.6% – 84.9% = 4.7%).

<img src="images/high-school-graduation-california.jpg" alt="LA County HS Graduation by Race Bar Chart">

## Summarizing Racial Disparity
We use a metric called the Index of Disparity (ID) to summarize overall equity in outcomes. The ID is the average of the absolute rate differences between each group rate and a reference rate. This average is expressed as a percentage of the reference rate (Pearcy and Keppel 2002, Harper et al. 2010, Harper 2011). For RACE COUNTS we use the best rate (best outcome) out of all racial groups as the reference rate for IDs to prioritize both equity and progress. Note: In rare cases where the best rate cannot be used because of data limitations, we have substituted the total population rate or the best non-zero rate. 

For example, consider Los Angeles County high school graduation. Los Angeles County schools graduate Filipinx students at the highest rate (96.1%), making this the reference rate. The ID or average difference in high school graduation rates of each race from the reference rate is 11.2%. This is more than double Orange County’s high school graduation ID of 4.6%.

The ID is sensitive to how we define an indicator (i.e., insured vs uninsured, employed vs unemployed). We make a call as the analyst “experts” on which is the best way to represent an indicator, based on how it is used in the literature, what we think is helpful for this project, and also, based on how the indicator is understood and used in general.

## Key Limitations
Our methodology has a number of limitations as does any data analysis, but three are worth highlighting here.

First, race is incredibly intersectional and RACE COUNTS primarily focuses on the racial experience. Intersectional experiences related to class, immigrant status, gender, and other population characteristics are largely absent from the outcome, disparity, and impact analysis. Thus, the results hide important findings by class, immigrant status and more.

Second, RACE COUNTS primarily includes data at city, county, and state levels. The county and state results can obscure important trends at sub-county levels.  

Finally, while RACE COUNTS is the most comprehensive compilation of data about racial equity by county in California, clear weaknesses in available data are evident. Data availability in the Democracy issue was particularly challenging for less populous places. The availability of data by race at sub-state levels was challenging across the board, and we created weighted averages to address this issue in some cases such as Lack of Greenspace. 

For more, please see our [RACE COUNTS: Race & Ethnicity Methodology](/Methodology/README_Race_Ethnicity.md).

## New This Year
Each year RACE COUNTS undergoes updates due to changes in data availability, how our sources collect data, and upgrades to our methodologies. Below is a summary of the changes for this year's RACE COUNTS release.

<details>
  <summary>Methodology-Related Changes</summary>
     <ul>
        <li>Working definition of Southwest Asian / North African: We expanded to include the following Census Ancestry responses: Amazigh, Azerbaijan, Copt, Djiboutian, Levantine, Mauritanian, Somali, Southwest Asian or North African, SWANA, West Asian, and Middle Eastern or North African Responses, not classified elsewhere. See our RACE COUNTS: Race & Ethnicity Methodology for more information.</li>
        <li>Denied Mortgages, Drinking Water Contaminants, Eviction, Foreclosure, Proximity to Hazards, Subprime Mortgages, and Toxic Releases indicators: In 2025, we converted the census tract-based data from 2010 vintage tracts to 2020 vintage tracts before calculating weighted averages by race/ethnicity. This resulted in minor changes to the results.</li>
        <li>Census Participation indicator: In 2025, we used householders not population as the denominator for rate calculations, as surveys are collected for households not individuals.</li>
        <li>Low Birthweight indicator: In 2025, we calculated rates for American Indian / Alaska Native alone or in combination with another race, including Latinx and non-Latinx. We used the same method for Native Hawaiian / Pacific Islander rates.</li>
        <li>Census Public Use Microdata Sample (PUMS) indicators, including Connected Youth, Living Wage, Employment as Officials & Managers, and Low-Quality Housing: In 2025, we used a population-weighted crosswalk from Geocorr 2022 to assign PUMAs to counties and legislative districts, rather than a crosswalk based on area intersect used in the past.</li>
        <li>Officer-Initiated Stops indicator: We now have two years of data for most agencies, so we reported the average annual stop rate and the multi-year total number of stops.</li>
        <li>High School Graduation indicator: In 2025, we suppressed data for geography and race combinations that have fewer than 20 students in the cohort. We previously suppressed data when there were fewer than 20 graduates in a cohort.</li>
        <li>Use of Force indicator: In 2025, we suppress data for geography and race combinations with fewer than 800 people. We previously suppressed data for populations smaller than 100.</li>
    </ul>
</details>

<details>
   <summary>Data-Related Change</summary>
      <ul>
        <li>None this year.</li>
      </ul>
</details>

<p align="right">(<a href="#RACE-COUNTS">back to top</a>)</p>


## Contributors
* [Alexandra Baker, Research & Data Analyst II](https://github.com/bakeralexan)
* [Alicia Vo, Research & Data Analyst I](https://github.com/avoCC)
* [Chris Ringewald, Senior Director of Research & Data Analysis](https://github.com/cringewald)
* [Elycia Mulholland Graves, Director of Research & Data Analysis](https://github.com/elyciamg)
* [Hillary Khan, Database Architect Manager](https://github.com/hillarykhan)
* [Jennifer Zhang, Research & Data Manager](https://github.com/jzhang514)
* [Leila Forouzan, Senior Manager of Research & Data Analysis](https://github.com/lforouzan)
* [Maria Khan, Senior Research & Data Analyst](https://github.com/mariatkhan)


## Contact Us
[Chris Ringewald](https://www.linkedin.com/in/chris-ringewald-6766369/) - cringewald[at]catalystcalifornia.org  <br>


[Leila Forouzan](https://www.linkedin.com/in/leilaforouzan/) - lforouzan[at]catalystcalifornia.org</p>


## Citation
To cite RACE COUNTS, please use the following:

Catalyst California; RACE COUNTS, racecounts.org, [current year].


## License

[License](License.md)


## RACE COUNTS Key Partners

* [CALIFORNIA CALLS](https://www.cacalls.org/)
* [USC Dornsife](https://dornsife.usc.edu/)
* [PICO California](http://www.picocalifornia.org/)

<p align="right">(<a href="#RACE-COUNTS">back to top</a>)</p>

