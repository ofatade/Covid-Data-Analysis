# Project 2 - Querying COVID-19 Data
## Table of Contents

* <a href="#project-description">Project Description</a>
* <a href="#contributors">Contributors</a>
* <a href="#made-with">Made With...</a>
* <a href="#the-queries">The Queries</a>
  * <a href="#bq1---by-brian-vegh">Brian Vegh's query #1</a>
  * <a href="#bq2---by-brian-vegh">Brian Vegh's query #2</a>
  * <a href="#jq1---by-jeffrey-hafner">Jeffrey Hafner's query #1</a>
  * <a href="#jq2---by-jeffrey-hafner">Jeffrey Hafner's query #2</a>
  * <a href="#eq1---by-eric-thistle">Eric Thistle's query #1</a>
  * <a href="#eq2---by-eric-thistle">Eric Thistle's query #2</a>
  * <a href="#dq1---by-dare-fatade">Dare Fatade's query #1</a>
  * <a href="#dq2---by-dare-fatade">Dare Fatade's query #2</a>
  * <a href="#tq1---by-md-tahmid-khan">Md Tahmid Khan's query #1</a>
  * <a href="#tq2---by-md-tahmid-khan">Md Tahmid Khan's query #2</a>

## Project Description

This was a group project where we were given COVID-19 data which covered the period from Jan. 22, 2020 to May 2, 2021, and we were to work together to create various queries in Scala/Spark to find trends and other information from that data, and then turn that data into various visualizations.

## Contributors

- Brian Vegh [@brianvegh](https://github.com/brianvegh)
- Jeffrey Hafner [@JeffH001](https://github.com/JeffH001)
- Eric Thistle [@erthis](https://github.com/erthis)
- Dare Fatade [@ofatade](https://github.com/ofatade)
- Md Tahmid Khan [@MdTahmidKhan](https://github.com/MdTahmidKhan)

## Made With...
- Scala v2.12.15
- sbt v1.6.2
- Java v8 (v1.8.0_312)
- Spark v3.1.3
- Tableau 2022.1.0

## The Queries
### BQ1 - by Brian Vegh

**???**

### BQ2 - by Brian Vegh

**???**

### JQ1 - by Jeffrey Hafner

**Percent of Deaths Per Case for the Top 10 Most Populous US Counties (May '20 – April '21)**

This query looked at the ten largest counties in the US and showed deaths per confirmed case of COVID-19 for each month from May '20 to April '21.

<div align="center"><a href="/images/JQ1.png"><img alt="Graph of data" src="/images/JQ1.png?raw=true" height=300></a>

(click image for full size view)</div>

### JQ2 - by Jeffrey Hafner

**Average Deaths Per Month Per Million in US States + DC (May '20 – April '21)**

This query looked at the 50 US states plus Washington DC and showed the number of deaths per million for each month, plus the average number of deaths per month, for the 12 month period starting from May '20.

<div align="center"><a href="/images/JQ2a.png"><img alt="Full graph of data" src="/images/JQ2a.png?raw=true" height=300></a>

Full graph of data
(click image for full size view)</div>

<div align="center"><a href="/images/JQ2b.png"><img alt="Graph of just the averages" src="/images/JQ2b.png?raw=true" height=300></a>

Graph of just the averages
(click image for full size view)</div>

### EQ1 - by Eric Thistle

**???**

<div align="center"><a href="/images/EQ1.png"><img alt="Graph of data" src="/images/EQ1.png?raw=true" height=300></a>

(click image for full size view)</div>

### EQ2 - by Eric Thistle

**???**

<div align="center"><a href="/images/EQ2.png"><img alt="Graph of data" src="/images/EQ2.png?raw=true" height=300></a>

(click image for full size view)</div>

### DQ1 - by Dare Fatade

**Most and Least Deaths by COVID-19 in US States and Territories**

This query looked at the ten US states and territories with highest and lowest raw number of deaths due to COVID-19.

<div align="center"><a href="/images/DQ1best.png"><img alt="10 US states and territories with the lowest death totals" src="/images/DQ1best.png?raw=true" height=300></a>

10 US states and territories with the lowest death totals, along with their populations
(click image for full size view)</div>

<div align="center"><a href="/images/DQ1worst.png"><img alt="10 US states and territories with the lowest death totals" src="/images/DQ1worst.png?raw=true" height=300></a>

10 US states and territories with the highest death totals, along with their populations
(click image for full size view)</div>

### DQ2 - by Dare Fatade

**COVID-19 Death/Case Ratio by Country as of April '21**

This query looked at the ratio of COVID-19 deaths to the confirmed cases in countries around the world, as of April 2021.

<div align="center"><a href="/images/DQ2.png"><img alt="Graph of data" src="/images/DQ2.png?raw=true" height=300></a>

(click image for full size view)</div>

### TQ1 - by Md Tahmid Khan

**Percent of Total Deaths Due to COVID-19 by US State**

This query pulled in outside data from the CDC on the <a href="https://www.cdc.gov/nchs/fastats/state-and-territorial-data.htm">total number of deaths in 2020 by State</a>, and used that to show what percentage of deaths overall that year were due to COVID-19 for each state.

<div align="center"><a href="/images/TQ1.png"><img alt="Graph of the data" src="/images/TQ1.png?raw=true" height=300></a>

(click image for full size view)</div>

### TQ2 - by Md Tahmid Khan

This query found the top ten best and worst states based on COVID-19 death rates vs population.

<div align="center"><a href="/images/TQ2best.png"><img alt="Top 10 US states for lowest death rate" src="/images/TQ2best.png?raw=true" height=300></a>

Best 10 US states with the lowest COVID-19 death rates
(click image for full size view)</div>

<div align="center"><a href="/images/TQ2worst.png"><img alt="Top 10 US states for lowest death rate" src="/images/TQ2worst.png?raw=true" height=300></a>

Worst 10 US states with the highest COVID-19 death rates
(click image for full size view)</div>
