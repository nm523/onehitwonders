# onehitwonders
Analysis for music royalty portfolio origination.

## Context

I undertook this work one weekend in 2021 as part of an application 
to work at a fund primarily focused on the buying and selling of music royalties.

As the fund no longer exists, I have uploaded my work as a portfolio piece.
Below, I provide commentary on various pieces of the work for those interested.

## The Ask

In short, the ask of the assessment was to investigate the "one hit wonder" hypothesis.

Within that, the exercise covered various activities in the data science skillset:

* Procuring and processing data (web scraping, API calls, and the like)
* Analysis (exploration, basic modelling)
* Write-up (making sense of the data)

## Contents

The repository is laid out as follows:

* scripts: scraping and ingestion scripts (essentially, the "ETL" bit)
* notebooks: the analysis
* outputs: the final output report

## Tooling

- Standard Python Data Science stack (pandas, sklearn, matplotlib)
- SQL (sqlite)
- Scraping / API (httpx, beautifulsoup)

## Comments

* If you have any feedback about this, I'd be glad to hear it. Send a message across.
* Looking back, I think the analysis is somewhat overcooked - we could reduce the number of one hit wonders by being more conservative (e.g. limiting to top 10). 

