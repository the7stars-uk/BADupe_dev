# BADupe

This solution determines changes the status of a keyword from 'ENABLED' when a competitor ad is showing in the SERP to 'PAUSED' when a competitor ad is not showing in the SERP.

Requirements: 
- Account ID
- Account name
- Keywords (labelled in GoogleAds with 'badupe kw')

Steps: 
1. Getting keywords from GoogleAds
This step is done in BigQuery and gets labelled keywords from GoogleAds into BigQuery using the BigQuery Data Transfer Service. 

2. Analysing GoogleAds keywords against SERP data from dataforseo
Analyses the labelled keywords against SERP data from the dataforseo API 

3. Making changes to GoogleAds account
Using the GoogleAds API, keyword statuses are updated depending on whether there is a competitor ad present or not. 