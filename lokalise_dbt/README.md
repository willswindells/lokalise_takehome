

### Setup
Pipenv for enviroment managment
Bigquery database - 

Pipenv from 2025_Lokalise_takehome folder
dbt from lokalise_dbt folder

Can change to DuckDb solution if needed, I am not familiar with it so used GoogleBQ
Would also use the UI on BQ to walk through the models for a show and tell


### Documentation:
dbt documentation done in ymls

### Learnings:

Not 5000 records in the csv files
A few errors in BQ import due to file returns and type errors. 
Fixed: hardcode types for a few columns in dbt seed staging.yml
Removed 4 returns within the csv. Could be bash script and sed if it was more than 4 entries and/or it was a pipeline

Also cast column name is dangerous `cast` is BQs workaround for this

Json in a seed csv into BQ using dbt BQ sql alone appears to not be out of the box. 
Going to work throuhg modeling once without extracting the json then will revisit that. 

Decided to Nest Json extracted within tables to boost performance at the cost of extra storage, there are not many films made
Coded a few Json extracts as an example

Added one custom test as examle checking the date of the films is in the past 
Incremental builds for int Json processing tables, Done using both release date and movie id. For example only. 


Added two analyses files- One using two dbt variabls as examples of how to use the data to find the top recent revenue generating main actors

