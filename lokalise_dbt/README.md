

### Setup
Pipenv for enviroment
Bigquery database


- dbt run
- dbt test

### Documentation:
dbt documentation and yml annotation

### Learnings:

Not 5000 records in the files
A few errors in BQ import due to file returns and type errors. 
Fixed: hardcode types for a few columns in staging.yml
Removed 4 returns within the csv. Could be bash script and sed if it was more than 4 entries
Also cast column name is dangerous `cast` is BQs workaround for this

Json in a seed csv into BQ using dbt BQ sql alone appears to not be out of the box. 
Going to work throuhg modeling once without extracting the json then will revisit that. 



