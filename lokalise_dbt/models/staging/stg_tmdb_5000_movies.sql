--
-- Movies Staging from Seed
--

with

movies as (

    select  
    * 
    from 
        {{ ref('seed_tmdb_5000_movies') }}

),

staging_movies as 
(
select 
id,
budget,
genres as raw_json_genres,
homepage,
keywords as raw_json_keywords,
original_language,
original_title,
overview,
popularity,
production_companies as raw_json_production_companies,
production_countries as raw_json_production_countries,
release_date,
revenue,
runtime,
spoken_languages as raw_json_spoken_languages,
status,
tagline,
title,
vote_average,
vote_count
from 
    movies
)

select * 
from 
    staging_movies