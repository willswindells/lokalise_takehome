--
-- Credits Staging from Seed
--

with

credits as (

    select 
    * 
    from {{ ref('seed_tmdb_5000_credits') }}

),

staging_credits as 
(
select 

movie_id,
title,
`cast` as raw_json_cast,
crew as raw_json_crew,
-- JSON_QUERY(json_crew, '$') AS crew_json
from 
credits
)

select 
*, 
-- JSON_QUERY(crew_json, '$.credit_id') AS credit_id,
from staging_credits