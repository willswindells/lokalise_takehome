--
-- Credits Staging from Seed
--

with

credits as (

    select 
    * 
    from 
        {{ ref('seed_tmdb_5000_credits') }}
        

),

staging_credits as 
(
select 
movie_id,
title,
`cast` as raw_json_cast,
crew as raw_json_crew,
from 
    credits
)

select 
*, 
from staging_credits