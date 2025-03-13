--
-- Credits Staging from Seed
--

with

credits as (

    select 
    --movie_id, title
    * 
    from {{ ref('seed_tmdb_5000_credits') }}

),

staging_credits as 
(
select 
*
-- movie_id,
-- title,
-- cast,
-- crew
from 
credits
)

select * 
from staging_credits
