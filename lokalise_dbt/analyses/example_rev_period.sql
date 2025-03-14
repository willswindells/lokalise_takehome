{{
    config(
        materialized = 'table',
    )
}}

select 
FORMAT_DATE( '%Y - %m',  release_date) AS release_period,
count(*) as movies,
sum(revenue) as revenue,
round(sum(revenue)/count(*),0) as revenue_per_movie,
from 
    {{ ref('int_tmdb_5000_movies') }} as  movies