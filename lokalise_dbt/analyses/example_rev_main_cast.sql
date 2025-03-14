{{
    config(
        materialized = 'table',
    )
}}


--Find the most successfull top 5 actors
select 	
movie_cast.name,
count(*) as movies,
sum(revenue) as revenue,
round(sum(revenue)/count(*),0) as revenue_per_movie,
from 
    {{ ref('int_tmdb_5000_movies') }} as  movies
left join 
  {{ ref('int_tmdb_5000_credits') }}  as credits
  on movies.id = credits.movie_id
    ,unnest(credits.movie_cast) as movie_cast
where 
--in the Main Cast list - specified in dbt profile
    cast(movie_cast.order as int) <= var('main_cast')
    and release_date < date_sub(current_date(), INTERVAL var('mainlookback_timeframe_days_cast') DAY)
group by 1

--Likely to take another movie
having 
    movies > 5