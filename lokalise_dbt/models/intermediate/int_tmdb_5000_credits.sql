{{
    config(
        materialized='incremental',
        cluster_by = 'movie_id',
        tags=["credits","movies"],
        unique_key='movie_id',
    )
}}

with
base as (

    select 
    * 
    from 
        {{ ref('stg_tmdb_5000_credits') }}
        
    --As example of increnemtal build
    {% if is_incremental() %}
        -- load records since the last update
        where movie_id > (select max(movie_id) from {{ this }})
    {% endif %}
),

movie_cast as
(SELECT movie_id,
    cast(JSON_QUERY(j_cast,'$.cast_id') as int) as cast_id,
    JSON_QUERY(j_cast,'$.credit_id') as credit_id,
    JSON_QUERY(j_cast,'$.name') as name,
    JSON_QUERY(j_cast,'$.gender') as gender,
    JSON_QUERY(j_cast,'$.order') as `order`,
    FROM 
        base
    ,UNNEST(JSON_QUERY_ARRAY(raw_json_cast)) as j_cast
),

movie_crew as
(SELECT movie_id,
    JSON_QUERY(j_crew,'$.credit_id') as credit_id,
    JSON_QUERY(j_crew,'$.department') as department,
    cast(JSON_QUERY(j_crew,'$.id') as int) as crew_id,
    JSON_QUERY(j_crew,'$.job') as `job`,
    JSON_QUERY(j_crew,'$.name') as `name`,
    FROM 
        base
    ,UNNEST(JSON_QUERY_ARRAY(raw_json_crew)) as j_crew
)

select

base.movie_id, 
base.title,

--Movie Cast Nested
array (select as struct 
        movie_cast.* 
        --except(movie_id)
        FROM 
            movie_cast as movie_cast
        WHERE base.movie_id =  movie_cast.movie_id 
) as movie_cast,

--Movie Crew Nested
array (select as struct 
        movie_crew.* 
        --except(movie_id)
        FROM 
            movie_crew as movie_crew
        WHERE base.movie_id =  movie_crew.movie_id 
) as movie_crew,
from 
    base
