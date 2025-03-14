{{
    config(
        materialized='incremental',
        cluster_by = 'id',
        tags=["movies","genres"],
        partition_by = {
            'field': 'release_date',
            'data_type': 'date',
        },
    )
}}

with
base as (

    select 
    * 
    from 
        {{ ref('stg_tmdb_5000_movies') }}

        --As example of increnemtal build
    {% if is_incremental() %}
        -- load records since the last update
        where release_date > (select max(release_date) from {{ this }})
    {% endif %}
),

movie_genres as
(SELECT id as movie_id,
    cast(JSON_QUERY(j_genres,'$.id') as int) as genre_id,
    JSON_QUERY(j_genres,'$.name') as genre_name,
FROM 
    base
,UNNEST(JSON_QUERY_ARRAY(raw_json_genres)) as j_genres
),

movie_keywords as
(SELECT id as movie_id,
    cast(JSON_QUERY(j_keywords,'$.id') as int) as keyword_id,
    JSON_QUERY(j_keywords,'$.name') as keyword_name,
FROM 
    base
,UNNEST(JSON_QUERY_ARRAY(raw_json_keywords)) as j_keywords
)

select

base.id as id,
budget,
raw_json_genres,
homepage,
raw_json_keywords,
original_language,
original_title,
overview,
popularity,
raw_json_production_companies,
raw_json_production_countries,
release_date,
revenue,
runtime,
raw_json_spoken_languages,
status,
tagline,
title,
vote_average,
vote_count,

--Movie Genres Nested
array (select as struct 
        movie_genres.* 
        --except(movie_id)
        FROM 
            movie_genres as movie_genres
        WHERE base.id =  movie_genres.movie_id 
) as movie_genres,

--Movie Crew Nested
array (select as struct 
        movie_keywords.* 
        --except(movie_id)
        FROM 
            movie_keywords as movie_keywords
        WHERE base.id =  movie_keywords.movie_id 
) as movie_keywords,
from 
    base
