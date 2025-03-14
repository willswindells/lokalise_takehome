--Unreleased Movies

select * 
from {{ ref('seed_tmdb_5000_movies') }} as movies_seed
where movies_seed.release_date > current_date()