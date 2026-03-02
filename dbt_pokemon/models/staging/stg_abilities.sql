with source as (
    select * from {{ source('raw', 'abilities') }}
),

deduplicated as (
    select 
        id as ability_id,
        raw_payload->>'name' as ability_name,
        (raw_payload->>'is_main_series')::boolean as is_main_series,
        loaded_at as ingested_at,
        row_number() over (partition by id order by loaded_at desc) as rn
    from source
)

select 
    ability_id,
    ability_name,
    is_main_series,
    ingested_at
from deduplicated
where rn = 1
