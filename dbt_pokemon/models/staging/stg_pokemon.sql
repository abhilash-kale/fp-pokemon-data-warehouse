with source as (
    select * from {{ source('raw', 'pokemon') }}
),

deduplicated as (
    select 
        id as pokemon_id,
        raw_payload->>'name' as pokemon_name,
        (raw_payload->>'height')::integer as height,
        (raw_payload->>'weight')::integer as weight,
        (raw_payload->>'base_experience')::integer as base_experience,
        
        -- Extract the arrays we explicitly need for the downstream Marts
        raw_payload->'stats' as stats_array,
        raw_payload->'types' as types_array,
        raw_payload->'abilities' as abilities_array,

        loaded_at as ingested_at,
        row_number() over (partition by id order by loaded_at desc) as rn
    from source
)

select 
    pokemon_id,
    pokemon_name,
    height,
    weight,
    base_experience,
    stats_array,
    types_array,
    abilities_array,
    ingested_at
from deduplicated
where rn = 1
