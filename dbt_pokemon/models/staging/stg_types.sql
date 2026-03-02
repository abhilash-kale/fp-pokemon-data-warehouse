with source as (
    select * from {{ source('raw', 'types') }}
),

deduplicated as (
    select 
        id as type_id,
        raw_payload->>'name' as type_name,
        loaded_at as ingested_at,
        row_number() over (partition by id order by loaded_at desc) as rn
    from source
)

select 
    type_id,
    type_name,
    ingested_at
from deduplicated
where rn = 1
