{{ config(materialized='incremental', unique_key=['pokemon_id', 'type_name']) }}

with filtered_stg as (
    select * from {{ ref('stg_pokemon') }}

    {% if is_incremental() %}
        where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    pokemon_id,
    json_extract_string(type_obj.value, '$.type.name') as type_name,
    (json_extract_string(type_obj.value, '$.slot'))::integer as type_slot,
    ((json_extract_string(type_obj.value, '$.slot'))::integer = 1) as is_primary,
    ingested_at
from filtered_stg,
json_each(types_array) as type_obj
