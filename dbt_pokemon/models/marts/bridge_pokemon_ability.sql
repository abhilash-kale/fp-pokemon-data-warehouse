{{ config(materialized='incremental', unique_key=['pokemon_id', 'ability_name']) }}

with filtered_stg as (
    select * from {{ ref('stg_pokemon') }}

    {% if is_incremental() %}
        where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    pokemon_id,
    json_extract_string(ability_obj.value, '$.ability.name') as ability_name,
    (json_extract_string(ability_obj.value, '$.is_hidden'))::boolean as is_hidden,
    (json_extract_string(ability_obj.value, '$.slot'))::integer as ability_slot,
    ingested_at
from filtered_stg,
json_each(abilities_array) as ability_obj
