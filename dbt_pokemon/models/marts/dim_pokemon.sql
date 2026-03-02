{{ config(materialized='incremental', unique_key='pokemon_id') }}

select
    pokemon_id,
    pokemon_name,
    height,
    weight,
    base_experience,
    ingested_at
from {{ ref('stg_pokemon') }}

{% if is_incremental() %}
    where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
