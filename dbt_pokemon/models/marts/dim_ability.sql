{{ config(materialized='incremental', unique_key='ability_id') }}

select
    ability_id,
    ability_name,
    is_main_series,
    ingested_at
from {{ ref('stg_abilities') }}

{% if is_incremental() %}
    where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
