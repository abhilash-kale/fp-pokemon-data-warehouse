{{ config(materialized='incremental', unique_key='type_id') }}

select
    type_id,
    type_name,
    ingested_at
from {{ ref('stg_types') }}

{% if is_incremental() %}
    where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
