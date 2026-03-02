{{ config(materialized='incremental', unique_key='pokemon_id') }}

with filtered_stg as (
    select * from {{ ref('stg_pokemon') }}
    
    {% if is_incremental() %}
        where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
),

unnested_stats as (
    select
        pokemon_id,
        json_extract_string(stat.value, '$.stat.name') as stat_name,
        (json_extract_string(stat.value, '$.base_stat'))::integer as base_stat,
        ingested_at
    from filtered_stg,
    json_each(stats_array) as stat
)

select 
    pokemon_id,
    coalesce(sum(case when stat_name = 'hp' then base_stat end), 0) as stat_hp,
    coalesce(sum(case when stat_name = 'attack' then base_stat end), 0) as stat_attack,
    coalesce(sum(case when stat_name = 'defense' then base_stat end), 0) as stat_defense,
    coalesce(sum(case when stat_name = 'special-attack' then base_stat end), 0) as stat_special_attack,
    coalesce(sum(case when stat_name = 'special-defense' then base_stat end), 0) as stat_special_defense,
    coalesce(sum(case when stat_name = 'speed' then base_stat end), 0) as stat_speed,
    max(ingested_at) as ingested_at
from unnested_stats
group by 1
