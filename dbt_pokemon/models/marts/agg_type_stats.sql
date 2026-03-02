{{ config(materialized='table') }}

select 
    t.type_name,
    count(distinct b.pokemon_id) as total_pokemon,
    round(avg(f.stat_hp), 2) as avg_hp,
    round(avg(f.stat_attack), 2) as avg_attack,
    round(avg(f.stat_defense), 2) as avg_defense,
    round(avg(f.stat_speed), 2) as avg_speed
from {{ ref('dim_type') }} t
left join {{ ref('bridge_pokemon_type') }} b 
    on t.type_name = b.type_name
left join {{ ref('fact_pokemon_stats') }} f 
    on b.pokemon_id = f.pokemon_id
group by 1
