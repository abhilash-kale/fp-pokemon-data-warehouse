-- Fails if any base stat is outside the plausible 1-255 range
select pokemon_id
from {{ ref('fact_pokemon_stats') }}
where stat_hp < 1 or stat_hp > 255
   or stat_attack < 1 or stat_attack > 255
   or stat_defense < 1 or stat_defense > 255
   or stat_special_attack < 1 or stat_special_attack > 255
   or stat_special_defense < 1 or stat_special_defense > 255
   or stat_speed < 1 or stat_speed > 255
