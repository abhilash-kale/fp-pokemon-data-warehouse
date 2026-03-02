"""
Data Contracts and Schema Registry.

This module defines the Pydantic models used to validate raw JSON payloads 
fetched from the PokeAPI. It acts as the primary quality gate before data 
enters the Data Lake. 

Design Philosophy: "Validate the core, preserve the rest." 
We strictly type the nested arrays and attributes that our downstream dbt 
models rely on (e.g., stats, types, abilities) to prevent pipeline crashes. 
However, by using `ConfigDict(extra='allow')`, we ensure the raw payload 
remains a lossless, append-only ledger in the Raw layer.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

# ==========================================
# SHARED COMPONENTS
# ==========================================
class NamedAPIResource(BaseModel):
    """
    Handles standard standard {'name': '...', 'url': '...'} nested objects.
    The PokeAPI heavily relies on this pattern for linked resources.
    """
    name: str
    url: str


# ==========================================
# POKEMON PAYLOAD COMPONENTS
# ==========================================
class AbilityItem(BaseModel):
    ability: NamedAPIResource
    is_hidden: bool
    slot: int

class StatItem(BaseModel):
    """
    Data Quality Guardrail: We enforce numerical boundaries at the ingestion layer.
    If the API sends a stat outside the 1-255 range, it violates the contract.
    """
    base_stat: int = Field(ge=1, le=255, description="Stats must be between 1 and 255")
    effort: int
    stat: NamedAPIResource

class TypeItem(BaseModel):
    slot: int
    type: NamedAPIResource


# ==========================================
# ABILITY PAYLOAD COMPONENTS
# ==========================================
class EffectEntry(BaseModel):
    effect: str
    language: NamedAPIResource
    short_effect: Optional[str] = None

class FlavorTextEntry(BaseModel):
    flavor_text: str
    language: NamedAPIResource
    version_group: NamedAPIResource

class AbilityPokemon(BaseModel):
    is_hidden: bool
    slot: int
    pokemon: NamedAPIResource


# ==========================================
# TYPE PAYLOAD COMPONENTS
# ==========================================
class DamageRelations(BaseModel):
    """
    Strictly types the complex combat matrix. If the API schema drifts here,
    we want to catch it immediately rather than failing silently in BI reports.
    """
    no_damage_to: List[NamedAPIResource]
    half_damage_to: List[NamedAPIResource]
    double_damage_to: List[NamedAPIResource]
    no_damage_from: List[NamedAPIResource]
    half_damage_from: List[NamedAPIResource]
    double_damage_from: List[NamedAPIResource]

class TypePokemon(BaseModel):
    slot: int
    pokemon: NamedAPIResource


# ==========================================
# MASTER DATA CONTRACTS
# ==========================================
class PokemonSchema(BaseModel):
    """Primary Data Contract for /pokemon/{id} endpoint."""
    model_config = ConfigDict(extra='allow')

    id: int
    name: str
    height: int
    weight: int
    base_experience: Optional[int] = None

    # Nested arrays required for downstream dbt unnesting (json_each)
    abilities: List[AbilityItem]
    stats: List[StatItem]
    types: List[TypeItem]

class TypeDetailSchema(BaseModel):
    """Primary Data Contract for /type/{id} endpoint."""
    model_config = ConfigDict(extra='allow') 

    id: int
    name: str
    damage_relations: DamageRelations
    pokemon: List[TypePokemon]

class AbilityDetailSchema(BaseModel):
    """Primary Data Contract for /ability/{id} endpoint."""
    model_config = ConfigDict(extra='allow')

    id: int
    name: str
    is_main_series: bool
    generation: NamedAPIResource

    # Nested arrays required for downstream dbt unnesting (json_each)
    effect_entries: List[EffectEntry]
    flavor_text_entries: List[FlavorTextEntry]
    pokemon: List[AbilityPokemon]
