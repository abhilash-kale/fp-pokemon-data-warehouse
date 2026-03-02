import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional, Type, Set
from tenacity import retry, wait_exponential, stop_after_attempt
from pydantic import BaseModel, ValidationError

from src.schemas import PokemonSchema, TypeDetailSchema, AbilityDetailSchema
from src.database import get_db_connection, get_db_watermark

# Configure module logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Pipeline Constants
POKEAPI_BASE_URL = "https://pokeapi.co/api/v2"
LIMIT = 151
MAX_THREADS = 10

# Global session to maintain a connection pool for high-throughput HTTP requests
session = requests.Session()

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def fetch_url(url: str) -> Dict[str, Any]:
    """
    Executes an HTTP GET request with exponential backoff.
    Resilience is critical here to prevent rate-limit bans from the PokeAPI.
    """
    response = session.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def get_partition_dir(entity_name: str) -> str:
    """
    Generates a partition directory path (e.g., year=2026/month=03/day=02).
    This structure optimizes downstream Data Lake querying and file pruning.
    """
    now = datetime.now(timezone.utc)
    path = os.path.join(
        f"data/raw/{entity_name}",
        f"year={now.year}",
        f"month={now.month:02d}",
        f"day={now.day:02d}"
    )
    os.makedirs(path, exist_ok=True)
    return path

def extract_id(url: str) -> int:
    """Extracts the integer ID from a REST API URL endpoint."""
    return int(url.rstrip('/').split('/')[-1])

def fetch_and_validate(url: str, schema_class: Type[BaseModel]) -> Optional[Dict[str, Any]]:
    """
    Worker function: Fetches API payload and enforces the Data Contract.
    If the payload violates our Pydantic schema, it is silently dropped and logged 
    rather than corrupting the Data Lake.
    """
    try:
        raw_data = fetch_url(url)
        schema_class.model_validate(raw_data)
        return raw_data 
    except ValidationError as e:
        logger.error(f"Data Contract Violation for {url}: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing {url}: {e}")

    return None

def write_batch_to_lake(entity_name: str, data_list: List[Dict[str, Any]]) -> None:
    """
    Sinks a validated batch of JSON records into an append-only .jsonl file in the Data Lake.
    """
    if not data_list:
        return

    partition_dir = get_partition_dir(entity_name)
    filename = f"extract_{int(time.time())}.jsonl"
    file_path = os.path.join(partition_dir, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        for item in data_list:
            f.write(json.dumps(item) + "\n")

    logger.info(f"Appended {len(data_list)} validated {entity_name} records to {file_path}")

def run_extraction() -> None:
    """
    Main orchestration logic for the Extraction phase.
    Handles incremental state tracking, concurrent fetching, and dependency resolution.
    """
    logger.info("Initializing API Extraction Phase...")

    # Establish incremental state boundaries (Watermarks)
    con = get_db_connection()
    try:
        known_pokemon: Set[int] = get_db_watermark(con, "pokemon")
        known_types: Set[int] = get_db_watermark(con, "types")
        known_abilities: Set[int] = get_db_watermark(con, "abilities")
    finally:
        con.close() 

    # Retrieve base entity list
    logger.info(f"Fetching target Pokemon list (Limit: {LIMIT})...")
    base_response = fetch_url(f"{POKEAPI_BASE_URL}/pokemon?limit={LIMIT}")
    pokemon_list = base_response.get("results", [])

    urls_to_fetch = [p["url"] for p in pokemon_list if extract_id(p["url"]) not in known_pokemon]

    if not urls_to_fetch:
        logger.info("No new Pokemon delta detected. Exiting extraction phase gracefully.")
        return

    valid_pokemon: List[Dict[str, Any]] = []
    new_types_urls: Set[str] = set()
    new_abilities_urls: Set[str] = set()

    # -------------------------------------------------------------------
    # Phase 1: Primary Entity Extraction (Pokemon)
    # -------------------------------------------------------------------
    logger.info(f"Extracting {len(urls_to_fetch)} new Pokemon records concurrently...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(fetch_and_validate, url, PokemonSchema) for url in urls_to_fetch]

        for future in as_completed(futures):
            data = future.result()
            if data:
                valid_pokemon.append(data)

                # Parse and enqueue downstream dependencies in RAM to minimize database roundtrips
                for t in data.get("types", []):
                    t_id = extract_id(t["type"]["url"])
                    if t_id not in known_types:
                        new_types_urls.add(t["type"]["url"])
                        known_types.add(t_id)

                for a in data.get("abilities", []):
                    a_id = extract_id(a["ability"]["url"])
                    if a_id not in known_abilities:
                        new_abilities_urls.add(a["ability"]["url"])
                        known_abilities.add(a_id)

    write_batch_to_lake("pokemon", valid_pokemon)

    # -------------------------------------------------------------------
    # Phase 2: Dependency Extraction (Types & Abilities)
    # -------------------------------------------------------------------
    valid_types: List[Dict[str, Any]] = []
    valid_abilities: List[Dict[str, Any]] = []

    if new_types_urls or new_abilities_urls:
        logger.info(f"Extracting dependencies: {len(new_types_urls)} Types, {len(new_abilities_urls)} Abilities...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

            type_futures = [executor.submit(fetch_and_validate, url, TypeDetailSchema) for url in new_types_urls]
            for future in as_completed(type_futures):
                res = future.result()
                if res: 
                    valid_types.append(res)

            ability_futures = [executor.submit(fetch_and_validate, url, AbilityDetailSchema) for url in new_abilities_urls]
            for future in as_completed(ability_futures):
                res = future.result()
                if res: 
                    valid_abilities.append(res)

        write_batch_to_lake("types", valid_types)
        write_batch_to_lake("abilities", valid_abilities)
    else:
        logger.info("No new dependencies to extract.")

    logger.info("Extraction complete. Data Lake layer is synchronized.")

if __name__ == "__main__":
    run_extraction()
