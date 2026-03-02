import duckdb
import logging
from typing import List

from src.database import get_db_connection, init_raw_tables

# Configure module logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Pipeline Constants
LAKE_BASE_PATH = "data/raw"
ENTITIES_TO_LOAD: List[str] = ["pokemon", "types", "abilities"]

def bulk_load_delta(con: duckdb.DuckDBPyConnection, entity_type: str) -> None:
    """
    Executes a delta load from the Data Lake (JSONL files) into the Raw database layer.

    This operates as an append-only ledger. It dynamically reads all partition directories
    and uses DuckDB's native 'filename=true' to track lineage and prevent duplicate ingestion
    of files we have already processed.

    Args:
        con (duckdb.DuckDBPyConnection): Active DuckDB connection.
        entity_type (str): The logical entity name (e.g., 'pokemon').
    """
    # Glob pattern to capture all Hive-partitioned files for the specific entity
    glob_path = f"{LAKE_BASE_PATH}/{entity_type}/*/*/*/*.jsonl"

    logger.info(f"Scanning Data Lake for new {entity_type} files...")

    try:
        # Pushing down the JSON parsing and Delta logic entirely into DuckDB's C++ engine.
        # This is orders of magnitude faster than parsing JSON iteratively in Python.
        query = f"""
            INSERT INTO raw.{entity_type}
            SELECT 
                (json->>'id')::INTEGER AS id,
                json AS raw_payload,
                filename AS source_file,
                CURRENT_TIMESTAMP AS loaded_at
            FROM read_json_objects('{glob_path}', filename=true, format='newline_delimited')
            WHERE filename NOT IN (SELECT source_file FROM raw.{entity_type})
        """

        # Execute the load and capture the insertion count
        result = con.execute(query)
        rows_inserted = result.fetchone()[0]

        if rows_inserted > 0:
            logger.info(f"Successfully appended {rows_inserted} new records into raw.{entity_type}.")
        else:
            logger.info(f"No new records to load for {entity_type}. Raw table is up to date.")

    except duckdb.IOException:
        # This is expected behavior if the pipeline is run before any data is extracted
        logger.warning(f"No Data Lake files found for {entity_type} at {glob_path}.")
    except Exception as e:
        logger.error(f"Failed to bulk load {entity_type} into the Raw layer: {e}")
        raise

def run_load() -> None:
    """
    Main orchestration logic for the Load phase (Data Lake -> Raw Layer).
    Ensures safe database connection management and sequential entity loading.
    """
    logger.info("Initializing Load Phase (Data Lake -> Raw)...")

    # Establish connection safely
    try:
        con = get_db_connection()

        # Ensure the idempotent Raw DDL has been executed before attempting loads
        init_raw_tables(con)

        # Sequentially load all defined entities
        for entity in ENTITIES_TO_LOAD:
            bulk_load_delta(con, entity)

        logger.info("Load complete. Raw Layer is fully synchronized.")

    except Exception as e:
        logger.error(f"Critical failure during Load phase: {e}")
        raise
    finally:
        # Guarantee the DuckDB process lock is released, even if the load crashes
        if 'con' in locals():
            con.close()
            logger.debug("Database connection safely closed.")

if __name__ == "__main__":
    run_load()
