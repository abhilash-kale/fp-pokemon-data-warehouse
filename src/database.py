import duckdb
import os
import logging
from typing import Set

# Configure module logger
logger = logging.getLogger(__name__)

# Pipeline Constants
DB_PATH = "data/pokemon_dw.duckdb"
RAW_SCHEMA = "raw"
RAW_ENTITIES = ["pokemon", "types", "abilities"]

def get_db_connection() -> duckdb.DuckDBPyConnection:
    """
    Initializes the local directory structure and establishes a connection to the DuckDB database.

    Returns:
        duckdb.DuckDBPyConnection: An active database connection. 
        Note: The caller is responsible for safely closing this connection.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Connect to DuckDB (creates the file if it doesn't exist locally)
    return duckdb.connect(DB_PATH)

def init_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    """
    Idempotent DDL execution to ensure the Raw layer schema and tables exist.
    This prepares the database to receive and store raw JSON payloads.

    Args:
        con (duckdb.DuckDBPyConnection): Active DuckDB connection.
    """
    logger.info(f"Initializing Raw layer schema: '{RAW_SCHEMA}'")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")

    # Dynamically generate tables for all tracked entities
    for entity in RAW_ENTITIES:
        # We store the raw JSON payload alongside metadata (source_file, loaded_at)
        # to guarantee full data lineage and traceability back to the exact API response.
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{entity} (
                id INTEGER,
                raw_payload JSON,
                source_file VARCHAR,
                loaded_at TIMESTAMP
            )
        """
        con.execute(ddl)

    logger.info(f"Raw layer tables {RAW_ENTITIES} successfully initialized.")

def get_db_watermark(con: duckdb.DuckDBPyConnection, table: str) -> Set[int]:
    """
    Retrieves a set of already ingested IDs for a given entity to enable incremental extraction.

    Args:
        con (duckdb.DuckDBPyConnection): Active DuckDB connection.
        table (str): The name of the table to query.

    Returns:
        Set[int]: A set of unique IDs currently present in the raw table.
    """
    try:
        # Fetch existing IDs to act as a state watermark for incremental runs
        query = f"SELECT DISTINCT id FROM {RAW_SCHEMA}.{table}"
        results = con.execute(query).fetchall()
        return set(row[0] for row in results)

    except duckdb.CatalogException:
        # If the table or schema doesn't exist yet (e.g., the very first pipeline run),
        # we catch the DuckDB exception and gracefully return an empty set.
        logger.debug(f"Table {RAW_SCHEMA}.{table} not found. Defaulting to empty watermark.")
        return set()
    except Exception as e:
        logger.error(f"Failed to fetch watermark for {RAW_SCHEMA}.{table}: {e}")
        raise
