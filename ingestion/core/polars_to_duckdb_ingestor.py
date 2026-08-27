import duckdb
import polars as pl
from typing import Literal

class DuckDBUploader:
    """
    A utility class to write a Polars DataFrame into a DuckDB database.
    """
    def __init__(self, db_path: str = ":memory:"):
        """
        Initializes the uploader.
        
        Args:
            db_path: Path to the DuckDB file (e.g., 'my_data.duckdb'). 
                     Defaults to ':memory:' for an in-memory database.
        """
        self.db_path = db_path

    def upload(
        self, 
        df: pl.DataFrame, 
        schema:str,
        table_name: str, 
        mode: Literal["replace", "append", "fail"] = "replace"
    ) -> None:
        """
        Writes the Polars DataFrame to DuckDB.
        
        Args:
            df: The Polars DataFrame to upload.
            table_name: The name of the table to create or insert into.
            mode: 'replace' (drops existing table), 'append' (adds rows), 
                  or 'fail' (raises an error if table exists).
        """
        # Connect to DuckDB using a context manager to ensure safe cleanup
        with duckdb.connect(self.db_path) as conn:
            
            # Register the Polars DataFrame as a virtual table in DuckDB
            # This is a zero-copy operation (extremely fast)
            conn.register("temp_df", df)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            try:
                if mode == "replace":
                    conn.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name}  AS SELECT * FROM temp_df")
                
                elif mode == "append":
                    # Ensure table exists first with the correct schema, then insert
                    conn.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} AS SELECT * FROM temp_df LIMIT 0")
                    conn.execute(f"INSERT INTO {schema}.{table_name}  SELECT * FROM temp_df")
                
                elif mode == "fail":
                    # Will throw a duckdb.CatalogException if the table already exists
                    conn.execute(f"CREATE TABLE {schema}.{table_name}  AS SELECT * FROM temp_df")
                
                else:
                    raise ValueError(f"Invalid mode: '{mode}'. Use 'replace', 'append', or 'fail'.")
            
            finally:
                # Clean up the virtual table reference
                conn.unregister("temp_df")

