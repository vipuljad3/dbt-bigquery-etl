from pathlib import Path
from dagster import definitions, load_from_defs_folder, Definitions
from dagster_duckdb_polars import DuckDBPolarsIOManager
import os
DUCKDBHOST = os.getenv('DB_HOST_NAME', None)
ENV=os.getenv('PROJECT_ENVIRONMENT', None)
@definitions
def defs():
    """Combine Components and Pythonic assets."""
    # 1. Load component definitions from the defs/ folder
    component_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)

    # 2. Create definitions for resources (No assets here to prevent duplicates)
    pythonic_defs = Definitions(
        resources={
            "io_manager": DuckDBPolarsIOManager(database=DUCKDBHOST)
        }
    )

    # 3. Merge component definitions with pythonic definitions
    return Definitions.merge(component_defs, pythonic_defs)