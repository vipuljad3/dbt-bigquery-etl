from pathlib import Path
from dagster import definitions, load_from_defs_folder, Definitions, EnvVar
from dagster_snowflake_polars import SnowflakePolarsIOManager
from dagster_duckdb_polars import DuckDBPolarsIOManager
import os
DUCKDBHOST = os.getenv('duckdb_file_path', None)
ENV=os.getenv('PROJECT_ENVIRONMENT', None)

def get_resources():
    if ENV != 'prod':
        return {"io_manager": DuckDBPolarsIOManager(database=DUCKDBHOST)}
    elif ENV=='prod':
        return {"io_manager": SnowflakePolarsIOManager(
                    account=EnvVar("showflake_account"),
                    user=EnvVar("showflake_user"),
                    password=EnvVar("showflake_password"),
                    schema=EnvVar("showflake_schema"),
                    role=EnvVar("showflake_role"),
                    database=EnvVar("showflake_database"),       # Target Snowflake Database
                    warehouse=EnvVar("showflake_warehouse"),       # Target Snowflake Warehouse
                )}
    else:
        raise('invalid environment.')

@definitions
def defs():
    """Combine Components and Pythonic assets."""
    # 1. Load component definitions from the defs/ folder
    component_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)

    # 2. Create definitions for resources (No assets here to prevent duplicates)
    pythonic_defs = Definitions(
        resources= get_resources()
    )

    # 3. Merge component definitions with pythonic definitions
    return Definitions.merge(component_defs, pythonic_defs)