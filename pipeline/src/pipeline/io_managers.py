from dagster_snowflake_polars import SnowflakePolarsIOManager
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster import EnvVar

def get_resources(ENV):
    if ENV != "prod":
        return {
            "io_manager": DuckDBPolarsIOManager(database=EnvVar("duckdb_file_path"))
        }
    elif ENV == "prod":
        return {
            "io_manager_jaffle_shop": SnowflakePolarsIOManager(
                account=EnvVar("showflake_account"),
                user=EnvVar("showflake_user"),
                password=EnvVar("showflake_password"),
                schema="staging",
                role=EnvVar("showflake_role"),
                database="JAFFLE_SHOP",  # Target Snowflake Database
                warehouse=EnvVar("showflake_warehouse"),  # Target Snowflake Warehouse
            ),
            "io_manager_melbourne_open_data": SnowflakePolarsIOManager(
                account=EnvVar("showflake_account"),
                user=EnvVar("showflake_user"),
                password=EnvVar("showflake_password"),
                schema="staging",
                role=EnvVar("showflake_role"),
                database="open_data",  # Target Snowflake Database
                warehouse=EnvVar("showflake_warehouse"),  # Target Snowflake Warehouse
            ),
        }
    else:
        raise ("invalid environment.")
