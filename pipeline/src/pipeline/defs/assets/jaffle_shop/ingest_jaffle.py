import yaml
import polars as pl
from dagster import Definitions, asset, file_relative_path
import os

ENV = os.getenv("PROJECT_ENVIRONMENT", None)
GROUP_NAME = "JAFFLE_SHOP"


YAML_FILE_PATH = file_relative_path(__file__, "config.yml")
with open(YAML_FILE_PATH, "r", encoding="utf-8") as file:
    YAML_CONFIG = file.read()

config_data = yaml.safe_load(YAML_CONFIG)


def build_ingestion_asset(table_def: dict):
    """
    Creates a Dagster asset for a specific table definition.
    Using a factory function ensures Python doesn't overwrite the loop variables.
    """

    @asset(
        name=table_def["name"],
        io_manager_key = 'io_manager_jaffle_shop',
        key_prefix=table_def["schema"],
        group_name=GROUP_NAME,
        kinds={"polars", "snowflake" if ENV == "prod" else "duckdb"},
        metadata={"database": GROUP_NAME, "schema":'staging'},
    )
    def _dynamic_asset() -> pl.DataFrame:
        return pl.read_csv(table_def["file_path"], **table_def["args"])

    return _dynamic_asset


ingestion_assets = [build_ingestion_asset(table) for table in config_data["tables"]]
