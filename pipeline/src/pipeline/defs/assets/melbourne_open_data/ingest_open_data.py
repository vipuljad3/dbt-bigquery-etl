from dagster import asset, AssetExecutionContext, file_relative_path
import polars as pl
from src.pipeline.defs.assets.melbourne_open_data.ingestion.open_data_ingestion import (
    OpenDataAPIClient,
)
import os
import yaml

GROUP_NAME = "open_data"
DATABASE_NAME = GROUP_NAME
ENV = os.getenv("PROJECT_ENVIRONMENT", None)

YAML_FILE_PATH = file_relative_path(__file__, "config.yml")
with open(YAML_FILE_PATH, "r", encoding="utf-8") as file:
    YAML_CONFIG = file.read()
config_data = yaml.safe_load(YAML_CONFIG)

print(YAML_CONFIG)


def build_open_data_ingestion_asset(table_def: dict):
    @asset(
        name=table_def["table_name"],
        io_manager_key="io_manager_melbourne_open_data",
        key_prefix=table_def["schema"],
        group_name=GROUP_NAME,
        kinds={"polars", "python", "snowflake" if ENV == "prod" else "duckdb"},
    )
    def staged_open_data_asset(context: AssetExecutionContext) -> pl.DataFrame:
        """
        Fetches API data and stages it as a Polars DataFrame.
        """
        config = table_def

        database_name = DATABASE_NAME
        client = OpenDataAPIClient(config, database_name, context=context)

        context.log.info(f"Fetching data for {database_name}...")
        df = client.fetch_data()

        context.log.info(
            f"Successfully fetched {df.height} rows. and {df.columns} columns  \n schema {df.schema}"
        )
        return df

    return staged_open_data_asset


ingestion_assets = [
    build_open_data_ingestion_asset(table) for table in config_data["tables"]
]
