import os
import polars
from pathlib import Path

from ingestion.utils.util import read_yaml
from ingestion.core.polars_file_ingestion import PolarDataLoader
from ingestion.core.polars_to_duckdb_ingestor import DuckDBUploader

env = os.getenv('PROJECT_ENVIRONMENT',None)
DB_HOST_NAME = os.getenv('DB_HOST_NAME',None)

current_dir = Path(__file__).parent.resolve()
configs = read_yaml(os.path.join(current_dir, "config.yml"))

if env == 'test':
    for config in configs['tables']:

        print(config)
        df = PolarDataLoader(config["file_path"]).read_csv(**config["args"])
        DuckDBUploader(DB_HOST_NAME).upload(df, table_name=config["name"], schema=config["schema"], mode="replace")
