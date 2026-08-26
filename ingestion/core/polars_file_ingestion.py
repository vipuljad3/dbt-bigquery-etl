import polars as pl
import urllib.error


class PolarDataLoader():
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_csv(self, **kwargs) -> pl.DataFrame:
        """Reads a CSV file from the file_path."""
        try:
            return pl.read_csv(self.file_path, **kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV from {self.file_path}. Error: {e}")

    def read_json(self, **kwargs) -> pl.DataFrame:
        """Reads a JSON file from the file_path."""
        try:
            return pl.read_json(self.file_path, **kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to read JSON from {self.file_path}. Error: {e}")

    def read_parquet(self, **kwargs) -> pl.DataFrame:
        """Reads a Parquet file from the file_path."""
        try:
            return pl.read_parquet(self.file_path, **kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet from {self.file_path}. Error: {e}")
