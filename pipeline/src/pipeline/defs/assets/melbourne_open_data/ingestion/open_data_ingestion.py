import requests
import polars as pl
from datetime import datetime, timedelta
from dateutil.tz import tzutc


class OpenDataAPIClient:
    """
    Client for fetching and processing data from the open data API into a Polars DataFrame.
    """

    NAMESPACE = "open_data"
    TYPE_MAP = {
        "int64": pl.Int64,
        "string": pl.String,  # Uses pl.Utf8 in older Polars versions
        "float64": pl.Float64,
        "list[float64]": pl.List(pl.Float64),
    }

    def __init__(self, config: dict, database: str, context: any):
        self.config = config
        self.database = config.get("name")
        self.url = config["open_api_url"]
        self.columns = config["columns"]
        self.context = context

    def apply_yaml_schema(self, df: pl.DataFrame, columns: dict) -> pl.DataFrame:
        self.context.log.info('Applying yaml schema')
        polars_schema = {}
        columns_mapped = []
        for col in columns:
            if col["name"] in df.columns:
                polars_schema[col["name"]] = self.TYPE_MAP.get(
                    col["data_type"], pl.String
                )
                columns_mapped.append(col["name"])

        self.context.log.info(polars_schema)
        return df.cast(polars_schema, strict=False).select(columns_mapped)

    def fetch_data(self) -> pl.DataFrame:
        """Main method to fetch API data and process into a flat Polars DataFrame."""
        if not self.config.get("lookback", False):
            self.context.log.info('Fetching without lookback')
            df = self._fetch_api_to_df(date=None)
        else:
            lookback_days = self.config.get("lookback_days", 0)
            self.context.log.info(f'Fetching with lookback days {lookback_days}')
            source_date_column = self.config.get("source_date_column")
            df = self._lookback_collect(lookback_days, source_date_column)

        if df.is_empty():
            return df

        # Emulate original `pd.DataFrame(list(df['fields']))` logic:
        # Extracts dictionaries stored in the 'fields' column into top-level DataFrame columns
        if "fields" in df.columns:
            fields_list = df["fields"].to_list()
            df = pl.DataFrame(fields_list)
        df = self.apply_yaml_schema(df=df, columns=self.columns) if self.columns else df
        return df

    def _lookback_collect(
        self, lookback_days: int, source_date_column: str
    ) -> pl.DataFrame:
        """Collects data iteratively over a lookback window."""
        today = datetime.now()
        start_date = today - timedelta(days=lookback_days)

        # Generate list of dates formatted as YYYY/MM/DD
        num_days = (today - start_date).days + 1
        date_list = [
            (start_date + timedelta(days=i)).strftime("%Y/%m/%d")
            for i in range(num_days)
        ]

        print("Looking for these dates: \n", date_list)

        # Accumulating dataframes in a list is highly recommended in Polars before concatenating
        dataframes = []

        for date in date_list:
            batch = self._fetch_api_to_df(
                date=date, source_date_column=source_date_column
            )
            print(f"Batch size for {date}: {batch.height}")
            if not batch.is_empty():
                dataframes.append(batch)

        if not dataframes:
            return pl.DataFrame()

        return pl.concat(dataframes, how="vertical_relaxed")

    def _fetch_api_to_df(
        self, date: str = None, source_date_column: str = None
    ) -> pl.DataFrame:
        """Constructs request parameters and loads JSON into a Polars DataFrame."""
        params = {
            "dataset": self.database,
            "rows": 10000,
            "format": "json",
            "timezone": "UTC",
        }

        if date is not None:
            params["q"] = f"{source_date_column} = {date}"
            params["sort"] = [source_date_column]
        else:
            params["start"] = 0

        print(f"Requesting: {self.url} with params {params}")
        response = requests.get(self.url, params=params)
        response.raise_for_status()

        records = response.json().get("records", [])
        return pl.DataFrame(records, infer_schema_length=None)

    @staticmethod
    def subset_date(
        df: pl.DataFrame, lookback_date_column: str, lookback_days: int
    ) -> pl.DataFrame:
        """
        Utility to filter a Polars DataFrame dynamically based on a lookback window.
        Assumes the target date column is already cast to a Polars Datetime type.
        """
        now = datetime.now(tzutc())
        subset_date = now - timedelta(days=lookback_days)

        filtered_df = df.filter(pl.col(lookback_date_column) >= subset_date)

        if filtered_df.is_empty():
            return None
        return filtered_df
