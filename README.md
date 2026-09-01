# dbt-dagster-etl

[![Python](https://img.shields.io/badge/Python-3.9%2B-yellow?style=for-the-badge&logo=python)](https://www.python.org/)
[![snowflake](https://img.shields.io/badge/SNOWFLAKE-Data_Warehouse_Prod-FF1111?style=for-the-badge&logo=snowflake)](https://www.getdbt.com/)
[![duckdb](https://img.shields.io/badge/DUCKDB-Data_Warehouse_Test-FG1111?style=for-the-badge&logo=duckdb)](https://www.getdbt.com/)
[![Dagster](https://img.shields.io/badge/Dagster-Orchestration-blue?style=for-the-badge&logo=dagster)](https://dagster.io/)
[![dbt](https://img.shields.io/badge/dbt-Data_Transformation-FF694B?style=for-the-badge&logo=dbt)](https://www.getdbt.com/)

A reusable **framework** for building ELT pipelines: ingest raw data with **Polars**, land it in a warehouse (**DuckDB** locally / **Snowflake** in production), transform it with **dbt**, and orchestrate everything with **Dagster**. `jaffle_shop` is included as the first working example — the goal of this repo is to make it easy to add new, similarly-structured "source projects" alongside it.

## Overview

The framework follows a consistent ELT pattern for every source project:

```
raw files (CSV/JSON/Parquet/API) --> Polars ingestion --> staging schema --> dbt (silver) --> dbt (gold)
                                                                    |
                                                        Dagster orchestrates it all,
                                                        DuckDB (test) / Snowflake (prod)
```

Every new data source is added as its **own project**: a config-driven ingestion module plus a dbt subproject, both plugged into the shared Dagster pipeline and the shared DuckDB/Snowflake resources. `jaffle_shop` is the reference implementation of this pattern.

## Repository Structure

```
dbt-dagster-etl/
├── dbt/
│   └── <project_name>/                    # One dbt project per source (e.g. jaffle_shop)
│       ├── dbt_project.yml                # silver -> table, gold -> table conventions
│       ├── profiles.yml                   # duckdb (test) / snowflake (prod) targets
│       ├── macros/
│       │   └── generate_schema_name.sql   # Shared macro: no schema prefixing
│       ├── models/
│       │   ├── sources/sources.yml        # Declares the `staging` source tables
│       │   ├── silver/*.sql               # 1:1 cleaned/typed views over sources
│       │   └── gold/*.sql                 # Business-facing dimensional/fact models
│       └── seeds/, snapshots/, tests/, analyses/, target/
│
├── ingestion/                              # Standalone Python ingestion library
│   ├── core/                               # Shared, source-agnostic ingestion utilities
│   │   ├── polars_file_ingestion.py        # PolarDataLoader: CSV/JSON/Parquet via Polars
│   │   ├── polars_to_duckdb_ingestor.py    # DuckDBUploader: DataFrame -> DuckDB schema.table
│   │   └── bigquery_ingestor.py            # Placeholder for a future BigQuery loader
│   ├── utils/
│   │   └── util.py                         # read_yaml() helper
│   └── <project_name>/                     # One folder per source project
│       ├── config.yml                      # table -> source file/URL mapping
│       └── ingest_<project_name>.py        # Script: config -> Polars -> DuckDB
│
├── pipeline/                                # Dagster project (orchestration layer)
│   ├── pyproject.toml                       # Shared deps: Dagster, dbt, DuckDB, Snowflake, Polars
│   ├── .env.example                         # Template for required environment variables
│   ├── src/pipeline/
│   │   ├── definitions.py                   # Picks DuckDB or Snowflake IO manager by env; merges all defs
│   │   └── defs/
│   │       ├── assets/
│   │       │   └── <project_name>/          # One folder per source project
│   │       │       ├── config.yml           # Same table -> source mapping, used by Dagster assets
│   │       │       └── ingest_<project>.py  # Dynamically builds one Dagster asset per table
│   │       └── pipeline/
│   │           └── defs.yaml                # Registers each dbt project as a Dagster Component
│   └── tests/
│
└── LICENSE
```

The `<project_name>` folders under `dbt/`, `ingestion/`, and `pipeline/src/pipeline/defs/assets/` are where each new source project lives. `jaffle_shop` currently occupies all three.

## The Framework, Layer by Layer

### 1. Ingestion assets (config-driven, one asset per table)
`pipeline/src/pipeline/defs/assets/<project_name>/ingest_<project_name>.py` reads a `config.yml` listing every raw table for that project:

```yaml
tables:
  - name: customers
    schema: staging
    file_path: https://.../raw_customers.csv
    args:
      has_header: True
      infer_schema_length: 0
```

A factory function (`build_ingestion_asset`) turns each entry into a Dagster `@asset` that reads the file with Polars and returns a `pl.DataFrame`. Dagster's configured I/O manager then persists it to the warehouse under the table's `schema`. This means **adding a new table to a project is just adding an entry to `config.yml`** — no new asset code required.

### 2. dbt transformations (silver → gold)
Each project's dbt models follow the same convention:
- **`models/sources/sources.yml`** declares the raw `staging` tables ingestion produces.
- **`models/silver/*.sql`** — one model per source table, materialized as tables. Start as passthroughs (`select * from {{ source(...) }}`) and layer in cleaning/typing/dedup logic here.
- **`models/gold/*.sql`** — business-facing dimensional/fact models built from silver models via `{{ ref(...) }}`.

Each dbt project is registered with Dagster as a `dagster_dbt.DbtProjectComponent` in `pipeline/src/pipeline/defs/pipeline/defs.yaml`, so `dbt build` shows up as Dagster assets in the same lineage graph as ingestion — one graph per project, all visible in the Dagster UI.

### 3. Shared orchestration & environments
`pipeline/src/pipeline/definitions.py` is shared across **all** projects:
- Reads `PROJECT_ENVIRONMENT` (`test` or `prod`) once and picks the I/O manager for everything:
  - **test**: `DuckDBPolarsIOManager` → local DuckDB file (`duckdb_file_path`)
  - **prod**: `SnowflakePolarsIOManager` → Snowflake, configured from `showflake_*` env vars
- `load_from_defs_folder` auto-discovers every project's assets and dbt component under `defs/`, and `Definitions.merge()` combines them with the shared resources into one Dagster `Definitions` object.
- Each dbt project's `profiles.yml` mirrors the same test/prod split, so dbt always targets the same warehouse Dagster is writing to.

New projects don't need their own `definitions.py` or resource wiring — they inherit the shared I/O manager just by living under `defs/assets/<project_name>/` and `defs/pipeline/`.

### 4. Standalone ingestion (optional, non-Dagster path)
`ingestion/<project_name>/ingest_<project_name>.py` provides a way to run the same ingestion outside Dagster (useful for quick backfills or debugging), reusing the shared `PolarDataLoader` and `DuckDBUploader` classes from `ingestion/core/`.

## Worked Example: `jaffle_shop`

- **Source:** [dbt-labs/jaffle-shop-data](https://github.com/dbt-labs/jaffle-shop-data) — `customers`, `items`, `orders`, `products`, `stores`, `supplies`.
- **Silver:** straight passthroughs of each source table.
- **Gold:** `dim_customers` — joins `customers` to aggregated `orders` (first order date, most recent order date, order count).

This project's files exist purely as the template to copy when adding a new one.

## How to Add a New Source Project

Say you want to add a project called `northwind`. The framework only asks you to touch config and dbt SQL — no orchestration code.

1. **Ingestion config** — create `pipeline/src/pipeline/defs/assets/northwind/config.yml`:
   ```yaml
   tables:
     - name: orders
       schema: staging
       file_path: https://.../northwind_orders.csv
       args:
         has_header: True
   ```
2. **Ingestion asset factory** — copy `ingest_jaffle.py` to `pipeline/src/pipeline/defs/assets/northwind/ingest_northwind.py`, updating the `group_name` to `"northwind"` (the loop over `config_data["tables"]` needs no other changes).
3. **(Optional) standalone script** — mirror the same `config.yml` under `ingestion/northwind/` and copy `ingest_jaffle_shop.py` to `ingestion/northwind/ingest_northwind.py`.
4. **dbt project** — create `dbt/northwind/` with the same layout as `dbt/jaffle_shop/` (`dbt_project.yml`, `profiles.yml`, `macros/generate_schema_name.sql`, `models/sources/sources.yml`, `models/silver/*.sql`, `models/gold/*.sql`). Point `sources.yml` at the `staging` tables your new ingestion assets produce.
5. **Register the dbt project with Dagster** — add a new component file (e.g. `pipeline/src/pipeline/defs/northwind_pipeline/defs.yaml`):
   ```yaml
   type: dagster_dbt.DbtProjectComponent
   attributes:
     project: '{{ context.project_root }}/../dbt/northwind'
     translation:
       group_name: northwind
       description: "Northwind"
   ```
6. **Run it** — `uv run dg dev` from `pipeline/`; the new `northwind` asset group appears alongside `jaffle_shop` automatically, sharing the same DuckDB/Snowflake resources.

No changes are needed to `definitions.py`, the shared `ingestion/core/` utilities, or environment variables — those are project-agnostic by design.

## Prerequisites

- Python 3.10–3.14
- [uv](https://docs.astral.sh/uv/) (the `pipeline` project uses `uv.lock`/`pyproject.toml`)
- DuckDB (bundled via `duckdb`/`dbt-duckdb` Python packages — no separate install needed)
- A Snowflake account, if running against `prod`

## Setup

### 1. Install the pipeline (Dagster) project
```bash
cd pipeline
uv sync
```

### 2. Configure environment variables
```bash
cp .env.example .env
```
```ini
# Project Environment: test | prod
PROJECT_ENVIRONMENT=test

# Snowflake config [prod] — shared across all projects
showflake_account=xxx
showflake_user=xxx
showflake_password=xxx
showflake_role=xxx
showflake_database=xxx
showflake_warehouse=xxx
showflake_schema=xxx

# DuckDB config [test] — shared across all projects
duckdb_file_path=/absolute/path/to/local.duckdb
```
These variable names are consumed by both `pipeline/src/pipeline/definitions.py` and every project's `dbt/<project_name>/profiles.yml`.

### 3. Run Dagster locally
```bash
cd pipeline
uv run dg dev
# or: uv run dagster dev
```
Every registered project appears as its own asset group in the Dagster UI. Materializing a group runs ingestion first, then dbt (silver, then gold) in lineage order.

### 4. (Alternative) Run standalone ingestion without Dagster
```bash
cd ingestion
export PROJECT_ENVIRONMENT=test
export DB_HOST_NAME=/absolute/path/to/local.duckdb
python -m jaffle_shop.ingest_jaffle_shop   # or any other <project_name>
```

### 5. Run a single project's dbt models directly
```bash
cd dbt/jaffle_shop   # or any other <project_name>
export PROJECT_ENVIRONMENT=test
export duckdb_file_path=/absolute/path/to/local.duckdb
dbt build
```

## Switching to Production (Snowflake)

Set `PROJECT_ENVIRONMENT=prod` and populate the `showflake_*` variables once — every project's Dagster assets and dbt `prod` target pick this up automatically, no per-project code changes required.

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | [Dagster](https://dagster.io/) (`dagster`, `dagster-dbt`, `dagster-duckdb-polars`, `dagster-snowflake-polars`) |
| Transformation | [dbt](https://www.getdbt.com/) (`dbt-duckdb`, `dbt-snowflake`) |
| Data processing | [Polars](https://pola.rs/) |
| Local warehouse | [DuckDB](https://duckdb.org/) |
| Production warehouse | [Snowflake](https://www.snowflake.com/) |
| Package/dependency management | [uv](https://docs.astral.sh/uv/) |

## Conventions to Keep When Adding Projects

- **Schemas:** raw ingestion lands in `staging`; dbt builds `silver` (cleaned, 1:1 with sources) and `gold` (business-facing) as tables, per `dbt_project.yml`.
- **Config-driven ingestion:** every table is declared once in a project's `config.yml` (`name`, `schema`, `file_path`, `args`); asset code should never hardcode table-specific logic — extend `args`/config instead.
- **One asset group per project**, named after the project, so the Dagster UI stays organized as more projects are added.
- **Shared resources only:** don't create project-specific I/O managers or `definitions.py` files — everything should flow through the single shared `pipeline/src/pipeline/definitions.py`.
- **Env var naming:** Snowflake variables are spelled `showflake_*` (not `snowflake_*`) throughout the codebase — keep this consistent in new projects to match `definitions.py` and `profiles.yml`.

## Known Gaps / Ideas for Improvement

- `ingestion/core/bigquery_ingestor.py` is an empty stub — a future ingestion target beyond files/DuckDB.
- Silver models are currently pure passthroughs — dbt tests, seeds, and snapshots folders are scaffolded but empty across projects.
- The ingestion `config.yml` is currently duplicated between `ingestion/<project>/` and `pipeline/src/pipeline/defs/assets/<project>/` — consolidating to one source of truth would reduce drift as more projects are added.

## License

See [LICENSE](./LICENSE).
