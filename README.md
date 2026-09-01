# 🚀 dbt-dagster-etl

[![Dagster](https://img.shields.io/badge/Dagster-Orchestration-blue?style=for-the-badge&logo=dagster)](https://dagster.io/)
[![dbt](https://img.shields.io/badge/dbt-Data_Transformation-FF694B?style=for-the-badge&logo=dbt)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/Python-3.9%2B-yellow?style=for-the-badge&logo=python)](https://www.python.org/)

A robust, code-first Data Engineering pipeline that integrates **dbt (data build tool)** for SQL-based data transformations with **Dagster** for data-aware orchestration. 

## 📖 Overview

This repository houses an End-to-End (E2E) ETL/ELT pipeline. It is designed to extract raw data, load it into a data warehouse, and transform it into analytics-ready models. 

Unlike traditional task-based orchestration (like Airflow), this project leverages Dagster's **Software-Defined Assets (SDAs)**. This allows us to orchestrate the pipeline based on the actual data assets being produced (tables, views, ML models) rather than arbitrary tasks. Dagster seamlessly reads the `manifest.json` from dbt, automatically translating dbt models, seeds, and snapshots into a dependency graph of Dagster assets.

### 🏗️ Architecture

1. **Extract & Load (EL):** Dagster custom assets/resources fetch data from source APIs/databases and load them into the raw layer of the Data Warehouse.
2. **Transform (T):** dbt takes over inside the warehouse, cleaning, joining, and aggregating the data through `staging`, `intermediate`, and `marts` layers.
3. **Orchestration:** Dagster orchestrates the entire lifecycle, managing upstream dependencies, triggering dbt runs, and capturing metadata/logs.

---

## 🛠️ Key Technologies

* **[Dagster](https://dagster.io/):** Asset-based orchestrator used for scheduling, dependency management, and monitoring.
* **[dbt (Core)](https://docs.getdbt.com/):** Compiles and runs modular SQL transformations in the data warehouse.
* **Python 3.9+:** Used for Dagster definitions, custom ingest scripts, and API interactions.
* **[Your Data Warehouse]**: (e.g., Snowflake / BigQuery / PostgreSQL) — *Update this with your specific DWH.*

---

## 📂 Project Structure

```text
dbt-dagster-etl/
├── dbt_project/                # dbt project root
│   ├── models/                 # SQL transformation models
│   │   ├── staging/            # Raw data cleanup and type casting
│   │   ├── intermediate/       # Business logic and complex joins
│   │   └── marts/              # Final analytics-ready dimensional models
│   ├── macros/                 # Reusable SQL macros (Jinja)
│   ├── tests/                  # Custom singular dbt tests
│   ├── dbt_project.yml         # dbt configuration file
│   └── profiles.yml            # Connection profiles (kept local, ignored in git)
├── dagster_etl/                # Dagster orchestration root
│   ├── assets/                 # Software-defined assets (Python & dbt)
│   ├── resources/              # External connections (DB connections, API clients)
│   ├── schedules/              # Cron schedules for pipeline runs
│   ├── sensors/                # Event-driven triggers
│   └── __init__.py             # Dagster repository definition
├── pyproject.toml              # Python dependencies and metadata
├── workspace.yaml              # Dagster workspace configuration
├── requirements.txt            # Python package requirements
└── README.md
⚙️ Local Development Setup
Prerequisites
Python 3.9 or higher

dbt-core and your specific database adapter (e.g., dbt-postgres, dbt-snowflake)

A working connection to your target data warehouse.

1. Clone the Repository
Bash
git clone [https://github.com/vipuljad3/dbt-dagster-etl.git](https://github.com/vipuljad3/dbt-dagster-etl.git)
cd dbt-dagster-etl
2. Set Up a Virtual Environment
It's highly recommended to isolate dependencies.

Bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
3. Install Dependencies
Install the required packages, including Dagster, dbt, and the dagster-dbt integration library.

Bash
pip install -r requirements.txt
4. Configure dbt Profile
Create or update your profiles.yml file (usually located in ~/.dbt/profiles.yml or the project root). Ensure it matches the profile name specified in dbt_project/dbt_project.yml.

YAML
# Example profiles.yml
dbt_project_name:
  target: dev
  outputs:
    dev:
      type: postgres # Change to your DWH
      host: localhost
      user: my_user
      password: my_password
      port: 5432
      dbname: my_db
      schema: dev_schema
      threads: 4
5. Compile dbt Models
Dagster relies on dbt's compiled manifest.json file to build the asset graph. You must compile the project before launching the Dagster UI.

Bash
cd dbt_project
dbt deps
dbt compile
cd ..
🚀 Running the Pipeline
Starting the Dagster UI
To visualize the pipeline, view logs, and trigger manual runs, start the Dagster development server:

Bash
dagster dev -w workspace.yaml
Navigate to http://localhost:3000 in your browser.

Click on Assets in the top navigation bar to see your dbt models visualized as a DAG.

Click Materialize All to run the end-to-end pipeline.

Interacting with dbt via CLI
You can still use standard dbt commands for rapid SQL development without spinning up the orchestrator:

Bash
cd dbt_project
dbt run -s +my_model+   # Run a model and its upstream/downstream dependencies
dbt test                # Run data quality tests
🧠 Deep Dive: How Dagster Integrates with dbt
This project utilizes the dagster-dbt library to bridge the gap between Python orchestration and SQL transformations.

Asset Loading: In dagster_etl/assets/dbt_assets.py, we use the @dbt_assets decorator pointing to our dbt manifest.json.

Upstream Python Assets: We define Python-based extraction assets (e.g., pulling data from an API) and link them to dbt sources using the DagsterDbtTranslator. This ensures dbt waits for the Python extraction to finish before running SQL transformations.

Metadata Collection: When Dagster runs a dbt model, it captures dbt's metadata (run times, row counts, test results) and displays it directly in the Dagster UI.

🧪 Testing & Linting
Data Quality Testing:
Data quality is enforced using dbt's native testing framework. Tests are defined in the schema.yml files alongside the models.

Generic Tests: unique, not_null, accepted_values, relationships.

Singular Tests: Custom SQL queries located in dbt_project/tests/.

Code Quality:

SQL formatting: Recommended to use SQLFluff.

Python linting: Configured with flake8 and black.

🚢 Deployment
To deploy this pipeline to a production environment (e.g., Dagster Cloud, AWS ECS, or Kubernetes):

Containerization: A Dockerfile should be used to package the dbt models and Python code together.

CI/CD: Set up GitHub Actions (in .github/workflows/) to automatically run dbt test and update the Dagster code location on every push to the main branch.

Environment Variables: Ensure all database credentials and API keys are injected securely via your deployment platform's secrets manager, rather than hardcoded in profiles.yml.

🤝 Contributing
Fork the project.

Create your Feature Branch (git checkout -b feature/AmazingFeature).

Commit your Changes (git commit -m 'Add some AmazingFeature').

Push to the Branch (git push origin feature/AmazingFeature).

Open a Pull Request.

Maintained by Vipul Jad