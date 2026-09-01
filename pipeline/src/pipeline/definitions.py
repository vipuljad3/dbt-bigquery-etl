from pathlib import Path
from dagster import definitions, load_from_defs_folder, Definitions
from src.pipeline.io_managers import get_resources
import os

ENV = os.getenv("PROJECT_ENVIRONMENT", None)


@definitions
def defs():
    """Combine Components and Pythonic assets."""
    dagster_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
    resources_def = Definitions(resources=get_resources(ENV=ENV))
    return Definitions.merge(dagster_defs, resources_def)
