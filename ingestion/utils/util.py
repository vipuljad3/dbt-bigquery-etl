import yaml
from pathlib import Path
from typing import Any, Dict

def read_yaml(file_path: str | Path) -> Dict[str, Any]:
    """
    Reads a YAML file and returns its contents as a Python dictionary.
    """
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"The file {file_path} does not exist.")
        
    with open(path, 'r', encoding='utf-8') as file:
        try:
            # Always use safe_load to prevent security vulnerabilities
            data = yaml.safe_load(file)
            return data if data is not None else {}
        except yaml.YAMLError as exc:
            raise ValueError(f"Error parsing YAML file: {exc}")
        