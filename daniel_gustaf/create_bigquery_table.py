"""
This script creates a BigQuery table from a JSON object.
Made by Daniel and Gustaf for the group project "Script bigquery table from json data".
"""
from datetime import datetime
from google.cloud import bigquery
from typing import Any, Dict, List
import json


def is_iso_date(value: str) -> bool:
    """Check if the value is an ISO date (YYYY-MM-DD)."""
    try:
        datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return False
    return True


def is_unix_timestamp(value: int) -> bool:
    """Check if the value is a Unix timestamp."""
    if isinstance(value, int) and 0 <= value <= 253402300799:
        try:
            datetime.fromtimestamp(value)
            return True
        except ValueError:
            return False
    return False

        
def infer_bigquery_type(key: str, value: any) -> str:
    """Infer the BigQuery type from a Python value."""
    if isinstance(value, str) and is_iso_date(value):
        return "DATE"
    elif isinstance(value, int) and 'date' in key and is_unix_timestamp(value):
        return "DATETIME"
    elif isinstance(value, bool):
        return "BOOL"
    elif isinstance(value, int):
        return "INT64"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, str):
        return "STRING"
    else:
        raise ValueError(f"Unsupported data type: {type(value)}")
    

def create_bigquery_schema(json_data: Dict[str, Any]) -> List[bigquery.SchemaField]:
    """Create a BigQuery schema from JSON data."""
    schema = []
    for key, value in json_data.items():
        field_type = infer_bigquery_type(key, value)
        schema.append(bigquery.SchemaField(key, field_type))
    return schema


def create_bigquery_table(project_id: str, dataset_id: str, table_id: str, json_data: Dict[str, Any]) -> bigquery.Table:
    """Create a BigQuery table object."""
    schema = create_bigquery_schema(json_data=json_data)
    table_ref = bigquery.TableReference.from_string(f"{project_id}.{dataset_id}.{table_id}")
    table = bigquery.Table(table_ref=table_ref, schema=schema)
    return table


def main():
    project_id = 'project_id'
    dataset_id = 'dataset_id'
    table_id = 'table_id'

    json_data = """{
    "weather": "humid",
    "name": "Alberta",
    "visibility": 10000,
    "wind": 5.5,
    "clouds": "Overcast",
    "datetime": 1485789600,
    "date": "2024-08-28",
    "id": 2643743,
    "cod": 200,
    "isRaining": true
    }"""

    json_data = json.loads(json_data)
    
    table = create_bigquery_table(project_id, dataset_id, table_id, json_data)

    print(f"Table type: {type(table)}")
    print(f"Table ID: {table.table_id}")
    print(f"Dataset ID: {table.dataset_id}")
    print(f"Project: {table.project}")
    print("Schema:")
    for field in table.schema:
        print(f"  - {field.name}: {field.field_type}")
    
    return table


if __name__ == "__main__":
    main()