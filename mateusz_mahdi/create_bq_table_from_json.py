from google.cloud import bigquery
from typing import Any, Dict, List

 
json_data = {
    "weather": "humid",
    "name": "Alberta",
    "visibility": 10000,
    "wind": 5.5,
    "clouds": "Overcast",
    "datetime": 1485789600,
    "date": "2024-08-28",
    "id": 2643743,
    "city name": "London",
    "cod": 200,
    "isRaining": True
}

def get_bq_type(value:any):
        if isinstance(value, int):
            return "INT64"
        elif isinstance(value, float):
            return "FLOAT64"
        elif isinstance(value, bool):
            return "BOOL"
        elif isinstance(value, str):
            try:
                from datetime import datetime
                datetime.strptime(value, "%Y-%m-%d")
                return "TIMESTAMP"
            except ValueError:
                pass
            return "STRING"
        elif isinstance(value, int) and 0 <= value <= datetime.now().timestamp():
            try:
                datetime.fromtimestamp(value)
                return True
            except ValueError:
                return False                
        else:
            return "STRING"
 
def convert_json_to_bq_schema(json_data: Dict[str, Any]) -> List[bigquery.SchemaField]:
    """Converts JSON data to a list of SchemaField objects for BigQuery."""
    bq_schema = []
    for key, value in json_data.items():
        bq_schema.append(bigquery.SchemaField(name=key, field_type=get_bq_type(value), mode="NULLABLE"))
    return bq_schema
 
### Test here: ###
schema = convert_json_to_bq_schema(json_data)
print(schema)