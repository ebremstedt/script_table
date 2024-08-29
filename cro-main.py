from google.cloud import bigquery
from datetime import datetime


def python_to_bigquery_type(json_data) -> str:
    "function that checks and returns type for json_data"

    if isinstance(json_data, str):
        return "STRING"
    elif isinstance(json_data, int):
        return "INT64"
    elif isinstance(json_data, float):
        return "FLOAT64"
    elif isinstance(json_data, bool):
        return "BOOL"
    elif isinstance(json_data, list):
        return "ARRAY"
    elif isinstance(json_data, dict):
        return "STRUCT"
    elif json_data is None:
        return "NULL"


json_data = {
    "weather": "humid",
    "name": "Alberta",
    "visibility": 10000,
    "wind": 5.5,
    "clouds": "Overcast",
    "datetime": 1485789600,
    "date": "2024-08-28",
    "id": 2643743,
    "city": "London",
    "cod": 200,
    "isRaining": True
}


def datetime_converter(datetime:int) -> datetime:
    """Converts unix to datetime"""

    if "datetime" in json_data:
        timestamp = json_data["datetime"]
    
        date_object = datetime.utcfromtimestamp(timestamp)
        
        formatted_date = date_object.strftime('%Y-%m-%d')
        
        json_data["datetime"] = formatted_date


def creating_table() -> list:
    """Creating a BigQuery-table from json_data"""

    table_id = "ex_project.ex_dataset.ex_table"

    schema = []

    for key, value in json_data.items():
        bigquery_type = python_to_bigquery_type(value)

        schema.append(bigquery.SchemaField(key, bigquery_type))

    
    table = bigquery.Table(table_id, schema=schema)

    return schema




