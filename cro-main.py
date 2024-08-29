from google.cloud import bigquery
from datetime import datetime


def python_to_bigquery_type(value):
    "function that checks and returns type for json_data"

    if isinstance(value, str):
        return "STRING"
    elif isinstance(value, int):
        return "INT64"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, bool):
        return "BOOL"
    elif isinstance(value, list):
        return "ARRAY"
    elif isinstance(value, dict):
        return "STRUCT"
    elif value is None:
        return "NULL"
    else:
        return "UNKNOWN" 


json_data = {
    "weather": "humid",
    "name": "Alberta",
    "visibility": 10000,
    "wind": 5.5,
    "clouds": "Overcast",
    "datetime": 1485789600,
    "date": "2024-08-28",
    "id": 2643743,
    "name": "London",
    "cod": 200,
    "isRaining": True
}

if "datetime" in json_data:
    timestamp = json_data["datetime"]
   
    date_object = datetime.utcfromtimestamp(timestamp)
    
    formatted_date = date_object.strftime('%Y-%m-%d')
    
    json_data["datetime"] = formatted_date


schema = []

for key, value in json_data.items():
    bigquery_type = python_to_bigquery_type(value)
 

    if bigquery_type != "UNKNOWN":  
        schema.append(bigquery.SchemaField(key, bigquery_type))


table = bigquery.Table(table_id, schema=schema)




