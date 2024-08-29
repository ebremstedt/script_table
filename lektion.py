import datetime
import json
from typing import Dict, Any

json_data = """{
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
    "isRaining": true
}"""

def convert_datatype(value: Any) -> str:
    translation = {
        str: "STRING",
        int: "INT64",
        bool: "BOOLEAN",
        datetime.datetime: "DATETIME",
        datetime.date: "DATE",
        float: "FLOAT64"
    }

    datatype = type(value)
    return translation.get(datatype, "UNKNOWN")

def create_table(data: str) -> str:
    data_dict = json.loads(data)

    sql_columns = []
    for key, value in data_dict.items():
        sql_value = f'{key} {convert_datatype(value)}'
        sql_columns.append(sql_value)

    sql_columns_list = ", ".join(sql_columns)
    create_bigquery_table = f'CREATE TABLE Weatherdata ({sql_columns_list})'
    return create_bigquery_table

result = create_table(json_data)
print(result)
