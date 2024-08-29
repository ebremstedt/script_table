import json
from google.cloud.bigquery import SchemaField
import datetime as dt

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

def unpack_json(json_data:list, timestamp_name:str)->list:

    """"insert json data that you want unpacked as json_data
        insert the column name that contains the unix code."""

    #this unpacks the json format to a dict
    raw_data_dict = json.loads(json_data)


    schema_list = []

    for key, value in raw_data_dict.items():
        if key == timestamp_name:
            #converts unix code to datetime format
            value = dt.datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S UTC")
            schema = SchemaField(key, "TIMESTAMP", mode="REQUIRED")
        
        
        else:   
            if isinstance(value, str):
                value_type = "STRING"
            elif isinstance(value, int):
                value_type = "INTEGER"
            elif isinstance(value, float):
                value_type = "FLOAT"
            elif isinstance(value, bool):
                value_type = "BOOLEAN"
            else:
                value_type = "STRING" 

            schema = SchemaField(key, value_type, mode="REQUIRED")
            print(f"Processed key: {key}")

        schema_list.append(schema)

    return schema_list