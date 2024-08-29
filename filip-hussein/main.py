import json
import datetime
from typing import List
from google.cloud import bigquery

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
}
"""

data_types = {
    datetime.date: "DATE",
    datetime.datetime: "DATETIME",
    str: "STRING",
    int: "INTEGER",
    float: "FLOAT64",
    bool: "BOOL"
}


def convert_json(json_data: str) -> dict:
    """Converts a JSON string to a Python dictionary."""
    json_dict = json.loads(json_data)
    return json_dict


def return_bq_type(a_dict: dict) -> dict:
    """Returns a dictionary with column names and respective data types for BigQuery."""
    new_df = {}
    for key, value in a_dict.items():
        try:
            new_df[key] = "DATETIME"
            continue
        except (ValueError, TypeError):
            pass

        try:
            parsed_value = datetime.datetime.strptime(value, "%Y-%m-%d").date()
            new_df[key] = "DATE"
            continue
        except (ValueError, TypeError):
            pass

        for pytype, sqltype in data_types.items():
            if isinstance(value, pytype):
                new_df[key] = sqltype
                break

    return new_df


def generate_bq_schema(schema_dict: dict) -> List[bigquery.SchemaField]:
    """Returns a list of BigQuery SchemaField objects."""
    schema = []
    for column, bq_type in schema_dict.items():
        schema.append(bigquery.SchemaField(column, bq_type))
    return schema


def main():
    json_dict = convert_json(json_data)
    json_dict["datetime"] = str(datetime.datetime.fromtimestamp(json_dict["datetime"]))

    updated_dict = return_bq_type(json_dict)

    client = bigquery.Client()
    schema = generate_bq_schema(updated_dict)

    table_id = "my_table_id"
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)

if __name__ == "__main__":
    main()
