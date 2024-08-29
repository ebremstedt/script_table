import json
from datetime import datetime
from typing import Any, Dict
from google.cloud import bigquery
from typing import List

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

# ändrar till bigquery typer baserad på type av värdet
def get_bigquery_type(value:any)->str:
    if isinstance(value,str):
        return "STRING"
    elif isinstance(value,bool):
        return "BOOL"
    elif isinstance(value,int):
        return "INT64"
    elif isinstance(value,float):
        return "FLOAT64"
    elif isinstance(value,float):
        return "FLOAT64"
    elif isinstance(value, datetime):
        return "DATETIME"
    else:
        raise TypeError(f"Unsupported data type: {type(value)}")

# ändrar unix tid till datetime:
def convert_unix_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value)

# ändrar str till datetime
def convert_str_to_datetime(value:str)->datetime:
    return datetime.strptime(value,"%Y-%m-%d")

data = json.loads(json_data)
items=data.items()


def generate_bigquery_table(data:Dict[str,any],table_id:str)->bigquery.Table:

    schema = []

    for key,value in items:
        if isinstance(value,int) and key.lower()=='datetime':
            value = convert_unix_to_datetime(value)
        if isinstance(value,str) and key.lower() == 'date':
            value = convert_str_to_datetime(value)
        
        # type_value = type(value)
        # print(f'{key}:{type_value}')

        bigquery_type = get_bigquery_type(value)
        schema.append(bigquery.SchemaField(key,bigquery_type))

    table = bigquery.Table(table_id, schema=schema)
    return table


table_id = "my_project.my_dataset.my_table"
table = generate_bigquery_table(data,table_id)
schema = table.schema


# göra om table till sql command:
def generate_create_table_sql(table_id:str,schema:List[bigquery.SchemaField])->str:
    """
    Generates a SQL CREATE TABLE command based on the provided table ID and schema.

    Args:
        table_id (str): The ID of the table in BigQuery (e.g., `project_id.dataset_id.table_name`).
        schema (List[bigquery.SchemaField]): A list of BigQuery SchemaField objects defining the table schema.

    Returns:
        str: A SQL command to create the table in BigQuery.
    """
    sql_command = f"CREATE TABLE `{table_id}` (\n"

    for field in schema:
        sql_command += f"  {field.name} {field.field_type},\n"

    # tar bort kommatecken:
    sql_command = sql_command.rstrip(",\n") + "\n);"

    return sql_command

command_line = generate_create_table_sql(table_id,schema)
print('')
print(command_line)