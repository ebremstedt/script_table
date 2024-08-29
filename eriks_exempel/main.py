from google.cloud import bigquery
import json
from data_typing import json_typing, create_schema, table_info

# TODO: Set table_id to the ID of table you wish to create.
table_id = 'project.dataset.table'

with open("./table.json") as f:
        json_data = json.load(f)

column_params = json_typing(json_data=json_data)

schema = create_schema(column_params=column_params)

table = bigquery.Table(table_id, schema=schema)

print(table_info(table=table))

# # Create Client object for API request
# client = bigquery.Client()
# # Make API request
# table = client.create_table(table)