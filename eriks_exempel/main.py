from eriks_exempel.create_schema import create_schema, json_data, create_table
import json

client = "hej"
dataset_id = "läget?"
table_id = "sup!"
def main():
     raw_data_dict = json.loads(json_data)
     schema_list = create_schema(json_data=raw_data_dict, timestamp_name='timestamp')
     create_table(client, dataset_id, table_id, schema_list)   

if __name__ == "__main__":
    main()