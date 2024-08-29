from google.cloud import bigquery
from dateutil.parser import parse
from datetime import datetime
from typing import Any, Dict

def is_date(value: str):
    '''
    Checks whether string has date format.
    '''
    try:
        parse(value)
        return True
    except ValueError:
        return False

def datatype_to_gbq(name: str, value: Any) -> str:
    '''
    Returns bigquery datatype of value.
    '''
    if isinstance(value, int):
        if 'date' in str(name):
            return 'DATETIME'
        else:
            return 'INT64'
    elif isinstance(value, float):
        return 'FLOAT64'
    elif isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, str):
        if is_date(value):
            return 'DATE'
        else:
            return 'STRING'
    else:
        raise ValueError

def json_typing(json_data: dict) -> Dict[str, dict]:
    '''
    Returns bigquery column parameters including datatype from json dictionary.
    '''
    column_params = {key:{'datatype':None, 'mode':None} for key in json_data.keys()}

    for key, value in json_data.items():

        if str(key).upper() in ['ID', 'NAME', 'DATE']: # <- .upper() to catch 'id', 'Id', 'ID' and 'iD'.
            column_params[key]['mode'] = 'REQUIRED'
        else:
            column_params[key]['mode'] = 'NULLABLE' # Default value for mode

        column_params[key]['datatype'] = datatype_to_gbq(key, value)

    return column_params

def create_schema(column_params: Dict[str, dict]) -> bigquery.SchemaField:
    '''
    Creates schema with column name, datatype and mode.
    '''
    schema = [bigquery.SchemaField(
        cn, params['datatype'], 
        mode=params['mode']) 
        for cn, params in column_params.items()]
    #       ^^-column_name
    return schema

def table_info(table: bigquery.Table) -> str:
    '''
    Returns verbose string of table information such as IDs, Datatypes, etc.
    '''

    return_string = (f'Table ID: {table.project}.{table.dataset_id}.{table.table_id}\n'
                    f'Column parameters:\n')

    for schemafield in table.schema:
        return_string += f"  Name: {'{:10}'.format(schemafield.name)} "
        return_string += f"Type: {'{:10}'.format(schemafield.field_type)} "
        return_string += f"Mode: {'{:10}'.format(schemafield.mode)}\n"

    return return_string