# Script bigquery table from json data

Your task is to use **python** to create a **bigquery table** in CLEAN (which means it should have a *schema*), using a snippet of JSON to infer which *types* each field has.

Work with the code locally, on your computer or a teammates.

## Background

We are doing it using code because your source system has 100 tables, with each table having 100+ fields.

## Team up!

Divide into groups of 2-3 people, with people you haven't previously worked with.

## Your starting JSON

```
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
    "isRaining": true
}
```

# Your task is to:
- Create a function that checks which [pythonic type](https://www.w3schools.com/python/python_datatypes.asp) a value is. Then return the [BigQuery Equivalent](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
- Loop through each value of the JSON data, each time getting the BigQuery equivalent by using the function previously mentioned.
- **No unix timestamps** are allowed. Convert it to another, suitable, data type.
- Each function **must** use typing!

The end result is either a `SQL-script`, which you can run in BigQuery manually, or a `bigquery.Table` object, which you can use to create the table in BigQuery from Python.

## Turn the assignment in:

- git clone this repo
- create a new branch
    - add
    - commit
    - push
- create a pull request with your code.

Your code should be in a folder with the name of your team.

## Optional: review another teams pull request.

You can find those here:
https://github.com/ebremstedt/script_table/pulls

## Helpful links

https://cloud.google.com/bigquery/docs/tables


## Note

This is a small example, but some tables really do have 100+ fields. Doing this manually will not be very fun, _trust me_
