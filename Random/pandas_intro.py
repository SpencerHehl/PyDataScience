#!/usr/bin/env python3
from google.cloud import bigquery
from collections import defaultdict
import pandas_gbq as pgbq

client = bigquery.Client()

query = """
    SELECT *
    FROM `archerfrs-test.archer_device_telem.drone_battery`
"""
data_frame = pgbq.read_gbq(query, project_id="archerfrs-test")  # Make an API request.

print("The query data:")
print(data_frame)

print("The top 5 rows")
print(data_frame.head(5))

print("The bottom 5 rows")
print(data_frame.tail(5))

print("Describe the data")
print(data_frame.describe())
