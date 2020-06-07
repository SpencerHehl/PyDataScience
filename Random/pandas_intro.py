#!/usr/bin/env python3
from google.cloud import bigquery
from collections import defaultdict
import pandas_gbq as pgbq
import pandas as pd

client = bigquery.Client()

query = """
    SELECT *
    FROM `archerfrs-test.archer_device_telem.drone_battery`
"""
data_frame = pgbq.read_gbq(query, project_id="archerfrs-test")  # Make an API request.
data_frame_indexed = data_frame.set_index('timeSent')
print("The query data:")
print(data_frame_indexed)

print('\n Describe the data')
print(data_frame_indexed.describe())

print('\n The top 5 rows')
print(data_frame_indexed.head(5))

print("\n The bottom 5 rows")
print(data_frame_indexed.tail(5))

print("\n Device Statistics")
grouped_frame = data_frame_indexed.groupby(["deviceId",pd.Grouper(freq='1M')])["percentage"]

print("Mean Battery")
print(grouped_frame.mean())

print("\n Max Battery")
print(grouped_frame.max())

print("\n Min Battery")
print(grouped_frame.min())