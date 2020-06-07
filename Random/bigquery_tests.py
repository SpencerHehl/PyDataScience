#!/usr/bin/env python3
from google.cloud import bigquery
from collections import defaultdict
from tqdm import tqdm

query = """
    SELECT *
    FROM `archerfrs-test.archer_device_telem.drone_battery`
"""

client = bigquery.Client()
query_job = tqdm(client.query(query), desc="Querying Data")

battery_states = []

for row in query_job:
    battery_states.append((row["percentage"], row["deviceId"]))

batt_percent_by_deviceId = defaultdict(list)
for percentage, device_id in battery_states:
    batt_percent_by_deviceId[device_id].append(percentage)

data_count_by_device = {
    device_id: len(percentage)
    for device_id, percentage in batt_percent_by_deviceId.items()
}
print("Row count per device ID")
print(data_count_by_device)

average_battery_by_device = {
    device_id: sum(percentage) / len(percentage)
    for device_id, percentage in batt_percent_by_deviceId.items()
}

print("Average Percent by Device ID")
print(average_battery_by_device)

max_perc_by_device = {
    device_id: max(percentage)
    for device_id, percentage in batt_percent_by_deviceId.items()
}

print("Max Percentage Value by Device ID")
print(max_perc_by_device)

min_perc_by_device = {
    device_id: min(percentage)
    for device_id, percentage in batt_percent_by_deviceId.items()
}

print("Min Percentage Value by Device ID")
print(min_perc_by_device)