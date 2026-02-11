#!/usr/bin/env python3
import pandas as pd
from sodapy import Socrata
import json
from pathlib import Path
from datetime import datetime

# Initialize the client
client = Socrata("data.texas.gov", None)

def get_all_records(dataset_id):
    all_results = []
    limit = 2000
    offset = 0

    while True:
        results = client.get(dataset_id, limit=limit, offset=offset)
        all_results.extend(results)

        if len(results) < limit:
            break

        offset += limit
        print(f"Fetched {len(all_results)} records...")

    return all_results

# Crosswalk mapping for standardizing field names
field_mapping = {
    "rsin": "series_id",
    "record_series_title": "series_title",
    "record_series_description": "series_description",
    "years": "retention_years",
    "remarks": "comments"
}

# Additional metadata to append to every record
metadata = {
    "state": "tx",
    "schedule_type": "general",
    "last_checked": datetime.now().strftime("%Y-%m-%d")
}

# Create output directory
output_dir = Path("../../data/tx")
output_dir.mkdir(parents=True, exist_ok=True)

# Dataset ID for Texas State Records Retention Schedule
dataset_identifier = "f6ng-hrgc"

# Execute extraction
records_list = get_all_records(dataset_identifier)

# Apply crosswalk and append metadata
standardized_records = []
for record in records_list:
    standardized_record = {}

    # First, copy all fields as-is
    for key, value in record.items():
        standardized_record[key] = value

    # Then rename the mapped fields
    for old_key, new_key in field_mapping.items():
        if old_key in standardized_record:
            standardized_record[new_key] = standardized_record.pop(old_key)

    # Append metadata fields
    standardized_record.update(metadata)

    standardized_records.append(standardized_record)

# Save to JSON
output_file = output_dir / "tx_retention_series.json"
with open(output_file, "w", encoding='utf-8') as f:
    json.dump(standardized_records, f, indent=4)

print(f"Successfully saved {len(standardized_records)} records to {output_file}")
print(f"Standardized fields: {', '.join(field_mapping.values())}")
print(f"Added metadata: {', '.join(metadata.keys())}")
