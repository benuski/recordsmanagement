#!/usr/bin/env python3
import pandas as pd
from sodapy import Socrata
import json
import os
from pathlib import Path

# Initialize the client
# Public data doesn't strictly require a token, but it's recommended for higher limits
client = Socrata("data.texas.gov", None)

def get_all_records(dataset_id):
    all_results = []
    limit = 2000
    offset = 0

    while True:
        # Fetching records with explicit long-form logic for pagination
        results = client.get(dataset_id, limit=limit, offset=offset)
        all_results.extend(results)

        if len(results) < limit:
            # We've reached the end of the dataset
            break

        offset += limit
        print(f"Fetched {len(all_results)} records...")

    return all_results

# Create output directory if it doesn't exist
output_dir = Path("../../data/tx")
output_dir.mkdir(parents=True, exist_ok=True)

# Dataset ID for Texas State Records Retention Schedule
dataset_identifier = "f6ng-hrgc"

# Execute extraction
records_list = get_all_records(dataset_identifier)

# Save to a JSON file in the specified directory
output_file = output_dir / "retention_series.json"
with open(output_file, "w") as f:
    json.dump(records_list, f, indent=4)

print(f"Successfully saved {len(records_list)} records to {output_file}")
