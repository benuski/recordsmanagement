import pandas as pd
import sys
import os
import json

def convert_json_to_csv(json_path):
    csv_path = os.path.splitext(json_path)[0] + '.csv'
    try:
        with open(json_path) as f:
            df = pd.json_normalize(json.load(f))
        df.to_csv(csv_path, index=False)
        print(f"Converted: {json_path} -> {csv_path}")
    except Exception as e:
        print(f"Failed: {json_path} ({e})")

# Get directory from command line argument
directory = sys.argv[1]

# Find and convert all .json files in the directory
json_files = [f for f in os.listdir(directory) if f.endswith('.json')]

if not json_files:
    print("No JSON files found in directory.")
else:
    for filename in json_files:
        convert_json_to_csv(os.path.join(directory, filename))
