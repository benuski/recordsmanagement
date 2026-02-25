#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime
from sodapy import Socrata

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

# All required output fields — any not populated from source will default to ""
RECORD_TEMPLATE = {
    "state": "",
    "schedule_type": "",
    "schedule_id": "",
    "series_id": "",
    "series_title": "",
    "series_description": "",
    "retention_statement": "",
    "retention_years": "",
    "retention_code": "",
    "comments": "",
    "disposition": "",
    "confidential": "",
    "legal_citation": "",
    "last_checked": "",
    "last_updated": "",
    "next_update": ""
}

# Crosswalk: source field name → standardized field name
FIELD_MAPPING = {
    "rsin":                     "series_id",
    "record_series_title":      "series_title",
    "record_series_description":"series_description",
    "years":                    "retention_years",
    "remarks":                  "comments",
    # Add additional Texas source fields below as you identify them
    # "next_recertification":   "next_update",
    # "legal_authority":        "legal_citation",
    # "disposition_authority":  "disposition",
}

# Static metadata applied to every record
METADATA = {
    "state":         "tx",
    "schedule_type": "general",
    "last_checked":  datetime.now().strftime("%Y-%m-%d"),
}

def extract_schedule_id(series_id: str) -> str:
    """Derive schedule_id (x.y) from a Texas series_id (x.y.zzz)."""
    parts = series_id.split(".")
    if len(parts) >= 3:
        return f"{parts[0]}.{parts[1]}"
    return ""

def standardize_record(raw: dict) -> dict:
    record = dict(raw)

    # Rename source fields to standardized names
    for src_key, std_key in FIELD_MAPPING.items():
        if src_key in record:
            record[std_key] = record.pop(src_key)

    # Derive schedule_id from series_id (x.y from x.y.zzz)
    record["schedule_id"] = extract_schedule_id(record.get("series_id", ""))

    # Apply static metadata (overwrites any conflicting source values)
    record.update(METADATA)

    # Ensure every template field is present, defaulting to "" if absent
    for field, default in RECORD_TEMPLATE.items():
        record.setdefault(field, default)

    return record

# Create output directory
output_dir = Path("../../data/tx")
output_dir.mkdir(parents=True, exist_ok=True)

# Dataset ID for Texas State Records Retention Schedule
dataset_identifier = "f6ng-hrgc"

# Fetch, standardize, and save
raw_records = get_all_records(dataset_identifier)
standardized_records = [standardize_record(r) for r in raw_records]

output_file = output_dir / "tx_retention_series.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(standardized_records, f, indent=4)

print(f"Successfully saved {len(standardized_records)} records to {output_file}")
print(f"Renamed fields:  {', '.join(f'{s} → {t}' for s, t in FIELD_MAPPING.items())}")
print(f"Derived fields:  schedule_id")
print(f"Metadata fields: {', '.join(METADATA.keys())}")
