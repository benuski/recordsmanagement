#!/usr/bin/env python3
import csv
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

def load_retention_codes(filepath: str) -> dict:
    """Load retentioncodes.csv into a dict keyed by code."""
    codes = {}
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            codes[row["code"]] = row
    return codes

RETENTION_CODES = load_retention_codes("retentioncodes.csv")

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
    "retention_code_definition": "",
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
    "rsin":                      "series_id",
    "record_series_title":       "series_title",
    "record_series_description": "series_description",
    "years":                     "retention_years",
    "remarks":                   "comments",
    "ac_definition":             "retention_code_definition",
    # Add additional Texas source fields below as you identify them
    # "next_recertification":    "next_update",
    # "legal_authority":         "legal_citation",
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

def resolve_retention_code_definition(record: dict) -> str:
    """
    AC codes: title from CSV + ac_definition from source (stripping 'AC = ').
    All other codes: title and definition both from CSV.
    """
    code = record.get("retention_code", "")
    if code not in RETENTION_CODES:
        return ""
    title = RETENTION_CODES[code]["title"]
    if code == "AC":
        definition = record.get("retention_code_definition", "").removeprefix("AC = ")
    else:
        definition = RETENTION_CODES[code]["definition"]
    return f"{title}: {definition}"

def resolve_retention_statement(record: dict) -> str:
    """Construct retention_statement as 'title plus retention_years year(s)'."""
    code = record.get("retention_code", "")
    years = record.get("retention_years", "")
    if code not in RETENTION_CODES:
        return ""
    title = RETENTION_CODES[code]["title"]
    if years:
        year_label = "year" if years == "1" else "years"
        return f"{title} plus {years} {year_label}"
    return title

def resolve_disposition(record: dict) -> str:
    """Map source 'archival' field to a disposition value."""
    archival = record.get("archival", "")
    if archival == "A":
        return "Permanent, Archives"
    elif archival == "R":
        return "Must offer to Archives prior to destruction"
    return ""

def standardize_record(raw: dict) -> dict:
    record = dict(raw)

    # Rename source fields to standardized names
    for src_key, std_key in FIELD_MAPPING.items():
        if src_key in record:
            record[std_key] = record.pop(src_key)

    # Derive schedule_id from series_id (x.y from x.y.zzz)
    record["schedule_id"] = extract_schedule_id(record.get("series_id", ""))

    # Resolve retention_code_definition from code + source field
    record["retention_code_definition"] = resolve_retention_code_definition(record)

    # Construct retention_statement from code title + retention_years
    record["retention_statement"] = resolve_retention_statement(record)

    # Resolve disposition from archival field
    record["disposition"] = resolve_disposition(record)

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
print(f"Derived fields:  schedule_id, retention_code_definition, retention_statement, disposition")
print(f"Metadata fields: {', '.join(METADATA.keys())}")
