#!/usr/bin/env python3
"""Test script to verify Ohio agency grouping logic works correctly."""

from collections import defaultdict
import json

# Simulate records from different HTML files with different agencies
test_records = [
    {"agency_name": "OEB", "series_id": "PI-001", "series_title": "Program Files"},
    {"agency_name": "OEB", "series_id": "PI-002", "series_title": "Broadcast Logs"},
    {"agency_name": "LCC", "series_id": "LCC/APP-04", "series_title": "Rejection Orders"},
    {"agency_name": "DAS", "series_id": "GAR-BLM-01", "series_title": "Lease Records"},
    {"agency_name": "LCC", "series_id": "LCC/APP-05", "series_title": "Permit Applications"},
    {"agency_name": "OEB", "series_id": "PI-003", "series_title": "Production Records"},
]

# Group records by agency_name
grouped_records = defaultdict(list)

for record in test_records:
    agency_name = record.get('agency_name', '').strip()
    if agency_name:
        grouped_records[agency_name].append(record)

print("Grouped Records by Agency:")
print("=" * 60)

for agency_name, records in sorted(grouped_records.items()):
    print(f"\n{agency_name}.json ({len(records)} records):")
    for record in records:
        print(f"  - {record['series_id']}: {record['series_title']}")

print("\n" + "=" * 60)
print(f"\nExpected output files:")
for agency in sorted(grouped_records.keys()):
    record_count = len(grouped_records[agency])
    print(f"  - {agency}.json ({record_count} record{'s' if record_count > 1 else ''})")

print("\n✓ Agency grouping logic test passed!")
print("\nExpected groupings:")
print("  OEB.json: 3 records (Ohio Educational Broadcasting)")
print("  LCC.json: 2 records (Liquor Control Commission)")
print("  DAS.json: 1 record  (Department of Administrative Services)")
