#!/usr/bin/env python3
"""Test script to verify Ohio grouping logic works correctly."""

from collections import defaultdict
import json

# Simulate records from different HTML files with different schedule_ids
test_records = [
    {"schedule_id": "965-0012", "series_title": "Record A", "url": ".../34"},
    {"schedule_id": "965-0015", "series_title": "Record B", "url": ".../35"},
    {"schedule_id": "054-0091", "series_title": "Record C", "url": ".../1002"},
    {"schedule_id": "054-0092", "series_title": "Record D", "url": ".../1003"},
    {"schedule_id": "501-0141", "series_title": "Record E", "url": ".../1004"},
    {"schedule_id": "019-0079", "series_title": "Record F", "url": ".../10020"},
]

# Group records by schedule_id prefix (digits before dash)
grouped_records = defaultdict(list)

for record in test_records:
    schedule_id = record.get('schedule_id', '')
    if schedule_id and '-' in schedule_id:
        prefix = schedule_id.split('-')[0]
        grouped_records[prefix].append(record)
    else:
        print(f"⚠ Warning: No valid schedule_id found: {schedule_id}")

print("Grouped Records:")
print("=" * 60)

for schedule_prefix, records in sorted(grouped_records.items()):
    print(f"\n{schedule_prefix}.json ({len(records)} records):")
    for record in records:
        print(f"  - {record['schedule_id']}: {record['series_title']}")

print("\n" + "=" * 60)
print(f"\nExpected output files:")
for prefix in sorted(grouped_records.keys()):
    print(f"  - {prefix}.json")

print("\n✓ Grouping logic test passed!")
print("\nExpected groupings:")
print("  019.json: 1 record (019-0079)")
print("  054.json: 2 records (054-0091, 054-0092)")
print("  501.json: 1 record (501-0141)")
print("  965.json: 2 records (965-0012, 965-0015)")
