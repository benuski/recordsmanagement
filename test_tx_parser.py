#!/usr/bin/env python3
"""Test script for Texas PDF parser"""

import logging
import json
from pathlib import Path
from processing.tx.parser import parse_texas_structure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
structure_json = Path("processing/tx/pdfs/pdfplumberstructuretext.json")
retention_codes = Path("processing/tx/retentioncodes.csv")
output_schema_path = Path("processing/output_template_clean.json")
output_file = Path("data/tx/test_601_parsed.json")

# Load schema
with open(output_schema_path, 'r') as f:
    output_schema = json.load(f)

# Parse the structure
records = parse_texas_structure(structure_json, output_schema, retention_codes)

print(f"\n{'='*60}")
print(f"Parsed {len(records)} records")
print(f"{'='*60}\n")

if records:
    # Show first 3 records
    for i, record in enumerate(records[:3]):
        print(f"Record {i+1}:")
        for key, value in record.items():
            if value:  # Only show non-empty fields
                print(f"  {key}: {value}")
        print()

    # Save to file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(records)} records to {output_file}")
