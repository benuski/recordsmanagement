#!/usr/bin/env python3
"""Test script for direct Texas PDF processing"""

import logging
import json
from pathlib import Path
from processing.tx.tx_pdf_processor import process_texas_pdf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
pdf_file = Path("processing/tx/pdfs/601.pdf")
retention_codes = Path("processing/tx/retentioncodes.csv")
output_schema_path = Path("processing/output_template_clean.json")
output_file = Path("data/tx/601_direct_parsed.json")

# Load schema
with open(output_schema_path, 'r') as f:
    output_schema = json.load(f)

# Process the PDF
records = process_texas_pdf(pdf_file, output_schema, retention_codes)

print(f"\n{'='*60}")
print(f"Parsed {len(records)} records")
print(f"{'='*60}\n")

if records:
    # Show first 5 records
    for i, record in enumerate(records[:5]):
        print(f"Record {i+1}:")
        for key, value in record.items():
            if value:  # Only show non-empty fields
                print(f"  {key}: {value[:100] if isinstance(value, str) and len(value) > 100 else value}")
        print()

    # Save to file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(records)} records to {output_file}")
