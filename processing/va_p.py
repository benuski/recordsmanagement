#!/usr/bin/env python3
"""
Structured OCR Extraction for LVA Records
Optimized for RTX 4080 Super (Local ONNX/Torch)
"""
import argparse
import json
import pandas as pd
from pathlib import Path
from datalab_sdk import DatalabClient, ConvertOptions

def get_records_schema():
    """Defines the schema to align LVA PDFs with your template."""
    return {
        "type": "object",
        "properties": {
            "records": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "series_id": {"type": "string", "description": "6-digit Series Number"},
                        "series_title": {"type": "string", "description": "The bolded title of the record series"},
                        "series_description": {"type": "string", "description": "The descriptive paragraph text"},
                        "retention_statement": {"type": "string", "description": "Full text of the Scheduled Retention and Disposition column"},
                        "legal_citation": {"type": "string", "description": "Any Code of Virginia or COV citations mentioned"}
                    },
                    "required": ["series_id", "series_title"]
                }
            }
        }
    }

def run_structured_ocr(pdf_path: Path, use_gpu: bool = True):
    """Runs OCR using Chandra/Marker with a forced JSON schema."""
    
    # Initialize client for local execution
    # Note: Ensure datalab-sdk is installed in your pixi environment
    client = DatalabClient() 

    schema = get_records_schema()
    
    # Using 'chandra' mode for higher accuracy on the LVA table layouts
    # It handles the multi-line descriptions much better than standard surya
    options = ConvertOptions(
        page_schema=json.dumps(schema),
        mode="chandra" if use_gpu else "balanced"
    )

    print(f"Processing {pdf_path.name} with structured extraction...")
    result = client.convert(str(pdf_path), options=options)
    
    return json.loads(result.extraction_schema_json)

def map_to_template(extracted_json):
    """Wraps extracted data into your specific JSON template format."""
    final_records = []
    
    for item in extracted_json.get("records", []):
        row = {
            "state": "va",
            "schedule_type": "general",
            "schedule_id": "113",
            "series_id": item.get("series_id", ""),
            "series_title": item.get("series_title", ""),
            "series_description": item.get("series_description", ""),
            "retention_statement": item.get("retention_statement", ""),
            "retention_years": "",  # Logic for regex extraction can go here
            "retention_code": "",
            "comments": "",
            "disposition": "temporary" if "destroy" in item.get("retention_statement", "").lower() else "permanent",
            "confidential": "y" if "confidential" in item.get("retention_statement", "").lower() else "n",
            "legal_citation": item.get("legal_citation", "COV 42.1-76"),
            "last_updated": "2026-02-15"
        }
        final_records.append(row)
    
    return pd.DataFrame(final_records)

def main():
    parser = argparse.ArgumentParser(description="Structured OCR for LVA Records")
    parser.add_argument("pdf_file", type=str, help="Path to the PDF file")
    parser.add_argument("--csv", action="store_true", help="Save output to CSV")
    args = parser.parse_args()

    pdf_path = Path(args.pdf_file)
    if not pdf_path.exists():
        print(f"Error: {pdf_path} not found.")
        return

    # 1. Run OCR
    try:
        raw_data = run_structured_ocr(pdf_path)
    except Exception as e:
        print(f"OCR Failed: {e}")
        return

    # 2. Map to your template and create DataFrame
    df = map_to_template(raw_data)

    # 3. Display results
    print("\n" + "="*80)
    print("EXTRACTED RECORDS (Mapped to Template):")
    print("="*80)
    print(df.to_string(index=False))

    if args.csv:
        output_name = pdf_path.with_suffix(".csv")
        df.to_csv(output_name, index=False)
        print(f"\nSaved to {output_name}")

if __name__ == "__main__":
    main()