import json
import re
from pathlib import Path
from datetime import date

def process_schedule_data(input_json_path, output_json_path=None):
    """
    Process extracted table data into structured schedule records.

    Args:
        input_json_path: Path to the input JSON file from PDF extraction
        output_json_path: Path for the output JSON file (optional)

    Returns:
        List of processed records
    """
    # Read the input JSON
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    processed_records = []

    # Iterate through all tables
    for table in data.get('tables', []):
        # Process each row in the table's data
        for row in table.get('data', []):
            # Split the series and description field
            series_and_desc = row.get('RECORDS SERIES AND DESCRIPTION', '')
            if '\n' in series_and_desc:
                series_title, series_description = series_and_desc.split('\n', 1)
            else:
                series_title = series_and_desc
                series_description = ''

            # Extract retention years (first number from retention statement)
            retention_statement = row.get('SCHEDULED RETENTION PERIOD', '')
            retention_years_match = re.search(r'\d+', retention_statement)
            retention_years = int(retention_years_match.group()) if retention_years_match else None

            # Create the processed record
            record = {
                "state": "va",
                "schedule_type": "general",
                "series_id": row.get('SERIES NUMBER', ''),
                "series_title": series_title.strip(),
                "series_description": series_description.strip(),
                "retention_statement": retention_statement,
                "retention_years": retention_years,
                "disposition": "",
                "confidential": "",
                "legal_citation": "",
                "last_updated": str(date.today())
            }

            processed_records.append(record)

    # Save to file if output path provided
    if output_json_path:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(processed_records, f, indent=2, ensure_ascii=False)

    return processed_records

# Example usage
if __name__ == "__main__":
    # Input JSON file (from the extraction script)
    input_file = "../../data/va/GS-101_0.json"

    # Output file for processed data
    output_file = "../../data/va/GS-101_0_processed.json"

    # Process the data
    records = process_schedule_data(input_file, output_file)
