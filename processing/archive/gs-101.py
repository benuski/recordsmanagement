import pdfplumber
import json
import re
from pathlib import Path
from datetime import datetime, date

def extract_effective_date(page_text):
    """Extract the effective schedule date from the PDF text."""
    date_match = re.search(r'EFFECTIVE SCHEDULE DATE:\s*(\d{1,2}/\d{1,2}/\d{4})', page_text)
    if date_match:
        date_str = date_match.group(1)
        parsed_date = datetime.strptime(date_str, '%m/%d/%Y')
        return parsed_date.strftime('%Y-%m-%d')
    return None

def process_pdf_tables(pdf_path, output_path):
    """
    Extract tables from PDF and convert to structured records.

    Args:
        pdf_path: Path to the PDF file
        output_path: Path for the output JSON file
    """
    processed_records = []
    effective_date = None

    # Open the PDF
    with pdfplumber.open(pdf_path) as pdf:
        # Extract effective date from first page
        if pdf.pages:
            first_page_text = pdf.pages[0].extract_text()
            effective_date = extract_effective_date(first_page_text)

        # Iterate through each page
        for page in pdf.pages:
            # Extract tables from the current page
            tables = page.extract_tables()

            # Process each table on the page
            for table in tables:
                if not table or len(table) < 2:
                    continue

                # First row contains headers
                headers = table[0]
                rows = table[1:]

                # Process each row
                for row in rows:
                    if not row or len(row) < len(headers):
                        continue

                    # Create dictionary for the row
                    row_dict = {headers[i]: row[i] for i in range(len(headers)) if i < len(row)}

                    # Split the series and description field
                    series_and_desc = row_dict.get('RECORDS SERIES AND DESCRIPTION', '')
                    if '\n' in series_and_desc:
                        series_title, series_description = series_and_desc.split('\n', 1)
                    else:
                        series_title = series_and_desc
                        series_description = ''

                    # Extract retention years
                    retention_statement = row_dict.get('SCHEDULED RETENTION PERIOD', '')
                    retention_years_match = re.search(r'(\d+)\s*[Yy]ear', retention_statement)
                    if retention_years_match:
                        retention_years = int(retention_years_match.group(1))
                    elif 'permanent' in retention_statement.lower():
                        retention_years = None
                    else:
                        retention_years = None

                    # Get disposition and check confidential status
                    disposition = row_dict.get('DISPOSITION METHOD', '')
                    is_confidential = "confidential" in disposition.lower() and "non-confidential" not in disposition.lower()

                    # Create the processed record
                    record = {
                        "state": "va",
                        "schedule_type": "general",
                        "schedule_id": row_dict.get('SERIES NUMBER', ''),
                        "series_title": series_title.strip(),
                        "series_description": series_description.strip(),
                        "retention_statement": retention_statement,
                        "retention_years": retention_years,
                        "disposition": disposition,
                        "confidential": is_confidential,
                        "legal_citation": "",
                        "last_updated": effective_date,
                        "last_checked": str(date.today())
                    }

                    processed_records.append(record)

    # Save to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(processed_records, f, indent=2, ensure_ascii=False)

# Example usage
if __name__ == "__main__":
    # PDF file path
    pdf_file = "../pdfs/GS-101.pdf"

    # Create output filename based on input PDF name
    pdf_path = Path(pdf_file)
    output_file = Path("../../data/va") / f"{pdf_path.stem}.json"

    # Process PDF to JSON
    process_pdf_tables(pdf_file, output_file)
