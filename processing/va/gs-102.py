import fitz  # pymupdf
import json
import re
from pathlib import Path
from datetime import datetime, date

def extract_text_from_pdf(pdf_path):
    """
    Extract text from PDF using pymupdf.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        Extracted text as string
    """
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()
    return text

def extract_effective_date(text):
    """
    Extract the effective schedule date from the PDF text.

    Args:
        text: Full text from PDF

    Returns:
        Date string in YYYY-MM-DD format, or None if not found
    """
    # Look for pattern like "EFFECTIVE SCHEDULE DATE: 3/28/2024"
    date_match = re.search(r'EFFECTIVE SCHEDULE DATE:\s*(\d{1,2}/\d{1,2}/\d{4})', text)
    if date_match:
        date_str = date_match.group(1)
        # Parse the date (M/D/YYYY format)
        parsed_date = datetime.strptime(date_str, '%m/%d/%Y')
        return parsed_date.strftime('%Y-%m-%d')
    return None

def parse_gs_schedule_text(text, effective_date):
    """
    Parse GS schedule text into structured records.

    Args:
        text: Extracted text from PDF
        effective_date: Effective schedule date from PDF header

    Returns:
        List of processed records
    """
    lines = [line.rstrip() for line in text.split('\n')]

    processed_records = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Skip empty lines and header content
        if not line or 'RECORDS RETENTION' in line or 'Government Records' in line:
            i += 1
            continue

        # Check if next line is a 6-digit series number
        if i + 1 < len(lines) and re.match(r'^\d{6}$', lines[i + 1].strip()):
            series_title = line
            series_number = lines[i + 1].strip()
            retention_statement = lines[i + 2].strip() if i + 2 < len(lines) else ''
            disposition = lines[i + 3].strip() if i + 3 < len(lines) else ''

            # Collect description lines
            description_lines = []
            j = i + 4
            while j < len(lines):
                next_line = lines[j].strip()
                if j + 1 < len(lines) and re.match(r'^\d{6}$', lines[j + 1].strip()):
                    break
                if next_line:
                    description_lines.append(next_line)
                j += 1

            series_description = ' '.join(description_lines)

            # Extract retention years
            retention_years_match = re.search(r'(\d+)\s*[Yy]ear', retention_statement)
            if retention_years_match:
                retention_years = int(retention_years_match.group(1))
            elif 'permanent' in retention_statement.lower():
                retention_years = None
            else:
                retention_years = None

            # Check confidential status
            is_confidential = "confidential" in disposition.lower() and "non-confidential" not in disposition.lower()

            # Create record
            record = {
                "state": "va",
                "schedule_type": "general",
                "schedule_id": series_number,
                "series_title": series_title,
                "series_description": series_description,
                "retention_statement": retention_statement,
                "retention_years": retention_years,
                "disposition": disposition,
                "confidential": is_confidential,
                "legal_citation": "",
                "last_updated": effective_date,
                "last_checked": str(date.today())
            }

            processed_records.append(record)

            # Move to next record
            i = j
        else:
            i += 1

    return processed_records

def process_pdf_to_json(pdf_path, output_path):
    """
    Complete workflow: PDF -> Text -> Structured JSON

    Args:
        pdf_path: Path to the PDF file
        output_path: Path for the output JSON file
    """
    # Extract text from PDF
    text = extract_text_from_pdf(pdf_path)

    # Extract effective date from header
    effective_date = extract_effective_date(text)

    # Parse text into structured records
    records = parse_gs_schedule_text(text, effective_date)

    # Save to JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

# Example usage
if __name__ == "__main__":
    # PDF file path
    pdf_file = "GS-103.pdf"

    # Create output filename based on input PDF name
    pdf_path = Path(pdf_file)
    output_file = Path("../../data/va") / f"{pdf_path.stem}.json"

    # Process PDF to JSON
    process_pdf_to_json(pdf_file, output_file)
