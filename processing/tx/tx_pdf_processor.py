"""
Texas PDF processor - extracts records from TX retention schedule PDFs using pdfplumber table extraction.
Similar to Virginia's approach but adapted for Texas PDF structure.
"""

import re
import csv
import json
import logging
import pdfplumber
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def load_retention_codes(csv_path: Path) -> dict:
    """Load retention codes CSV into a dict keyed by code."""
    codes = {}
    if not csv_path.exists():
        logger.warning(f"Retention codes CSV not found at {csv_path}")
        return codes

    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                codes[row["code"]] = {
                    "title": row["title"],
                    "definition": row["definition"]
                }
    except Exception as e:
        logger.error(f"Failed to load retention codes: {e}")

    return codes


def extract_metadata_from_pdf(pdf_path: Path) -> dict:
    """Extract metadata from the first few pages of the PDF."""
    metadata = {
        'state': 'tx',
        'schedule_type': '',
        'last_updated': '',
        'next_update': '',
        'agency_name': '',
        'schedule_id': '',
        'last_checked': datetime.now().strftime("%Y-%m-%d"),
        'url': ''
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Check first 3 pages for metadata
            for page in pdf.pages[:3]:
                text = page.extract_text()
                if not text:
                    continue

                # Look for dates
                if not metadata['last_updated']:
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
                    if date_match:
                        try:
                            date_obj = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                            metadata['last_updated'] = date_obj.strftime('%Y-%m-%d')
                        except:
                            pass

                if not metadata['next_update']:
                    next_match = re.search(r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})', text, re.IGNORECASE)
                    if next_match:
                        try:
                            date_obj = datetime.strptime(next_match.group(1), '%B %Y')
                            metadata['next_update'] = date_obj.strftime('%Y-%m')
                        except:
                            pass

                # Look for Agency Code
                if not metadata['schedule_id']:
                    code_match = re.search(r'Agency\s+Code[:\s]+(\d{3,4})', text, re.IGNORECASE)
                    if code_match:
                        metadata['schedule_id'] = code_match.group(1)

                # Look for Agency Name
                if not metadata['agency_name']:
                    name_match = re.search(r'Agency\s+Name[:\s]+(.+?)(?:\n|$)', text, re.IGNORECASE)
                    if name_match:
                        metadata['agency_name'] = name_match.group(1).strip()

    except Exception as e:
        logger.error(f"Error extracting metadata from {pdf_path}: {e}")

    # Determine schedule type
    if metadata['schedule_id']:
        metadata['schedule_type'] = 'agency-specific'
        metadata['url'] = f"https://www.tsl.texas.gov/sites/default/files/public/tslac/slrm/state/schedules/{metadata['schedule_id']}.pdf"
    else:
        metadata['schedule_type'] = 'general'

    return metadata


def parse_retention_field(retention_text: str, retention_codes: dict) -> dict:
    """Parse retention code and years from a retention field."""
    result = {
        'retention_code': '',
        'retention_years': '',
        'retention_statement': ''
    }

    if not retention_text:
        return result

    retention_text = retention_text.strip()

    # Pattern: "AC + 2" or "FE + 3" or "PM" or "US"
    retention_match = re.match(r'([A-Z]{2,3})(?:\s*\+\s*(\d+))?', retention_text)
    if retention_match:
        result['retention_code'] = retention_match.group(1)
        if retention_match.group(2):
            result['retention_years'] = retention_match.group(2)

        # Build full retention statement using codes CSV
        if result['retention_code'] in retention_codes:
            code_info = retention_codes[result['retention_code']]
            title = code_info['title']

            if result['retention_years']:
                years_int = int(result['retention_years'])
                year_label = 'year' if years_int == 1 else 'years'
                result['retention_statement'] = f"{title} plus {result['retention_years']} {year_label}"
            else:
                result['retention_statement'] = title

    return result


def process_texas_pdf(pdf_path: Path, output_schema: dict, retention_codes_path: Path) -> list[dict]:
    """
    Process a Texas retention schedule PDF and extract records.

    Args:
        pdf_path: Path to the PDF file
        output_schema: Output record schema template
        retention_codes_path: Path to retentioncodes.csv

    Returns:
        List of standardized record dictionaries
    """
    logger.info(f"Processing Texas PDF: {pdf_path}")

    # Load retention codes
    retention_codes = load_retention_codes(retention_codes_path)
    logger.info(f"Loaded {len(retention_codes)} retention codes")

    # Extract metadata
    metadata = extract_metadata_from_pdf(pdf_path)
    logger.info(f"Extracted metadata: schedule_id={metadata['schedule_id']}, agency={metadata['agency_name']}")

    records = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                # Extract tables from the page
                tables = page.extract_tables()

                if not tables:
                    continue

                logger.info(f"Page {page_num}: Found {len(tables)} tables")

                for table_idx, table in enumerate(tables):
                    if not table or len(table) < 2:  # Need header + at least one row
                        continue

                    # Find header row and map columns
                    header_row = None
                    for row in table[:5]:  # Check first 5 rows for header
                        if row and any(cell and 'item no' in str(cell).lower() for cell in row):
                            header_row = row
                            break

                    if not header_row:
                        logger.warning(f"Page {page_num}, Table {table_idx+1}: No header row found, skipping")
                        continue

                    # Map column indices
                    col_map = {}
                    for idx, header in enumerate(header_row):
                        if not header:
                            continue
                        header_lower = str(header).lower()
                        if 'item no' in header_lower or 'rsin' in header_lower:
                            col_map['series_id'] = idx
                        elif 'series title' in header_lower or 'record series title' in header_lower:
                            col_map['series_title'] = idx
                        elif 'description' in header_lower:
                            col_map['description'] = idx
                        elif 'retention period' in header_lower or 'ret. code' in header_lower:
                            col_map['retention'] = idx
                        elif 'remark' in header_lower:
                            col_map['remarks'] = idx
                        elif 'legal' in header_lower or 'citation' in header_lower:
                            col_map['legal'] = idx
                        elif 'archival' in header_lower:
                            col_map['archival'] = idx

                    # Process data rows
                    for row in table:
                        if row == header_row:
                            continue

                        # Skip empty rows
                        if not row or not any(row):
                            continue

                        # Skip header-like rows
                        if any(cell and isinstance(cell, str) and 'item no' in cell.lower() for cell in row):
                            continue

                        # Extract data
                        series_id = row[col_map.get('series_id', 0)] if col_map.get('series_id') is not None and len(row) > col_map.get('series_id', 0) else ''
                        series_title = row[col_map.get('series_title', 1)] if col_map.get('series_title') is not None and len(row) > col_map.get('series_title', 1) else ''

                        # Skip if no series_id or title
                        if not series_id or not series_title:
                            continue

                        series_id = str(series_id).strip()
                        series_title = str(series_title).strip()

                        record = dict(output_schema)  # Copy schema template
                        record.update({
                            'series_id': series_id,
                            'series_title': series_title,
                            'series_description': '',
                            'retention_code': '',
                            'retention_years': '',
                            'retention_statement': '',
                            'disposition': '',
                            'confidential': 'No',
                            'legal_citation': '',
                            'comments': ''
                        })

                        # Extract other fields
                        if 'description' in col_map and len(row) > col_map['description']:
                            record['series_description'] = str(row[col_map['description']] or '').strip()

                        if 'retention' in col_map and len(row) > col_map['retention']:
                            retention_data = parse_retention_field(str(row[col_map['retention']] or ''), retention_codes)
                            record.update(retention_data)

                        if 'remarks' in col_map and len(row) > col_map['remarks']:
                            record['comments'] = str(row[col_map['remarks']] or '').strip()

                        if 'legal' in col_map and len(row) > col_map['legal']:
                            record['legal_citation'] = str(row[col_map['legal']] or '').strip()

                        if 'archival' in col_map and len(row) > col_map['archival']:
                            archival = str(row[col_map['archival']] or '').strip().upper()
                            if archival == 'A':
                                record['disposition'] = 'Permanent, Archives'
                            elif archival == 'R':
                                record['disposition'] = 'Must offer to Archives prior to destruction'

                        # Default disposition if not set
                        if not record['disposition']:
                            if record['retention_code'] == 'PM':
                                record['disposition'] = 'Permanent, Archives'
                            else:
                                record['disposition'] = 'Non-confidential Destruction'

                        # Apply metadata
                        record.update({
                            'state': metadata['state'],
                            'schedule_type': metadata['schedule_type'],
                            'schedule_id': metadata['schedule_id'],
                            'agency_name': metadata['agency_name'],
                            'last_updated': metadata['last_updated'],
                            'next_update': metadata['next_update'],
                            'last_checked': metadata['last_checked'],
                            'url': metadata['url']
                        })

                        records.append(record)

    except Exception as e:
        logger.error(f"Error processing PDF {pdf_path}: {e}")
        raise

    logger.info(f"Extracted {len(records)} records from {pdf_path}")
    return records
