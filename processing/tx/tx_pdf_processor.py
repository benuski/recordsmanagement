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
from processing.tx.parse_agencies import parse_agencies_html

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

    # Fallback: extract schedule_id from filename (e.g., "601.pdf" -> "601")
    filename_match = re.match(r'^(\d{3,4})\.pdf$', pdf_path.name)
    if filename_match:
        metadata['schedule_id'] = filename_match.group(1)

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
        metadata['schedule_type'] = 'specific'
        metadata['url'] = f"https://www.tsl.texas.gov/sites/default/files/public/tslac/slrm/state/schedules/{metadata['schedule_id']}.pdf"
    else:
        metadata['schedule_type'] = 'general'

    return metadata


def parse_retention_field(retention_text: str, retention_codes: dict) -> dict:
    """Parse retention code and periods (years, months, weeks, days) from a retention field."""
    result = {
        'retention_code': '',
        'retention_years': '',
        'retention_months': '',
        'retention_weeks': '',
        'retention_days': '',
        'retention_statement': ''
    }

    if not retention_text:
        return result

    retention_text = retention_text.strip()

    # Pattern: "AC + 2" or "FE + 3 months" or "PM" or "US"
    # Basic code extraction
    code_match = re.match(r'^([A-Z]{2,3})', retention_text)
    if code_match:
        result['retention_code'] = code_match.group(1)
        
        # Look for numbers with units
        years_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:year|yr)', retention_text, re.IGNORECASE)
        months_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:month|mo)', retention_text, re.IGNORECASE)
        weeks_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:week|wk)', retention_text, re.IGNORECASE)
        days_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:day)', retention_text, re.IGNORECASE)

        if years_match: result['retention_years'] = years_match.group(1)
        if months_match: result['retention_months'] = months_match.group(1)
        if weeks_match: result['retention_weeks'] = weeks_match.group(1)
        if days_match: result['retention_days'] = days_match.group(1)
        
        # If no explicit units but has a number like "AC + 2"
        if not any([result['retention_years'], result['retention_months'], result['retention_weeks'], result['retention_days']]):
            num_match = re.search(r'\+\s*(\d+(?:\.\d+)?)', retention_text)
            if num_match:
                # Default to years if no unit specified
                result['retention_years'] = num_match.group(1)

        # Build full retention statement using codes CSV
        if result['retention_code'] in retention_codes:
            code_info = retention_codes[result['retention_code']]
            title = code_info['title']
            
            parts = []
            if result['retention_years']:
                label = 'year' if result['retention_years'] == '1' else 'years'
                parts.append(f"{result['retention_years']} {label}")
            if result['retention_months']:
                label = 'month' if result['retention_months'] == '1' else 'months'
                parts.append(f"{result['retention_months']} {label}")
            if result['retention_weeks']:
                label = 'week' if result['retention_weeks'] == '1' else 'weeks'
                parts.append(f"{result['retention_weeks']} {label}")
            if result['retention_days']:
                label = 'day' if result['retention_days'] == '1' else 'days'
                parts.append(f"{result['retention_days']} {label}")
                
            if parts:
                result['retention_statement'] = f"{title} plus {' and '.join(parts)}"
            else:
                result['retention_statement'] = title

    return result


def process_texas_pdf(pdf_path: Path, output_schema: dict, retention_codes_path: Path, agency_mapping: dict = None) -> list[dict]:
    """
    Process a Texas retention schedule PDF and extract records.

    Args:
        pdf_path: Path to the PDF file
        output_schema: Output record schema template
        retention_codes_path: Path to retentioncodes.csv
        agency_mapping: Optional dict mapping schedule_id to agency info (from agencies.html)

    Returns:
        List of standardized record dictionaries
    """
    logger.info(f"Processing Texas PDF: {pdf_path}")

    # Load retention codes
    retention_codes = load_retention_codes(retention_codes_path)
    logger.info(f"Loaded {len(retention_codes)} retention codes")

    # Extract metadata
    metadata = extract_metadata_from_pdf(pdf_path)

    # Enhance metadata with agency mapping if available
    # Prefer agency mapping over PDF extraction (more reliable)
    if agency_mapping and metadata['schedule_id'] in agency_mapping:
        agency_info = agency_mapping[metadata['schedule_id']]
        # Always use agency mapping for name (more authoritative than PDF extraction)
        metadata['agency_name'] = agency_info['name']
        # Use dates from mapping if not found in PDF
        if not metadata['last_updated']:
            metadata['last_updated'] = agency_info['last_updated']
        if not metadata['next_update']:
            metadata['next_update'] = agency_info['next_update']

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
                    header_row_idx = -1
                    for r_idx, row in enumerate(table[:5]):  # Check first 5 rows for header
                        if row and any(cell and 'item no' in str(cell).lower() for cell in row):
                            header_row_idx = r_idx
                            break

                    if header_row_idx == -1:
                        logger.warning(f"Page {page_num}, Table {table_idx+1}: No header row found, skipping")
                        continue

                    # Combine main header and sub-header to catch "Years", "Months", "Days"
                    row1 = table[header_row_idx]
                    row2 = table[header_row_idx + 1] if header_row_idx + 1 < len(table) else []
                    
                    combined_headers = []
                    for idx in range(max(len(row1), len(row2))):
                        c1 = str(row1[idx]).strip() if idx < len(row1) and row1[idx] else ''
                        c2 = str(row2[idx]).strip() if idx < len(row2) and row2[idx] else ''
                        combined_headers.append(f"{c1} {c2}".strip().lower().replace('\n', ' '))

                    # Map column indices
                    col_map = {}
                    for idx, header_lower in enumerate(combined_headers):
                        if not header_lower:
                            continue
                        # Field 3: Agency Item Number (AIN) - unique, permanent identifier
                        if 'agency' in header_lower and 'item' in header_lower:
                            col_map['series_id'] = idx
                        # Field 4: Record Series Item Number (RSIN) - reference to state schedule
                        elif 'rsin' in header_lower or ('record' in header_lower and 'series' in header_lower and 'item' in header_lower):
                            col_map['rsin'] = idx
                        elif 'series title' in header_lower or 'record series title' in header_lower:
                            col_map['series_title'] = idx
                        elif 'description' in header_lower:
                            col_map['description'] = idx
                        elif ('ret.' in header_lower and 'code' in header_lower) or 'edoc .ter' in header_lower:
                            col_map['retention_code'] = idx
                        elif ('retention' in header_lower and 'years' in header_lower) or 'sraey' in header_lower or 'years' in header_lower.split():
                            col_map['retention_years'] = idx
                        elif ('retention' in header_lower and 'months' in header_lower) or 'shtnom' in header_lower or 'months' in header_lower.split():
                            col_map['retention_months'] = idx
                        elif ('retention' in header_lower and 'days' in header_lower) or 'syad' in header_lower or 'days' in header_lower.split():
                            col_map['retention_days'] = idx
                        elif ('retention period' in header_lower) or ('retention' in header_lower):
                            # Default fallback if no specific units are found
                            if 'retention_years' not in col_map:
                                col_map['retention_years'] = idx
                            col_map['retention'] = idx
                        elif 'remark' in header_lower:
                            col_map['remarks'] = idx
                        elif 'legal' in header_lower or 'citation' in header_lower:
                            col_map['legal'] = idx
                        elif 'archival' in header_lower or 'lavihcra' in header_lower:
                            col_map['archival'] = idx

                    # Process data rows
                    for r_idx, row in enumerate(table):
                        if r_idx <= header_row_idx:
                            continue

                        # Skip empty rows
                        if not row or not any(row):
                            continue

                        # Skip header-like rows or rows with vertical reversed headers in cells
                        row_text_full = ' '.join(str(cell or '').lower() for cell in row)
                        if any(header in row_text_full for header in ['item no', 'rsin', 'record series', 'edoc .ter', 'lavihcra', 'sraey', 'shtnom', 'syad']):
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
                            'retention_months': '',
                            'retention_weeks': '',
                            'retention_days': '',
                            'retention_statement': '',
                            'disposition': '',
                            'confidential': '',
                            'legal_citation': '',
                            'comments': '',
                            'rsin': ''
                        })

                        # Extract other fields
                        if 'description' in col_map and len(row) > col_map['description']:
                            record['series_description'] = str(row[col_map['description']] or '').strip()

                        # Extract RSIN (Field 4: Record Series Item Number from state general schedule)
                        if 'rsin' in col_map and len(row) > col_map['rsin']:
                            record['rsin'] = str(row[col_map['rsin']] or '').strip()

                        # Extract Retention
                        ret_code = ''
                        ret_period_text = ''

                        if 'retention_code' in col_map and len(row) > col_map['retention_code']:
                            ret_code = str(row[col_map['retention_code']] or '').strip()
                            # Handle reversed text if it leaked into data
                            if ret_code == 'CA': ret_code = 'AC'
                            elif ret_code == 'VA': ret_code = 'AV'
                            elif ret_code == 'EC': ret_code = 'CE'
                            elif ret_code == 'EF': ret_code = 'FE'
                            elif ret_code == 'AL': ret_code = 'LA'
                            elif ret_code == 'MP': ret_code = 'PM'
                            elif ret_code == 'SU': ret_code = 'US'
                        
                        period_parts = []
                        if 'retention_years' in col_map and len(row) > col_map['retention_years']:
                            y_val = str(row[col_map['retention_years']] or '').strip()
                            if y_val: period_parts.append(f"{y_val} years")
                        if 'retention_months' in col_map and len(row) > col_map['retention_months']:
                            m_val = str(row[col_map['retention_months']] or '').strip()
                            if m_val: period_parts.append(f"{m_val} months")
                        if 'retention_days' in col_map and len(row) > col_map['retention_days']:
                            d_val = str(row[col_map['retention_days']] or '').strip()
                            if d_val: period_parts.append(f"{d_val} days")
                        
                        ret_period_text = " ".join(period_parts)

                        # If ret_code is missing but ret_period_text has something like "AC + 2"
                        if not ret_code and ret_period_text:
                            parsed = parse_retention_field(ret_period_text, retention_codes)
                            if parsed['retention_code']:
                                record.update(parsed)
                                ret_code = parsed['retention_code']
                        
                        if not ret_code and 'retention' in col_map and len(row) > col_map['retention']:
                            # Fallback to combined field if separate ones aren't found
                            retention_data = parse_retention_field(str(row[col_map['retention']] or ''), retention_codes)
                            record.update(retention_data)
                        elif ret_code:
                            record['retention_code'] = ret_code
                            # Parse units from period text
                            parsed_units = parse_retention_field(f"{ret_code} + {ret_period_text}", retention_codes)
                            record.update({
                                'retention_years': parsed_units['retention_years'],
                                'retention_months': parsed_units['retention_months'],
                                'retention_weeks': parsed_units['retention_weeks'],
                                'retention_days': parsed_units['retention_days'],
                                'retention_statement': parsed_units['retention_statement']
                            })

                        if 'remarks' in col_map and len(row) > col_map['remarks']:
                            record['comments'] = str(row[col_map['remarks']] or '').strip()

                        if 'legal' in col_map and len(row) > col_map['legal']:
                            record['legal_citation'] = str(row[col_map['legal']] or '').strip()

                        # If retention is still missing, check archival column
                        if not record['retention_code'] and 'archival' in col_map and len(row) > col_map['archival']:
                            archival = str(row[col_map['archival']] or '').strip().upper()
                            if archival == 'A' or 'ARCHIVE' in archival:
                                record['retention_code'] = 'PM'
                                record['retention_statement'] = 'Permanent'

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
