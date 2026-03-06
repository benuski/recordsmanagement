import json
import re
import csv
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retention Codes Loading
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Structure Traversal Helpers
# ---------------------------------------------------------------------------
def find_nodes_by_type(node, node_type: str) -> list:
    """Recursively find all nodes of a specific type in the document tree."""
    results = []
    if isinstance(node, dict):
        if node.get('type') == node_type:
            results.append(node)
        if 'children' in node:
            for child in node['children']:
                results.extend(find_nodes_by_type(child, node_type))
    elif isinstance(node, list):
        for item in node:
            results.extend(find_nodes_by_type(item, node_type))
    return results


def extract_text_from_node(node) -> str:
    """Extract all text from a node and its children."""
    text_parts = []

    if isinstance(node, dict):
        # Direct text in this node
        if 'text' in node:
            text_parts.extend(node['text'])

        # Recurse into children
        if 'children' in node:
            for child in node['children']:
                text_parts.append(extract_text_from_node(child))

    return ' '.join(text_parts).strip()


# ---------------------------------------------------------------------------
# Metadata Extraction
# ---------------------------------------------------------------------------
def extract_metadata_from_structure(structure: dict, pdf_path: Path) -> dict:
    """Extract metadata from cover pages (dates, agency info, etc.)."""
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

    # Find all paragraph nodes on early pages (cover pages)
    paragraphs = find_nodes_by_type(structure, 'P')

    for para in paragraphs[:100]:  # Check first 100 paragraphs (cover and cert pages)
        text = extract_text_from_node(para)

        # Look for last updated date (e.g., "4/21/2025")
        if not metadata['last_updated']:
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
            if date_match:
                try:
                    date_obj = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                    metadata['last_updated'] = date_obj.strftime('%Y-%m-%d')
                except:
                    pass

        # Look for next update (e.g., "April 2030")
        if not metadata['next_update']:
            next_match = re.search(r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})', text, re.IGNORECASE)
            if next_match:
                try:
                    date_obj = datetime.strptime(next_match.group(1), '%B %Y')
                    metadata['next_update'] = date_obj.strftime('%Y-%m')
                except:
                    pass

        # Look for Agency Code in "Section 1. Agency Information"
        if not metadata['schedule_id']:
            code_match = re.search(r'Agency\s+Code[:\s]+(\d{3,4})', text, re.IGNORECASE)
            if code_match:
                metadata['schedule_id'] = code_match.group(1)

        # Look for Agency Name in "Section 1. Agency Information"
        if not metadata['agency_name']:
            name_match = re.search(r'Agency\s+Name[:\s]+(.+?)(?:\s*$|\s*\d)', text, re.IGNORECASE)
            if name_match:
                metadata['agency_name'] = name_match.group(1).strip()

    # Determine schedule type
    if metadata['schedule_id']:
        metadata['schedule_type'] = 'agency-specific'
    else:
        metadata['schedule_type'] = 'general'

    # Build URL from schedule_id
    if metadata['schedule_id']:
        metadata['url'] = f"https://www.tsl.texas.gov/sites/default/files/public/tslac/slrm/state/schedules/{metadata['schedule_id']}.pdf"

    return metadata


# ---------------------------------------------------------------------------
# Table Parsing
# ---------------------------------------------------------------------------
def parse_table_row(row_node, retention_codes: dict) -> dict | None:
    """Parse a single table row into a record dict."""
    cells = [c for c in row_node.get('children', []) if c.get('type') in ['TD', 'TH']]

    if len(cells) < 3:  # Need at least series_id, title, and retention
        return None

    # Extract text from each cell
    cell_texts = [extract_text_from_node(cell) for cell in cells]

    # Skip header rows
    first_cell = cell_texts[0].lower() if cell_texts else ''
    if any(header in first_cell for header in ['record series', 'item no', 'rsin', 'agency item', 'ain']):
        return None

    # Skip empty rows
    if not any(cell_texts):
        return None

    record = {
        'series_id': '',
        'series_title': '',
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
        'comments': ''
    }

    # Texas table structure (based on 601.pdf):
    # 0: Item No (RSIN/series_id)
    # 1: Record Series Title
    # 2-9: Various columns depending on schedule
    # 10: Retention Period
    # 11: Remarks (comments)
    # 12: Legal Citations

    # Extract based on typical positions, but be flexible
    if len(cell_texts) >= 1:
        record['series_id'] = cell_texts[0].strip()

    if len(cell_texts) >= 2:
        record['series_title'] = cell_texts[1].strip()

    # Find retention column (usually has pattern like "AC + 2" or "PM")
    retention_col_idx = None
    for idx, cell_text in enumerate(cell_texts):
        if re.match(r'^[A-Z]{2,3}(\s*\+\s*\d+)?$', cell_text.strip()):
            retention_col_idx = idx
            record['retention_statement'] = cell_text.strip()
            break

    # If we found retention, extract surrounding columns
    if retention_col_idx:
        # Description might be before retention (multiple columns merged)
        desc_parts = []
        for idx in range(2, retention_col_idx):
            if cell_texts[idx].strip():
                desc_parts.append(cell_texts[idx].strip())
        if desc_parts:
            record['series_description'] = ' '.join(desc_parts)

        # Remarks column (typically after retention)
        if retention_col_idx + 1 < len(cell_texts):
            remarks = cell_texts[retention_col_idx + 1].strip()
            if remarks:
                record['comments'] = remarks

        # Legal citations column (typically after remarks)
        if retention_col_idx + 2 < len(cell_texts):
            legal = cell_texts[retention_col_idx + 2].strip()
            if legal:
                record['legal_citation'] = legal

    # Alternative: if no clear retention pattern, use fixed column indices
    if not retention_col_idx:
        # Assume standard layout
        if len(cell_texts) >= 3:
            record['series_description'] = cell_texts[2].strip()
        if len(cell_texts) >= 11:
            record['retention_statement'] = cell_texts[10].strip()
        if len(cell_texts) >= 12:
            record['comments'] = cell_texts[11].strip()
        if len(cell_texts) >= 13:
            record['legal_citation'] = cell_texts[12].strip()

    # Parse retention code and periods from retention_statement
    if record['retention_statement']:
        # Basic code extraction
        code_match = re.match(r'^([A-Z]{2,3})', record['retention_statement'])
        if code_match:
            record['retention_code'] = code_match.group(1)
            
            # Look for numbers with units
            years_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:year|yr)', record['retention_statement'], re.IGNORECASE)
            months_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:month|mo)', record['retention_statement'], re.IGNORECASE)
            weeks_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:week|wk)', record['retention_statement'], re.IGNORECASE)
            days_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:day)', record['retention_statement'], re.IGNORECASE)

            if years_match: record['retention_years'] = years_match.group(1)
            if months_match: record['retention_months'] = months_match.group(1)
            if weeks_match: record['retention_weeks'] = weeks_match.group(1)
            if days_match: record['retention_days'] = days_match.group(1)
            
            # If no explicit units but has a number like "AC + 2"
            if not any([record['retention_years'], record['retention_months'], record['retention_weeks'], record['retention_days']]):
                num_match = re.search(r'\+\s*(\d+(?:\.\d+)?)', record['retention_statement'])
                if num_match:
                    # Default to years if no unit specified
                    record['retention_years'] = num_match.group(1)

            # Build full retention statement using codes CSV
            if record['retention_code'] in retention_codes:
                code_info = retention_codes[record['retention_code']]
                title = code_info['title']
                
                parts = []
                if record['retention_years']:
                    label = 'year' if record['retention_years'] == '1' else 'years'
                    parts.append(f"{record['retention_years']} {label}")
                if record['retention_months']:
                    label = 'month' if record['retention_months'] == '1' else 'months'
                    parts.append(f"{record['retention_months']} {label}")
                if record['retention_weeks']:
                    label = 'week' if record['retention_weeks'] == '1' else 'weeks'
                    parts.append(f"{record['retention_weeks']} {label}")
                if record['retention_days']:
                    label = 'day' if record['retention_days'] == '1' else 'days'
                    parts.append(f"{record['retention_days']} {label}")
                    
                if parts:
                    record['retention_statement'] = f"{title} plus {' and '.join(parts)}"
                else:
                    record['retention_statement'] = title

    # If retention is still missing, check archival column (index 9)
    if not record['retention_code'] and len(cell_texts) > 9:
        archival = cell_texts[9].strip().upper()
        if archival == 'A' or 'ARCHIVE' in archival:
            record['retention_code'] = 'PM'
            record['retention_statement'] = 'Permanent'

    # Skip records without essential fields
    if not record['series_id'] or not record['series_title']:
        return None

    return record


def parse_texas_structure(structure_json_path: Path, output_schema: dict, retention_codes_path: Path) -> list[dict]:
    """
    Parse a Texas PDF using pdfplumber structure JSON.

    Args:
        structure_json_path: Path to the pdfplumber --structure-text JSON file
        output_schema: Output record schema template
        retention_codes_path: Path to retentioncodes.csv

    Returns:
        List of standardized record dictionaries
    """
    logger.info(f"Parsing Texas structure from {structure_json_path}")

    # Load retention codes
    retention_codes = load_retention_codes(retention_codes_path)
    logger.info(f"Loaded {len(retention_codes)} retention codes")

    # Load structure JSON
    with open(structure_json_path, 'r', encoding='utf-8') as f:
        structure = json.load(f)

    # Extract metadata from cover pages
    metadata = extract_metadata_from_structure(structure, structure_json_path)
    logger.info(f"Extracted metadata: schedule_id={metadata['schedule_id']}, agency={metadata['agency_name']}")

    # Find all tables in the document
    tables = find_nodes_by_type(structure, 'Table')
    logger.info(f"Found {len(tables)} tables")

    records = []

    for table_idx, table in enumerate(tables):
        rows = [c for c in table.get('children', []) if c.get('type') == 'TR']
        logger.info(f"Table {table_idx + 1}: {len(rows)} rows")

        for row in rows:
            record = parse_table_row(row, retention_codes)
            if record:
                # Apply metadata to each record
                record.update({
                    'state': metadata['state'],
                    'schedule_type': metadata['schedule_type'],
                    'schedule_id': metadata['schedule_id'],
                    'last_updated': metadata['last_updated'],
                    'next_update': metadata['next_update'],
                    'last_checked': metadata['last_checked'],
                    'url': metadata['url']
                })

                # If agency_name is in metadata, add it
                if metadata['agency_name']:
                    record['agency_name'] = metadata['agency_name']

                # Ensure all schema fields are present (pass through empty fields)
                for field in output_schema:
                    if field not in record:
                        record[field] = output_schema[field]

                records.append(record)

    logger.info(f"Extracted {len(records)} records from {len(tables)} tables")
    return records