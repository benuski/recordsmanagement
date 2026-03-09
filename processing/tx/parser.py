"""
Texas-specific parsers for both agency index HTML and retention schedule PDFs.
Consolidated from tx_pdf_processor.py and parse_agencies.py.
"""

import re
import csv
import logging
import pdfplumber
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

from processing.tx.config import texas_config
from processing.utils import make_record, clean_record_fields

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTML Agency Index Parser
# ---------------------------------------------------------------------------

def parse_agencies_html(html_path: Path) -> dict:
    """
    Parse agencies.html and return a dict mapping schedule_id to agency info.
    Returns: dict: {schedule_id: {name, last_updated, next_update}}
    """
    agencies = {}
    if not html_path.exists():
        logger.warning(f"Texas agencies.html not found at {html_path}")
        return agencies

    with open(html_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows[1:]:  # Skip header row
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            agency_cell = cells[0].get_text(strip=True)
            match = re.match(r'(.+?)\((\d{3,4})\)', agency_cell)
            if match:
                agency_name = match.group(1).strip()
                schedule_id = match.group(2)

                approval_date = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                next_recert = cells[3].get_text(strip=True) if len(cells) > 3 else ''

                last_updated = ''
                if approval_date:
                    try:
                        date_obj = datetime.strptime(approval_date, '%Y-%m-%d')
                        last_updated = date_obj.strftime('%Y-%m-%d')
                    except: pass

                next_update = next_recert if re.match(r'\d{4}-\d{2}', next_recert) else ''

                agencies[schedule_id] = {
                    'name': agency_name,
                    'last_updated': last_updated,
                    'next_update': next_update
                }
    return agencies

# ---------------------------------------------------------------------------
# PDF Retention Schedule Parser
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

def extract_metadata_from_pdf(pdf_path: Path) -> dict:
    """Extract metadata from the first few pages of the PDF."""
    metadata = {
        'state': 'tx',
        'schedule_type': 'specific',
        'last_updated': '',
        'next_update': '',
        'agency_name': '',
        'schedule_id': '',
        'last_checked': datetime.now().strftime("%Y-%m-%d"),
        'url': ''
    }

    filename_match = re.match(r'^(\d{3,4})\.pdf$', pdf_path.name)
    if filename_match:
        metadata['schedule_id'] = filename_match.group(1)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:3]:
                text = page.extract_text()
                if not text: continue

                if not metadata['last_updated']:
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
                    if date_match:
                        try:
                            date_obj = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                            metadata['last_updated'] = date_obj.strftime('%Y-%m-%d')
                        except: pass

                if not metadata['schedule_id']:
                    code_match = re.search(r'Agency\s+Code[:\s]+(\d{3,4})', text, re.IGNORECASE)
                    if code_match: metadata['schedule_id'] = code_match.group(1)

                if not metadata['agency_name']:
                    name_match = re.search(r'Agency\s+Name[:\s]+(.+?)(?:\n|$)', text, re.IGNORECASE)
                    if name_match: metadata['agency_name'] = name_match.group(1).strip()
    except Exception as e:
        logger.error(f"Error extracting metadata from {pdf_path}: {e}")

    if metadata['schedule_id']:
        metadata['url'] = f"https://www.tsl.texas.gov/sites/default/files/public/tslac/slrm/state/schedules/{metadata['schedule_id']}.pdf"
    
    return metadata

def parse_retention_field(retention_text: str, retention_codes: dict) -> dict:
    """Parse retention code and periods from a raw retention string."""
    result = {
        'retention_code': '',
        'retention_years': '',
        'retention_months': '',
        'retention_weeks': '',
        'retention_days': '',
        'retention_statement': ''
    }
    if not retention_text: return result

    retention_text = retention_text.strip()
    code_match = re.match(r'^([A-Z]{2,3})\b', retention_text)
    if code_match: result['retention_code'] = code_match.group(1)
        
    for unit in ['year', 'month', 'week', 'day']:
        m = re.search(fr'(\d+(?:\.\d+)?)\s*(?:{unit})', retention_text, re.IGNORECASE)
        if m: result[f'retention_{unit}s'] = m.group(1)
    
    if not any([result['retention_years'], result['retention_months'], result['retention_weeks'], result['retention_days']]):
        num_match = re.search(r'\+\s*(\d+(?:\.\d+)?)', retention_text)
        if num_match: result['retention_years'] = num_match.group(1)

    parts = []
    for unit in ['year', 'month', 'week', 'day']:
        val = result[f'retention_{unit}s']
        if val: parts.append(f"{val} {unit}{'s' if val != '1' else ''}")

    if result['retention_code'] and result['retention_code'] in retention_codes:
        title = retention_codes[result['retention_code']]['title']
        result['retention_statement'] = f"{title} plus {' and '.join(parts)}" if parts else title
    elif parts:
        result['retention_statement'] = " and ".join(parts)

    return result

def process_texas_pdf(pdf_path: Path, schema: dict, retention_codes_path: Path, agency_mapping: dict = None) -> list[dict]:
    """Process a Texas retention schedule PDF and extract records."""
    logger.info(f"Processing Texas PDF: {pdf_path}")
    retention_codes = load_retention_codes(retention_codes_path)
    metadata = extract_metadata_from_pdf(pdf_path)

    if agency_mapping and metadata['schedule_id'] in agency_mapping:
        agency_info = agency_mapping[metadata['schedule_id']]
        metadata['agency_name'] = agency_info['name']
        if not metadata['last_updated']: metadata['last_updated'] = agency_info['last_updated']
        if not metadata['next_update']: metadata['next_update'] = agency_info['next_update']

    records = []
    current_col_map = None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                if not tables: continue

                for table in tables:
                    if not table: continue
                    header_row_idx = -1
                    for r_idx, row in enumerate(table[:5]):
                        if row and any(cell and ("itemno" in "".join(str(cell).lower().split()) or "item#" in "".join(str(cell).lower().split())) for cell in row):
                            header_row_idx = r_idx
                            break

                    if header_row_idx == -1:
                        if current_col_map: col_map = current_col_map
                        else: continue
                    else:
                        row1, row2 = table[header_row_idx], (table[header_row_idx + 1] if header_row_idx + 1 < len(table) else [])
                        combined_headers = [f"{str(row1[i] or '').strip()} {str(row2[i] or '').strip()}".strip().lower() for i in range(max(len(row1), len(row2)))]
                        
                        col_map = {}
                        for idx, h in enumerate(combined_headers):
                            clean_h = "".join(h.split())
                            if 'agency' in clean_h and 'item' in clean_h: col_map['series_id'] = idx
                            elif 'rsin' in clean_h or ('record' in clean_h and 'series' in clean_h and 'item' in clean_h): col_map['rsin'] = idx
                            elif 'seriestitle' in clean_h or 'recordseriestitle' in clean_h: col_map['series_title'] = idx
                            elif 'description' in clean_h: col_map['description'] = idx
                            elif ('ret.' in clean_h and 'code' in clean_h) or 'edoc.ter' in clean_h or 'retcode' in clean_h: col_map['retention_code'] = idx
                            elif 'years' in h.split() or 'sraey' in clean_h: col_map['retention_years'] = idx
                            elif 'months' in h.split() or 'shtnom' in clean_h: col_map['retention_months'] = idx
                            elif 'days' in h.split() or 'syad' in clean_h: col_map['retention_days'] = idx
                            elif 'remark' in clean_h: col_map['remarks'] = idx
                            elif 'legal' in clean_h or 'citation' in clean_h: col_map['legal'] = idx
                            elif 'archival' in clean_h or 'lavihcra' in clean_h: col_map['archival'] = idx
                        current_col_map = col_map

                    for r_idx, row in enumerate(table):
                        if r_idx <= header_row_idx or not row or not any(row): continue
                        row_text_full = ' '.join(str(cell or '').lower() for cell in row)
                        if any(kw in row_text_full for kw in ['item no', 'rsin', 'record series', 'edoc .ter', 'lavihcra']): continue

                        series_id = str(row[col_map.get('series_id', 0)] or '').strip() if len(row) > col_map.get('series_id', 0) else ''
                        series_title = str(row[col_map.get('series_title', 1)] or '').strip() if len(row) > col_map.get('series_title', 1) else ''
                        
                        # Skip if no series_id, no title, or if series_id doesn't match the pattern (e.g., TOC entries)
                        if not series_id or not series_title or not texas_config.series_id_pattern.match(series_id):
                            continue

                        raw_record = make_record(
                            schema,
                            series_id=series_id,
                            series_title=series_title,
                            series_description=str(row[col_map['description']] or '').strip() if 'description' in col_map and len(row) > col_map['description'] else '',
                            rsin=str(row[col_map['rsin']] or '').strip() if 'rsin' in col_map and len(row) > col_map['rsin'] else '',
                            comments=str(row[col_map['remarks']] or '').strip() if 'remarks' in col_map and len(row) > col_map['remarks'] else '',
                            legal_citation=str(row[col_map['legal'] or ''] or '').strip() if 'legal' in col_map and len(row) > col_map['legal'] else '',
                            **metadata
                        )

                        ret_code = str(row[col_map['retention_code']] or '').strip() if 'retention_code' in col_map and len(row) > col_map['retention_code'] else ''
                        # Handle mirrored text
                        ret_code = "".join(reversed(ret_code)) if ret_code in ["CA", "VA", "EC", "EF", "AL", "MP", "SU"] else ret_code
                        
                        period_text = " ".join([f"{str(row[col_map[k]] or '').strip()} {k.split('_')[1]}" for k in ['retention_years', 'retention_months', 'retention_days'] if k in col_map and len(row) > col_map[k] and str(row[col_map[k]] or '').strip()])
                        
                        parsed = parse_retention_field(f"{ret_code} + {period_text}", retention_codes)
                        raw_record.update(parsed)

                        if not raw_record['retention_code'] and 'archival' in col_map and len(row) > col_map['archival']:
                            if 'A' in str(row[col_map['archival']]).upper():
                                raw_record.update({'retention_code': 'PM', 'retention_statement': 'Permanent'})

                        records.append(clean_record_fields(raw_record, texas_config))

    except Exception as e:
        logger.error(f"Error processing PDF {pdf_path}: {e}")
        raise
    return records
