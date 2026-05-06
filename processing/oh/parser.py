import json
import re
import logging
from datetime import date, datetime
from pathlib import Path
from bs4 import BeautifulSoup
from word2number import w2n

from processing.oh.config import ohio_config
from processing.central_file import (
    make_record, 
    update_record, 
    get_nested_val, 
    clean_record_fields as universal_clean_record_fields
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ohio General Schedule Constants & Regexes
# ---------------------------------------------------------------------------

_WHITESPACE_RE      = re.compile(r'\s+')
_PERMANENT_RE       = re.compile(r'permanently?', re.IGNORECASE)
_RETAIN_EMPTY_RE    = re.compile(r'^Retain[\s\.]*$', re.IGNORECASE)
_THEN_DISP_RE       = re.compile(r'(?:,\s*)?then\s+(.*)', re.IGNORECASE)
_OAKS_RE            = re.compile(r'\.?\s*OAKS:.*', re.IGNORECASE)
_TRAILING_PUNCT_RE  = re.compile(r'[\.,;:]$')
_DIGIT_YEAR_RE      = re.compile(r'\(?(\d+)\)?\s*year', re.IGNORECASE)
_WORD_NUM_RE        = re.compile(r'\b([a-zA-Z]+(?:-[a-zA-Z]+)?)\b\s*year', re.IGNORECASE)
_LEGAL_CITATION_RE  = re.compile(r'(\bORC\s*\d+\.\d+|\b\d+\s*CFR\s*\d+|\b\d+\s*USC\s*\d+)', re.IGNORECASE)

# ---------------------------------------------------------------------------
# Ohio General Schedule Helpers
# ---------------------------------------------------------------------------
def _normalize(s: str) -> str:
    return _WHITESPACE_RE.sub(' ', s).strip()

def _cap_first(s: str) -> str:
    return s[0].upper() + s[1:] if s else ""

def _clean_punct(s: str) -> str:
    return _TRAILING_PUNCT_RE.sub('', s).strip()

def clean_ohio_general_record(record: dict) -> dict:
    title       = _normalize(get_nested_val(record, 'series_title') or '')
    desc        = _normalize(get_nested_val(record, 'series_description') or '')
    retention   = _normalize(get_nested_val(record, 'retention_statement') or '')
    disposition = _normalize(get_nested_val(record, 'disposition') or '')

    if not disposition and _PERMANENT_RE.search(retention):
        disposition = "Permanent"
        retention = _PERMANENT_RE.sub('', retention).strip()
        retention = _RETAIN_EMPTY_RE.sub('', retention).strip()

    if not disposition:
        disp_match = _THEN_DISP_RE.search(retention)
        if disp_match:
            extracted_disp = disp_match.group(1).strip()

            oaks_match = _OAKS_RE.search(extracted_disp)
            if oaks_match:
                oaks_text = oaks_match.group(0)
                extracted_disp = extracted_disp.replace(oaks_text, '').strip()
                retention = retention.replace(disp_match.group(0), oaks_text).strip()
            else:
                retention = retention.replace(disp_match.group(0), '').strip()

            extracted_disp = _clean_punct(extracted_disp)
            retention = _clean_punct(retention)

            disp_lower = extracted_disp.lower()
            if 'archives' in disp_lower and not ('possible' in disp_lower or 'review' in disp_lower):
                disposition = "Permanent"
            else:
                disposition = _cap_first(extracted_disp)

    m = _LEGAL_CITATION_RE.search(desc) or _LEGAL_CITATION_RE.search(retention)
    legal_citation = m.group(1).strip() if m else ""

    if 'permanent' in retention.lower() or 'permanent' in disposition.lower():
        retention_years = None
    else:
        digit_m = _DIGIT_YEAR_RE.search(retention)
        if digit_m:
            retention_years = int(digit_m.group(1))
        else:
            word_m = _WORD_NUM_RE.search(retention)
            if word_m:
                try:
                    retention_years = w2n.word_to_num(word_m.group(1).lower())
                except ValueError:
                    retention_years = None
            else:
                retention_years = None

    disp_lower = disposition.lower()
    is_confidential = 'confidential' in disp_lower and 'non-confidential' not in disp_lower

    update_record(record, 
        series_title=title,
        series_description=desc,
        retention_statement=retention,
        retention_years=retention_years,
        disposition=disposition,
        confidential=is_confidential,
        legal_citation=legal_citation
    )
    return record

# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def process_ohio_general_html(html_file: Path, schema: dict) -> list[dict]:
    """Parses Ohio General Schedules (table-based HTML) into a list of records."""
    with open(html_file, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table_body = soup.find('tbody')
    if not table_body:
        logger.error("Could not find a <tbody> in %s", html_file.name)
        return []

    schedules = []
    for row in table_body.find_all('tr'):
        cols = row.find_all('td')

        if len(cols) != 4:
            logger.warning("Skipping row with %d column(s) in %s", len(cols), html_file.name)
            continue

        raw_record = make_record(
            schema,
            state="oh",
            agency_name="",
            schedule_type="general",
            schedule_id="General",
            series_id=cols[0].get_text(separator=' ', strip=True),
            series_title=cols[1].get_text(separator=' ', strip=True),
            series_description=cols[2].get_text(separator=' ', strip=True),
            retention_statement=cols[3].get_text(separator=' ', strip=True),
            disposition="",
            last_updated=None,
            last_checked=str(date.today()),
            url="https://rims.das.ohio.gov/GeneralSchedule"
        )
        schedules.append(clean_ohio_general_record(raw_record))

    return schedules

def extract_field_text(soup: BeautifulSoup, label_pattern: str) -> str:
    for b_tag in soup.find_all('b'):
        if re.search(label_pattern, b_tag.get_text(strip=True), re.IGNORECASE):
            parent = b_tag.parent
            if parent:
                full_text = parent.get_text(strip=True)
                return full_text.replace(b_tag.get_text(strip=True), '').strip()
    return ""

def process_ohio_html(html_file: Path, schema: dict) -> dict | None:
    """Parses Ohio Specific Agency Schedules (detail page DOM) into a standardized record."""
    record_id = html_file.stem.replace("spec_", "")
    source_url = f"https://rims.das.ohio.gov/Schedule/Details/{record_id}"

    with open(html_file, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
        
    try:
        auth_number = extract_field_text(soup, r"Authorization Number\s*:")
        agency_code = extract_field_text(soup, r"Agency\s*:")
        series_no = extract_field_text(soup, r"Agency Series No\.?\s*:")
        title = extract_field_text(soup, r"Record Title\s*:")
        desc = extract_field_text(soup, r"Record Description\s*:")
        
        series_id = series_no if series_no else auth_number
        
        retention_statements, dispositions = [], []
        latest_date_str = None
        tables = soup.find_all('table')
        
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            
            if 'retention period' in headers:
                tbody = table.find('tbody')
                rows = tbody.find_all('tr') if tbody else table.find_all('tr')
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        ret_text = cols[0].get_text(strip=True)
                        media = cols[2].get_text(strip=True)
                        disp_text = cols[3].get_text(strip=True)

                        disp_match = re.search(r'(?i)(?:,\s*)?then\s+(.*)', ret_text)
                        if disp_match:
                            extracted_disp = disp_match.group(1).strip()
                            oaks_match = re.search(r'(?i)(\.?\s*OAKS:.*)', extracted_disp)
                            if oaks_match:
                                oaks_text = oaks_match.group(1)
                                extracted_disp = extracted_disp.replace(oaks_text, '').strip()
                                ret_text = ret_text.replace(disp_match.group(0), oaks_text).strip()
                            else:
                                ret_text = ret_text.replace(disp_match.group(0), "").strip()
                            
                            extracted_disp = re.sub(r'[\.,;:]$', '', extracted_disp).strip()
                            if not disp_text or disp_text.lower() == 'none':
                                disp_text = extracted_disp.title()
                        
                        prefix = f"{media}: " if media and media.lower() not in ['none', 'n/a', '-', ''] else ""
                        if ret_text: retention_statements.append(f"{prefix}{ret_text}")
                        if disp_text and disp_text.lower() != 'none': dispositions.append(f"{prefix}{disp_text.title()}")
            
            if 'date' in headers and 'status' in headers:
                dates = []
                tbody = table.find('tbody')
                rows = tbody.find_all('tr') if tbody else table.find_all('tr')
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        try:
                            dates.append(datetime.strptime(cols[3].get_text(strip=True), "%m/%d/%Y %I:%M:%S %p"))
                        except ValueError:
                            pass
                if dates:
                    latest_date_str = max(dates).strftime('%Y-%m-%d')

        raw_record = make_record(
            schema,
            state="oh",
            agency_name=agency_code, 
            schedule_type="specific",
            schedule_id=auth_number, 
            series_id=series_id,
            series_title=title,
            series_description=desc,
            retention_statement=" ; ".join(retention_statements),
            disposition=" ; ".join(dispositions), 
            last_updated=latest_date_str, 
            last_checked=str(date.today()),
            url=source_url
        )
        
        return universal_clean_record_fields(raw_record, ohio_config)
        
    except Exception as e:
        logger.error(f"Error parsing {html_file.name}: {e}")
        return None
