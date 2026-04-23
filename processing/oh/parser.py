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
        schedules.append(universal_clean_record_fields(raw_record, ohio_config))

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
