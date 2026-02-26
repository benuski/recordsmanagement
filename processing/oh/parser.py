import json
import re
import logging
import argparse
from datetime import date, datetime
from pathlib import Path
from bs4 import BeautifulSoup

from processing.oh.ohio import ohio_config
from extractor_engine import clean_record_fields, make_record

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_field_text(soup: BeautifulSoup, label_pattern: str) -> str:
    """Safely extracts the text next to a bold label in the Ohio DOM."""
    for b_tag in soup.find_all('b'):
        if re.search(label_pattern, b_tag.get_text(strip=True), re.IGNORECASE):
            parent = b_tag.parent
            if parent:
                full_text = parent.get_text(strip=True)
                return full_text.replace(b_tag.get_text(strip=True), '').strip()
    return ""

def process_ohio_html(html_file: Path, schema: dict) -> dict | None:
    """Parses a single Ohio HTML file into a standardized record."""
    record_id = html_file.stem
    source_url = f"https://rims.das.ohio.gov/Schedule/Details/{record_id}"

    with open(html_file, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
        
    try:
        # DOM Extraction
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
            
            # Extract Retention & Disposition Table
            if 'retention period' in headers:
                tbody = table.find('tbody')
                rows = tbody.find_all('tr') if tbody else table.find_all('tr')
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        ret_text = cols[0].get_text(strip=True)
                        media = cols[2].get_text(strip=True)
                        disp_text = cols[3].get_text(strip=True)

                        # Ohio-specific Pre-cleaning
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
            
            # Extract Date Table
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

        # Build raw record matching the output schema
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
        
        # Pass through the universal cleaner using the Ohio Config
        return clean_record_fields(raw_record, ohio_config)
        
    except Exception as e:
        logger.error(f"Error parsing {html_file.name}: {e}")
        return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parse Ohio HTML retention schedules into JSON.")
    parser.add_argument("--input-directory", required=True, type=Path, help="Directory containing the raw HTML files")
    parser.add_argument("--output-file", required=True, type=Path, help="Path to save the resulting JSON file")
    parser.add_argument("--schema-path", type=Path, default=Path("output_template_clean.json"), help="Path to the output JSON schema")
    args = parser.parse_args()

    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(args.schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)

    html_files = list(args.input_directory.glob('*.html'))
    logger.info(f"Found {len(html_files)} HTML files. Starting parser...")
    
    schedules = []
    for i, file_path in enumerate(html_files):
        record = process_ohio_html(file_path, schema)
        if record:
            schedules.append(record)
            
        if (i + 1) % 500 == 0:
            logger.info(f"Parsed {i+1}/{len(html_files)} files...")

    with open(args.output_file, 'w', encoding='utf-8') as f:
        json.dump(schedules, f, indent=4)
        
    logger.info(f"Done! Successfully extracted {len(schedules)} records to {args.output_file}")