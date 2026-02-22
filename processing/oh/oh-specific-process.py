import json
import re
from datetime import date, datetime
from pathlib import Path
from bs4 import BeautifulSoup

def clean_record_fields(record: dict) -> dict:
    # Use the EXACT SAME clean_record_fields function we perfected for the General Schedules here.
    # (Pasted below for completeness)
    title = re.sub(r'\s+', ' ', record['series_title']).strip()
    desc = re.sub(r'\s+', ' ', record['series_description']).strip()
    retention = re.sub(r'\s+', ' ', record['retention_statement']).strip()
    disposition = re.sub(r'\s+', ' ', record['disposition']).strip()

    if not disposition and re.search(r'(?i)permanently?', retention):
        disposition = "Permanent"
        retention = re.sub(r'(?i)\bpermanently?\b', '', retention).strip()
        retention = re.sub(r'(?i)^Retain[\s\.]*$', '', retention).strip()

    if not disposition:
        disp_match = re.search(r'(?i)(?:,\s*)?then\s+(.*)', retention)
        if disp_match:
            extracted_disp = disp_match.group(1).strip()
            
            oaks_match = re.search(r'(?i)(\.?\s*OAKS:.*)', extracted_disp)
            if oaks_match:
                oaks_text = oaks_match.group(1)
                extracted_disp = extracted_disp.replace(oaks_text, '').strip()
                retention = retention.replace(disp_match.group(0), oaks_text).strip()
            else:
                retention = retention.replace(disp_match.group(0), "").strip()

            extracted_disp = re.sub(r'[\.,;:]$', '', extracted_disp).strip()
            retention = re.sub(r'[\.,;:]$', '', retention).strip()
            
            if "archives" in extracted_disp.lower():
                if "possible" in extracted_disp.lower() or "review" in extracted_disp.lower():
                    disposition = extracted_disp[0].upper() + extracted_disp[1:] if extracted_disp else ""
                else:
                    disposition = "Permanent"
            else:
                disposition = extracted_disp[0].upper() + extracted_disp[1:] if extracted_disp else ""

    legal_citation = ""
    citation_pattern = r'(\bORC\s*\d+\.\d+|\b\d+\s*CFR\s*\d+|\b\d+\s*USC\s*\d+)'
    
    citation_match = re.search(citation_pattern, desc, re.IGNORECASE)
    if citation_match:
        legal_citation = citation_match.group(1).strip()
    else:
        citation_match = re.search(citation_pattern, retention, re.IGNORECASE)
        if citation_match:
            legal_citation = citation_match.group(1).strip()

    if 'permanent' in retention.lower() or 'permanent' in disposition.lower():
        retention_years = None
    else:
        retention_years_match = re.search(r'(\d+)\s*[Yy]ear', retention)
        if retention_years_match:
            retention_years = int(retention_years_match.group(1))
        else:
            word_to_num = {
                'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 
                'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
                'eleven': 11, 'twelve': 12, 'fifteen': 15, 'sixteen': 16
            }
            word_match = re.search(r'\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|fifteen|sixteen)\b\s*[Yy]ear', retention, re.IGNORECASE)
            if word_match:
                retention_years = word_to_num[word_match.group(1).lower()]
            else:
                retention_years = None

    is_confidential = (
        "confidential" in disposition.lower()
        and "non-confidential" not in disposition.lower()
    )

    record.update({
        'series_title': title,
        'series_description': desc,
        'retention_statement': retention,
        'retention_years': retention_years,
        'disposition': disposition,
        'confidential': is_confidential,
        'legal_citation': legal_citation
    })
    return record


def extract_field_text(soup, label_text):
    """Helper to find a label (like 'Agency :') and return the text immediately following it."""
    label_tag = soup.find('b', string=re.compile(label_text))
    if label_tag:
        parent = label_tag.parent
        # Get all text in the parent div, then remove the label part
        full_text = parent.get_text(strip=True)
        return full_text.replace(label_tag.get_text(strip=True), '').strip()
    return ""


def parse_ohio_specific_html(html_dir: Path, output_json: Path):
    schedules = []
    html_files = list(html_dir.glob('*.html'))
    
    print(f"Found {len(html_files)} HTML files to parse.")

    for i, file_path in enumerate(html_files):
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
            
        try:
            # 1. Extract Header Fields
            auth_number = extract_field_text(soup, r"Authorization Number\s*:")
            agency_code = extract_field_text(soup, r"Agency\s*:")
            series_no = extract_field_text(soup, r"Agency Series No\.\s*:")
            title = extract_field_text(soup, r"Record Title\s*:")
            desc = extract_field_text(soup, r"Record Description\s*:")
            
            # Combine auth_number and series_no as our unique identifier if needed, 
            # though series_no is usually the primary key in agency records.
            series_id = series_no if series_no else auth_number
            
            # 2. Extract Retention Tables
            # We look for the specific table headers to ensure we get the right table
            retention_statements = []
            dispositions = []
            tables = soup.find_all('table')
            
            for table in tables:
                headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                if 'retention period' in headers:
                    for row in table.find('tbody').find_all('tr'):
                        cols = row.find_all('td')
                        if len(cols) >= 4:
                            retention_text = cols[0].get_text(strip=True)
                            disposition_text = cols[3].get_text(strip=True)
                            
                            if retention_text:
                                retention_statements.append(retention_text)
                            if disposition_text and disposition_text.lower() != 'none':
                                dispositions.append(disposition_text)
            
            # Join multiple rows with a semicolon or newline
            combined_retention = " ; ".join(retention_statements)
            combined_disposition = " ; ".join(dispositions)
            
            # 3. Extract Latest Approval Date
            latest_date_str = None
            for table in tables:
                headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                if 'date' in headers and 'status' in headers:
                    dates = []
                    for row in table.find('tbody').find_all('tr'):
                        cols = row.find_all('td')
                        if len(cols) >= 4:
                            date_str = cols[3].get_text(strip=True)
                            try:
                                # Convert "1/16/2003 5:31:00 PM" into a date object
                                parsed_date = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
                                dates.append(parsed_date)
                            except ValueError:
                                pass
                    if dates:
                        latest_date_str = max(dates).strftime('%Y-%m-%d')

            # 4. Build and Clean the Record
            raw_record = {
                "state": "oh",
                "agency_name": agency_code, 
                "schedule_type": "specific",
                "schedule_id": auth_number, 
                "series_id": series_id,
                "series_title": title,
                "series_description": desc,
                "retention_statement": combined_retention,
                "disposition": combined_disposition, 
                "last_updated": latest_date_str, 
                "last_checked": str(date.today())
            }
            
            cleaned_record = clean_record_fields(raw_record)
            schedules.append(cleaned_record)
            
        except Exception as e:
            print(f"Error parsing {file_path.name}: {e}")

        # Log progress
        if (i + 1) % 500 == 0:
            print(f"Parsed {i+1}/{len(html_files)} files...")

    # Save to JSON
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(schedules, f, indent=4)
        
    print(f"Successfully extracted {len(schedules)} specific schedules into {output_json}")


if __name__ == '__main__':
    # Input: The directory where your downloader is currently saving the files
    input_dir = Path("../ohio_specific")
    
    # Output: The final combined JSON for all specific schedules
    output_file = Path("../../data/oh/specific.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    parse_ohio_specific_html(input_dir, output_file)