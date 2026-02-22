import json
import re
from datetime import date, datetime
from pathlib import Path
from bs4 import BeautifulSoup

def clean_record_fields(record: dict) -> dict:
    title = re.sub(r'\s+', ' ', record['series_title']).strip()
    desc = re.sub(r'\s+', ' ', record['series_description']).strip()
    retention = re.sub(r'\s+', ' ', record['retention_statement']).strip()
    disposition = re.sub(r'\s+', ' ', record['disposition']).strip()

    # Look for Ohio Revised Code (ORC) or Federal citations
    legal_citation = ""
    citation_pattern = r'(\bORC\s*\d+\.\d+|\b\d+\s*CFR\s*\d+|\b\d+\s*USC\s*\d+)'
    
    citation_match = re.search(citation_pattern, desc, re.IGNORECASE)
    if citation_match:
        legal_citation = citation_match.group(1).strip()
    else:
        citation_match = re.search(citation_pattern, retention, re.IGNORECASE)
        if citation_match:
            legal_citation = citation_match.group(1).strip()

    # Extract numeric years
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

    # Update: Set to True if it mentions "confidential" OR if the disposition involves shredding
    is_confidential = (
        ("confidential" in disposition.lower() and "non-confidential" not in disposition.lower()) or
        "shred" in disposition.lower()
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

def extract_field_text(soup, label_pattern):
    """Safely extracts the text next to a bold label."""
    for b_tag in soup.find_all('b'):
        if re.search(label_pattern, b_tag.get_text(strip=True), re.IGNORECASE):
            parent = b_tag.parent
            if parent:
                full_text = parent.get_text(strip=True)
                return full_text.replace(b_tag.get_text(strip=True), '').strip()
    return ""


def parse_ohio_specific_html(html_dir: Path, output_json: Path):
    if not html_dir.exists():
        print(f"CRITICAL ERROR: The directory {html_dir.resolve()} does not exist.")
        return

    html_files = list(html_dir.glob('*.html'))
    if len(html_files) == 0:
        print(f"CRITICAL ERROR: No HTML files found in {html_dir.resolve()}.")
        return
        
    print(f"Found {len(html_files)} HTML files. Starting parser...")
    
    schedules = []
    error_count = 0

    for i, file_path in enumerate(html_files):
        # Extract the unique database ID from the filename (e.g., "18613" from "18613.html")
        record_id = file_path.stem
        source_url = f"https://rims.das.ohio.gov/Schedule/Details/{record_id}"

        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
            
        try:
            # 1. Extract Header Fields
            auth_number = extract_field_text(soup, r"Authorization Number\s*:")
            agency_code = extract_field_text(soup, r"Agency\s*:")
            series_no = extract_field_text(soup, r"Agency Series No\.?\s*:")
            title = extract_field_text(soup, r"Record Title\s*:")
            desc = extract_field_text(soup, r"Record Description\s*:")
            
            series_id = series_no if series_no else auth_number
            
            # 2. Extract Retention Tables Safely
            retention_statements = []
            dispositions = []
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
                            
                            # Catch explicit "permanently"
                            if re.search(r'(?i)permanently?', ret_text):
                                disp_text = "Permanent"
                                ret_text = re.sub(r'(?i)\bpermanently?\b', '', ret_text).strip()
                                ret_text = re.sub(r'(?i)^Retain[\s\.]*$', '', ret_text).strip()

                            # Remove redundant ", then [disposition]" from the retention text
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
                                
                                # Use the extracted disposition if the column was blank
                                if not disp_text or disp_text.lower() == 'none':
                                    disp_text = extracted_disp.title()
                            
                            # Apply the Media Prefix
                            prefix = f"{media}: " if media and media.lower() not in ['none', 'n/a', '-', ''] else ""
                            
                            if ret_text:
                                retention_statements.append(f"{prefix}{ret_text}")
                            if disp_text and disp_text.lower() != 'none':
                                dispositions.append(f"{prefix}{disp_text.title()}")
            
            combined_retention = " ; ".join(retention_statements)
            combined_disposition = " ; ".join(dispositions)
            
            # 3. Extract Latest Approval Date
            latest_date_str = None
            for table in tables:
                headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                if 'date' in headers and 'status' in headers:
                    dates = []
                    tbody = table.find('tbody')
                    rows = tbody.find_all('tr') if tbody else table.find_all('tr')
                    
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 4:
                            date_str = cols[3].get_text(strip=True)
                            try:
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
                "last_checked": str(date.today()),
                "url": source_url  # <-- Added your URL field here
            }
            
            cleaned_record = clean_record_fields(raw_record)
            schedules.append(cleaned_record)
            
        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"Error parsing {file_path.name}: {e}")

        # Log progress
        if (i + 1) % 500 == 0:
            print(f"Parsed {i+1}/{len(html_files)} files...")

    # Save to JSON
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(schedules, f, indent=4)
        
    print(f"Done! Successfully extracted {len(schedules)} records.")


if __name__ == '__main__':
    input_dir = Path("ohio_specific")
    output_file = Path("../../data/oh/specific.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    parse_ohio_specific_html(input_dir, output_file)