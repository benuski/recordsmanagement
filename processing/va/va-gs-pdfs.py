import pdfplumber
import json
import re
import os
from pathlib import Path
from datetime import datetime, date

def extract_effective_date(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0].extract_text()
        if first_page:
            match = re.search(r'(?i)(?:EFFECTIVE.*?DATE|:)\s*(\d{1,2}/\d{1,2}/\d{4})', first_page)
            if match:
                return datetime.strptime(match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
    return None

def stringify_words(word_list):
    """Sorts words top-to-bottom, left-to-right, and cleans up line-break hyphenation."""
    if not word_list: return ""
    # Round top coordinates to nearest 4 pixels to account for baseline jitter
    word_list.sort(key=lambda w: (round(w['top']/4)*4, w['x0']))
    text = " ".join([w['text'] for w in word_list])
    return re.sub(r'-\s+', '', text).strip()

def process_pdf_to_json(pdf_path, output_dir):
    schedule_id = Path(pdf_path).stem
    effective_date = extract_effective_date(pdf_path)
    
    all_records = []
    current_record = None
    
    # Default column gutters (Will calibrate automatically on page 1)
    g1 = 150 # Between Desc and Series No.
    g2 = 300 # Between Series No. and Retention
    g3 = 500 # Between Retention and Disposition

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(keep_blank_chars=False)
            if not words: continue

            page_header_bottom = 0
            found_headers = False

            # 1. Calibrate the vertical silos by finding the exact X-coordinates of the headers
            for i, w in enumerate(words):
                text = w['text'].lower()
                # Only check the top portion of the page for headers
                if w['top'] > 250: continue 
                
                if text == "series" and i+1 < len(words) and "number" in words[i+1]['text'].lower():
                    if w['x0'] > 100: # Prevent matching "Record Series" in Col 1
                        g1 = w['x0'] - 10
                        page_header_bottom = max(page_header_bottom, w['bottom'])
                        found_headers = True
                elif text == "scheduled" and i+1 < len(words) and "retention" in words[i+1]['text'].lower():
                    g2 = w['x0'] - 10
                    page_header_bottom = max(page_header_bottom, w['bottom'])
                elif text == "disposition" and i+1 < len(words) and "method" in words[i+1]['text'].lower():
                    g3 = w['x0'] - 10
                    page_header_bottom = max(page_header_bottom, w['bottom'])

            # 2. Filter out boilerplate headers and footers
            valid_words = []
            for w in words:
                text = w['text'].lower()
                
                # Exclude standard headers (using our calibrated header_bottom), footers, and margins
                if w['top'] < page_header_bottom + 5 or w['top'] < 50 or w['bottom'] > page.height - 40: continue
                if re.match(r'^(page\s*)?\d+\s+of\s+\d+$', text): continue
                if "800 e. broad" in text or "23219" in text or "692-3600" in text: continue
                if "records retention and disposition" in text or "effective schedule date" in text: continue
                
                # Exclude common section banners
                sections = ["administrative records", "fiscal records", "personnel records", 
                            "health records", "law enforcement", "general services", 
                            "criminal justice", "college and university", "library and museum"]
                if any(sec == text for sec in sections): continue
                
                valid_words.append(w)

            # 3. Locate the 6-digit Series Numbers to act as row dividers (Must be in Col 2)
            anchors = [w for w in valid_words if g1 <= w['x0'] < g2 and re.match(r'^\d{6}$', w['text'].strip())]
            anchors.sort(key=lambda x: x['top'])

            # If there are no new records, everything on this page belongs to the previous page's record!
            if not anchors:
                if current_record:
                    for w in valid_words:
                        if w['x1'] < g1: current_record['col1_words'].append(w)
                        elif g2 <= w['x0'] < g3: current_record['col3_words'].append(w)
                        elif w['x0'] >= g3: current_record['col4_words'].append(w)
                continue

            # 4. Create horizontal bands (rows) and drop words into their exact X/Y silos
            bands = []
            for i, anchor in enumerate(anchors):
                y_start = anchor['top'] - 15 # Buffer to catch Titles just above the number
                y_end = anchors[i+1]['top'] - 15 if i + 1 < len(anchors) else page.height
                bands.append({
                    'series_id': anchor['text'].strip(),
                    'y_start': y_start,
                    'y_end': y_end,
                    'col1_words': [],
                    'col3_words': [],
                    'col4_words': []
                })

            for w in valid_words:
                if w in anchors: continue
                
                assigned = False
                for band in bands:
                    if band['y_start'] <= w['top'] < band['y_end']:
                        if w['x1'] < g1: band['col1_words'].append(w)
                        elif g2 <= w['x0'] < g3: band['col3_words'].append(w)
                        elif w['x0'] >= g3: band['col4_words'].append(w)
                        assigned = True
                        break

                # Catch descriptions spilling over from the very bottom of the previous page
                if not assigned and current_record and w['top'] < bands[0]['y_start']:
                    if w['x1'] < g1: current_record['col1_words'].append(w)
                    elif g2 <= w['x0'] < g3: current_record['col3_words'].append(w)
                    elif w['x0'] >= g3: current_record['col4_words'].append(w)

            # Move completed rows to the master list, keeping the last one for potential spillover
            for band in bands:
                if current_record: all_records.append(current_record)
                current_record = band

    if current_record:
        all_records.append(current_record)

    # 5. Build and format the final JSON strings
    processed_records = []
    for rec in all_records:
        raw_desc = stringify_words(rec['col1_words'])
        retention = stringify_words(rec['col3_words'])
        disposition = stringify_words(rec['col4_words'])
        
        # Isolate Title from Description
        match = re.search(r'((?:This series\s+)?(?:documents|Documents|Collects|Verifies)\b.*)', raw_desc, re.IGNORECASE)
        if match:
            series_title = raw_desc[:match.start()].strip()
            series_description = match.group(1).strip()
        else:
            parts = raw_desc.split('.', 1)
            if len(parts) > 1 and len(parts[0]) < 100:
                series_title = parts[0].strip()
                series_description = parts[1].strip() if parts[1] else ""
            else:
                series_title = raw_desc
                series_description = ""

        # Legal Citations
        legal_citation = ""
        citation_pattern = r'(\b\d+\s*CFR.*|\b\d+\s*VAC.*|\bCode of Virginia\b.*|\bCOV\b.*|\b\d+\s*USC.*)$'
        citation_match = re.search(citation_pattern, series_description, re.IGNORECASE)
        if citation_match:
            legal_citation = citation_match.group(1).strip()
            series_description = series_description[:citation_match.start()].strip()
            series_description = re.sub(r'[\.,;:]$', '', series_description).strip()

        # Retention Years
        retention_years_match = re.search(r'(\d+)\s*[Yy]ear', retention)
        if retention_years_match:
            retention_years = int(retention_years_match.group(1))
        elif 'permanent' in retention.lower() or 'permanent' in disposition.lower():
            retention_years = None
        else:
            retention_years = None

        is_confidential = "confidential" in disposition.lower() and "non-confidential" not in disposition.lower()

        processed_records.append({
            "state": "va",
            "schedule_type": "general",
            "schedule_id": schedule_id,
            "series_id": rec['series_id'],
            "series_title": series_title,
            "series_description": series_description,
            "retention_statement": retention,
            "retention_years": retention_years,
            "disposition": disposition,
            "confidential": is_confidential,
            "legal_citation": legal_citation,
            "last_updated": effective_date,
            "last_checked": str(date.today())
        })

    output_path = Path(output_dir) / f"{schedule_id}.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(processed_records, f, indent=2, ensure_ascii=False)
        
    print(f"Processed: {schedule_id}.pdf -> {len(processed_records)} records found.")

if __name__ == "__main__":
    input_directory = "../pdfs" 
    output_directory = "../../data/va"
    os.makedirs(output_directory, exist_ok=True)
    
    for filename in os.listdir(input_directory):
        if filename.lower().endswith(".pdf"):
            process_pdf_to_json(os.path.join(input_directory, filename), output_directory)