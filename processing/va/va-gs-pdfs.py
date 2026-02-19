import pdfplumber
import json
import re
import os
from pathlib import Path
from datetime import datetime, date

def extract_effective_date(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) == 0: return None
        first_page = pdf.pages[0].extract_text()
        if first_page:
            match = re.search(r'(?i)(?:EFFECTIVE.*?DATE|:)\s*(\d{1,2}/\d{1,2}/\d{4})', first_page)
            if match:
                return datetime.strptime(match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
    return None

def stringify_words(word_list):
    """Sorts words top-to-bottom, left-to-right, and cleans up line-break hyphenation."""
    if not word_list: return ""
    word_list.sort(key=lambda w: (round(w['top']/5)*5, w['x0']))
    text = " ".join([w['text'] for w in word_list])
    return re.sub(r'-\s+', '', text).strip()

def clean_record_fields(record):
    """Universal clean-up for raw extracted data."""
    title = re.sub(r'\s+', ' ', record['series_title']).strip()
    desc = re.sub(r'\s+', ' ', record['series_description']).strip()
    retention = re.sub(r'\s+', ' ', record['retention_statement']).strip()
    disposition = re.sub(r'\s+', ' ', record['disposition']).strip()

    # Base Retention/Disposition Split
    disp_match = re.search(r'(?i)(Non-confidential Destruction|Confidential Destruction|Permanent, Archives|Permanent, In Agency|Archives|Destruction)$', disposition if disposition else retention)
    if disp_match and not disposition:
        disposition = disp_match.group(1).title()
        retention = retention[:disp_match.start()].strip()

    # Pluck drifted Confidentiality modifiers out of the retention text
    conf_match = re.search(r'(?i)\b(Non-confidential|Confidential)\b', retention)
    if conf_match:
        retention = retention[:conf_match.start()] + retention[conf_match.end():]
        retention = re.sub(r'\s+', ' ', retention).strip()
        if not disposition.lower().startswith(conf_match.group(1).lower()):
            disposition = f"{conf_match.group(1).title()} {disposition}".strip()
            
    # Pluck drifted "Destruction" or "Archives" from the MIDDLE of the retention string
    for kw in ["Destruction", "Archives"]:
        kw_match = re.search(fr'(?i)\b{kw}\b', retention)
        if kw_match and kw.lower() not in disposition.lower():
            retention = retention[:kw_match.start()] + retention[kw_match.end():]
            retention = re.sub(r'\s+', ' ', retention).strip()
            disposition = f"{disposition} {kw}".strip()

    # Extract Legal Citations
    legal_citation = ""
    citation_pattern = r'(\b\d+\s*CFR.*|\b\d+\s*VAC.*|\bCode of Virginia\b.*|\bCOV\b.*|\b\d+\s*USC.*)$'
    citation_match = re.search(citation_pattern, desc, re.IGNORECASE)
    if citation_match:
        legal_citation = citation_match.group(1).strip()
        desc = desc[:citation_match.start()].strip()
        desc = re.sub(r'[\.,;:]$', '', desc).strip()

    retention_years_match = re.search(r'(\d+)\s*[Yy]ear', retention)
    if retention_years_match:
        retention_years = int(retention_years_match.group(1))
    elif 'permanent' in retention.lower() or 'permanent' in disposition.lower():
        retention_years = None
    else:
        retention_years = None

    is_confidential = "confidential" in disposition.lower() and "non-confidential" not in disposition.lower()

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


def parse_using_table_engine(pdf_path, schedule_id, effective_date):
    """Method A: Uses pdfplumber's extract_tables()"""
    processed_records = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2: continue
                
                first_row = " ".join([str(c) for c in table[0] if c]).upper()
                if "SERIES" in first_row or "DESCRIPTION" in first_row:
                    headers = [str(c).replace('\n', ' ').strip() if c else "" for c in table[0]]
                    rows = table[1:]
                else:
                    headers = ['RECORDS SERIES AND DESCRIPTION', 'SERIES NUMBER', 'SCHEDULED RETENTION PERIOD', 'DISPOSITION METHOD']
                    rows = table

                col_idx = {'desc': 0, 'id': 1, 'ret': 2, 'disp': 3}
                for i, h in enumerate(headers):
                    h_upper = h.upper()
                    if "DESCRIPTION" in h_upper: col_idx['desc'] = i
                    elif "NUMBER" in h_upper: col_idx['id'] = i
                    elif "RETENTION" in h_upper: col_idx['ret'] = i
                    elif "DISPOSITION" in h_upper: col_idx['disp'] = i

                for row in rows:
                    clean_row = [str(cell) if cell else "" for cell in row]
                    if len(clean_row) <= max(col_idx.values()): continue

                    series_number = clean_row[col_idx['id']].replace('\n', '').strip()
                    if not re.match(r'^\d{6}$', series_number): continue

                    series_and_desc = clean_row[col_idx['desc']]
                    
                    if '\n' in series_and_desc:
                        parts = series_and_desc.split('\n', 1)
                        series_title = parts[0]
                        series_description = parts[1]
                    else:
                        match = re.search(r'((?:This series\s+)?(?:documents|Documents|Collects|Verifies|Consists|consists)\b.*)', series_and_desc, re.IGNORECASE)
                        if match:
                            series_title = series_and_desc[:match.start()]
                            series_description = match.group(1)
                        else:
                            series_title = series_and_desc
                            series_description = ""

                    raw_record = {
                        "state": "va",
                        "schedule_type": "general",
                        "schedule_id": schedule_id,
                        "series_id": series_number,
                        "series_title": series_title,
                        "series_description": series_description,
                        "retention_statement": clean_row[col_idx['ret']],
                        "disposition": clean_row[col_idx['disp']],
                        "last_updated": effective_date,
                        "last_checked": str(date.today())
                    }
                    processed_records.append(clean_record_fields(raw_record))
    return processed_records

def parse_using_vertical_silo(pdf_path, schedule_id, effective_date):
    """Method B: Uses your preferred exact X/Y pixel coordinates vertical walls."""
    all_records = []
    current_record = None
    g1, g2, g3 = 150, 400, 550 

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(keep_blank_chars=False)
            if not words: continue

            header_bottom = 0
            page_g1, page_g2, page_g3 = None, None, None

            for i, w in enumerate(words):
                text = w['text'].lower()
                
                if text == "series" and i+1 < len(words) and "number" in words[i+1]['text'].lower():
                    if w['x0'] > 100:  
                        page_g1 = w['x0'] - 10
                        header_bottom = max(header_bottom, w['bottom'])
                elif text == "scheduled" and i+1 < len(words) and "retention" in words[i+1]['text'].lower():
                    page_g2 = w['x0'] - 10
                    header_bottom = max(header_bottom, w['bottom'])
                elif text == "disposition" and i+1 < len(words) and "method" in words[i+1]['text'].lower():
                    page_g3 = w['x0'] - 10
                    header_bottom = max(header_bottom, w['bottom'])

            if page_g1: g1 = page_g1
            if page_g2: g2 = page_g2
            if page_g3: g3 = page_g3

            valid_words = []
            for w in words:
                text = w['text'].lower()
                
                if w['top'] < header_bottom + 5 or w['top'] < 50 or w['bottom'] > page.height - 40: continue
                if re.match(r'^(page\s*)?\d+\s+of\s+\d+$', text): continue
                if "800 e. broad" in text or "23219" in text or "692-3600" in text: continue
                if "records retention and disposition" in text or "effective schedule date" in text: continue
                
                valid_words.append(w)

            anchors = [w for w in valid_words if g1 <= w['x0'] < g2 and re.match(r'^\d{6}$', w['text'].strip())]
            anchors.sort(key=lambda x: x['top'])

            if not anchors:
                if current_record:
                    for w in valid_words:
                        if w['x1'] < g1: current_record['desc_words'].append(w)
                        elif g2 <= w['x0'] < g3: current_record['ret_words'].append(w)
                        elif w['x0'] >= g3: current_record['disp_words'].append(w)
                continue

            bands = []
            for i, anchor in enumerate(anchors):
                y_start = anchor['top'] - 15
                y_end = anchors[i+1]['top'] - 15 if i + 1 < len(anchors) else page.height
                bands.append({
                    'series_id': anchor['text'].strip(),
                    'y_start': y_start,
                    'y_end': y_end,
                    'desc_words': [],
                    'ret_words': [],
                    'disp_words': []
                })

            for w in valid_words:
                if w in anchors: continue
                
                assigned = False
                for band in bands:
                    if band['y_start'] <= w['top'] < band['y_end']:
                        if w['x1'] < g1: band['desc_words'].append(w)
                        elif g2 <= w['x0'] < g3: band['ret_words'].append(w)
                        elif w['x0'] >= g3: band['disp_words'].append(w)
                        assigned = True
                        break

                if not assigned and current_record and w['top'] < bands[0]['y_start']:
                    if w['x1'] < g1: current_record['desc_words'].append(w)
                    elif g2 <= w['x0'] < g3: current_record['ret_words'].append(w)
                    elif w['x0'] >= g3: current_record['disp_words'].append(w)

            for band in bands:
                if current_record: all_records.append(current_record)
                current_record = band

    if current_record:
        all_records.append(current_record)

    processed_records = []
    for rec in all_records:
        raw_desc = stringify_words(rec['desc_words'])
        retention = stringify_words(rec['ret_words'])
        disposition = stringify_words(rec['disp_words'])
        
        match = re.search(r'((?:This series\s+)?(?:documents|Documents|Collects|Verifies|consists|Consists)\b.*)', raw_desc, re.IGNORECASE)
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

        raw_record = {
            "state": "va",
            "schedule_type": "general",
            "schedule_id": schedule_id,
            "series_id": rec['series_id'],
            "series_title": series_title,
            "series_description": series_description,
            "retention_statement": retention,
            "disposition": disposition,
            "last_updated": effective_date,
            "last_checked": str(date.today())
        }
        processed_records.append(clean_record_fields(raw_record))

    return processed_records

def score_records(records):
    """Evaluates the quality of a parsed dataset to determine the winner."""
    if not records: return -9999
    
    score = len(records) * 10
    
    for r in records:
        title = r.get('series_title', '').strip()
        desc = r.get('series_description', '').strip()
        ret = r.get('retention_statement', '').strip()
        
        if not title: score -= 15
        if not desc: score -= 5
        if not ret: score -= 10
        
        if title.lower().startswith('this series'): score -= 15
        if title.lower().startswith('documents '): score -= 15
        
        if desc and not title: score -= 10
        
        if "COV" in title or "CFR" in title or "VAC" in title: score -= 10
        if re.search(r'\d{6}', title): score -= 10 
        
    return score

def process_and_evaluate(pdf_path, output_dir):
    schedule_id = Path(pdf_path).stem
    effective_date = extract_effective_date(pdf_path)
    
    records_via_table = parse_using_table_engine(pdf_path, schedule_id, effective_date)
    records_via_silo = parse_using_vertical_silo(pdf_path, schedule_id, effective_date)
    
    score_table = score_records(records_via_table)
    score_silo = score_records(records_via_silo)
    
    if score_silo >= score_table and score_silo > 0:
        best_records = records_via_silo
        winning_method = "Vertical Silo"
    else:
        best_records = records_via_table
        winning_method = "Table Engine"
        
    output_path = Path(output_dir) / f"{schedule_id}.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(best_records, f, indent=2, ensure_ascii=False)
        
    print(f"Processed: {schedule_id}.pdf -> {len(best_records)} records. (Winner: {winning_method} | Score: {max(score_table, score_silo)} vs {min(score_table, score_silo)})")

if __name__ == "__main__":
    input_directory = "../pdfs" 
    output_directory = "../../data/va"
    os.makedirs(output_directory, exist_ok=True)
    
    for filename in os.listdir(input_directory):
        if filename.lower().endswith(".pdf"):
            process_and_evaluate(os.path.join(input_directory, filename), output_directory)