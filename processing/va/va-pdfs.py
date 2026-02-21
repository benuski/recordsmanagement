import os
import gc
import pdfplumber
import json
import re
import csv
import logging
import subprocess
import multiprocessing
from pathlib import Path
from datetime import datetime, date
from functools import partial
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CONFIG = {
    "state": "va",
    "default_walls": (150, 400, 550),
    "footer_strings": [
        "800 e. broad", "23219", "692-3600",
        "records retention and disposition", "effective schedule date"
    ],
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def load_agency_mapping(csv_path: Path) -> dict[str, str]:
    """Loads the agency code to agency name mapping from a CSV."""
    mapping = {}
    if not csv_path.exists():
        logger.warning(f"Agency CSV not found at {csv_path}. Agency names will be None.")
        return mapping
        
    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("Agency Code", "").strip()
                name = row.get("Agency Name", "").strip()
                if code and name:
                    mapping[code] = name
    except Exception as e:
        logger.error(f"Failed to load agency mapping: {e}")
        
    return mapping


def analyze_pdf_preflight(pdf_path: Path) -> tuple[bool, str | None]:
    """
    Single-pass check to minimize pdfplumber I/O and memory overhead.
    Returns: (is_image_pdf, effective_date)
    """
    is_image = True
    eff_date = None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return is_image, eff_date

            # Check up to 2 pages to account for image-based cover sheets
            pages_to_check = min(2, len(pdf.pages))

            for i in range(pages_to_check):
                text = pdf.pages[i].extract_text(x_tolerance=2, y_tolerance=2)

                if text and text.strip():
                    is_image = False  # We found selectable text!

                    # Only search for the date if we haven't found it yet
                    if eff_date is None:
                        match = re.search(r'(?i)EFFECTIVE\s+(?:SCHEDULE\s+)?DATE[:\s]+(\d{1,2}/\d{1,2}/\d{4})', text)
                        if match:
                            eff_date = datetime.strptime(match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                    
                    # If we know it's a text PDF AND we found the date, stop reading pages
                    if eff_date:
                        break

    except Exception as e:
        logger.warning(f"Could not complete pre-flight check for {pdf_path}: {e}")

    return is_image, eff_date


def stringify_words(word_list: list[dict]) -> str:
    """Sorts words top-to-bottom, left-to-right, and cleans up line-break hyphenation."""
    if not word_list:
        return ""
    word_list.sort(key=lambda w: (round(w['top'] / 5) * 5, w['x0']))
    text = " ".join([w['text'] for w in word_list])
    return re.sub(r'-\s+', '', text).strip()


def split_title_and_description(raw_text: str) -> tuple[str, str]:
    """Shared helper to split a combined title+description string."""
    match = re.search(
        r'((?:This series\s+)?(?:documents|Collects|Verifies|Consists|consists)\b.*)',
        raw_text, re.IGNORECASE
    )
    if match:
        return raw_text[:match.start()].strip(), match.group(1).strip()
    parts = raw_text.split('.', 1)
    if len(parts) > 1 and len(parts[0]) < 100:
        return parts[0].strip(), parts[1].strip()
    return raw_text.strip(), ""


def clean_record_fields(record: dict) -> dict:
    """Universal clean-up for raw extracted data."""
    title = re.sub(r'\s+', ' ', record['series_title']).strip()
    desc = re.sub(r'\s+', ' ', record['series_description']).strip()
    retention = re.sub(r'\s+', ' ', record['retention_statement']).strip()
    disposition = re.sub(r'\s+', ' ', record['disposition']).strip()

    disp_match = re.search(
        r'(?i)(Non-confidential Destruction|Confidential Destruction|Permanent, Archives|Permanent, In Agency|Archives|Destruction)$',
        disposition if disposition else retention
    )
    if disp_match and not disposition:
        disposition = disp_match.group(1).title()
        retention = retention[:disp_match.start()].strip()

    conf_match = re.search(r'(?i)\b(Non-confidential|Confidential)\b', retention)
    if conf_match:
        retention = retention[:conf_match.start()] + retention[conf_match.end():]
        retention = re.sub(r'\s+', ' ', retention).strip()
        if not disposition.lower().startswith(conf_match.group(1).lower()):
            disposition = f"{conf_match.group(1).title()} {disposition}".strip()

    for kw in ["Destruction", "Archives"]:
        kw_match = re.search(fr'(?i)\b{kw}\b', retention)
        if kw_match and kw.lower() not in disposition.lower():
            retention = retention[:kw_match.start()] + retention[kw_match.end():]
            retention = re.sub(r'\s+', ' ', retention).strip()
            disposition = f"{disposition} {kw}".strip()

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


# ---------------------------------------------------------------------------
# Parsing Strategies
# ---------------------------------------------------------------------------

def parse_using_table_engine(
    pdf_path: Path,
    schedule_id: str,
    effective_date: str | None,
    agency_name: str | None
) -> list[dict]:
    """Method A: Uses pdfplumber's extract_tables()."""
    processed_records = []
    schedule_type = "general" if schedule_id.startswith("GS") else "specific"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue

                first_row = " ".join([str(c) for c in table[0] if c]).upper()
                if "SERIES" in first_row or "DESCRIPTION" in first_row:
                    headers = [str(c).replace('\n', ' ').strip() if c else "" for c in table[0]]
                    rows = table[1:]
                else:
                    headers = [
                        'RECORDS SERIES AND DESCRIPTION', 'SERIES NUMBER',
                        'SCHEDULED RETENTION PERIOD', 'DISPOSITION METHOD'
                    ]
                    rows = table

                col_idx = {'desc': 0, 'id': 1, 'ret': 2, 'disp': 3}
                for i, h in enumerate(headers):
                    h_upper = h.upper()
                    if "DESCRIPTION" in h_upper:
                        col_idx['desc'] = i
                    elif "NUMBER" in h_upper:
                        col_idx['id'] = i
                    elif "RETENTION" in h_upper:
                        col_idx['ret'] = i
                    elif "DISPOSITION" in h_upper:
                        col_idx['disp'] = i

                for row in rows:
                    clean_row = [str(cell) if cell else "" for cell in row]
                    if len(clean_row) <= max(col_idx.values()):
                        continue

                    series_number = clean_row[col_idx['id']].replace('\n', '').strip()
                    if not re.match(r'^\d{6}$', series_number):
                        continue

                    series_and_desc = clean_row[col_idx['desc']]

                    if '\n' in series_and_desc:
                        parts = series_and_desc.split('\n', 1)
                        series_title = parts[0].strip()
                        series_description = parts[1].strip()
                    else:
                        series_title, series_description = split_title_and_description(series_and_desc)

                    raw_record = {
                        "state": CONFIG["state"],
                        "agency_name": agency_name,
                        "schedule_type": schedule_type,
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


def parse_using_vertical_silo(
    pdf_path: Path,
    schedule_id: str,
    effective_date: str | None,
    agency_name: str | None
) -> list[dict]:
    """Method B: Uses exact X/Y pixel coordinate vertical walls."""
    all_records = []
    current_record = None
    g1, g2, g3 = CONFIG["default_walls"]
    footer_strings = CONFIG["footer_strings"]
    
    schedule_type = "general" if schedule_id.startswith("GS") else "specific"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(keep_blank_chars=False)
            if not words:
                continue

            header_bottom = 0
            page_g1, page_g2, page_g3 = None, None, None

            for i, w in enumerate(words):
                text = w['text'].lower()
                if text == "series" and i + 1 < len(words) and "number" in words[i + 1]['text'].lower():
                    if w['x0'] > 100:
                        page_g1 = w['x0'] - 10
                        header_bottom = max(header_bottom, w['bottom'])
                elif text == "scheduled" and i + 1 < len(words) and "retention" in words[i + 1]['text'].lower():
                    page_g2 = w['x0'] - 10
                    header_bottom = max(header_bottom, w['bottom'])
                elif text == "disposition" and i + 1 < len(words) and "method" in words[i + 1]['text'].lower():
                    page_g3 = w['x0'] - 10
                    header_bottom = max(header_bottom, w['bottom'])

            if page_g1:
                g1 = page_g1
            if page_g2:
                g2 = page_g2
            if page_g3:
                g3 = page_g3

            valid_words = []
            for w in words:
                text_lower = w['text'].lower()
                if w['top'] < header_bottom + 5 or w['top'] < 50 or w['bottom'] > page.height - 40:
                    continue
                if re.match(r'^(page\s*)?\d+\s+of\s+\d+$', text_lower):
                    continue
                if any(fs in text_lower for fs in footer_strings):
                    continue
                valid_words.append(w)

            anchors = [
                w for w in valid_words
                if g1 <= w['x0'] < g2 and re.match(r'^\d{6}$', w['text'].strip())
            ]
            anchors.sort(key=lambda x: x['top'])

            if not anchors:
                if current_record:
                    for w in valid_words:
                        if w['x1'] < g1:
                            current_record['desc_words'].append(w)
                        elif g2 <= w['x0'] < g3:
                            current_record['ret_words'].append(w)
                        elif w['x0'] >= g3:
                            current_record['disp_words'].append(w)
                continue

            bands = []
            for i, anchor in enumerate(anchors):
                y_start = anchor['top'] - 15
                y_end = anchors[i + 1]['top'] - 15 if i + 1 < len(anchors) else page.height
                bands.append({
                    'series_id': anchor['text'].strip(),
                    'y_start': y_start,
                    'y_end': y_end,
                    'desc_words': [],
                    'ret_words': [],
                    'disp_words': []
                })

            for w in valid_words:
                if w in anchors:
                    continue
                assigned = False
                for band in bands:
                    if band['y_start'] <= w['top'] < band['y_end']:
                        if w['x1'] < g1:
                            band['desc_words'].append(w)
                        elif g2 <= w['x0'] < g3:
                            band['ret_words'].append(w)
                        elif w['x0'] >= g3:
                            band['disp_words'].append(w)
                        assigned = True
                        break
                if not assigned and current_record and w['top'] < bands[0]['y_start']:
                    if w['x1'] < g1:
                        current_record['desc_words'].append(w)
                    elif g2 <= w['x0'] < g3:
                        current_record['ret_words'].append(w)
                    elif w['x0'] >= g3:
                        current_record['disp_words'].append(w)

            for band in bands:
                if current_record:
                    all_records.append(current_record)
                current_record = band

    if current_record:
        all_records.append(current_record)

    processed_records = []
    for rec in all_records:
        raw_desc = stringify_words(rec['desc_words'])
        retention = stringify_words(rec['ret_words'])
        disposition = stringify_words(rec['disp_words'])

        series_title, series_description = split_title_and_description(raw_desc)

        raw_record = {
            "state": CONFIG["state"],
            "agency_name": agency_name,
            "schedule_type": schedule_type,
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


def parse_using_marker_html(
    html_content: str,
    schedule_id: str,
    effective_date: str | None,
    agency_name: str | None
) -> list[dict]:
    """Helper method: Parses raw HTML string output generated by marker-pdf."""
    processed_records = []
    schedule_type = "general" if schedule_id.startswith("GS") else "specific"
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    current_record = None

    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) == 3:
                col1 = cells[0].get_text(strip=True, separator='\n')
                col2 = cells[1].get_text(strip=True)
                col3 = cells[2].get_text(strip=True)
                
                if "RECORDS SERIES" in col1.upper() or "EFFECTIVE SCHEDULE" in col1.upper():
                    continue
                    
                if re.match(r'^\d{6}$', col2):
                    if current_record:
                        processed_records.append(clean_record_fields(current_record))
                        
                    series_title, series_desc = split_title_and_description(col1)
                    
                    current_record = {
                        "state": CONFIG["state"],
                        "agency_name": agency_name,
                        "schedule_type": schedule_type,
                        "schedule_id": schedule_id,
                        "series_id": col2,
                        "series_title": series_title,
                        "series_description": series_desc,
                        "retention_statement": col3, 
                        "disposition": "", 
                        "last_updated": effective_date,
                        "last_checked": str(date.today())
                    }
                
                elif current_record and not col2 and not col3:
                    if current_record["series_description"]:
                        current_record["series_description"] += " " + col1.replace('\n', ' ')
                    else:
                        current_record["series_description"] = col1.replace('\n', ' ')

    if current_record:
        processed_records.append(clean_record_fields(current_record))
        
    return processed_records


def parse_using_marker_html_optimized(
    pdf_path: Path, 
    schedule_id: str, 
    effective_date: str | None, 
    agency_name: str | None,
    is_image: bool
) -> list[dict]:
    """Method C: Memory-optimized marker_single wrapper with GPU management."""
    expected_marker_dir = pdf_path.parent / schedule_id
    html_path = expected_marker_dir / f"{schedule_id}.html"
    
    if not html_path.exists() and pdf_path.with_suffix('.html').exists():
        html_path = pdf_path.with_suffix('.html')

    env = os.environ.copy()
    env['INFERENCE_RAM'] = '14'  
    env['CUDA_VISIBLE_DEVICES'] = '0' 

    if html_path.exists() and html_path.stat().st_mtime > pdf_path.stat().st_mtime:
        logger.info(f"[{schedule_id}] Using existing, up-to-date HTML file")
        
    elif is_image:
        logger.info(f"[{schedule_id}] Running marker_single with memory optimization...")
        try:
            subprocess.run(
                [
                    "marker_single", 
                    str(pdf_path), 
                    "--output_dir", str(pdf_path.parent),
                    "--output_format", "html"
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=600, 
                env=env
            )
            if expected_marker_dir.exists():
                html_path = expected_marker_dir / f"{schedule_id}.html"
                
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            error_msg = e.stderr if isinstance(e, subprocess.CalledProcessError) else "Timeout Expired"
            logger.error(f"[{schedule_id}] marker_single failed: {error_msg}")
            return []

    if html_path.exists():
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return parse_using_marker_html(html_content, schedule_id, effective_date, agency_name)
    
    return []


# ---------------------------------------------------------------------------
# Scoring & Routing
# ---------------------------------------------------------------------------

def score_records(records: list[dict]) -> int:
    """Evaluates the quality of a parsed dataset to determine the winner."""
    if not records:
        return -9999

    score = len(records) * 10
    seen_ids: set[str] = set()

    for r in records:
        title = r.get('series_title', '').strip()
        desc = r.get('series_description', '').strip()
        ret = r.get('retention_statement', '').strip()
        sid = r.get('series_id', '')

        if sid in seen_ids:
            score -= 20
        seen_ids.add(sid)

        if not title:
            score -= 15
        if not desc:
            score -= 5
        if not ret:
            score -= 10

        if title.lower().startswith('this series'):
            score -= 15
        if title.lower().startswith('documents '):
            score -= 15

        if desc and not title:
            score -= 10

        if "COV" in title or "CFR" in title or "VAC" in title:
            score -= 10
        if re.search(r'\d{6}', title):
            score -= 10

        if r.get('retention_years') is not None:
            score += 3

    return score


def select_optimal_strategy_memory_aware(pdf_path: Path, is_image: bool) -> list[str]:
    """Select parsing strategy based on PDF characteristics and memory constraints."""
    file_size_mb = pdf_path.stat().st_size / (1024 * 1024)
    
    if is_image:
        return ['html']
        
    if file_size_mb > 50:
        logger.warning(f"[{pdf_path.stem}] Large file detected ({file_size_mb:.1f}MB). Routing to Silo exclusively.")
        return ['silo']
        
    return ['table', 'silo']


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def process_and_evaluate(pdf_path: Path, output_dir: Path, agency_mapping: dict) -> None:
    """Sequential memory-optimized processing with penalty-free early termination."""
    pdf_path = Path(pdf_path)
    schedule_id = pdf_path.stem
    
    match = re.match(r'^(\d+)', schedule_id)
    agency_code = match.group(1) if match else schedule_id[:3]
    agency_name = agency_mapping.get(agency_code, None)

    try:
        is_image, effective_date = analyze_pdf_preflight(pdf_path)
        strategies = select_optimal_strategy_memory_aware(pdf_path, is_image)
        
        best_score = -9999
        best_records = []
        winning_method = "None"

        for strategy in strategies:
            logger.info(f"[{schedule_id}] Attempting strategy: {strategy.upper()}")
            records = []
            
            if strategy == 'html':
                records = parse_using_marker_html_optimized(
                    pdf_path, schedule_id, effective_date, agency_name, is_image
                )
            elif strategy == 'table':
                records = parse_using_table_engine(pdf_path, schedule_id, effective_date, agency_name)
            elif strategy == 'silo':
                records = parse_using_vertical_silo(pdf_path, schedule_id, effective_date, agency_name)

            score = score_records(records)
            
            if score > best_score:
                best_score = score
                best_records = records
                winning_method = strategy.upper()

            # Early Termination: Stop trying parsers if we got a totally penalty-free extraction
            if len(records) > 0 and score >= (len(records) * 10):
                logger.info(f"[{schedule_id}] Early termination triggered: {strategy.upper()} achieved a penalty-free extraction.")
                break

            del records
            gc.collect()

        if not best_records or best_score <= 0:
            logger.warning(f"[{schedule_id}] No valid records found across attempted strategies.")
            return

        output_path = output_dir / f"{schedule_id}.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(best_records, f, indent=2, ensure_ascii=False)

        logger.info(
            f"Processed: {schedule_id}.pdf -> {len(best_records)} records. "
            f"(Winner: {winning_method} | Score: {best_score})"
        )

    except Exception as e:
        logger.error(f"Failed to process {pdf_path.name}: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    input_dir = Path("../pdfs")
    output_dir = Path("../../data/va")
    csv_path = Path("agencies.csv") 
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    agency_mapping = load_agency_mapping(csv_path)

    pdf_files = list(input_dir.glob("*.pdf"))

    if not pdf_files:
        logger.warning(f"No PDF files found in {input_dir}")
    else:
        worker = partial(process_and_evaluate, output_dir=output_dir, agency_mapping=agency_mapping)
        
        # 'spawn' guarantees a clean OS slate for the new process
        ctx = multiprocessing.get_context('spawn')
        
        # processes=1 prevents PyTorch/CUDA Out-of-Memory crashes
        # maxtasksperchild=25 reduces process-creation overhead while preventing infinite RAM leaks
        with ctx.Pool(processes=1, maxtasksperchild=25) as pool:
            pool.map(worker, pdf_files)