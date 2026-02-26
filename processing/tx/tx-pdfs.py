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
    "state": "tx",
    "default_walls": (80, 160, 280, 550, 650, 750),  # Adjusted for Texas column boundaries
    "footer_strings": [
        "slr 105", "rev.", "library archives", "state of texas", 
        "records retention schedule", "retention codes", "archival codes", 
        "av-administratively valuable", "ce-calendar year end", 
        "fe- fiscal year end", "la-life of asset", "pm-permanent", 
        "us-until superseded", "exempt from archival review"
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
    is_image = True
    eff_date = None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return is_image, eff_date

            pages_to_check = min(3, len(pdf.pages))

            for i in range(pages_to_check):
                text = pdf.pages[i].extract_text(x_tolerance=2, y_tolerance=2)

                if text and text.strip():
                    is_image = False 

                    if eff_date is None:
                        # Texas specific pattern: "approved for use as of 12/14/2020"
                        match = re.search(r'(?i)approved for use as of\s+(\d{1,2}/\d{1,2}/\d{4})', text)
                        if match:
                            eff_date = datetime.strptime(match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                    
                    if eff_date:
                        break

    except Exception as e:
        logger.warning(f"Could not complete pre-flight check for {pdf_path}: {e}")

    return is_image, eff_date


def stringify_words(word_list: list[dict]) -> str:
    if not word_list:
        return ""
    word_list.sort(key=lambda w: (round(w['top'] / 5) * 5, w['x0']))
    text = " ".join([w['text'] for w in word_list])
    return re.sub(r'-\s+', '', text).strip()


def split_title_and_description(raw_text: str) -> tuple[str, str]:
    match = re.search(
        r'((?:This series\s+)?(?:documents|Collects|Verifies|Consists|consists|Includes|Records)\b.*)',
        raw_text, re.IGNORECASE
    )
    if match:
        return raw_text[:match.start()].strip(), match.group(1).strip()
    parts = raw_text.split('.', 1)
    if len(parts) > 1 and len(parts[0]) < 100:
        return parts[0].strip(), parts[1].strip()
    return raw_text.strip(), ""


def clean_record_fields(record: dict) -> dict:
    title = re.sub(r'\s+', ' ', record['series_title']).strip()
    desc = re.sub(r'\s+', ' ', record['series_description']).strip()
    retention = re.sub(r'\s+', ' ', record['retention_statement']).strip()
    disposition = re.sub(r'\s+', ' ', record['disposition']).strip()

    legal_citation = record.get('legal_citation', "")
    citation_pattern = r'(\b\d+\s*CFR.*|\b\d+\s*TAC.*|\bGovernment Code\b.*|\bUSC\b.*)$'
    
    if not legal_citation:
        citation_match = re.search(citation_pattern, desc, re.IGNORECASE)
        if citation_match:
            legal_citation = citation_match.group(1).strip()
            desc = desc[:citation_match.start()].strip()
            desc = re.sub(r'[\.,;:]$', '', desc).strip()

    retention_years = None
    retention_years_match = re.search(r'\b(\d+)\b', retention)
    if retention_years_match:
        retention_years = int(retention_years_match.group(1))
    elif 'PM' in retention.upper() or 'PM' in disposition.upper():
        retention_years = 999 

    is_confidential = (
        "confidential" in desc.lower() or "confidential" in disposition.lower()
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
    processed_records = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue

                # Texas tables can be wide; combine header rows if split
                first_row = " ".join([str(c) for c in table[0] if c]).upper()
                if "AGENCY ITEM" in first_row or "RECORD SERIES" in first_row:
                    headers = [str(c).replace('\n', ' ').strip() if c else "" for c in table[0]]
                    rows = table[1:]
                else:
                    headers = [
                        'Agency Item No.', 'Record Series Item No.', 'Record Series Title',
                        'Description', 'Retention Period', 'AC Definition', 'Archival', 'Remarks', 'Legal Citations'
                    ]
                    rows = table

                col_idx = {'agency_id': 0, 'series_id': 1, 'title': 2, 'desc': 3, 'ret': 4, 'ac_def': 5, 'remarks': 7, 'legal': 8}
                for i, h in enumerate(headers):
                    h_upper = h.upper()
                    if "AGENCY ITEM" in h_upper: col_idx['agency_id'] = i
                    elif "RECORD SERIES ITEM" in h_upper: col_idx['series_id'] = i
                    elif "SERIES TITLE" in h_upper: col_idx['title'] = i
                    elif "DESCRIPTION" in h_upper: col_idx['desc'] = i
                    elif "RETENTION" in h_upper or "RET." in h_upper or "CODE" in h_upper: col_idx['ret'] = i
                    elif "AC DEFINITION" in h_upper: col_idx['ac_def'] = i
                    elif "REMARKS" in h_upper: col_idx['remarks'] = i
                    elif "LEGAL" in h_upper: col_idx['legal'] = i

                for row in rows:
                    clean_row = [str(cell).replace('\n', ' ') if cell else "" for cell in row]
                    if len(clean_row) <= max(col_idx.values()):
                        continue

                    series_number = clean_row[col_idx['series_id']].strip()
                    agency_item = clean_row[col_idx['agency_id']].strip()
                    
                    if not series_number and not agency_item:
                        continue

                    title = clean_row[col_idx['title']]
                    desc = clean_row[col_idx['desc']]
                    
                    # Merge Title and Desc if they fell into the same column
                    if not desc and len(title) > 50:
                        title, desc = split_title_and_description(title)

                    retention_logic = f"{clean_row[col_idx['ret']]} {clean_row[col_idx['ac_def']]}".strip()

                    raw_record = {
                        "state": CONFIG["state"],
                        "agency_name": agency_name,
                        "schedule_type": "specific",
                        "schedule_id": schedule_id,
                        "agency_item_id": agency_item,
                        "series_id": series_number,
                        "series_title": title,
                        "series_description": desc,
                        "retention_statement": retention_logic,
                        "disposition": clean_row[col_idx['remarks']],
                        "legal_citation": clean_row.get(col_idx['legal'], ""),
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
    processed_records = []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    current_record = None

    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) >= 4:
                col1 = cells[0].get_text(strip=True)  # Agency ID
                col2 = cells[1].get_text(strip=True)  # State Series ID
                col3 = cells[2].get_text(strip=True, separator=' ')  # Title / Desc
                col4 = cells[3].get_text(strip=True, separator=' ')  # Retention codes/years
                
                if "ITEM NO" in col1.upper() or "RECORD SERIES" in col2.upper():
                    continue
                    
                if col1 or re.match(r'^\d+\.\d+', col2):
                    if current_record:
                        processed_records.append(clean_record_fields(current_record))
                        
                    series_title, series_desc = split_title_and_description(col3)
                    
                    current_record = {
                        "state": CONFIG["state"],
                        "agency_name": agency_name,
                        "schedule_type": "specific",
                        "schedule_id": schedule_id,
                        "agency_item_id": col1,
                        "series_id": col2,
                        "series_title": series_title,
                        "series_description": series_desc,
                        "retention_statement": col4, 
                        "disposition": cells[-2].get_text(strip=True, separator=' ') if len(cells) > 5 else "", 
                        "legal_citation": cells[-1].get_text(strip=True, separator=' ') if len(cells) > 6 else "",
                        "last_updated": effective_date,
                        "last_checked": str(date.today())
                    }
                
                elif current_record and not col1 and not col2:
                    if current_record["series_description"]:
                        current_record["series_description"] += " " + col3
                    else:
                        current_record["series_description"] = col3

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
                    "--output-dir", str(pdf_path.parent),
                    "--output-format", "html"
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
    if not records:
        return -9999

    score = len(records) * 10
    seen_ids: set[str] = set()

    for r in records:
        title = r.get('series_title', '').strip()
        desc = r.get('series_description', '').strip()
        ret = r.get('retention_statement', '').strip()
        sid = r.get('series_id', '')
        aid = r.get('agency_item_id', '')

        if sid in seen_ids or aid in seen_ids:
            score -= 20
        
        if sid: seen_ids.add(sid)
        if aid: seen_ids.add(aid)

        if not title:
            score -= 15
        if not desc:
            score -= 5
        if not ret:
            score -= 10

        if title.lower().startswith('this series'):
            score -= 15
        if title.lower().startswith('records '):
            score -= 15

        if desc and not title:
            score -= 10

        if "CFR" in title or "TAC" in title:
            score -= 10

        if r.get('retention_years') is not None:
            score += 3

    return score


def select_optimal_strategy_memory_aware(pdf_path: Path, is_image: bool) -> list[str]:
    file_size_mb = pdf_path.stat().st_size / (1024 * 1024)
    
    if is_image:
        return ['html']
        
    if file_size_mb > 50:
        logger.warning(f"[{pdf_path.stem}] Large file detected ({file_size_mb:.1f}MB). Routing to Table exclusively.")
        return ['table']
        
    return ['table', 'html']


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def process_and_evaluate(pdf_path: Path, output_dir: Path, agency_mapping: dict) -> None:
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

            score = score_records(records)
            
            if score > best_score:
                best_score = score
                best_records = records
                winning_method = strategy.upper()

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
    input_dir = Path("pdfs")
    output_dir = Path("../../data/tx")
    csv_path = Path("agencies.csv") 
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    agency_mapping = load_agency_mapping(csv_path)

    pdf_files = list(input_dir.glob("*.pdf"))

    if not pdf_files:
        logger.warning(f"No PDF files found in {input_dir}")
    else:
        worker = partial(process_and_evaluate, output_dir=output_dir, agency_mapping=agency_mapping)
        
        ctx = multiprocessing.get_context('spawn')
        
        with ctx.Pool(processes=1, maxtasksperchild=25) as pool:
            pool.map(worker, pdf_files)