import os
import re
import logging
import subprocess
import contextlib
from pathlib import Path
from datetime import date
import pdfplumber
from bs4 import BeautifulSoup

from processing.base_config import StateScheduleConfig
from processing.core import stringify_words, split_title_and_description
from processing.central_file import make_record, get_nested_val, set_nested_val, clean_record_fields

logger = logging.getLogger(__name__)

def parse_using_table_engine(
    pdf_path: Path, schedule_id: str, effective_date: str | None,
    schema: dict, config: StateScheduleConfig
) -> list[dict]:
    processed_records = []
    schedule_type = "general" if schedule_id.startswith("GS") else "specific"

    def match_header(cell_text: str, keyword_list: list[str]) -> bool:
        text_upper = cell_text.upper()
        return any(kw in text_upper for kw in keyword_list)

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue

                first_row = " ".join([str(c) for c in table[0] if c]).upper()
                is_header_row = False
                for kw_list in config.header_keywords.values():
                    if any(kw in first_row for kw in kw_list):
                        is_header_row = True
                        break

                if is_header_row:
                    headers = [str(c).replace('\n', ' ').strip() if c else "" for c in table[0]]
                    rows = table[1:]
                else:
                    headers = [
                        config.header_keywords.get('desc', [''])[0],
                        config.header_keywords.get('id', [''])[0],
                        config.header_keywords.get('ret', [''])[0],
                        config.header_keywords.get('disp', [''])[0]
                    ]
                    rows = table

                col_idx = {'desc': 0, 'id': 1, 'ret': 2, 'disp': 3}
                for i, h in enumerate(headers):
                    if match_header(h, config.header_keywords.get('desc', [])): col_idx['desc'] = i
                    elif match_header(h, config.header_keywords.get('id', [])): col_idx['id'] = i
                    elif match_header(h, config.header_keywords.get('ret', [])): col_idx['ret'] = i
                    elif match_header(h, config.header_keywords.get('disp', [])): col_idx['disp'] = i

                for row in rows:
                    clean_row = [str(cell) if cell else "" for cell in row]
                    
                    if len(clean_row) <= col_idx['id']:
                        continue

                    series_number = clean_row[col_idx['id']].replace('\n', '').strip()
                    if not config.series_id_pattern.match(series_number):
                        continue

                    series_and_desc = clean_row[col_idx['desc']]

                    if '\n' in series_and_desc:
                        parts = series_and_desc.split('\n', 1)
                        series_title = parts[0].strip()
                        series_description = parts[1].strip()
                    else:
                        series_title, series_description = split_title_and_description(series_and_desc)

                    ret_start_col = col_idx['id'] + 1
                    ret_end_col = col_idx['disp']
                    if ret_end_col <= ret_start_col:
                        ret_end_col = ret_start_col + 1
                        
                    retention_pieces = []
                    for c in range(ret_start_col, ret_end_col):
                        if c < len(clean_row) and clean_row[c]:
                            retention_pieces.append(clean_row[c].replace('\n', ' ').strip())
                    
                    retention_statement = " ".join(retention_pieces)
                    disp_idx = col_idx['disp']
                    raw_disposition = clean_row[disp_idx].replace('\n', ' ').strip() if disp_idx < len(clean_row) else ""

                    raw_record = make_record(
                        schema,
                        state=config.state_code,
                        schedule_type=schedule_type,
                        schedule_id=schedule_id,
                        series_id=series_number,
                        series_title=series_title,
                        series_description=series_description,
                        retention_statement=retention_statement,
                        disposition=raw_disposition,
                        last_updated=effective_date,
                        last_checked=str(date.today())
                    )
                    processed_records.append(clean_record_fields(raw_record, config))

    return processed_records
    
def parse_using_vertical_silo(
    pdf_path: Path, schedule_id: str, effective_date: str | None,
    schema: dict, config: StateScheduleConfig
) -> list[dict]:
    all_records = []
    current_record = None
    g1, g2, g3 = config.default_walls
    footer_strings = config.footer_strings
    schedule_type = "general" if schedule_id.startswith("GS") else "specific"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(keep_blank_chars=False)
            if not words:
                continue

            header_bottom = 0
            page_g1, page_g2, page_g3 = None, None, None

            for i, w in enumerate(words):
                text_1 = w['text'].upper()
                text_2 = f"{text_1} {words[i+1]['text'].upper()}" if i + 1 < len(words) else text_1

                if not page_g1 and any(kw in text_1 or kw in text_2 for kw in config.header_keywords.get('id', [])):
                    if w['x0'] > 100:
                        page_g1 = w['x0'] - 10
                        header_bottom = max(header_bottom, w['bottom'])
                elif not page_g2 and any(kw in text_1 or kw in text_2 for kw in config.header_keywords.get('ret', [])):
                    page_g2 = w['x0'] - 10
                    header_bottom = max(header_bottom, w['bottom'])
                elif not page_g3 and any(kw in text_1 or kw in text_2 for kw in config.header_keywords.get('disp', [])):
                    page_g3 = w['x0'] - 10
                    header_bottom = max(header_bottom, w['bottom'])

            if page_g1: g1 = page_g1
            if page_g2: g2 = page_g2
            if page_g3: g3 = page_g3

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
                if g1 <= w['x0'] < g2 and config.series_id_pattern.match(w['text'].strip())
            ]
            anchors.sort(key=lambda x: x['top'])

            if not anchors:
                if current_record:
                    for w in valid_words:
                        if w['x0'] < g1: current_record['desc_words'].append(w)
                        elif w['x0'] >= g3: current_record['disp_words'].append(w)
                        else: current_record['ret_words'].append(w)
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
                if w in anchors: continue
                assigned = False
                for band in bands:
                    if band['y_start'] <= w['top'] < band['y_end']:
                        if w['x0'] < g1: 
                            band['desc_words'].append(w)
                        elif w['x0'] >= g3: 
                            band['disp_words'].append(w)
                        else:
                            band['ret_words'].append(w)
                        assigned = True
                        break
                if not assigned and current_record and w['top'] < bands[0]['y_start']:
                    if w['x0'] < g1: 
                        current_record['desc_words'].append(w)
                    elif w['x0'] >= g3: 
                        current_record['disp_words'].append(w)
                    else:
                        current_record['ret_words'].append(w)

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

        series_title, series_description = split_title_and_description(raw_desc)

        raw_record = make_record(
            schema,
            state=config.state_code,
            schedule_type=schedule_type,
            schedule_id=schedule_id,
            series_id=rec['series_id'],
            series_title=series_title,
            series_description=series_description,
            retention_statement=retention,
            disposition=disposition,
            last_updated=effective_date,
            last_checked=str(date.today())
        )
        processed_records.append(clean_record_fields(raw_record, config))

    return processed_records

def parse_using_marker_html(
    html_content: str, schedule_id: str, effective_date: str | None,
    schema: dict, config: StateScheduleConfig
) -> list[dict]:
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

                if config.series_id_pattern.match(col2):
                    if current_record:
                        processed_records.append(clean_record_fields(current_record, config))

                    series_title, series_desc = split_title_and_description(col1)

                    current_record = make_record(
                        schema,
                        state=config.state_code,
                        schedule_type=schedule_type,
                        schedule_id=schedule_id,
                        series_id=col2,
                        series_title=series_title,
                        series_description=series_desc,
                        retention_statement=col3,
                        disposition="",
                        last_updated=effective_date,
                        last_checked=str(date.today())
                    )

                elif current_record and not col2 and not col3:
                    existing_desc = get_nested_val(current_record, 'series_description') or ""
                    new_desc = (existing_desc + " " + col1.replace('\n', ' ')).strip()
                    set_nested_val(current_record, 'series_description', new_desc)

    if current_record:
        processed_records.append(clean_record_fields(current_record, config))

    return processed_records

def parse_using_marker_html_optimized(
    pdf_path: Path, schedule_id: str, effective_date: str | None,
    is_image: bool, schema: dict, config: StateScheduleConfig,
    gpu_semaphore = None
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
        # Use the semaphore to ensure only one worker uses the GPU at a time
        semaphore_context = gpu_semaphore if gpu_semaphore else contextlib.nullcontext()
        
        with semaphore_context:
            logger.info(f"[{schedule_id}] GPU Key Acquired. Running marker_single...")
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
        return parse_using_marker_html(html_content, schedule_id, effective_date, schema, config)

    return []

def select_optimal_strategy_memory_aware(pdf_path: Path, is_image: bool) -> list[str]:
    file_size_mb = pdf_path.stat().st_size / (1024 * 1024)

    if is_image:
        return ['html']

    if file_size_mb > 50:
        logger.warning(f"[{pdf_path.stem}] Large file detected ({file_size_mb:.1f}MB). Routing to Silo exclusively.")
        return ['silo']

    return ['table', 'silo']
